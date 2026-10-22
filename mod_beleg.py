"""
mod_beleg.py – S3, PDF-Konvertierung, GPT-Analyse, Beleg-Pipeline
"""
from __future__ import annotations
import io, base64, re, json, os
import httpx
from datetime import date

OPENAI_KEY   = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
OPENAI_URL   = "https://api.openai.com/v1/chat/completions"
# Optional: Denkaufwand neuerer Modelle (GPT-5/6-Familie), z.B. "low" für
# schnellere/günstigere Belegauslesung. Leer lassen = Modell-Standard.
OPENAI_REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "")


def _modell_ist_klassisch() -> bool:
    """GPT-4/3.5-Familie nutzt max_tokens + temperature; neuere Modelle
    (GPT-5/6, o-Serie) verlangen stattdessen max_completion_tokens und lehnen
    ein abweichendes temperature mit HTTP 400 ab."""
    m = (OPENAI_MODEL or "").lower()
    return m.startswith("gpt-4") or m.startswith("gpt-3")


def _modell_params(max_tokens: int, klassisch: bool) -> dict:
    if klassisch:
        return {"max_tokens": max_tokens, "temperature": 0.0}
    # Bei Denk-Modellen zählen auch die internen Denkschritte zum Limit –
    # deshalb großzügiger bemessen, sonst bleibt für die Antwort nichts übrig.
    p = {"max_completion_tokens": max_tokens * 4}
    if OPENAI_REASONING_EFFORT:
        p["reasoning_effort"] = OPENAI_REASONING_EFFORT
    return p


async def _openai_chat(client, messages: list, max_tokens: int):
    """
    Einheitlicher OpenAI-Aufruf, unabhängig vom eingestellten Modell
    (OPENAI_MODEL). Passt die Parameter automatisch an die Modellfamilie an
    und versucht es EINMAL mit der jeweils anderen Parameter-Variante, falls
    das Modell die gewählte mit "unsupported/max_tokens/temperature" ablehnt –
    so überlebt die App einen Modellwechsel (z.B. wegen Abschaltung der
    GPT-4-Familie), ohne dass jeder Beleg-Upload mit HTTP 400 scheitert.
    """
    klassisch = _modell_ist_klassisch()
    resp = None
    for versuch in range(2):
        resp = await client.post(
            OPENAI_URL,
            headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
            json={"model": OPENAI_MODEL, "messages": messages,
                  **_modell_params(max_tokens, klassisch)})
        if resp.status_code != 400 or versuch == 1:
            return resp
        txt = resp.text.lower()
        if any(k in txt for k in ("max_tokens", "max_completion_tokens", "temperature",
                                   "unsupported", "reasoning_effort")):
            klassisch = not klassisch  # andere Parameter-Variante probieren
        else:
            return resp
    return resp
S3_ENDPOINT  = os.getenv("S3_ENDPOINT", "")
S3_BUCKET    = os.getenv("S3_BUCKET", "")
S3_ACCESS_KEY= os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY= os.getenv("S3_SECRET_KEY", "")
IMAP_USER    = os.getenv("IMAP_USER", "")

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_belegdeckung import auto_verknuepfen
from mod_anon import anonymisieren
from mod_geo import adresse_geokodieren
from mod_vma import IATA_KOORDINATEN
from mod_vma_tage import vma_auto_aktualisieren, vma_auto_fuer_beleg
import mod_a1


def _segment_koordinaten_praezisieren(ki_result: dict) -> None:
    """
    Ersetzt bei jedem Flug-/Bahnsegment die von der KI geschätzten
    von_lat/von_lon/nach_lat/nach_lon durch die EXAKTEN Koordinaten aus der
    öffentlichen Flughafendatenbank (IATA_KOORDINATEN, ~9.000 Flughäfen),
    sofern ein IATA-Code bekannt ist. Eine KI-Schätzung "aus dem Kopf" kann
    auch bei bekannten Flughäfen mehrere Kilometer daneben liegen – das führt
    auf der Karte dazu, dass der Marker nicht exakt am Flughafen sitzt.
    Bahnhöfe (kein IATA-Code) bleiben unverändert bei der KI-Schätzung, dafür
    gibt es keine vergleichbare öffentliche Datenbank in dieser App.
    Ändert ki_result in-place, gibt nichts zurück.
    """
    for seg in (ki_result.get("segmente") or []):
        for praefix in ("von", "nach"):
            iata = (seg.get(f"{praefix}_iata") or "").strip().upper()
            koord = IATA_KOORDINATEN.get(iata) if iata else None
            if koord:
                seg[f"{praefix}_lat"], seg[f"{praefix}_lon"] = koord
from mod_vma import IATA_KOORDINATEN

_s3_cache: dict = {}


def get_s3():
    """S3/Hetzner Object Storage Client. Wird wiederverwendet (der Neuaufbau des Clients kostet jedes Mal
    einige hundert Millisekunden; boto3-Clients sind für den gemeinsamen Gebrauch durch mehrere Threads gedacht)."""
    import boto3
    schluessel = (S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY)
    client = _s3_cache.get(schluessel)
    if client is None:
        client = boto3.client("s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY)
        _s3_cache.clear()
        _s3_cache[schluessel] = client
    return client

def s3_upload(key: str, data: bytes, content_type: str = "application/pdf") -> str:
    """Lädt Datei zu S3 hoch. Gibt Key zurück."""
    s3 = get_s3()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)
    return key

def s3_download(key: str) -> bytes:
    """Lädt Datei von S3 herunter."""
    s3 = get_s3()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read()

BILD_TYPEN = ("image/jpeg", "image/jpg", "image/png", "image/heic", "image/heif", "image/webp", "image/tiff")
MAX_BILD_SEITEN = 20     # mehrseitige TIF-Scans: höchstens so viele Seiten ins PDF


def inhaltstyp_bestimmen(datei_bytes: bytes, dateiname: str, content_type: str) -> str:
    """TIFF erkennt man an den ersten Bytes (II*\\0 oder MM\\0*). Browser und Mail-Programme melden
    dafür oft nur "application/octet-stream" – sonst würde das Bild als Text verarbeitet."""
    if datei_bytes[:4] in (b"II*\x00", b"MM\x00*"):
        return "image/tiff"
    return content_type


def bild_zu_pdf(bild_bytes: bytes, dateiname: str = "bild") -> bytes:
    """Konvertiert JPG/PNG/TIF zu PDF mit Pillow + ReportLab. Mehrseitige TIF-Scans: eine PDF-Seite je Bildseite."""
    from PIL import Image, ImageOps
    from reportlab.lib.units import mm
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    quelle = Image.open(io.BytesIO(bild_bytes))
    seiten = min(getattr(quelle, "n_frames", 1) or 1, MAX_BILD_SEITEN)
    pdf_buf = io.BytesIO()
    w_pt, h_pt = A4
    rand = 20 * mm
    c_pdf = canvas.Canvas(pdf_buf, pagesize=A4)
    for nr in range(seiten):
        if seiten > 1:
            quelle.seek(nr)
        img = quelle.copy()
        # EXIF-Rotation korrigieren
        try:
            img = ImageOps.exif_transpose(img)
        except Exception:
            pass
        # Transparenz -> weißer Hintergrund, sonst zu RGB konvertieren
        if img.mode in ("RGBA", "LA"):
            hintergrund = Image.new("RGB", img.size, (255, 255, 255))
            hintergrund.paste(img, mask=img.split()[-1])
            img = hintergrund
        elif img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        # Bild als JPEG in Buffer speichern
        img_buf = io.BytesIO()
        img.save(img_buf, format="JPEG", quality=95)
        img_buf.seek(0)
        # Skalieren auf A4 mit Rand
        img_w, img_h = img.size
        scale = min((w_pt - 2 * rand) / img_w, (h_pt - 2 * rand) / img_h)
        draw_w, draw_h = img_w * scale, img_h * scale
        c_pdf.drawImage(ImageReader(img_buf), (w_pt - draw_w) / 2, (h_pt - draw_h) / 2, draw_w, draw_h)
        c_pdf.showPage()
    c_pdf.save()
    return pdf_buf.getvalue()

def text_zu_pdf(text: str, titel: str = "Dokument") -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=25*mm, rightMargin=25*mm,
        topMargin=25*mm, bottomMargin=25*mm)
    styles = getSampleStyleSheet()
    def esc(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    story = [Paragraph(esc(titel), styles["Heading1"]), Spacer(1, 6*mm)]
    for line in text.splitlines():
        line = line.strip()
        if line:
            story.append(Paragraph(esc(line), styles["Normal"]))
        else:
            story.append(Spacer(1, 3*mm))
    doc.build(story)
    return buf.getvalue()

def pruefkopf_pdf_erzeugen(reise_code: str, geprueft_von: str, geprueft_am, pruef_vermerk: str,
                            anbieter: str, betrag: float, waehrung: str, belegdatum) -> bytes:
    """Erstellt ein Deckblatt (1 Seite) mit den Prüf-Angaben, das dem Original-
    Beleg vorangestellt wird, bevor er an Habel übertragen wird."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=25*mm, rightMargin=25*mm, topMargin=25*mm, bottomMargin=25*mm)
    styles = getSampleStyleSheet()
    def esc(s): return (s or "–").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    story = [
        Paragraph("Prüfvermerk – Reisekostenbeleg", styles["Heading1"]),
        Spacer(1, 8*mm),
    ]
    # Bemerkung als Absatz, damit lange Texte (z. B. Verweis auf einen Bewirtungsbeleg) umbrechen
    vermerk_absatz = Paragraph(esc(pruef_vermerk).replace("\n", "<br/>"), styles["Normal"])
    daten = [
        ["Geprüft am / durch:", f"{fmt_date(geprueft_am)} · {geprueft_von or '–'}"],
        ["Bemerkung:", vermerk_absatz],
        ["Reise-ID:", reise_code or "–"],
        ["Anbieter:", anbieter or "–"],
        ["Betrag:", f"{betrag:.2f} {waehrung}" if betrag else "–"],
        ["Belegdatum:", fmt_date(belegdatum)],
    ]
    tbl = Table(daten, colWidths=[45*mm, 110*mm])
    tbl.setStyle(TableStyle([
        ("FONTNAME", (0,0), (0,-1), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ("TOPPADDING", (0,0), (-1,-1), 6),
        ("LINEBELOW", (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
        ("TEXTCOLOR", (0,0), (0,-1), colors.HexColor("#374151")),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 10*mm))
    story.append(Paragraph("Es folgt der geprüfte Originalbeleg.", styles["Italic"]))
    doc.build(story)
    return buf.getvalue()


def beleg_mit_pruefkopf(original_pdf_bytes: bytes, deckblatt_pdf_bytes: bytes) -> bytes:
    """Fügt Deckblatt + Original-PDF zu einem gemeinsamen PDF zusammen."""
    import pypdf
    writer = pypdf.PdfWriter()
    for seite in pypdf.PdfReader(io.BytesIO(deckblatt_pdf_bytes)).pages:
        writer.add_page(seite)
    for seite in pypdf.PdfReader(io.BytesIO(original_pdf_bytes)).pages:
        writer.add_page(seite)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def pdf_text_lesen(pdf_bytes: bytes) -> str:
    """Liest Text aus PDF mit pypdf."""
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join(p.extract_text() or "" for p in reader.pages).strip()
    except: return ""


MWST_NIE_BERECHNEN = ("Flug", "Bahn")   # Fahrpreise: MwSt. nur übernehmen, wenn aufgedruckt


def _zahlen_im_text(text: str) -> list:
    """Alle Beträge mit zwei Nachkommastellen im Text (deutsche und englische Schreibweise)."""
    erg = []
    for m in re.finditer(r"\d{1,3}(?:\.\d{3})*,\d{2}(?!\d)|\d{1,3}(?:,\d{3})*\.\d{2}(?!\d)|\d+[.,]\d{2}(?!\d)", text or ""):
        z = m.group(0).replace(" ", "")
        if "," in z and "." in z:
            dez = "," if z.rfind(",") > z.rfind(".") else "."
            tausend = "." if dez == "," else ","
            z = z.replace(tausend, "").replace(dez, ".")
        else:
            z = z.replace(",", ".")
        try:
            erg.append(float(z))
        except ValueError:
            pass
    return erg


def mwst_aufgedruckt(rohtext: str, betrag) -> bool:
    """Steht dieser MwSt.-Betrag so im Belegtext? Wenn nicht, wurde er hochgerechnet."""
    try:
        b = float(betrag)
    except (TypeError, ValueError):
        return False
    return any(abs(z - b) <= 0.005 + 1e-9 for z in _zahlen_im_text(rohtext))


def mwst_pruefen(result: dict, rohtext: str | None = None) -> dict:
    """Bei Flug und Bahn zählt nur eine aufgedruckte MwSt. (Fall Reisebüro: 19 % gelten nur für die
    Servicepauschale, nicht für den Flugpreis). Hat die KI sie selbst berechnet oder steht der Betrag nicht
    im Belegtext, werden Netto und MwSt. geleert."""
    if (result.get("transportart") or "") not in MWST_NIE_BERECHNEN:
        return result
    try:
        mwst = float(result.get("betrag_mwst")) if result.get("betrag_mwst") is not None else None
    except (TypeError, ValueError):
        mwst = None
    if mwst is None:
        return result
    flag = result.get("mwst_berechnet")
    berechnet = flag is True or str(flag).strip().lower() == "true"
    if rohtext and rohtext.strip() and not mwst_aufgedruckt(rohtext, mwst):
        berechnet = True
    if berechnet:
        result["betrag_netto"] = None
        result["betrag_mwst"] = None
    return result


def mwst_flug_bahn_reparieren() -> int:
    """Bereinigt ältere Belege: Bei Flug/Bahn hat die KI die MwSt. früher aus dem Bruttobetrag hochgerechnet
    (19 % oder 7 %). Geleert wird, wenn der MwSt.-Betrag nirgends im Belegtext steht (also nicht aufgedruckt
    war). Foto-Belege ohne echten Text bleiben unberührt. Läuft bei jedem Start und ändert nur solche Belege
    (idempotent). Gibt die Anzahl zurück."""
    P = ph()
    db = get_db(); cur = db.cursor()
    n = 0
    try:
        cur.execute("SELECT id, betrag_mwst, rohtext FROM belege "
                    "WHERE transportart IN ('Flug','Bahn') AND betrag_mwst IS NOT NULL AND rohtext IS NOT NULL")
        for r in cur.fetchall():
            g = (lambda k, i: r[k]) if hasattr(r, "keys") else (lambda k, i: r[i])
            text = g("rohtext", 2) or ""
            if not text.strip() or text.strip().startswith("{"):      # Bild-Belege: kein echter Belegtext
                continue
            if not mwst_aufgedruckt(text, float(g("betrag_mwst", 1))):
                cur.execute(f"UPDATE belege SET betrag_netto=NULL, betrag_mwst=NULL WHERE id={P}", (g("id", 0),))
                n += 1
        db.commit()
    finally:
        cur.close(); db.close()
    return n


def belegart_normalisieren(result: dict) -> dict:
    """Auffangregel nach der KI-Analyse: Ein Beleg mit Betrag, der weder Buchung noch Änderung ist,
    ist ein Zahlungsbeleg. Fehlt die Belegart oder steht dort nur „Sonstiges“, wird „Quittung“ gesetzt,
    sonst wäre der Beleg nicht prüfbar. Eine manuelle Wahl im Beleg bleibt davon unberührt."""
    art = (result.get("belegart") or "").strip()
    kurz = art.lower()
    if kurz in ("receipt", "kassenbon", "beleg", "zahlungsbeleg", "kartenbeleg"):
        art = "Quittung"
    elif kurz in ("invoice", "tax invoice"):
        art = "Rechnung"
    try:
        hat_betrag = float(result.get("betrag_brutto") or 0) > 0
    except (TypeError, ValueError):
        hat_betrag = False
    if art in ("", "Sonstiges") and hat_betrag:
        art = "Quittung"
    result["belegart"] = art or None
    return result


MAX_KI_SEITEN = 4   # so viele Seiten eines Scans liest die KI (Steuertabellen stehen oft auf der letzten Seite)


def _bild_optimieren(bild_bytes: bytes, content_type: str) -> tuple:
    """Bild für die Bilderkennung vorbereiten: drehen, Transparenz weiß, Mindestgröße, JPEG. Gibt (Bytes, Typ) zurück."""
    try:
        from PIL import Image, ImageOps
        img = Image.open(io.BytesIO(bild_bytes))
        try: img = ImageOps.exif_transpose(img)
        except: pass
        # RGBA (PNG mit Transparenz) → weißer Hintergrund
        if img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            img = bg
        elif img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        # Mindestgröße 1000px auf längster Seite für gute OCR-Qualität
        w, h = img.size
        min_px = 1000
        if max(w, h) < min_px:
            scale = min_px / max(w, h)
            img = img.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        return buf.getvalue(), "image/jpeg"
    except Exception as img_e:
        print(f"[Bild-Optimierung] {img_e}")
        if content_type in ("image/heic", "image/heif"):
            content_type = "image/jpeg"
        return bild_bytes, content_type


VEKTOR_SEITE_BYTES = 20000   # Seiten mit mehr Zeichenbefehlen als so vielen Bytes gelten als „gezeichnet“, nicht als Scan
SEITE_DPI = 150              # Auflösung beim Umwandeln einer gezeichneten Seite in ein Bild (A4 = ca. 1240 x 1754 Pixel)


def _seite_ist_gezeichnet(seite) -> bool:
    """True bei Seiten, deren Inhalt aus Linien/Schrift-Umrissen besteht (z. B. „Microsoft: Print To PDF“ ohne Textebene).
    Ein Scan oder ein aus einem Foto erzeugtes PDF besteht dagegen nur aus einem Bild und hat einen winzigen Inhaltsstrom."""
    try:
        inhalt = seite.get_contents()
        return inhalt is not None and len(inhalt.get_data()) > VEKTOR_SEITE_BYTES
    except Exception:
        return False


def _seite_rendern(pdf_bytes: bytes, seiten_nr: int, dpi: int = SEITE_DPI):
    """Die ganze Seite als JPEG-Bild (Bytes) oder None. Zuerst pypdfium2 (steht in requirements.txt),
    ersatzweise das Programm pdftoppm, falls es auf dem Rechner installiert ist."""
    try:
        import pypdfium2 as pdfium
        pdf = pdfium.PdfDocument(pdf_bytes)
        try:
            bild = pdf[seiten_nr].render(scale=dpi / 72).to_pil().convert("RGB")
        finally:
            pdf.close()
        buf = io.BytesIO()
        bild.save(buf, format="JPEG", quality=90)
        return buf.getvalue()
    except Exception as e:
        print(f"[Seitenbilder] pypdfium2: {e}")
    try:
        import shutil, subprocess, tempfile, os
        if not shutil.which("pdftoppm"):
            return None
        with tempfile.TemporaryDirectory() as tmp:
            pfad = os.path.join(tmp, "in.pdf")
            with open(pfad, "wb") as f:
                f.write(pdf_bytes)
            subprocess.run(["pdftoppm", "-jpeg", "-r", str(dpi), "-f", str(seiten_nr + 1), "-l", str(seiten_nr + 1),
                            pfad, os.path.join(tmp, "s")], check=True, timeout=60)
            for name in sorted(os.listdir(tmp)):
                if name.startswith("s") and name.endswith(".jpg"):
                    with open(os.path.join(tmp, name), "rb") as f:
                        return f.read()
    except Exception as e:
        print(f"[Seitenbilder] pdftoppm: {e}")
    return None


def pdf_titel_lesen(pdf_bytes: bytes) -> str:
    """Der Titel in den PDF-Eigenschaften (z. B. „Bescheinigung A1 - Onlineversion“), sonst leer."""
    try:
        from pypdf import PdfReader
        t = (PdfReader(io.BytesIO(pdf_bytes)).metadata or {}).get("/Title")
        return str(t or "").strip()
    except Exception:
        return ""


def pdf_seitenbilder(pdf_bytes: bytes, max_seiten: int = MAX_KI_SEITEN) -> list:
    """Je PDF-Seite ein Bild für die KI, höchstens max_seiten Stück:
    - Scan / aus Foto erzeugtes PDF: das eingebettete Bild in voller Auflösung,
    - gezeichnete Seite (Formular als „Print To PDF“ ohne Textebene, nur Schrift-Umrisse): die Seite wird als Bild
      gerendert. Früher wurde hier nur das eingebettete Bild gelesen – bei solchen Seiten ein Kopfbalken oder gar nichts.
    Seiten ohne Bild und ohne Zeichnung werden übersprungen. Leere Liste = nichts zu lesen."""
    try:
        from pypdf import PdfReader
        erg = []
        for nr, seite in enumerate(PdfReader(io.BytesIO(pdf_bytes)).pages):
            if _seite_ist_gezeichnet(seite):
                bild = _seite_rendern(pdf_bytes, nr)
                if bild:
                    erg.append(bild)
                    if len(erg) >= max_seiten:
                        break
                    continue
            try:
                bilder = list(seite.images)
            except Exception as e:
                print(f"[Seitenbilder] Seite übersprungen: {e}")
                continue
            if bilder:
                erg.append(bilder[0].data)
            if len(erg) >= max_seiten:
                break
        return erg
    except Exception as e:
        print(f"[Seitenbilder] {e}")
        return []


async def gpt_analyse_bild(bild_bytes: bytes, content_type: str,
                            dateiname: str = "", weitere_seiten: list | None = None) -> dict:
    """
    Schickt ein Bild direkt an GPT-4o Vision für OCR + Analyse.
    Für Fotos von Belegen (Tankquittungen, Kassenzettel etc.) und Scans.
    weitere_seiten: Bilder (Bytes) der Folgeseiten desselben Belegs, in Reihenfolge.
    """
    if not OPENAI_KEY:
        return {"fehler": "OPENAI_API_KEY nicht gesetzt",
                "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["OPENAI_API_KEY fehlt"]}

    bild_bytes, content_type = _bild_optimieren(bild_bytes, content_type)
    b64 = base64.b64encode(bild_bytes).decode()
    folge_b64 = []
    for extra in (weitere_seiten or [])[:MAX_KI_SEITEN - 1]:
        eb, _ = _bild_optimieren(extra, "image/jpeg")
        folge_b64.append(base64.b64encode(eb).decode())

    heute_str = date.today().strftime("%d.%m.%Y")
    prompt = f"""Analysiere dieses Foto eines Reisebelegs und fülle ALLE erkennbaren Felder aus.
Antworte NUR mit einem validen JSON-Objekt – kein Text davor oder danach.
Nicht erkennbare Felder = null.

Heutiges Datum (zur Orientierung, falls auf dem Beleg KEIN Jahr angegeben ist,
z.B. nur "27 August, Donnerstag"): {heute_str}. Ermittle das Jahr in diesem Fall
über den Wochentag (welches Jahr passt zum genannten Wochentag+Datum, möglichst
nahe am heutigen Datum, i.d.R. nicht länger als 12 Monate in der Vergangenheit
oder Zukunft). Rate NIE ein Jahr ohne Anhaltspunkt – nutze immer den Wochentag,
falls dieser angegeben ist.

WICHTIG bei "betrag_eur_geschaetzt"/"kurs_geschaetzt": Ist "waehrung" NICHT "EUR",
schätze den Euro-Gegenwert von "betrag_brutto" anhand des dir bekannten
ungefähren Wechselkurses zum Datum von "belegdatum" (historischer Tageskurs,
aus deinem eigenen Wissen). Bei "waehrung"="EUR" beide Felder null lassen.

WICHTIG bei "betrag_netto"/"betrag_mwst":
1. Grundregel: Übernimm Netto- und MwSt.-Betrag NUR so, wie sie auf dem Beleg ausgewiesen
   sind, und setze dann "mwst_berechnet" = false. Rechne NIE einen Steuersatz auf den
   Gesamtbetrag hoch, wenn auf dem Beleg nur ein Teil davon mit MwSt. ausgewiesen ist
   (Beispiel Reisebüro: Die 19 % MwSt. gelten nur für die Servicepauschale, nicht für den
   Flugpreis. Richtig ist dann genau der aufgedruckte MwSt.-Betrag).
2. Selbst berechnen ("mwst_berechnet" = true) darfst du NUR bei deutschen Belegen
   ("land_beleg"="DE") ohne MwSt.-Ausweis (typisch bei ÖPNV-Fahrscheinen, Parkscheinen,
   Kleinbeträgen, die "inkl. MwSt." tragen) und nur nach deutschem Steuerrecht:
   - Ermäßigter Satz 7%: Nahverkehr/ÖPNV (Bus, Bahn, Tram im Personennahverkehr,
     Fahrstrecke bis 50km bzw. Gemeindegebiet), Bücher, Zeitungen, die meisten
     Lebensmittel, reine Hotelübernachtung (ohne Frühstück/Extras)
   - Regulärer Satz 19%: Kraftstoff, Parkschein, Restaurant/Bewirtung vor Ort,
     die meisten sonstigen Waren/Dienstleistungen
   Rechnung: betrag_netto = betrag_brutto / (1 + Satz), betrag_mwst = betrag_brutto - betrag_netto,
   beide auf 2 Nachkommastellen gerundet.
3. NIEMALS selbst berechnen (beide Felder null, wenn nicht ausgewiesen) bei: Flugtickets
   (Auslandsflüge tragen keine deutsche MwSt.; auch bei Inlandsflügen nichts schätzen),
   Bahn-Fernverkehr, Fernbus, Schiff/Fähre, Rechnungen von Reisebüros/Vermittlern (der
   Fahrpreis ist ein durchlaufender Posten), Visa- und Behördengebühren.
4. Bei Belegen aus anderen Ländern ("land_beleg" != "DE") beide Felder null lassen, wenn
   nicht explizit ausgewiesen – ausländische MwSt.-Sätze/-Regeln nicht selbst schätzen.
5. Gibt es mehrere Steuersätze (z. B. 0 % "auf fremde Rechnung" und 19 % "auf eigene Rechnung"
   wie bei Reisebüros), ist "betrag_mwst" die SUMME der ausgewiesenen Steuerbeträge (Spalte
   "Steuer") und "betrag_netto" = "betrag_brutto" minus "betrag_mwst". Eine Tabelle
   "Enthaltene Mehrwertsteuer" steht oft auf der letzten Seite.

WICHTIG bei "belegart" (niemals leer lassen):
- "Quittung" = jeder Zahlungsbeleg über einen bezahlten Betrag: Kassenbon, Kartenbeleg,
  Taxi-/Uber-/Bolt-Beleg, Parkschein, Mautbeleg, Tankquittung, auch Stornogebühren und
  sonstige Gebühren.
- "Rechnung" = Dokument, das ausdrücklich Rechnung/Invoice/Tax Invoice heißt oder eine
  Rechnungsnummer samt Rechnungsanschrift trägt.
- "A1Bescheinigung" = A1-Bescheinigung ("Bescheinigung über die anzuwendenden Rechtsvorschriften",
  Portable Document A1, Entsendung ins EU-/EWR-Ausland, Schweiz, Vereinigtes Königreich). Dann ist
  "reisender" der Name der Person, "event_datum_von"/"event_datum_bis" der Gültigkeitszeitraum,
  "belegdatum" das Ausstellungsdatum, "anbieter" die ausstellende Stelle (Krankenkasse oder
  Rentenversicherung); es gibt keinen Betrag. "transportart" bleibt leer. Eine Rechnung des
  Telekommunikationsanbieters "A1 Telekom" ist KEINE A1-Bescheinigung.
  Die amtliche deutsche Online-Fassung erkennst du am grünen Kopf "A1" mit EU-Flagge, dem Titel "Bescheinigung über
  die Rechtsvorschriften der sozialen Sicherheit, die auf den/die Inhaber/in anzuwenden sind", "Verordnungen (EG)
  Nr. 883/2004 und Nr. 987/2009", 3 Seiten und nummerierten Feldern (1.1 bis 6.11). Dafür gilt genau:
  "reisender" = Feld 1.3 Vorname(n) + Feld 1.2 Nachname (Vorname zuerst; NICHT der Geburtsname 1.4),
  "event_datum_von" = Feld 2.2 Anfangsdatum, "event_datum_bis" = Feld 2.3 Enddatum (leer, wenn keines steht),
  "belegdatum" = Feld 6.10 Datum (Ausstellungsdatum, NICHT das Geburtsdatum 1.5),
  "anbieter" = Feld 6.1 Name des ausstellenden Trägers (z. B. Techniker Krankenkasse, AOK Bayern),
  "a1_laender" = Liste der ISO-Ländercodes des Staates, in dem gearbeitet wird: Land am Ende der Anschrift unter
  Feld 5.2 (z. B. "Bladkowo 1c, 72210 Dobra, PL" -> ["PL"]), ersatzweise Feld 1.9.4 Ländercode; "land_beleg" = derselbe Code.
  NICHT verwenden: Feld 2.1 Mitgliedstaat (das ist der Staat, dessen Recht gilt, bei uns immer DE) und die
  Ländercodes 1.8.4, 4.4.2, 6.5 (alle in Deutschland). Die Fußnoten (Spanien, Schweden, Portugal) sind keine Reiseländer.
- "Sonstiges" NUR, wenn das Dokument keinen bezahlten Betrag enthält und weder Buchung
  noch Änderung ist.

Pflichtfelder: belegdatum, transportart, anbieter, betrag_brutto, waehrung, event_datum_von
Setze pflichtfelder_ok=false wenn ein Pflichtfeld fehlt.

""" + """{
  "belegdatum": "DD.MM.YYYY",
  "belegart": "Rechnung|Quittung|Flugaenderung|A1Bescheinigung|Sonstiges",
  "transportart": "Hotel|Flug|Bahn|Mietwagen|Taxi|Tanken|Verpflegung|Bewirtung|Sonstiges",
  "transportart_freitext": "nur wenn Sonstiges",
  "anbieter": "Name des Anbieters",
  "rechnungsnummer": "Belegnummer",
  "buchungscode": null,
  "reisender": null,
  "land_beleg": "DE|FR|US|...",
  "betrag_brutto": 45.30,
  "betrag_netto": null,
  "betrag_mwst": null,
  "mwst_berechnet": "true nur wenn du Netto/MwSt. selbst berechnet hast, false wenn sie auf dem Beleg stehen, sonst null",
  "waehrung": "EUR",
  "betrag_eur_geschaetzt": "nur bei waehrung != EUR: geschätzter Euro-Gegenwert zum Kurs des Belegdatums, sonst null",
  "kurs_geschaetzt": "nur bei waehrung != EUR: verwendeter Wechselkurs (1 EUR = X Fremdwährung), sonst null",
  "zahlungsart": "Kreditkarte|Bar|Ueberweisung|PayPal|DurchKunde|Unbekannt (DurchKunde nur wenn explizit vermerkt, dass ein Kunde die Kosten direkt selbst uebernommen/bezahlt hat, z.B. 'vom Kunden beglichen'; PayPal erkennbar an 'PayPal' im Text, z.B. 'Diese Zahlung wurde ueber PayPal getaetigt' – PayPal gilt als eigene Kategorie, auch wenn im Hintergrund eine Kreditkarte hinterlegt ist; sonst Kreditkarte erkennbar an Visa/Mastercard/Amex, maskierter Kartennummer wie xxxx1234, 'bar bezahlt'/cash, oder Ueberweisungshinweisen)",
  "event_datum_von": "DD.MM.YYYY (bei Parkplatzreservierung am Flughafen: EINSTELLDATUM)",
  "event_datum_bis": "DD.MM.YYYY (bei Parkplatzreservierung am Flughafen: ABHOLDATUM – WICHTIG: immer ausfuellen, auch wenn nur ein Zeitraum wie '20.09.-28.09.' auf dem Beleg steht, NICHT nur das erste Datum uebernehmen)",
  "event_zeit": "HH:MM (Uhrzeit auf dem Beleg, z.B. bei Tankquittung, Parkschein, Mautbeleg)",
  "event_ort_von": "Ort",
  "event_ort_bis": null,
  "tanken_kraftstoff": "Benzin|Diesel|AdBlue|Super|SuperPlus|Elektro",
  "tanken_menge": 45.3,
  "tanken_einheit": "Liter|kWh",
  "tanken_preis_pro_einheit": 1.789,
  "tanken_tankstelle": "Name und Ort",
  "tanken_kennzeichen": "Kennzeichen falls sichtbar",
  "a1_laender": "nur bei A1Bescheinigung: Liste der ISO-Ländercodes des Arbeitsortes, z.B. [\"PL\"], sonst null",
  "pflichtfelder_ok": true,
  "fehlende_pflichtfelder": []
}"""

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            seiten_text = ""
            if folge_b64:
                seiten_text = (f"\n\nDas sind {1 + len(folge_b64)} Seiten DESSELBEN Belegs in Reihenfolge. "
                               "Lies alle Seiten: Summen, Steuertabellen (z. B. 'Enthaltene Mehrwertsteuer') und "
                               "Zahlungsangaben stehen oft auf der letzten Seite.")
            inhalt = [{"type": "text", "text": prompt + seiten_text},
                      {"type": "image_url", "image_url": {"url": f"data:{content_type};base64,{b64}", "detail": "high"}}]
            for fb in folge_b64:
                inhalt.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{fb}", "detail": "high"}})
            resp = await _openai_chat(client, [{"role": "user", "content": inhalt}], 3000)

            if resp.status_code != 200:
                return {"fehler": f"HTTP {resp.status_code}: {resp.text[:200]}",
                        "pflichtfelder_ok": False,
                        "fehlende_pflichtfelder": [f"HTTP {resp.status_code}: {resp.text[:200]}"]}

            raw = resp.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                result = mwst_pruefen(belegart_normalisieren(json.loads(m.group(0))))
                pflicht = ["belegdatum","transportart","anbieter",
                           "betrag_brutto","waehrung","event_datum_von","zahlungsart"]
                if result.get("belegart") == "Buchungsbestaetigung":
                    pflicht = [f for f in pflicht if f not in ("betrag_brutto","waehrung","zahlungsart")]
                if result.get("belegart") == "A1Bescheinigung":
                    pflicht = []          # A1-Bescheinigung: kein Betrag, keine Pflichtfelder der Kostenbelege
                fehlend = [f for f in pflicht if not result.get(f)]
                result["pflichtfelder_ok"] = len(fehlend) == 0
                result["fehlende_pflichtfelder"] = fehlend
                return result
            return {"fehler": "Kein JSON", "pflichtfelder_ok": False,
                    "fehlende_pflichtfelder": [f"Kein JSON: {raw[:150]}"]}
    except Exception as e:
        import traceback
        return {"fehler": str(e), "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["Exception: " + str(e)[:80]]}


async def gpt_analyse(rohtext: str, dateiname: str = "") -> dict:
    """
    Sendet den extrahierten Text an GPT-4o zur Analyse.
    Kein PDF, kein Base64 – einfach Text. Schnell und zuverlässig.
    """
    if not OPENAI_KEY:
        return {"fehler": "OPENAI_API_KEY nicht gesetzt",
                "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["OPENAI_API_KEY fehlt"]}

    if not rohtext or len(rohtext.strip()) < 10:
        return {"fehler": "Kein Text vorhanden",
                "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["Kein Text"]}

    heute_str = date.today().strftime("%d.%m.%Y")
    prompt = f"""Analysiere diesen Reisebeleg und fülle ALLE erkennbaren Felder aus.
Antworte NUR mit einem validen JSON-Objekt – kein Text davor oder danach.
Nicht erkennbare Felder = null.

Heutiges Datum (zur Orientierung, falls auf dem Beleg KEIN Jahr angegeben ist,
z.B. bei manchen Booking.com-Bestätigungen nur "27 August, Donnerstag"): {heute_str}.
Ermittle das Jahr in diesem Fall über den genannten Wochentag (welches Jahr passt
zum Wochentag+Datum, möglichst nahe am heutigen Datum, i.d.R. nicht länger als
12 Monate in Vergangenheit/Zukunft). Rate NIE ein Jahr ohne Anhaltspunkt – nutze
immer den Wochentag, falls dieser angegeben ist.

WICHTIG bei "segmente": Erfasse WIRKLICH JEDE einzelne Teilstrecke als eigenes
Segment – Hinflug UND Rückflug, alle Zwischenstopps/Umstiege, auch kombinierte
Bahn+Flug-Reisen (z.B. Zubringerzug zum Flughafen zählt als eigenes Segment).
Ein Ticket mit z.B. 4 Teilstrecken (Zug, Flug, Flug, Zug) muss auch 4 Einträge
in "segmente" ergeben, nicht nur den ersten.

WICHTIG bei DB-Fahrkarten/Online-Tickets (Deutsche Bahn): Diese enthalten
typischerweise ZWEI Darstellungen derselben Reise – eine kurze "Zugbindung"-
Aufzählung (nur Zugname + Abfahrtszeit) UND eine detaillierte Tabelle
"Ihre Reiseverbindung und Reservierung" mit den Spalten Halt/Datum/Zeit/Gleis/
Produkte. NUTZE die detaillierte Tabelle als Hauptquelle, nicht nur die kurze
Aufzählung: Dort steht JEDE Teilstrecke als ZWEI aufeinanderfolgende Zeilen –
die erste Zeile ist der Start-Halt mit "ab"-Zeit, die zweite Zeile ist der
Ziel-Halt mit "an"-Zeit, beide gehören zu demselben Segment (erkennbar am
gemeinsamen Zugnamen in der Spalte "Produkte", der nur bei der ersten Zeile
steht). Erfasse GENAU SO VIELE Segmente, wie es solche Start/Ziel-Zeilenpaare
gibt (Hinfahrt UND Rückfahrt zusammengezählt) – nicht nur die Anzahl der in
der kurzen Zugbindungs-Liste genannten Zugnamen, falls diese von der
detaillierten Tabelle abweicht. "von_ort"/"nach_ort" sind die jeweiligen
Halt-Namen (z.B. "Würzburg Hbf", "Fulda"), "abreise_zeit" die "ab"-Zeit der
ersten Zeile, "ankunft_zeit" die "an"-Zeit der zweiten Zeile.

WICHTIG bei Flugänderungs-/Umbuchungsdokumenten: Airlines verschicken bei
Flugplanänderungen, Umbuchungen oder Ersatzflügen oft ein SEPARATES Dokument
zur ursprünglichen Buchung – erkennbar an Formulierungen wie "Ihre Flugzeiten
haben sich geändert", "Schedule Change", "Your flight has been rescheduled",
"Flugplanänderung", "Ersatzverbindung", "Umbuchungsbestätigung", "Involuntary
Rebooking" oder ähnlich, OFT OHNE dass das Wort "Rechnung" oder "Buchung"
überhaupt vorkommt. Erkennst du so ein Dokument:
- "belegart" = "Flugaenderung" setzen (NICHT "Rechnung" oder "Sonstiges").
- "segmente" trotzdem ganz normal mit den NEUEN (geänderten) Flugdaten füllen
  – das ist oft die einzige Stelle, an der die aktuell gültigen Zeiten/
  Flugnummern stehen, nicht nur ein Vermerk ohne Nutzdaten.
- Steht auf dem Dokument sowohl die ALTE als auch die NEUE Verbindung
  (manche Airlines zeigen beides zum Vergleich), nur die NEUE als "segmente"
  übernehmen, die alte NICHT – sonst entstehen doppelte/falsche Einträge.
- "notiz"-artige Zusatzinfos (sofern ein solches Feld im Schema vorhanden
  ist) nutzen, um kurz zu vermerken "Flugänderung zu Beleg-Buchungscode X",
  falls der ursprüngliche Buchungscode auf dem Dokument erkennbar ist – hilft
  später beim Zuordnen zur Originalbuchung.

WICHTIG bei "von_iata"/"nach_iata": Fülle diese Felder IMMER, auch wenn auf dem
Beleg nur der Stadt-/Flughafenname steht (z.B. "Mexico City", nicht "MEX") –
leite den 3-stelligen IATA-Code dann aus deinem eigenen Wissen über den
Hauptflughafen dieser Stadt ab (z.B. "Mexico City" -> "MEX", "Frankfurt" ->
"FRA"). Nur wenn wirklich keine sinnvolle Zuordnung möglich ist, null setzen.
Bei Bahnhöfen (kein Flughafen) "von_iata"/"nach_iata" auf null lassen.

WICHTIG bei "von_lat"/"von_lon"/"nach_lat"/"nach_lon": Gib IMMER die
ungefähren geografischen Koordinaten (Dezimalgrad) des jeweiligen Flughafens/
Bahnhofs aus deinem eigenen Wissen an, unabhängig davon, ob sie auf dem Beleg
stehen (stehen dort normalerweise nicht). Genauigkeit auf 2-3 Nachkommastellen
reicht. Nur bei völlig unbekanntem/nicht zuordenbarem Ort null setzen.

WICHTIG bei "hotel_lat"/"hotel_lon": Genau wie bei den Flug-/Bahn-Koordinaten
IMMER die ungefähren geografischen Koordinaten des Hotels/der Hotelstadt aus
deinem eigenen Wissen angeben – auch wenn nur der Hotelname und die Stadt
bekannt sind, nicht die exakte Adresse. Das ist wichtig für die korrekte
Positionsanzeige auf der Karte: ohne diese Koordinaten würde sonst nur ein
grober Länder-Mittelpunkt verwendet (z.B. Rom als Rückfall für "Italien",
selbst wenn das Hotel tatsächlich in Turin liegt). Nur bei völlig
unbekanntem/nicht zuordenbarem Ort null setzen.

WICHTIG bei "ankunft_zeit"/"ankunft_datum": Steht die Ankunftszeit nicht
explizit auf dem Beleg, SCHÄTZE sie aus deinem eigenen Wissen über die
typische Reisedauer dieser Strecke (z.B. Kurzstreckenflug Frankfurt-Bukarest
~2 Std., Zug Nürnberg-Frankfurt ~2 Std.) und rechne sie zur Abreisezeit hinzu.
Setze ankunft_zeit NUR dann auf null, wenn wirklich überhaupt keine plausible
Schätzung möglich ist – lass es NIEMALS einfach leer, obwohl eine Schätzung
möglich wäre, und setze es NIE gleich der Abreisezeit oder auf 00:00, wenn
das nicht wirklich die Ankunftszeit ist.

WICHTIG bei "betrag_eur_geschaetzt"/"kurs_geschaetzt": Ist "waehrung" NICHT "EUR",
schätze den Euro-Gegenwert von "betrag_brutto" anhand des dir bekannten
ungefähren Wechselkurses zum Datum von "belegdatum" (historischer Tageskurs,
aus deinem eigenen Wissen – keine Live-Abfrage nötig). Das ist eine grobe
Arbeitsgrundlage, keine exakte Buchung; der Organisator kann sie später anhand
der echten Kreditkartenabrechnung überschreiben. Bei "waehrung"="EUR" beide
Felder null lassen.

WICHTIG bei "betrag_netto"/"betrag_mwst":
1. Grundregel: Übernimm Netto- und MwSt.-Betrag NUR so, wie sie auf dem Beleg ausgewiesen
   sind, und setze dann "mwst_berechnet" = false. Rechne NIE einen Steuersatz auf den
   Gesamtbetrag hoch, wenn auf dem Beleg nur ein Teil davon mit MwSt. ausgewiesen ist
   (Beispiel Reisebüro: Die 19 % MwSt. gelten nur für die Servicepauschale, nicht für den
   Flugpreis. Richtig ist dann genau der aufgedruckte MwSt.-Betrag).
2. Selbst berechnen ("mwst_berechnet" = true) darfst du NUR bei deutschen Belegen
   ("land_beleg"="DE") ohne MwSt.-Ausweis (typisch bei ÖPNV-Fahrscheinen, Parkscheinen,
   Kleinbeträgen, die "inkl. MwSt." tragen) und nur nach deutschem Steuerrecht:
   - Ermäßigter Satz 7%: Nahverkehr/ÖPNV (Bus, Bahn, Tram im Personennahverkehr,
     Fahrstrecke bis 50km bzw. Gemeindegebiet), Bücher, Zeitungen, die meisten
     Lebensmittel, reine Hotelübernachtung (ohne Frühstück/Extras)
   - Regulärer Satz 19%: Kraftstoff, Parkschein, Restaurant/Bewirtung vor Ort,
     die meisten sonstigen Waren/Dienstleistungen
   Rechnung: betrag_netto = betrag_brutto / (1 + Satz), betrag_mwst = betrag_brutto - betrag_netto,
   beide auf 2 Nachkommastellen gerundet.
3. NIEMALS selbst berechnen (beide Felder null, wenn nicht ausgewiesen) bei: Flugtickets
   (Auslandsflüge tragen keine deutsche MwSt.; auch bei Inlandsflügen nichts schätzen),
   Bahn-Fernverkehr, Fernbus, Schiff/Fähre, Rechnungen von Reisebüros/Vermittlern (der
   Fahrpreis ist ein durchlaufender Posten), Visa- und Behördengebühren.
4. Bei Belegen aus anderen Ländern ("land_beleg" != "DE") beide Felder null lassen, wenn
   nicht explizit ausgewiesen – ausländische MwSt.-Sätze/-Regeln nicht selbst schätzen.
5. Gibt es mehrere Steuersätze (z. B. 0 % "auf fremde Rechnung" und 19 % "auf eigene Rechnung"
   wie bei Reisebüros), ist "betrag_mwst" die SUMME der ausgewiesenen Steuerbeträge (Spalte
   "Steuer") und "betrag_netto" = "betrag_brutto" minus "betrag_mwst". Eine Tabelle
   "Enthaltene Mehrwertsteuer" steht oft auf der letzten Seite.

WICHTIG bei "abreise_utc_offset"/"ankunft_utc_offset": Zeiten auf Reisebelegen
stehen IMMER in der ORTSZEIT des jeweiligen Ortes – die Abreisezeit in der
Zeitzone des Abflugorts, die Ankunftszeit in der Zeitzone des Zielorts (diese
können unterschiedlich sein, z.B. Frankfurt MESZ +02:00, Bukarest EEST
+03:00). Gib IMMER den UTC-Offset an, der an diesem konkreten Datum an diesem
Ort gilt (inkl. Sommer-/Winterzeit, aus deinem eigenen Wissen abgeleitet),
im Format "+02:00"/"-05:00". Das ist wichtig für die korrekte Berechnung,
wann ein Flug/Zug tatsächlich ankommt.

WICHTIG bei "belegart" (niemals leer lassen):
- "Quittung" = jeder Zahlungsbeleg über einen bezahlten Betrag: Kassenbon, Kartenbeleg,
  Taxi-/Uber-/Bolt-Beleg, Parkschein, Mautbeleg, Tankquittung, auch Stornogebühren und
  sonstige Gebühren.
- "Rechnung" = Dokument, das ausdrücklich Rechnung/Invoice/Tax Invoice heißt oder eine
  Rechnungsnummer samt Rechnungsanschrift trägt.
- "Buchungsbestaetigung" = Vorab-Bestätigung einer Buchung (siehe oben), "Flugaenderung" = Änderungsdokument.
- "A1Bescheinigung" = A1-Bescheinigung ("Bescheinigung über die anzuwendenden Rechtsvorschriften",
  Portable Document A1, Entsendung ins EU-/EWR-Ausland, Schweiz, Vereinigtes Königreich). Dann ist
  "reisender" der Name der Person, "event_datum_von"/"event_datum_bis" der Gültigkeitszeitraum,
  "belegdatum" das Ausstellungsdatum, "anbieter" die ausstellende Stelle (Krankenkasse oder
  Rentenversicherung); es gibt keinen Betrag. "transportart" bleibt leer. Eine Rechnung des
  Telekommunikationsanbieters "A1 Telekom" ist KEINE A1-Bescheinigung.
  Die amtliche deutsche Online-Fassung erkennst du am grünen Kopf "A1" mit EU-Flagge, dem Titel "Bescheinigung über
  die Rechtsvorschriften der sozialen Sicherheit, die auf den/die Inhaber/in anzuwenden sind", "Verordnungen (EG)
  Nr. 883/2004 und Nr. 987/2009", 3 Seiten und nummerierten Feldern (1.1 bis 6.11). Dafür gilt genau:
  "reisender" = Feld 1.3 Vorname(n) + Feld 1.2 Nachname (Vorname zuerst; NICHT der Geburtsname 1.4),
  "event_datum_von" = Feld 2.2 Anfangsdatum, "event_datum_bis" = Feld 2.3 Enddatum (leer, wenn keines steht),
  "belegdatum" = Feld 6.10 Datum (Ausstellungsdatum, NICHT das Geburtsdatum 1.5),
  "anbieter" = Feld 6.1 Name des ausstellenden Trägers (z. B. Techniker Krankenkasse, AOK Bayern),
  "a1_laender" = Liste der ISO-Ländercodes des Staates, in dem gearbeitet wird: Land am Ende der Anschrift unter
  Feld 5.2 (z. B. "Bladkowo 1c, 72210 Dobra, PL" -> ["PL"]), ersatzweise Feld 1.9.4 Ländercode; "land_beleg" = derselbe Code.
  NICHT verwenden: Feld 2.1 Mitgliedstaat (das ist der Staat, dessen Recht gilt, bei uns immer DE) und die
  Ländercodes 1.8.4, 4.4.2, 6.5 (alle in Deutschland). Die Fußnoten (Spanien, Schweden, Portugal) sind keine Reiseländer.
- "Sonstiges" NUR, wenn das Dokument keinen bezahlten Betrag enthält und weder Buchung
  noch Änderung ist.

Pflichtfelder: belegdatum, transportart, anbieter, betrag_brutto, waehrung, event_datum_von
Setze pflichtfelder_ok=false und liste fehlende_pflichtfelder wenn ein Pflichtfeld null ist.

JSON-Format:
""" + """{
  "belegdatum": "DD.MM.YYYY",
  "belegart": "Rechnung|Buchungsbestaetigung|Quittung|Flugaenderung|A1Bescheinigung|Sonstiges",
  "transportart": "Hotel|Flug|Bahn|Mietwagen|Taxi|Tanken|Verpflegung|Bewirtung|Sonstiges",
  "transportart_freitext": "nur wenn Sonstiges",
  "anbieter": "Name des Anbieters",
  "rechnungsnummer": "Rechnungs- oder Belegnummer",
  "buchungscode": "PNR oder Bestaetigungsnummer",
  "reisender": "Vollstaendiger Name des Reisenden",
  "land_beleg": "ISO-Laendercode z.B. DE, FR, US",
  "betrag_brutto": 107.20,
  "betrag_netto": 89.33,
  "betrag_mwst": 17.87,
  "mwst_berechnet": false,
  "waehrung": "EUR",
  "betrag_eur_geschaetzt": "nur bei waehrung != EUR: geschätzter Euro-Gegenwert zum Kurs des Belegdatums, sonst null",
  "kurs_geschaetzt": "nur bei waehrung != EUR: verwendeter Wechselkurs (1 EUR = X Fremdwährung), sonst null",
  "zahlungsart": "Kreditkarte|Bar|Ueberweisung|PayPal|DurchKunde|Unbekannt (DurchKunde nur wenn explizit vermerkt, dass ein Kunde die Kosten direkt selbst uebernommen/bezahlt hat, z.B. 'vom Kunden beglichen'; PayPal erkennbar an 'PayPal' im Text, z.B. 'Diese Zahlung wurde ueber PayPal getaetigt' – PayPal gilt als eigene Kategorie, auch wenn im Hintergrund eine Kreditkarte hinterlegt ist; sonst Kreditkarte erkennbar an Visa/Mastercard/Amex, maskierter Kartennummer wie xxxx1234, 'bar bezahlt'/cash, oder Ueberweisungshinweisen; bei Reisebuero-Buchungen meist Ueberweisung)",
  "event_datum_von": "DD.MM.YYYY (bei Parkplatzreservierung am Flughafen: EINSTELLDATUM)",
  "event_datum_bis": "DD.MM.YYYY (bei Parkplatzreservierung am Flughafen: ABHOLDATUM – WICHTIG: immer ausfuellen, auch wenn nur ein Zeitraum wie '20.09.-28.09.' auf dem Beleg steht, NICHT nur das erste Datum uebernehmen)",
  "event_zeit": "HH:MM (Uhrzeit auf dem Beleg, z.B. bei Tankquittung, Parkschein, Mautbeleg – NICHT bei Flug/Bahn/Hotel, dafuer gibt es eigene Zeitfelder)",
  "event_ort_von": "Stadtname",
  "event_ort_bis": "Stadtname",
  "hotel_name": "Hotelname",
  "hotel_adresse": "Straße Hausnummer, PLZ Ort (falls auf dem Beleg vorhanden, sonst null)",
  "hotel_lat": 45.0703,
  "hotel_lon": 7.6869,
  "hotel_checkin_datum": "DD.MM.YYYY",
  "hotel_checkin_zeit": "HH:MM",
  "hotel_checkout_datum": "DD.MM.YYYY",
  "hotel_checkout_zeit": "HH:MM",
  "hotel_naechte": 2,
  "tanken_kraftstoff": "Benzin|Diesel|AdBlue|Elektro|Super|SuperPlus",
  "tanken_menge": 45.3,
  "tanken_einheit": "Liter|kWh",
  "tanken_preis_pro_einheit": 1.789,
  "tanken_tankstelle": "Name und Ort der Tankstelle",
  "tanken_kennzeichen": "Fahrzeugkennzeichen",
  "segmente": [
    {
      "nr": 1,
      "abreise_datum": "DD.MM.YYYY",
      "abreise_zeit": "HH:MM",
      "abreise_zeitzone": "MEZ|UTC|EST|CR|GMT",
      "abreise_utc_offset": "+02:00",
      "abreise_terminal": "z.B. Terminal 1 (falls angegeben, sonst null)",
      "ankunft_datum": "DD.MM.YYYY",
      "ankunft_zeit": "HH:MM",
      "ankunft_zeitzone": "MEZ|UTC|EST|CR|GMT",
      "ankunft_utc_offset": "+03:00",
      "ankunft_terminal": "z.B. Terminal 2 (falls angegeben, sonst null)",
      "von_ort": "Stadtname z.B. Frankfurt",
      "von_iata": "FRA",
      "von_lat": 50.0333,
      "von_lon": 8.5706,
      "nach_ort": "Stadtname z.B. Lyon",
      "nach_iata": "LYS",
      "nach_lat": 45.7256,
      "nach_lon": 5.0811,
      "transport_name": "Lufthansa|Swiss|ITA Airways|DB",
      "transport_nummer": "LH3463|AZ123|ICE123",
      "klasse": "Economy|Business|1.Klasse",
      "sitzplatz": "z.B. 12A (Bahn: Wagen 23, Platz 61) – nur wenn auf dem Beleg angegeben, sonst null",
      "hinweis": "z.B. operated by Edelweiss"
    }
  ],
  "a1_laender": "nur bei A1Bescheinigung: Liste der ISO-Ländercodes des Arbeitsortes, z.B. [\"PL\"], sonst null",
  "pflichtfelder_ok": true,
  "fehlende_pflichtfelder": []
}

--- BELEGTEXT ---
""" + rohtext[:30000]

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await _openai_chat(client, [{"role": "user", "content": prompt}], 4000)

            if resp.status_code != 200:
                return {"fehler": f"HTTP {resp.status_code}: {resp.text[:300]}",
                        "pflichtfelder_ok": False,
                        "fehlende_pflichtfelder": [f"HTTP {resp.status_code}: {resp.text[:200]}"]}

            raw = resp.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                result = mwst_pruefen(belegart_normalisieren(json.loads(m.group(0))), rohtext)
                pflicht = ["belegdatum","transportart","anbieter",
                           "betrag_brutto","waehrung","event_datum_von","zahlungsart"]
                if result.get("belegart") == "Buchungsbestaetigung":
                    pflicht = [f for f in pflicht if f not in ("betrag_brutto","waehrung","zahlungsart")]
                if result.get("belegart") == "A1Bescheinigung":
                    pflicht = []          # A1-Bescheinigung: kein Betrag, keine Pflichtfelder der Kostenbelege
                fehlend = [f for f in pflicht if not result.get(f)]
                result["pflichtfelder_ok"] = len(fehlend) == 0
                result["fehlende_pflichtfelder"] = fehlend
                return result
            return {"fehler": "Kein JSON in Antwort", "raw": raw[:300],
                    "pflichtfelder_ok": False,
                    "fehlende_pflichtfelder": [f"Kein JSON: {raw[:150]}"]}

    except Exception as e:
        import traceback
        return {"fehler": str(e),
                "trace": traceback.format_exc()[:500],
                "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["Exception: " + str(e)[:100]]}


def lade_ma_daten() -> tuple:
    """Lädt Mitarbeiternamen aus DB für Anonymisierung."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT klarname, email, email2, email3 FROM mitarbeiter")
        rows = cur.fetchall()
        cur.close(); db.close()
        namen = [r[0] if isinstance(r, tuple) else r["klarname"] for r in rows]
        mails = [IMAP_USER] if IMAP_USER else []
        for r in rows:
            for idx in (1, 2, 3):
                val = r[idx] if isinstance(r, tuple) else r[["email","email2","email3"][idx-1]]
                if val:
                    mails.append(val)
        return namen, mails
    except Exception as e:
        import traceback
        print(f"[lade_ma_daten FEHLER] Anonymisierung nutzt leere Mitarbeiterliste! {e}")
        print(traceback.format_exc()[:500])
        return [], []


def _zahl(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def kurs_je_einheit(kurs_ki, brutto, eur) -> float | None:
    """Wechselkurs in der Schreibweise der App: EUR je 1 Fremdwährung (z. B. 1 MXN = 0,0549 EUR).
    Die KI nennt ihn als 'Fremdwährung je 1 EUR' (1 EUR = 18,2 MXN), manchmal auch umgekehrt.
    Maßgeblich ist der Euro-Gegenwert: Passt der Kehrwert des KI-Kurses (oder der Kurs selbst) auf ±5 % zu
    Euro-Betrag / Brutto-Betrag, gilt dieser, sonst der Quotient selbst. Ohne Beträge gilt der Kehrwert."""
    k, b, e = _zahl(kurs_ki), _zahl(brutto), _zahl(eur)
    if b and e:
        quotient = e / b
        for kand in ((1.0 / k, k) if k else ()):
            if abs(kand - quotient) / quotient <= 0.05:
                return round(kand, 6)
        return round(quotient, 6)
    return round(1.0 / k, 6) if k else None


def kurse_vereinheitlichen(db) -> int:
    """Stellt KI-geschätzte Wechselkurse, die als 'Fremdwährung je 1 EUR' gespeichert wurden (z. B. 18.2 MXN),
    auf 'EUR je 1 Fremdwährung' um (0.0549). Erkennung: Der Kehrwert passt zum Euro-Gegenwert, der Kurs selbst nicht.
    Idempotent, Commit macht der Aufrufer. Gibt die Zahl korrigierter Belege zurück."""
    P = ph()
    cur = db.cursor()
    cur.execute("""SELECT id, kurs_eur, betrag_brutto, betrag_eur FROM belege
                   WHERE kurs_quelle = 'KI-geschätzt' AND kurs_eur IS NOT NULL AND betrag_brutto IS NOT NULL
                   AND betrag_eur IS NOT NULL""")
    n = 0
    for z in cur.fetchall():
        bid, k, b, e = (z[i] for i in range(4))
        k, b, e = _zahl(k), _zahl(b), _zahl(e)
        if not (k and b and e):
            continue
        quotient = e / b
        if abs(k - quotient) / quotient > 0.05 and abs(1.0 / k - quotient) / quotient <= 0.05:
            cur.execute(f"UPDATE belege SET kurs_eur={P} WHERE id={P}", (round(1.0 / k, 6), bid))
            n += 1
    cur.close()
    return n


APP_BELEGARTEN = ("Bewirtungsbeleg", "Vorschussquittung", "Rueckgabequittung", "Bargeldbeschaffung")
APP_BELEG_HINWEIS = ("Dieser Beleg wurde von der App selbst erzeugt (Bewirtung bzw. Vorschuss/Bargeld) und wird "
                     "nicht per KI analysiert. Änderungen bitte über die Bewirtung bzw. das Bargeld-Konto vornehmen.")


def ist_app_erzeugt(cur, bid: int) -> bool:
    """True für Belege, die die App selbst erzeugt hat (Bewirtungsbeleg, Vorschuss-/Rückgabequittung,
    Bargeldbeschaffung). Erkennt sie auch dann, wenn die Belegart überschrieben wurde (Verknüpfung zur Buchung)."""
    P = ph()
    cur.execute(f"SELECT belegart FROM belege WHERE id={P}", (bid,))
    r = cur.fetchone()
    if r and (r[0] if isinstance(r, tuple) else r["belegart"]) in APP_BELEGARTEN:
        return True
    for tabelle in ("bewirtungen", "bargeld_buchungen"):
        try:
            cur.execute(f"SELECT 1 FROM {tabelle} WHERE beleg_id={P}", (bid,))
            if cur.fetchone():
                return True
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
    return False


async def beleg_neu_analysieren(bid: int) -> dict:
    """
    Führt die KI-Analyse für einen bereits gespeicherten Beleg erneut aus
    (z.B. um neue Felder wie event_zeit nachträglich zu befüllen) und
    aktualisiert die Analyse-Felder in der DB.
    Funktioniert für beide Beleg-Arten:
    - Text-Belege: nutzt den gespeicherten Rohtext direkt.
    - Bild-Belege (Foto ohne extrahierbaren Text): das Originalfoto wird nie
      separat gespeichert, sondern nur eingebettet in "original.pdf" (siehe
      bild_zu_pdf()) – hier wird das Bild aus genau diesem PDF wieder
      extrahiert und erneut per Bilderkennung (gpt_analyse_bild) analysiert,
      statt die Neuanalyse für Bild-Belege komplett abzulehnen.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    # Änderungs-Belege dürfen nicht neu analysiert werden: das Ergebnis würde
    # deren (bewusst neutralisierte) Segmente/Beträge wieder in die Reise
    # einspeisen und alles doppelt zählen.
    try:
        cur.execute(f"SELECT aenderung_status FROM belege WHERE id={P}", (bid,))
        ar = cur.fetchone()
        if ar and (ar[0] if isinstance(ar, tuple) else ar["aenderung_status"]):
            cur.close(); db.close()
            return {"fehler": "Dies ist ein Änderungs-Beleg – Neu-Analyse nicht möglich. "
                              "Bitte die Änderungen übernehmen oder verwerfen."}
    except Exception:
        db.rollback()
    if ist_app_erzeugt(cur, bid):
        cur.close(); db.close()
        return {"fehler": APP_BELEG_HINWEIS}
    cur.execute(f"SELECT rohtext, s3_original, dateiname FROM belege WHERE id={P}", (bid,))
    row = cur.fetchone()
    if not row:
        cur.close(); db.close()
        return {"fehler": "Beleg nicht gefunden"}
    rohtext = row[0] if isinstance(row, tuple) else row["rohtext"]
    s3_original = row[1] if isinstance(row, tuple) else row["s3_original"]
    dateiname = row[2] if isinstance(row, tuple) else row["dateiname"]
    cur.close(); db.close()

    ist_bild_beleg = not rohtext or rohtext.strip().startswith("{")

    if ist_bild_beleg:
        if not s3_original:
            return {"fehler": "Kein gespeichertes Original vorhanden – Neuanalyse nicht möglich"}
        try:
            pdf_bytes = s3_download(s3_original)
            seiten_bilder = pdf_seitenbilder(pdf_bytes)
            if not seiten_bilder:
                return {"fehler": "Konnte das Originalfoto nicht aus dem gespeicherten PDF extrahieren"}
            ki_result = await gpt_analyse_bild(seiten_bilder[0], "image/jpeg", dateiname or "beleg", seiten_bilder[1:])
        except Exception as e:
            return {"fehler": f"Bild-Neuanalyse fehlgeschlagen: {e}"}
    elif mod_a1.a1_erkennen(rohtext, dateiname or ""):
        ki_result = mod_a1.ki_ergebnis(mod_a1.a1_auswerten(rohtext, dateiname or ""), dateiname or "")
    else:
        ki_result = await gpt_analyse(rohtext, "")
    ki_result = mod_a1.a1_pruefen(ki_result) or ki_result

    if "fehler" in ki_result and not ki_result.get("anbieter"):
        return {"fehler": ki_result.get("fehler", "KI-Analyse fehlgeschlagen")}

    def pd(key):
        v = ki_result.get(key)
        if not v: return None
        try:
            from datetime import datetime as _dtt
            return _dtt.strptime(str(v).strip(), "%d.%m.%Y").date()
        except: return None

    def pn(key):
        v = ki_result.get(key)
        try: return float(v) if v is not None else None
        except: return None

    # Bei Hotels die Adresse ECHT geokodieren (Nominatim), statt nur die
    # KI-Schätzung "aus dem Kopf" zu übernehmen – eine Adresse mit PLZ ist
    # eindeutig, eine reine KI-Schätzung kann bei weniger bekannten Orten
    # (z.B. kleineren Schweizer Gemeinden) daneben liegen. Nur bei Erfolg
    # überschreiben; schlägt die Geokodierung fehl, bleibt die KI-Schätzung
    # als Rückfall bestehen.
    if ki_result.get("transportart") == "Hotel" and ki_result.get("hotel_adresse"):
        geo = adresse_geokodieren(ki_result["hotel_adresse"])
        if geo:
            ki_result["hotel_lat"], ki_result["hotel_lon"] = geo[0], geo[1]

    # Bei allen Flug-/Bahnsegmenten die geschätzten Koordinaten durch die
    # exakten Flughafen-Koordinaten ersetzen (siehe Funktionskommentar oben).
    _segment_koordinaten_praezisieren(ki_result)

    ki_json_str = json.dumps(ki_result, ensure_ascii=False)
    pflicht_ok = bool(ki_result.get("pflichtfelder_ok", False))
    fehlend_str = json.dumps(ki_result.get("fehlende_pflichtfelder", []), ensure_ascii=False)
    status = "ok" if pflicht_ok else "fehlerhaft"
    waehrung_ki = ki_result.get("waehrung", "EUR") or "EUR"
    betrag_eur_geschaetzt = pn("betrag_eur_geschaetzt") if waehrung_ki != "EUR" else None
    kurs_geschaetzt = pn("kurs_geschaetzt") if waehrung_ki != "EUR" else None

    db = get_db(); cur = db.cursor()
    # Bisherige Kurs-Quelle prüfen: eine manuell/per EZB gepflegte Schätzung
    # wird durch die Neuanalyse nicht überschrieben, nur eine vorherige
    # KI-Schätzung oder ein noch leerer Wert.
    cur.execute(f"SELECT kurs_quelle FROM belege WHERE id={P}", (bid,))
    row_kurs = cur.fetchone()
    alte_kurs_quelle = (row_kurs[0] if isinstance(row_kurs, tuple) else row_kurs["kurs_quelle"]) if row_kurs else None
    kurs_ueberschreibbar = alte_kurs_quelle in (None, "KI-geschätzt")

    kurs_update_sql = ""
    kurs_update_vals = ()
    if betrag_eur_geschaetzt is not None and kurs_ueberschreibbar:
        kurs_update_sql = f", betrag_eur={P}, kurs_eur={P}, kurs_datum={P}, kurs_quelle={P}"
        kurs_update_vals = (betrag_eur_geschaetzt,
                            kurs_je_einheit(kurs_geschaetzt, pn("betrag_brutto"), betrag_eur_geschaetzt),
                            pd("belegdatum"), "KI-geschätzt")

    cur.execute(f"""UPDATE belege SET
        ki_json={P}, pflichtfelder_ok={P}, fehlende_felder={P},
        belegdatum={P}, belegart={P}, transportart={P}, transportart_freitext={P},
        anbieter={P}, rechnungsnummer={P}, buchungscode={P}, reisender={P}, land_beleg={P},
        betrag_brutto={P}, betrag_netto={P}, betrag_mwst={P}, waehrung={P}, zahlungsart={P},
        event_datum_von={P}, event_datum_bis={P}, event_zeit={P},
        event_ort_von={P}, event_ort_bis={P},
        hotel_name={P}, hotel_adresse={P}, hotel_lat={P}, hotel_lon={P},
        hotel_checkin_datum={P}, hotel_checkin_zeit={P},
        hotel_checkout_datum={P}, hotel_checkout_zeit={P}, hotel_naechte={P},
        tanken_kraftstoff={P}, tanken_menge={P}, tanken_einheit={P},
        tanken_preis_einheit={P}, tanken_tankstelle={P}, tanken_kennzeichen={P},
        status={P}{kurs_update_sql}
        WHERE id={P}""", (
        ki_json_str, pflicht_ok, fehlend_str,
        pd("belegdatum"), ki_result.get("belegart"),
        ki_result.get("transportart"), ki_result.get("transportart_freitext"),
        ki_result.get("anbieter"), ki_result.get("rechnungsnummer"),
        ki_result.get("buchungscode"), ki_result.get("reisender"),
        ki_result.get("land_beleg"),
        pn("betrag_brutto"), pn("betrag_netto"), pn("betrag_mwst"),
        waehrung_ki, ki_result.get("zahlungsart"),
        pd("event_datum_von"), pd("event_datum_bis"), ki_result.get("event_zeit"),
        ki_result.get("event_ort_von"), ki_result.get("event_ort_bis"),
        ki_result.get("hotel_name"), ki_result.get("hotel_adresse"),
        pn("hotel_lat"), pn("hotel_lon"), pd("hotel_checkin_datum"),
        ki_result.get("hotel_checkin_zeit"), pd("hotel_checkout_datum"),
        ki_result.get("hotel_checkout_zeit"),
        ki_result.get("hotel_naechte"),
        ki_result.get("tanken_kraftstoff"), pn("tanken_menge"),
        ki_result.get("tanken_einheit"), pn("tanken_preis_pro_einheit"),
        ki_result.get("tanken_tankstelle"), ki_result.get("tanken_kennzeichen"),
        status) + kurs_update_vals + (bid,))
    db.commit(); cur.close(); db.close()
    vma_auto_fuer_beleg(bid)          # Hotel-/Flugdaten können sich geändert haben -> VMA der Reise aktualisieren
    return {"ok": True}


async def beleg_neu_anonymisieren(bid: int) -> dict:
    """
    Führt die Anonymisierung für einen bereits gespeicherten Beleg erneut aus
    (z.B. wenn beim ersten Durchlauf die Mitarbeiterliste leer war) und
    überschreibt anon_text + anon.pdf in DB und S3.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    if ist_app_erzeugt(cur, bid):
        cur.close(); db.close()
        return {"fehler": APP_BELEG_HINWEIS}
    cur.execute(f"SELECT rohtext, dateiname, s3_anon FROM belege WHERE id={P}", (bid,))
    row = cur.fetchone()
    if not row:
        cur.close(); db.close()
        return {"fehler": "Beleg nicht gefunden"}

    rohtext = row[0] if isinstance(row, tuple) else row["rohtext"]
    dateiname = row[1] if isinstance(row, tuple) else row["dateiname"]
    s3_anon_key = row[2] if isinstance(row, tuple) else row["s3_anon"]

    if not rohtext:
        cur.close(); db.close()
        return {"fehler": "Kein Rohtext vorhanden – kann nicht neu anonymisiert werden"}

    ma_namen, ma_mails = lade_ma_daten()
    anon_text = anonymisieren(rohtext, ma_namen, ma_mails)
    anon_pdf = text_zu_pdf(anon_text, f"Anonymisiert: {dateiname or ''}")

    if s3_anon_key:
        s3_upload(s3_anon_key, anon_pdf)

    cur.execute(f"UPDATE belege SET anon_text={P} WHERE id={P}",
                (anon_text[:50000] or None, bid))
    db.commit(); cur.close(); db.close()

    return {"ok": True, "ma_anzahl": len(ma_namen)}


def erechnung_erkennen(original_pdf: bytes) -> dict:
    """
    Prüft, ob im PDF eine eingebettete eRechnungs-XML steckt (ZUGFeRD/Factur-X-
    Format – üblich bei deutschen/europäischen eRechnungen). Liest die Datei
    NUR aus, verändert sie nicht. Gibt {"ist_erechnung", "format", "xml_bytes"} zurück.
    """
    ZUGFERD_NAMEN = ("factur-x.xml", "zugferd-invoice.xml", "xrechnung.xml",
                      "invoice.xml", "cii.xml", "order-x.xml")
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(original_pdf))
        anhaenge = reader.attachments  # Mapping name -> [bytes, ...]
        for name in anhaenge:
            name_l = name.lower()
            if name_l.endswith(".xml") or name_l in ZUGFERD_NAMEN:
                versionen = list(anhaenge[name])
                if versionen:
                    fmt = "ZUGFeRD/Factur-X" if "factur" in name_l or "zugferd" in name_l or "cii" in name_l else "XRechnung (eingebettet)"
                    return {"ist_erechnung": True, "format": fmt, "xml_bytes": versionen[0], "xml_name": name}
    except Exception:
        pass
    return {"ist_erechnung": False, "format": None, "xml_bytes": None, "xml_name": None}


# ── Änderungs-Erkennung: neues Dokument vs. bereits vorhandener Beleg ─────────
_AENDERUNGS_TYPEN = ("Flug", "Bahn", "Mietwagen", "Hotel")
_SEG_FELDER = [
    ("transport_nummer", "Flug-/Zugnummer"), ("transport_name", "Gesellschaft"),
    ("von_ort", "Von"), ("nach_ort", "Nach"),
    ("abreise_datum", "Abreisedatum"), ("abreise_zeit", "Abreisezeit"),
    ("ankunft_datum", "Ankunftsdatum"), ("ankunft_zeit", "Ankunftszeit"),
    ("abreise_terminal", "Abreise-Terminal/Gleis"), ("ankunft_terminal", "Ankunfts-Terminal/Gleis"),
    ("klasse", "Klasse"), ("sitzplatz", "Sitzplatz"),
]
_TOP_FELDER = [
    ("betrag_brutto", "Betrag brutto"), ("waehrung", "Währung"),
    ("event_datum_von", "Datum von"), ("event_datum_bis", "Datum bis"),
    ("hotel_name", "Hotel"), ("hotel_adresse", "Hotel-Adresse"),
    ("hotel_checkin_datum", "Hotel Check-in (Datum)"), ("hotel_checkin_zeit", "Hotel Check-in (Zeit)"),
    ("hotel_checkout_datum", "Hotel Check-out (Datum)"), ("hotel_checkout_zeit", "Hotel Check-out (Zeit)"),
]


def _namens_tokens(name) -> set:
    """Namen robust zerlegen: "DIESSLIN/RALF MR" == "Ralf Dießlin" (ß/ss, Umlaute,
    Reihenfolge, Anrede egal)."""
    import unicodedata
    s = str(name or "").lower()
    for a, b in (("ß", "ss"), ("ä", "ae"), ("ö", "oe"), ("ü", "ue")):
        s = s.replace(a, b)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-z\s]", " ", s)
    stop = {"herr", "frau", "mr", "mrs", "ms", "miss", "mister", "dr", "prof", "hr", "fr", "mstr", "chd", "inf"}
    return {t for t in s.split() if len(t) >= 2 and t not in stop}


def _gleicher_reisender(a, b) -> bool:
    """False NUR, wenn beide Namen bekannt sind und keinen gemeinsamen Namensteil
    haben (= klar verschiedene Personen, z.B. zwei Kollegen mit eigenem Ticket
    für denselben Flug). Ist ein Name unbekannt, gilt vorsichtig 'gleich'."""
    ta, tb = _namens_tokens(a), _namens_tokens(b)
    if not ta or not tb:
        return True
    return bool(ta & tb)


def _leer(v) -> bool:
    return v is None or str(v).strip() == ""


def _wert_gleich(feld: str, a, b) -> bool:
    """Tolerant vergleichen: Groß/Kleinschreibung, Leerzeichen, 9:05 = 09:05,
    Beträge auf den Cent genau."""
    if "betrag" in feld:
        try: return abs(float(a) - float(b)) < 0.005
        except (TypeError, ValueError): pass
    sa, sb = re.sub(r"\s+", " ", str(a)).strip().lower(), re.sub(r"\s+", " ", str(b)).strip().lower()
    if "zeit" in feld:
        ma, mb = re.match(r"^(\d{1,2}):(\d{2})", sa), re.match(r"^(\d{1,2}):(\d{2})", sb)
        if ma and mb:
            return (int(ma.group(1)), ma.group(2)) == (int(mb.group(1)), mb.group(2))
    return sa == sb


def _dup_zeilen(zeilen) -> list:
    """DB-Zeilen (id, ki_json, belegart, rechnungsnummer, betrag_brutto) als dicts."""
    namen = ("id", "ki_json", "belegart", "rechnungsnummer", "betrag_brutto")
    return [{n: (z[n] if hasattr(z, "keys") else z[i]) for i, n in enumerate(namen)} for z in zeilen]


def _beleg_klasse(art) -> str:
    a = (art or "").lower()
    if "buchungsbest" in a:
        return "buchung"
    if "aenderung" in a or "änderung" in a:
        return "aenderung"
    return "beleg"


def _gleiche_beleg_klasse(art_alt, art_neu) -> bool:
    """Buchungsbestätigung und Rechnung/Quittung sind verschiedene Belegklassen. Änderungs-Dokumente
    (z. B. Flugänderung) passen zu jeder Klasse."""
    k1, k2 = _beleg_klasse(art_alt), _beleg_klasse(art_neu)
    return k1 == k2 or "aenderung" in (k1, k2)


def _ist_derselbe_beleg(alt: dict, ki: dict) -> bool:
    """
    Gleicher Buchungscode/gleiche Nummer + gleicher Anbieter: ist das wirklich derselbe Beleg
    (Duplikat oder Änderung) – oder ein anderer Beleg derselben Buchung?
    - Buchungsbestätigung und Rechnung/Quittung sind verschiedene Belege (Buchung x, Rechnungen x1 + x2).
    - Zwei Rechnungen sind nur dann derselbe Beleg, wenn die Rechnungsnummer übereinstimmt
      (fehlt sie, entscheidet der Betrag; fehlt auch der, gilt vorsichtig "derselbe").
    - Ein Änderungs-Dokument (z. B. Flugänderung) gehört immer zum vorhandenen Beleg.
    """
    k_alt, k_neu = _beleg_klasse(alt.get("belegart")), _beleg_klasse(ki.get("belegart"))
    if "aenderung" in (k_alt, k_neu):
        return True
    if k_alt != k_neu:
        return False
    if k_neu == "buchung":
        return True
    rn_alt = re.sub(r"\s+", "", str(alt.get("rechnungsnummer") or "")).lower()
    rn_neu = re.sub(r"\s+", "", str(ki.get("rechnungsnummer") or "")).lower()
    if rn_alt and rn_neu:
        return rn_alt == rn_neu
    try:
        return abs(float(alt.get("betrag_brutto")) - float(ki.get("betrag_brutto"))) < 0.005
    except (TypeError, ValueError):
        return True


def _seg_titel(s: dict) -> str:
    return (f'{s.get("transport_nummer") or s.get("transport_name") or "Segment"} '
            f'{s.get("von_ort") or "?"}→{s.get("nach_ort") or "?"}').strip()


def _aenderungen_ermitteln(alt: dict, neu: dict) -> list:
    """
    Vergleicht den bereits gespeicherten Beleg (alt) mit dem neu eingegangenen
    Dokument (neu) und liefert eine Liste der Unterschiede. Regeln:
    - Nur Werte, die im NEUEN Dokument tatsächlich stehen, zählen (fehlt etwas
      im neuen Dokument, ist das keine Änderung – es wird nichts überschrieben).
    - Fehlt ein Wert im alten Beleg, im neuen steht er: Ergänzung.
    - Segmente werden zuerst über die Flug-/Zugnummer zugeordnet, dann über
      die Strecke, dann (bei gleich vielen übrigen) über die Reihenfolge –
      so wird auch eine GEÄNDERTE Flugnummer als Änderung erkannt statt als
      "neues + wegfallendes Segment".
    """
    diffs = []
    for feld, label in _TOP_FELDER:
        a, b = alt.get(feld), neu.get(feld)
        if _leer(b) or (not _leer(a) and _wert_gleich(feld, a, b)):
            continue
        diffs.append({"typ": "aenderung", "pfad": f"feld:{feld}", "label": label,
                      "alt": "" if _leer(a) else str(a), "neu": str(b)})

    alt_s, neu_s = alt.get("segmente") or [], neu.get("segmente") or []
    key = lambda s: re.sub(r"\s+", "", str(s.get("transport_nummer") or "")).upper()
    used, paare = set(), []
    for ni, ns in enumerate(neu_s):                       # 1) gleiche Nummer
        kand = [oi for oi, os_ in enumerate(alt_s) if oi not in used and key(ns) and key(os_) == key(ns)]
        if kand:
            best = next((oi for oi in kand if _wert_gleich("datum", alt_s[oi].get("abreise_datum"),
                                                            ns.get("abreise_datum"))), kand[0])
            used.add(best); paare.append([ni, best])
        else:
            paare.append([ni, None])
    for p in paare:                                        # 2) gleiche Strecke
        if p[1] is None:
            ns = neu_s[p[0]]
            kand = [oi for oi, os_ in enumerate(alt_s) if oi not in used
                    and _wert_gleich("x", os_.get("von_ort") or "", ns.get("von_ort") or "")
                    and _wert_gleich("x", os_.get("nach_ort") or "", ns.get("nach_ort") or "")]
            if kand: used.add(kand[0]); p[1] = kand[0]
    offen_n = [p for p in paare if p[1] is None]
    offen_a = [oi for oi in range(len(alt_s)) if oi not in used]
    if offen_n and len(offen_n) == len(offen_a):           # 3) gleiche Reihenfolge
        for p, oi in zip(offen_n, offen_a):
            p[1] = oi; used.add(oi)

    for ni, oi in paare:
        ns = neu_s[ni]
        if oi is None:
            diffs.append({"typ": "neu_segment", "pfad": "neu_segment", "neu_segment": ns,
                          "label": f"Neues Segment: {_seg_titel(ns)} am {ns.get('abreise_datum') or '?'} {ns.get('abreise_zeit') or ''}".strip(),
                          "alt": "", "neu": "hinzufügen"})
            continue
        os_ = alt_s[oi]
        for feld, label in _SEG_FELDER:
            a, b = os_.get(feld), ns.get(feld)
            if _leer(b) or (not _leer(a) and _wert_gleich(feld, a, b)):
                continue
            diffs.append({"typ": "aenderung", "pfad": f"seg:{oi}:{feld}",
                          "label": f"Segment {oi + 1} ({_seg_titel(os_)}) · {label}",
                          "alt": "" if _leer(a) else str(a), "neu": str(b)})
    if neu_s and len(neu_s) >= len(alt_s):                 # Dokument wirkt vollständig
        for oi in range(len(alt_s)):
            if oi not in used:
                diffs.append({"typ": "entfaellt", "pfad": f"entfaellt:{oi}",
                              "label": f"Segment {oi + 1} fehlt im neuen Dokument: {_seg_titel(alt_s[oi])}",
                              "alt": "bleibt bestehen", "neu": "entfernen"})
    return diffs



async def beleg_verarbeiten(
    datei_bytes: bytes,
    dateiname: str,
    reise_code: str | None,
    content_type: str = "application/pdf",
    dokumentart: str | None = None,
    a1_kuerzel: str | None = None,
) -> dict:
    """
    Komplette Beleg-Pipeline (dokumentart="A1Bescheinigung": ausdrücklich als A1 hochgeladen, a1_kuerzel =
    Mitarbeiter, dem sie gehört):
    1. Zu PDF konvertieren
    2. Anonymisieren
    3. GPT-4o Analyse
    4. S3 speichern
    5. DB-Eintrag
    Gibt beleg_id zurück.
    """
    import uuid
    beleg_id_temp = str(uuid.uuid4())[:8]
    content_type = inhaltstyp_bestimmen(datei_bytes, dateiname, content_type)

    # 1. Zu PDF konvertieren
    ist_eigenstaendige_xml = (
        dateiname.lower().endswith(".xml")
        or content_type in ("application/xml", "text/xml")
    )
    if ist_eigenstaendige_xml:
        # Eigenständige XRechnung (reine XML-Datei, kein ZUGFeRD-PDF).
        # Original-XML bleibt für die Archivierung unverändert erhalten;
        # für die interne Ansicht/GPT-Analyse wird zusätzlich eine lesbare
        # PDF-Wiedergabe erzeugt.
        xml_text = datei_bytes.decode("utf-8", errors="ignore")
        original_pdf = text_zu_pdf(xml_text, f"XRechnung: {dateiname}")
        erechnung_info = {"ist_erechnung": True, "format": "XRechnung (eigenständig)",
                           "xml_bytes": datei_bytes, "xml_name": dateiname}
    elif content_type in BILD_TYPEN:
        original_pdf = bild_zu_pdf(datei_bytes, dateiname)
        erechnung_info = {"ist_erechnung": False, "format": None, "xml_bytes": None, "xml_name": None}
    elif content_type == "application/pdf":
        original_pdf = datei_bytes
        # 1b. eRechnung erkennen (ZUGFeRD/Factur-X eingebettete XML) – nur
        # lesend, das Original-PDF bleibt unverändert.
        erechnung_info = erechnung_erkennen(original_pdf)
    else:
        # Text/Mail → PDF
        text = datei_bytes.decode(errors="ignore")
        original_pdf = text_zu_pdf(text, dateiname)
        erechnung_info = {"ist_erechnung": False, "format": None, "xml_bytes": None, "xml_name": None}

    # 2. Text aus PDF lesen
    rohtext = pdf_text_lesen(original_pdf)

    # 2b. A1-Bescheinigung? Erkennung am Text (bei Scans am Dateinamen) oder ausdrücklich als A1 hochgeladen.
    # Mit Text wird sie direkt ausgewertet (ohne KI, ohne Betrag) und als Beleg „A1Bescheinigung“ abgelegt.
    # Bei Scans liest die KI die Angaben; sagt sie „keine A1“, obwohl nur der Dateiname so aussah, gilt die KI.
    is_image = content_type in BILD_TYPEN
    ohne_text = len(rohtext.strip()) < 20
    a1_felder = None
    # Ohne Text hilft neben dem Dateinamen der Titel in den PDF-Eigenschaften („Bescheinigung A1 - Onlineversion“)
    name_hinweis = f"{dateiname} {pdf_titel_lesen(original_pdf)}" if ohne_text else dateiname
    if dokumentart == mod_a1.BELEGART or mod_a1.a1_erkennen(rohtext, name_hinweis):
        a1_felder = mod_a1.a1_auswerten(rohtext, dateiname)
    a1_scan = a1_felder is not None and ohne_text

    # Bild ohne extrahierbaren Text → GPT-4o Vision direkt
    # Scan ohne Text (Bild oder PDF aus Bildern): die KI liest alle Seiten (höchstens MAX_KI_SEITEN)
    seiten_bilder = pdf_seitenbilder(original_pdf) if (ohne_text and (is_image or content_type == "application/pdf")) else []
    if a1_felder is not None and not a1_scan:
        ki_result = mod_a1.ki_ergebnis(a1_felder, dateiname)
    elif ohne_text and (is_image or (content_type == "application/pdf" and seiten_bilder)):
        if len(seiten_bilder) > 1:
            ki_result = await gpt_analyse_bild(seiten_bilder[0], "image/jpeg", dateiname, seiten_bilder[1:])
        else:
            ki_result = await gpt_analyse_bild(datei_bytes if is_image else seiten_bilder[0],
                                               content_type if is_image else "image/jpeg", dateiname)
        rohtext = json.dumps(ki_result, ensure_ascii=False, indent=2)
    else:
        ki_result = await gpt_analyse(rohtext, dateiname)
    if a1_felder is None or a1_scan:
        a1_ki = mod_a1.a1_pruefen(ki_result)          # die KI hat das Dokument als A1 erkannt
        if a1_ki:
            ki_result, a1_felder = a1_ki, (a1_felder or {})
        elif a1_scan and (dokumentart == mod_a1.BELEGART or "fehler" in ki_result):
            ki_result = mod_a1.ki_ergebnis(a1_felder, dateiname)   # KI nicht erreichbar oder ohne Ergebnis: einfache A1
            rohtext = json.dumps(ki_result, ensure_ascii=False, indent=2)
        else:
            a1_felder = None                              # nur der Dateiname sah nach A1 aus, die KI sagt: etwas anderes
    ist_a1 = a1_felder is not None
    if ist_a1:
        dup_a1 = mod_a1.duplikat_id(reise_code, ki_result, dateiname)
        if dup_a1:
            return {"beleg_id": dup_a1, "duplikat": True, "status": "duplikat",
                    "zusammenfassung": f"Duplikat – diese A1-Bescheinigung ist bereits als Beleg #{dup_a1} vorhanden."}

    # Inhaltsbasierter Duplikat-Check: Message-ID allein reicht nicht, da eine
    # WEITERLEITUNG einer E-Mail technisch eine neue, eigene Message-ID
    # bekommt – der reine Message-ID-Check in mod_mail.py erkennt mehrfach
    # weitergeleitete Belege deshalb NICHT als Duplikat. Hier stattdessen auf
    # den von der KI erkannten Buchungscode/Rechnungsnummer prüfen, der bei
    # jeder Weiterleitung/jedem erneuten Hochladen gleich bleibt.
    # WICHTIG: Gleicher Buchungscode heißt NICHT automatisch "Duplikat". Airlines/
    # Bahn schicken Änderungen (neue Zeiten, Ersatzflug, Gleiswechsel) ebenfalls
    # mit demselben Buchungscode – die dürfen nicht stillschweigend verworfen
    # werden. Deshalb Inhalt vergleichen: identisch = echtes Duplikat (z.B.
    # mehrfach weitergeleitet), abweichend = Änderungs-Beleg, der die
    # Unterschiede zur manuellen Klärung bereithält (siehe aenderung_*-Spalten).
    # WICHTIG: Gleicher Code heißt auch NICHT "gleicher Beleg", wenn es eine andere Art von Beleg ist:
    # Buchungsbestätigung (Betrag x) und die dazugehörigen Rechnungen (x1 + x2 = x) tragen oft denselben
    # Buchungscode. Die sind getrennte Belege und werden stattdessen verknüpft (mod_belegdeckung).
    aenderung_info = None
    dup_code = ki_result.get("buchungscode") or ki_result.get("rechnungsnummer")
    if dup_code and ki_result.get("anbieter"):
        db_dup = get_db(); cur_dup = db_dup.cursor(); P_dup = ph()
        try:
            cur_dup.execute(f"""SELECT id, ki_json, belegart, rechnungsnummer, betrag_brutto FROM belege
                WHERE (buchungscode={P_dup} OR rechnungsnummer={P_dup}) AND anbieter={P_dup}
                AND aenderung_status IS NULL
                ORDER BY id LIMIT 30""", (dup_code, dup_code, ki_result.get("anbieter")))
        except Exception:
            # Spalte aenderung_status fehlt noch (/init nicht gelaufen) -> altes Verhalten
            db_dup.rollback()
            cur_dup.execute(f"""SELECT id, ki_json, belegart, rechnungsnummer, betrag_brutto FROM belege
                WHERE (buchungscode={P_dup} OR rechnungsnummer={P_dup}) AND anbieter={P_dup}
                ORDER BY id LIMIT 30""", (dup_code, dup_code, ki_result.get("anbieter")))
        treffer = _dup_zeilen(cur_dup.fetchall())
        # Rückfall: Die Anbieter-Schreibweise kann zwischen Original und Änderung
        # abweichen ("Lufthansa" / "Deutsche Lufthansa AG"). Bei Flug/Bahn ist der
        # Buchungscode innerhalb DERSELBEN Reise eindeutig genug.
        if (not any(_ist_derselbe_beleg(t, ki_result) for t in treffer)
                and reise_code and ki_result.get("buchungscode")
                and ki_result.get("transportart") in ("Flug", "Bahn")):
            try:
                cur_dup.execute(f"""SELECT id, ki_json, belegart, rechnungsnummer, betrag_brutto FROM belege
                    WHERE buchungscode={P_dup} AND reise_code={P_dup} AND transportart={P_dup}
                    AND aenderung_status IS NULL ORDER BY id LIMIT 30""",
                    (ki_result["buchungscode"], reise_code, ki_result["transportart"]))
                treffer = _dup_zeilen(cur_dup.fetchall())
            except Exception:
                db_dup.rollback(); treffer = []
        cur_dup.close(); db_dup.close()
        vorhanden = next((t for t in treffer if _ist_derselbe_beleg(t, ki_result)), None)
        if vorhanden:
            bestehende_id = vorhanden["id"]
            alt_ki_str = vorhanden["ki_json"]
            try: alt_ki = json.loads(alt_ki_str or "{}")
            except Exception: alt_ki = {}
            # Anderer Reisender (z.B. zwei Kollegen, jeder mit eigenem Ticket auf
            # demselben Flug/Zug) = eigener Beleg, weder Duplikat noch Änderung.
            if _gleicher_reisender(alt_ki.get("reisender"), ki_result.get("reisender")):
                unterschiede = (_aenderungen_ermitteln(alt_ki, ki_result)
                                if ki_result.get("transportart") in _AENDERUNGS_TYPEN else [])
                if not unterschiede:
                    return {"beleg_id": bestehende_id, "duplikat": True,
                            "zusammenfassung": f"Duplikat von Beleg #{bestehende_id} (gleicher Buchungscode "
                                                f"'{dup_code}' + Anbieter, z.B. durch mehrfache Weiterleitung)"}
                aenderung_info = {"zu": bestehende_id, "diffs": unterschiede}

    # 3. Anonymisieren
    ma_namen, ma_mails = lade_ma_daten()
    anon_text = anonymisieren(rohtext, ma_namen, ma_mails)
    anon_pdf = text_zu_pdf(anon_text, f"Anonymisiert: {dateiname}")

    # 4. GPT-4o Analyse
    ki_json_str = json.dumps(ki_result, ensure_ascii=False)

    # 4b. Duplikat-Check: gleicher Anbieter + Betrag + Belegdatum (oder gleiche Rechnungsnummer)
    # bereits vorhanden? Dann keinen neuen Beleg anlegen.
    anbieter_chk = ki_result.get("anbieter")
    betrag_chk = ki_result.get("betrag_brutto")
    beleg_dat_chk = ki_result.get("belegdatum")
    rechnr_chk = ki_result.get("rechnungsnummer")
    if anbieter_chk and betrag_chk:
        P0 = ph()
        db0 = get_db(); cur0 = db0.cursor()
        try:
            if rechnr_chk:
                cur0.execute(f"SELECT id, belegart FROM belege WHERE rechnungsnummer={P0} AND anbieter={P0} AND betrag_brutto={P0}",
                             (rechnr_chk, anbieter_chk, float(betrag_chk)))
            else:
                bd_parsed = None
                try:
                    from datetime import datetime as _dtt2
                    bd_parsed = _dtt2.strptime(str(beleg_dat_chk).strip(), "%d.%m.%Y").date()
                except: pass
                cur0.execute(f"SELECT id, belegart FROM belege WHERE anbieter={P0} AND betrag_brutto={P0} AND belegdatum={P0}",
                             (anbieter_chk, float(betrag_chk), bd_parsed))
            # Buchungsbestätigung und Rechnung über denselben Betrag sind KEIN Duplikat, sondern zwei Belege
            # derselben Buchung (werden verknüpft) – nur gleichartige Belege kommen als Duplikat in Frage.
            dupe_row = next((x0 for x0 in cur0.fetchall()
                             if _gleiche_beleg_klasse(x0[1] if isinstance(x0, tuple) else x0["belegart"],
                                                      ki_result.get("belegart"))), None)
        except Exception:
            dupe_row = None
        cur0.close(); db0.close()
        if dupe_row and not aenderung_info:
            dupe_id = dupe_row[0] if isinstance(dupe_row, tuple) else dupe_row["id"]
            # Auch hier gilt: gleicher Betrag/Anbieter/Datum heißt bei Reise-Belegen
            # NICHT automatisch "Duplikat" – eine Änderungsmail zeigt fast immer
            # denselben Preis. Erst Inhalt vergleichen, nur bei Gleichheit verwerfen.
            unterschiede2 = []
            anderer_reisender = False
            if ki_result.get("transportart") in _AENDERUNGS_TYPEN:
                try:
                    P1 = ph(); db1 = get_db(); c1 = db1.cursor()
                    c1.execute(f"SELECT ki_json FROM belege WHERE id={P1}", (dupe_id,))
                    rr = c1.fetchone(); c1.close(); db1.close()
                    alt_ki2 = json.loads((rr[0] if isinstance(rr, tuple) else rr["ki_json"]) or "{}")
                    if _gleicher_reisender(alt_ki2.get("reisender"), ki_result.get("reisender")):
                        unterschiede2 = _aenderungen_ermitteln(alt_ki2, ki_result)
                    else:
                        anderer_reisender = True   # gleicher Preis/Tag, aber andere Person
                except Exception:
                    unterschiede2 = []
            if anderer_reisender:
                pass  # eigener Beleg für den anderen Reisenden
            elif unterschiede2:
                aenderung_info = {"zu": dupe_id, "diffs": unterschiede2}
            else:
                return {"beleg_id": dupe_id, "duplikat": True, "status": "duplikat",
                        "zusammenfassung": f"Duplikat – bereits vorhanden als Beleg #{dupe_id}, nicht erneut angelegt."}

    # Zusammenfassung aus KI-Ergebnis
    if "fehler" not in ki_result:
        typ = ki_result.get("dokumenttyp", "Sonstiges")
        vendor = ki_result.get("vendor", "")
        betrag = ki_result.get("betrag")
        waehrung = ki_result.get("waehrung", "EUR")
        zusammenfassung = f"{typ}: {vendor} – {betrag} {waehrung}" if betrag else f"{typ}: {vendor}"
    else:
        zusammenfassung = f"Fehler: {ki_result.get('fehler','')}"

    # Analyse-PDF erstellen
    analyse_text = f"KI-Analyse: {dateiname}\n\n{zusammenfassung}\n\n" + ki_json_str
    analyse_pdf = text_zu_pdf(analyse_text, f"Analyse: {dateiname}")

    # 5. S3 Upload
    prefix = f"belege/{reise_code or 'unzugeordnet'}/{beleg_id_temp}"
    s3_original = s3_upload(f"{prefix}/original.pdf", original_pdf)
    s3_anon     = s3_upload(f"{prefix}/anon.pdf", anon_pdf)
    s3_analyse  = s3_upload(f"{prefix}/analyse.pdf", analyse_pdf)

    # eRechnung-XML unverändert (Byte-für-Byte identisch) zusätzlich sichern –
    # wird NIE angefasst/umgeschrieben, nur gelesen und archiviert.
    s3_erechnung_xml = None
    if erechnung_info["ist_erechnung"] and erechnung_info["xml_bytes"]:
        s3_erechnung_xml = s3_upload(f"{prefix}/erechnung.xml", erechnung_info["xml_bytes"],
                                      content_type="application/xml")

    # 6. DB-Eintrag
    def pd(key):
        v = ki_result.get(key)
        if not v: return None
        try:
            from datetime import datetime as _dtt
            return _dtt.strptime(str(v).strip(), "%d.%m.%Y").date()
        except: return None

    def pn(key):
        v = ki_result.get(key)
        try: return float(v) if v is not None else None
        except: return None

    # Bei Hotels die Adresse ECHT geokodieren (Nominatim), statt nur die
    # KI-Schätzung "aus dem Kopf" zu übernehmen – siehe ausführlicher
    # Kommentar in beleg_neu_analysieren() weiter oben im File.
    if ki_result.get("transportart") == "Hotel" and ki_result.get("hotel_adresse"):
        geo = adresse_geokodieren(ki_result["hotel_adresse"])
        if geo:
            ki_result["hotel_lat"], ki_result["hotel_lon"] = geo[0], geo[1]

    # Bei allen Flug-/Bahnsegmenten die geschätzten Koordinaten durch die
    # exakten Flughafen-Koordinaten ersetzen (siehe Funktionskommentar oben).
    _segment_koordinaten_praezisieren(ki_result)

    pflicht_ok = bool(ki_result.get("pflichtfelder_ok", False))
    fehlend_str = json.dumps(ki_result.get("fehlende_pflichtfelder", []), ensure_ascii=False)
    status = "ok" if pflicht_ok else "fehlerhaft"
    zusammenfassung = (f"{ki_result.get('transportart','?')}: "
                       f"{ki_result.get('anbieter','?')} – "
                       f"{ki_result.get('betrag_brutto','?')} "
                       f"{ki_result.get('waehrung','EUR')}")
    if ist_a1:
        zusammenfassung = mod_a1.zusammenfassung(ki_result)

    # Von der KI geschätzter Euro-Gegenwert bei Fremdwährungsbelegen – nur als
    # Arbeitsgrundlage, der Organisator kann ihn später anhand der echten
    # Kreditkartenabrechnung überschreiben (betrag_eur_final, siehe main.py).
    waehrung_ki = ki_result.get("waehrung", "EUR") or "EUR"
    betrag_eur_geschaetzt = pn("betrag_eur_geschaetzt") if waehrung_ki != "EUR" else None
    kurs_geschaetzt = pn("kurs_geschaetzt") if waehrung_ki != "EUR" else None

    P = ph()
    db = get_db(); cur = db.cursor()
    sql = f"""INSERT INTO belege
        (reise_code, dateiname, s3_original, s3_anon, s3_analyse,
         rohtext, anon_text, ki_json,
         pflichtfelder_ok, fehlende_felder,
         belegdatum, belegart, transportart, transportart_freitext,
         anbieter, rechnungsnummer, buchungscode, reisender, land_beleg,
         betrag_brutto, betrag_netto, betrag_mwst, waehrung, zahlungsart,
         event_datum_von, event_datum_bis, event_zeit, event_ort_von, event_ort_bis,
         hotel_name, hotel_adresse, hotel_lat, hotel_lon, hotel_checkin_datum, hotel_checkin_zeit,
         hotel_checkout_datum, hotel_checkout_zeit, hotel_naechte,
         tanken_kraftstoff, tanken_menge, tanken_einheit,
         tanken_preis_einheit, tanken_tankstelle, tanken_kennzeichen,
         status, ist_erechnung, erechnung_format, s3_erechnung_xml,
         betrag_eur, kurs_eur, kurs_datum, kurs_quelle)
        VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P})"""

    vals = (
        reise_code, dateiname, s3_original, s3_anon, s3_analyse,
        rohtext[:50000] or None, anon_text[:50000] or None,
        ki_json_str, pflicht_ok, fehlend_str,
        pd("belegdatum"), ki_result.get("belegart"),
        ki_result.get("transportart"), ki_result.get("transportart_freitext"),
        ki_result.get("anbieter"), ki_result.get("rechnungsnummer"),
        ki_result.get("buchungscode"), ki_result.get("reisender"),
        ki_result.get("land_beleg"),
        pn("betrag_brutto"), pn("betrag_netto"), pn("betrag_mwst"),
        waehrung_ki, ki_result.get("zahlungsart"),
        pd("event_datum_von"), pd("event_datum_bis"), ki_result.get("event_zeit"),
        ki_result.get("event_ort_von"), ki_result.get("event_ort_bis"),
        ki_result.get("hotel_name"), ki_result.get("hotel_adresse"),
        pn("hotel_lat"), pn("hotel_lon"), pd("hotel_checkin_datum"),
        ki_result.get("hotel_checkin_zeit"), pd("hotel_checkout_datum"),
        ki_result.get("hotel_checkout_zeit"),
        ki_result.get("hotel_naechte"),
        ki_result.get("tanken_kraftstoff"), pn("tanken_menge"),
        ki_result.get("tanken_einheit"), pn("tanken_preis_pro_einheit"),
        ki_result.get("tanken_tankstelle"), ki_result.get("tanken_kennzeichen"),
        status, erechnung_info["ist_erechnung"], erechnung_info["format"], s3_erechnung_xml,
        betrag_eur_geschaetzt, kurs_je_einheit(kurs_geschaetzt, pn("betrag_brutto"), betrag_eur_geschaetzt),
        pd("belegdatum") if betrag_eur_geschaetzt else None,
        "KI-geschätzt" if betrag_eur_geschaetzt else None)

    if is_postgres():
        cur.execute(sql + " RETURNING id", vals)
        beleg_id = cur.fetchone()[0]
    else:
        cur.execute(sql, vals)
        beleg_id = cur.lastrowid

    if aenderung_info:
        # Änderungs-Beleg: Segmente/Beträge/Hoteldaten NICHT in die Reise
        # übernehmen (sonst doppelte Flüge in Reiseplan, Karte, Kosten und VMA).
        # Die neuen Werte liegen im Diff und werden erst nach Klärung durch
        # den Organisator in den Original-Beleg übernommen.
        ki_mod = dict(ki_result)
        ki_mod["segmente_aenderung"] = ki_mod.pop("segmente", None) or []
        P_a = ph()
        cur.execute(f"""UPDATE belege SET aenderung_zu_beleg_id={P_a}, aenderung_diff_json={P_a},
                aenderung_status='offen', ki_json={P_a}, betrag_brutto=NULL,
                event_datum_von=NULL, event_datum_bis=NULL,
                hotel_checkin_datum=NULL, hotel_checkout_datum=NULL
                WHERE id={P_a}""",
            (aenderung_info["zu"], json.dumps(aenderung_info["diffs"], ensure_ascii=False),
             json.dumps(ki_mod, ensure_ascii=False), beleg_id))

    db.commit(); cur.close(); db.close()

    if ist_a1 and a1_kuerzel:
        try:
            mod_a1.a1_zuordnen(beleg_id, a1_kuerzel)
        except Exception as ex:
            print(f"[A1] Zuordnung zu {a1_kuerzel} nicht gespeichert (Neustart/Init nötig?): {ex}")

    if reise_code and not aenderung_info and not ist_a1:
        vma_auto_aktualisieren(reise_code)     # neuer Hotel-/Flugbeleg: Land und Satz der Tage aktualisieren

    # Gleicher Buchungscode in derselben Reise: Buchungsbestätigung und Rechnung/Quittung
    # automatisch zu einer Gruppe verknüpfen (Änderungs-Belege bleiben außen vor).
    verknuepft = 0
    if not aenderung_info and not ist_a1:
        try:
            verknuepft = auto_verknuepfen(beleg_id)
        except Exception:
            verknuepft = 0
        if verknuepft:
            zusammenfassung += f" · automatisch mit {verknuepft} Beleg(en) derselben Buchung verknüpft"
    return {"beleg_id": beleg_id, "zusammenfassung": zusammenfassung,
            "ki": ki_result, "pflichtfelder_ok": pflicht_ok, "verknuepft": verknuepft,
            "aenderung_zu": aenderung_info["zu"] if aenderung_info else None}



# ── Beleg hochladen (Web) ──────────────────────────────────────────────────────
