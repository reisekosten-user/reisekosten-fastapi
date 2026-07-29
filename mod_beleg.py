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
S3_ENDPOINT  = os.getenv("S3_ENDPOINT", "")
S3_BUCKET    = os.getenv("S3_BUCKET", "")
S3_ACCESS_KEY= os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY= os.getenv("S3_SECRET_KEY", "")
IMAP_USER    = os.getenv("IMAP_USER", "")

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_anon import anonymisieren

def get_s3():
    """S3/Hetzner Object Storage Client."""
    import boto3
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY)

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

def bild_zu_pdf(bild_bytes: bytes, dateiname: str = "bild") -> bytes:
    """Konvertiert JPG/PNG zu PDF mit Pillow + ReportLab."""
    from PIL import Image
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Image as RLImage
    img = Image.open(io.BytesIO(bild_bytes))
    # EXIF-Rotation korrigieren
    try:
        from PIL import ImageOps
        img = ImageOps.exif_transpose(img)
    except: pass
    # Zu RGB konvertieren falls nötig
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    # Bild als JPEG in Buffer speichern
    img_buf = io.BytesIO()
    img.save(img_buf, format="JPEG", quality=95)
    img_buf.seek(0)
    # PDF erstellen
    pdf_buf = io.BytesIO()
    from reportlab.lib.units import mm
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    w_pt, h_pt = A4
    img_w, img_h = img.size
    # Skalieren auf A4 mit Rand
    rand = 20 * mm
    max_w = w_pt - 2 * rand
    max_h = h_pt - 2 * rand
    scale = min(max_w / img_w, max_h / img_h)
    draw_w = img_w * scale
    draw_h = img_h * scale
    x = (w_pt - draw_w) / 2
    y = (h_pt - draw_h) / 2
    c_pdf = canvas.Canvas(pdf_buf, pagesize=A4)
    c_pdf.drawImage(ImageReader(img_buf), x, y, draw_w, draw_h)
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

def pdf_text_lesen(pdf_bytes: bytes) -> str:
    """Liest Text aus PDF mit pypdf."""
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join(p.extract_text() or "" for p in reader.pages).strip()
    except: return ""


async def gpt_analyse_bild(bild_bytes: bytes, content_type: str,
                            dateiname: str = "") -> dict:
    """
    Schickt ein Bild direkt an GPT-4o Vision für OCR + Analyse.
    Für Fotos von Belegen (Tankquittungen, Kassenzettel etc.)
    """
    if not OPENAI_KEY:
        return {"fehler": "OPENAI_API_KEY nicht gesetzt",
                "pflichtfelder_ok": False,
                "fehlende_pflichtfelder": ["OPENAI_API_KEY fehlt"]}

    # Bild optimieren: upscale wenn zu klein, HEIC konvertieren
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
        bild_bytes = buf.getvalue()
        content_type = "image/jpeg"
    except Exception as img_e:
        print(f"[Bild-Optimierung] {img_e}")
        if content_type in ("image/heic", "image/heif"):
            content_type = "image/jpeg"

    b64 = base64.b64encode(bild_bytes).decode()

    prompt = """Analysiere dieses Foto eines Reisebelegs und fülle ALLE erkennbaren Felder aus.
Antworte NUR mit einem validen JSON-Objekt – kein Text davor oder danach.
Nicht erkennbare Felder = null.

Pflichtfelder: belegdatum, transportart, anbieter, betrag_brutto, waehrung, event_datum_von
Setze pflichtfelder_ok=false wenn ein Pflichtfeld fehlt.

{
  "belegdatum": "DD.MM.YYYY",
  "belegart": "Rechnung|Quittung|Sonstiges",
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
  "waehrung": "EUR",
  "zahlungsart": "Kreditkarte|Bar|Ueberweisung|PayPal|Unbekannt (PayPal erkennbar an 'PayPal' im Text, z.B. 'Diese Zahlung wurde ueber PayPal getaetigt' – PayPal gilt als eigene Kategorie, auch wenn im Hintergrund eine Kreditkarte hinterlegt ist; sonst Kreditkarte erkennbar an Visa/Mastercard/Amex, maskierter Kartennummer wie xxxx1234, 'bar bezahlt'/cash, oder Ueberweisungshinweisen)",
  "event_datum_von": "DD.MM.YYYY",
  "event_datum_bis": "DD.MM.YYYY",
  "event_zeit": "HH:MM (Uhrzeit auf dem Beleg, z.B. bei Tankquittung, Parkschein, Mautbeleg)",
  "event_ort_von": "Ort",
  "event_ort_bis": null,
  "tanken_kraftstoff": "Benzin|Diesel|AdBlue|Super|SuperPlus|Elektro",
  "tanken_menge": 45.3,
  "tanken_einheit": "Liter|kWh",
  "tanken_preis_pro_einheit": 1.789,
  "tanken_tankstelle": "Name und Ort",
  "tanken_kennzeichen": "Kennzeichen falls sichtbar",
  "pflichtfelder_ok": true,
  "fehlende_pflichtfelder": []
}"""

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {OPENAI_KEY}",
                         "Content-Type": "application/json"},
                json={"model": OPENAI_MODEL,
                      "messages": [{"role": "user", "content": [
                          {"type": "text", "text": prompt},
                          {"type": "image_url", "image_url": {
                              "url": f"data:{content_type};base64,{b64}",
                              "detail": "high"}}
                      ]}],
                      "max_tokens": 1500,
                      "temperature": 0.0})

            if resp.status_code != 200:
                return {"fehler": f"HTTP {resp.status_code}: {resp.text[:200]}",
                        "pflichtfelder_ok": False,
                        "fehlende_pflichtfelder": [f"HTTP {resp.status_code}: {resp.text[:200]}"]}

            raw = resp.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                result = json.loads(m.group(0))
                pflicht = ["belegdatum","transportart","anbieter",
                           "betrag_brutto","waehrung","event_datum_von","zahlungsart"]
                if result.get("belegart") == "Buchungsbestaetigung":
                    pflicht = [f for f in pflicht if f not in ("betrag_brutto","waehrung","zahlungsart")]
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

    prompt = """Analysiere diesen Reisebeleg und fülle ALLE erkennbaren Felder aus.
Antworte NUR mit einem validen JSON-Objekt – kein Text davor oder danach.
Nicht erkennbare Felder = null.

Pflichtfelder: belegdatum, transportart, anbieter, betrag_brutto, waehrung, event_datum_von
Setze pflichtfelder_ok=false und liste fehlende_pflichtfelder wenn ein Pflichtfeld null ist.

JSON-Format:
{
  "belegdatum": "DD.MM.YYYY",
  "belegart": "Rechnung|Buchungsbestaetigung|Quittung|Sonstiges",
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
  "waehrung": "EUR",
  "zahlungsart": "Kreditkarte|Bar|Ueberweisung|PayPal|Unbekannt (PayPal erkennbar an 'PayPal' im Text, z.B. 'Diese Zahlung wurde ueber PayPal getaetigt' – PayPal gilt als eigene Kategorie, auch wenn im Hintergrund eine Kreditkarte hinterlegt ist; sonst Kreditkarte erkennbar an Visa/Mastercard/Amex, maskierter Kartennummer wie xxxx1234, 'bar bezahlt'/cash, oder Ueberweisungshinweisen; bei Reisebuero-Buchungen meist Ueberweisung)",
  "event_datum_von": "DD.MM.YYYY",
  "event_datum_bis": "DD.MM.YYYY",
  "event_zeit": "HH:MM (Uhrzeit auf dem Beleg, z.B. bei Tankquittung, Parkschein, Mautbeleg – NICHT bei Flug/Bahn/Hotel, dafuer gibt es eigene Zeitfelder)",
  "event_ort_von": "Stadtname",
  "event_ort_bis": "Stadtname",
  "hotel_name": "Hotelname",
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
      "abreise_terminal": "z.B. Terminal 1 (falls angegeben, sonst null)",
      "ankunft_datum": "DD.MM.YYYY",
      "ankunft_zeit": "HH:MM",
      "ankunft_zeitzone": "MEZ|UTC|EST|CR|GMT",
      "ankunft_terminal": "z.B. Terminal 2 (falls angegeben, sonst null)",
      "von_ort": "Stadtname z.B. Frankfurt",
      "von_iata": "FRA",
      "nach_ort": "Stadtname z.B. Lyon",
      "nach_iata": "LYS",
      "transport_name": "Lufthansa|Swiss|ITA Airways|DB",
      "transport_nummer": "LH3463|AZ123|ICE123",
      "klasse": "Economy|Business|1.Klasse",
      "hinweis": "z.B. operated by Edelweiss"
    }
  ],
  "pflichtfelder_ok": true,
  "fehlende_pflichtfelder": []
}

--- BELEGTEXT ---
""" + rohtext[:8000]

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {OPENAI_KEY}",
                         "Content-Type": "application/json"},
                json={"model": OPENAI_MODEL,
                      "messages": [{"role": "user",
                                    "content": prompt}],
                      "max_tokens": 2000,
                      "temperature": 0.0})

            if resp.status_code != 200:
                return {"fehler": f"HTTP {resp.status_code}: {resp.text[:300]}",
                        "pflichtfelder_ok": False,
                        "fehlende_pflichtfelder": [f"HTTP {resp.status_code}: {resp.text[:200]}"]}

            raw = resp.json()["choices"][0]["message"]["content"].strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                result = json.loads(m.group(0))
                pflicht = ["belegdatum","transportart","anbieter",
                           "betrag_brutto","waehrung","event_datum_von","zahlungsart"]
                if result.get("belegart") == "Buchungsbestaetigung":
                    pflicht = [f for f in pflicht if f not in ("betrag_brutto","waehrung","zahlungsart")]
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


async def beleg_neu_analysieren(bid: int) -> dict:
    """
    Führt die KI-Analyse für einen bereits gespeicherten Beleg erneut aus
    (z.B. um neue Felder wie event_zeit nachträglich zu befüllen) und
    aktualisiert die Analyse-Felder in der DB. Nutzt den gespeicherten
    Rohtext – funktioniert nicht für reine Bild-Belege ohne Text.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT rohtext FROM belege WHERE id={P}", (bid,))
    row = cur.fetchone()
    if not row:
        cur.close(); db.close()
        return {"fehler": "Beleg nicht gefunden"}
    rohtext = row[0] if isinstance(row, tuple) else row["rohtext"]
    cur.close(); db.close()

    if not rohtext or rohtext.strip().startswith("{"):
        return {"fehler": "Kein Text-Rohtext vorhanden (Bild-Beleg) – Neuanalyse nicht möglich"}

    ki_result = await gpt_analyse(rohtext, "")
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

    ki_json_str = json.dumps(ki_result, ensure_ascii=False)
    pflicht_ok = bool(ki_result.get("pflichtfelder_ok", False))
    fehlend_str = json.dumps(ki_result.get("fehlende_pflichtfelder", []), ensure_ascii=False)
    status = "ok" if pflicht_ok else "fehlerhaft"

    db = get_db(); cur = db.cursor()
    cur.execute(f"""UPDATE belege SET
        ki_json={P}, pflichtfelder_ok={P}, fehlende_felder={P},
        belegdatum={P}, belegart={P}, transportart={P}, transportart_freitext={P},
        anbieter={P}, rechnungsnummer={P}, buchungscode={P}, reisender={P}, land_beleg={P},
        betrag_brutto={P}, betrag_netto={P}, betrag_mwst={P}, waehrung={P}, zahlungsart={P},
        event_datum_von={P}, event_datum_bis={P}, event_zeit={P},
        event_ort_von={P}, event_ort_bis={P},
        hotel_name={P}, hotel_checkin_datum={P}, hotel_checkin_zeit={P},
        hotel_checkout_datum={P}, hotel_checkout_zeit={P}, hotel_naechte={P},
        tanken_kraftstoff={P}, tanken_menge={P}, tanken_einheit={P},
        tanken_preis_einheit={P}, tanken_tankstelle={P}, tanken_kennzeichen={P},
        status={P}
        WHERE id={P}""", (
        ki_json_str, pflicht_ok, fehlend_str,
        pd("belegdatum"), ki_result.get("belegart"),
        ki_result.get("transportart"), ki_result.get("transportart_freitext"),
        ki_result.get("anbieter"), ki_result.get("rechnungsnummer"),
        ki_result.get("buchungscode"), ki_result.get("reisender"),
        ki_result.get("land_beleg"),
        pn("betrag_brutto"), pn("betrag_netto"), pn("betrag_mwst"),
        ki_result.get("waehrung","EUR"), ki_result.get("zahlungsart"),
        pd("event_datum_von"), pd("event_datum_bis"), ki_result.get("event_zeit"),
        ki_result.get("event_ort_von"), ki_result.get("event_ort_bis"),
        ki_result.get("hotel_name"), pd("hotel_checkin_datum"),
        ki_result.get("hotel_checkin_zeit"), pd("hotel_checkout_datum"),
        ki_result.get("hotel_checkout_zeit"),
        ki_result.get("hotel_naechte"),
        ki_result.get("tanken_kraftstoff"), pn("tanken_menge"),
        ki_result.get("tanken_einheit"), pn("tanken_preis_pro_einheit"),
        ki_result.get("tanken_tankstelle"), ki_result.get("tanken_kennzeichen"),
        status, bid))
    db.commit(); cur.close(); db.close()
    return {"ok": True}


async def beleg_neu_anonymisieren(bid: int) -> dict:
    """
    Führt die Anonymisierung für einen bereits gespeicherten Beleg erneut aus
    (z.B. wenn beim ersten Durchlauf die Mitarbeiterliste leer war) und
    überschreibt anon_text + anon.pdf in DB und S3.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
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


async def beleg_verarbeiten(
    datei_bytes: bytes,
    dateiname: str,
    reise_code: str | None,
    content_type: str = "application/pdf"
) -> dict:
    """
    Komplette Beleg-Pipeline:
    1. Zu PDF konvertieren
    2. Anonymisieren
    3. GPT-4o Analyse
    4. S3 speichern
    5. DB-Eintrag
    Gibt beleg_id zurück.
    """
    import uuid
    beleg_id_temp = str(uuid.uuid4())[:8]

    # 1. Zu PDF konvertieren
    if content_type in ("image/jpeg", "image/jpg", "image/png", "image/heic"):
        original_pdf = bild_zu_pdf(datei_bytes, dateiname)
    elif content_type == "application/pdf":
        original_pdf = datei_bytes
    else:
        # Text/Mail → PDF
        text = datei_bytes.decode(errors="ignore")
        original_pdf = text_zu_pdf(text, dateiname)

    # 2. Text aus PDF lesen
    rohtext = pdf_text_lesen(original_pdf)

    # Bild ohne extrahierbaren Text → GPT-4o Vision direkt
    is_image = content_type in ("image/jpeg","image/jpg","image/png",
                                "image/heic","image/heif","image/webp")
    if is_image and len(rohtext.strip()) < 20:
        ki_result = await gpt_analyse_bild(datei_bytes, content_type, dateiname)
        rohtext = json.dumps(ki_result, ensure_ascii=False, indent=2)
    else:
        ki_result = await gpt_analyse(rohtext, dateiname)

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
                cur0.execute(f"SELECT id FROM belege WHERE rechnungsnummer={P0} AND anbieter={P0} AND betrag_brutto={P0}",
                             (rechnr_chk, anbieter_chk, float(betrag_chk)))
            else:
                bd_parsed = None
                try:
                    from datetime import datetime as _dtt2
                    bd_parsed = _dtt2.strptime(str(beleg_dat_chk).strip(), "%d.%m.%Y").date()
                except: pass
                cur0.execute(f"SELECT id FROM belege WHERE anbieter={P0} AND betrag_brutto={P0} AND belegdatum={P0}",
                             (anbieter_chk, float(betrag_chk), bd_parsed))
            dupe_row = cur0.fetchone()
        except Exception:
            dupe_row = None
        cur0.close(); db0.close()
        if dupe_row:
            dupe_id = dupe_row[0] if isinstance(dupe_row, tuple) else dupe_row["id"]
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

    pflicht_ok = bool(ki_result.get("pflichtfelder_ok", False))
    fehlend_str = json.dumps(ki_result.get("fehlende_pflichtfelder", []), ensure_ascii=False)
    status = "ok" if pflicht_ok else "fehlerhaft"
    zusammenfassung = (f"{ki_result.get('transportart','?')}: "
                       f"{ki_result.get('anbieter','?')} – "
                       f"{ki_result.get('betrag_brutto','?')} "
                       f"{ki_result.get('waehrung','EUR')}")

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
         hotel_name, hotel_checkin_datum, hotel_checkin_zeit,
         hotel_checkout_datum, hotel_checkout_zeit, hotel_naechte,
         tanken_kraftstoff, tanken_menge, tanken_einheit,
         tanken_preis_einheit, tanken_tankstelle, tanken_kennzeichen,
         status)
        VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},
                {P},{P},{P},{P},{P},{P},{P})"""

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
        ki_result.get("waehrung","EUR"), ki_result.get("zahlungsart"),
        pd("event_datum_von"), pd("event_datum_bis"), ki_result.get("event_zeit"),
        ki_result.get("event_ort_von"), ki_result.get("event_ort_bis"),
        ki_result.get("hotel_name"), pd("hotel_checkin_datum"),
        ki_result.get("hotel_checkin_zeit"), pd("hotel_checkout_datum"),
        ki_result.get("hotel_checkout_zeit"),
        ki_result.get("hotel_naechte"),
        ki_result.get("tanken_kraftstoff"), pn("tanken_menge"),
        ki_result.get("tanken_einheit"), pn("tanken_preis_pro_einheit"),
        ki_result.get("tanken_tankstelle"), ki_result.get("tanken_kennzeichen"),
        status)

    if is_postgres():
        cur.execute(sql + " RETURNING id", vals)
        beleg_id = cur.fetchone()[0]
    else:
        cur.execute(sql, vals)
        beleg_id = cur.lastrowid

    db.commit(); cur.close(); db.close()
    return {"beleg_id": beleg_id, "zusammenfassung": zusammenfassung,
            "ki": ki_result, "pflichtfelder_ok": pflicht_ok}



# ── Beleg hochladen (Web) ──────────────────────────────────────────────────────
