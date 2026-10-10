"""
mod_bewirtung.py – Bewirtungsbeleg (Eigenbeleg zu Anlass und Teilnehmern) mit
elektronischer Bestätigung durch den Mitarbeiter.

Hintergrund (BMF-Schreiben vom 19.11.2025, in der Praxis auch GoBD): Der Nachweis
einer Bewirtung muss schriftlich, zeitnah und vollständig sein: Ort, Tag, Teilnehmer,
Anlass, Höhe. Bei Bewirtung in der Gaststätte kommt die echte Rechnung/der Kassenbeleg
dazu. Ein digitaler Eigenbeleg ist zulässig, wenn er mit der Rechnung eindeutig
verknüpft, elektronisch genehmigt oder unterschrieben, zeitnah erstellt, mit Zeitstempel
protokolliert und nachträglich nicht unbemerkt änderbar ist.

Umsetzung hier:
  - Entwurf -> Bestätigung (Haken, vollständiger Name, optionale Unterschrift) -> gesperrt.
  - Bei der Bestätigung: Zeitstempel (UTC), IP, Browserkennung und SHA-256-Prüfsumme über
    alle Inhalte werden gespeichert. Die Prüfsumme lässt sich jederzeit neu berechnen.
  - Korrekturen nur als neue Version; die alte Version bleibt samt PDF erhalten.
  - Fester Index (BW-<Reise>-<lfd. Nr.>) verknüpft Eigenbeleg, Rechnungsbeleg und PDF.
Keine KI, kein externer Dienst (außer S3 für die PDF-Ablage).
"""
from __future__ import annotations
import base64, hashlib, io, json, re
from datetime import date, datetime, timezone

from mod_db import ph, is_postgres

try:
    from zoneinfo import ZoneInfo
    BERLIN = ZoneInfo("Europe/Berlin")
except Exception:          # pragma: no cover
    BERLIN = None

SPALTEN = ["id", "reise_code", "kuerzel", "nummer", "version", "ersetzt_id", "status",
           "hauptbeleg_id", "eigenbeleg", "eigenbeleg_begruendung", "datum", "ort", "anlass",
           "teilnehmer_json", "betrag", "trinkgeld", "waehrung", "bestaetigt_am",
           "bestaetigt_name", "bestaetigt_ip", "bestaetigt_agent", "signatur_png",
           "pruefsumme", "beleg_id", "s3_pdf"]


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────
def _falten(s: str | None) -> str:
    s = (s or "").lower()
    for a, b in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        s = s.replace(a, b)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def namen_gleich(a: str | None, b: str | None) -> bool:
    """Gleicher Name unabhängig von Reihenfolge, Groß-/Kleinschreibung und Umlauten."""
    return bool(_falten(a)) and set(_falten(a).split()) == set(_falten(b).split())


def _num(x):
    return float(x) if x is not None else None


def _iso(x) -> str | None:
    return str(x)[:10] if x else None


def jetzt_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lokal_text(iso: str | None) -> str:
    """'2026-10-10T17:36:00Z' -> '10.10.2026 19:36:00 MESZ'"""
    if not iso:
        return "–"
    try:
        d = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if BERLIN:
            d = d.astimezone(BERLIN)
            zone = {"CEST": "MESZ", "CET": "MEZ"}.get(d.tzname(), d.tzname())
        else:
            zone = "UTC"
        return d.strftime("%d.%m.%Y %H:%M:%S") + " " + zone
    except Exception:
        return iso


def _row(r) -> dict:
    d = {k: (r[k] if hasattr(r, "keys") else r[i]) for i, k in enumerate(SPALTEN)}
    d["betrag"] = _num(d["betrag"])
    d["trinkgeld"] = _num(d["trinkgeld"])
    d["datum"] = _iso(d["datum"])
    d["eigenbeleg"] = bool(d["eigenbeleg"])
    try:
        d["teilnehmer"] = json.loads(d["teilnehmer_json"] or "[]")
    except Exception:
        d["teilnehmer"] = []
    return d


# ── Eingaben prüfen ───────────────────────────────────────────────────────────
def teilnehmer_aus_formular(namen: list[str], firmen: list[str]) -> list[dict]:
    """Aus den Formularzeilen die Teilnehmerliste bauen (leere Zeilen weglassen)."""
    out = []
    for i, n in enumerate(namen):
        name = (n or "").strip()
        firma = (firmen[i] if i < len(firmen) else "").strip()
        if name:
            out.append({"name": name[:120], "firma": firma[:120]})
    return out


def zahl_parsen(text: str | None) -> float | None:
    t = (text or "").strip().replace(" ", "")
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", ".")
    try:
        v = round(float(t), 2)
    except ValueError:
        return None
    return v if v >= 0 else None


def validieren(d: dict) -> list[str]:
    """Pflichtangaben laut BMF: Ort, Tag, Teilnehmer, Anlass, Höhe (+ Beleg oder Eigenbeleg-Grund)."""
    f = []
    if not d.get("datum"):
        f.append("Bitte das Datum der Bewirtung angeben.")
    else:
        try:
            if date.fromisoformat(d["datum"]) > date.today():
                f.append("Das Datum der Bewirtung liegt in der Zukunft.")
        except ValueError:
            f.append("Das Datum der Bewirtung ist ungültig.")
    if not (d.get("ort") or "").strip():
        f.append("Bitte Ort bzw. Restaurant angeben.")
    if len((d.get("anlass") or "").strip()) < 5:
        f.append("Bitte den Anlass der Bewirtung angeben (z. B. Projektbesprechung mit Kunde X).")
    tn = d.get("teilnehmer") or []
    if len(tn) < 2:
        f.append("Bitte mindestens zwei Teilnehmer mit Namen angeben (der Bewirtende gehört dazu).")
    if d.get("betrag") is None:
        f.append("Bitte den Betrag angeben.")
    if not d.get("hauptbeleg_id") and not d.get("eigenbeleg"):
        f.append("Bitte einen Beleg wählen, einen neuen hochladen oder 'Eigenbeleg' auswählen.")
    if d.get("eigenbeleg") and len((d.get("eigenbeleg_begruendung") or "").strip()) < 10:
        f.append("Ohne Originalbeleg bitte die Begründung angeben (warum gibt es keinen Beleg?).")
    return f


# ── Prüfsumme ─────────────────────────────────────────────────────────────────
def pruefsumme_berechnen(d: dict) -> str:
    """SHA-256 über alle inhaltlich relevanten Felder (kanonisches JSON)."""
    sig = d.get("signatur_png") or ""
    kern = {
        "nummer": d["nummer"], "version": d["version"], "reise_code": d["reise_code"],
        "kuerzel": d["kuerzel"], "hauptbeleg_id": d.get("hauptbeleg_id"),
        "eigenbeleg": bool(d.get("eigenbeleg")),
        "eigenbeleg_begruendung": (d.get("eigenbeleg_begruendung") or "").strip(),
        "datum": _iso(d.get("datum")), "ort": (d.get("ort") or "").strip(),
        "anlass": (d.get("anlass") or "").strip(),
        "teilnehmer": d.get("teilnehmer") or [],
        "betrag": None if d.get("betrag") is None else round(float(d["betrag"]), 2),
        "trinkgeld": None if d.get("trinkgeld") is None else round(float(d["trinkgeld"]), 2),
        "waehrung": d.get("waehrung") or "EUR",
        "bestaetigt_am": d.get("bestaetigt_am"), "bestaetigt_name": d.get("bestaetigt_name"),
        "signatur_sha256": hashlib.sha256(sig.encode()).hexdigest() if sig else None,
    }
    text = json.dumps(kern, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def integritaet_ok(d: dict) -> bool:
    return d.get("status") in ("bestaetigt", "ersetzt") and bool(d.get("pruefsumme")) \
        and pruefsumme_berechnen(d) == d["pruefsumme"]


# ── Datenbank ─────────────────────────────────────────────────────────────────
def laden(db, bid: int) -> dict | None:
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(SPALTEN) + f" FROM bewirtungen WHERE id={ph()}", (bid,))
    r = cur.fetchone()
    cur.close()
    return _row(r) if r else None


def liste_fuer(db, reise_code: str, kuerzel: str) -> list[dict]:
    """Aktuelle Versionen (ohne ersetzte) eines Mitarbeiters für eine Reise."""
    P = ph()
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(SPALTEN) + f" FROM bewirtungen WHERE reise_code={P} "
                f"AND kuerzel={P} AND status != 'ersetzt' ORDER BY id", (reise_code, kuerzel))
    rows = [_row(r) for r in cur.fetchall()]
    cur.close()
    return rows


def versionen(db, nummer: str) -> list[dict]:
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(SPALTEN) + f" FROM bewirtungen WHERE nummer={ph()} "
                "ORDER BY version", (nummer,))
    rows = [_row(r) for r in cur.fetchall()]
    cur.close()
    return rows


def zu_beleg(db, beleg_id: int) -> list[dict]:
    """Bewirtungen, bei denen dieser Beleg der Nebenbeleg ODER der Rechnungsbeleg ist."""
    P = ph()
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(SPALTEN) + f" FROM bewirtungen WHERE (beleg_id={P} "
                f"OR hauptbeleg_id={P}) AND status != 'ersetzt' ORDER BY id", (beleg_id, beleg_id))
    rows = [_row(r) for r in cur.fetchall()]
    cur.close()
    return rows


def nummer_vergeben(db, reise_code: str) -> str:
    """Nächste freie Nummer BW-<Reise>-<nn> (höchste vorhandene + 1)."""
    cur = db.cursor()
    cur.execute(f"SELECT DISTINCT nummer FROM bewirtungen WHERE reise_code={ph()}", (reise_code,))
    hoechste = 0
    for r in cur.fetchall():
        m = re.search(r"-(\d+)$", (r[0] if isinstance(r, tuple) else r["nummer"]) or "")
        if m:
            hoechste = max(hoechste, int(m.group(1)))
    cur.close()
    return f"BW-{reise_code}-{hoechste + 1:02d}"


def belege_zur_auswahl(db, reise_code: str, klarname: str, nummer: str | None = None,
                       zusaetzlich_id: int | None = None) -> list[dict]:
    """Belege der Reise, die als Rechnungsbeleg zur Bewirtung infrage kommen: noch nicht
    einer anderen Bewirtung zugeordnet und von diesem Mitarbeiter (oder ohne Reisenden).
    `nummer`: eigene Bewirtung (ihre Versionen blockieren den Beleg nicht).
    `zusaetzlich_id`: dieser Beleg wird immer angeboten (z. B. gerade hochgeladen)."""
    P = ph()
    cur = db.cursor()
    cur.execute(f"""SELECT id, belegart, transportart, anbieter, betrag_brutto, waehrung, belegdatum,
                    reisender FROM belege
                    WHERE reise_code={P} AND betrag_brutto IS NOT NULL
                    AND COALESCE(belegart,'') != 'Bewirtungsbeleg'
                    AND (aenderung_status IS NULL OR aenderung_status != 'offen')
                    ORDER BY id DESC""", (reise_code,))
    rows = cur.fetchall()
    cur.execute(f"""SELECT hauptbeleg_id FROM bewirtungen WHERE reise_code={P}
                    AND hauptbeleg_id IS NOT NULL AND status != 'ersetzt' AND nummer != {P}""",
                (reise_code, nummer or ""))
    belegt = {(r[0] if isinstance(r, tuple) else r["hauptbeleg_id"]) for r in cur.fetchall()}
    cur.close()
    out = []
    for r in rows:
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        if g("id", 0) in belegt:
            continue
        reisender = g("reisender", 7)
        if (reisender and g("id", 0) != zusaetzlich_id
                and not (set(_falten(reisender).split()) & set(_falten(klarname).split()))):
            continue            # Beleg eines Kollegen
        out.append({"id": g("id", 0), "belegart": g("belegart", 1), "transportart": g("transportart", 2),
                    "anbieter": g("anbieter", 3), "betrag": _num(g("betrag_brutto", 4)),
                    "waehrung": g("waehrung", 5) or "EUR", "datum": _iso(g("belegdatum", 6))})
    # Restaurantbelege zuerst
    out.sort(key=lambda b: 0 if ((b["transportart"] or "") in ("Bewirtung", "Verpflegung")
                                 or (b["belegart"] or "") == "Bewirtung") else 1)
    return out


def beleg_kurz(db, beleg_id: int) -> dict | None:
    P = ph()
    cur = db.cursor()
    cur.execute(f"""SELECT id, anbieter, betrag_brutto, waehrung, belegdatum, zahlungsart,
                    beleg_gruppe_id, s3_original, reise_code FROM belege WHERE id={P}""", (beleg_id,))
    r = cur.fetchone()
    cur.close()
    if not r:
        return None
    g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
    return {"id": g("id", 0), "anbieter": g("anbieter", 1), "betrag": _num(g("betrag_brutto", 2)),
            "waehrung": g("waehrung", 3) or "EUR", "datum": _iso(g("belegdatum", 4)),
            "zahlungsart": g("zahlungsart", 5), "gruppe": g("beleg_gruppe_id", 6),
            "s3_original": g("s3_original", 7), "reise_code": g("reise_code", 8)}


def entwurf_speichern(db, d: dict, bid: int | None = None) -> int:
    """Legt einen Entwurf an oder aktualisiert einen bestehenden Entwurf."""
    P = ph()
    cur = db.cursor()
    werte = (d.get("hauptbeleg_id"), bool(d.get("eigenbeleg")),
             (d.get("eigenbeleg_begruendung") or "").strip() or None,
             d.get("datum"), (d.get("ort") or "").strip(), (d.get("anlass") or "").strip(),
             json.dumps(d.get("teilnehmer") or [], ensure_ascii=False),
             d.get("betrag"), d.get("trinkgeld"), d.get("waehrung") or "EUR")
    if bid:
        cur.execute(f"""UPDATE bewirtungen SET hauptbeleg_id={P}, eigenbeleg={P}, eigenbeleg_begruendung={P},
                        datum={P}, ort={P}, anlass={P}, teilnehmer_json={P}, betrag={P}, trinkgeld={P},
                        waehrung={P} WHERE id={P} AND status='entwurf'""", werte + (bid,))
        neu_id = bid
    else:
        nummer = nummer_vergeben(db, d["reise_code"])
        sql = f"""INSERT INTO bewirtungen (reise_code, kuerzel, nummer, version, status,
                  hauptbeleg_id, eigenbeleg, eigenbeleg_begruendung, datum, ort, anlass,
                  teilnehmer_json, betrag, trinkgeld, waehrung)
                  VALUES ({P},{P},{P},1,'entwurf',{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})"""
        args = (d["reise_code"], d["kuerzel"], nummer) + werte
        if is_postgres():
            cur.execute(sql + " RETURNING id", args)
            neu_id = cur.fetchone()[0]
        else:
            cur.execute(sql, args)
            neu_id = cur.lastrowid
    cur.close()
    return neu_id


def korrektur_starten(db, bid: int) -> int:
    """Kopiert eine bestätigte Bewirtung als neuen Entwurf (Version + 1). Die alte Version
    bleibt unverändert, bis die neue bestätigt wird."""
    alt = laden(db, bid)
    if not alt or alt["status"] != "bestaetigt":
        raise ValueError("Nur bestätigte Bewirtungen können korrigiert werden.")
    cur = db.cursor()
    cur.execute(f"SELECT COUNT(*) FROM bewirtungen WHERE ersetzt_id={ph()} AND status='entwurf'", (bid,))
    if (cur.fetchone() or [0])[0]:
        cur.close()
        raise ValueError("Es gibt bereits einen offenen Korrektur-Entwurf.")
    P = ph()
    sql = f"""INSERT INTO bewirtungen (reise_code, kuerzel, nummer, version, ersetzt_id, status,
              hauptbeleg_id, eigenbeleg, eigenbeleg_begruendung, datum, ort, anlass, teilnehmer_json,
              betrag, trinkgeld, waehrung, beleg_id)
              VALUES ({P},{P},{P},{P},{P},'entwurf',{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})"""
    args = (alt["reise_code"], alt["kuerzel"], alt["nummer"], alt["version"] + 1, bid,
            alt["hauptbeleg_id"], alt["eigenbeleg"], alt["eigenbeleg_begruendung"], alt["datum"],
            alt["ort"], alt["anlass"], json.dumps(alt["teilnehmer"], ensure_ascii=False),
            alt["betrag"], alt["trinkgeld"], alt["waehrung"], alt["beleg_id"])
    if is_postgres():
        cur.execute(sql + " RETURNING id", args)
        neu_id = cur.fetchone()[0]
    else:
        cur.execute(sql, args)
        neu_id = cur.lastrowid
    cur.close()
    return neu_id


def entwurf_loeschen(db, bid: int) -> None:
    cur = db.cursor()
    cur.execute(f"DELETE FROM bewirtungen WHERE id={ph()} AND status='entwurf'", (bid,))
    cur.close()


# ── PDF ───────────────────────────────────────────────────────────────────────
def _esc(s) -> str:
    return (str(s) if s is not None else "–").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _betrag_txt(x, waehrung="EUR") -> str:
    if x is None:
        return "–"
    return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + f" {waehrung}"


betrag_text = _betrag_txt


def datum_de(iso) -> str:
    try:
        return date.fromisoformat(str(iso)[:10]).strftime("%d.%m.%Y")
    except Exception:
        return "–"


def pdf_bauen(d: dict, klarname: str, hauptbeleg: dict | None) -> bytes:
    """Bewirtungsbeleg als PDF (nur Eigenbeleg-Seite; der Originalbeleg liegt als eigener Beleg vor)."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
                                    Image, KeepTogether)

    styles = getSampleStyleSheet()
    klein = ParagraphStyle("klein", parent=styles["Normal"], fontSize=8, leading=10,
                           textColor=colors.HexColor("#475569"))
    mono = ParagraphStyle("mono", parent=styles["Normal"], fontName="Courier", fontSize=7.5, leading=9)
    normal = ParagraphStyle("n", parent=styles["Normal"], fontSize=9.5, leading=12)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11, spaceBefore=10, spaceAfter=4)
    w = d.get("waehrung") or "EUR"
    version_txt = f" (Version {d['version']})" if d.get("version", 1) > 1 else ""

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm,
                            topMargin=16 * mm, bottomMargin=16 * mm,
                            title=f"Bewirtungsbeleg {d['nummer']}", author="Herrhammer Reisekosten")
    story = [Paragraph(f"Bewirtungsbeleg {_esc(d['nummer'])}{version_txt}", styles["Title"]),
             Paragraph("Angaben zur Bewirtung aus geschäftlichem Anlass (Eigenbeleg zum "
                       "Rechnungsbeleg)", klein), Spacer(1, 6)]

    def zeile(k, v):
        return [Paragraph(f"<b>{_esc(k)}</b>", normal), Paragraph(_esc(v), normal)]

    kopf = [zeile("Reise", d["reise_code"]),
            zeile("Bewirtender (Mitarbeiter)", klarname),
            zeile("Tag der Bewirtung", datum_de(d.get("datum"))),
            zeile("Ort / Restaurant", d.get("ort")),
            zeile("Anlass der Bewirtung", d.get("anlass"))]
    t = Table(kopf, colWidths=[52 * mm, 122 * mm])
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                           ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0"))]))
    story += [t, Paragraph("Teilnehmer der Bewirtung", h2)]

    tn = [["Nr.", "Name", "Firma / Funktion"]] + [
        [str(i), Paragraph(_esc(p.get("name")), normal), Paragraph(_esc(p.get("firma") or "–"), normal)]
        for i, p in enumerate(d.get("teilnehmer") or [], start=1)]
    tt = Table(tn, colWidths=[12 * mm, 80 * mm, 82 * mm], repeatRows=1)
    tt.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f1f5f9")),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 9),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0"))]))
    story += [tt, Paragraph("Kosten", h2)]

    brutto = d.get("betrag")
    tg = d.get("trinkgeld")
    kosten = [["Betrag laut Beleg", _betrag_txt(brutto, w)]]
    if tg:
        kosten.append(["zzgl. Trinkgeld (nicht auf dem Beleg)", _betrag_txt(tg, w)])
        kosten.append(["Gesamt", _betrag_txt((brutto or 0) + tg, w)])
    ktab = Table(kosten, colWidths=[110 * mm, 64 * mm])
    ktab.setStyle(TableStyle([("FONTSIZE", (0, 0), (-1, -1), 9.5), ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                              ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0"))]))
    story += [ktab, Paragraph("Zugehöriger Beleg", h2)]

    if d.get("eigenbeleg"):
        story.append(Paragraph(
            "<b>Eigenbeleg ohne Originalbeleg (Ausnahme).</b> Begründung: "
            + _esc(d.get("eigenbeleg_begruendung")), normal))
    elif hauptbeleg:
        story.append(Paragraph(
            f"Rechnungs-/Kassenbeleg: Beleg <b>#{hauptbeleg['id']}</b> – {_esc(hauptbeleg.get('anbieter'))}, "
            f"{datum_de(hauptbeleg.get('datum'))}, {_betrag_txt(hauptbeleg.get('betrag'), hauptbeleg.get('waehrung') or 'EUR')}. "
            f"Der Originalbeleg liegt in der Belegablage und ist über Beleg-Nr. und diesen Index "
            f"<b>{_esc(d['nummer'])}</b> eindeutig zugeordnet.", normal))

    story.append(Paragraph("Elektronische Bestätigung", h2))
    story.append(Paragraph(
        "Ich bestätige, dass die vorstehenden Angaben vollständig und richtig sind und die Bewirtung "
        "geschäftlich veranlasst war.", normal))
    bestaetigt = [
        ["Bestätigt von", _esc(d.get("bestaetigt_name"))],
        ["Zeitpunkt", lokal_text(d.get("bestaetigt_am")) + "  (UTC: " + _esc(d.get("bestaetigt_am")) + ")"],
        ["IP-Adresse", _esc(d.get("bestaetigt_ip") or "–")],
    ]
    btab = Table([[Paragraph(f"<b>{a}</b>", klein), Paragraph(b, klein)] for a, b in bestaetigt],
                 colWidths=[35 * mm, 139 * mm])
    btab.setStyle(TableStyle([("BOTTOMPADDING", (0, 0), (-1, -1), 2), ("TOPPADDING", (0, 0), (-1, -1), 2)]))
    block = [btab]
    sig = d.get("signatur_png")
    if sig:
        try:
            from PIL import Image as PILImage
            raw = base64.b64decode(sig)
            iw, ih = PILImage.open(io.BytesIO(raw)).size
            maxw, maxh = 70 * mm, 26 * mm
            s = min(maxw / iw, maxh / ih)
            block += [Spacer(1, 4), Paragraph("Unterschrift (am Bildschirm gezeichnet):", klein),
                      Image(io.BytesIO(raw), width=iw * s, height=ih * s, hAlign="LEFT")]
        except Exception:
            pass
    block += [Spacer(1, 6), Paragraph("Prüfsumme (SHA-256 über alle Angaben und die Bestätigung):", klein),
              Paragraph(_esc(d.get("pruefsumme")), mono), Spacer(1, 6),
              Paragraph("Dieser Beleg wurde elektronisch erstellt und bestätigt. Nach der Bestätigung sind "
                        "Änderungen nur als neue Version mit Protokoll möglich; frühere Versionen bleiben "
                        "erhalten.", klein)]
    story.append(KeepTogether(block))
    doc.build(story)
    return buf.getvalue()


# ── Bestätigen ────────────────────────────────────────────────────────────────
def signatur_pruefen(data_url: str | None) -> str | None:
    """Nimmt 'data:image/png;base64,...' entgegen, prüft es und gibt reines Base64 zurück."""
    if not data_url:
        return None
    m = re.match(r"^data:image/png;base64,([A-Za-z0-9+/=]+)$", data_url.strip())
    if not m:
        raise ValueError("Die Unterschrift hat ein ungültiges Format.")
    raw = base64.b64decode(m.group(1), validate=True)
    if not raw.startswith(b"\x89PNG\r\n\x1a\n") or len(raw) > 300_000:
        raise ValueError("Die Unterschrift ist ungültig oder zu groß.")
    return m.group(1)


def bestaetigen(db, bid: int, klarname: str, name_eingabe: str, haken: bool,
                signatur_data_url: str | None, ip: str, agent: str) -> dict:
    """
    Bestätigt einen Entwurf: prüft alles, erzeugt PDF und Prüfsumme, legt den Nebenbeleg
    in der Belegtabelle an (bzw. aktualisiert ihn bei einer Korrektur) und verknüpft ihn
    mit dem Rechnungsbeleg. Alles in einer Transaktion (Commit macht der Aufrufer).
    """
    import mod_beleg
    d = laden(db, bid)
    if not d or d["status"] != "entwurf":
        raise ValueError("Diese Bewirtung ist nicht mehr als Entwurf offen.")
    fehler = validieren(d)
    if fehler:
        raise ValueError(" ".join(fehler))
    if not haken:
        raise ValueError("Bitte bestätigen, dass die Angaben vollständig und richtig sind.")
    if not namen_gleich(name_eingabe, klarname):
        raise ValueError(f"Bitte den vollständigen Namen eingeben, so wie er im System steht: {klarname}")
    signatur = signatur_pruefen(signatur_data_url)

    d["bestaetigt_am"] = jetzt_utc_iso()
    d["bestaetigt_name"] = klarname      # Name wie im System (die Eingabe wurde oben verglichen)
    d["bestaetigt_ip"] = (ip or "")[:64]
    d["bestaetigt_agent"] = (agent or "")[:300]
    d["signatur_png"] = signatur
    d["pruefsumme"] = pruefsumme_berechnen(d)

    haupt = beleg_kurz(db, d["hauptbeleg_id"]) if d["hauptbeleg_id"] else None
    if d["hauptbeleg_id"] and (not haupt or haupt["reise_code"] != d["reise_code"]):
        raise ValueError("Der gewählte Beleg gehört nicht zu dieser Reise.")

    pdf = pdf_bauen(d, klarname, haupt)
    s3_key = f"bewirtung/{d['reise_code']}/{d['nummer']}_v{d['version']}.pdf"
    mod_beleg.s3_upload(s3_key, pdf)

    P = ph()
    cur = db.cursor()
    dateiname = f"Bewirtungsbeleg_{d['nummer']}.pdf"
    betrag_beleg = d["betrag"] if d["eigenbeleg"] else None    # Nebenbeleg: Kosten stehen am Rechnungsbeleg
    zahlungsart = (haupt or {}).get("zahlungsart")
    if d.get("ersetzt_id"):
        alt = laden(db, d["ersetzt_id"])
        beleg_id = (alt or {}).get("beleg_id") or d.get("beleg_id")
    else:
        beleg_id = None
    if beleg_id:
        cur.execute(f"""UPDATE belege SET dateiname={P}, s3_original={P}, belegdatum={P}, anbieter={P},
                        betrag_brutto={P}, waehrung={P}, geprueft=FALSE, geprueft_von=NULL, geprueft_am=NULL
                        WHERE id={P}""".replace("FALSE", "FALSE" if is_postgres() else "0"),
                    (dateiname, s3_key, d["datum"], d["ort"], betrag_beleg, d["waehrung"], beleg_id))
        if haupt:
            _verknuepfen(db, cur, beleg_id, haupt["id"])
    else:
        sql = f"""INSERT INTO belege (reise_code, dateiname, s3_original, ki_json, pflichtfelder_ok,
                  fehlende_felder, belegdatum, belegart, transportart, anbieter, reisender,
                  betrag_brutto, waehrung, zahlungsart, status)
                  VALUES ({P},{P},{P},{P},{P},{P},{P},'Bewirtungsbeleg','Bewirtung',{P},{P},{P},{P},{P},'ok')"""
        args = (d["reise_code"], dateiname, s3_key, "{}", True, "[]", d["datum"], d["ort"], klarname,
                betrag_beleg, d["waehrung"], zahlungsart)
        if is_postgres():
            cur.execute(sql + " RETURNING id", args)
            beleg_id = cur.fetchone()[0]
        else:
            cur.execute(sql, args)
            beleg_id = cur.lastrowid
        if haupt:
            _verknuepfen(db, cur, beleg_id, haupt["id"])

    cur.execute(f"""UPDATE bewirtungen SET status='bestaetigt', bestaetigt_am={P}, bestaetigt_name={P},
                    bestaetigt_ip={P}, bestaetigt_agent={P}, signatur_png={P}, pruefsumme={P},
                    beleg_id={P}, s3_pdf={P} WHERE id={P}""",
                (d["bestaetigt_am"], d["bestaetigt_name"], d["bestaetigt_ip"], d["bestaetigt_agent"],
                 signatur, d["pruefsumme"], beleg_id, s3_key, bid))
    if d.get("ersetzt_id"):
        cur.execute(f"UPDATE bewirtungen SET status='ersetzt' WHERE id={P}", (d["ersetzt_id"],))
    cur.close()
    return {"beleg_id": beleg_id, "pruefsumme": d["pruefsumme"], "nummer": d["nummer"]}


def _verknuepfen(db, cur, beleg_a: int, beleg_b: int) -> None:
    """Fasst zwei Belege zu einer Gruppe zusammen (gleiche Logik wie /beleg/{id}/verknuepfen)."""
    P = ph()
    cur.execute(f"SELECT beleg_gruppe_id FROM belege WHERE id={P}", (beleg_b,))
    r = cur.fetchone()
    gruppe = (r[0] if isinstance(r, tuple) else r["beleg_gruppe_id"]) if r else None
    if not gruppe:
        if is_postgres():
            cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES RETURNING id")
            gruppe = cur.fetchone()[0]
        else:
            cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES")
            gruppe = cur.lastrowid
        cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gruppe, beleg_b))
    cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gruppe, beleg_a))


def pdf_laden(d: dict) -> bytes:
    import mod_beleg
    if not d.get("s3_pdf"):
        raise ValueError("Noch kein PDF vorhanden (Entwurf).")
    return mod_beleg.s3_download(d["s3_pdf"])
