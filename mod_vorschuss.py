"""
mod_vorschuss.py – Reisevorschuss und Bargeld-Konto je Reise, Mitarbeiter und Währung.

Drei Buchungsarten:
  auszahlung        Der Organisator zahlt Vorschuss aus (Währung, Betrag, Kurs von Hand).
                    Der Mitarbeiter quittiert den Erhalt.
  eigenbeschaffung  Der Mitarbeiter hat selbst Bargeld beschafft (Geldautomat, Wechselstube).
                    Er trägt Betrag und den belasteten Euro-Betrag laut Auszug ein.
  rueckgabe         Der Organisator nimmt Bargeld zurück (Betrag, Abgabekurs von Hand).
                    Der Mitarbeiter bestätigt die Rückgabe zusätzlich.

Jede Buchung wird bestätigt (Haken, Name, Zeitstempel, optional Unterschrift, SHA-256-Prüfsumme)
und danach gesperrt. Änderungen nur als neue Version. Nach der Bestätigung entsteht ein
PDF-Beleg und ein Beleg-Datensatz ohne Betrag (zählt nicht als Kosten).

Kursschreibweise: 1 EUR = <kurs> Fremdwährung (so steht es auf Wechselbelegen und EZB-Listen).

Abrechnung je Währung (nur bestätigte Buchungen):
  netto_firma       = Auszahlung − Rückgabe       (Firmengeld, das beim Mitarbeiter war)
  bar_ausgaben      = Summe der Bar-Belege der Reise in dieser Währung
  Rückgabe offen    = netto_firma − bar_ausgaben, falls positiv
  Eigenanteil       = bar_ausgaben − netto_firma, falls positiv (aus eigenem Geld bezahlt,
                      wird zum Kurs der Eigenbeschaffung erstattet)
  Bewertung der Bar-Belege: Firmenanteil zum Auszahlungskurs (gewichtet), Eigenanteil zum
  Kurs der Eigenbeschaffung (gewichtet).
  Kursergebnis      = Rückgabe in EUR − Rückgabe zum Auszahlungskurs.
"""
from __future__ import annotations
import base64, hashlib, io, json, re
from datetime import date

from mod_db import ph, is_postgres
from mod_bewirtung import (namen_gleich, signatur_pruefen, jetzt_utc_iso, lokal_text,
                           datum_de, zahl_parsen, _falten, _iso, _num, _esc)

ARTEN = {"auszahlung": "Vorschuss-Auszahlung",
         "eigenbeschaffung": "Bargeld selbst beschafft",
         "rueckgabe": "Rückgabe Bargeld"}
BELEGARTEN = {"auszahlung": "Vorschussquittung",
              "eigenbeschaffung": "Bargeldbeschaffung",
              "rueckgabe": "Rueckgabequittung"}
QUELLEN = ["Geldautomat", "Wechselstube", "Bank", "Sonstiges"]
WAEHRUNGEN = ["EUR", "USD", "GBP", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "HUF",
              "JPY", "CAD", "AUD", "AED", "TRY", "CNY", "INR", "MXN", "BRL", "ZAR"]

SPALTEN = ["id", "reise_code", "kuerzel", "nummer", "version", "ersetzt_id", "status", "art",
           "datum", "waehrung", "betrag", "kurs", "eur_betrag", "quelle_txt", "notiz",
           "aussteller_kuerzel", "aussteller_name", "aussteller_am",
           "bestaetigt_am", "bestaetigt_name", "bestaetigt_ip", "bestaetigt_agent",
           "signatur_png", "pruefsumme", "beleg_id", "s3_pdf"]


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────
def _row(r) -> dict:
    d = {k: (r[k] if hasattr(r, "keys") else r[i]) for i, k in enumerate(SPALTEN)}
    for k in ("betrag", "kurs", "eur_betrag"):
        d[k] = _num(d[k])
    d["datum"] = _iso(d["datum"])
    return d


def zahl_exakt(text: str | None, stellen: int = 6) -> float | None:
    """Wie zahl_parsen, aber mit mehr Nachkommastellen (für Kurse)."""
    t = (text or "").strip().replace(" ", "")
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", ".")
    try:
        v = round(float(t), stellen)
    except ValueError:
        return None
    return v if v > 0 else None


def name_passt(a: str | None, b: str | None) -> bool:
    """Beleg-Reisender passt zum Mitarbeiter: gleiche Namensteile (auch mit Titel/zweitem Vornamen)."""
    ta, tb = set(_falten(a).split()), set(_falten(b).split())
    if not ta or not tb:
        return False
    if ta == tb:
        return True
    kleiner = ta if len(ta) <= len(tb) else tb
    groesser = tb if kleiner is ta else ta
    return len(kleiner) >= 2 and kleiner <= groesser


def kurs_berechnen(waehrung: str, betrag: float | None, kurs_text: str | None,
                   eur_text: str | None) -> tuple[float | None, float | None, str | None]:
    """Liefert (kurs, eur_betrag, fehler). Kurs = Fremdwährung je 1 EUR.
    Eingabe genügt entweder als Kurs ODER als Euro-Gegenwert."""
    if betrag is None or betrag <= 0:
        return None, None, "Bitte einen Betrag größer 0 angeben."
    if waehrung == "EUR":
        return 1.0, round(betrag, 2), None
    kurs = zahl_exakt(kurs_text, 6)
    eur = zahl_parsen(eur_text)
    if eur is not None and eur <= 0:
        eur = None
    if kurs is None and eur is None:
        return None, None, "Bitte den Kurs (1 EUR = … Fremdwährung) oder den Euro-Gegenwert angeben."
    if kurs is not None and eur is not None:
        soll = betrag / kurs
        if abs(soll - eur) > 0.02 + 0.005 * eur:
            return None, None, ("Kurs und Euro-Gegenwert passen nicht zusammen "
                                f"({betrag:.2f} {waehrung} ÷ {kurs} = {soll:.2f} EUR, eingegeben: {eur:.2f} EUR).")
        return kurs, eur, None
    if kurs is not None:
        return kurs, round(betrag / kurs, 2), None
    return round(betrag / eur, 6), eur, None


def kurs_text(d: dict) -> str:
    """'1 EUR = 1,0850 USD (1 USD = 0,9217 EUR)'"""
    if d["waehrung"] == "EUR" or not d.get("kurs"):
        return "–"
    inv = 1 / d["kurs"]
    z = lambda x, n: f"{x:.{n}f}".replace(".", ",")
    return f"1 EUR = {z(d['kurs'], 4)} {d['waehrung']} (1 {d['waehrung']} = {z(inv, 4)} EUR)"


def betrag_text(x, w) -> str:
    if x is None:
        return "–"
    return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + f" {w}"


def validieren(d: dict) -> list[str]:
    f = []
    if d.get("art") not in ARTEN:
        f.append("Unbekannte Buchungsart.")
    if not d.get("datum"):
        f.append("Bitte das Datum angeben.")
    else:
        try:
            if date.fromisoformat(d["datum"]) > date.today():
                f.append("Das Datum liegt in der Zukunft.")
        except ValueError:
            f.append("Das Datum ist ungültig.")
    if not re.fullmatch(r"[A-Z]{3}", d.get("waehrung") or ""):
        f.append("Bitte die Währung als dreistelligen Code angeben (z. B. USD).")
    if d.get("betrag") is None or d["betrag"] <= 0:
        f.append("Bitte einen Betrag größer 0 angeben.")
    if d.get("eur_betrag") is None or d["eur_betrag"] <= 0:
        f.append("Es fehlt der Kurs oder der Euro-Gegenwert.")
    if d.get("art") == "eigenbeschaffung" and (d.get("quelle_txt") or "") not in QUELLEN:
        f.append("Bitte angeben, wo das Bargeld beschafft wurde.")
    if d.get("art") in ("auszahlung", "rueckgabe") and not d.get("aussteller_name"):
        f.append("Der Organisator konnte nicht erkannt werden (bitte neu anmelden).")
    return f


# ── Prüfsumme ─────────────────────────────────────────────────────────────────
def pruefsumme_berechnen(d: dict) -> str:
    sig = d.get("signatur_png") or ""
    kern = {"nummer": d["nummer"], "version": d["version"], "reise_code": d["reise_code"],
            "kuerzel": d["kuerzel"], "art": d["art"], "datum": _iso(d.get("datum")),
            "waehrung": d["waehrung"], "betrag": round(float(d["betrag"]), 2),
            "kurs": None if d.get("kurs") is None else round(float(d["kurs"]), 6),
            "eur_betrag": round(float(d["eur_betrag"]), 2),
            "quelle_txt": d.get("quelle_txt") or "", "notiz": (d.get("notiz") or "").strip(),
            "aussteller_name": d.get("aussteller_name") or "", "aussteller_am": d.get("aussteller_am") or "",
            "bestaetigt_am": d.get("bestaetigt_am"), "bestaetigt_name": d.get("bestaetigt_name"),
            "signatur_sha256": hashlib.sha256(sig.encode()).hexdigest() if sig else None}
    text = json.dumps(kern, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def integritaet_ok(d: dict) -> bool:
    return d.get("status") in ("bestaetigt", "ersetzt") and bool(d.get("pruefsumme")) \
        and pruefsumme_berechnen(d) == d["pruefsumme"]


# ── Datenbank ─────────────────────────────────────────────────────────────────
def _alle(db, where: str, params: tuple, order: str = "ORDER BY id") -> list[dict]:
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(SPALTEN) + f" FROM bargeld_buchungen WHERE {where} {order}", params)
    rows = [_row(r) for r in cur.fetchall()]
    cur.close()
    return rows


def laden(db, bid: int) -> dict | None:
    r = _alle(db, f"id={ph()}", (bid,))
    return r[0] if r else None


def liste_reise(db, reise_code: str, kuerzel: str | None = None) -> list[dict]:
    """Aktuelle Buchungen (ohne ersetzte Versionen) einer Reise, optional nur eines Mitarbeiters."""
    if kuerzel:
        return _alle(db, f"reise_code={ph()} AND kuerzel={ph()} AND status != 'ersetzt'", (reise_code, kuerzel))
    return _alle(db, f"reise_code={ph()} AND status != 'ersetzt'", (reise_code,))


def versionen(db, nummer: str) -> list[dict]:
    return _alle(db, f"nummer={ph()}", (nummer,), "ORDER BY version")


def zu_beleg(db, beleg_id: int) -> list[dict]:
    return _alle(db, f"beleg_id={ph()} AND status != 'ersetzt'", (beleg_id,))


def nummer_vergeben(db, reise_code: str) -> str:
    cur = db.cursor()
    cur.execute(f"SELECT DISTINCT nummer FROM bargeld_buchungen WHERE reise_code={ph()}", (reise_code,))
    hoechste = 0
    for r in cur.fetchall():
        m = re.search(r"-(\d+)$", (r[0] if isinstance(r, tuple) else r["nummer"]) or "")
        if m:
            hoechste = max(hoechste, int(m.group(1)))
    cur.close()
    return f"BG-{reise_code}-{hoechste + 1:02d}"


def _insert(db, sql: str, args: tuple) -> int:
    cur = db.cursor()
    if is_postgres():
        cur.execute(sql + " RETURNING id", args)
        neu = cur.fetchone()[0]
    else:
        cur.execute(sql, args)
        neu = cur.lastrowid
    cur.close()
    return neu


def speichern(db, d: dict, bid: int | None = None) -> int:
    """Legt eine offene Buchung an oder aktualisiert eine offene Buchung."""
    P = ph()
    werte = (d["art"], d.get("datum"), d.get("waehrung"), d.get("betrag"), d.get("kurs"), d.get("eur_betrag"),
             d.get("quelle_txt") or None, (d.get("notiz") or "").strip() or None,
             d.get("aussteller_kuerzel"), d.get("aussteller_name"), d.get("aussteller_am"))
    if bid:
        cur = db.cursor()
        cur.execute(f"""UPDATE bargeld_buchungen SET art={P}, datum={P}, waehrung={P}, betrag={P}, kurs={P},
                        eur_betrag={P}, quelle_txt={P}, notiz={P}, aussteller_kuerzel={P}, aussteller_name={P},
                        aussteller_am={P} WHERE id={P} AND status='offen'""", werte + (bid,))
        cur.close()
        return bid
    nummer = nummer_vergeben(db, d["reise_code"])
    return _insert(db, f"""INSERT INTO bargeld_buchungen (reise_code, kuerzel, nummer, version, status, art, datum,
        waehrung, betrag, kurs, eur_betrag, quelle_txt, notiz, aussteller_kuerzel, aussteller_name, aussteller_am)
        VALUES ({P},{P},{P},1,'offen',{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
        (d["reise_code"], d["kuerzel"], nummer) + werte)


def korrektur_starten(db, bid: int) -> int:
    alt = laden(db, bid)
    if not alt or alt["status"] != "bestaetigt":
        raise ValueError("Nur bestätigte Buchungen können korrigiert werden.")
    cur = db.cursor()
    cur.execute(f"SELECT COUNT(*) FROM bargeld_buchungen WHERE ersetzt_id={ph()} AND status='offen'", (bid,))
    if (cur.fetchone() or [0])[0]:
        cur.close()
        raise ValueError("Es gibt bereits einen offenen Korrektur-Entwurf.")
    cur.close()
    P = ph()
    return _insert(db, f"""INSERT INTO bargeld_buchungen (reise_code, kuerzel, nummer, version, ersetzt_id, status,
        art, datum, waehrung, betrag, kurs, eur_betrag, quelle_txt, notiz, aussteller_kuerzel, aussteller_name,
        aussteller_am, beleg_id)
        VALUES ({P},{P},{P},{P},{P},'offen',{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
        (alt["reise_code"], alt["kuerzel"], alt["nummer"], alt["version"] + 1, bid, alt["art"], alt["datum"],
         alt["waehrung"], alt["betrag"], alt["kurs"], alt["eur_betrag"], alt["quelle_txt"], alt["notiz"],
         alt["aussteller_kuerzel"], alt["aussteller_name"], alt["aussteller_am"], alt["beleg_id"]))


def loeschen(db, bid: int) -> None:
    cur = db.cursor()
    cur.execute(f"DELETE FROM bargeld_buchungen WHERE id={ph()} AND status='offen'", (bid,))
    cur.close()


# ── PDF ───────────────────────────────────────────────────────────────────────
def pdf_bauen(d: dict, klarname: str) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, KeepTogether)

    styles = getSampleStyleSheet()
    klein = ParagraphStyle("klein", parent=styles["Normal"], fontSize=8, leading=10, textColor=colors.HexColor("#475569"))
    mono = ParagraphStyle("mono", parent=styles["Normal"], fontName="Courier", fontSize=7.5, leading=9)
    normal = ParagraphStyle("n", parent=styles["Normal"], fontSize=9.5, leading=12)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=11, spaceBefore=10, spaceAfter=4)
    titel = {"auszahlung": "Quittung über Reisevorschuss", "rueckgabe": "Bestätigung Rückgabe Bargeld",
             "eigenbeschaffung": "Bestätigung Bargeld-Eigenbeschaffung"}[d["art"]]
    version_txt = f" (Version {d['version']})" if d.get("version", 1) > 1 else ""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=16 * mm,
                            bottomMargin=16 * mm, title=f"{titel} {d['nummer']}", author="Herrhammer Reisekosten")
    story = [Paragraph(f"{_esc(titel)} {_esc(d['nummer'])}{version_txt}", styles["Title"]),
             Paragraph("Elektronisch erstellter und bestätigter Beleg zum Bargeld-Konto der Reise", klein), Spacer(1, 6)]

    def zeile(k, v):
        return [Paragraph(f"<b>{_esc(k)}</b>", normal), Paragraph(_esc(v), normal)]

    kopf = [zeile("Reise", d["reise_code"]), zeile("Mitarbeiter", klarname), zeile("Datum", datum_de(d["datum"])),
            zeile("Art", ARTEN[d["art"]])]
    if d["art"] == "eigenbeschaffung":
        kopf.append(zeile("Beschafft über", d.get("quelle_txt")))
    kopf += [zeile("Betrag", betrag_text(d["betrag"], d["waehrung"]))]
    if d["waehrung"] != "EUR":
        kopf += [zeile("Kurs", kurs_text(d))]
    kopf += [zeile("Gegenwert in EUR" + (" (belastet, inkl. Gebühren)" if d["art"] == "eigenbeschaffung" else ""),
                   betrag_text(d["eur_betrag"], "EUR"))]
    if d.get("notiz"):
        kopf.append(zeile("Notiz", d["notiz"]))
    t = Table(kopf, colWidths=[62 * mm, 112 * mm])
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                           ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0"))]))
    story.append(t)

    aussteller = ""
    if d.get("aussteller_name"):
        aussteller = (f"{'Ausgezahlt' if d['art'] == 'auszahlung' else 'Entgegengenommen'} durch "
                      f"<b>{_esc(d['aussteller_name'])}</b> am {_esc(lokal_text(d.get('aussteller_am')))}.")
    text_best = {
        "auszahlung": "Ich bestätige, den oben genannten Betrag als Reisevorschuss für die genannte Reise erhalten zu "
                      "haben. Der Vorschuss wird mit den tatsächlichen Reisekosten abgerechnet; nicht verbrauchtes "
                      "Bargeld gebe ich zurück.",
        "rueckgabe": "Ich bestätige, den oben genannten Betrag in bar zurückgegeben zu haben.",
        "eigenbeschaffung": "Ich bestätige, das Bargeld wie angegeben selbst beschafft zu haben und dass die Angaben "
                            "vollständig und richtig sind.",
    }[d["art"]]
    story.append(Paragraph("Elektronische Bestätigung", h2))
    if aussteller:
        story.append(Paragraph(aussteller, normal))
        story.append(Spacer(1, 4))
    story.append(Paragraph(text_best, normal))
    bestaetigt = [["Bestätigt von", _esc(d.get("bestaetigt_name"))],
                  ["Zeitpunkt", lokal_text(d.get("bestaetigt_am")) + "  (UTC: " + _esc(d.get("bestaetigt_am")) + ")"],
                  ["IP-Adresse", _esc(d.get("bestaetigt_ip") or "–")]]
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
            s = min(70 * mm / iw, 26 * mm / ih)
            block += [Spacer(1, 4), Paragraph("Unterschrift (am Bildschirm gezeichnet):", klein),
                      Image(io.BytesIO(raw), width=iw * s, height=ih * s, hAlign="LEFT")]
        except Exception:
            pass
    block += [Spacer(1, 6), Paragraph("Prüfsumme (SHA-256 über alle Angaben und die Bestätigung):", klein),
              Paragraph(_esc(d.get("pruefsumme")), mono), Spacer(1, 6),
              Paragraph("Dieser Beleg wurde elektronisch erstellt und bestätigt. Nach der Bestätigung sind Änderungen "
                        "nur als neue Version mit Protokoll möglich; frühere Versionen bleiben erhalten.", klein)]
    story.append(KeepTogether(block))
    doc.build(story)
    return buf.getvalue()


# ── Bestätigen ────────────────────────────────────────────────────────────────
def bestaetigen(db, bid: int, klarname: str, name_eingabe: str, haken: bool,
                signatur_data_url: str | None, ip: str, agent: str) -> dict:
    """Mitarbeiter bestätigt die Buchung: Prüfsumme, PDF, Beleg-Datensatz. Commit macht der Aufrufer."""
    import mod_beleg
    d = laden(db, bid)
    if not d or d["status"] != "offen":
        raise ValueError("Diese Buchung ist nicht mehr offen.")
    fehler = validieren(d)
    if fehler:
        raise ValueError(" ".join(fehler))
    if not haken:
        raise ValueError("Bitte bestätigen, dass die Angaben richtig sind.")
    if not namen_gleich(name_eingabe, klarname):
        raise ValueError(f"Bitte den vollständigen Namen eingeben, so wie er im System steht: {klarname}")
    signatur = signatur_pruefen(signatur_data_url)

    d["bestaetigt_am"] = jetzt_utc_iso()
    d["bestaetigt_name"] = klarname
    d["bestaetigt_ip"] = (ip or "")[:64]
    d["bestaetigt_agent"] = (agent or "")[:300]
    d["signatur_png"] = signatur
    d["pruefsumme"] = pruefsumme_berechnen(d)

    pdf = pdf_bauen(d, klarname)
    s3_key = f"vorschuss/{d['reise_code']}/{d['nummer']}_v{d['version']}.pdf"
    mod_beleg.s3_upload(s3_key, pdf)

    P = ph()
    cur = db.cursor()
    dateiname = f"{BELEGARTEN[d['art']]}_{d['nummer']}.pdf"
    anbieter = f"{ARTEN[d['art']]} {betrag_text(d['betrag'], d['waehrung'])}"
    beleg_id = d.get("beleg_id")
    if not beleg_id and d.get("ersetzt_id"):
        alt = laden(db, d["ersetzt_id"])
        beleg_id = (alt or {}).get("beleg_id")
    if beleg_id:
        cur.execute(f"""UPDATE belege SET dateiname={P}, s3_original={P}, belegdatum={P}, anbieter={P}, waehrung={P},
                        geprueft={'FALSE' if is_postgres() else '0'}, geprueft_von=NULL, geprueft_am=NULL
                        WHERE id={P}""", (dateiname, s3_key, d["datum"], anbieter, d["waehrung"], beleg_id))
    else:
        beleg_id = _insert(db, f"""INSERT INTO belege (reise_code, dateiname, s3_original, ki_json, pflichtfelder_ok,
            fehlende_felder, belegdatum, belegart, transportart, anbieter, reisender, betrag_brutto, waehrung,
            zahlungsart, status, geprueft)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},'Sonstiges',{P},{P},NULL,{P},'Bar','ok',
                    {'FALSE' if is_postgres() else '0'})""",
            (d["reise_code"], dateiname, s3_key, "{}", True, "[]", d["datum"], BELEGARTEN[d["art"]], anbieter,
             klarname, d["waehrung"]))
    cur.execute(f"""UPDATE bargeld_buchungen SET status='bestaetigt', bestaetigt_am={P}, bestaetigt_name={P},
                    bestaetigt_ip={P}, bestaetigt_agent={P}, signatur_png={P}, pruefsumme={P}, beleg_id={P},
                    s3_pdf={P} WHERE id={P}""",
                (d["bestaetigt_am"], d["bestaetigt_name"], d["bestaetigt_ip"], d["bestaetigt_agent"],
                 signatur, d["pruefsumme"], beleg_id, s3_key, bid))
    if d.get("ersetzt_id"):
        cur.execute(f"UPDATE bargeld_buchungen SET status='ersetzt' WHERE id={P}", (d["ersetzt_id"],))
    cur.close()
    return {"beleg_id": beleg_id, "pruefsumme": d["pruefsumme"], "nummer": d["nummer"]}


def pdf_laden(d: dict) -> bytes:
    import mod_beleg
    if not d.get("s3_pdf"):
        raise ValueError("Noch kein PDF vorhanden (offen).")
    return mod_beleg.s3_download(d["s3_pdf"])


# ── Abrechnung je Mitarbeiter und Währung ─────────────────────────────────────
def _mitarbeiter_der_reise(db, reise_code: str) -> dict:
    cur = db.cursor()
    cur.execute(f"""SELECT m.kuerzel, m.klarname FROM reise_mitarbeiter rm
                    JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel WHERE rm.reise_code={ph()}""", (reise_code,))
    out = {(r[0] if isinstance(r, tuple) else r["kuerzel"]): (r[1] if isinstance(r, tuple) else r["klarname"])
           for r in cur.fetchall()}
    cur.close()
    return out


def _namen(db, kuerzel_liste: list[str]) -> dict:
    out = {}
    cur = db.cursor()
    for k in kuerzel_liste:
        cur.execute(f"SELECT klarname FROM mitarbeiter WHERE kuerzel={ph()}", (k,))
        r = cur.fetchone()
        out[k] = (r[0] if isinstance(r, tuple) else r["klarname"]) if r else k
    cur.close()
    return out


def bar_belege(db, reise_code: str) -> list[dict]:
    """Bar bezahlte Belege der Reise mit Betrag (ohne Quittungen dieses Moduls und Bewirtungs-Eigenbelege)."""
    cur = db.cursor()
    cur.execute(f"""SELECT id, reisender, waehrung, betrag_brutto, belegdatum, anbieter, betrag_eur_final, kurs_quelle
                    FROM belege WHERE reise_code={ph()} AND zahlungsart='Bar' AND betrag_brutto IS NOT NULL
                    AND COALESCE(belegart,'') NOT IN ('Vorschussquittung','Rueckgabequittung','Bargeldbeschaffung')
                    AND (aenderung_status IS NULL OR aenderung_status != 'offen') ORDER BY id""", (reise_code,))
    out = []
    for r in cur.fetchall():
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        out.append({"id": g("id", 0), "reisender": g("reisender", 1), "waehrung": (g("waehrung", 2) or "EUR").upper(),
                    "betrag": _num(g("betrag_brutto", 3)), "datum": _iso(g("belegdatum", 4)),
                    "anbieter": g("anbieter", 5), "eur_final": _num(g("betrag_eur_final", 6)),
                    "kurs_quelle": g("kurs_quelle", 7)})
    cur.close()
    return out


def abrechnung(db, reise_code: str) -> dict:
    """
    {"personen": {kuerzel: {"name", "offen": n, "waehrungen": {c: {...}}}},
     "unzugeordnet": [bar-belege]}
    """
    buchungen = liste_reise(db, reise_code)
    teilnehmer = _mitarbeiter_der_reise(db, reise_code)
    kuerzel_mit_buchung = sorted({b["kuerzel"] for b in buchungen})
    namen = _namen(db, kuerzel_mit_buchung)
    personen = {k: {"name": namen.get(k, k), "offen": 0, "waehrungen": {}} for k in kuerzel_mit_buchung}

    def w(k, c):
        return personen[k]["waehrungen"].setdefault(c, {
            "V": 0.0, "V_eur": 0.0, "R": 0.0, "R_eur": 0.0, "T": 0.0, "T_eur": 0.0, "A": 0.0, "belege": []})

    for b in buchungen:
        if b["status"] != "bestaetigt":
            personen[b["kuerzel"]]["offen"] += 1
            w(b["kuerzel"], b["waehrung"])
            continue
        x = w(b["kuerzel"], b["waehrung"])
        schl = {"auszahlung": "V", "rueckgabe": "R", "eigenbeschaffung": "T"}[b["art"]]
        x[schl] += b["betrag"]
        x[schl + "_eur"] += b["eur_betrag"]

    # Bar-Belege den Mitarbeitern zuordnen (über den Namen am Beleg; ohne Namen nur, wenn es genau
    # einen Mitarbeiter mit Bargeld-Konto gibt)
    unzugeordnet = []
    for bel in bar_belege(db, reise_code):
        treffer = [k for k, n in teilnehmer.items() if name_passt(bel["reisender"], n)]
        for k in kuerzel_mit_buchung:
            if k not in treffer and name_passt(bel["reisender"], namen.get(k)):
                treffer.append(k)
        if len(treffer) == 1:
            ziel = treffer[0]
        elif len(treffer) > 1:
            unzugeordnet.append(bel)
            continue
        elif not kuerzel_mit_buchung:
            continue
        elif len(kuerzel_mit_buchung) == 1 and not _falten(bel["reisender"]):
            ziel = kuerzel_mit_buchung[0]
        else:
            unzugeordnet.append(bel)
            continue
        if ziel not in personen:
            continue                    # Beleg eines Kollegen ohne Bargeld-Konto
        x = w(ziel, bel["waehrung"])
        x["A"] += bel["betrag"]
        x["belege"].append(bel)

    for k, p in personen.items():
        for c, x in p["waehrungen"].items():
            _auswerten(c, x)
    return {"personen": personen, "unzugeordnet": unzugeordnet}


def _auswerten(c: str, x: dict) -> None:
    """Berechnet Rest, Eigenanteil, Kurse und Bewertung für eine Währung (ändert x)."""
    eur = (c == "EUR")
    x["kurs_V"] = 1.0 if eur else (x["V"] / x["V_eur"] if x["V_eur"] else None)    # Fremdwährung je EUR
    x["kurs_R"] = 1.0 if eur else (x["R"] / x["R_eur"] if x["R_eur"] else None)
    x["kurs_T"] = 1.0 if eur else (x["T"] / x["T_eur"] if x["T_eur"] else None)
    netto = x["V"] - x["R"]
    x["netto_firma"] = round(netto, 2)
    x["rueckgabe_offen"] = round(max(0.0, netto - x["A"]), 2)
    x["eigenanteil"] = round(max(0.0, x["A"] - netto), 2)
    firmenanteil = min(x["A"], max(netto, 0.0))
    warnungen = []
    if netto < -0.005:
        warnungen.append("Es wurde mehr zurückgegeben als ausgezahlt.")
    firmen_eur = None
    if firmenanteil > 0.005:
        firmen_eur = firmenanteil / x["kurs_V"] if x["kurs_V"] else None
        if firmen_eur is None:
            warnungen.append("Kein Auszahlungskurs vorhanden: Bar-Belege können nicht bewertet werden.")
    else:
        firmen_eur = 0.0
    eigen_eur = None
    if x["eigenanteil"] > 0.005:
        if x["kurs_T"]:
            eigen_eur = x["eigenanteil"] / x["kurs_T"]
            if x["eigenanteil"] > x["T"] + 0.005:
                warnungen.append("Der Eigenanteil ist größer als das erfasste selbst beschaffte Bargeld "
                                 "(Eigenbeschaffung fehlt?). Bewertung zum Durchschnittskurs der Eigenbeschaffung.")
        else:
            warnungen.append("Bar-Ausgaben übersteigen das ausgezahlte Bargeld, aber es ist keine Eigenbeschaffung "
                             "erfasst.")
    else:
        eigen_eur = 0.0
    x["eigenanteil_eur"] = None if eigen_eur is None else round(eigen_eur, 2)
    if x["rueckgabe_offen"] <= 0.005:
        x["rueckgabe_offen_eur"] = 0.0
    elif x["kurs_V"]:
        x["rueckgabe_offen_eur"] = round(x["rueckgabe_offen"] / x["kurs_V"], 2)
    else:
        x["rueckgabe_offen_eur"] = None
    # Bewertung der Bar-Belege: EUR je Fremdwährungseinheit
    x["bewertung_eur_je_fw"] = None
    if x["A"] > 0.005 and firmen_eur is not None and eigen_eur is not None:
        x["bewertung_eur_je_fw"] = (firmen_eur + eigen_eur) / x["A"]
    x["bar_eur"] = None if x["bewertung_eur_je_fw"] is None else round(x["A"] * x["bewertung_eur_je_fw"], 2)
    # Kursergebnis aus der Rückgabe (positiv = Gewinn für die Firma)
    x["kursergebnis"] = None
    if x["R"] > 0 and x["kurs_V"] and not eur:
        x["kursergebnis"] = round(x["R_eur"] - x["R"] / x["kurs_V"], 2)
    x["warnungen"] = warnungen


def belege_bewerten(db, reise_code: str, kuerzel: str, waehrung: str, anwenden: bool = False) -> list[dict]:
    """Bewertet die Bar-Belege eines Mitarbeiters in einer Fremdwährung mit dem Kurs aus dem Bargeld-Konto.
    Überschrieben werden nur Belege ohne Euro-Endbetrag oder solche, die schon aus dem Bargeld-Konto bewertet wurden."""
    if waehrung == "EUR":
        return []
    erg = abrechnung(db, reise_code)
    x = (erg["personen"].get(kuerzel) or {}).get("waehrungen", {}).get(waehrung)
    if not x or not x.get("bewertung_eur_je_fw"):
        return []
    marker = f"Bargeld-Konto {reise_code}"
    aenderungen = []
    cur = db.cursor()
    for bel in x["belege"]:
        neu = round(bel["betrag"] * x["bewertung_eur_je_fw"], 2)
        frei = bel["eur_final"] is None or (bel["kurs_quelle"] or "").startswith("Bargeld-Konto")
        aenderungen.append({"id": bel["id"], "anbieter": bel["anbieter"], "betrag": bel["betrag"],
                            "alt": bel["eur_final"], "neu": neu, "ueberschreibbar": frei})
        if anwenden and frei:
            cur.execute(f"UPDATE belege SET betrag_eur_final={ph()}, kurs_quelle={ph()} WHERE id={ph()}",
                        (neu, marker, bel["id"]))
    cur.close()
    return aenderungen


def offene_punkte_aus(erg: dict) -> list[str]:
    """Kurze Texte, was im Ergebnis von abrechnung() noch offen ist."""
    punkte = []
    for k, p in erg["personen"].items():
        if p["offen"]:
            punkte.append(f"{p['name']}: {p['offen']} Buchung(en) nicht bestätigt")
        for c, x in sorted(p["waehrungen"].items()):
            if x["rueckgabe_offen"] > 0.005:
                punkte.append(f"{p['name']}: Rückgabe offen {betrag_text(x['rueckgabe_offen'], c)}")
            if x["eigenanteil"] > 0.005:
                punkte.append(f"{p['name']}: Erstattung offen {betrag_text(x['eigenanteil'], c)}")
    if erg["unzugeordnet"]:
        punkte.append(f"{len(erg['unzugeordnet'])} Bar-Beleg(e) keinem Mitarbeiter zugeordnet")
    return punkte


def offene_punkte(db, reise_code: str) -> list[str]:
    """Kurze Texte, was beim Bargeld-Konto der Reise noch offen ist (für ToDo/Abschluss)."""
    return offene_punkte_aus(abrechnung(db, reise_code))
