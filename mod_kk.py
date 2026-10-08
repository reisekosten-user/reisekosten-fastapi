"""
mod_kk.py – Kreditkartenabrechnung (qards BusinessCard / Sparkasse) lokal
auslesen und mit den Belegen abgleichen.

WICHTIG (Datenschutz): Dieses Modul nutzt KEINE KI und ruft KEINEN externen
Dienst auf. Die PDF wird nur im Arbeitsspeicher gelesen und nie gespeichert.
Name, Kartennummer und alle Positionen, die zu keinem Beleg gehören, werden
weder gespeichert noch weitergegeben. Gespeichert wird nur das Ergebnis am
jeweiligen Beleg: endgültiger Euro-Betrag, echte Auslandsgebühr, Abrechnungsdatum.

Aufbau:
  abrechnung_parsen()   PDF-Bytes -> Positionen (reine Regeln, kein Raten)
  abrechnung_zuordnen() Positionen + Belege -> Treffer (reine Funktion, testbar)
  belege_laden()        Kandidaten-Belege aus der Datenbank
  treffer_speichern()   bestätigte Treffer in die Belege schreiben
"""
from __future__ import annotations
import io, re
from datetime import date, datetime, timedelta

from mod_db import ph

# ── Muster für das Zeilenformat der Abrechnung ────────────────────────────────
_BETRAG = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"
_KURS = r"\d+,\d{3,6}"

# Positionsbeginn: "08.09.26 09.09.26 LUFTHANSA ..." (Belegdatum, Buchungsdatum, Text)
_POS_START = re.compile(r"^(\d{2}\.\d{2}\.\d{2})\s+(\d{2}\.\d{2}\.\d{2})\s+(.+)$")
# Fremdwährung am Zeilenende: "USD 60,17 1,1591 51,91 -"
_FREMD = re.compile(r"(?:^|\s)([A-Z]{3})\s+(" + _BETRAG + r")\s+(" + _KURS + r")\s+("
                    + _BETRAG + r")\s*([-+])?\s*$")
# Nur Euro am Zeilenende: "113,10 -"
_EUR_ENDE = re.compile(r"(?:^|\s)(" + _BETRAG + r")\s*([-+])\s*$")
# Gebührenzeile: "2% für Währungsumrechnung 1,04 -"
_ENTGELT = re.compile(r"^(\d+(?:,\d+)?)\s*%\s*f\S{1,3}r\s+W\S{1,3}hrungsumrechnung\s+("
                      + _BETRAG + r")\s*([-+])?\s*$")
# Flug-Zusatzzeilen unter Lufthansa-Positionen
_TICKET = re.compile(r"^(Ticket-Nummer|Passagier|Abflugdatum|Abflugort|Destination|"
                     r"Verkaufsstelle):\s*(.*)$")
# Zeilen, die eine Position beenden (Seitenwechsel, Summen)
_STOPP = re.compile(r"^(Zwischensumme|Übertrag|Neuer Saldo|Alter Saldo|Vorsaldo|"
                    r"Allgemeine Fragen)", re.IGNORECASE)
# Wiederholte Tabellenköpfe
_KOPF = {"Datum", "Beleg", "Buchung", "Angabe des Unternehmens /", "Verwendungszweck",
         "Währung Betrag Kurs Betrag in", "EUR", "Währung", "Betrag", "Kurs", "Betrag in"}

_MONATE = {"januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4, "mai": 5,
           "juni": 6, "juli": 7, "august": 8, "september": 9, "oktober": 10,
           "november": 11, "dezember": 12}

# Wörter, die als Namensabgleich nichts aussagen
_STOPWORTE = {"gmbh", "hotel", "hotels", "restaurant", "station", "service", "tankstelle",
              "germany", "deutschland", "operating", "flughafen", "parken", "vertrieb",
              "www", "com", "the", "und", "filiale", "shop", "store", "kg", "ag"}


def _zahl(s: str) -> float:
    """'6.919,76' -> 6919.76"""
    return float(s.replace(".", "").replace(",", "."))


def _datum_kurz(s: str) -> date | None:
    """'08.09.26' -> date(2026, 9, 8)"""
    try:
        return datetime.strptime(s, "%d.%m.%y").date()
    except Exception:
        return None


def _datum(x) -> date | None:
    if not x:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return None


def _falten(s: str | None) -> str:
    """Kleinbuchstaben, Umlaute aufgelöst, nur Buchstaben/Ziffern/Leerzeichen."""
    s = (s or "").lower()
    for a, b in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")):
        s = s.replace(a, b)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _tokens(s: str | None) -> set[str]:
    return {t for t in _falten(s).split() if len(t) >= 4 and not t.isdigit()
            and t not in _STOPWORTE}


# ── 1. PDF lesen ──────────────────────────────────────────────────────────────
def _pdf_text(pdf_bytes: bytes) -> str:
    import pypdf
    reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join((p.extract_text() or "") for p in reader.pages)


def _position_abschliessen(block: dict) -> dict | None:
    """Wertet die gesammelten Zeilen einer Position aus."""
    pos = {
        "datum_beleg": block["datum_beleg"], "datum_buchung": block["datum_buchung"],
        "waehrung": "EUR", "betrag_fremd": None, "kurs": None, "betrag_eur": None,
        "entgelt_eur": 0.0, "entgelt_prozent": None, "gutschrift": False,
        "ticket": None, "passagier": None, "text": "",
    }
    text_teile = []
    for z in block["zeilen"]:
        mt = _TICKET.match(z)
        if mt:
            if mt.group(1) == "Ticket-Nummer":
                pos["ticket"] = re.sub(r"\D", "", mt.group(2)) or None
            elif mt.group(1) == "Passagier":
                pos["passagier"] = mt.group(2).strip()
            continue
        me = _ENTGELT.match(z)
        if me:
            pos["entgelt_prozent"] = _zahl(me.group(1))
            pos["entgelt_eur"] = abs(_zahl(me.group(2)))
            continue
        mf = _FREMD.search(z)
        if mf and pos["betrag_eur"] is None:
            pos["waehrung"] = mf.group(1)
            pos["betrag_fremd"] = abs(_zahl(mf.group(2)))
            pos["kurs"] = _zahl(mf.group(3))
            pos["betrag_eur"] = abs(_zahl(mf.group(4)))
            pos["gutschrift"] = (mf.group(5) == "+")
            rest = z[:mf.start()].strip()
            if rest:
                text_teile.append(rest)
            continue
        mu = _EUR_ENDE.search(z)
        if mu and pos["betrag_eur"] is None:
            pos["betrag_eur"] = abs(_zahl(mu.group(1)))
            pos["gutschrift"] = (mu.group(2) == "+")
            rest = z[:mu.start()].strip()
            if rest:
                text_teile.append(rest)
            continue
        text_teile.append(z)
    pos["text"] = " ".join(text_teile).strip()
    if pos["betrag_eur"] is None:
        return None          # keine Betragszeile gefunden -> nicht raten
    return pos


def abrechnung_parsen(pdf_bytes: bytes) -> dict:
    """
    Liest eine qards-BusinessCard-Abrechnung (Text-PDF).
    Rückgabe: {"fehler": str} oder
      {"karteninhaber", "karte_ende", "zeitraum": (von, bis), "abrechnungsdatum",
       "positionen": [...], "nicht_gelesen": int, "summe_positionen",
       "saldo_neu", "summe_ok": bool|None}
    """
    try:
        text = _pdf_text(pdf_bytes)
    except Exception as e:
        return {"fehler": f"PDF konnte nicht gelesen werden: {e}"}
    if not text.strip():
        return {"fehler": "Kein Text im PDF (gescannt?). Nur Text-PDFs werden unterstützt."}

    # Kopfdaten
    m = re.search(r"Karteninhaber:\s*(.+)", text)
    karteninhaber = m.group(1).strip() if m else None
    m = re.search(r"BusinessCard-Nummer:\s*([0-9Xx ]+)", text)
    karte_ende = re.sub(r"\D", "", (m.group(1).strip()[-4:] if m else "")) or None
    m = re.search(r"Ihre Abrechnung vom\s+(\d{2}\.\d{2}\.\d{4})\s+bis\s+(\d{2}\.\d{2}\.\d{4})", text)
    zeitraum = None
    if m:
        try:
            zeitraum = (datetime.strptime(m.group(1), "%d.%m.%Y").date(),
                        datetime.strptime(m.group(2), "%d.%m.%Y").date())
        except Exception:
            zeitraum = None
    abr_datum = None
    m = re.search(r"Abrechnungsdatum:\s*(\d{1,2})\.\s*([A-Za-zäöüÄÖÜ]+)\s+(\d{4})", text)
    if m:
        mon = _MONATE.get(m.group(2).lower())
        if mon:
            try:
                abr_datum = date(int(m.group(3)), mon, int(m.group(1)))
            except Exception:
                abr_datum = None
    if not abr_datum and zeitraum:
        abr_datum = zeitraum[1]

    # Positionen sammeln
    positionen, block, nicht_gelesen = [], None, 0

    def fertig():
        nonlocal block, nicht_gelesen
        if block is not None:
            p = _position_abschliessen(block)
            if p:
                positionen.append(p)
            else:
                nicht_gelesen += 1
        block = None

    for roh in text.splitlines():
        z = roh.strip()
        if not z:
            continue
        if _STOPP.match(z):
            fertig()
            continue
        ms = _POS_START.match(z)
        if ms:
            fertig()
            d1, d2 = _datum_kurz(ms.group(1)), _datum_kurz(ms.group(2))
            if d1 and d2:
                block = {"datum_beleg": d1, "datum_buchung": d2, "zeilen": [ms.group(3)]}
            continue
        if block is None or z in _KOPF:
            continue
        block["zeilen"].append(z)
    fertig()

    if not karteninhaber or not positionen:
        return {"fehler": "Format nicht erkannt – es wurden keine Buchungspositionen gefunden. "
                          "Unterstützt ist die qards-BusinessCard-Abrechnung als Text-PDF."}

    # Kontrollsumme gegen "Neuer Saldo"
    summe = round(sum((-1 if p["gutschrift"] else 1) * (p["betrag_eur"] + p["entgelt_eur"])
                      for p in positionen), 2)
    saldo_neu, summe_ok = None, None
    m = re.search(r"Neuer Saldo\s+(" + _BETRAG + r")", text)
    if m:
        saldo_neu = abs(_zahl(m.group(1)))
        alt = 0.0
        ma = re.search(r"(?:Alter Saldo|Vorsaldo)\s+(" + _BETRAG + r")", text)
        if ma:
            alt = abs(_zahl(ma.group(1)))
        summe_ok = abs((summe + alt) - saldo_neu) < 0.015 and nicht_gelesen == 0

    return {"karteninhaber": karteninhaber, "karte_ende": karte_ende, "zeitraum": zeitraum,
            "abrechnungsdatum": abr_datum, "positionen": positionen,
            "nicht_gelesen": nicht_gelesen, "summe_positionen": summe,
            "saldo_neu": saldo_neu, "summe_ok": summe_ok}


# ── 2. Karteninhaber -> Mitarbeiter ───────────────────────────────────────────
def mitarbeiter_finden(karteninhaber: str, mitarbeiter: list[dict]) -> dict | None:
    """Findet den Mitarbeiter zum Karteninhaber (Reihenfolge der Namen egal)."""
    ziel = set(_falten(karteninhaber).split())
    treffer = [m for m in mitarbeiter if m.get("klarname")
               and set(_falten(m["klarname"]).split()) == ziel]
    return treffer[0] if len(treffer) == 1 else None


# ── 3. Positionen den Belegen zuordnen ────────────────────────────────────────
def _tage_diff(pos: dict, b: dict) -> int | None:
    daten = [_datum(b.get(k)) for k in
             ("belegdatum", "event_datum_von", "hotel_checkin_datum", "hotel_checkout_datum")]
    daten = [d for d in daten if d]
    if not daten:
        return None
    ref = [pos["datum_beleg"], pos["datum_buchung"]]
    return min(abs((d - r).days) for d in daten for r in ref)


def _bewerten(pos: dict, b: dict, karteninhaber_tokens: set[str]) -> tuple[int, list[str]] | None:
    """Punktzahl für Position <-> Beleg; None = passt nicht."""
    grund = []
    b_waehr = (b.get("waehrung") or "EUR").upper()
    b_betrag = b.get("betrag_brutto")
    if b_betrag is None:
        return None
    ticket_treffer = bool(pos.get("ticket") and pos["ticket"] in (b.get("suchtext") or ""))
    tage = _tage_diff(pos, b)
    name_treffer = bool(_tokens(pos["text"]) &
                        _tokens(" ".join(str(b.get(k) or "") for k in
                                         ("anbieter", "hotel_name", "tanken_tankstelle"))))

    if pos["waehrung"] != "EUR":
        if b_waehr != pos["waehrung"] or abs(float(b_betrag) - pos["betrag_fremd"]) > 0.005:
            return None
        if tage is not None and tage > 60 and not ticket_treffer:
            return None
        score = 60
        grund.append(f"{pos['betrag_fremd']:.2f} {pos['waehrung']} gleich")
    else:
        if b_waehr != "EUR" or abs(float(b_betrag) - pos["betrag_eur"]) > 0.005:
            return None
        if not (ticket_treffer or name_treffer):
            return None      # EUR-Beträge allein sind zu unspezifisch
        if not ticket_treffer and (tage is None or tage > 21):
            return None
        score = 40
        grund.append("Betrag gleich")

    if ticket_treffer:
        score += 100
        grund.append("Ticketnummer gleich")
    if name_treffer:
        score += 25
        grund.append("Name ähnlich")
    if tage is not None:
        if tage <= 3:
            score += 20
            grund.append("Datum passt")
        elif tage <= 14:
            score += 10
            grund.append(f"Datum {tage} Tage Abstand")
        else:
            grund.append(f"Datum {tage} Tage Abstand")
    rt = _tokens(b.get("reisender"))
    if rt and karteninhaber_tokens and not (rt & karteninhaber_tokens):
        score -= 20
        grund.append("anderer Reisender")
    return score, grund


def abrechnung_zuordnen(positionen: list[dict], belege: list[dict],
                        karteninhaber: str = "", kuerzel: str | None = None) -> dict:
    """
    Ordnet Positionen eins-zu-eins Belegen zu. Reine Funktion (ohne Datenbank).
    Rückgabe: {"treffer": [...], "offen": [Positionen ohne Beleg]}
    Jeder Treffer: {"pos", "beleg", "score", "sicher", "gruende"}
    """
    k_tokens = {t for t in _falten(karteninhaber).split() if len(t) >= 3}
    kandidaten = []        # (score, -tage, pos_index, beleg_index, gruende)
    for i, pos in enumerate(positionen):
        if pos["gutschrift"]:
            continue
        for j, b in enumerate(belege):
            zahl = (b.get("zahlungsart") or "").strip()
            if zahl not in ("", "Kreditkarte", "Unbekannt"):
                continue
            karte = (b.get("kreditkarte_karte") or "").strip()
            if karte and kuerzel and not karte.endswith("-" + kuerzel):
                continue     # Beleg gehört zu einer anderen Karte
            erg = _bewerten(pos, b, k_tokens)
            if erg:
                tage = _tage_diff(pos, b)
                kandidaten.append((erg[0], -(tage if tage is not None else 999), i, j, erg[1]))
    kandidaten.sort(key=lambda x: (x[0], x[1]), reverse=True)

    belegt_pos, belegt_beleg, zuordnung = set(), set(), {}
    for score, _t, i, j, gruende in kandidaten:
        if i in belegt_pos or j in belegt_beleg:
            continue
        belegt_pos.add(i)
        belegt_beleg.add(j)
        zuordnung[i] = (j, score, gruende)

    treffer = []
    for i, (j, score, gruende) in sorted(zuordnung.items()):
        # Mehrdeutig? Gleichwertiger Konkurrent um dieselbe Position oder denselben Beleg
        konkurrenz = any(sc >= score - 15 and ((pi == i and bj != j) or (bj == j and pi != i))
                         for sc, _tt, pi, bj, _g in kandidaten)
        treffer.append({"pos": positionen[i], "beleg": belege[j], "score": score,
                        "sicher": score >= 70 and not konkurrenz,
                        "mehrdeutig": konkurrenz, "gruende": gruende})
    offen = [p for i, p in enumerate(positionen) if i not in zuordnung and not p["gutschrift"]]
    return {"treffer": treffer, "offen": offen}


# ── 4. Datenbank ──────────────────────────────────────────────────────────────
def _row(r, keys):
    return {k: (r[k] if hasattr(r, "keys") else r[i]) for i, k in enumerate(keys)}


_BELEG_SPALTEN = ["id", "reise_code", "belegart", "anbieter", "hotel_name", "tanken_tankstelle",
                  "reisender", "belegdatum", "event_datum_von", "hotel_checkin_datum",
                  "hotel_checkout_datum", "betrag_brutto", "waehrung", "zahlungsart",
                  "kreditkarte_karte", "rechnungsnummer", "buchungscode"]


def belege_laden(db, zeitraum: tuple[date, date] | None) -> list[dict]:
    """Kandidaten: reisebezogene Belege, die noch in keiner Abrechnung bestätigt wurden."""
    cur = db.cursor()
    cur.execute("SELECT " + ", ".join(_BELEG_SPALTEN) + " FROM belege "
                "WHERE reise_code IS NOT NULL AND betrag_brutto IS NOT NULL "
                "AND kk_abrechnung_datum IS NULL "
                "AND (aenderung_status IS NULL OR aenderung_status = 'uebernommen') "
                "ORDER BY id")
    belege = [_row(r, _BELEG_SPALTEN) for r in cur.fetchall()]
    cur.close()
    if zeitraum:
        von, bis = zeitraum[0] - timedelta(days=120), zeitraum[1] + timedelta(days=60)
        def im_fenster(b):
            ds = [_datum(b.get(k)) for k in ("belegdatum", "event_datum_von",
                                              "hotel_checkin_datum", "hotel_checkout_datum")]
            ds = [d for d in ds if d]
            return (not ds) or any(von <= d <= bis for d in ds)
        belege = [b for b in belege if im_fenster(b)]
    # Suchtext (Ziffernfolgen) für den Ticketnummern-Abgleich nur bei Flug/Bahn
    P = ph()
    for b in belege:
        b["suchtext"] = re.sub(r"(?<=\d)[\s\-](?=\d)", "", " ".join(
            str(b.get(k) or "") for k in ("rechnungsnummer", "buchungscode")))
    ids = [b["id"] for b in belege if (b.get("belegart") or "") in ("Flug", "Bahn", "Flugaenderung")]
    cur = db.cursor()
    for bid in ids:
        cur.execute(f"SELECT rohtext FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        roh = (r[0] if isinstance(r, tuple) else r["rohtext"]) if r else ""
        for b in belege:
            if b["id"] == bid:
                b["suchtext"] += " " + re.sub(r"(?<=\d)[\s\-](?=\d)", "", roh or "")
                break
    cur.close()
    return belege


def treffer_speichern(db, eintraege: list[dict], abrechnungsdatum: date,
                      karten_label: str) -> int:
    """
    Schreibt bestätigte Treffer in die Belege.
    eintraege: [{"beleg_id", "fremdwaehrung": bool, "betrag_eur", "entgelt_eur"}]
    Fremdwährung: endgültiger Euro-Betrag + echte Auslandsgebühr.
    Immer: Abrechnungsdatum, Zahlungsart/Karte (nur falls noch leer).
    """
    P = ph()
    cur = db.cursor()
    n = 0
    for e in eintraege:
        zahl_karte = (f"zahlungsart=CASE WHEN zahlungsart IS NULL OR zahlungsart IN ('', 'Unbekannt') "
                      f"THEN 'Kreditkarte' ELSE zahlungsart END, "
                      f"kreditkarte_karte=CASE WHEN kreditkarte_karte IS NULL OR kreditkarte_karte='' "
                      f"THEN {P} ELSE kreditkarte_karte END")
        if e["fremdwaehrung"]:
            cur.execute(f"UPDATE belege SET betrag_eur_final={P}, kreditkarte_auslandsentgelt_eur={P}, "
                        f"kk_abrechnung_datum={P}, {zahl_karte} WHERE id={P}",
                        (e["betrag_eur"], e["entgelt_eur"], abrechnungsdatum.isoformat(),
                         karten_label, e["beleg_id"]))
        else:
            cur.execute(f"UPDATE belege SET kk_abrechnung_datum={P}, {zahl_karte} WHERE id={P}",
                        (abrechnungsdatum.isoformat(), karten_label, e["beleg_id"]))
        n += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    cur.close()
    return n
