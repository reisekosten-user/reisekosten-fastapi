"""
mod_abwicklung.py – Reise absagen oder verschieben, mit Checkliste (v3.56)

Eine geplante Reise kann storniert oder auf einen anderen Zeitraum (auch ins nächste Jahr) verschoben werden.
- Verschieben: Es entsteht automatisch eine neue Reise (Titel, Teilnehmer, Länder, Termine werden übernommen und um die
  Verschiebung versetzt). Die alte Reise bleibt mit allen Belegen und Stornokosten stehen und trägt den Status „verschoben“.
- Stornieren: Die Reise bleibt mit dem Status „storniert“ stehen.
- In beiden Fällen werden die Outlook-Einladungen automatisch abgesagt; optional geht eine Info-Mail an die Reisenden.
- Die Checkliste führt alle Buchungen der Reise (Flug, Bahn, Hotel, Mietwagen, Visum/Einreise, A1) sowie Vorschuss und
  Termine auf. Je Punkt: Status (kostenlos storniert, Erstattung erwartet, umgebucht, Gutschein, …), Betrag, Frist, Notiz.
  Offene Punkte und erwartete Erstattungen erscheinen auf der ToDo-Seite, bis alles erledigt ist.
Datumswerte stehen als Text JJJJ-MM-TT, Beträge als Zahl (läuft in Postgres und SQLite gleich).
"""
from __future__ import annotations
import json
import secrets
from datetime import date, datetime, timedelta
from html import escape as e

from mod_db import get_db, ph, is_postgres, next_reise_code

TABELLE = "reise_abwicklung"

# ── Status ──────────────────────────────────────────────────────────────────────────────────────────────────────
STATUS_BUCHUNG = [
    ("offen", "Offen – noch nicht geklärt"),
    ("kostenlos", "Kostenlos storniert"),
    ("erstattung", "Erstattung erwartet"),
    ("erstattet", "Erstattung erhalten"),
    ("gebuehr", "Storniert – keine Erstattung"),
    ("umgebucht", "Umgebucht"),
    ("gutschein", "Gutschein erhalten"),
    ("gilt_weiter", "Gilt weiter / wieder verwendbar"),
    ("entfaellt", "Entfällt"),
]
STATUS_AUFGABE = [
    ("offen", "Offen"),
    ("erledigt", "Erledigt"),
    ("entfaellt", "Entfällt"),
]
STATUS_TEXT = {k: v for k, v in STATUS_BUCHUNG + STATUS_AUFGABE}
OFFEN = ("offen", "erstattung")            # diese Status zählen als „noch zu tun“
BETRAG_STATUS = ("erstattung", "erstattet", "gebuehr", "gutschein")   # hier ist ein Betrag sinnvoll

BUCHUNGS_ARTEN = ("Flug", "Bahn", "Hotel", "Mietwagen", "Visum")
SORT_ART = {"Flug": 0, "Bahn": 1, "Hotel": 2, "Mietwagen": 3, "Visum": 4, "A1": 5}
SYMBOL = {"Flug": "✈", "Bahn": "🚆", "Hotel": "🏨", "Mietwagen": "🚗", "Visum": "🛂", "A1": "📄"}
HINWEIS_ART = {
    "Visum": "Einreisegenehmigungen gelten meist nur für den beantragten Zeitraum – für die neue Reise ggf. neu beantragen.",
    "A1": "Die A1-Bescheinigung gilt nur für den beantragten Zeitraum – für die neue Reise neu beantragen.",
}


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def datum_lesen(v):
    """Text (JJJJ-MM-TT oder TT.MM.JJJJ), date oder datetime -> date; sonst None."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    return None


def dt(v) -> str:
    d = datum_lesen(v)
    return d.strftime("%d.%m.%Y") if d else (str(v) if v else "")


def zahl(text) -> float | None:
    """„1.234,56“ / „1234.56“ / „12,5“ -> float; leer oder ungültig -> None."""
    if text is None:
        return None
    s = str(text).strip().replace(" ", "").replace("€", "").replace("EUR", "")
    if not s:
        return None
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def euro(x) -> str:
    return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €"


# ── Tabellen/Spalten ────────────────────────────────────────────────────────────────────────────────────────────
_REISEN_SPALTEN = (("status", "TEXT DEFAULT 'aktiv'"), ("status_datum", "TEXT"), ("status_notiz", "TEXT"),
                   ("nachfolger_code", "TEXT"), ("vorgaenger_code", "TEXT"))


def tabelle_sichern() -> None:
    """Legt Tabelle und Reise-Spalten an, falls sie fehlen (Neustart genügt, /init ist nicht nötig)."""
    try:
        db = get_db(); cur = db.cursor()
        key = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} ("
                    f"id {key}, reise_code TEXT NOT NULL, schluessel TEXT NOT NULL, titel TEXT, "
                    f"status TEXT NOT NULL DEFAULT 'offen', betrag NUMERIC(12,2), frist TEXT, erledigt_am TEXT, "
                    f"notiz TEXT, aktualisiert TEXT, UNIQUE (reise_code, schluessel))")
        if is_postgres():
            for sp, typ in _REISEN_SPALTEN:
                cur.execute(f"ALTER TABLE reisen ADD COLUMN IF NOT EXISTS {sp} {typ}")
        else:
            cur.execute("PRAGMA table_info(reisen)")
            vorhanden = {(r["name"] if hasattr(r, "keys") else r[1]) for r in cur.fetchall()}
            for sp, typ in _REISEN_SPALTEN:
                if sp not in vorhanden:
                    cur.execute(f"ALTER TABLE reisen ADD COLUMN {sp} {typ}")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[Abwicklung] Tabelle/Spalten nicht angelegt: {ex}")


# ── Reise lesen ─────────────────────────────────────────────────────────────────────────────────────────────────
_RS = ("code", "titel", "abreise", "rueckkehr", "notiz", "status", "status_datum", "status_notiz",
       "nachfolger_code", "vorgaenger_code")


def reise_lesen(cur, code: str) -> dict | None:
    cur.execute("SELECT " + ", ".join(_RS) + " FROM reisen WHERE code=" + ph(), (code.upper(),))
    r = cur.fetchone()
    if not r:
        return None
    d = {k: _g(r, k, i) for i, k in enumerate(_RS)}
    d["status"] = d["status"] or "aktiv"
    return d


def status_von(code: str) -> str:
    """'aktiv', 'verschoben' oder 'storniert' – bei Fehlern 'aktiv' (nie eine Reise versehentlich sperren)."""
    try:
        db = get_db(); cur = db.cursor()
        try:
            cur.execute("SELECT status FROM reisen WHERE code=" + ph(), (str(code).upper(),))
            r = cur.fetchone()
        finally:
            cur.close(); db.close()
        return ((_g(r, "status", 0) if r else None) or "aktiv")
    except Exception:
        return "aktiv"


# ── Checkliste ──────────────────────────────────────────────────────────────────────────────────────────────────
def _strecke(ki_json) -> str:
    try:
        ki = json.loads(ki_json) if isinstance(ki_json, str) else (ki_json or {})
        segs = ki.get("segmente") or []
        if not segs:
            return ""
        s0, s1 = segs[0], segs[-1]
        von = s0.get("von_ort") or s0.get("von_iata") or ""
        nach = s1.get("nach_ort") or s1.get("nach_iata") or ""
        tag = s0.get("abreise_datum") or ""
        teile = [t for t in (f"{von} → {nach}" if von and nach else "", tag) if t]
        return " · ".join(teile)
    except Exception:
        return ""


def _wert(row_vals) -> tuple:
    """(Betrag, Währung) des Belegs; bevorzugt den Euro-Wert."""
    eur_final, eur, brutto, wae = row_vals
    for v in (eur_final, eur):
        if v is not None and float(v) > 0:
            return float(v), "EUR"
    if brutto is not None and float(brutto) > 0:
        return float(brutto), (wae or "EUR")
    return 0.0, "EUR"


def _buchungen_laden(cur, code: str) -> list:
    """Buchungen der Reise als Checklistenpunkte; gleiche Buchungscodes (Bestätigung + Rechnung) werden zusammengefasst."""
    P = ph()
    spalten = ("id", "transportart", "transportart_freitext", "anbieter", "buchungscode", "reisender", "betrag_brutto",
               "waehrung", "betrag_eur", "betrag_eur_final", "belegart", "hotel_name", "hotel_checkin_datum",
               "hotel_checkout_datum", "belegdatum", "a1_kuerzel", "ki_json")
    arten = ",".join("'%s'" % a for a in BUCHUNGS_ARTEN)
    cur.execute("SELECT " + ", ".join(spalten) + " FROM belege WHERE reise_code=" + P +
                " AND aenderung_status IS NULL AND (transportart IN (" + arten + ") "
                "OR belegart IN ('A1Bescheinigung','Einreisegenehmigung')) ORDER BY id", (code,))
    gruppen: dict = {}
    for r in cur.fetchall():
        b = {k: _g(r, k, i) for i, k in enumerate(spalten)}
        if b["belegart"] == "A1Bescheinigung":
            art = "A1"
        elif b["belegart"] == "Einreisegenehmigung" or b["transportart"] == "Visum":
            art = "Visum"
        else:
            art = b["transportart"]
        cd = (b["buchungscode"] or "").strip().upper()
        wer = (b["reisender"] or "").strip().lower()
        gk = (art, cd, wer) if cd else (art, "#%s" % b["id"], "")
        g = gruppen.get(gk)
        wert, wae = _wert((b["betrag_eur_final"], b["betrag_eur"], b["betrag_brutto"], b["waehrung"]))
        if g is None:
            gruppen[gk] = {"art": art, "ids": [b["id"]], "b": b, "wert": wert, "wae": wae}
        else:
            g["ids"].append(b["id"])
            if wert > g["wert"]:
                g["wert"], g["wae"] = wert, wae
            if not g["b"].get("ki_json") and b.get("ki_json"):
                g["b"] = b
    punkte = []
    for g in gruppen.values():
        b, art = g["b"], g["art"]
        anbieter = (b["hotel_name"] if art == "Hotel" and b["hotel_name"] else b["anbieter"]) or ""
        name = {"Flug": "Flug", "Bahn": "Bahn", "Hotel": "Hotel", "Mietwagen": "Mietwagen",
                "Visum": "Visum/Einreisegenehmigung", "A1": "A1-Bescheinigung"}[art]
        titel = name + (f": {anbieter}" if anbieter else "")
        if art == "A1" and b["a1_kuerzel"]:
            titel += f" ({b['a1_kuerzel']})"
        elif art == "Visum" and b["transportart_freitext"]:
            titel = f"Visum/Einreisegenehmigung: {b['transportart_freitext']}"
        teile = []
        if b["reisender"]:
            teile.append(str(b["reisender"]))
        if art in ("Flug", "Bahn"):
            s = _strecke(b["ki_json"])
            if s:
                teile.append(s)
        if art == "Hotel" and (b["hotel_checkin_datum"] or b["hotel_checkout_datum"]):
            teile.append(f"{dt(b['hotel_checkin_datum'])} – {dt(b['hotel_checkout_datum'])}")
        if b["buchungscode"]:
            teile.append(f"Buchungscode {b['buchungscode']}")
        if g["wert"] > 0:
            teile.append("Wert " + (euro(g["wert"]) if g["wae"] == "EUR" else f"{g['wert']:.2f} {g['wae']}"))
        punkte.append({"schluessel": f"beleg:{min(g['ids'])}", "art": art, "titel": titel,
                       "detail": " · ".join(teile), "beleg_ids": g["ids"], "typ": "buchung",
                       "hinweis": HINWEIS_ART.get(art, ""), "wert": g["wert"] if g["wae"] == "EUR" else 0.0,
                       "sort": (SORT_ART.get(art, 9), b.get("belegdatum") and str(b["belegdatum"]) or "", min(g["ids"]))})
    punkte.sort(key=lambda p: p["sort"])
    return punkte


def _gespeichert(cur, code: str) -> dict:
    cur.execute("SELECT schluessel, titel, status, betrag, frist, erledigt_am, notiz FROM " + TABELLE +
                " WHERE reise_code=" + ph() + " ORDER BY id", (code,))
    out = {}
    for r in cur.fetchall():
        d = {k: _g(r, k, i) for i, k in enumerate(("schluessel", "titel", "status", "betrag", "frist", "erledigt_am", "notiz"))}
        d["betrag"] = float(d["betrag"]) if d["betrag"] is not None else None
        out[d["schluessel"]] = d
    return out


def punkte_laden(db, code: str) -> list:
    """Alle Checklistenpunkte der Reise (berechnete Buchungen + gespeicherte Angaben + freie Punkte)."""
    code = code.upper()
    cur = db.cursor()
    P = ph()
    try:
        reise = reise_lesen(cur, code)
        gesp = _gespeichert(cur, code)
        punkte = _buchungen_laden(cur, code)
        cur.execute("SELECT COUNT(*) AS n FROM termine WHERE reise_code=" + P, (code,))
        n_termine = int(_g(cur.fetchone(), "n", 0) or 0)
        cur.execute("SELECT COUNT(*) AS n FROM bargeld_buchungen WHERE reise_code=" + P + " AND status <> 'ersetzt'", (code,))
        n_vorschuss = int(_g(cur.fetchone(), "n", 0) or 0)
    finally:
        cur.close()
    fest = []
    if n_termine:
        fest.append({"schluessel": "termine", "art": "Termin", "typ": "aufgabe",
                     "titel": f"Termine ({n_termine}) neu abstimmen" if (reise and reise["status"] == "verschoben")
                              else f"Termine ({n_termine}) absagen",
                     "detail": "Kunden/Ansprechpartner informieren, Termine neu vereinbaren" if (reise and reise["status"] == "verschoben")
                               else "Kunden/Ansprechpartner informieren",
                     "hinweis": "", "wert": 0.0, "beleg_ids": []})
    if n_vorschuss:
        fest.append({"schluessel": "vorschuss", "art": "Vorschuss", "typ": "aufgabe",
                     "titel": "Reisevorschuss / Bargeld klären",
                     "detail": "Rückgabe oder Übertrag auf die neue Reise; Buchungen unter „Vorschuss“ bestätigen",
                     "hinweis": "", "wert": 0.0, "beleg_ids": [], "link": f"/reise/{code}/vorschuss"})
    for p in punkte + fest:
        p.setdefault("link", "")
    alle = punkte + fest
    for sk, s in gesp.items():                      # System- und freie Punkte, die nur gespeichert existieren
        if sk in {p["schluessel"] for p in alle}:
            continue
        if sk in ("outlook", "infomail"):
            art = "Info"
        elif sk.startswith("frei:"):
            art = "Frei"
        else:
            continue
        alle.append({"schluessel": sk, "art": art, "typ": "aufgabe", "titel": s["titel"] or sk, "detail": "",
                     "hinweis": "", "wert": 0.0, "beleg_ids": [], "link": ""})
    for p in alle:
        s = gesp.get(p["schluessel"])
        ist_buchung = p["typ"] == "buchung"
        p["status"] = (s["status"] if s else "offen")
        if s and s.get("titel") and p["art"] in ("Info", "Frei"):
            p["titel"] = s["titel"]
        p["betrag"] = s["betrag"] if s else None
        p["frist"] = s["frist"] if s else None
        p["erledigt_am"] = s["erledigt_am"] if s else None
        p["notiz"] = (s["notiz"] if s else "") or ""
        p["status_liste"] = STATUS_BUCHUNG if ist_buchung else STATUS_AUFGABE
        p["offen"] = p["status"] in OFFEN
    reihenfolge = {"Info": 50, "Frei": 60, "Termin": 30, "Vorschuss": 40}
    alle.sort(key=lambda p: (SORT_ART.get(p["art"], reihenfolge.get(p["art"], 99)), ))
    return alle


def speichern(code: str, schluessel: str, status: str, betrag=None, frist: str = "", notiz: str = "",
              titel: str | None = None) -> str | None:
    """Speichert die Angaben zu einem Punkt. Gibt eine Fehlermeldung zurück oder None."""
    code = code.upper()
    if status not in STATUS_TEXT:
        return "Unbekannter Status."
    db = get_db(); cur = db.cursor(); P = ph()
    try:
        if not reise_lesen(cur, code):
            return "Reise nicht gefunden."
        b = zahl(betrag) if not isinstance(betrag, (int, float)) else float(betrag)
        if betrag not in (None, "") and b is None:
            return "Der Betrag ist keine gültige Zahl."
        fr = datum_lesen(frist)
        if (frist or "").strip() and not fr:
            return "Das Datum ist ungültig."
        erledigt = None if status in OFFEN else date.today().isoformat()
        jetzt = datetime.now().strftime("%Y-%m-%d %H:%M")
        cur.execute("SELECT id, titel FROM " + TABELLE + " WHERE reise_code=" + P + " AND schluessel=" + P, (code, schluessel))
        r = cur.fetchone()
        werte = (status, b, fr.isoformat() if fr else None, erledigt, (notiz or "").strip()[:500] or None, jetzt)
        if r:
            neu_titel = titel if titel is not None else _g(r, "titel", 1)
            cur.execute("UPDATE " + TABELLE + " SET status=" + P + ", betrag=" + P + ", frist=" + P + ", erledigt_am=" + P +
                        ", notiz=" + P + ", aktualisiert=" + P + ", titel=" + P + " WHERE id=" + P,
                        werte + (neu_titel, _g(r, "id", 0)))
        else:
            cur.execute("INSERT INTO " + TABELLE + " (reise_code, schluessel, titel, status, betrag, frist, erledigt_am, "
                        "notiz, aktualisiert) VALUES (" + ",".join([P] * 9) + ")",
                        (code, schluessel, titel, status, b, fr.isoformat() if fr else None, erledigt,
                         (notiz or "").strip()[:500] or None, jetzt))
        db.commit()
        return None
    finally:
        cur.close(); db.close()


def frei_anlegen(code: str, titel: str) -> str | None:
    titel = (titel or "").strip()[:200]
    if not titel:
        return "Bitte einen Text für den Punkt eingeben."
    return speichern(code, "frei:" + secrets.token_hex(4), "offen", titel=titel)


def frei_loeschen(code: str, schluessel: str) -> None:
    if not schluessel.startswith("frei:"):
        return
    db = get_db(); cur = db.cursor()
    try:
        cur.execute("DELETE FROM " + TABELLE + " WHERE reise_code=" + ph() + " AND schluessel=" + ph(),
                    (code.upper(), schluessel))
        db.commit()
    finally:
        cur.close(); db.close()


def zusammenfassung(punkte: list) -> dict:
    offen = [p for p in punkte if p["status"] == "offen"]
    erst = [p for p in punkte if p["status"] == "erstattung"]
    return {"gesamt": len(punkte), "offen": len(offen), "erstattung": len(erst),
            "erstattung_summe": round(sum(p["betrag"] or 0 for p in erst), 2),
            "erledigt": len(punkte) - len(offen) - len(erst)}


def offene_aufgaben() -> list:
    """Für die ToDo-Seite: je abgesagter/verschobener Reise die offenen Punkte und erwarteten Erstattungen."""
    out = []
    try:
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute("SELECT code, titel, status FROM reisen WHERE COALESCE(status,'aktiv') <> 'aktiv' ORDER BY abreise")
            reisen = [(_g(r, "code", 0), _g(r, "titel", 1) or "", _g(r, "status", 2)) for r in cur.fetchall()]
            cur.close()
            for code, titel, st in reisen:
                z = zusammenfassung(punkte_laden(db, code))
                if z["offen"] or z["erstattung"]:
                    out.append({"code": code, "titel": titel, "status": st, **z})
        finally:
            db.close()
    except Exception as ex:
        print(f"[Abwicklung] ToDo nicht geladen: {ex}")
    return out


# ── Absagen / Verschieben ───────────────────────────────────────────────────────────────────────────────────────
def vorschau(code: str) -> dict | None:
    """Zahlen für das Formular: Termine, aktive Outlook-Einladungen, Belege, Reisende mit E-Mail, schon begonnen?"""
    db = get_db(); cur = db.cursor(); P = ph()
    try:
        r = reise_lesen(cur, code)
        if not r:
            return None
        cur.execute("SELECT COUNT(*) AS n FROM termine WHERE reise_code=" + P, (r["code"],))
        r["n_termine"] = int(_g(cur.fetchone(), "n", 0) or 0)
        cur.execute("SELECT COUNT(*) AS n FROM belege WHERE reise_code=" + P, (r["code"],))
        r["n_belege"] = int(_g(cur.fetchone(), "n", 0) or 0)
        try:
            cur.execute("SELECT COUNT(*) AS n FROM kalender_versand WHERE reise_code=" + P + " AND status='aktiv'", (r["code"],))
            r["n_einladungen"] = int(_g(cur.fetchone(), "n", 0) or 0)
        except Exception:
            db.rollback()
            r["n_einladungen"] = 0
        r["reisende"] = _reisende(cur, r["code"])
        ab = datum_lesen(r["abreise"])
        r["begonnen"] = bool(ab and ab <= date.today())
        r["standard_abreise"] = (ab + timedelta(days=364)).isoformat() if ab else ""
        zu = datum_lesen(r["rueckkehr"])
        r["standard_rueckkehr"] = (zu + timedelta(days=364)).isoformat() if zu else ""
        return r
    finally:
        cur.close(); db.close()


def _reisende(cur, code: str) -> list:
    cur.execute("SELECT m.kuerzel, m.klarname, m.email, m.aktiv FROM mitarbeiter m JOIN reise_mitarbeiter rm "
                "ON rm.kuerzel = m.kuerzel WHERE rm.reise_code=" + ph() + " ORDER BY m.klarname", (code,))
    out = []
    for r in cur.fetchall():
        akt = _g(r, "aktiv", 3)
        out.append({"kuerzel": _g(r, "kuerzel", 0), "klarname": _g(r, "klarname", 1) or "",
                    "email": (_g(r, "email", 2) or "").strip(), "aktiv": True if akt is None else bool(akt)})
    return out


def _folgereise_anlegen(db, cur, alt: dict, n_ab: date, n_zu: date) -> dict:
    """Legt die neue Reise als Kopie der alten an (Teilnehmer, Länder, Termine – um die Verschiebung versetzt)."""
    P = ph()
    ab, zu = datum_lesen(alt["abreise"]), datum_lesen(alt["rueckkehr"])
    delta = (n_ab - ab).days
    neu = next_reise_code(cur, n_ab.isoformat())
    notiz = ((alt["notiz"] or "").strip() + "\n" if (alt["notiz"] or "").strip() else "") + \
        f"Verschoben von {alt['code']} (ursprünglich {dt(ab)} – {dt(zu)})."
    cur.execute("INSERT INTO reisen (code, titel, abreise, rueckkehr, notiz, vorgaenger_code) VALUES (" +
                ",".join([P] * 6) + ")", (neu, alt["titel"], n_ab.isoformat(), n_zu.isoformat(), notiz, alt["code"]))
    cur.execute("SELECT kuerzel FROM reise_mitarbeiter WHERE reise_code=" + P, (alt["code"],))
    for r in cur.fetchall():
        cur.execute("INSERT INTO reise_mitarbeiter (reise_code, kuerzel) VALUES (" + P + "," + P + ")", (neu, _g(r, "kuerzel", 0)))
    # Länder: um die Verschiebung versetzen, auf den neuen Zeitraum begrenzen, Sätze neu holen
    try:
        from mod_vma import vma_fuer_land
    except Exception:
        vma_fuer_land = None
    cur.execute("SELECT datum_von, datum_bis, land_code, land_name, vma_voll, vma_halb FROM reise_laender "
                "WHERE reise_code=" + P + " ORDER BY datum_von", (alt["code"],))
    laender = []
    for r in cur.fetchall():
        von, bis = datum_lesen(_g(r, "datum_von", 0)), datum_lesen(_g(r, "datum_bis", 1))
        if not von or not bis:
            continue
        laender.append([von + timedelta(days=delta), bis + timedelta(days=delta), _g(r, "land_code", 2),
                        _g(r, "land_name", 3), _g(r, "vma_voll", 4), _g(r, "vma_halb", 5)])
    for l in laender:
        l[0], l[1] = max(l[0], n_ab), min(l[1], n_zu)
    laender = [l for l in laender if l[0] <= l[1]]
    if laender and laender[-1][1] < n_zu:
        laender[-1][1] = n_zu
    if laender and laender[0][0] > n_ab:
        laender[0][0] = n_ab
    for von, bis, lcode, lname, voll, halb in laender:
        if vma_fuer_land:
            try:
                voll, halb = vma_fuer_land(lcode)
            except Exception:
                pass
        cur.execute("INSERT INTO reise_laender (reise_code, datum_von, datum_bis, land_code, land_name, vma_voll, vma_halb) "
                    "VALUES (" + ",".join([P] * 7) + ")", (neu, von.isoformat(), bis.isoformat(), lcode, lname, voll, halb))
    # Termine
    spalten = ("datum", "uhrzeit_von", "uhrzeit_bis", "titel", "typ", "ort", "ansprechpartner", "telefon", "notiz",
               "land_code", "serie_id")
    cur.execute("SELECT " + ", ".join(spalten) + " FROM termine WHERE reise_code=" + P + " ORDER BY datum, uhrzeit_von", (alt["code"],))
    serien: dict = {}
    n_termine = 0
    for r in cur.fetchall():
        t = {k: _g(r, k, i) for i, k in enumerate(spalten)}
        d = datum_lesen(t["datum"])
        if not d:
            continue
        sid = t["serie_id"]
        if sid:
            serien.setdefault(sid, secrets.token_hex(8))
            sid = serien[sid]
        cur.execute("INSERT INTO termine (reise_code, datum, uhrzeit_von, uhrzeit_bis, titel, typ, ort, ansprechpartner, "
                    "telefon, notiz, land_code, serie_id) VALUES (" + ",".join([P] * 12) + ")",
                    (neu, (d + timedelta(days=delta)).isoformat(), t["uhrzeit_von"], t["uhrzeit_bis"], t["titel"], t["typ"],
                     t["ort"], t["ansprechpartner"], t["telefon"], t["notiz"], t["land_code"], sid))
        n_termine += 1
    return {"code": neu, "termine": n_termine, "delta": delta}


def info_mail_text(alt: dict, art: str, grund: str, neu: dict | None, n_ab, n_zu, zusatz: str, name: str) -> tuple:
    verb = "verschoben" if art == "verschoben" else "storniert"
    betreff = f"Reise {alt['code']} {alt['titel']}: {verb}"
    z = [f"Hallo {name},", "",
         f"die Reise {alt['code']} – {alt['titel']} ({dt(alt['abreise'])} – {dt(alt['rueckkehr'])}) wurde {verb}."]
    if neu:
        z.append(f"Neuer Zeitraum: {dt(n_ab)} – {dt(n_zu)} (neue Reisenummer {neu['code']}).")
    if (grund or "").strip():
        z.append(f"Grund: {grund.strip()}")
    if (zusatz or "").strip():
        z += ["", zusatz.strip()]
    z += ["", "Bitte stornieren oder ändern Sie bestehende Buchungen nicht selbst; wir melden uns dazu.",
          "Die Outlook-Termine zu dieser Reise werden automatisch abgesagt.", "", "Viele Grüße", "Reiseorganisation"]
    return betreff, "\n".join(z)


def absagen(code: str, art: str, grund: str = "", neue_abreise=None, neue_rueckkehr=None, info_mail: bool = False,
            zusatz: str = "", bestaetigt: bool = False) -> dict:
    """Führt die Absage/Verschiebung aus. Rückgabe: {'fehler'} | {'rueckfrage'} | {'ok', 'neu', 'outlook', 'mail'}."""
    code = code.upper()
    if art not in ("verschoben", "storniert"):
        return {"fehler": "Bitte „Verschieben“ oder „Stornieren“ wählen."}
    db = get_db(); cur = db.cursor(); P = ph()
    try:
        alt = reise_lesen(cur, code)
        if not alt:
            return {"fehler": "Reise nicht gefunden."}
        if alt["status"] != "aktiv":
            return {"fehler": f"Die Reise ist bereits {alt['status']}."}
        ab, zu = datum_lesen(alt["abreise"]), datum_lesen(alt["rueckkehr"])
        if not ab or not zu:
            return {"fehler": "Die Reise hat kein gültiges Datum."}
        begonnen = ab <= date.today()
        if begonnen and not bestaetigt:
            return {"rueckfrage": "begonnen"}
        n_ab = n_zu = None
        if art == "verschoben":
            n_ab, n_zu = datum_lesen(neue_abreise), datum_lesen(neue_rueckkehr)
            if not n_ab or not n_zu:
                return {"fehler": "Bitte den neuen Zeitraum (Abreise und Rückkehr) angeben."}
            if n_zu < n_ab:
                return {"fehler": "Die Rückkehr liegt vor der Abreise."}
            if n_ab == ab and n_zu == zu:
                return {"fehler": "Der neue Zeitraum ist derselbe wie bisher."}
        neu = _folgereise_anlegen(db, cur, alt, n_ab, n_zu) if art == "verschoben" else None
        cur.execute("UPDATE reisen SET status=" + P + ", status_datum=" + P + ", status_notiz=" + P +
                    ", nachfolger_code=" + P + " WHERE code=" + P,
                    (art, date.today().isoformat(), (grund or "").strip()[:500] or None, neu["code"] if neu else None, code))
        reisende = _reisende(cur, code)
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close(); db.close()

    if not begonnen:                                     # abgeleitete Tage der noch nicht begonnenen Reise entfernen
        for tabelle in ("vma_tage", "reisetage_person"):
            try:
                d2 = get_db(); c2 = d2.cursor()
                c2.execute(f"DELETE FROM {tabelle} WHERE reise_code=" + ph(), (code,))
                d2.commit(); c2.close(); d2.close()
            except Exception as ex:
                print(f"[Abwicklung] {tabelle} nicht bereinigt: {ex}")
    if neu:
        try:
            from mod_vma_tage import vma_auto_aktualisieren
            vma_auto_aktualisieren(neu["code"])
        except Exception as ex:
            print(f"[Abwicklung] VMA für {neu['code']} nicht berechnet: {ex}")

    erg = {"ok": True, "neu": neu, "begonnen": begonnen}
    if neu and neu["termine"]:                           # Termine sind kopiert, müssen aber noch neu abgestimmt werden
        speichern(code, "termine", "offen",
                  notiz=f"{neu['termine']} Termin(e) in {neu['code']} übernommen")

    # Outlook-Absagen
    try:
        import mod_kalender
        k = mod_kalender.kalender_abgleich(code, sofort=True)
        if k.get("fehler"):
            speichern(code, "outlook", "offen", notiz="; ".join(k["fehler"])[:300], titel="Outlook-Einladungen absagen")
            erg["outlook"] = "Fehler: " + "; ".join(k["fehler"])[:200]
        else:
            n = int(k.get("abgesagt") or 0)
            speichern(code, "outlook", "erledigt",
                      notiz=f"{n} Einladung(en) abgesagt" if n else "Es waren keine Einladungen versendet",
                      titel="Outlook-Einladungen absagen")
            erg["outlook"] = f"{n} Einladung(en) abgesagt"
    except Exception as ex:
        speichern(code, "outlook", "offen", notiz=str(ex)[:300], titel="Outlook-Einladungen absagen")
        erg["outlook"] = f"Fehler: {ex}"

    # Info-Mail an die Reisenden
    if info_mail:
        try:
            from mod_mail import sende_mail, smtp_sitzung
            empf = [r for r in reisende if r["aktiv"] and "@" in r["email"]]
            gesendet, fehler = [], []
            with smtp_sitzung():
                for r in empf:
                    betreff, text = info_mail_text(alt, art, grund, neu, n_ab, n_zu, zusatz, r["klarname"] or r["kuerzel"])
                    res = sende_mail(r["email"], betreff, text)
                    (fehler if res.get("fehler") else gesendet).append(r["klarname"] or r["kuerzel"])
            if not empf:
                speichern(code, "infomail", "offen", notiz="Kein Reisender mit E-Mail-Adresse", titel="Reisende informieren")
                erg["mail"] = "Keine E-Mail-Adresse vorhanden"
            elif fehler:
                speichern(code, "infomail", "offen", notiz="Nicht zugestellt an: " + ", ".join(fehler), titel="Reisende informieren")
                erg["mail"] = "Fehler bei: " + ", ".join(fehler)
            else:
                speichern(code, "infomail", "erledigt", notiz="Gesendet an: " + ", ".join(gesendet), titel="Reisende informieren")
                erg["mail"] = "Gesendet an: " + ", ".join(gesendet)
        except Exception as ex:
            speichern(code, "infomail", "offen", notiz=str(ex)[:300], titel="Reisende informieren")
            erg["mail"] = f"Fehler: {ex}"
    return erg


def reaktivieren(code: str) -> dict:
    """Macht eine abgesagte/verschobene Reise wieder aktiv (Outlook-Einladungen kommen neu, VMA-Tage werden berechnet)."""
    code = code.upper()
    db = get_db(); cur = db.cursor(); P = ph()
    try:
        r = reise_lesen(cur, code)
        if not r:
            return {"fehler": "Reise nicht gefunden."}
        if r["status"] == "aktiv":
            return {"fehler": "Die Reise ist bereits aktiv."}
        nachfolger = r["nachfolger_code"]
        cur.execute("UPDATE reisen SET status='aktiv', status_datum=NULL, status_notiz=NULL, nachfolger_code=NULL "
                    "WHERE code=" + P, (code,))
        cur.execute("UPDATE reisen SET vorgaenger_code=NULL WHERE vorgaenger_code=" + P, (code,))
        cur.execute("DELETE FROM " + TABELLE + " WHERE reise_code=" + P, (code,))
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close(); db.close()
    try:
        from mod_vma_tage import vma_auto_aktualisieren
        vma_auto_aktualisieren(code)
    except Exception as ex:
        print(f"[Abwicklung] VMA für {code} nicht berechnet: {ex}")
    erg = {"ok": True, "nachfolger": nachfolger}
    try:
        import mod_kalender
        erg["kalender"] = mod_kalender.kalender_abgleich(code, sofort=True)
    except Exception as ex:
        erg["kalender"] = {"fehler": [str(ex)]}
    return erg


# ── Seiten (Inhalt; main.py setzt den Rahmen) ───────────────────────────────────────────────────────────────────
_STATUS_FARBE = {"offen": "#b45309", "erstattung": "#b45309", "erstattet": "#166534", "kostenlos": "#166534",
                 "umgebucht": "#1d4ed8", "gutschein": "#1d4ed8", "gilt_weiter": "#1d4ed8", "gebuehr": "#b91c1c",
                 "erledigt": "#166534", "entfaellt": "#6b7280"}


def banner_html(code: str) -> str:
    """Hinweiskasten für die Reiseseite einer abgesagten/verschobenen Reise ('' bei aktiven Reisen)."""
    try:
        db = get_db()
        try:
            cur = db.cursor()
            r = reise_lesen(cur, code)
            cur.close()
            if not r or r["status"] == "aktiv":
                return ""
            z = zusammenfassung(punkte_laden(db, code))
        finally:
            db.close()
    except Exception:
        return ""
    wort = "verschoben" if r["status"] == "verschoben" else "storniert"
    ziel = (f' – neue Reise <a href="/reise/{e(r["nachfolger_code"])}" style="color:#7c2d12;font-weight:700">'
            f'{e(r["nachfolger_code"])}</a>') if r["nachfolger_code"] else ""
    offen = z["offen"] + z["erstattung"]
    stand = (f"{offen} Punkt(e) offen" if offen else "alles abgewickelt ✔")
    return ('<div class="alert" style="background:#fff7ed;border:2px solid #fb923c;color:#7c2d12;padding:12px 16px;'
            'border-radius:8px;margin-bottom:16px">'
            f'🚫 <b>Diese Reise wurde {wort}</b> am {dt(r["status_datum"])}{ziel}. '
            f'{e(r["status_notiz"] or "")} '
            f'<a href="/reise/{e(code)}/abwicklung" style="color:#7c2d12;font-weight:700">Abwicklung: {stand} →</a></div>')


def vorgaenger_html(code: str) -> str:
    """Hinweis auf der neuen Reise: 'Verschoben von …'."""
    try:
        db = get_db(); cur = db.cursor()
        try:
            r = reise_lesen(cur, code)
        finally:
            cur.close(); db.close()
        if not r or not r["vorgaenger_code"]:
            return ""
        return ('<div class="alert alert-info" style="margin-bottom:16px">↪ Verschoben von '
                f'<a href="/reise/{e(r["vorgaenger_code"])}/abwicklung"><b>{e(r["vorgaenger_code"])}</b></a> – '
                'Buchungen und Stornokosten der alten Reise stehen dort.</div>')
    except Exception:
        return ""


def absage_formular_html(v: dict, fehler: str = "", w: dict | None = None) -> str:
    w = w or {}
    code = e(v["code"])
    art = w.get("art") or "verschoben"
    mit_mail = [r for r in v["reisende"] if r["aktiv"] and "@" in r["email"]]
    warn = ""
    if v["begonnen"]:
        warn = ('<div class="alert alert-warn" style="margin-bottom:16px"><b>Die Reise hat schon begonnen.</b> '
                'VMA-Tage und Portal-Einträge bleiben erhalten (die Reise wurde teilweise durchgeführt).<br>'
                '<label style="display:block;margin-top:8px"><input type="checkbox" name="bestaetigt" value="1"%s> '
                'Ja, trotzdem absagen/verschieben</label></div>' % (" checked" if w.get("bestaetigt") else ""))
    fehler_html = f'<div class="alert alert-err" style="margin-bottom:16px">{e(fehler)}</div>' if fehler else ""
    inp = "padding:8px;border:1px solid var(--border);border-radius:6px;font-size:14px"
    return f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <div><div style="font-family:monospace;font-size:13px;color:var(--muted)">{code}</div>
      <h1 class="page-title" style="margin:0">Reise absagen / verschieben</h1>
      <div style="color:var(--muted);font-size:13px">{e(v['titel'] or '')} · {dt(v['abreise'])} – {dt(v['rueckkehr'])}</div></div>
      <a href="/reise/{code}" class="btn btn-secondary">← Zurück</a>
    </div>
    {fehler_html}
    <form method="post" action="/reise/{code}/absage">
    {warn}
    <div class="card" style="margin-bottom:16px"><div class="card-body">
      <label style="display:block;margin-bottom:8px"><input type="radio" name="art" value="verschoben"{' checked' if art == 'verschoben' else ''} onchange="absageUmschalten()">
        <b>Verschieben</b> – neue Reise wird angelegt (Teilnehmer, Länder, Termine werden übernommen)</label>
      <label style="display:block;margin-bottom:14px"><input type="radio" name="art" value="storniert"{' checked' if art == 'storniert' else ''} onchange="absageUmschalten()">
        <b>Stornieren</b> – Reise entfällt ganz</label>
      <div style="margin-bottom:14px"><label style="font-size:13px;color:var(--muted)">Grund (optional)</label><br>
        <input name="grund" value="{e(w.get('grund', ''))}" style="{inp};width:100%;max-width:560px" placeholder="z. B. Kunde hat Termin verschoben"></div>
      <div id="neuer-zeitraum" style="margin-bottom:6px">
        <div style="font-size:13px;color:var(--muted);margin-bottom:6px">Neuer Zeitraum (Vorschlag: genau 52 Wochen später, gleicher Wochentag)</div>
        <input type="date" name="neue_abreise" value="{e(w.get('neue_abreise') or v['standard_abreise'])}" style="{inp}">
        –
        <input type="date" name="neue_rueckkehr" value="{e(w.get('neue_rueckkehr') or v['standard_rueckkehr'])}" style="{inp}">
        <div style="font-size:12px;color:var(--muted);margin-top:6px">Die neue Reisenummer wird automatisch vergeben (Jahr der neuen Abreise).</div>
      </div>
    </div></div>
    <div class="card" style="margin-bottom:16px"><div class="card-body">
      <label style="display:block;margin-bottom:6px"><input type="checkbox" name="info_mail" value="1"{' checked' if w.get('info_mail') else ''}>
        <b>Info-Mail an die Reisenden senden</b> ({len(mit_mail)} von {len(v['reisende'])} Teilnehmern mit E-Mail-Adresse)</label>
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Kurze Mail mit Grund, ggf. neuem Zeitraum und dem Hinweis, Buchungen nicht selbst zu ändern.</div>
      <input name="zusatz" value="{e(w.get('zusatz', ''))}" style="{inp};width:100%;max-width:560px" placeholder="Zusatztext für die Mail (optional)">
    </div></div>
    <div class="card" style="margin-bottom:16px"><div class="card-body" style="font-size:13px">
      <b>Das passiert:</b>
      <ul style="margin:6px 0 0 18px;padding:0">
        <li>Die Reise bekommt den Status „verschoben“ bzw. „storniert“ und bleibt mit ihren {v['n_belege']} Beleg(en) stehen.</li>
        <li>{v['n_einladungen']} Outlook-Einladung(en) werden sofort abgesagt.</li>
        <li>Eine Checkliste führt alle Buchungen auf (Erstattung, Umbuchung, Gutschein …) – offene Punkte stehen auf der ToDo-Seite.</li>
        <li id="hinweis-neu">{v['n_termine']} Termin(e) werden in die neue Reise übernommen.</li>
      </ul>
    </div></div>
    <button type="submit" class="btn btn-primary">Ausführen</button>
    <a href="/reise/{code}" class="btn btn-secondary">Abbrechen</a>
    </form>
    <script>
    function absageUmschalten() {{
      var v = document.querySelector('input[name=art]:checked').value === 'verschoben';
      document.getElementById('neuer-zeitraum').style.display = v ? '' : 'none';
      document.getElementById('hinweis-neu').style.display = v ? '' : 'none';
    }}
    absageUmschalten();
    </script>"""


def abwicklung_html(db, code: str, meldung: str = "") -> str | None:
    cur = db.cursor()
    try:
        r = reise_lesen(cur, code)
    finally:
        cur.close()
    if not r:
        return None
    punkte = punkte_laden(db, code)
    z = zusammenfassung(punkte)
    c = e(r["code"])
    wort = {"verschoben": "verschoben", "storniert": "storniert", "aktiv": "aktiv"}[r["status"]]
    kopf_status = ""
    if r["status"] != "aktiv":
        kopf_status = f'<b>{wort.capitalize()}</b> am {dt(r["status_datum"])}'
        if r["nachfolger_code"]:
            kopf_status += f' → neue Reise <a href="/reise/{e(r["nachfolger_code"])}"><b>{e(r["nachfolger_code"])}</b></a>'
        if r["status_notiz"]:
            kopf_status += f' · Grund: {e(r["status_notiz"])}'
    else:
        kopf_status = 'Diese Reise ist aktiv. <a href="/reise/%s/absage">Absagen / verschieben →</a>' % c
    zeilen = ""
    inp = "padding:6px;border:1px solid var(--border);border-radius:6px;font-size:13px"
    for p in punkte:
        sk = e(p["schluessel"])
        optionen = "".join('<option value="%s"%s>%s</option>' % (k, " selected" if k == p["status"] else "", e(t))
                           for k, t in p["status_liste"])
        farbe = _STATUS_FARBE.get(p["status"], "#6b7280")
        belege = " ".join('<a href="/beleg/%s" style="font-size:12px">Beleg %s</a>' % (i, i) for i in p["beleg_ids"][:4])
        if p.get("link"):
            belege += ' <a href="%s" style="font-size:12px">öffnen →</a>' % e(p["link"])
        hinweis = ('<div style="font-size:12px;color:#92400e;margin-top:4px">ℹ %s</div>' % e(p["hinweis"])) if p["hinweis"] else ""
        erledigt = (f'<span style="font-size:11px;color:var(--muted)"> · {dt(p["erledigt_am"])}</span>' if p["erledigt_am"] else "")
        loeschen = ""
        if p["schluessel"].startswith("frei:"):
            loeschen = (f'<form method="post" action="/reise/{c}/abwicklung/loeschen" style="display:inline;margin-left:6px" '
                        f'onsubmit="return confirm(\'Diesen Punkt löschen?\')"><input type="hidden" name="schluessel" value="{sk}">'
                        '<button class="btn btn-secondary btn-sm" title="Punkt löschen">✕</button></form>')
        betrag_wert = "" if p["betrag"] is None else f"{p['betrag']:.2f}".replace(".", ",")
        zeilen += f"""
        <div style="padding:12px 16px;border-bottom:1px solid var(--border)">
          <div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap">
            <div style="flex:1;min-width:240px">
              <div style="font-weight:600">{SYMBOL.get(p['art'], '•')} {e(p['titel'])}
                <span style="font-size:12px;font-weight:600;color:{farbe}">· {e(STATUS_TEXT.get(p['status'], p['status']))}</span>{erledigt}</div>
              <div style="font-size:12px;color:var(--muted)">{e(p['detail'])} {belege}</div>{hinweis}
            </div>
            <form method="post" action="/reise/{c}/abwicklung/speichern" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
              <input type="hidden" name="schluessel" value="{sk}">
              <select name="status" style="{inp}">{optionen}</select>
              <input name="betrag" value="{e(betrag_wert)}" placeholder="Betrag €" size="9" style="{inp}" title="Erstattung, Gutschein oder Gebühr in Euro">
              <input type="date" name="frist" value="{e(str(p['frist'] or '')[:10])}" style="{inp}" title="Frist: Erstattung bis / Gutschein gültig bis">
              <input name="notiz" value="{e(p['notiz'])}" placeholder="Notiz" size="22" style="{inp}">
              <button class="btn btn-secondary btn-sm">Speichern</button>{loeschen}
            </form>
          </div>
        </div>"""
    if not zeilen:
        zeilen = '<div style="padding:16px;color:var(--muted)">Keine Buchungen in dieser Reise. Eigene Punkte lassen sich unten ergänzen.</div>'
    erst = (f' · erwartete Erstattungen: <b>{euro(z["erstattung_summe"])}</b>' if z["erstattung"] else "")
    msg = f'<div class="alert alert-ok" style="margin-bottom:16px">{e(meldung)}</div>' if meldung else ""
    reaktiv = ""
    if r["status"] != "aktiv":
        reaktiv = (f'<form method="post" action="/reise/{c}/reaktivieren" style="display:inline" '
                   'onsubmit="return confirm(\'Reise wieder aktivieren? Outlook-Einladungen werden neu gesendet.\')">'
                   '<button class="btn btn-secondary">↩ Reise wieder aktivieren</button></form>')
    return f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <div><div style="font-family:monospace;font-size:13px;color:var(--muted)">{c}</div>
      <h1 class="page-title" style="margin:0">Abwicklung: {e(r['titel'] or '')}</h1>
      <div style="color:var(--muted);font-size:13px">{dt(r['abreise'])} – {dt(r['rueckkehr'])}</div></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap"><a href="/reise/{c}" class="btn btn-secondary">← Zur Reise</a>{reaktiv}</div>
    </div>
    {msg}
    <div class="alert alert-info" style="margin-bottom:16px">{kopf_status}</div>
    <div style="margin-bottom:12px;font-size:14px">
      <b>{z['offen'] + z['erstattung']}</b> von {z['gesamt']} Punkten offen{erst}
    </div>
    <div class="card" style="margin-bottom:16px"><div class="card-header"><span class="card-title">Checkliste</span></div>
      {zeilen}
    </div>
    <div class="card"><div class="card-body">
      <form method="post" action="/reise/{c}/abwicklung/neu" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
        <input name="titel" placeholder="Eigenen Punkt ergänzen, z. B. Mietwagen bei Sixt abbestellen" size="60" style="{inp}">
        <button class="btn btn-primary btn-sm">+ Hinzufügen</button>
      </form>
    </div></div>"""
