"""
mod_a1.py – A1-Bescheinigung (Sozialversicherung bei Reisen in Europa), v3.41

Für Reisen in EU-/EWR-Länder, die Schweiz und das Vereinigte Königreich muss jeder Reisende
eine A1-Bescheinigung (bzw. die gleichwertige Bescheinigung) mitführen.

Dieses Modul
  * erkennt eine A1-Bescheinigung am Text des Dokuments (auch englisch/französisch/italienisch/spanisch)
    oder – bei Scans ohne Text – am Dateinamen,
  * liest Name, Zeitraum, Ausstellungsdatum, Länder und ausstellende Stelle aus,
  * liefert das Ergebnis im Format der KI-Beleganalyse (damit die normale Beleg-Pipeline weiterläuft),
  * berechnet je Reise und Reisendem, ob eine A1 vorliegt (Status) und welche To-dos offen sind.
Die Ablage erfolgt als normaler Beleg mit der Belegart „A1Bescheinigung“ (ohne Betrag, keine Kosten).
"""
from __future__ import annotations
import re
import unicodedata
from datetime import date, datetime
from html import escape

from mod_db import get_db, ph
import mod_laender

BELEGART = "A1Bescheinigung"
ANZEIGE = "A1-Bescheinigung"

# EU-Länder (ohne Deutschland), EWR (IS, LI, NO), Schweiz, Vereinigtes Königreich
A1_LAENDER = frozenset({
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT",
    "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "IS", "LI", "NO", "CH", "GB"})

_EN_NAMEN = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "HR": "Croatia", "CY": "Cyprus", "CZ": "Czech Republic",
    "DK": "Denmark", "EE": "Estonia", "FI": "Finland", "FR": "France", "GR": "Greece", "HU": "Hungary",
    "IE": "Ireland", "IT": "Italy", "LV": "Latvia", "LT": "Lithuania", "LU": "Luxembourg", "MT": "Malta",
    "NL": "Netherlands", "PL": "Poland", "PT": "Portugal", "RO": "Romania", "SK": "Slovakia", "SI": "Slovenia",
    "ES": "Spain", "SE": "Sweden", "IS": "Iceland", "LI": "Liechtenstein", "NO": "Norway", "CH": "Switzerland",
    "GB": "United Kingdom",
}
_WEITERE_NAMEN = {"GB": ("Großbritannien", "Vereinigtes Königreich", "Great Britain"),
                  "CZ": ("Tschechien", "Tschechische Republik"), "NL": ("Niederlande", "Holland")}


# ── Hilfen ────────────────────────────────────────────────────────

def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def _norm(text) -> str:
    """Kleinschreibung, Umlaute aufgelöst, nur Buchstaben/Ziffern – für den Namensvergleich."""
    t = str(text or "").casefold()
    for a, b in (("ß", "ss"), ("ä", "ae"), ("ö", "oe"), ("ü", "ue")):
        t = t.replace(a, b)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def namen_passen(a, b) -> bool:
    """Gehören zwei Namen zur selben Person? Reihenfolge egal („Diesslin Ralf“ = „Ralf Diesslin“),
    Umlaute/ß egal. Beide Namen müssen mindestens zwei Namensteile haben und der kürzere muss im längeren stecken."""
    ta = {t for t in _norm(a).split() if len(t) > 1}
    tb = {t for t in _norm(b).split() if len(t) > 1}
    if len(ta) < 2 or len(tb) < 2:
        return False
    return ta <= tb or tb <= ta


def _datum_text(d) -> str:
    return d.strftime("%d.%m.%Y") if isinstance(d, (date, datetime)) else ""


def _als_datum(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


# ── Erkennung ─────────────────────────────────────────────────────

_TITEL = (
    (r"portable\s+document\s+a1", 5),
    (r"bescheinigung\s+(?:ueber|über)\s+die\s+anzuwendenden\s+rechtsvorschriften", 5),
    (r"anzuwendende[nr]?\s+rechtsvorschriften", 3),
    (r"certificate\s+concerning\s+the\s+legislation", 5),
    (r"legislation\s+applicable", 3),
    (r"certificat\s+concernant\s+la\s+l[ée]gislation", 5),
    (r"certificato\s+relativo\s+alla\s+legislazione", 5),
    (r"certificado\s+relativo\s+a\s+la\s+legislaci[oó]n", 5),
    (r"a1[\s\-]*bescheinigung", 4),
    (r"entsendebescheinigung", 4),
    (r"bescheinigung\s+(?:ueber|über)\s+die\s+entsendung", 4),
    (r"883\s*/\s*2004", 2),
    (r"987\s*/\s*2009", 2),
    (r"\bentsend\w*", 1),
    (r"soziale[rn]?\s+sicherheit|social\s+security", 1),
    (r"\ba1\b", 1),
)
_GEGEN = (r"rechnungsnummer", r"umsatzsteuer", r"\bmwst\b", r"\bust\.?-?id", r"zahlbetrag", r"gesamtbetrag",
          r"invoice\s+(?:no|number)", r"\bvat\b")
_NAME_RECHNUNG = re.compile(r"rechnung|invoice|faktura|beleg|quittung|ticket|buchung", re.I)
_NAME_A1 = re.compile(r"(?<![a-z0-9])a1(?![a-z0-9])|a1[\s_\-]?bescheinigung|entsendebescheinigung", re.I)
SCHWELLE = 4


def a1_punkte(text, dateiname: str = "") -> int:
    """Wie sicher ist es eine A1-Bescheinigung? (4 und mehr = ja)"""
    t = re.sub(r"\s+", " ", str(text or "").casefold())
    if len(t.strip()) < 20:
        # Scan ohne Text: nur der Dateiname kann helfen („A1_Mustermann.pdf“) – nie bei Rechnungen/Buchungen
        name = str(dateiname or "")
        if _NAME_A1.search(name) and not _NAME_RECHNUNG.search(name):
            return SCHWELLE
        return 0
    punkte = 0
    for muster, p in _TITEL:
        if re.search(muster, t):
            punkte += p
    for muster in _GEGEN:
        if re.search(muster, t):
            punkte -= 3
    return punkte


def a1_erkennen(text, dateiname: str = "") -> bool:
    return a1_punkte(text, dateiname) >= SCHWELLE


# ── Auswertung ────────────────────────────────────────────────────

_NAMENS_STOPP = re.compile(
    r"\s{2,}|\b(?:vorname|nachname|familienname|geburtsname|geburtsdatum|geburtsort|staatsangeh\w*|"
    r"first\s*name|surname|last\s*name|date\s+of\s+birth|nationality|pers[oö]nliche)\b", re.I)


def _name_bereinigen(s) -> str:
    teil = _NAMENS_STOPP.split(str(s or "").strip(), maxsplit=1)[0].strip(" :-–.,;")
    if not teil or len(teil) < 2 or len(teil) > 60 or re.search(r"\d", teil):
        return ""
    if not re.fullmatch(r"[A-Za-zÀ-ÖØ-öø-ÿ'’.\- ]+", teil):
        return ""
    if teil.isupper():
        teil = teil.title()
    return teil


def _wert_nach(zeilen: list, labels: str) -> str:
    """Wert hinter einer Beschriftung: auf derselben Zeile oder in der nächsten nicht leeren Zeile."""
    muster = re.compile(r"(?:^|[\s\d.)])(?:" + labels + r")\b\s*[:：]?\s*(.*)$", re.I)
    for i, z in enumerate(zeilen):
        m = muster.search(z)
        if not m:
            continue
        wert = _name_bereinigen(m.group(1))
        if not wert:
            for z2 in zeilen[i + 1:i + 3]:
                wert = _name_bereinigen(z2)
                if wert:
                    break
        if wert:
            return wert
    return ""


_DATUM = re.compile(r"(?<!\d)(\d{1,2})\s*[./]\s*(\d{1,2})\s*[./]\s*(\d{4})(?!\d)|(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")
_KW_VON = re.compile(r"\b(?:vom|von|ab|beginn|begin|start|from|g[üu]ltig\s+ab|valid\s+from)\b", re.I)
_KW_BIS = re.compile(r"\b(?:bis(?:\s+zum)?|ende|end|until|to|g[üu]ltig\s+bis|valid\s+(?:until|to))\b", re.I)
_KW_GEBURT = re.compile(r"geburt|birth|born|geboren", re.I)
_KW_AUSGESTELLT = re.compile(r"ausgestellt|ausstellung|issued|date\s+of\s+issue|issue\s+date|erstellt", re.I)


def _daten_mit_kontext(text: str) -> list:
    """[(Datum, Art)] Art: 'von' | 'bis' | 'geburt' | 'ausgestellt' | ''."""
    erg = []
    for m in _DATUM.finditer(text):
        try:
            if m.group(1):
                d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            else:
                d = date(int(m.group(4)), int(m.group(5)), int(m.group(6)))
        except ValueError:
            continue
        fenster = text[max(0, m.start() - 60):m.start()]
        # Beschriftung = das Stichwort, das dem Datum am nächsten steht
        kandidaten = []
        for art, kw in (("geburt", _KW_GEBURT), ("ausgestellt", _KW_AUSGESTELLT), ("von", _KW_VON), ("bis", _KW_BIS)):
            letzter = None
            for k in kw.finditer(fenster):
                letzter = k
            if letzter:
                kandidaten.append((letzter.end(), art))
        art = max(kandidaten)[1] if kandidaten else ""
        erg.append((d, art))
    return erg


def _zeitraum(text: str):
    daten = _daten_mit_kontext(text)
    von = next((d for d, a in daten if a == "von"), None)
    bis = next((d for d, a in daten if a == "bis" and (von is None or d >= von)), None)
    if von and bis:
        return von, bis
    frei = [d for d, a in daten if a == ""]
    if von is None and bis is None and len(frei) >= 2 and frei[1] >= frei[0]:
        return frei[0], frei[1]
    return von, bis


def _laender(text: str) -> list:
    t = str(text or "")
    gefunden = []
    for code in sorted(A1_LAENDER):
        namen = [mod_laender.land_name(code), _EN_NAMEN.get(code, "")] + list(_WEITERE_NAMEN.get(code, ()))
        for n in namen:
            if n and re.search(r"(?<![A-Za-zÄÖÜäöüß])" + re.escape(n) + r"(?![A-Za-zÄÖÜäöüß])", t, re.I):
                gefunden.append(code)
                break
    return gefunden


_STELLEN = (
    r"Deutsche\s+Rentenversicherung[^\n]{0,40}",
    r"Deutsche\s+Verbindungsstelle[^\n]{0,60}",
    r"GKV[- ]Spitzenverband[^\n]{0,40}",
    r"(?:AOK|BKK|IKK|KKH|Barmer|DAK[- ]Gesundheit|Techniker\s+Krankenkasse|Knappschaft|hkk|SBK|Mobil\s+Krankenkasse|"
    r"VIACTIV|Audi\s+BKK|Bahn[- ]BKK|energie[- ]BKK)\b[^\n]{0,40}",
)


def _aussteller(text: str, zeilen: list) -> str:
    for m_ in _STELLEN:
        m = re.search(m_, text, re.I)
        if m:
            return re.sub(r"\s+", " ", m.group(0)).strip(" ,;:-")[:80]
    wert = _wert_nach(zeilen, r"ausstellende\s+stelle|ausstellender\s+tr[äa]ger|issuing\s+institution|"
                              r"name\s+des\s+tr[äa]gers|institution")
    return wert


def a1_auswerten(text, dateiname: str = "") -> dict:
    """Liest die Angaben der A1-Bescheinigung (so gut es der Text hergibt; fehlende Angaben bleiben leer)."""
    roh = str(text or "")
    zeilen = [re.sub(r"[ \t]+", " ", z).strip() for z in roh.splitlines()]
    zeilen = [z for z in zeilen if z]
    nachname = _wert_nach(zeilen, r"nachname|familienname|surname|last\s*name|family\s*name")
    vorname = _wert_nach(zeilen, r"vorname[n]?|first\s*name|given\s*name[s]?|forename")
    if not nachname and not vorname:
        voll = _wert_nach(zeilen, r"name")
        if voll and not re.search(r"stelle|institution|tr[äa]ger|kasse|versicherung", voll, re.I):
            nachname = voll
    name = " ".join(x for x in (vorname, nachname) if x)
    von, bis = _zeitraum(roh)
    ausgestellt = next((d for d, a in _daten_mit_kontext(roh) if a == "ausgestellt"), None)
    return {"name": name, "vorname": vorname, "nachname": nachname, "von": von, "bis": bis,
            "ausgestellt": ausgestellt, "laender": _laender(roh), "aussteller": _aussteller(roh, zeilen),
            "dateiname": dateiname}


def ki_ergebnis(felder: dict, dateiname: str = "") -> dict:
    """Ergebnis im Format der KI-Beleganalyse, damit die normale Beleg-Pipeline (Ablage, Anonymisierung) weiterläuft."""
    laender = felder.get("laender") or []
    return {
        "dokumenttyp": ANZEIGE, "belegart": BELEGART, "transportart": None,
        "anbieter": felder.get("aussteller") or ANZEIGE,
        "reisender": felder.get("name") or None,
        "belegdatum": _datum_text(felder.get("ausgestellt")) or None,
        "event_datum_von": _datum_text(felder.get("von")) or None,
        "event_datum_bis": _datum_text(felder.get("bis")) or None,
        "land_beleg": laender[0] if laender else None,
        "waehrung": "EUR", "betrag_brutto": None, "zahlungsart": None,
        "pflichtfelder_ok": True, "fehlende_pflichtfelder": [],
        "a1_erkannt": True, "a1_laender": laender, "a1_dateiname": dateiname,
    }


def a1_pruefen(ki: dict) -> dict | None:
    """Falls die KI ein Dokument als A1 eingestuft hat: Ergebnis auf A1-Form bringen (keine Pflichtfeld-Fehler,
    kein Betrag). Sonst None."""
    if not isinstance(ki, dict) or (ki.get("belegart") or "") != BELEGART:
        return None
    fest = ki_ergebnis({"name": ki.get("reisender"), "aussteller": ki.get("anbieter"),
                        "von": _als_datum(ki.get("event_datum_von")), "bis": _als_datum(ki.get("event_datum_bis")),
                        "ausgestellt": _als_datum(ki.get("belegdatum")),
                        "laender": [c for c in (ki.get("a1_laender") or []) if c in A1_LAENDER]})
    return fest


def zusammenfassung(ki: dict) -> str:
    """Eine Zeile für die Übersicht nach dem Hochladen."""
    von, bis = ki.get("event_datum_von"), ki.get("event_datum_bis")
    zeitraum = f" · gültig {von} – {bis}" if von and bis else (f" · gültig ab {von}" if von else "")
    return f"{ANZEIGE}: {ki.get('reisender') or 'Name nicht erkannt'}{zeitraum}"


def duplikat_id(reise_code, ki: dict, dateiname: str = ""):
    """ID einer bereits vorhandenen, gleichen A1 (gleiche Person und gleicher Zeitraum; ohne Zeitraum: gleicher
    Dateiname) oder None. Eine A1, die schon einer ANDEREN Reise gehört, zählt nicht – sie wird dort noch einmal gebraucht."""
    von, bis = _als_datum(ki.get("event_datum_von")), _als_datum(ki.get("event_datum_bis"))
    name = ki.get("reisender") or ""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"""SELECT id, reise_code, reisender, event_datum_von, event_datum_bis, dateiname
                        FROM belege WHERE belegart={P} ORDER BY id""", (BELEGART,))
        rows = cur.fetchall()
    except Exception:
        try: db.rollback()
        except Exception: pass
        return None
    finally:
        cur.close(); db.close()
    for r in rows:
        r_reise = _g(r, "reise_code", 1)
        if r_reise and reise_code and r_reise != reise_code:
            continue
        r_name = _g(r, "reisender", 2) or ""
        gleiche_person = (not name or not r_name or _norm(name) == _norm(r_name) or namen_passen(name, r_name))
        if not gleiche_person:
            continue
        r_von, r_bis = _als_datum(_g(r, "event_datum_von", 3)), _als_datum(_g(r, "event_datum_bis", 4))
        if von and bis and r_von and r_bis:
            if (r_von, r_bis) == (von, bis):
                return _g(r, "id", 0)
        elif dateiname and (_g(r, "dateiname", 5) or "") == dateiname and name and r_name:
            return _g(r, "id", 0)
    return None


# ── Tabelle: „A1 nicht nötig“ ─────────────────────────────────────

def tabelle_sichern() -> None:
    """Merkt sich, für wen auf welcher Reise bewusst keine A1 nötig ist (läuft in Postgres und SQLite gleich)."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS a1_befreiung ("
                    "reise_code TEXT, kuerzel TEXT, grund TEXT, erstellt TEXT, PRIMARY KEY (reise_code, kuerzel))")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[A1] Tabelle a1_befreiung nicht angelegt: {ex}")


def befreiung_setzen(reise_code: str, kuerzel: str, grund: str = "") -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM a1_befreiung WHERE reise_code={P} AND kuerzel={P}", (reise_code, kuerzel))
        cur.execute(f"INSERT INTO a1_befreiung (reise_code, kuerzel, grund, erstellt) VALUES ({P},{P},{P},{P})",
                    (reise_code, kuerzel, (grund or "").strip()[:200] or None, datetime.now().strftime("%Y-%m-%d %H:%M")))
        db.commit()
    finally:
        cur.close(); db.close()


def befreiung_loeschen(reise_code: str, kuerzel: str) -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM a1_befreiung WHERE reise_code={P} AND kuerzel={P}", (reise_code, kuerzel))
        db.commit()
    finally:
        cur.close(); db.close()


def _teile(liste: list, n: int = 400):
    """Zerlegt eine lange Liste in Stücke (für IN-Abfragen; SQLite erlaubt höchstens 999 Platzhalter)."""
    for i in range(0, len(liste), n):
        yield liste[i:i + n]


def _befreiungen_fuer(codes: list) -> dict:
    """{reise_code: {kuerzel: grund}} für mehrere Reisen mit einer einzigen Abfrage."""
    erg = {c: {} for c in codes}
    if not codes:
        return erg
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        for teil in _teile(list(codes)):
            marken = ",".join([P] * len(teil))
            cur.execute(f"SELECT reise_code, kuerzel, grund FROM a1_befreiung WHERE reise_code IN ({marken})",
                        tuple(teil))
            for r in cur.fetchall():
                erg.setdefault(_g(r, "reise_code", 0), {})[_g(r, "kuerzel", 1)] = (_g(r, "grund", 2) or "")
    except Exception:
        try: db.rollback()
        except Exception: pass
        return {c: {} for c in codes}
    finally:
        cur.close(); db.close()
    return erg


def befreiungen(reise_code: str) -> dict:
    """{kuerzel: grund} für die Reise."""
    return _befreiungen_fuer([reise_code]).get(reise_code, {})


# ── Reise: Länder, Reisende, Status ───────────────────────────────

def _laender_fuer(codes: list) -> dict:
    """{reise_code: [(Code, Name)]} der Reiseländer, für die eine A1 nötig ist – mehrere Reisen, wenige Abfragen."""
    gefunden = {c: set() for c in codes}
    if not codes:
        return {}
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        for teil in _teile(list(codes)):
            marken = ",".join([P] * len(teil))
            for tabelle in ("vma_tage", "reise_laender"):
                try:
                    cur.execute(f"SELECT DISTINCT reise_code, land_code FROM {tabelle} WHERE reise_code IN ({marken})",
                                tuple(teil))
                    for r in cur.fetchall():
                        rc = _g(r, "reise_code", 0)
                        c = str(_g(r, "land_code", 1) or "").strip().upper()
                        if c in A1_LAENDER and rc in gefunden:
                            gefunden[rc].add(c)
                except Exception:
                    try: db.rollback()
                    except Exception: pass
    finally:
        cur.close(); db.close()
    return {rc: sorted(((c, mod_laender.land_name(c) or c) for c in cs), key=lambda x: x[1])
            for rc, cs in gefunden.items()}


def reise_a1_laender(reise_code: str) -> list:
    """[(Code, Name)] der Reiseländer, für die eine A1 nötig ist (EU/EWR/CH/GB, ohne Deutschland)."""
    return _laender_fuer([reise_code]).get(reise_code, [])


def _belege_fuer(codes: list) -> dict:
    """{reise_code: [{id, reisender, kuerzel, von, bis, dateiname}]} – A1-Belege mehrerer Reisen, wenige Abfragen."""
    erg = {c: [] for c in codes}
    if not codes:
        return erg
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        for teil in _teile(list(codes)):
            marken = ",".join([P] * len(teil))
            try:
                cur.execute(f"""SELECT reise_code, id, reisender, a1_kuerzel, event_datum_von, event_datum_bis, dateiname
                                FROM belege WHERE reise_code IN ({marken}) AND belegart={P} ORDER BY id""",
                            tuple(teil) + (BELEGART,))
            except Exception:
                db.rollback()
                cur.execute(f"""SELECT reise_code, id, reisender, NULL, event_datum_von, event_datum_bis, dateiname
                                FROM belege WHERE reise_code IN ({marken}) AND belegart={P} ORDER BY id""",
                            tuple(teil) + (BELEGART,))
            for r in cur.fetchall():
                erg.setdefault(_g(r, "reise_code", 0), []).append(
                    {"id": _g(r, "id", 1), "reisender": _g(r, "reisender", 2) or "",
                     "kuerzel": _g(r, "a1_kuerzel", 3) or "",
                     "von": _als_datum(_g(r, "event_datum_von", 4)), "bis": _als_datum(_g(r, "event_datum_bis", 5)),
                     "dateiname": _g(r, "dateiname", 6) or ""})
    finally:
        cur.close(); db.close()
    return erg


def a1_belege(reise_code: str) -> list:
    """Alle A1-Belege der Reise: [{id, reisender, kuerzel, von, bis, dateiname}]"""
    return _belege_fuer([reise_code]).get(reise_code, [])


def _reisende_fuer(codes: list) -> dict:
    """{reise_code: [(kuerzel, klarname, email)]} – dieselbe Reihenfolge wie mod_portal.reisende_der_reise."""
    erg = {c: [] for c in codes}
    if not codes:
        return erg
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        for teil in _teile(list(codes)):
            marken = ",".join([P] * len(teil))
            cur.execute(f"""SELECT rm.reise_code, m.kuerzel, m.klarname, m.email
                            FROM reise_mitarbeiter rm JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel
                            WHERE rm.reise_code IN ({marken}) ORDER BY m.klarname""", tuple(teil))
            for r in cur.fetchall():
                erg.setdefault(_g(r, "reise_code", 0), []).append(
                    (_g(r, "kuerzel", 1), _g(r, "klarname", 2), _g(r, "email", 3)))
    finally:
        cur.close(); db.close()
    return erg


def _reisezeitraum(reise_code: str):
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    return (_als_datum(_g(r, "abreise", 0)), _als_datum(_g(r, "rueckkehr", 1))) if r else (None, None)


def a1_status(reise_code: str, reisende: list) -> dict:
    """
    Status der A1-Bescheinigungen einer Reise.
    reisende: [(kuerzel, klarname, email)] (wie mod_portal.reisende_der_reise).
    Ergebnis: {"noetig": bool, "laender": [(code,name)], "personen": [...], "nicht_zugeordnet": [Belege]}
    je Person: {kuerzel, klarname, beleg, status, hinweis}
      status: "ok" (vorhanden, deckt die Reise), "pruefen" (vorhanden, Zeitraum fehlt/deckt nicht),
              "fehlt" (nichts vorhanden), "befreit" (bewusst nicht nötig)
    """
    laender = reise_a1_laender(reise_code)
    belege = a1_belege(reise_code)
    frei = befreiungen(reise_code)
    ab, zu = _reisezeitraum(reise_code)
    return _a1_status_berechnen(laender, belege, frei, ab, zu, reisende)


def _a1_status_berechnen(laender: list, belege: list, frei: dict, ab, zu, reisende: list) -> dict:
    """Reine Rechnung ohne Datenbankzugriff (wird auch von a1_todos mit gesammelten Daten genutzt)."""
    verwendet = set()
    personen = []
    for r in reisende:
        k, name = _g(r, "kuerzel", 0), _g(r, "klarname", 1) or ""
        # fest zugeordnete A1 (Kürzel) und noch nicht zugeordnete A1 mit passendem Namen
        passend = [b for b in belege if b["kuerzel"] == k or (not b["kuerzel"] and namen_passen(b["reisender"], name))]
        eintrag = {"kuerzel": k, "klarname": name, "beleg": None, "status": "fehlt", "hinweis": ""}
        if passend:
            b = passend[-1]                       # neueste Bescheinigung zuerst prüfen
            for kand in reversed(passend):
                if _deckt(kand, ab, zu) is True:
                    b = kand
                    break
            verwendet.update(x["id"] for x in passend)
            eintrag["beleg"] = b
            deckt = _deckt(b, ab, zu)
            if deckt is True:
                eintrag["status"] = "ok"
            elif deckt is None:
                eintrag["status"] = "pruefen"
                eintrag["hinweis"] = "Gültigkeitszeitraum nicht erkannt – bitte prüfen"
            else:
                eintrag["status"] = "pruefen"
                eintrag["hinweis"] = (f"gilt {_datum_text(b['von'])} – {_datum_text(b['bis'])} "
                                      f"und deckt die Reise nicht ganz ab")
        elif k in frei:
            eintrag["status"] = "befreit"
            eintrag["hinweis"] = frei[k]
        personen.append(eintrag)
    offen = [b for b in belege if b["id"] not in verwendet]
    return {"noetig": bool(laender), "laender": laender, "personen": personen, "nicht_zugeordnet": offen}


def _deckt(b: dict, ab, zu):
    """True = Zeitraum deckt die Reise, False = deckt nicht, None = nicht prüfbar."""
    if not b.get("von") or not b.get("bis") or not ab or not zu:
        return None
    return b["von"] <= ab and b["bis"] >= zu


def a1_zuordnen(beleg_id: int, kuerzel: str, klarname: str | None = None) -> None:
    """Ordnet eine A1 einem Reisenden fest zu (Name im Beleg bleibt unverändert, wenn er schon passt)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        if not klarname:
            cur.execute(f"SELECT klarname FROM mitarbeiter WHERE kuerzel={P}", (kuerzel,))
            m = cur.fetchone()
            klarname = (_g(m, "klarname", 0) or "") if m else ""
        cur.execute(f"SELECT reisender FROM belege WHERE id={P}", (beleg_id,))
        r = cur.fetchone()
        alt = (_g(r, "reisender", 0) or "") if r else ""
        neu = alt if (namen_passen(alt, klarname) or not klarname) else klarname
        cur.execute(f"UPDATE belege SET a1_kuerzel={P}, reisender={P} WHERE id={P}", (kuerzel, neu, beleg_id))
        db.commit()
    finally:
        cur.close(); db.close()


def bestehende_zuordnen(beleg_id: int, reise_code: str, kuerzel: str | None) -> None:
    """Eine schon vorhandene A1 (Duplikat beim erneuten Hochladen) der Reise und dem Mitarbeiter zuordnen,
    falls sie bisher keiner Reise gehört."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE belege SET reise_code={P} WHERE id={P} AND reise_code IS NULL", (reise_code, beleg_id))
        db.commit()
    finally:
        cur.close(); db.close()
    if kuerzel:
        a1_zuordnen(beleg_id, kuerzel)


# ── To-do-Liste ───────────────────────────────────────────────────

def a1_todos(heute: date | None = None) -> list:
    """
    Offene A1-Aufgaben für Reisen, die noch nicht zu Ende sind:
    [{reise_code, titel, abreise, rueckkehr, laender:[(c,n)], fehlt:[Klarnamen], pruefen:[Klarnamen], nicht_zugeordnet:int}]
    Die Daten aller Reisen werden gesammelt abgefragt (5 Abfragen insgesamt statt 5 je Reise).
    """
    heute = heute or date.today()
    db = get_db(); cur = db.cursor()
    P = ph()
    try:
        cur.execute(f"SELECT code, titel, abreise, rueckkehr FROM reisen WHERE rueckkehr >= {P} ORDER BY abreise",
                    (heute.isoformat(),))
        reisen = cur.fetchall()
    finally:
        cur.close(); db.close()
    if not reisen:
        return []
    codes = [_g(r, "code", 0) for r in reisen]
    try:
        laender_je = _laender_fuer(codes)
        belege_je = _belege_fuer(codes)
        frei_je = _befreiungen_fuer(codes)
        reisende_je = _reisende_fuer(codes)
    except Exception:
        return []
    erg = []
    for r in reisen:
        code = _g(r, "code", 0)
        ab, zu = _als_datum(_g(r, "abreise", 2)), _als_datum(_g(r, "rueckkehr", 3))
        try:
            st = _a1_status_berechnen(laender_je.get(code, []), belege_je.get(code, []), frei_je.get(code, {}),
                                      ab, zu, reisende_je.get(code, []))
        except Exception:
            continue
        if not st["noetig"]:
            continue
        fehlt = [p["klarname"] for p in st["personen"] if p["status"] == "fehlt"]
        pruefen = [p["klarname"] for p in st["personen"] if p["status"] == "pruefen"]
        if not st["personen"] and not st["nicht_zugeordnet"]:
            fehlt = ["(noch kein Reisender zugeordnet)"]
        if fehlt or pruefen or st["nicht_zugeordnet"]:
            erg.append({"reise_code": code, "titel": _g(r, "titel", 1) or "",
                        "abreise": ab, "rueckkehr": zu,
                        "laender": st["laender"], "fehlt": fehlt, "pruefen": pruefen,
                        "nicht_zugeordnet": len(st["nicht_zugeordnet"])})
    return erg


def todo_text(t: dict) -> str:
    """Kurzer Text für eine A1-Aufgabe (ohne HTML)."""
    teile = []
    if t["fehlt"]:
        teile.append("A1 fehlt: " + ", ".join(t["fehlt"]))
    if t["pruefen"]:
        teile.append("A1 prüfen: " + ", ".join(t["pruefen"]))
    if t["nicht_zugeordnet"]:
        teile.append(f"{t['nicht_zugeordnet']} A1 ohne Zuordnung")
    return " · ".join(teile)


def laender_text(laender: list) -> str:
    return ", ".join(escape(n) for _c, n in laender)
