"""
mod_visum.py – Einreisegenehmigungen (eTA, ESTA, eVisa, ETIAS …) als Belegart (v3.50)

Eine Einreisegenehmigung ist oft schon Wochen vor der Reise beantragt und kostet eine Gebühr. Dieses Modul
  * erkennt sie (KI-Ergebnis plus feste Regeln für „eTA“, „ESTA“, „eVisa“ … in Text und Dateiname),
  * ordnet sie als Kategorie „Visum“ ein (Belegart „Einreisegenehmigung“, solange kein Betrag auf dem Dokument steht;
    mit Betrag bleibt es eine Quittung bzw. Rechnung, damit die Kosten wie jeder Beleg laufen),
  * liest Art, Land, Gültigkeit, Einreisen, Aufenthaltsdauer und Antragsnummer aus (Feld „visum“ im KI-Ergebnis),
  * erstellt daraus einen Vorschlag für das Visa-Profil des Mitarbeiters (Beleg-Seite, Übernahme mit einem Klick).
Es wird nichts geraten: Was nicht auf dem Dokument steht, bleibt leer und muss beim Übernehmen eingetragen werden.
"""
from __future__ import annotations
import json
import re
from datetime import date, datetime
from decimal import Decimal
from html import escape

from mod_db import get_db, ph
import mod_laender
import mod_a1

BELEGART = "Einreisegenehmigung"
TRANSPORTART = "Visum"
ANZEIGE = "Einreisegenehmigung (eTA/ESTA/eVisa)"
PUNKTE_MIN = 3          # so viele Punkte braucht die Regel-Erkennung (ein schwacher Treffer allein genügt nie)


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def _s(v) -> str:
    return str(v).strip() if v is not None else ""


def _json_default(o):
    """Postgres liefert Beträge als Decimal und Daten als date – beides muss in JSON geschrieben werden können."""
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _json(o) -> str:
    return json.dumps(o, ensure_ascii=False, default=_json_default)


# ── Datum ─────────────────────────────────────────────────────────

_MONATE = {"jan": 1, "january": 1, "januar": 1, "feb": 2, "february": 2, "februar": 2, "mar": 3, "march": 3,
           "mär": 3, "maerz": 3, "märz": 3, "apr": 4, "april": 4, "may": 5, "mai": 5, "jun": 6, "june": 6,
           "juni": 6, "jul": 7, "july": 7, "juli": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
           "september": 9, "oct": 10, "october": 10, "okt": 10, "oktober": 10, "nov": 11, "november": 11,
           "dec": 12, "december": 12, "dez": 12, "dezember": 12}
_DAT = (r"(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[./]\d{1,2}[./]\d{4}|\d{1,2}[ \-][A-Za-zäÄ]{3,9}\.?,?[ \-]\d{4}"
        r"|[A-Za-zäÄ]{3,9}\.? \d{1,2},? \d{4})")


def _mk(j, m, t):
    try:
        return date(int(j), int(m), int(t))
    except (TypeError, ValueError):
        return None


def datum_aus(text):
    """Text -> date. Erkennt JJJJ-MM-TT, TT.MM.JJJJ, „12 March 2031“, „March 12, 2031“ und TT/MM/JJJJ bzw. MM/TT/JJJJ,
    wenn eindeutig (sonst None – es wird nicht geraten)."""
    if isinstance(text, datetime):
        return text.date()
    if isinstance(text, date):
        return text
    t = _s(text)
    if not t:
        return None
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", t)
    if m:
        return _mk(m.group(1), m.group(2), m.group(3))
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", t)
    if m:
        return _mk(m.group(3), m.group(2), m.group(1))
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", t)
    if m:
        a, b, j = int(m.group(1)), int(m.group(2)), m.group(3)
        if a > 12 >= b:
            return _mk(j, b, a)
        if b > 12 >= a:
            return _mk(j, a, b)
        return None                      # 03/04/2028: Tag oder Monat zuerst? Nicht raten.
    m = re.search(r"(\d{1,2})[ \-]([A-Za-zäÄ]{3,9})\.?,?[ \-](\d{4})", t)
    if m and m.group(2).lower() in _MONATE:
        return _mk(m.group(3), _MONATE[m.group(2).lower()], m.group(1))
    m = re.search(r"([A-Za-zäÄ]{3,9})\.? (\d{1,2}),? (\d{4})", t)
    if m and m.group(1).lower() in _MONATE:
        return _mk(m.group(3), _MONATE[m.group(1).lower()], m.group(2))
    return None


def datum_text(d) -> str:
    return d.strftime("%d.%m.%Y") if d else ""


def _datum_nach(text: str, labels: str):
    m = re.search(rf"(?:{labels})\s*[:\-–]?\s*{_DAT}", text or "", re.I)
    return datum_aus(m.group(1)) if m else None


_L_BIS = (r"valid\s+(?:until|through|thru|till|to)|good\s+until|expir(?:es|y|ation)(?:\s+date)?(?:\s+on)?|"
          r"gültig\s+bis(?:\s+zum)?|ablaufdatum|verfällt\s+am")
_L_VON = (r"valid\s+from|date\s+issued|issue\s+date|date\s+of\s+issue|issued(?:\s+on)?|approved\s+on|"
          r"approval\s+date|date\s+approved|ausgestellt\s+am|ausstellungsdatum|gültig\s+ab")


# ── Erkennung ─────────────────────────────────────────────────────

# (Muster, Punkte, Groß-/Kleinschreibung beachten)
_REGELN = (
    (r"electronic\s+system\s+for\s+travel\s+authori[sz]ation", 3, False),
    (r"esta\.cbp\.dhs\.gov", 3, False),
    (r"\bESTA\b", 2, True),
    (r"\bETIAS\b|european\s+travel\s+information\s+and\s+authori[sz]ation", 3, True),
    (r"electronic\s+travel\s+authori[sz]ation|electronic\s+travel\s+authority", 3, False),
    (r"\beTA\b", 2, True),
    (r"\bNZeTA\b|\bK-?ETA\b|\bUK\s+ETA\b", 3, True),
    (r"\be-?visa\b|\belectronic\s+visa\b|\bevisitor\b", 3, False),
    (r"visa\s+(?:application|approval|approved|grant|fee|on\s+arrival)", 3, False),
    (r"\bvisum\b|visumantrag|visumgeb|visumsgeb", 2, False),
    (r"einreisegenehmigung|einreiseerlaubnis|reisegenehmigung", 3, False),
    (r"(?:entry|travel)\s+(?:permit|authori[sz]ation)", 2, False),
    (r"immigration,?\s+refugees\s+and\s+citizenship\s+canada|\bIRCC\b", 3, True),
)
_REGELN_C = tuple((re.compile(p, 0 if fall else re.I), pt) for p, pt, fall in _REGELN)
_DATEINAME_WOERTER = {"eta": 3, "esta": 3, "evisa": 3, "etias": 3, "visum": 3, "einreisegenehmigung": 3, "visa": 1}


def erkennungs_punkte(text: str = "", dateiname: str = "") -> int:
    """Wie deutlich sieht das Dokument nach Einreisegenehmigung aus? Jedes Muster zählt einmal.
    „Visa“ allein (Kreditkarte) und „ETA“ (Ankunftszeit) zählen nicht oder kaum."""
    punkte = 0
    for rx, pt in _REGELN_C:
        if rx.search(text or ""):
            punkte += pt
    woerter = [w for w in re.split(r"[^a-z0-9]+", _s(dateiname).lower()) if w]
    punkte += sum(pt for w, pt in _DATEINAME_WOERTER.items() if w in woerter)
    return punkte


def ist_visum_dokument(text: str = "", dateiname: str = "") -> bool:
    return erkennungs_punkte(text, dateiname) >= PUNKTE_MIN


_ART_REGELN = (
    ("ESTA", re.compile(r"\bESTA\b|(?i:electronic\s+system\s+for\s+travel)")),
    ("ETIAS", re.compile(r"\bETIAS\b")),
    ("eTA", re.compile(r"\beTA\b|\bNZeTA\b|\bK-?ETA\b|\bUK\s+ETA\b|"
                       r"(?i:electronic\s+travel\s+authori[sz]ation|electronic\s+travel\s+authority)")),
    ("eVisa", re.compile(r"\be-?visa\b|\belectronic\s+visa\b|\bevisitor\b", re.I)),
    ("Visum", re.compile(r"\bvisum\b|visa\s+(?:application|approval|grant|fee)|einreisegenehmigung", re.I)),
)


def art_aus_text(text: str = "", dateiname: str = "") -> str:
    """Art der Genehmigung aus Text und Dateiname. „ESTA“, „ETIAS“ und „eTA“ gelten nur in dieser Schreibweise
    („esta“ ist spanisch, „ETA“ heißt im Flugverkehr Ankunftszeit)."""
    quelle = f"{_s(dateiname).replace('_', ' ')} {text or ''}"
    for art, rx in _ART_REGELN:
        if rx.search(quelle):
            return art
    return ""


def art_normalisieren(v) -> str:
    t = _s(v)
    k = re.sub(r"[^a-z0-9]+", "", t.lower())
    if not k:
        return ""
    if k.startswith("esta") or "electronicsystemfortravel" in k:
        return "ESTA"
    if "etias" in k:
        return "ETIAS"
    if "evisa" in k or "electronicvisa" in k or "evisitor" in k:
        return "eVisa"
    if re.fullmatch(r"(?:uk|nz|k)?eta", k) or "electronictravelauthori" in k:
        return "eTA"
    if "visum" in k or k.startswith("visa"):
        return "Visum"
    return t[:24]


# Englische Ländernamen häufiger Einreisegenehmigungen (deutsche Namen stehen in mod_laender)
_EN_LAENDER = {
    "canada": "CA", "india": "IN", "united states": "US", "usa": "US", "u.s.a.": "US", "australia": "AU",
    "new zealand": "NZ", "united kingdom": "GB", "great britain": "GB", "sri lanka": "LK", "turkey": "TR",
    "türkiye": "TR", "turkiye": "TR", "vietnam": "VN", "viet nam": "VN", "kenya": "KE", "egypt": "EG",
    "indonesia": "ID", "thailand": "TH", "south korea": "KR", "republic of korea": "KR", "japan": "JP",
    "brazil": "BR", "saudi arabia": "SA", "united arab emirates": "AE", "tanzania": "TZ", "uganda": "UG",
    "ghana": "GH", "nigeria": "NG", "morocco": "MA", "cambodia": "KH", "china": "CN", "mexico": "MX",
    "south africa": "ZA", "singapore": "SG", "malaysia": "MY", "philippines": "PH", "israel": "IL",
    "jordan": "JO", "oman": "OM", "qatar": "QA", "bahrain": "BH", "kuwait": "KW", "zambia": "ZM",
    "zimbabwe": "ZW", "ethiopia": "ET", "rwanda": "RW", "azerbaijan": "AZ", "uzbekistan": "UZ",
    "kazakhstan": "KZ", "myanmar": "MM", "laos": "LA", "bangladesh": "BD", "pakistan": "PK", "nepal": "NP",
    "argentina": "AR", "chile": "CL", "colombia": "CO", "peru": "PE", "russia": "RU", "ukraine": "UA",
    "serbia": "RS", "albania": "AL", "angola": "AO", "mozambique": "MZ", "namibia": "NA", "cuba": "CU",
}
_STANDARD_LAND = {"ESTA": "US"}


def land_pruefen(code) -> str:
    c = _s(code).upper()
    return c if c in mod_laender.LAND_NAMEN else ""


def _land_aus_text(text: str) -> str:
    """Am häufigsten genanntes Land im Text (Deutschland zählt nicht: das ist der Pass, nicht das Ziel)."""
    t = (text or "").lower()
    zaehler: dict = {}
    namen = dict(_EN_LAENDER)
    for code, name in mod_laender.LAND_NAMEN.items():
        namen.setdefault(name.lower(), code)
    for name, code in namen.items():
        if code == "DE" or len(name) < 4:
            continue
        n = len(re.findall(r"(?<![a-zäöüß])" + re.escape(name) + r"(?![a-zäöüß])", t))
        if n:
            zaehler[code] = zaehler.get(code, 0) + n
    if not zaehler:
        return ""
    return sorted(zaehler.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


# ── Felder lesen ──────────────────────────────────────────────────

_NR_RX = re.compile(
    r"\b(?:application|reference|confirmation|authori[sz]ation|eta|esta|visa|travel\s+authori[sz]ation|file|request)"
    r"\s*(?:(?:number|no\.?|nr\.?|id)(?![A-Za-z])|#)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-]{5,19})"
    r"|\b(?:antragsnummer|referenznummer|genehmigungsnummer|aktenzeichen)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-]{5,19})", re.I)


def _antragsnummer(text: str) -> str:
    for m in _NR_RX.finditer(text or ""):
        nr = m.group(1) or m.group(2) or ""
        if any(c.isdigit() for c in nr):
            return nr.upper()
    return ""


def _einreisen(v) -> str:
    k = _s(v).lower()
    if not k:
        return ""
    if "mehr" in k or "multi" in k:
        return "mehrfach"
    if "einfach" in k or "einmal" in k or "single" in k or k == "ein":
        return "einfach"
    return ""


def _tage(v):
    m = re.search(r"\d{1,4}", _s(v))
    if not m:
        return None
    n = int(m.group(0))
    return n if 1 <= n <= 3650 else None


def felder_aus_text(text: str) -> dict:
    """Rückfall, wenn die KI etwas nicht geliefert hat: Angaben, die klar im Text stehen."""
    t = text or ""
    erg = {"gueltig_von": _datum_nach(t, _L_VON), "gueltig_bis": _datum_nach(t, _L_BIS),
           "antragsnummer": _antragsnummer(t), "einreisen": "", "max_aufenthalt_tage": None}
    zr = re.search(rf"valid\s+from\s+{_DAT}\s+(?:until|to|through|thru)\s+{_DAT}", t, re.I)
    if zr:
        erg["gueltig_von"] = erg["gueltig_von"] or datum_aus(zr.group(1))
        erg["gueltig_bis"] = erg["gueltig_bis"] or datum_aus(zr.group(2))
    if re.search(r"multiple[\s\-]*entr|multi[\s\-]*entr|mehrfach", t, re.I):
        erg["einreisen"] = "mehrfach"
    elif re.search(r"single[\s\-]*entr|einmalige\s+einreise|einfache\s+einreise", t, re.I):
        erg["einreisen"] = "einfach"
    m = (re.search(r"(?:stay|aufenthalt)[^.\n]{0,45}?(\d{1,3})\s*(?:days|tage)", t, re.I)
         or re.search(r"(\d{1,3})\s*(?:days|tage)[^.\n]{0,30}?(?:per|each|je|pro)\s*(?:visit|stay|entry|aufenthalt|einreise)", t, re.I))
    if m:
        erg["max_aufenthalt_tage"] = _tage(m.group(1))
    return erg


def visum_felder(ki: dict, text: str = "", dateiname: str = "") -> dict:
    """Die Visum-Angaben: zuerst was die KI geliefert hat, sonst was klar im Text steht. Fehlt etwas, bleibt es leer."""
    ai = ki.get("visum") if isinstance(ki.get("visum"), dict) else {}
    rueck = felder_aus_text(text)
    art = art_normalisieren(ai.get("art")) or art_aus_text(text, dateiname) or "Visum"
    land = (land_pruefen(ai.get("land_code")) or
            (land_pruefen(ki.get("land_beleg")) if land_pruefen(ki.get("land_beleg")) != "DE" else "") or
            _land_aus_text(f"{_s(dateiname).replace('_', ' ')} {text or ''}") or _STANDARD_LAND.get(art, ""))
    von = datum_aus(ai.get("gueltig_von")) or rueck["gueltig_von"]
    bis = datum_aus(ai.get("gueltig_bis")) or rueck["gueltig_bis"]
    return {
        "art": art, "land_code": land,
        "gueltig_von": datum_text(von), "gueltig_bis": datum_text(bis),
        "einreisen": _einreisen(ai.get("einreisen")) or rueck["einreisen"],
        "max_aufenthalt_tage": _tage(ai.get("max_aufenthalt_tage")) or rueck["max_aufenthalt_tage"],
        "antragsnummer": _s(ai.get("antragsnummer")) or rueck["antragsnummer"] or _s(ki.get("buchungscode")),
        "status": "offen",
    }


def _betrag(ki: dict) -> float:
    try:
        return float(ki.get("betrag_brutto") or 0)
    except (TypeError, ValueError):
        return 0.0


def nachbearbeiten(ki: dict, rohtext: str = "", dateiname: str = "", manuell: bool = False) -> dict:
    """
    Nach der KI-Analyse: Ist es eine Einreisegenehmigung (laut KI oder laut Regeln), wird das Ergebnis passend
    gesetzt – Kategorie „Visum“, Belegart, Land, Visum-Angaben und die Pflichtfelder. Alle anderen Belege bleiben unberührt.
    manuell=True: Jemand hat die Belegart „Einreisegenehmigung“ selbst gewählt; sie bleibt auch dann, wenn ein Betrag da ist.
    """
    if not isinstance(ki, dict) or ki.get("fehler"):
        return ki
    ai = ki.get("visum") if isinstance(ki.get("visum"), dict) else {}
    transport = _s(ki.get("transportart"))
    art_alt = _s(ki.get("belegart"))
    ki_sagt = (manuell or art_alt == BELEGART or transport == TRANSPORTART
               or bool(art_normalisieren(ai.get("art")) and land_pruefen(ai.get("land_code"))))
    regel = (not ki_sagt and transport in ("", "Sonstiges")
             and art_alt not in ("Buchungsbestaetigung", "Flugaenderung", "A1Bescheinigung")
             and ist_visum_dokument(rohtext, dateiname))
    if not (ki_sagt or regel):
        return ki
    v = visum_felder(ki, rohtext, dateiname)
    ki["visum"] = v
    ki["transportart"] = TRANSPORTART
    frei = _s(ki.get("transportart_freitext"))
    if not frei or frei.lower() == "sonstiges":
        ki["transportart_freitext"] = f"{v['art']} {mod_laender.land_name(v['land_code'])}".strip()
    hat_betrag = _betrag(ki) > 0
    if manuell:
        ki["belegart"] = BELEGART
    elif hat_betrag:
        ki["belegart"] = art_alt if art_alt in ("Rechnung", "Quittung") else "Quittung"
    else:
        ki["belegart"] = BELEGART
    if v["land_code"] and not land_pruefen(ki.get("land_beleg")):
        ki["land_beleg"] = v["land_code"]
    if not _s(ki.get("buchungscode")) and v["antragsnummer"]:
        ki["buchungscode"] = v["antragsnummer"]
    # Die Gültigkeit steht im Feld „visum“; das Ereignisdatum ist der Tag des Belegs (so zeigt der Tagesverlauf den Beleg am Zahltag)
    ki["event_datum_von"] = ki.get("belegdatum")
    ki["event_datum_bis"] = None
    pflicht = ["belegdatum", "betrag_brutto", "waehrung", "zahlungsart"] if (hat_betrag and not manuell) else []
    fehlend = [f for f in pflicht if not ki.get(f)]
    ki["pflichtfelder_ok"] = not fehlend
    ki["fehlende_pflichtfelder"] = fehlend
    return ki


def ist_visum_beleg(transportart, belegart, ki: dict | None = None) -> bool:
    return (_s(transportart) == TRANSPORTART or _s(belegart) == BELEGART
            or bool(ki and isinstance(ki.get("visum"), dict) and ki["visum"].get("art")))


# ── Vorschlag fürs Visa-Profil ────────────────────────────────────

def notiz_text(v: dict) -> str:
    """Kurztext für das Feld „Notiz“ des Visums: Art, Einreisen, Aufenthaltsdauer, Antragsnummer (höchstens 200 Zeichen)."""
    teile = [_s(v.get("art")) or "Visum"]
    e = _s(v.get("einreisen"))
    if e == "mehrfach":
        teile.append("Mehrfacheinreise")
    elif e == "einfach":
        teile.append("Einmalige Einreise")
    if v.get("max_aufenthalt_tage"):
        teile.append(f"max. {v['max_aufenthalt_tage']} Tage je Aufenthalt")
    if _s(v.get("antragsnummer")):
        teile.append(f"Antrag {_s(v['antragsnummer'])}")
    return " · ".join(teile)[:200]


def vorschlag(ki: dict, reisezeitraum: tuple = (None, None)) -> dict | None:
    """Der Vorschlag zu einem Beleg (None, wenn der Beleg keine Einreisegenehmigung ist), samt Abgleich mit der Reise."""
    v = ki.get("visum") if isinstance(ki, dict) else None
    if not isinstance(v, dict) or not v.get("art"):
        return None
    von, bis = datum_aus(v.get("gueltig_von")), datum_aus(v.get("gueltig_bis"))
    land = land_pruefen(v.get("land_code"))
    erg = {"art": _s(v.get("art")), "land_code": land, "land_name": mod_laender.land_name(land),
           "von": von, "bis": bis, "einreisen": _s(v.get("einreisen")), "max_tage": v.get("max_aufenthalt_tage"),
           "antragsnummer": _s(v.get("antragsnummer")), "notiz": notiz_text(v),
           "status": _s(v.get("status")) or "offen", "visa_id": v.get("visa_id"), "kuerzel": _s(v.get("kuerzel")),
           "hinweise": []}
    ab, zu = reisezeitraum
    if ab and zu:
        if bis and bis < zu:
            erg["hinweise"].append(("warn", f"Läuft am {datum_text(bis)} ab – vor Reiseende ({datum_text(zu)})."))
        elif von and von > ab:
            erg["hinweise"].append(("warn", f"Gilt erst ab {datum_text(von)} – nach Reisebeginn ({datum_text(ab)})."))
        elif bis:
            erg["hinweise"].append(("ok", f"Deckt die Reise {datum_text(ab)} – {datum_text(zu)} ab."))
        if erg["max_tage"]:
            try:
                dauer = (zu - ab).days + 1
                if dauer > int(erg["max_tage"]):
                    erg["hinweise"].append(("warn", f"Die Reise dauert {dauer} Tage, erlaubt sind {erg['max_tage']} Tage je Aufenthalt."))
            except (TypeError, ValueError):
                pass
    if von and bis and bis < von:
        erg["hinweise"].append(("warn", "„Gültig bis“ liegt vor „gültig von“ – bitte prüfen."))
    return erg


# ── Mitarbeiter finden ────────────────────────────────────────────

def kandidaten(reisender: str, reise_code: str | None) -> tuple:
    """(Vorauswahl-Kürzel oder '', [(kuerzel, klarname, auf_der_reise)]) – Reisende der Reise zuerst.
    Vorausgewählt wird nur, wenn der Name auf dem Dokument genau zu einer Person passt."""
    db = get_db(); cur = db.cursor()
    try:
        cur.execute("SELECT kuerzel, klarname, aktiv FROM mitarbeiter ORDER BY klarname")
        alle = [(_g(r, "kuerzel", 0), _g(r, "klarname", 1) or _g(r, "kuerzel", 0), _g(r, "aktiv", 2)) for r in cur.fetchall()]
    except Exception:
        alle = []
    finally:
        cur.close(); db.close()
    alle = [(k, n) for k, n, a in alle if a is None or bool(a)]
    auf_reise = set()
    if reise_code:
        try:
            auf_reise = {k for k, _n, _m in mod_a1._reisende_fuer([reise_code]).get(reise_code, [])}
        except Exception:
            auf_reise = set()
    passend = [k for k, n in alle if _s(reisender) and mod_a1.namen_passen(reisender, n)]
    wahl = ""
    if len(passend) == 1:
        wahl = passend[0]
    elif len(passend) > 1:
        auf = [k for k in passend if k in auf_reise]
        wahl = auf[0] if len(auf) == 1 else ""
    liste = sorted(((k, n, k in auf_reise) for k, n in alle), key=lambda x: (not x[2], x[1].lower()))
    return wahl, liste


# ── Speichern der Entscheidung ────────────────────────────────────

def _ki_laden(bid: int):
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT ki_json, reise_code, s3_original, dateiname FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    if not r:
        return None
    try:
        ki = json.loads(_g(r, "ki_json", 0) or "{}")
    except Exception:
        ki = {}
    return {"ki": ki if isinstance(ki, dict) else {}, "reise_code": _g(r, "reise_code", 1),
            "s3_original": _g(r, "s3_original", 2), "dateiname": _g(r, "dateiname", 3) or ""}


def _ki_speichern(bid: int, ki: dict) -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE belege SET ki_json={P} WHERE id={P}", (_json(ki), bid))
        db.commit()
    finally:
        cur.close(); db.close()


def status_setzen(bid: int, status: str, visa_id=None, kuerzel: str = "") -> bool:
    """Vermerkt am Beleg: offen | uebernommen | verworfen. Gibt False zurück, wenn es keinen Vorschlag gibt."""
    d = _ki_laden(bid)
    if not d or not isinstance(d["ki"].get("visum"), dict):
        return False
    d["ki"]["visum"]["status"] = status
    if visa_id is not None:
        d["ki"]["visum"]["visa_id"] = visa_id
        d["ki"]["visum"]["kuerzel"] = kuerzel
    if status == "offen":
        d["ki"]["visum"].pop("visa_id", None)
        d["ki"]["visum"].pop("kuerzel", None)
    _ki_speichern(bid, d["ki"])
    return True


def uebernehmen(bid: int, kuerzel: str, land: str, von, bis, notiz: str = "") -> dict:
    """
    Legt das Visum im Profil des Mitarbeiters an (mit der Kopie des Belegs als Datei) und vermerkt es am Beleg.
    Gibt es für Mitarbeiter, Land und Zeitraum schon ein Visum, wird es nur verknüpft (kein Duplikat).
    Rückgabe: {visa_id, kuerzel, neu: bool, datei: bool}; ungültige Eingaben -> ValueError mit klarem Text.
    """
    import mod_visa
    d = _ki_laden(bid)
    if not d:
        raise ValueError("Beleg nicht gefunden.")
    kz = _s(kuerzel).upper()
    if not kz:
        raise ValueError("Bitte den Mitarbeiter wählen.")
    code, d_von, d_bis = mod_visa.felder_pruefen(land, von, bis)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT kuerzel FROM mitarbeiter WHERE kuerzel={P}", (kz,))
        if not cur.fetchone():
            raise ValueError("Diesen Mitarbeiter gibt es nicht.")
    finally:
        cur.close(); db.close()
    vorhanden = next((x for x in mod_visa.liste(kz)
                      if x["land_code"] == code and x["von"] == d_von and x["bis"] == d_bis), None)
    if vorhanden:
        status_setzen(bid, "uebernommen", vorhanden["id"], kz)
        return {"visa_id": vorhanden["id"], "kuerzel": kz, "neu": False, "datei": vorhanden["hat_datei"]}
    daten = None
    if d["s3_original"]:
        try:
            from mod_beleg import s3_download
            daten = s3_download(d["s3_original"])
        except Exception as ex:
            print(f"[Visum] Kopie des Belegs nicht geladen: {ex}")
    text = _s(notiz) or notiz_text(d["ki"].get("visum") or {})
    datei_ok = bool(daten)
    try:
        vid = mod_visa.hinzufuegen(kz, code, d_von, d_bis, text, daten, d["dateiname"], "application/pdf")
    except ValueError:
        if not daten:
            raise
        # Datei nicht brauchbar (z. B. zu groß): Visum trotzdem anlegen, die Kopie bleibt am Beleg
        vid = mod_visa.hinzufuegen(kz, code, d_von, d_bis, text)
        datei_ok = False
    status_setzen(bid, "uebernommen", vid, kz)
    return {"visa_id": vid, "kuerzel": kz, "neu": True, "datei": datei_ok}


def beleg_einstufen(bid: int) -> bool:
    """
    Stuft einen vorhandenen Beleg als Einreisegenehmigung ein (Belegart wurde von Hand gewählt oder der Hinweis auf der
    Beleg-Seite bestätigt): Visum-Angaben aus Text und KI-Daten lesen, Kategorie, Land, Pflichtfelder und Status setzen.
    Betrag, Bezahlart und Reise bleiben unverändert. Gibt False zurück, wenn es den Beleg nicht gibt.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"""SELECT ki_json, rohtext, dateiname, belegdatum, betrag_brutto, transportart_freitext, land_beleg,
                        buchungscode, reisender FROM belege WHERE id={P}""", (bid,))
        r = cur.fetchone()
        if not r:
            return False
        try:
            ki = json.loads(_g(r, "ki_json", 0) or "{}")
        except Exception:
            ki = {}
        ki = ki if isinstance(ki, dict) else {}
        ki.pop("fehler", None)
        bd = datum_aus(_g(r, "belegdatum", 3))
        if bd:
            ki["belegdatum"] = datum_text(bd)
        try:
            ki["betrag_brutto"] = float(_g(r, "betrag_brutto", 4)) if _g(r, "betrag_brutto", 4) is not None else None
        except (TypeError, ValueError):
            ki["betrag_brutto"] = None
        for feld, i in (("transportart_freitext", 5), ("land_beleg", 6), ("buchungscode", 7), ("reisender", 8)):
            if _g(r, feld, i):
                ki[feld] = _g(r, feld, i)
        text = _g(r, "rohtext", 1) or ""
        if text.strip().startswith("{"):         # Foto-Beleg: der „Text“ ist das KI-Ergebnis, kein Belegtext
            text = ""
        ki = nachbearbeiten(ki, text, _g(r, "dateiname", 2) or "", manuell=True)
        fehlend = ki.get("fehlende_pflichtfelder") or []
        cur.execute(f"""UPDATE belege SET ki_json={P}, belegart={P}, transportart={P}, transportart_freitext={P},
                        land_beleg={P}, buchungscode={P}, event_datum_von=belegdatum, event_datum_bis=NULL,
                        pflichtfelder_ok={P}, fehlende_felder={P}, status={P} WHERE id={P}""",
                    (_json(ki), ki.get("belegart"), ki.get("transportart"),
                     ki.get("transportart_freitext"), ki.get("land_beleg"), ki.get("buchungscode"),
                     not fehlend, _json(fehlend), "ok" if not fehlend else "fehlerhaft", bid))
        db.commit()
    finally:
        cur.close(); db.close()
    return True


def hinweis_html(bid: int, darf: bool) -> str:
    """Gelber Hinweis auf der Beleg-Seite: Das Dokument sieht nach Einreisegenehmigung aus, ist aber anders eingestuft."""
    if not darf:
        return ""
    return f"""
        <div class="alert alert-warn" id="visum" style="margin-bottom:16px;display:flex;justify-content:space-between;
             align-items:center;gap:12px;flex-wrap:wrap">
          <span>🛂 Sieht nach einer <b>Einreisegenehmigung</b> (eTA/ESTA/eVisa) aus. Als solche einstufen, dann erscheint der
            Vorschlag fürs Visa-Profil.</span>
          <form method="post" action="/beleg/{bid}/belegart" style="margin:0">
            <input type="hidden" name="belegart" value="{BELEGART}">
            <button type="submit" class="btn btn-primary btn-sm">Als Einreisegenehmigung einstufen</button>
          </form>
        </div>"""


# ── Aufgaben ──────────────────────────────────────────────────────

def offene_vorschlaege() -> list:
    """Belege mit Einreisegenehmigung, deren Vorschlag noch nicht übernommen oder verworfen wurde (neueste zuerst)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT id, reise_code, anbieter, reisender, belegdatum, ki_json FROM belege "
                    f"WHERE transportart={P} OR belegart={P} ORDER BY id DESC", (TRANSPORTART, BELEGART))
        rows = cur.fetchall()
    except Exception:
        try: db.rollback()
        except Exception: pass
        rows = []
    finally:
        cur.close(); db.close()
    erg = []
    for r in rows:
        try:
            ki = json.loads(_g(r, "ki_json", 5) or "{}")
        except Exception:
            continue
        v = ki.get("visum") if isinstance(ki, dict) else None
        if not isinstance(v, dict) or not v.get("art"):
            continue
        if (_s(v.get("status")) or "offen") != "offen":
            continue
        land = land_pruefen(v.get("land_code"))
        erg.append({"id": _g(r, "id", 0), "reise_code": _g(r, "reise_code", 1) or "", "anbieter": _g(r, "anbieter", 2) or "",
                    "reisender": _g(r, "reisender", 3) or "", "art": _s(v.get("art")),
                    "land_name": mod_laender.land_name(land) or land})
    return erg


def todo_text(t: dict) -> str:
    wer = f" für {t['reisender']}" if t.get("reisender") else ""
    ort = f" {t['land_name']}" if t.get("land_name") else ""
    return f"{t['art']}{ort}{wer}"


# ── Karte auf der Beleg-Seite ─────────────────────────────────────

def _e(x) -> str:
    return escape(_s(x), quote=True)


def karte_html(bid: int, vs: dict, wahl: str, liste: list, darf: bool) -> str:
    """Die Karte „Visum-Vorschlag“ der Beleg-Seite. vs = vorschlag(), wahl/liste = kandidaten(); darf = Organisator."""
    kopf = f'<span class="card-title">🛂 Visum-Vorschlag: {_e(vs["art"])}{" · " + _e(vs["land_name"]) if vs["land_name"] else ""}</span>'
    hinweise = "".join(
        f'<div class="alert alert-{"ok" if k == "ok" else "warn"}" style="font-size:12px;margin:0 0 8px">{_e(t)}</div>'
        for k, t in vs["hinweise"])
    if vs["status"] == "uebernommen":
        k = _e(vs["kuerzel"])
        return f"""
        <div class="card" id="visum" style="margin-bottom:16px"><div class="card-header">{kopf}</div>
          <div class="card-body">
            <div class="alert alert-ok" style="font-size:13px;margin:0 0 8px">✅ Im Visa-Profil von <b>{k}</b> übernommen.
              <a href="/mitarbeiter/{k}/bearbeiten#visa">Profil öffnen →</a></div>
            {hinweise}
            {'<form method="post" action="/beleg/%d/visum/offen"><button type="submit" class="btn btn-secondary btn-sm">Vorschlag wieder öffnen</button></form>' % bid if darf else ''}
          </div></div>"""
    if vs["status"] == "verworfen":
        return f"""
        <div class="card" id="visum" style="margin-bottom:16px"><div class="card-header">{kopf}</div>
          <div class="card-body">
            <div style="font-size:13px;color:var(--muted);margin-bottom:8px">Vorschlag nicht übernommen.</div>
            {'<form method="post" action="/beleg/%d/visum/offen"><button type="submit" class="btn btn-secondary btn-sm">Vorschlag wieder öffnen</button></form>' % bid if darf else ''}
          </div></div>"""
    zeit = (f"{datum_text(vs['von'])} – {datum_text(vs['bis'])}" if vs["von"] and vs["bis"] else
            "Gültigkeit nicht auf dem Dokument gefunden – bitte unten eintragen")
    eckdaten = " · ".join(x for x in (vs["notiz"],) if x)
    if not darf:
        return f"""
        <div class="card" id="visum" style="margin-bottom:16px"><div class="card-header">{kopf}</div>
          <div class="card-body"><div style="font-size:13px">{_e(zeit)}<br><span style="color:var(--muted)">{_e(eckdaten)}</span></div>
            <div style="font-size:12px;color:var(--muted);margin-top:6px">Übernehmen können nur Organisatoren.</div></div></div>"""
    opts = '<option value="">– Mitarbeiter wählen –</option>' + "".join(
        f'<option value="{_e(k)}"{" selected" if k == wahl else ""}>{_e(n)} ({_e(k)}){" · auf der Reise" if auf else ""}</option>'
        for k, n, auf in liste)
    von_w = vs["von"].isoformat() if vs["von"] else ""
    bis_w = vs["bis"].isoformat() if vs["bis"] else ""
    vorwahl = "" if wahl else '<div class="form-hint">Der Name auf dem Dokument passt zu keinem oder mehreren Mitarbeitern – bitte wählen.</div>'
    return f"""
        <div class="card" id="visum" style="margin-bottom:16px"><div class="card-header">{kopf}</div>
          <div class="card-body">
            <div style="font-size:13px;margin-bottom:8px">Gültig: <b>{_e(zeit)}</b><br>
              <span style="color:var(--muted)">{_e(eckdaten)}</span></div>
            {hinweise}
            <form method="post" action="/beleg/{bid}/visum/uebernehmen">
              <div class="form-grid form-grid-2">
                <div class="form-group full"><label>Mitarbeiter <span class="required">*</span></label>
                  <select name="kuerzel" required>{opts}</select>{vorwahl}</div>
                <div class="form-group full"><label>Land <span class="required">*</span></label>
                  <select name="land" required>{mod_laender.land_optionen_html(vs["land_code"], "– Land wählen –")}</select></div>
                <div class="form-group"><label>Gültig von <span class="required">*</span></label>
                  <input type="date" name="von" value="{_e(von_w)}" required></div>
                <div class="form-group"><label>Gültig bis <span class="required">*</span></label>
                  <input type="date" name="bis" value="{_e(bis_w)}" required></div>
                <div class="form-group full"><label>Notiz</label>
                  <input type="text" name="notiz" value="{_e(vs["notiz"])}" maxlength="200"></div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">🛂 Ins Visa-Profil übernehmen</button>
              </div>
              <div class="form-hint">Die Kopie des Belegs wird als Visum-Datei mitgespeichert. Läuft das Visum ab, meldet das Dashboard es.</div>
            </form>
            <form method="post" action="/beleg/{bid}/visum/verwerfen" style="margin-top:8px">
              <button type="submit" class="btn btn-secondary btn-sm">Nicht übernehmen</button>
            </form>
          </div></div>"""
