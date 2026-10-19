"""
mod_zeitzone.py – Zeitzonen für die Uhrzeiten im Reisenden-Portal (v3.39)

Grundregel: Alle Uhrzeiten gelten als ORTSZEIT des Reiselandes dieses Tages.
Nur wenn ein Reisender eine Zeit in einer anderen Zeitzone angibt (z. B. deutsche Zeit),
wählt er sie im Dropdown hinter der Uhrzeit. Gespeichert wird je Uhrzeit:
  None / ""  = Ortszeit des Tageslandes (Standard)
  "de"       = Deutsche Zeit (Europe/Berlin, MEZ/MESZ)
  "<IANA>"   = eine Zone aus der Auswahlliste ZONEN
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from html import escape
from zoneinfo import ZoneInfo

BERLIN = "Europe/Berlin"

# (Code, Beschriftung im Dropdown, Kurzname für Abschluss/PDF)
ZONEN = [
    ("UTC", "UTC / GMT", "UTC"),
    ("Europe/London", "Großbritannien / Portugal (London)", "London"),
    ("Europe/Bucharest", "Osteuropa: Rumänien, Griechenland, Finnland … (Bukarest)", "Bukarest"),
    ("Europe/Istanbul", "Türkei (Istanbul)", "Istanbul"),
    ("Europe/Moscow", "Russland (Moskau)", "Moskau"),
    ("Africa/Cairo", "Ägypten (Kairo)", "Kairo"),
    ("Africa/Johannesburg", "Südafrika (Johannesburg)", "Johannesburg"),
    ("Asia/Riyadh", "Saudi-Arabien / Katar / Kuwait (Riad)", "Riad"),
    ("Asia/Dubai", "Vereinigte Arabische Emirate (Dubai)", "Dubai"),
    ("Asia/Kolkata", "Indien (Neu-Delhi)", "Indien"),
    ("Asia/Bangkok", "Thailand / Vietnam / Indonesien West (Bangkok)", "Bangkok"),
    ("Asia/Shanghai", "China / Singapur / Hongkong / Malaysia (Shanghai)", "China"),
    ("Asia/Tokyo", "Japan / Südkorea (Tokio)", "Tokio"),
    ("Australia/Sydney", "Australien Ostküste (Sydney)", "Sydney"),
    ("Pacific/Auckland", "Neuseeland (Auckland)", "Auckland"),
    ("America/Sao_Paulo", "Brasilien (São Paulo)", "São Paulo"),
    ("America/New_York", "USA / Kanada Ostküste (New York)", "New York"),
    ("America/Chicago", "USA Mitte (Chicago)", "Chicago"),
    ("America/Denver", "USA Rocky Mountains (Denver)", "Denver"),
    ("America/Los_Angeles", "USA Westküste (Los Angeles)", "Los Angeles"),
    ("America/Mexico_City", "Mexiko (Mexiko-Stadt)", "Mexiko"),
]
_ZONEN_CODES = {z[0] for z in ZONEN}
_KURZ = {z[0]: (z[2] if z[0] == "UTC" else z[2] + "-Zeit") for z in ZONEN}
_KURZ["de"] = "DE-Zeit"

# Hauptzeitzone je Land (ISO-Code). Länder mit mehreren Zonen: Hauptstadt bzw. Hauptzone.
LAND_ZONE = {
    "DE": "Europe/Berlin", "FR": "Europe/Paris", "CH": "Europe/Zurich", "AT": "Europe/Vienna",
    "GB": "Europe/London", "IT": "Europe/Rome", "ES": "Europe/Madrid", "NL": "Europe/Amsterdam",
    "BE": "Europe/Brussels", "PL": "Europe/Warsaw", "CZ": "Europe/Prague", "SE": "Europe/Stockholm",
    "NO": "Europe/Oslo", "DK": "Europe/Copenhagen", "FI": "Europe/Helsinki", "PT": "Europe/Lisbon",
    "GR": "Europe/Athens", "TR": "Europe/Istanbul", "US": "America/New_York", "CA": "America/Toronto",
    "JP": "Asia/Tokyo", "CN": "Asia/Shanghai", "SG": "Asia/Singapore", "IN": "Asia/Kolkata",
    "AE": "Asia/Dubai", "QA": "Asia/Qatar", "AU": "Australia/Sydney", "BR": "America/Sao_Paulo",
    "MX": "America/Mexico_City", "AR": "America/Argentina/Buenos_Aires", "ZA": "Africa/Johannesburg",
    "CR": "America/Costa_Rica", "PA": "America/Panama", "DO": "America/Santo_Domingo",
    "CO": "America/Bogota", "CL": "America/Santiago", "KR": "Asia/Seoul", "TH": "Asia/Bangkok",
    "ID": "Asia/Jakarta", "MY": "Asia/Kuala_Lumpur", "HK": "Asia/Hong_Kong", "IL": "Asia/Jerusalem",
    "RU": "Europe/Moscow", "UA": "Europe/Kyiv", "HU": "Europe/Budapest", "RO": "Europe/Bucharest",
    "HR": "Europe/Zagreb", "SK": "Europe/Bratislava", "SI": "Europe/Ljubljana", "BG": "Europe/Sofia",
    "RS": "Europe/Belgrade", "EG": "Africa/Cairo", "MA": "Africa/Casablanca", "NG": "Africa/Lagos",
    "KE": "Africa/Nairobi", "PH": "Asia/Manila", "VN": "Asia/Ho_Chi_Minh", "NZ": "Pacific/Auckland",
    # weitere häufige Reiseländer
    "LU": "Europe/Luxembourg", "IE": "Europe/Dublin", "IS": "Atlantic/Reykjavik", "EE": "Europe/Tallinn",
    "LV": "Europe/Riga", "LT": "Europe/Vilnius", "MT": "Europe/Malta", "CY": "Asia/Nicosia",
    "SA": "Asia/Riyadh", "KW": "Asia/Kuwait", "BH": "Asia/Bahrain", "OM": "Asia/Muscat",
    "JO": "Asia/Amman", "PK": "Asia/Karachi", "BD": "Asia/Dhaka", "LK": "Asia/Colombo",
    "TW": "Asia/Taipei", "TN": "Africa/Tunis", "DZ": "Africa/Algiers", "PE": "America/Lima",
    "BA": "Europe/Sarajevo", "MK": "Europe/Skopje", "AL": "Europe/Tirane", "ME": "Europe/Podgorica",
    "BY": "Europe/Minsk", "MD": "Europe/Chisinau", "KZ": "Asia/Almaty", "UZ": "Asia/Tashkent",
}
_ERSATZ = {"Europe/Kyiv": "Europe/Kiev"}      # ältere Zeitzonen-Datenbanken kennen nur den alten Namen

_cache: dict = {}


def _zone(name: str) -> ZoneInfo:
    if name not in _cache:
        try:
            _cache[name] = ZoneInfo(name)
        except Exception:
            _cache[name] = ZoneInfo(_ERSATZ.get(name, BERLIN))
    return _cache[name]


def land_zone_name(land_code) -> str:
    """IANA-Name der Ortszeit für den Ländercode (z. B. 'RO'); unbekannt -> Berlin."""
    code = (str(land_code or "DE")).strip().upper()[:2]
    return LAND_ZONE.get(code, BERLIN)


def land_zone(land_code) -> ZoneInfo:
    return _zone(land_zone_name(land_code))


def zone_normalisieren(code, land_code=None):
    """Eingabe aus dem Formular -> None (= Ortszeit) oder gültiger Code ('de' / IANA aus ZONEN).
    Unbekanntes wird zu Ortszeit. 'de' an einem Tag in Deutschland ist Ortszeit."""
    c = (code or "").strip()
    if not c or c == "ort":
        return None
    if c == "de":
        return None if land_zone_name(land_code) == BERLIN else "de"
    if c in _ZONEN_CODES:
        return None if c == land_zone_name(land_code) else c
    return None


def zone_aufloesen(code, land_code=None) -> ZoneInfo:
    """Gespeicherter Code -> ZoneInfo. Leer/ungültig = Ortszeit des Tageslandes."""
    c = (code or "").strip()
    if c == "de":
        return _zone(BERLIN)
    if c in _ZONEN_CODES:
        return _zone(c)
    return land_zone(land_code)


def kurzname(code) -> str:
    """Kurzname für Abschluss/PDF; leer bei Ortszeit."""
    return _KURZ.get((code or "").strip(), "")


def zeit_mit_zone(zeit, code) -> str:
    """'06:00' + Zone -> '06:00 (DE-Zeit)'; bei Ortszeit nur '06:00'; ohne Zeit '–'."""
    z = (zeit or "").strip()
    if not z:
        return "–"
    k = kurzname(code)
    return f"{z} ({k})" if k else z


def in_ortszeit(tag: date, hhmm, code, land_code) -> str:
    """Rechnet 'HH:MM' am Tag in der Zone 'code' in die Ortszeit des Tageslandes um (nur Uhrzeit).
    Bei Ortszeit oder nicht lesbarer Zeit kommt der Text unverändert zurück."""
    z = (hhmm or "").strip()
    if not z or not zone_normalisieren(code, land_code):
        return z
    try:
        h, m = z.split(":")[:2]
        lokal = datetime(tag.year, tag.month, tag.day, int(h), int(m), tzinfo=zone_aufloesen(code, land_code))
    except (ValueError, TypeError):
        return z
    ort = lokal.astimezone(land_zone(land_code))
    return "%02d:%02d" % (ort.hour, ort.minute)


def optionen_html(gewaehlt, land_code, land_name: str = "") -> str:
    """<option>-Liste für das Dropdown. Erste Option = Ortszeit (Standard)."""
    wahl = zone_normalisieren(gewaehlt, land_code)
    name = escape(land_name or "Reiseland")
    teile = [f'<option value=""{" selected" if wahl is None else ""}>Ortszeit ({name})</option>']
    if land_zone_name(land_code) != BERLIN:
        teile.append(f'<option value="de"{" selected" if wahl == "de" else ""}>Deutsche Zeit</option>')
    teile.append('<optgroup label="Andere Zeitzone">')
    eigene = land_zone_name(land_code)
    for code, label, _kurz in ZONEN:
        if code == eigene:
            continue                                # entspricht der Ortszeit
        teile.append(f'<option value="{escape(code)}"{" selected" if wahl == code else ""}>{escape(label)}</option>')
    teile.append("</optgroup>")
    return "".join(teile)


def instant(tag: date, minuten: int, zone: ZoneInfo) -> datetime:
    """Zeitpunkt 'tag + minuten' (Minuten ab 00:00, auch >1440) in der Zone, als UTC."""
    lokal = datetime(tag.year, tag.month, tag.day) + timedelta(minutes=minuten)
    return lokal.replace(tzinfo=zone).astimezone(timezone.utc)


_FELDER = (("reise_beginn", "Reisebeginn"), ("reise_ende", "Reiseende"),
           ("arbeit_beginn", "Arbeitsbeginn"), ("arbeit_ende", "Arbeitsende"))


def zonen_hinweis(werte: dict, land_code) -> str:
    """Kurzer Text für Abschluss/PDF, falls Zeiten nicht in Ortszeit angegeben wurden, sonst ''.
    werte: {reise_beginn, reise_beginn_zone, ...}."""
    teile = []
    for feld, name in _FELDER:
        zeit = (werte.get(feld) or "").strip()
        code = zone_normalisieren(werte.get(feld + "_zone"), land_code)
        if zeit and code:
            teile.append(f"{name} {zeit} in {kurzname(code)}")
    return "; ".join(teile)
