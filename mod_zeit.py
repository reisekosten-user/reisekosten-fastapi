"""
mod_zeit.py – Gemeinsame Zeitzonen-Hilfsfunktionen für Flug-/Bahnsegmente.

Hintergrund: Auf Reisebelegen stehen Ab-/Ankunftszeiten IMMER in der
JEWEILIGEN ORTSZEIT (Abreisezeit in der Zeitzone des Abflugorts, Ankunftszeit
in der Zeitzone des Zielorts) – nicht einheitlich in einer Zeitzone. Ein Flug
Frankfurt->Bukarest "ab 10:00, an 13:30" bedeutet 10:00 MESZ und 13:30
EEST (Bukarest liegt eine Stunde vor Deutschland). Wird das ignoriert und
beide Uhrzeiten naiv wie Berlin-Zeit behandelt, entstehen bis zu mehrstündige
Fehler bei der Frage "ist der Flug schon gelandet?".
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone


# ── Hotel-Standardzeiten (v3.60) ────────────────────────────────────────────────────────
# Steht im Beleg keine Check-in-/Check-out-Zeit, gilt: Check-in 16:00, Check-out 10:00 –
# jeweils in der Ortszeit des Hotels.
HOTEL_CHECKIN_STANDARD = "16:00"
HOTEL_CHECKOUT_STANDARD = "10:00"


def hotel_zeit(roh, standard: str) -> tuple:
    """(Uhrzeit 'HH:MM', angenommen?) – fehlt die Zeit im Beleg oder ist sie ungültig, gilt die Standardzeit."""
    import re
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(roh or ""))
    if m and int(m.group(1)) <= 23 and int(m.group(2)) <= 59:
        return "%02d:%s" % (int(m.group(1)), m.group(2)), False
    return standard, True


# ── Hotel gegen Flug/Bahn abgleichen (v3.64) ────────────────────────────────────────────
# Standardzeiten dürfen nie vor einer Ankunft oder nach einer Abreise liegen, die am Hotelort
# stattfindet (Flug 7:00 -> Check-out davor; Flug landet 19:00 -> Check-in danach). Ein Flug
# an einem ANDEREN Ort ändert am Hotel nichts.
HOTEL_NAEHE_KM = 100.0   # Flughäfen liegen oft 30-60 km außerhalb der Stadt


def entfernung_km(a, b) -> float:
    """Luftlinie in km zwischen zwei (lat, lon)-Paaren."""
    import math
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(h)))


def ort_passt(a, b, radius_km: float = HOTEL_NAEHE_KM) -> bool:
    """True, wenn beide Orte bekannt und höchstens radius_km auseinander liegen.
    Ist ein Ort unbekannt (keine Koordinaten), wird NICHT geraten: True = bisheriges Verhalten."""
    if not a or not b:
        return True
    try:
        return entfernung_km(a, b) <= radius_km
    except Exception:
        return True


def _minuten(z):
    import re
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(z or ""))
    if not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
        return None
    return int(m.group(1)) * 60 + int(m.group(2))


def _hhmm(minuten: int) -> str:
    minuten = max(0, min(int(minuten), 23 * 60 + 59))
    return "%02d:%02d" % (minuten // 60, minuten % 60)


def checkout_vor_abreise(co_zeit: str, angenommen: bool, abreisen) -> tuple:
    """(Sortierzeit, Anzeigetext) für den Check-out. Nur die ANGENOMMENE Standardzeit wird angepasst:
    Liegt eine Abreise (Flug/Bahn/Mietwagen) am Hotelort früher am selben Tag, erfolgt der Check-out davor."""
    if not angenommen:
        return co_zeit, None
    ref = _minuten(co_zeit)
    frueh = [m for m in (_minuten(z) for z in (abreisen or [])) if m is not None and ref is not None and m < ref]
    if not frueh:
        return co_zeit, None
    erste = min(frueh)
    return _hhmm(erste - 1), "vor " + _hhmm(erste)


def checkin_nach_ankunft(ci_zeit: str, angenommen: bool, ankuenfte) -> tuple:
    """(Sortierzeit, Anzeigetext) für den Check-in. Nur die ANGENOMMENE Standardzeit wird angepasst:
    Landet am selben Tag am Hotelort später ein Flug/Zug, erfolgt der Check-in danach."""
    if not angenommen:
        return ci_zeit, None
    ref = _minuten(ci_zeit)
    spaet = [m for m in (_minuten(z) for z in (ankuenfte or [])) if m is not None and ref is not None and m > ref]
    if not spaet:
        return ci_zeit, None
    letzte = max(spaet)
    return _hhmm(letzte + 1), "ab " + _hhmm(letzte)


def hotel_zeit_zu_utc(datum: date | None, zeit: str | None, land_code) -> datetime | None:
    """Hotel-Uhrzeit (Ortszeit des Hotels) als UTC. Bekanntes Hotelland -> Zeitzone des Landes samt
    Sommer-/Winterzeit; sonst wie bisher der MESZ-Rückfall von segment_zeit_zu_utc."""
    if not datum or not zeit:
        return None
    import mod_zeitzone as zz
    code = str(land_code or "").strip().upper()[:2]
    if code in zz.LAND_ZONE:
        try:
            h, m = str(zeit).split(":")[:2]
            return zz.instant(datum, int(h) * 60 + int(m), zz.land_zone(code))
        except Exception:
            pass
    return segment_zeit_zu_utc(datum, zeit, None)


def utc_offset_parsen(offset_str: str | None) -> timedelta | None:
    """Parst '+02:00', '-05:00', '+0200', 'UTC', 'GMT', 'Z' zu einem timedelta."""
    if not offset_str:
        return None
    s = str(offset_str).strip()
    if s.upper() in ("UTC", "GMT", "Z", ""):
        return timedelta(0)
    try:
        vorzeichen = -1 if s[0] == "-" else 1
        if s[0] in "+-":
            s = s[1:]
        s = s.replace(":", "")
        if len(s) == 4:
            stunden, minuten = int(s[:2]), int(s[2:])
        elif len(s) <= 2:
            stunden, minuten = int(s), 0
        else:
            return None
        return vorzeichen * timedelta(hours=stunden, minutes=minuten)
    except Exception:
        return None


def segment_zeit_zu_utc(datum: date | None, zeit: str | None,
                         utc_offset: str | None) -> datetime | None:
    """
    Wandelt ein Datum+Uhrzeit-Paar aus einem Segment (Ortszeit!) in ein
    UTC-aware datetime um. Fehlt der Offset (z.B. bei älteren, vor dieser
    Funktion analysierten Belegen), wird ersatzweise MESZ (+02:00) als
    Rückfall angenommen – ungenau, aber deutlich besser als eine Zeitzone
    komplett zu ignorieren.
    """
    if not datum or not zeit:
        return None
    try:
        naiv = datetime.strptime(f"{datum.isoformat()} {zeit}", "%Y-%m-%d %H:%M")
    except Exception:
        return None
    offset = utc_offset_parsen(utc_offset)
    if offset is None:
        offset = timedelta(hours=2)  # Rückfall: MESZ
    return naiv.replace(tzinfo=timezone(offset))
