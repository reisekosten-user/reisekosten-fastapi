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
