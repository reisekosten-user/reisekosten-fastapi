"""
mod_kalender.py – Outlook-/Exchange-Kalendereinladungen für Reisesegmente und Termine.

Prinzip: Jeder Reisende mit gesetztem Haken "Kalendereinladungen" (Profil,
mitarbeiter.kalender_mail) bekommt für jedes Flug-/Bahnsegment seiner Reise und
für jeden manuell angelegten Termin eine Einladung per E-Mail (iCalendar,
METHOD:REQUEST). Outlook/Exchange legt sie automatisch in den Kalender.

Abgleich statt Einzel-Auslöser: kalender_abgleich() vergleicht den SOLL-Stand
(aktuelle Segmente/Termine der Reise) mit dem bereits gesendeten IST-Stand
(Tabelle kalender_versand) und verschickt nur das, was sich geändert hat:
  - neu            -> Einladung (SEQUENCE 0)
  - geändert       -> Aktualisierung (gleiche UID, SEQUENCE + 1)
  - entfallen      -> Absage (METHOD:CANCEL, SEQUENCE + 1)
Dadurch ist es egal, WIE sich etwas ändert (neuer Beleg, Umbuchung, KI-Korrektur,
Flugdaten-Abgleich, Termin gelöscht) – der Abgleich merkt es.

Entprellung: Änderungen werden erst gesendet, wenn sie RUHE_SEKUNDEN unverändert
geblieben sind (vermeidet Mail-Flut beim Korrigieren). "sofort=True" (Button)
überspringt das Warten.

Zeiten: Belege enthalten Ortszeiten. Die Einladung enthält absolute UTC-Zeiten
(Outlook rechnet in die eigene Zeitzone um); die Ortszeiten stehen zusätzlich
im Beschreibungstext. Fehlt der UTC-Offset im Beleg, wird MEZ/MESZ angenommen
(mit Hinweis im Text).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta, timezone

from mod_db import get_db, ph, is_postgres
from mod_zeit import utc_offset_parsen
from mod_beleg import _namens_tokens
import mod_mail
from mod_mail import sende_mail

RUHE_SEKUNDEN = int(os.getenv("KALENDER_RUHE_SEKUNDEN", "120") or "120")
ERINNERUNG_MIN = {"Flug": 180, "Bahn": 45, "Termin": 30}
UID_DOMAIN = "reisekosten.herrhammer"
PRODID = "-//Herrhammer Reisekosten//Kalender 1.0//DE"
_LOCK_ID = 7310021
_WOCHENTAG = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
_TERMIN_ICON = {"termin": "🤝", "kundenbesuch": "🤝", "fahrt": "🚕",
                "mietwagen": "🚗", "hotel": "🏨", "sonstiges": "📌"}
_TITEL_WOERTER = {"mr", "mrs", "ms", "miss", "mstr", "dr", "prof", "herr", "frau", "hr", "fr", "chd", "inf"}


# ── kleine Helfer ─────────────────────────────────────────────────────────────
def _g(row, k, i):
    return row[k] if hasattr(row, "keys") else row[i]


def _jetzt_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _iso_lesen(s):
    if not s:
        return None
    if isinstance(s, datetime):
        return s.replace(tzinfo=timezone.utc) if s.tzinfo is None else s
    try:
        return datetime.fromisoformat(str(s)[:19]).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _datum(wert):
    if not wert:
        return None
    s = str(wert).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def _lokal(d, zeit):
    """Datum + 'HH:MM' -> naives lokales datetime, sonst None."""
    m = re.match(r"^(\d{1,2}):(\d{2})", str(zeit or "").strip())
    if not d or not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 23 or mi > 59:
        return None
    return datetime(d.year, d.month, d.day, h, mi)


def _letzter_sonntag(jahr: int, monat: int) -> date:
    d = date(jahr, monat, 31)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def berlin_offset(naiv_lokal: datetime) -> timedelta:
    """MEZ/MESZ nach EU-Regel (Sommerzeit: letzter So. März bis letzter So. Oktober, 01:00 UTC)."""
    y = naiv_lokal.year
    beginn = datetime.combine(_letzter_sonntag(y, 3), datetime.min.time()) + timedelta(hours=1)
    ende = datetime.combine(_letzter_sonntag(y, 10), datetime.min.time()) + timedelta(hours=1)
    utc_wenn_mez = naiv_lokal - timedelta(hours=1)
    return timedelta(hours=2) if beginn <= utc_wenn_mez < ende else timedelta(hours=1)


def naiv_zu_utc(naiv: datetime, offset_str):
    """Ortszeit + Offset-Text ('+02:00') -> (UTC-datetime, geschaetzt). Ohne Offset: MEZ/MESZ."""
    off = utc_offset_parsen(offset_str)
    geschaetzt = off is None
    if off is None:
        off = berlin_offset(naiv)
    return (naiv - off).replace(tzinfo=timezone.utc), geschaetzt


def _lokal_text(naiv: datetime | None, mit_datum: bool = True) -> str:
    if naiv is None:
        return "Zeit unbekannt"
    zeit = naiv.strftime("%H:%M")
    if not mit_datum:
        return zeit
    return "%s %s, %s" % (_WOCHENTAG[naiv.weekday()], naiv.strftime("%d.%m.%Y"), zeit)


def _utc_zu_berlin_text(iso_s) -> str:
    dt = _iso_lesen(iso_s)
    if not dt:
        return ""
    naiv = dt.replace(tzinfo=None)
    return (naiv + berlin_offset(naiv + timedelta(hours=1))).strftime("%d.%m.%Y %H:%M")


# ── Namenszuordnung ───────────────────────────────────────────────────────────
def _tokens(name) -> set:
    return {t for t in _namens_tokens(name) if t not in _TITEL_WOERTER}


def name_passt(reisender, klarname) -> bool:
    """'DIESSLIN/RALF MR' passt zu 'Ralf Dießlin'. Bei einem einzelnen Namensteil
    muss alles gleich sein (kein Treffer nur wegen gleichem Nachnamen)."""
    ta, tb = _tokens(reisender), _tokens(klarname)
    if not ta or not tb:
        return False
    if ta == tb:
        return True
    klein, gross = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    return len(klein) >= 2 and klein <= gross


# ── iCalendar ─────────────────────────────────────────────────────────────────
def _ics_text(wert) -> str:
    s = str(wert or "").replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
    return s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")


def _falten(zeile: str) -> str:
    """RFC 5545: Zeilen auf max. 75 Oktette kürzen, Fortsetzung mit Leerzeichen."""
    if len(zeile.encode("utf-8")) <= 75:
        return zeile
    teile, aktuell, laenge = [], "", 0
    for ch in zeile:
        l = len(ch.encode("utf-8"))
        if laenge + l > 75:
            teile.append(aktuell)
            aktuell, laenge = " ", 1
        aktuell += ch
        laenge += l
    teile.append(aktuell)
    return "\r\n".join(teile)


def _cn(name) -> str:
    return '"%s"' % str(name or "").replace('"', "").replace("\r", " ").replace("\n", " ")


def _dt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ics_erzeugen(*, uid, sequence, methode, summary, start, ende, ganztag, location,
                 description, erinnerung_min, organisator_mail, organisator_name,
                 teilnehmer_mail, teilnehmer_name, jetzt=None) -> str:
    """Baut ein iCalendar-Dokument (CRLF, gefaltet). methode: REQUEST oder CANCEL.
    ganztag=True: start/ende sind date-Objekte (ende = Folgetag), sonst UTC-datetime."""
    jetzt = jetzt or _jetzt_utc()
    abgesagt = (methode == "CANCEL")
    z = ["BEGIN:VCALENDAR", "PRODID:" + PRODID, "VERSION:2.0", "CALSCALE:GREGORIAN",
         "METHOD:" + methode, "BEGIN:VEVENT",
         "UID:" + uid,
         "DTSTAMP:" + _dt_utc(jetzt),
         "SEQUENCE:%d" % int(sequence)]
    if ganztag:
        z.append("DTSTART;VALUE=DATE:" + start.strftime("%Y%m%d"))
        z.append("DTEND;VALUE=DATE:" + ende.strftime("%Y%m%d"))
    else:
        z.append("DTSTART:" + _dt_utc(start))
        z.append("DTEND:" + _dt_utc(ende))
    z.append("SUMMARY:" + _ics_text(summary))
    if location:
        z.append("LOCATION:" + _ics_text(location))
    if description:
        z.append("DESCRIPTION:" + _ics_text(description))
    z.append("ORGANIZER;CN=%s:mailto:%s" % (_cn(organisator_name), organisator_mail))
    z.append("ATTENDEE;CN=%s;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=FALSE:mailto:%s"
             % (_cn(teilnehmer_name), teilnehmer_mail))
    z.append("STATUS:CANCELLED" if abgesagt else "STATUS:CONFIRMED")
    z.append("TRANSP:TRANSPARENT" if ganztag else "TRANSP:OPAQUE")
    z.append("X-MICROSOFT-CDO-BUSYSTATUS:" + ("FREE" if ganztag else "BUSY"))
    if ganztag:
        z.append("X-MICROSOFT-CDO-ALLDAYEVENT:TRUE")
    if not abgesagt and erinnerung_min and not ganztag:
        z += ["BEGIN:VALARM", "ACTION:DISPLAY", "DESCRIPTION:Erinnerung",
              "TRIGGER:-PT%dM" % int(erinnerung_min), "END:VALARM"]
    z += ["END:VEVENT", "END:VCALENDAR"]
    return "\r\n".join(_falten(x) for x in z) + "\r\n"


def _absender() -> tuple:
    mail = os.getenv("KALENDER_ABSENDER", "").strip() or mod_mail.SMTP_USER
    if "@" not in (mail or ""):
        mail = "reisekosten@herrhammer.invalid"
    return mail, "Herrhammer Reisekosten"


# ── SOLL-Stand aus Belegen/Terminen ───────────────────────────────────────────
_BAHN_WORT = re.compile(r"\b(bahn|train|zug|sncf|ice|tgv|railjet)\b", re.I)


def _zusatz(wert, art) -> str:
    w = str(wert or "").strip()
    if not w:
        return ""
    if re.match(r"(?i)^(terminal|gleis|gate|track|platform|bahnsteig)\b", w):
        return w
    return ("Terminal " if art == "Flug" else "Gleis ") + w


def _segment_ereignisse(beleg: dict, ankuenfte: list, probleme: list) -> list:
    try:
        ki = json.loads(beleg.get("ki_json") or "{}")
    except Exception:
        return []
    reisender = beleg.get("reisender") or ki.get("reisender") or ""
    out = []
    for idx, s in enumerate(ki.get("segmente") or []):
        art = beleg.get("transportart") or "Flug"
        if art == "Flug" and _BAHN_WORT.search(str(s.get("transport_name") or "")):
            art = "Bahn"
        if art not in ("Flug", "Bahn"):
            continue
        nr = re.sub(r"\s+", "", str(s.get("transport_nummer") or "")).upper()
        marke = "Beleg #%s Segment %d" % (beleg.get("id"), idx + 1)
        d_ab = _datum(s.get("abreise_datum"))
        naiv_ab = _lokal(d_ab, s.get("abreise_zeit"))
        if not naiv_ab:
            probleme.append("%s (%s): Abreisedatum/-zeit fehlt – kein Termin." % (marke, nr or art))
            continue
        utc_ab, g1 = naiv_zu_utc(naiv_ab, s.get("abreise_utc_offset"))
        d_an = _datum(s.get("ankunft_datum"))
        naiv_an = _lokal(d_an or d_ab, s.get("ankunft_zeit"))
        utc_an, g2, an_bekannt = None, False, False
        if naiv_an:
            utc_an, g2 = naiv_zu_utc(naiv_an, s.get("ankunft_utc_offset"))
            if utc_an <= utc_ab:
                kandidat = utc_an + timedelta(days=1)       # Nachtflug/-zug ohne Folgetag
                if kandidat - utc_ab <= timedelta(hours=20):
                    utc_an, naiv_an = kandidat, naiv_an + timedelta(days=1)
                    an_bekannt = True
                else:
                    utc_an, naiv_an = None, None
            else:
                an_bekannt = True
            if an_bekannt and utc_offset_parsen(s.get("ankunft_utc_offset")) is not None:
                ankuenfte.append((naiv_an, s.get("ankunft_utc_offset")))
        if not an_bekannt:
            utc_an = utc_ab + timedelta(hours=2)
            naiv_an = None
        von, nach = s.get("von_ort") or s.get("von_iata") or "?", s.get("nach_ort") or s.get("nach_iata") or "?"
        schluessel = "%s|%s|%s" % (art.lower(), nr or ("%s-%s" % (von, nach)).upper(), d_ab.isoformat())
        out.append({
            "schluessel": schluessel, "art": art, "nummer": str(s.get("transport_nummer") or "").strip(),
            "gesellschaft": str(s.get("transport_name") or "").strip(),
            "klasse": str(s.get("klasse") or "").strip(), "sitz": str(s.get("sitzplatz") or "").strip(),
            "von_ort": s.get("von_ort") or "", "von_iata": s.get("von_iata") or "",
            "nach_ort": s.get("nach_ort") or "", "nach_iata": s.get("nach_iata") or "",
            "ab_term": str(s.get("abreise_terminal") or "").strip(),
            "an_term": str(s.get("ankunft_terminal") or "").strip(),
            "naiv_ab": naiv_ab, "naiv_an": naiv_an, "utc_ab": utc_ab, "utc_an": utc_an,
            "zeit_geschaetzt": bool(g1 or (an_bekannt and g2)),
            "reisender": reisender, "beleg_id": beleg.get("id"), "ganztag": False,
        })
    return out


def _termin_ereignisse(termine: list, ankuenfte: list) -> list:
    out = []
    for t in termine:
        d = _datum(t.get("datum"))
        if not d:
            continue
        naiv = _lokal(d, t.get("uhrzeit_von"))
        ev = {"schluessel": "termin|%s" % t.get("id"), "art": "Termin", "titel": t.get("titel") or "Termin",
              "typ": t.get("typ") or "termin", "ort": t.get("ort") or "",
              "ansprechpartner": t.get("ansprechpartner") or "", "telefon": t.get("telefon") or "",
              "notiz": t.get("notiz") or "", "reisender": None, "beleg_id": None,
              "naiv_ab": naiv, "naiv_an": None, "zeit_geschaetzt": False}
        if naiv is None:
            ev.update({"ganztag": True, "datum": d, "utc_ab": None, "utc_an": None})
        else:
            naiv_bis = _lokal(d, t.get("uhrzeit_bis"))
            if naiv_bis is None or naiv_bis <= naiv:
                naiv_bis = naiv + timedelta(hours=1)
            offset = None
            frueher = [a for a in ankuenfte if a[0] <= naiv]
            if frueher:
                offset = max(frueher, key=lambda a: a[0])[1]
            utc_ab, geschaetzt = naiv_zu_utc(naiv, offset)
            utc_an, _ = naiv_zu_utc(naiv_bis, offset)
            land = str(t.get("land_code") or "").upper()
            ev.update({"ganztag": False, "utc_ab": utc_ab, "utc_an": utc_an, "naiv_an": naiv_bis,
                       "zeit_geschaetzt": bool(geschaetzt and land and land != "DE")})
        out.append(ev)
    return out


def _ort_zeile(art, ort, iata, zusatz) -> str:
    if art == "Flug":
        name = ort
        if iata and not re.search(r"\b%s\b" % re.escape(iata), str(ort), re.I):
            name = ("%s (%s)" % (ort, iata)) if ort else iata
        if name and not re.search(r"(?i)flughafen|airport", name):
            name = "Flughafen " + name
    else:
        name = ort or iata
    teile = [name] if name else []
    z = _zusatz(zusatz, art)
    if z:
        teile.append(z)
    return ", ".join(teile)


def _komponieren(ev: dict, reise: dict, klarname: str) -> dict:
    """Macht aus dem Ereignis die Texte für die Einladung."""
    reise_zeile = "Reise: %s – %s" % (reise.get("code"), reise.get("titel") or "")
    if ev["art"] in ("Flug", "Bahn"):
        flug = ev["art"] == "Flug"
        symbol = "✈" if flug else "🚆"
        bez = ev["nummer"] or ev["gesellschaft"] or ev["art"]
        von = ev["von_ort"] or ev["von_iata"] or "?"
        nach = ev["nach_ort"] or ev["nach_iata"] or "?"
        summary = "%s %s %s → %s" % (symbol, bez, von, nach)
        ort_ab = _ort_zeile(ev["art"], ev["von_ort"], ev["von_iata"], ev["ab_term"])
        ort_an = _ort_zeile(ev["art"], ev["nach_ort"], ev["nach_iata"], ev["an_term"])
        kopf = [("Flug" if flug else "Zug") + " " + bez]
        if ev["gesellschaft"] and ev["gesellschaft"] not in bez:
            kopf.append(ev["gesellschaft"])
        if ev["klasse"]:
            kopf.append(ev["klasse"])
        z = [" · ".join(kopf),
             "%s: %s%s" % ("Abflug" if flug else "Abfahrt", _lokal_text(ev["naiv_ab"]),
                           (" – " + ort_ab) if ort_ab else ""),
             "Ankunft: %s%s" % (_lokal_text(ev["naiv_an"]), (" – " + ort_an) if ort_an else "")]
        if ev["sitz"]:
            z.append("Sitzplatz: " + ev["sitz"])
        z.append("Reisender: " + (klarname or ev.get("reisender") or ""))
        z.append(reise_zeile)
        z.append("Alle Zeiten sind Ortszeit des jeweiligen Orts.")
        if ev["zeit_geschaetzt"]:
            z.append("Hinweis: Zeitzone im Beleg nicht erkannt – MEZ/MESZ angenommen.")
        z.append("Bei Änderungen der Buchung wird dieser Termin automatisch aktualisiert.")
        return {"summary": summary, "location": ort_ab, "description": "\n".join(z),
                "erinnerung": ERINNERUNG_MIN[ev["art"]]}
    # manueller Termin
    summary = "%s %s" % (_TERMIN_ICON.get(ev["typ"], "📌"), ev["titel"])
    z = []
    if ev["naiv_ab"]:
        z.append("Zeit: %s – %s (Ortszeit)" % (_lokal_text(ev["naiv_ab"]), _lokal_text(ev["naiv_an"], False)))
    else:
        z.append("Ganztägig: %s" % ev["datum"].strftime("%d.%m.%Y"))
    if ev["ort"]:
        z.append("Ort: " + ev["ort"])
    if ev["ansprechpartner"]:
        z.append("Ansprechpartner: " + ev["ansprechpartner"])
    if ev["telefon"]:
        z.append("Telefon: " + ev["telefon"])
    if ev["notiz"]:
        z.append("Notiz: " + ev["notiz"])
    z.append(reise_zeile)
    if ev["zeit_geschaetzt"]:
        z.append("Hinweis: Zeitzone des Termins nicht bekannt – MEZ/MESZ angenommen.")
    return {"summary": summary, "location": ev["ort"], "description": "\n".join(z),
            "erinnerung": ERINNERUNG_MIN["Termin"]}


def _inhalt(ev: dict, reise: dict, klarname: str) -> dict:
    """Vollständiger, speicherbarer Einladungsinhalt (auch für spätere Absage)."""
    k = _komponieren(ev, reise, klarname)
    if ev["ganztag"]:
        start, ende = ev["datum"].isoformat(), (ev["datum"] + timedelta(days=1)).isoformat()
    else:
        start, ende = _iso(ev["utc_ab"]), _iso(ev["utc_an"])
    k.update({"start": start, "ende": ende, "ganztag": bool(ev["ganztag"]),
              "start_text": _lokal_text(ev["naiv_ab"]) if ev["naiv_ab"] else ev["datum"].strftime("%d.%m.%Y")})
    return k


def _hash(inhalt: dict) -> str:
    return hashlib.sha256(json.dumps(inhalt, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:32]


# ── Datenbank ─────────────────────────────────────────────────────────────────
def _lade_teilnehmer(cur, code: str) -> list:
    P = ph()
    cur.execute("SELECT m.kuerzel, m.klarname, m.email, m.kalender_mail, m.aktiv "
                "FROM reise_mitarbeiter rm JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel "
                "WHERE rm.reise_code=" + P + " ORDER BY m.kuerzel", (code,))
    out = []
    for r in cur.fetchall():
        aktiv = _g(r, "aktiv", 4)
        mail = (_g(r, "email", 2) or "").strip()
        out.append({"kuerzel": _g(r, "kuerzel", 0), "klarname": _g(r, "klarname", 1) or "",
                    "email": mail, "aktiv": True if aktiv is None else bool(aktiv),
                    "haken": bool(_g(r, "kalender_mail", 3)) and "@" in mail})
    return out


def _lade_soll(cur, code: str, alle: bool = False) -> dict | None:
    """Liefert {'reise','teilnehmer','soll': {(kuerzel,schluessel): ev},'probleme'} oder None.
    alle=True (nur Testversand): jeder aktive Teilnehmer gilt als eingeladen, unabhängig vom Haken im Profil."""
    P = ph()
    cur.execute("SELECT code, titel, abreise, rueckkehr FROM reisen WHERE code=" + P, (code,))
    r = cur.fetchone()
    if not r:
        return None
    reise = {"code": _g(r, "code", 0), "titel": _g(r, "titel", 1) or "",
             "abreise": _g(r, "abreise", 2), "rueckkehr": _g(r, "rueckkehr", 3)}
    teilnehmer = [t for t in _lade_teilnehmer(cur, code) if t["aktiv"]]
    if alle:
        for t in teilnehmer:
            t["haken"] = True
    probleme, ankuenfte, ereignisse = [], [], []
    cur.execute("SELECT id, transportart, anbieter, reisender, ki_json FROM belege "
                "WHERE reise_code=" + P + " AND transportart IN ('Flug','Bahn') "
                "AND aenderung_status IS NULL ORDER BY id", (code,))
    for b in cur.fetchall():
        beleg = {"id": _g(b, "id", 0), "transportart": _g(b, "transportart", 1), "anbieter": _g(b, "anbieter", 2),
                 "reisender": _g(b, "reisender", 3), "ki_json": _g(b, "ki_json", 4)}
        ereignisse += _segment_ereignisse(beleg, ankuenfte, probleme)
    cur.execute("SELECT id, datum, uhrzeit_von, uhrzeit_bis, titel, typ, ort, ansprechpartner, telefon, notiz, land_code "
                "FROM termine WHERE reise_code=" + P + " ORDER BY datum, uhrzeit_von", (code,))
    termine = []
    for t in cur.fetchall():
        termine.append({k: _g(t, k, i) for i, k in enumerate(
            ("id", "datum", "uhrzeit_von", "uhrzeit_bis", "titel", "typ", "ort", "ansprechpartner",
             "telefon", "notiz", "land_code"))})
    ereignisse += _termin_ereignisse(termine, ankuenfte)

    haken = [t for t in teilnehmer if t["haken"]]
    soll = {}
    for ev in ereignisse:
        if ev["art"] == "Termin":
            empf = haken
        else:
            treffer = [t for t in teilnehmer if name_passt(ev["reisender"], t["klarname"])]
            if treffer:
                empf = [t for t in treffer if t["haken"]]
            elif not str(ev["reisender"] or "").strip() and len(teilnehmer) == 1:
                empf = haken
            else:
                empf = []
                if haken:
                    probleme.append("%s %s: Reisender „%s“ keinem Teilnehmer zuordenbar – nicht gesendet."
                                    % (ev["art"], ev["nummer"] or ev["schluessel"], ev["reisender"] or "unbekannt"))
        for t in empf:
            alt = soll.get((t["kuerzel"], ev["schluessel"]))
            if alt is None:
                soll[(t["kuerzel"], ev["schluessel"])] = dict(ev)
            else:                                           # Dublette aus 2. Beleg: Lücken füllen
                for f in ("sitz", "ab_term", "an_term", "klasse", "gesellschaft"):
                    if not alt.get(f) and ev.get(f):
                        alt[f] = ev[f]
    return {"reise": reise, "teilnehmer": teilnehmer, "soll": soll, "probleme": probleme}


_ZEILEN_SPALTEN = ("kuerzel", "schluessel", "uid", "seq_nr", "status", "inhalt_hash", "titel", "start_text",
                   "daten", "offen_hash", "offen_seit", "gesendet_am", "empfaenger", "letzter_fehler")


def _lade_ist(cur, code: str) -> dict:
    cur.execute("SELECT " + ", ".join(_ZEILEN_SPALTEN) + " FROM kalender_versand WHERE reise_code=" + ph(), (code,))
    out = {}
    for r in cur.fetchall():
        z = {k: _g(r, k, i) for i, k in enumerate(_ZEILEN_SPALTEN)}
        z["seq_nr"] = -1 if z["seq_nr"] is None else int(z["seq_nr"])
        out[(z["kuerzel"], z["schluessel"])] = z
    return out


def _speichern(db, cur, code: str, z: dict) -> None:
    """UPDATE, falls vorhanden – sonst INSERT."""
    P = ph()
    felder = [c for c in _ZEILEN_SPALTEN if c not in ("kuerzel", "schluessel")]
    cur.execute("UPDATE kalender_versand SET " + ", ".join("%s=%s" % (c, P) for c in felder) +
                " WHERE reise_code=" + P + " AND kuerzel=" + P + " AND schluessel=" + P,
                tuple(z.get(c) for c in felder) + (code, z["kuerzel"], z["schluessel"]))
    if cur.rowcount == 0:
        cols = ["reise_code"] + list(_ZEILEN_SPALTEN)
        cur.execute("INSERT INTO kalender_versand (" + ", ".join(cols) + ") VALUES (" + ", ".join([P] * len(cols)) + ")",
                    (code,) + tuple(z.get(c) for c in _ZEILEN_SPALTEN))
    db.commit()


def _loeschen(db, cur, code: str, z: dict) -> None:
    P = ph()
    cur.execute("DELETE FROM kalender_versand WHERE reise_code=" + P + " AND kuerzel=" + P + " AND schluessel=" + P,
                (code, z["kuerzel"], z["schluessel"]))
    db.commit()


# ── Versand ───────────────────────────────────────────────────────────────────
def _uid(code: str, kuerzel: str, schluessel: str) -> str:
    h = hashlib.sha1(("%s|%s|%s" % (code, kuerzel, schluessel)).encode("utf-8")).hexdigest()[:16]
    return "rk-%s-%s-%s@%s" % (code, kuerzel, h, UID_DOMAIN)


def _sende_einladung(person: dict, reise: dict, z: dict, inhalt: dict, sequence: int, methode: str,
                     aktualisierung: bool, test_an: str | None = None) -> dict:
    """test_an: Testversand – Einladung geht NUR an diese Adresse (Betreff mit [TEST], Hinweis im Text)."""
    org_mail, org_name = _absender()
    ziel = test_an or person["email"]
    if inhalt["ganztag"]:
        start, ende = date.fromisoformat(inhalt["start"]), date.fromisoformat(inhalt["ende"])
    else:
        start, ende = _iso_lesen(inhalt["start"]), _iso_lesen(inhalt["ende"])
    ics = ics_erzeugen(uid=z["uid"], sequence=sequence, methode=methode, summary=inhalt["summary"],
                       start=start, ende=ende, ganztag=inhalt["ganztag"], location=inhalt["location"],
                       description=inhalt["description"], erinnerung_min=inhalt["erinnerung"],
                       organisator_mail=org_mail, organisator_name=org_name,
                       teilnehmer_mail=ziel, teilnehmer_name=person["klarname"])
    vorname = (person["klarname"].split() or [""])[0]
    betreff = "%s (%s)" % (inhalt["summary"], inhalt["start_text"])
    if methode == "CANCEL":
        betreff = "Abgesagt: " + betreff
        kopf = "dieser Termin zur Reise %s entfällt (Buchung geändert oder storniert):" % reise["code"]
        fuss = "Er wird in Outlook automatisch entfernt."
    elif aktualisierung:
        betreff = "Aktualisiert: " + betreff
        kopf = "dein Termin zur Reise %s wurde aktualisiert:" % reise["code"]
        fuss = "Bitte in Outlook die Aktualisierung annehmen."
    else:
        kopf = "hier dein Kalendereintrag zur Reise %s:" % reise["code"]
        fuss = "Bitte in Outlook annehmen. Bei Änderungen der Buchung wird der Termin automatisch aktualisiert."
    text = "Hallo %s,\n\n%s\n\n%s\n\n%s\n\n%s\n\nViele Grüße" % (
        vorname, kopf, inhalt["summary"], inhalt["description"], fuss)
    if test_an:
        betreff = "[TEST] " + betreff
        text = ("TESTVERSAND – diese Einladung wäre regulär an %s (%s) gegangen. Du hast sie nur als Test erhalten; "
                "es wurde nichts als gesendet vermerkt.\n\n%s" % (person["klarname"], person["email"] or "keine E-Mail", text))
    return sende_mail(ziel, betreff, text, ics=ics, ics_methode=methode)


def _ruhe_vorbei(db, cur, code, z, ziel_hash, jetzt, sofort) -> bool:
    """Merkt sich die Änderung; True, wenn sie lange genug unverändert besteht."""
    if sofort:
        return True
    if z.get("offen_hash") != ziel_hash:
        z["offen_hash"], z["offen_seit"] = ziel_hash, _iso(jetzt)
        _speichern(db, cur, code, z)
    seit = _iso_lesen(z.get("offen_seit"))
    return RUHE_SEKUNDEN <= 0 or (seit is not None and (jetzt - seit).total_seconds() >= RUHE_SEKUNDEN)


def _reise_abgleichen(db, cur, code: str, sofort: bool, jetzt: datetime, erg: dict) -> None:
    if erg.get("abbruch"):                                   # Mailserver war in diesem Lauf nicht erreichbar
        return
    if not any(t["haken"] and t["aktiv"] for t in _lade_teilnehmer(cur, code)):
        return                                               # niemand mit Haken: nichts zu tun
    daten = _lade_soll(cur, code)
    if not daten:
        return
    reise, teilnehmer, soll = daten["reise"], daten["teilnehmer"], daten["soll"]
    erg["probleme"] += ["%s: %s" % (code, p) for p in daten["probleme"]]
    personen = {t["kuerzel"]: t for t in teilnehmer if t["haken"]}
    if not personen:
        return
    ist = _lade_ist(cur, code)

    for (kz, key), ev in sorted(soll.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        person = personen[kz]
        inhalt = _inhalt(ev, reise, person["klarname"])
        h = _hash(inhalt)
        z = ist.get((kz, key))
        if z and z["status"] == "aktiv" and z["inhalt_hash"] == h:
            if z.get("offen_hash"):                          # Änderung wurde zurückgenommen
                z["offen_hash"], z["offen_seit"] = None, None
                _speichern(db, cur, code, z)
            continue
        if z is None:
            ende_utc = ev["utc_an"] or ev["utc_ab"]
            if ev["ganztag"] and ev["datum"] < jetzt.date():
                continue
            if (not ev["ganztag"]) and ende_utc < jetzt:
                continue                                     # Vergangenes nicht neu einladen
            z = {"kuerzel": kz, "schluessel": key, "uid": _uid(code, kz, key), "seq_nr": -1,
                 "status": "wartend", "inhalt_hash": None, "daten": None, "offen_hash": None,
                 "offen_seit": None, "gesendet_am": None, "empfaenger": person["email"], "letzter_fehler": None}
        z["titel"], z["start_text"] = inhalt["summary"], inhalt["start_text"]
        if not _ruhe_vorbei(db, cur, code, z, h, jetzt, sofort):
            erg["wartend"] += 1
            continue
        seq = z["seq_nr"] + 1
        aktualisierung = (z["status"] == "aktiv")
        res = _sende_einladung(person, reise, z, inhalt, seq, "REQUEST", aktualisierung)
        if res.get("ok"):
            z.update({"seq_nr": seq, "status": "aktiv", "inhalt_hash": h, "daten": json.dumps(inhalt, ensure_ascii=False),
                      "offen_hash": None, "offen_seit": None, "gesendet_am": _iso(_jetzt_utc()),
                      "empfaenger": person["email"], "letzter_fehler": None})
            erg["aktualisiert" if aktualisierung else "gesendet"] += 1
        else:
            z["letzter_fehler"] = str(res.get("fehler") or "unbekannter Fehler")[:300]
            erg["fehler"].append("%s/%s %s: %s" % (code, kz, inhalt["summary"], z["letzter_fehler"]))
            if res.get("verbindung"):
                erg["abbruch"] = True
        _speichern(db, cur, code, z)
        if erg["abbruch"]:
            return

    for (kz, key), z in list(ist.items()):
        if kz not in personen or (kz, key) in soll:
            continue
        if z["status"] == "wartend":                          # nie gesendet und schon wieder weg
            _loeschen(db, cur, code, z)
            continue
        if z["status"] != "aktiv" or not z.get("daten"):
            continue
        if not _ruhe_vorbei(db, cur, code, z, "__abgesagt__", jetzt, sofort):
            erg["wartend"] += 1
            continue
        person = personen[kz]
        inhalt = json.loads(z["daten"])
        seq = z["seq_nr"] + 1
        res = _sende_einladung(person, reise, z, inhalt, seq, "CANCEL", False)
        if res.get("ok"):
            z.update({"seq_nr": seq, "status": "storniert", "inhalt_hash": None, "offen_hash": None,
                      "offen_seit": None, "gesendet_am": _iso(_jetzt_utc()), "letzter_fehler": None})
            erg["abgesagt"] += 1
        else:
            z["letzter_fehler"] = str(res.get("fehler") or "unbekannter Fehler")[:300]
            erg["fehler"].append("%s/%s Absage %s: %s" % (code, kz, z.get("titel"), z["letzter_fehler"]))
            if res.get("verbindung"):
                erg["abbruch"] = True
        _speichern(db, cur, code, z)
        if erg["abbruch"]:
            return


def kalender_abgleich(reise_code: str | None = None, sofort: bool = False, jetzt: datetime | None = None) -> dict:
    """Gleicht SOLL (Segmente/Termine) und IST (bereits gesendet) ab und verschickt nur Änderungen.
    reise_code=None: alle Reisen, die noch nicht beendet sind (für den Cron-Aufruf)."""
    jetzt = jetzt or _jetzt_utc()
    erg = {"reisen": 0, "gesendet": 0, "aktualisiert": 0, "abgesagt": 0, "wartend": 0,
           "fehler": [], "probleme": [], "abbruch": False}
    db = get_db()
    cur = db.cursor()
    gesperrt = False
    try:
        if is_postgres():
            cur.execute("SELECT pg_try_advisory_lock(%s)" % _LOCK_ID)
            if not cur.fetchone()[0]:
                return {**erg, "uebersprungen": "läuft bereits"}
            gesperrt = True
        P = ph()
        if reise_code:
            cur.execute("SELECT code FROM reisen WHERE code=" + P, (reise_code.upper(),))
        else:
            cur.execute("SELECT code FROM reisen WHERE rueckkehr >= " + P + " ORDER BY abreise",
                        (jetzt.date().isoformat(),))
        codes = [_g(r, "code", 0) for r in cur.fetchall()]
        with mod_mail.smtp_sitzung():                        # alle Einladungen über eine Mail-Verbindung
            for code in codes:
                try:
                    _reise_abgleichen(db, cur, code, sofort, jetzt, erg)
                    erg["reisen"] += 1
                except Exception as e:                       # eine Reise darf die anderen nicht blockieren
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    erg["fehler"].append("%s: %s" % (code, e))
        return erg
    finally:
        try:
            if gesperrt:
                cur.execute("SELECT pg_advisory_unlock(%s)" % _LOCK_ID)
        except Exception:
            pass
        cur.close()
        db.close()


# ── Testversand (Bestandsdaten, nur an eine Testadresse) ──────────────────────
TEST_MAX = 25                                                # höchstens so viele Test-Einladungen je Klick


def test_ereignisse(reise_code: str) -> dict | None:
    """Alle Termine einer Reise aus dem Bestand (Flüge, Züge, manuelle Termine) – je Termin EINMAL,
    unabhängig vom Haken im Profil. Schreibt nichts."""
    code = reise_code.upper()
    db = get_db()
    cur = db.cursor()
    try:
        daten = _lade_soll(cur, code, alle=True)
    finally:
        cur.close()
        db.close()
    if not daten:
        return None
    reise = daten["reise"]
    personen = {t["kuerzel"]: t for t in daten["teilnehmer"]}
    liste, gesehen = [], set()
    for (kz, key), ev in sorted(daten["soll"].items(), key=lambda kv: (str(kv[0][1]), kv[0][0])):
        if ev["art"] == "Termin":                            # gemeinsamer Termin: nur einmal testen
            if key in gesehen:
                continue
            gesehen.add(key)                                 # Flüge/Züge dagegen je Person (eigene Sitzplätze)
        person = personen[kz]
        inhalt = _inhalt(ev, reise, person["klarname"])
        liste.append({"kuerzel": kz, "name": person["klarname"], "email": person["email"], "person": person,
                      "key": key, "inhalt": inhalt, "titel": inhalt["summary"], "start": inhalt["start_text"]})
    liste.sort(key=lambda x: (x["inhalt"]["start"], x["kuerzel"]))
    return {"reise": reise, "liste": liste, "probleme": daten["probleme"], "teilnehmer": daten["teilnehmer"]}


def kalender_test(reise_code: str, test_mail: str, jetzt: datetime | None = None) -> dict:
    """Schickt die Einladungen der Reise NUR an test_mail. Es wird nichts in kalender_versand gespeichert,
    die echten Einladungen (Haken im Profil, Cron) bleiben unberührt. Gleiche UID je Termin: ein
    erneuter Test aktualisiert den Eintrag in Outlook statt einen weiteren anzulegen."""
    jetzt = jetzt or _jetzt_utc()
    erg = {"gesendet": 0, "gesamt": 0, "nicht_gesendet": 0, "abgebrochen": 0, "fehler": [], "probleme": []}
    d = test_ereignisse(reise_code)
    if not d:
        erg["fehler"].append("Reise nicht gefunden")
        return erg
    erg["probleme"] = list(d["probleme"])
    erg["gesamt"] = len(d["liste"])
    code = d["reise"]["code"]
    seq = int(jetzt.timestamp() // 60)                       # wächst mit jedem Test -> Outlook übernimmt die neue Fassung
    with mod_mail.smtp_sitzung():                            # alle Test-Einladungen über eine Mail-Verbindung
        for i, x in enumerate(d["liste"]):
            if i >= TEST_MAX:
                erg["nicht_gesendet"] = len(d["liste"]) - TEST_MAX
                break
            z = {"uid": _uid("TEST-" + code, x["kuerzel"], x["key"])}
            res = _sende_einladung(x["person"], d["reise"], z, x["inhalt"], seq, "REQUEST", False, test_an=test_mail)
            if res.get("ok"):
                erg["gesendet"] += 1
            else:
                erg["fehler"].append("%s: %s" % (x["titel"], str(res.get("fehler") or "unbekannter Fehler")[:200]))
                if res.get("verbindung") or "SMTP nicht konfiguriert" in erg["fehler"][-1]:
                    erg["abgebrochen"] = max(0, min(len(d["liste"]), TEST_MAX) - i - 1)
                    break                                    # Server nicht erreichbar: weitere Versuche sind sinnlos
    return erg


# ── Übersicht für die Oberfläche ──────────────────────────────────────────────
def kalender_uebersicht(reise_code: str) -> dict | None:
    """Lesender Überblick: Teilnehmer mit/ohne Haken und pro Person/Termin der Versandstatus."""
    code = reise_code.upper()
    db = get_db()
    cur = db.cursor()
    try:
        daten = _lade_soll(cur, code)
        if not daten:
            return None
        ist = _lade_ist(cur, code)
    finally:
        cur.close()
        db.close()
    reise, soll = daten["reise"], daten["soll"]
    namen = {t["kuerzel"]: t["klarname"] for t in daten["teilnehmer"]}
    zeilen = []
    for (kz, key), ev in soll.items():
        inhalt = _inhalt(ev, reise, namen.get(kz, ""))
        z = ist.get((kz, key))
        if z and z["status"] == "aktiv" and z["inhalt_hash"] == _hash(inhalt):
            status = "✔ gesendet (Version %d)" % (z["seq_nr"] + 1)
        elif z and z["status"] == "aktiv":
            status = "⏳ Änderung wartet auf Versand"
        else:
            status = "⏳ noch nicht gesendet"
        zeilen.append({"kuerzel": kz, "name": namen.get(kz, kz), "titel": inhalt["summary"],
                       "start": inhalt["start_text"], "sortierung": inhalt["start"], "status": status,
                       "gesendet_am": _utc_zu_berlin_text(z["gesendet_am"]) if z else "",
                       "fehler": (z or {}).get("letzter_fehler") or ""})
    for (kz, key), z in ist.items():
        if (kz, key) in soll or kz not in namen:
            continue
        if z["status"] == "aktiv":
            status = "⏳ Absage wartet auf Versand"
        elif z["status"] == "storniert":
            status = "✖ abgesagt"
        else:
            continue
        zeilen.append({"kuerzel": kz, "name": namen.get(kz, kz), "titel": z.get("titel") or key,
                       "start": z.get("start_text") or "", "sortierung": "", "status": status,
                       "gesendet_am": _utc_zu_berlin_text(z["gesendet_am"]), "fehler": z.get("letzter_fehler") or ""})
    zeilen.sort(key=lambda x: (x["name"], x["sortierung"] or "9", x["titel"]))
    return {"reise": reise, "teilnehmer": daten["teilnehmer"], "zeilen": zeilen, "probleme": daten["probleme"]}
