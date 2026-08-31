"""
mod_flugalert.py – Flug-/Bahn-Statusüberwachung (Verspätung, Ausfall, Gate-Änderung)

Datenquellen:
  - Flüge: AeroDataBox (via RapidAPI oder API.market) – Env: AERODATABOX_API_KEY, AERODATABOX_HOST
  - Bahn:  v6.db.transport.rest (kostenlose Community-Schnittstelle auf Basis der
           offiziellen HAFAS-Auskunft der Deutschen Bahn, kein API-Key nötig)
"""
from __future__ import annotations
import os, json, httpx
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from mod_db import get_db, ph, is_postgres
from mod_mail import sende_mail

AERODATABOX_API_KEY = os.getenv("AERODATABOX_API_KEY", "")
AERODATABOX_HOST = os.getenv("AERODATABOX_HOST", "aerodatabox.p.rapidapi.com")
DB_TRANSPORT_REST_URL = "https://v6.db.transport.rest"

CRON_SECRET = os.getenv("CRON_SECRET", "")

BERLIN_TZ = ZoneInfo("Europe/Berlin")


def jetzt_lokal() -> datetime:
    """
    Aktuelle Zeit in Europe/Berlin, als naives datetime (ohne tzinfo) – damit sie
    direkt mit den auf Belegen gedruckten lokalen Uhrzeiten vergleichbar ist.
    Render-Server laufen meist in UTC; ohne diese Umrechnung würde "Stunden bis
    Abreise" um 1-2 Stunden falsch berechnet.
    """
    return datetime.now(BERLIN_TZ).replace(tzinfo=None)


# ── Checkpoint-Schema ──────────────────────────────────────────────────────────
# Statt eines laufenden Intervalls werden feste Zeitpunkte relativ zur
# GEPLANTEN Abreise/Ankunft geprüft:
#   4h / 3h / 2h / 1h / 30min / 15min VOR der geplanten Abreise
#   + bei erkannter Verspätung zusätzlich 15min VOR der NEUEN erwarteten Abreise
#   + WÄHREND des Flugs (zwischen Abreise und Ankunft-30min): kein Check
#   + 30min VOR der erwarteten Landung (inkl. bekannter Verspätung)
#   + nach der Landung: kein Check mehr

CHECKPOINTS_VOR_ABREISE = [timedelta(hours=4), timedelta(hours=3), timedelta(hours=2),
                           timedelta(hours=1), timedelta(minutes=30), timedelta(minutes=15)]
CHECKPOINT_VOR_ANKUNFT = timedelta(minutes=30)
VERSPAETUNGS_ALARM_SCHWELLE_MIN = 15
MINDESTABSTAND_MINUTEN = 10  # harte Sperre, siehe segment_check_faellig


def _checkpoints_fuer_segment(seg: dict, alt_status: dict | None) -> list:
    """Berechnet die STATISCHEN Prüf-Zeitpunkte für ein Segment (nur für die
    Diagnose-Anzeige – die eigentliche Fällig-Logik inkl. Verspätungsfall steht
    in segment_check_faellig)."""
    dt_ab = seg["dt_ab"]; dt_an = seg["dt_an"]
    punkte = [dt_ab - td for td in CHECKPOINTS_VOR_ABREISE]
    punkte.append(dt_an - CHECKPOINT_VOR_ANKUNFT)
    return sorted(punkte)


def segment_check_faellig(seg: dict, jetzt: datetime, alt_status: dict | None) -> bool:
    """
    True, wenn ein Check fällig ist.

    - Ist bereits eine Verspätung bekannt (Flug/Zug sollte laut Plan schon
      losgefahren sein, hat sich aber verzögert): einfach alle
      MINDESTABSTAND_MINUTEN erneut nachsehen, bis sich der Status ändert
      (z.B. Ankunft bestätigt). KEIN rechnerisch verschobener Einzel-
      Checkpoint mehr – der hätte sich mit jeder neu gemeldeten Verspätung
      ein Stück nach vorn verschoben und wäre dadurch nach JEDEM Check sofort
      wieder "fällig" gewesen (führte zum Minutentakt-Bug).
    - Sonst: feste Checkpoints vor der geplanten Abreise (4h/3h/2h/1h/30/15min)
      und 30 Min vor der geplanten Landung, jeweils mit demselben
      Mindestabstand zum letzten Check.
    - Punkte zwischen Abreise und Ankunft-30min existieren bewusst nicht ->
      während des Flugs finden automatisch keine Checks statt.
    """
    letzter_check = alt_status.get("letzter_check_am") if alt_status else None
    if isinstance(letzter_check, str):
        try: letzter_check = datetime.fromisoformat(letzter_check[:19])
        except Exception: letzter_check = None

    minuten_seit_check = None
    if letzter_check is not None:
        minuten_seit_check = (jetzt - letzter_check).total_seconds() / 60
        if minuten_seit_check < MINDESTABSTAND_MINUTEN:
            return False

    bekannte_verspaetung = (alt_status.get("verspaetung_minuten") or 0) if alt_status else 0
    if bekannte_verspaetung > 0:
        # Bereits verspätet und mindestens MINDESTABSTAND_MINUTEN seit dem
        # letzten Check vergangen (oder noch nie geprüft) -> einfach erneut nachsehen.
        return True

    for p in _checkpoints_fuer_segment(seg, alt_status):
        if p <= jetzt and (letzter_check is None or letzter_check < p):
            return True
    return False


# ── Konfiguration (nur noch informativ – Zeitpunkte oben sind fest codiert) ────

def konfiguration_laden() -> dict:
    """Für die Anzeige auf der Einstellungsseite – das Schema selbst ist fest."""
    return {"checkpoints": ["4h", "3h", "2h", "1h", "30min", "15min"],
            "verspaetung_alarm_ab_min": VERSPAETUNGS_ALARM_SCHWELLE_MIN}


# ── Externe APIs ───────────────────────────────────────────────────────────────

def flugstatus_abrufen(transport_nummer: str, abreise_datum: date) -> dict:
    """Fragt AeroDataBox nach dem aktuellen Status eines Fluges ab."""
    if not AERODATABOX_API_KEY:
        return {"fehler": "AERODATABOX_API_KEY nicht konfiguriert"}
    try:
        nummer = transport_nummer.replace(" ", "")
        url = f"https://{AERODATABOX_HOST}/flights/number/{nummer}/{abreise_datum.isoformat()}"
        resp = httpx.get(url, headers={
            "X-RapidAPI-Key": AERODATABOX_API_KEY,
            "X-RapidAPI-Host": AERODATABOX_HOST,
        }, timeout=20)
        resp.raise_for_status()
        daten = resp.json()
        flug = daten[0] if isinstance(daten, list) and daten else daten
        if not flug:
            return {"fehler": "Kein Flug gefunden"}
        abflug = flug.get("departure", {})
        status = flug.get("status", "Unbekannt")
        verspaetung = None
        geplant = abflug.get("scheduledTime", {}).get("local")
        revidiert = abflug.get("revisedTime", {}).get("local")
        if geplant and revidiert:
            try:
                t1 = datetime.fromisoformat(geplant[:19])
                t2 = datetime.fromisoformat(revidiert[:19])
                verspaetung = int((t2 - t1).total_seconds() / 60)
            except Exception:
                pass
        return {
            "status": status,
            "verspaetung_minuten": verspaetung,
            "gate": abflug.get("gate"),
            "terminal": abflug.get("terminal"),
            "rohdaten": json.dumps(flug, ensure_ascii=False)[:4000],
        }
    except Exception as e:
        return {"fehler": str(e)}


def bahnstatus_abrufen(transport_nummer: str, von_ort: str, abreise_datum: date, abreise_zeit: str) -> dict:
    """Fragt die kostenlose db.transport.rest-Schnittstelle nach dem Zugstatus ab."""
    try:
        # 1. Bahnhof suchen
        resp = httpx.get(f"{DB_TRANSPORT_REST_URL}/locations",
                          params={"query": von_ort, "results": 1}, timeout=20)
        resp.raise_for_status()
        orte = resp.json()
        if not orte:
            return {"fehler": f"Bahnhof '{von_ort}' nicht gefunden"}
        stop_id = orte[0].get("id")

        # 2. Abfahrten an diesem Bahnhof zur passenden Zeit abrufen
        when = f"{abreise_datum.isoformat()}T{abreise_zeit or '00:00'}:00"
        resp = httpx.get(f"{DB_TRANSPORT_REST_URL}/stops/{stop_id}/departures",
                          params={"when": when, "duration": 120}, timeout=20)
        resp.raise_for_status()
        abfahrten = resp.json().get("departures", resp.json()) if isinstance(resp.json(), dict) else resp.json()

        treffer = None
        nummer_norm = transport_nummer.replace(" ", "").upper()
        for a in abfahrten:
            line_name = (a.get("line", {}).get("name") or "").replace(" ", "").upper()
            if nummer_norm in line_name or line_name in nummer_norm:
                treffer = a
                break
        if not treffer:
            return {"fehler": f"Zug '{transport_nummer}' nicht in den Abfahrten gefunden"}

        verspaetung = treffer.get("delay")
        verspaetung_min = int(verspaetung / 60) if verspaetung else 0
        status = "cancelled" if treffer.get("cancelled") else (
            "delayed" if verspaetung_min and verspaetung_min > 0 else "on-time")
        return {
            "status": status,
            "verspaetung_minuten": verspaetung_min,
            "gate": None,
            "terminal": treffer.get("platform"),
            "rohdaten": json.dumps(treffer, ensure_ascii=False)[:4000],
        }
    except Exception as e:
        return {"fehler": str(e)}


# ── Segmente aus Belegen extrahieren ────────────────────────────────────────────

def _to_d_ddmmyyyy(v):
    if not v: return None
    try: return datetime.strptime(str(v).strip(), "%d.%m.%Y").date()
    except Exception: return None


def ueberwachte_segmente_laden(debug: bool = False):
    """
    Liest alle Flug-/Bahn-Segmente aus Belegen mit Abreise in den nächsten 24h,
    die noch nicht (lange) stattgefunden haben. Filtert bewusst NICHT scharf über
    das Top-Level-Feld event_datum_von in der SQL-Abfrage – bei mehrteiligen
    Tickets (Hin-/Rückflug, mehrere Segmente) kann dieses Feld vom tatsächlichen
    Datum eines einzelnen Segments abweichen. Stattdessen wird ein großzügiges
    Fenster geladen und die genaue Filterung anhand der echten Segment-Daten
    (aus dem KI-JSON) vorgenommen.
    Mit debug=True wird zusätzlich ein Diagnose-Dict zurückgegeben (zweites
    Rückgabeelement), das zeigt, wie viele Belege/Segmente gefunden, aber
    aus welchem Grund verworfen wurden – hilfreich, wenn "0 gefunden" gemeldet
    wird, obwohl ein Beleg eigentlich vorhanden sein sollte.
    """
    db = get_db(); cur = db.cursor()
    P = ph()
    heute = date.today()
    fenster_von = heute - timedelta(days=2)
    fenster_bis = heute + timedelta(days=3)
    cur.execute(f"""SELECT id, reise_code, transportart, ki_json, event_datum_von
                    FROM belege
                    WHERE transportart IN ('Flug','Bahn')
                    AND (
                        (event_datum_von >= {P} AND event_datum_von <= {P})
                        OR event_datum_von IS NULL
                    )""",
                (fenster_von.isoformat(), fenster_bis.isoformat()))
    rows = cur.fetchall()
    cur.close(); db.close()

    jetzt = jetzt_lokal()
    segmente = []
    diag = {
        "jetzt_lokal": jetzt.isoformat(),
        "belege_gefunden": len(rows),
        "belege_details": [],
    }
    for r in rows:
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        bid = g("id", 0)
        rcode = g("reise_code", 1)
        typ = g("transportart", 2)
        ki_str = g("ki_json", 3) or ""
        beleg_diag = {"beleg_id": bid, "hat_ki_json": bool(ki_str), "segmente_roh": 0,
                      "segmente_uebernommen": 0, "verworfen": []}
        try:
            segs = json.loads(ki_str).get("segmente") or []
        except Exception:
            segs = []
        beleg_diag["segmente_roh"] = len(segs)
        for idx, s in enumerate(segs):
            d_ab = _to_d_ddmmyyyy(s.get("abreise_datum"))
            zeit_ab = s.get("abreise_zeit") or "00:00"
            if not d_ab:
                beleg_diag["verworfen"].append(
                    f"Segment {idx}: kein/ungültiges abreise_datum ('{s.get('abreise_datum')}')")
                continue
            try:
                dt_ab = datetime.strptime(f"{d_ab.isoformat()} {zeit_ab}", "%Y-%m-%d %H:%M")
            except Exception:
                beleg_diag["verworfen"].append(
                    f"Segment {idx}: Datum/Zeit nicht parsbar ({d_ab} {zeit_ab})")
                continue
            stunden_bis = (dt_ab - jetzt).total_seconds() / 3600
            if stunden_bis < -24 or stunden_bis > 24:
                # -24h Kulanz: bei Verspätungen liegt die GEPLANTE Abreise schon
                # in der Vergangenheit, obwohl der Flug/Zug noch nicht losgefahren
                # ist. Erst nach 24h ohne bestätigten Abflug gilt das Segment als
                # abgeschlossen und wird nicht mehr überwacht.
                beleg_diag["verworfen"].append(
                    f"Segment {idx} ({s.get('transport_nummer')}, {dt_ab}): "
                    f"{stunden_bis:.1f}h bis Abreise – außerhalb -24h/+24h-Fenster")
                continue

            # Ankunftszeit parsen (für den Lande-Checkpoint 30min vor Landung
            # nötig) – falls nicht vorhanden/parsbar, wird als grobe Näherung
            # 2h nach Abreise angenommen, damit der Checkpoint trotzdem existiert.
            d_an = _to_d_ddmmyyyy(s.get("ankunft_datum"))
            zeit_an = s.get("ankunft_zeit") or "00:00"
            dt_an = None
            if d_an:
                try:
                    dt_an = datetime.strptime(f"{d_an.isoformat()} {zeit_an}", "%Y-%m-%d %H:%M")
                except Exception:
                    dt_an = None
            if not dt_an or dt_an <= dt_ab:
                dt_an = dt_ab + timedelta(hours=2)

            beleg_diag["segmente_uebernommen"] += 1
            segmente.append({
                "beleg_id": bid, "reise_code": rcode, "segment_index": idx,
                "transport_typ": typ, "transport_nummer": s.get("transport_nummer") or "",
                "von_ort": s.get("von_ort") or s.get("von_iata") or "",
                "nach_ort": s.get("nach_ort") or s.get("nach_iata") or "",
                "abreise_datum": d_ab, "abreise_zeit": zeit_ab,
                "stunden_bis_abreise": stunden_bis,
                "dt_ab": dt_ab, "dt_an": dt_an,
            })
        diag["belege_details"].append(beleg_diag)

    if debug:
        return segmente, diag
    return segmente


def status_holen(beleg_id: int, segment_index: int) -> dict | None:
    db = get_db(); cur = db.cursor()
    P = ph()
    cur.execute(f"SELECT letzter_check_am, status, verspaetung_minuten, gate, terminal "
                f"FROM flug_status WHERE beleg_id={P} AND segment_index={P}", (beleg_id, segment_index))
    r = cur.fetchone()
    cur.close(); db.close()
    if not r:
        return None
    g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
    return {"letzter_check_am": g("letzter_check_am", 0), "status": g("status", 1),
            "verspaetung_minuten": g("verspaetung_minuten", 2), "gate": g("gate", 3),
            "terminal": g("terminal", 4)}


def status_speichern(seg: dict, ergebnis: dict):
    """
    Speichert Zeitstempel EXPLIZIT als Python-Wert (jetzt_lokal(), Europe/Berlin) –
    NICHT über die DB-Funktion NOW()/datetime('now'). Die lief in UTC, während
    der spätere Vergleich in cron_flug_alerts() mit jetzt_lokal() (Berlin, im
    Sommer UTC+2) rechnet. Diese ~2h-Differenz führte dazu, dass jedes Segment
    bei JEDEM minütlichen Cron-Aufruf als "längst fällig" galt und die externe
    API faktisch bei jedem Tick erneut abgefragt wurde – Ursache der massiven
    Anfragenflut.
    """
    db = get_db(); cur = db.cursor()
    P = ph()
    jetzt = jetzt_lokal().isoformat()
    cur.execute(f"""SELECT id FROM flug_status WHERE beleg_id={P} AND segment_index={P}""",
                (seg["beleg_id"], seg["segment_index"]))
    vorhanden = cur.fetchone()
    if vorhanden:
        cur.execute(f"""UPDATE flug_status SET letzter_check_am={P}, status={P},
            verspaetung_minuten={P}, gate={P}, terminal={P}, rohdaten={P}
            WHERE beleg_id={P} AND segment_index={P}""",
            (jetzt, ergebnis.get("status"), ergebnis.get("verspaetung_minuten"),
             ergebnis.get("gate"), ergebnis.get("terminal"), ergebnis.get("rohdaten"),
             seg["beleg_id"], seg["segment_index"]))
    else:
        cur.execute(f"""INSERT INTO flug_status
            (beleg_id, segment_index, transport_typ, transport_nummer, von_ort, nach_ort,
             abreise_datum, abreise_zeit, letzter_check_am, status, verspaetung_minuten,
             gate, terminal, rohdaten)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
            (seg["beleg_id"], seg["segment_index"], seg["transport_typ"], seg["transport_nummer"],
             seg["von_ort"], seg["nach_ort"], seg["abreise_datum"].isoformat(), seg["abreise_zeit"],
             jetzt, ergebnis.get("status"), ergebnis.get("verspaetung_minuten"),
             ergebnis.get("gate"), ergebnis.get("terminal"), ergebnis.get("rohdaten")))
    db.commit(); cur.close(); db.close()


def alert_markieren(beleg_id: int, segment_index: int):
    db = get_db(); cur = db.cursor()
    P = ph()
    jetzt = jetzt_lokal().isoformat()
    cur.execute(f"UPDATE flug_status SET alert_gesendet_am={P} "
                f"WHERE beleg_id={P} AND segment_index={P}", (jetzt, beleg_id, segment_index))
    db.commit(); cur.close(); db.close()


def relevante_aenderung(alt: dict | None, neu: dict) -> bool:
    """
    Alarmiert sofort bei:
    - Stornierung/Umleitung
    - Verspätung > 15 Minuten, sobald diese Schwelle erstmals erreicht/
      überschritten wird (nicht bei jedem weiteren Check erneut, außer die
      Verspätung wächst nochmal um mind. 15 Minuten weiter)
    """
    neu_status = (neu.get("status") or "").lower()
    if any(k in neu_status for k in ("cancel", "divert")):
        return True
    neu_v = neu.get("verspaetung_minuten") or 0
    alt_v = (alt.get("verspaetung_minuten") or 0) if alt else 0
    if neu_v >= VERSPAETUNGS_ALARM_SCHWELLE_MIN and (
            alt is None or alt_v < VERSPAETUNGS_ALARM_SCHWELLE_MIN or neu_v - alt_v >= 15):
        return True
    if alt and alt.get("gate") != neu.get("gate") and neu.get("gate"):
        return True
    return False


def reisende_und_organisatoren_mailadressen(reise_code: str) -> list:
    db = get_db(); cur = db.cursor()
    P = ph()
    cur.execute(f"""SELECT DISTINCT m.email FROM reise_mitarbeiter rm
                    JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel
                    WHERE rm.reise_code={P} AND m.email IS NOT NULL""", (reise_code,))
    reisende_mails = [r[0] if isinstance(r, tuple) else r["email"] for r in cur.fetchall()]
    cur.execute(f"""SELECT DISTINCT email FROM mitarbeiter WHERE ist_organisator={P} AND email IS NOT NULL""",
                (True if is_postgres() else 1,))
    org_mails = [r[0] if isinstance(r, tuple) else r["email"] for r in cur.fetchall()]
    cur.close(); db.close()
    return list(set(reisende_mails + org_mails))


TERMINAL_STATUS_SCHLUESSELWOERTER = ("arriv", "cancel", "divert", "land")

def cron_flug_alerts(debug: bool = False) -> dict:
    """
    Wird von einem externen Cron-Pinger regelmäßig (idealerweise minütlich)
    aufgerufen. Prüft für jedes Flug-/Bahn-Segment, ob laut konfiguriertem
    Prüft für jedes Flug-/Bahn-Segment anhand fester Checkpoints (siehe
    segment_check_faellig), ob JETZT ein Check fällig ist, ruft bei Bedarf die
    externe API auf und verschickt bei relevanten Änderungen (Verspätung
    >= 15 Min., Stornierung, Umleitung) sofort einen Alert.

    Sparmaßnahme: Ist der zuletzt bekannte Status bereits ein Endzustand
    (angekommen/gelandet/storniert/umgeleitet), wird das Segment NICHT mehr
    weiter abgefragt – ein bereits gelandeter Flug ändert seinen Status
    praktisch nie mehr.
    """
    if debug:
        segmente, diag = ueberwachte_segmente_laden(debug=True)
        diag["verarbeitung"] = []
    else:
        segmente = ueberwachte_segmente_laden()
        diag = None
    geprueft = 0
    alerts_gesendet = 0
    fehler = []

    for seg in segmente:
        schritt = {"beleg_id": seg["beleg_id"], "segment_index": seg["segment_index"],
                   "transport_nummer": seg["transport_nummer"],
                   "stunden_bis_abreise": round(seg["stunden_bis_abreise"], 2)}
        alt_status = status_holen(seg["beleg_id"], seg["segment_index"])

        alter_status_text = (alt_status.get("status") or "").lower() if alt_status else ""
        if alter_status_text and any(k in alter_status_text for k in TERMINAL_STATUS_SCHLUESSELWOERTER):
            schritt["ergebnis"] = f"Status bereits final ('{alt_status.get('status')}') – keine weitere Abfrage nötig"
            if debug: diag["verarbeitung"].append(schritt)
            continue

        naechste_checkpoints = [p.strftime("%d.%m. %H:%M") for p in _checkpoints_fuer_segment(seg, alt_status)]
        schritt["checkpoints"] = naechste_checkpoints
        schritt["letzter_check_am"] = str(alt_status.get("letzter_check_am")) if alt_status else None

        if not segment_check_faellig(seg, jetzt_lokal(), alt_status):
            schritt["ergebnis"] = "kein Checkpoint erreicht – übersprungen"
            if debug: diag["verarbeitung"].append(schritt)
            continue

        if seg["transport_typ"] == "Flug" and seg["transport_nummer"]:
            schritt["quelle"] = "AeroDataBox"
            schritt["api_key_gesetzt"] = bool(AERODATABOX_API_KEY)
            ergebnis = flugstatus_abrufen(seg["transport_nummer"], seg["abreise_datum"])
        elif seg["transport_typ"] == "Bahn" and seg["transport_nummer"]:
            schritt["quelle"] = "db.transport.rest"
            ergebnis = bahnstatus_abrufen(seg["transport_nummer"], seg["von_ort"],
                                          seg["abreise_datum"], seg["abreise_zeit"])
        else:
            schritt["ergebnis"] = "keine Transportnummer – übersprungen"
            if debug: diag["verarbeitung"].append(schritt)
            continue

        schritt["api_antwort"] = ergebnis

        if ergebnis.get("fehler"):
            fehler.append(f"Beleg {seg['beleg_id']} Segment {seg['segment_index']}: {ergebnis['fehler']}")
            schritt["ergebnis"] = f"API-Fehler: {ergebnis['fehler']}"
            if debug: diag["verarbeitung"].append(schritt)
            continue

        geprueft += 1
        status_speichern(seg, ergebnis)
        schritt["ergebnis"] = "erfolgreich geprüft"

        if relevante_aenderung(alt_status, ergebnis):
            empfaenger = reisende_und_organisatoren_mailadressen(seg["reise_code"])
            betreff = (f"⚠ {seg['transport_typ']} {seg['transport_nummer']} "
                       f"({seg['von_ort']}→{seg['nach_ort']}): {ergebnis.get('status')}")
            text = (f"Statusänderung bei {seg['transport_typ']} {seg['transport_nummer']}\n"
                    f"Reise: {seg['reise_code']}\n"
                    f"Strecke: {seg['von_ort']} → {seg['nach_ort']}\n"
                    f"Geplante Abreise: {seg['abreise_datum'].strftime('%d.%m.%Y')} {seg['abreise_zeit']}\n"
                    f"Status: {ergebnis.get('status')}\n"
                    f"Verspätung: {ergebnis.get('verspaetung_minuten') or 0} Minuten\n"
                    + (f"Gate: {ergebnis.get('gate')}\n" if ergebnis.get('gate') else "")
                    + (f"Terminal/Gleis: {ergebnis.get('terminal')}\n" if ergebnis.get('terminal') else ""))
            for empf in empfaenger:
                sende_mail(empf, betreff, text)
            alert_markieren(seg["beleg_id"], seg["segment_index"])
            alerts_gesendet += 1
            schritt["alert_gesendet"] = True

        if debug: diag["verarbeitung"].append(schritt)

    ergebnis = {"geprueft": geprueft, "alerts_gesendet": alerts_gesendet, "fehler": fehler,
                "segmente_im_fenster": len(segmente)}
    if debug:
        ergebnis["diagnose"] = diag
    return ergebnis


def offene_alerts_fuer_dashboard() -> list:
    """Für die Dashboard-Anzeige: Segmente mit kürzlich gesendetem Alert
    (nur die letzten 24 Stunden – danach verschwindet der Hinweis automatisch,
    auch ohne manuelles Wegklicken)."""
    db = get_db(); cur = db.cursor()
    grenze = (jetzt_lokal() - timedelta(hours=24)).isoformat()
    P = ph()
    cur.execute(f"""SELECT beleg_id, transport_typ, transport_nummer, von_ort, nach_ort,
                   status, verspaetung_minuten, alert_gesendet_am
                   FROM flug_status
                   WHERE alert_gesendet_am IS NOT NULL AND alert_gesendet_am >= {P}
                   ORDER BY alert_gesendet_am DESC LIMIT 5""", (grenze,))
    rows = cur.fetchall()
    cur.close(); db.close()
    out = []
    for r in rows:
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        out.append({"beleg_id": g("beleg_id",0), "typ": g("transport_typ",1),
                     "nummer": g("transport_nummer",2), "von": g("von_ort",3), "nach": g("nach_ort",4),
                     "status": g("status",5), "verspaetung": g("verspaetung_minuten",6)})
    return out
