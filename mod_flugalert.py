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


# ── Konfiguration ──────────────────────────────────────────────────────────────
# Drei Stufen: "fern" (mehr als 4h vor Abflug ODER nach der geplanten Abflugzeit,
# z.B. bei Verspätung/Warten auf Bestätigung des tatsächlichen Abflugs),
# "4h" (4h bis 1h vor Abflug), "1h" (unter 1h vor Abflug).

def konfiguration_laden() -> dict:
    db = get_db(); cur = db.cursor()
    cur.execute("SELECT intervall_24h_min, intervall_4h_min, intervall_1h_min "
                "FROM alert_konfiguration ORDER BY id LIMIT 1")
    r = cur.fetchone()
    cur.close(); db.close()
    if not r:
        return {"fern": 60, "4h": 30, "1h": 15}
    g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
    return {"fern": g("intervall_24h_min", 0), "4h": g("intervall_4h_min", 1),
            "1h": g("intervall_1h_min", 2)}


def konfiguration_speichern(i_fern: int, i_4h: int, i_1h: int):
    db = get_db(); cur = db.cursor()
    P = ph()
    cur.execute("SELECT id FROM alert_konfiguration ORDER BY id LIMIT 1")
    r = cur.fetchone()
    if r:
        gid = r[0] if isinstance(r, tuple) else r["id"]
        if is_postgres():
            cur.execute(f"""UPDATE alert_konfiguration SET
                intervall_24h_min={P}, intervall_4h_min={P},
                intervall_1h_min={P}, aktualisiert_am=NOW() WHERE id={P}""",
                (i_fern, i_4h, i_1h, gid))
        else:
            cur.execute(f"""UPDATE alert_konfiguration SET
                intervall_24h_min={P}, intervall_4h_min={P},
                intervall_1h_min={P}, aktualisiert_am=datetime('now') WHERE id={P}""",
                (i_fern, i_4h, i_1h, gid))
    else:
        cur.execute(f"""INSERT INTO alert_konfiguration
            (intervall_24h_min, intervall_4h_min, intervall_1h_min)
            VALUES ({P},{P},{P})""", (i_fern, i_4h, i_1h))
    db.commit(); cur.close(); db.close()


def intervall_fuer(stunden_bis_abreise: float, konfig: dict) -> int | None:
    """
    Gibt das passende Prüfintervall (Minuten) zurück, oder None wenn außerhalb
    des Überwachungsfensters (mehr als 24h vorher oder mehr als 24h nach der
    geplanten Abreise).
    - Unter 1h vor Abflug: höchste Frequenz (Stufe "1h")
    - 1h bis 4h vor Abflug: mittlere Frequenz (Stufe "4h")
    - Mehr als 4h vor Abflug ODER nach der geplanten Abflugzeit (z.B. bei
      Verspätung, wo die geplante Zeit schon verstrichen ist): Stufe "fern"
    """
    if stunden_bis_abreise < -24 or stunden_bis_abreise > 24:
        return None
    if 0 <= stunden_bis_abreise <= 1:
        return konfig["1h"]
    if 1 < stunden_bis_abreise <= 4:
        return konfig["4h"]
    return konfig["fern"]


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
            beleg_diag["segmente_uebernommen"] += 1
            segmente.append({
                "beleg_id": bid, "reise_code": rcode, "segment_index": idx,
                "transport_typ": typ, "transport_nummer": s.get("transport_nummer") or "",
                "von_ort": s.get("von_ort") or s.get("von_iata") or "",
                "nach_ort": s.get("nach_ort") or s.get("nach_iata") or "",
                "abreise_datum": d_ab, "abreise_zeit": zeit_ab,
                "stunden_bis_abreise": stunden_bis,
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
    db = get_db(); cur = db.cursor()
    P = ph()
    jetzt_sql = "NOW()" if is_postgres() else "datetime('now')"
    cur.execute(f"""SELECT id FROM flug_status WHERE beleg_id={P} AND segment_index={P}""",
                (seg["beleg_id"], seg["segment_index"]))
    vorhanden = cur.fetchone()
    if vorhanden:
        cur.execute(f"""UPDATE flug_status SET letzter_check_am={jetzt_sql}, status={P},
            verspaetung_minuten={P}, gate={P}, terminal={P}, rohdaten={P}
            WHERE beleg_id={P} AND segment_index={P}""",
            (ergebnis.get("status"), ergebnis.get("verspaetung_minuten"),
             ergebnis.get("gate"), ergebnis.get("terminal"), ergebnis.get("rohdaten"),
             seg["beleg_id"], seg["segment_index"]))
    else:
        cur.execute(f"""INSERT INTO flug_status
            (beleg_id, segment_index, transport_typ, transport_nummer, von_ort, nach_ort,
             abreise_datum, abreise_zeit, letzter_check_am, status, verspaetung_minuten,
             gate, terminal, rohdaten)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{jetzt_sql},{P},{P},{P},{P},{P})""",
            (seg["beleg_id"], seg["segment_index"], seg["transport_typ"], seg["transport_nummer"],
             seg["von_ort"], seg["nach_ort"], seg["abreise_datum"].isoformat(), seg["abreise_zeit"],
             ergebnis.get("status"), ergebnis.get("verspaetung_minuten"),
             ergebnis.get("gate"), ergebnis.get("terminal"), ergebnis.get("rohdaten")))
    db.commit(); cur.close(); db.close()


def alert_markieren(beleg_id: int, segment_index: int):
    db = get_db(); cur = db.cursor()
    P = ph()
    jetzt_sql = "NOW()" if is_postgres() else "datetime('now')"
    cur.execute(f"UPDATE flug_status SET alert_gesendet_am={jetzt_sql} "
                f"WHERE beleg_id={P} AND segment_index={P}", (beleg_id, segment_index))
    db.commit(); cur.close(); db.close()


def relevante_aenderung(alt: dict | None, neu: dict) -> bool:
    """Nur bei echten Änderungen (nicht bei jedem Check) alarmieren."""
    if alt is None:
        return neu.get("status") in ("cancelled", "delayed") or (neu.get("verspaetung_minuten") or 0) > 0
    if alt.get("status") != neu.get("status"):
        return True
    alt_v = alt.get("verspaetung_minuten") or 0
    neu_v = neu.get("verspaetung_minuten") or 0
    if abs(neu_v - alt_v) >= 10:
        return True
    if alt.get("gate") != neu.get("gate") and neu.get("gate"):
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


def cron_flug_alerts(debug: bool = False) -> dict:
    """
    Wird von einem externen Cron-Pinger regelmäßig (idealerweise minütlich)
    aufgerufen. Prüft für jedes Flug-/Bahn-Segment, ob laut konfiguriertem
    Intervall ein neuer Check fällig ist, ruft bei Bedarf die externe API auf
    und verschickt bei relevanten Änderungen einen Alert.
    """
    konfig = konfiguration_laden()
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
        intervall = intervall_fuer(seg["stunden_bis_abreise"], konfig)
        schritt["intervall_minuten"] = intervall
        if intervall is None:
            schritt["ergebnis"] = "kein Intervall (außerhalb Fenster) – übersprungen"
            if debug: diag["verarbeitung"].append(schritt)
            continue
        alt_status = status_holen(seg["beleg_id"], seg["segment_index"])
        letzter_check = alt_status.get("letzter_check_am") if alt_status else None
        schritt["letzter_check_am"] = str(letzter_check) if letzter_check else None
        if letzter_check:
            if isinstance(letzter_check, str):
                try:
                    letzter_check = datetime.fromisoformat(letzter_check[:19])
                except Exception:
                    letzter_check = None
            if letzter_check:
                minuten_seit_check = (jetzt_lokal() - letzter_check).total_seconds() / 60
                schritt["minuten_seit_letztem_check"] = round(minuten_seit_check, 1)
                if minuten_seit_check < intervall:
                    schritt["ergebnis"] = f"noch nicht fällig (erst in {intervall - minuten_seit_check:.1f} Min. wieder)"
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
