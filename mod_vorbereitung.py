"""
mod_vorbereitung.py – Hinweise zur Reisevorbereitung für den Organisator (v3.63)

Wird NUR oben auf der Reise-Seite und nur für Organisatoren angezeigt (keine Mail, kein ToDo, kein Dashboard).

Geprüft wird:
1. Hotelnächte ohne Buchung. Eine Nacht gilt als abgedeckt, wenn
   - ein Hotel sie umfasst (Check-in <= Nacht < Check-out; ohne Check-out-Datum zählen "Nächte", sonst 1 Nacht), oder
   - ein Nachtflug bzw. Nachtzug sie überbrückt (mindestens 3 Stunden zwischen 23:00 und 05:00 Uhr Ortszeit des Abflugs).
   Landet ein Flug nachts (z.B. 01:30), reicht er nicht als Unterkunft: Die Nacht bleibt offen, der Hinweis nennt die späte Ankunft.
2. Zwei aufeinanderfolgende Hotels, die weit auseinander liegen (ab 50 km Luftlinie oder verschiedene Länder),
   ohne Flug, Bahn, Mietwagen oder Fahrt-Termin dazwischen.

Jeder Hinweis lässt sich einzeln ausblenden (Tabelle reise_hinweis_aus, mit Grund) und wieder einblenden.
Es werden nur Reisen geprüft, die aktiv und noch nicht beendet sind.
"""
from __future__ import annotations
import json
import math
import re
from datetime import date, datetime, timedelta
from html import escape as e

from mod_db import get_db, ph, is_postgres
from mod_zeit import segment_zeit_zu_utc

TABELLE = "reise_hinweis_aus"
NACHT_VON, NACHT_BIS = "23:00", "05:00"     # Nachtfenster (Ortszeit des Abflugs)
NACHT_MIN_STUNDEN = 3.0                     # so lange muss ein Flug/Zug im Nachtfenster unterwegs sein
ABSTAND_KM = 50.0                           # ab dieser Luftlinie gelten zwei Hotels als "weit auseinander"
WT = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
GRUENDE_NACHT = ["Privat übernachtet", "Im Flugzeug / Nachtzug", "Bei Kunde / Bekannten", "Sonstiges"]
GRUENDE_TRANSPORT = ["Fahrt anders organisiert", "Abholung durch Kunde / Kollegen", "Sonstiges"]
_SCHLUESSEL = re.compile(r"^(nacht:\d{4}-\d{2}-\d{2}|transport:\d+:\d+)$")


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def _d(x) -> date | None:
    """Datum aus date, datetime oder 'YYYY-MM-DD…' (SQLite liefert Text, PostgreSQL Datumswerte)."""
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return None


def _tag(d: date) -> str:
    return "%s %s" % (WT[d.weekday()], d.strftime("%d.%m."))


# ── Tabelle für ausgeblendete Hinweise ───────────────────────────────────────────────────
def tabelle_sichern() -> None:
    """Legt die Tabelle an, falls sie fehlt (Neustart genügt, /init ist nicht nötig)."""
    try:
        db = get_db(); cur = db.cursor()
        key = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} ("
                    f"id {key}, reise_code TEXT NOT NULL, schluessel TEXT NOT NULL, grund TEXT, "
                    f"von TEXT, erstellt TEXT, UNIQUE (reise_code, schluessel))")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[Vorbereitung] Tabelle nicht angelegt: {ex}")


def ausgeblendet_laden(rcode: str) -> dict:
    """{schluessel: grund} der ausgeblendeten Hinweise dieser Reise."""
    try:
        db = get_db(); cur = db.cursor()
        try:
            cur.execute(f"SELECT schluessel, grund FROM {TABELLE} WHERE reise_code={ph()}", (str(rcode).upper(),))
            return {_g(r, "schluessel", 0): (_g(r, "grund", 1) or "") for r in cur.fetchall()}
        finally:
            cur.close(); db.close()
    except Exception:
        return {}


def ausblenden(rcode: str, schluessel: list, grund: str = "", von: str = "") -> int:
    """Blendet Hinweise aus. Ungültige Schlüssel werden ignoriert. Gibt die Anzahl gespeicherter Hinweise zurück."""
    keys = [k.strip() for k in schluessel if _SCHLUESSEL.match((k or "").strip())]
    if not keys:
        return 0
    db = get_db(); cur = db.cursor()
    try:
        for k in keys:
            cur.execute(f"DELETE FROM {TABELLE} WHERE reise_code={ph()} AND schluessel={ph()}", (rcode.upper(), k))
            cur.execute(f"INSERT INTO {TABELLE} (reise_code, schluessel, grund, von, erstellt) VALUES ({ph()},{ph()},{ph()},{ph()},{ph()})",
                        (rcode.upper(), k, (grund or "")[:120], (von or "")[:60], datetime.now().strftime("%Y-%m-%d %H:%M")))
        db.commit()
    except Exception:
        db.rollback(); raise
    finally:
        cur.close(); db.close()
    return len(keys)


def einblenden(rcode: str, schluessel: str) -> None:
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {TABELLE} WHERE reise_code={ph()} AND schluessel={ph()}", (rcode.upper(), schluessel))
        db.commit()
    except Exception:
        db.rollback(); raise
    finally:
        cur.close(); db.close()


# ── Daten der Reise lesen ────────────────────────────────────────────────────────────────
def _reisender(x) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().lower()


def daten_laden(rcode: str) -> dict | None:
    """Liest Reisezeitraum, Hotels, Flug-/Bahnsegmente, Mietwagen und Fahrt-Termine. None = nicht prüfbar."""
    rcode = str(rcode).upper()
    db = get_db(); cur = db.cursor()
    try:
        try:
            cur.execute(f"SELECT abreise, rueckkehr, status FROM reisen WHERE code={ph()}", (rcode,))
        except Exception:
            db.rollback()
            cur.execute(f"SELECT abreise, rueckkehr, NULL FROM reisen WHERE code={ph()}", (rcode,))
        r = cur.fetchone()
        if not r:
            return None
        out = {"abreise": _d(_g(r, "abreise", 0)), "rueckkehr": _d(_g(r, "rueckkehr", 1)),
               "status": _g(r, "status", 2) or "aktiv", "hotels": [], "segmente": [], "mietwagen": [], "fahrten": []}
        cur.execute(f"""SELECT id, transportart, anbieter, reisender, ki_json, hotel_name, hotel_adresse, hotel_lat, hotel_lon,
                        hotel_checkin_datum, hotel_checkout_datum, hotel_naechte, land_beleg, event_datum_von, event_datum_bis
                        FROM belege WHERE reise_code={ph()} AND transportart IN ('Flug','Bahn','Mietwagen','Hotel')
                        AND aenderung_status IS NULL ORDER BY id""", (rcode,))
        for b in cur.fetchall():
            art = _g(b, "transportart", 1)
            if art == "Hotel":
                ci = _d(_g(b, "hotel_checkin_datum", 9)); co = _d(_g(b, "hotel_checkout_datum", 10))
                if not ci:
                    continue
                if not co:
                    try:
                        n = int(_g(b, "hotel_naechte", 11) or 1)
                    except (TypeError, ValueError):
                        n = 1
                    co = ci + timedelta(days=max(n, 1))
                if co <= ci:
                    continue
                lat, lon = _g(b, "hotel_lat", 7), _g(b, "hotel_lon", 8)
                try:
                    koord = (float(lat), float(lon)) if lat is not None and lon is not None else None
                except (TypeError, ValueError):
                    koord = None
                out["hotels"].append({"id": _g(b, "id", 0), "name": _g(b, "hotel_name", 5) or _g(b, "anbieter", 2) or "Hotel",
                                      "adresse": _g(b, "hotel_adresse", 6) or "", "koord": koord, "ci": ci, "co": co,
                                      "land": str(_g(b, "land_beleg", 12) or "").strip().upper()[:2],
                                      "reisender": _reisender(_g(b, "reisender", 3))})
                continue
            try:
                segs = json.loads(_g(b, "ki_json", 4) or "{}").get("segmente") or []
            except Exception:
                segs = []
            if art == "Mietwagen":
                pv, pb = _d(_g(b, "event_datum_von", 13)), _d(_g(b, "event_datum_bis", 14))
                for s in segs:
                    pv = _d(s.get("abreise_datum")) or pv
                    pb = _d(s.get("ankunft_datum")) or pb
                if pv:
                    out["mietwagen"].append({"von": pv, "bis": pb or pv})
                continue
            for s in segs:
                d_ab = _d(s.get("abreise_datum"))
                if not d_ab:
                    continue
                d_an = _d(s.get("ankunft_datum")) or d_ab
                dt_ab = segment_zeit_zu_utc(d_ab, s.get("abreise_zeit"), s.get("abreise_utc_offset"))
                dt_an = (segment_zeit_zu_utc(d_an, s.get("ankunft_zeit"), s.get("ankunft_utc_offset"))
                         if s.get("ankunft_zeit") else None)
                if dt_ab and dt_an and dt_an <= dt_ab:
                    dt_an = dt_an + timedelta(days=1)          # Nachtflug/-zug ohne Ankunftsdatum
                    d_an = d_an + timedelta(days=1) if d_an == d_ab else d_an
                out["segmente"].append({
                    "nr": re.sub(r"\s+", "", str(s.get("transport_nummer") or "")).upper() or (_g(b, "anbieter", 2) or ""),
                    "d_ab": d_ab, "d_an": d_an, "dt_ab": dt_ab, "dt_an": dt_an,
                    "off_ab": s.get("abreise_utc_offset"), "an_zeit": str(s.get("ankunft_zeit") or "")})
        try:
            cur.execute(f"SELECT datum FROM termine WHERE reise_code={ph()} AND typ IN ('fahrt','mietwagen')", (rcode,))
            out["fahrten"] = [d for d in (_d(_g(t, "datum", 0)) for t in cur.fetchall()) if d]
        except Exception:
            db.rollback()
        return out
    finally:
        cur.close(); db.close()


# ── Prüfungen (reine Funktionen, ohne Datenbank) ─────────────────────────────────────────
def _ueberbrueckt(nacht: date, seg: dict) -> float:
    """Stunden, die ein Flug/Zug im Nachtfenster dieser Nacht unterwegs ist."""
    if not seg.get("dt_ab") or not seg.get("dt_an"):
        return 0.0
    start = segment_zeit_zu_utc(nacht, NACHT_VON, seg.get("off_ab"))
    ende = segment_zeit_zu_utc(nacht + timedelta(days=1), NACHT_BIS, seg.get("off_ab"))
    sek = (min(seg["dt_an"], ende) - max(seg["dt_ab"], start)).total_seconds()
    return max(sek, 0.0) / 3600.0


def entfernung_km(a: tuple, b: tuple) -> float:
    """Luftlinie in km (Haversine)."""
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(h)))


def _transport_dazwischen(von: date, bis: date, daten: dict) -> bool:
    for s in daten["segmente"]:
        if von <= s["d_ab"] <= bis or (s["d_an"] and von <= s["d_an"] <= bis):
            return True
    for m in daten["mietwagen"]:
        if m["von"] <= bis and m["bis"] >= von:
            return True
    return any(von <= f <= bis for f in daten["fahrten"])


def auswerten(daten: dict, heute: date, ausgeblendet: dict | None = None) -> dict | None:
    """
    Ergebnis: {"naechte": [ {schluessel, nacht, hinweis, ignoriert, grund} ],
               "transporte": [ {schluessel, titel, text, ignoriert, grund} ]}
    oder None, wenn die Reise nicht geprüft wird (inaktiv, beendet, ohne Datum).
    """
    ausgeblendet = ausgeblendet or {}
    ab, zu = daten.get("abreise"), daten.get("rueckkehr")
    if daten.get("status", "aktiv") != "aktiv" or not ab or not zu or zu < heute:
        return None

    naechte = []
    nacht = max(ab, heute)
    while nacht < zu:
        belegt = any(h["ci"] <= nacht < h["co"] for h in daten["hotels"])
        if not belegt:
            belegt = any(_ueberbrueckt(nacht, s) >= NACHT_MIN_STUNDEN for s in daten["segmente"])
        if not belegt:
            hinweis = ""
            for s in daten["segmente"]:
                m = re.match(r"^(\d{1,2}):(\d{2})", s["an_zeit"])
                if s["d_an"] == nacht + timedelta(days=1) and m and int(m.group(1)) < 6 and s["d_ab"] <= nacht + timedelta(days=1):
                    hinweis = "Ankunft %s um %02d:%s Uhr (nachts) – das Hotel wird schon für diese Nacht gebraucht" % (
                        s["nr"] or "Flug", int(m.group(1)), m.group(2))
                    break
            k = "nacht:%s" % nacht.isoformat()
            naechte.append({"schluessel": k, "nacht": nacht, "hinweis": hinweis,
                            "ignoriert": k in ausgeblendet, "grund": ausgeblendet.get(k, "")})
        nacht += timedelta(days=1)

    transporte = []
    gruppen: dict = {}
    for h in daten["hotels"]:
        gruppen.setdefault(h["reisender"], []).append(h)
    for hs in gruppen.values():
        hs = sorted(hs, key=lambda h: (h["ci"], h["co"]))
        for a, b in zip(hs, hs[1:]):
            if b["ci"] < a["co"]:
                continue                                   # überlappend: kein Ortswechsel dazwischen
            if a["koord"] and b["koord"]:
                km = entfernung_km(a["koord"], b["koord"])
                if km < ABSTAND_KM:
                    continue
                weit = "ca. %.0f km Luftlinie" % km
            elif a["land"] and b["land"] and a["land"] != b["land"]:
                weit = "in verschiedenen Ländern (%s → %s)" % (a["land"], b["land"])
            else:
                continue                                   # Entfernung unbekannt: nicht raten
            if _transport_dazwischen(a["co"], b["ci"], daten):
                continue
            k = "transport:%s:%s" % (a["id"], b["id"])
            transporte.append({
                "schluessel": k, "ignoriert": k in ausgeblendet, "grund": ausgeblendet.get(k, ""),
                "titel": "Kein Transport zwischen %s und %s" % (a["name"], b["name"]),
                "text": "Check-out %s, Check-in %s, %s – aber kein Flug, keine Bahn, kein Mietwagen und keine Fahrt dazwischen." % (
                    a["co"].strftime("%d.%m."), b["ci"].strftime("%d.%m."), weit)})
    return {"naechte": naechte, "transporte": transporte}


def hinweise_laden(rcode: str, heute: date | None = None) -> dict | None:
    d = daten_laden(rcode)
    if not d:
        return None
    return auswerten(d, heute or date.today(), ausgeblendet_laden(rcode))


# ── Anzeige ───────────────────────────────────────────────────────────────────────────────
def _auswahl(gruende: list) -> str:
    return ('<select name="grund" style="width:auto;display:inline-block;font-size:12px;padding:2px 6px;margin-left:6px">%s</select>'
            % "".join("<option>%s</option>" % e(g) for g in gruende))


_KNOPF = ('style="font-size:12px;padding:2px 9px;margin:2px 4px 2px 0;border:1px solid #d97706;border-radius:12px;'
          'background:#fff;color:#92400e;cursor:pointer"')


def banner_html(rcode: str, heute: date | None = None) -> str:
    """Hinweis-Kasten für die Reise-Seite. Leer, wenn nichts zu melden ist. Fehler brechen die Seite nie."""
    try:
        erg = hinweise_laden(rcode, heute)
    except Exception as ex:
        print(f"[Vorbereitung] Prüfung fehlgeschlagen: {ex}")
        return ""
    if not erg:
        return ""
    offen_n = [n for n in erg["naechte"] if not n["ignoriert"]]
    offen_t = [t for t in erg["transporte"] if not t["ignoriert"]]
    weg = [n for n in erg["naechte"] if n["ignoriert"]] + [t for t in erg["transporte"] if t["ignoriert"]]
    if not offen_n and not offen_t and not weg:
        return ""
    action = "/reise/%s/vorbereitung/" % e(rcode.upper())
    teile = []
    if offen_n:
        keys = ",".join(n["schluessel"] for n in offen_n)
        chips = "".join(
            '<button type="submit" name="schluessel" value="%s" title="Nacht vom %s auf %s ausblenden" %s>%s ✕</button>' % (
                e(n["schluessel"]), e(_tag(n["nacht"])), e(_tag(n["nacht"] + timedelta(days=1))), _KNOPF, e(_tag(n["nacht"])))
            for n in offen_n)
        hinw = "".join('<div style="font-size:12px;margin:2px 0 0 22px">• %s: %s</div>' % (e(_tag(n["nacht"])), e(n["hinweis"]))
                       for n in offen_n if n["hinweis"])
        teile.append(
            '<form method="post" action="%sausblenden" style="margin:6px 0 10px 0">'
            '<div>🛏 <b>%d %s nicht gebucht</b></div>'
            '<div style="margin:2px 0 0 22px;font-size:12px">Zum Ausblenden Grund wählen:%s</div>'
            '<div style="margin:4px 0 0 22px">%s'
            '<button type="submit" name="alle" value="%s" %s>Alle ausblenden</button></div>%s</form>' % (
                action, len(offen_n), "Hotelnacht" if len(offen_n) == 1 else "Hotelnächte",
                _auswahl(GRUENDE_NACHT), chips, e(keys), _KNOPF, hinw))
    for t in offen_t:
        teile.append(
            '<form method="post" action="%sausblenden" style="margin:6px 0 10px 0">'
            '<div>🚦 <b>%s</b></div><div style="font-size:12px;margin:2px 0 4px 22px">%s</div>'
            '<div style="margin-left:22px"><span style="font-size:12px">Zum Ausblenden Grund wählen:</span>%s '
            '<button type="submit" name="schluessel" value="%s" %s>Hinweis ausblenden</button></div></form>' % (
                action, e(t["titel"]), e(t["text"]), _auswahl(GRUENDE_TRANSPORT), e(t["schluessel"]), _KNOPF))
    if weg:
        zeilen = ""
        for w in weg:
            beschr = _tag(w["nacht"]) + " (Hotelnacht)" if "nacht" in w else w["titel"]
            zeilen += (
                '<form method="post" action="%seinblenden" style="margin:3px 0">• %s <span style="color:#6b7280">– %s</span> '
                '<button type="submit" name="schluessel" value="%s" %s>Wieder einblenden</button></form>' % (
                    action, e(beschr), e(w["grund"] or "ohne Grund"), e(w["schluessel"]), _KNOPF))
        teile.append('<details style="margin-top:4px;font-size:12px"><summary style="cursor:pointer">%s</summary>%s</details>'
                     % ("1 ausgeblendeter Hinweis" if len(weg) == 1 else "%d ausgeblendete Hinweise" % len(weg), zeilen))
    if not offen_n and not offen_t:
        # Alles ausgeblendet: nur die kleine Zeile mit der Möglichkeit, wieder einzublenden
        return ('<div style="background:#f8fafc;border:1px solid #e2e8f0;color:#475569;padding:8px 14px;border-radius:8px;'
                'margin-bottom:16px;font-size:12px">🧭 Reisevorbereitung: alle Hinweise ausgeblendet. %s</div>' % "".join(teile))
    return ('<div class="alert" style="background:#fffbeb;border:1px solid #fcd34d;color:#78350f;padding:12px 16px;'
            'border-radius:8px;margin-bottom:16px"><div style="font-weight:700;margin-bottom:2px">🧭 Reisevorbereitung '
            '<span style="font-weight:400;font-size:12px">(nur für Organisatoren sichtbar)</span></div>%s</div>' % "".join(teile))
