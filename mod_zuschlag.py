"""
mod_zuschlag.py – Arbeitsstunden und Zuschläge aus den Reise-/Arbeitszeiten der Reisenden.

Grundlage sind die Zeiten, die die Reisenden im Portal je Tag eintragen (Reisezeit von–bis, Arbeitszeit von–bis).
Jede Minute wird einer Tagesart zugeordnet (Werktag, Samstag, Sonntag, Feiertag) und ist Tag oder Nacht.
Daraus ergibt sich der Zuschlag in Prozent. Alle Sätze, der Beginn der Nacht und des Tages, die Kombination
(addieren oder nur der höchste) und das Bundesland für die Feiertage stehen in den Einstellungen.
"""
from __future__ import annotations
import re
from datetime import date, timedelta
import mod_zeitzone as zz

from mod_db import get_db, ph

TABELLE = "app_einstellungen"

STANDARD = {
    "zuschlag_reise": "0",        # % auf Reisezeit an Werktagen (Mo–Fr)
    "zuschlag_arbeit": "25",      # % auf Arbeitszeit an Werktagen (Mo–Fr)
    "zuschlag_nacht": "25",       # % für Stunden in der Nacht
    "zuschlag_samstag": "50",     # % am Samstag (Reise- und Arbeitszeit)
    "zuschlag_sonntag": "100",    # % am Sonntag
    "zuschlag_feiertag": "100",   # % an gesetzlichen Feiertagen
    "nacht_beginn": "22:00",
    "tag_beginn": "06:00",
    "kombination": "addieren",    # "addieren" oder "hoechster"
    "bundesland": "BY",
}
PROZENT_FELDER = ("zuschlag_reise", "zuschlag_arbeit", "zuschlag_nacht",
                  "zuschlag_samstag", "zuschlag_sonntag", "zuschlag_feiertag")
ZEIT_FELDER = ("nacht_beginn", "tag_beginn")
KOMBINATIONEN = {"addieren": "Addieren (Samstag nachts = 50 % + 25 % = 75 %)",
                 "hoechster": "Nur der höchste Zuschlag (Samstag nachts = 50 %)"}
BUNDESLAENDER = {
    "BW": "Baden-Württemberg", "BY": "Bayern", "BE": "Berlin", "BB": "Brandenburg", "HB": "Bremen",
    "HH": "Hamburg", "HE": "Hessen", "MV": "Mecklenburg-Vorpommern", "NI": "Niedersachsen",
    "NW": "Nordrhein-Westfalen", "RP": "Rheinland-Pfalz", "SL": "Saarland", "SN": "Sachsen",
    "ST": "Sachsen-Anhalt", "SH": "Schleswig-Holstein", "TH": "Thüringen",
}
MAX_STUNDEN_OHNE_HINWEIS = 16


# ── Einstellungen ───────────────────────────────────────────────────────────────

def tabelle_sichern(db=None) -> None:
    """Legt die Tabelle für Einstellungen an, falls sie fehlt (läuft in Postgres und SQLite gleich)."""
    eigene = db is None
    try:
        if eigene:
            db = get_db()
        cur = db.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} (schluessel TEXT PRIMARY KEY, wert TEXT)")
        db.commit(); cur.close()
    except Exception as ex:
        print(f"[Zuschlag] Tabelle {TABELLE} nicht angelegt: {ex}")
    finally:
        if eigene and db is not None:
            try: db.close()
            except Exception: pass


def einstellungen_laden(db=None) -> dict:
    """Aktuelle Einstellungen (Text-Werte); fehlt etwas oder die Tabelle, gilt der Standard."""
    cfg = dict(STANDARD)
    eigene = db is None
    try:
        if eigene:
            db = get_db()
        cur = db.cursor()
        cur.execute(f"SELECT schluessel, wert FROM {TABELLE}")
        for r in cur.fetchall():
            k = r[0] if isinstance(r, tuple) else r["schluessel"]
            v = r[1] if isinstance(r, tuple) else r["wert"]
            if k in cfg and v not in (None, ""):
                cfg[k] = str(v)
        cur.close()
    except Exception:
        try: db.rollback()
        except Exception: pass
    finally:
        if eigene and db is not None:
            try: db.close()
            except Exception: pass
    return cfg


def _prozent_parsen(text) -> float | None:
    t = str(text or "").strip().replace("%", "").replace(" ", "").replace(",", ".")
    if not t:
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    return v if 0 <= v <= 500 else None


def _zeit_parsen(text) -> int | None:
    """'HH:MM' -> Minuten seit 00:00, sonst None."""
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", str(text or ""))
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    return h * 60 + mi if h < 24 and mi < 60 else None


def _zahl_text(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else f"{v:.2f}".rstrip("0").rstrip(".")


def einstellungen_pruefen(form) -> tuple:
    """Prüft die Eingaben der Einstellungsseite. Gibt (Werte als Text, Fehlerliste) zurück."""
    werte, fehler = {}, []
    namen = {"zuschlag_reise": "Zuschlag Reisezeit", "zuschlag_arbeit": "Zuschlag Arbeitszeit",
             "zuschlag_nacht": "Zuschlag Nacht", "zuschlag_samstag": "Zuschlag Samstag",
             "zuschlag_sonntag": "Zuschlag Sonntag", "zuschlag_feiertag": "Zuschlag Feiertag",
             "nacht_beginn": "Beginn der Nacht", "tag_beginn": "Beginn des Tages"}
    for k in PROZENT_FELDER:
        v = _prozent_parsen(form.get(k))
        if v is None:
            fehler.append(f"{namen[k]}: bitte eine Zahl von 0 bis 500 eintragen (z. B. 25).")
        else:
            werte[k] = _zahl_text(v)
    for k in ZEIT_FELDER:
        m = _zeit_parsen(form.get(k))
        if m is None:
            fehler.append(f"{namen[k]}: bitte eine Uhrzeit wie 22:00 eintragen.")
        else:
            werte[k] = f"{m // 60:02d}:{m % 60:02d}"
    if len(werte) == len(PROZENT_FELDER) + len(ZEIT_FELDER) and werte["nacht_beginn"] == werte["tag_beginn"]:
        fehler.append("Beginn der Nacht und Beginn des Tages dürfen nicht gleich sein.")
    komb = (form.get("kombination") or "").strip()
    if komb not in KOMBINATIONEN:
        fehler.append("Bitte wählen, ob Zuschläge addiert werden oder nur der höchste gilt.")
    else:
        werte["kombination"] = komb
    land = (form.get("bundesland") or "").strip().upper()
    if land not in BUNDESLAENDER:
        fehler.append("Bitte ein Bundesland für die Feiertage wählen.")
    else:
        werte["bundesland"] = land
    return werte, fehler


def einstellungen_speichern(werte: dict, db=None) -> None:
    """Speichert die geprüften Werte (Einfügen oder Überschreiben je Schlüssel)."""
    eigene = db is None
    if eigene:
        db = get_db()
    try:
        tabelle_sichern(db)
        cur = db.cursor()
        P = ph()
        for k, v in werte.items():
            if k not in STANDARD:
                continue
            cur.execute(f"INSERT INTO {TABELLE} (schluessel, wert) VALUES ({P},{P}) "
                        f"ON CONFLICT (schluessel) DO UPDATE SET wert=EXCLUDED.wert", (k, str(v)))
        db.commit(); cur.close()
    finally:
        if eigene:
            db.close()


def konfiguration(cfg_text: dict) -> dict:
    """Wandelt die Text-Werte in Zahlen um (Prozent als float, Zeiten als Minuten)."""
    c = {}
    for k in PROZENT_FELDER:
        v = _prozent_parsen(cfg_text.get(k))
        c[k] = v if v is not None else float(STANDARD[k])
    for k in ZEIT_FELDER:
        m = _zeit_parsen(cfg_text.get(k))
        c[k] = m if m is not None else _zeit_parsen(STANDARD[k])
    c["kombination"] = cfg_text.get("kombination") if cfg_text.get("kombination") in KOMBINATIONEN else "addieren"
    c["bundesland"] = cfg_text.get("bundesland") if cfg_text.get("bundesland") in BUNDESLAENDER else "BY"
    return c


# ── Feiertage ───────────────────────────────────────────────────────────────────

def ostersonntag(jahr: int) -> date:
    """Ostersonntag (Gaußsche Osterformel, gregorianischer Kalender)."""
    a = jahr % 19
    b, c = divmod(jahr, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    monat, tag = divmod(h + l - 7 * m + 114, 31)
    return date(jahr, monat, tag + 1)


def feiertage(jahr: int, land: str) -> dict:
    """Gesetzliche Feiertage eines Bundeslandes: {Datum: Name}. Regionale Sonderfälle einzelner
    Orte (z. B. Augsburger Friedensfest, Mariä Himmelfahrt in Teilen Bayerns) sind nicht enthalten."""
    ostern = ostersonntag(jahr)
    f = {
        date(jahr, 1, 1): "Neujahr",
        ostern - timedelta(days=2): "Karfreitag",
        ostern + timedelta(days=1): "Ostermontag",
        date(jahr, 5, 1): "Tag der Arbeit",
        ostern + timedelta(days=39): "Christi Himmelfahrt",
        ostern + timedelta(days=50): "Pfingstmontag",
        date(jahr, 10, 3): "Tag der Deutschen Einheit",
        date(jahr, 12, 25): "1. Weihnachtstag",
        date(jahr, 12, 26): "2. Weihnachtstag",
    }
    if land in ("BW", "BY", "ST"):
        f[date(jahr, 1, 6)] = "Heilige Drei Könige"
    if (land == "BE" and jahr >= 2019) or (land == "MV" and jahr >= 2023):
        f[date(jahr, 3, 8)] = "Internationaler Frauentag"
    if land in ("BW", "BY", "HE", "NW", "RP", "SL"):
        f[ostern + timedelta(days=60)] = "Fronleichnam"
    if land == "SL":
        f[date(jahr, 8, 15)] = "Mariä Himmelfahrt"
    if land == "TH" and jahr >= 2019:
        f[date(jahr, 9, 20)] = "Weltkindertag"
    if land in ("BB", "MV", "SN", "ST", "TH") or (land in ("HB", "HH", "NI", "SH") and jahr >= 2018):
        f[date(jahr, 10, 31)] = "Reformationstag"
    if land in ("BW", "BY", "NW", "RP", "SL"):
        f[date(jahr, 11, 1)] = "Allerheiligen"
    if land == "SN":
        d = date(jahr, 11, 22)               # Mittwoch vor dem 23. November
        while d.weekday() != 2:
            d -= timedelta(days=1)
        f[d] = "Buß- und Bettag"
    return f


# ── Berechnung ──────────────────────────────────────────────────────────────────

def tagesart(tag: date, c: dict, _cache: dict | None = None) -> str:
    """'Feiertag', 'Sonntag', 'Samstag' oder 'Werktag'."""
    cache = _cache if _cache is not None else {}
    if tag.year not in cache:
        cache[tag.year] = feiertage(tag.year, c["bundesland"])
    if tag in cache[tag.year]:
        return "Feiertag"
    return {6: "Sonntag", 5: "Samstag"}.get(tag.weekday(), "Werktag")


def ist_nacht(minute_des_tages: int, c: dict) -> bool:
    nb, tb = c["nacht_beginn"], c["tag_beginn"]
    if nb == tb:
        return False
    if nb > tb:                                    # Nacht über Mitternacht, z. B. 22:00–06:00
        return minute_des_tages >= nb or minute_des_tages < tb
    return nb <= minute_des_tages < tb             # Nacht am selben Tag, z. B. 01:00–05:00


def zuschlag_prozent(art: str, tagesart_: str, nacht: bool, c: dict) -> float:
    """Zuschlag in Prozent für eine Minute. art = 'reise' oder 'arbeit'."""
    basis = {"Feiertag": c["zuschlag_feiertag"], "Sonntag": c["zuschlag_sonntag"],
             "Samstag": c["zuschlag_samstag"],
             "Werktag": c["zuschlag_reise"] if art == "reise" else c["zuschlag_arbeit"]}[tagesart_]
    if not nacht:
        return basis
    return basis + c["zuschlag_nacht"] if c["kombination"] == "addieren" else max(basis, c["zuschlag_nacht"])


def _leer() -> dict:
    return {"min": 0, "nacht": 0, "samstag": 0, "sonntag": 0, "feiertag": 0, "zuschlag": 0.0, "je_satz": {}}


def _addieren(summe: dict, teil: dict) -> None:
    for k in ("min", "nacht", "samstag", "sonntag", "feiertag"):
        summe[k] += teil[k]
    summe["zuschlag"] += teil["zuschlag"]
    for satz, mi in teil["je_satz"].items():
        summe["je_satz"][satz] = summe["je_satz"].get(satz, 0) + mi


def intervall_berechnen(tag: date, von, bis, art: str, c: dict, cache: dict | None = None,
                        zone_von=None, zone_bis=None, ortszone=None) -> dict:
    """Minuten und Zuschläge für ein Intervall am Tag 'tag'. Endet das Intervall vor dem Beginn
    (z. B. 22:00–06:00), läuft es über Mitternacht in den Folgetag. Fehlt eine Zeit: leeres Ergebnis.
    Zeitzonen (alle drei optional, ZoneInfo): ortszone = Ortszeit des Tageslandes, zone_von/zone_bis =
    Zone, in der Beginn bzw. Ende angegeben wurden (None = Ortszeit). Die Dauer ist die echte verstrichene
    Zeit; Nacht, Wochentag und Feiertag werden in der Ortszeit bestimmt."""
    erg = _leer()
    v, b = _zeit_parsen(von), _zeit_parsen(bis)
    if v is None or b is None or v == b:
        return erg
    cache = cache if cache is not None else {}
    arten = {}
    fremd = any(z is not None and z is not ortszone for z in (zone_von, zone_bis))
    if fremd:
        ort = ortszone or zz.land_zone("DE")
        t0 = zz.instant(tag, v, zone_von or ort)
        t1 = zz.instant(tag, b, zone_bis or ort)
        if t1 <= t0:
            t1 = zz.instant(tag, b + 1440, zone_bis or ort)
        punkte = []
        for i in range(int((t1 - t0).total_seconds() // 60)):
            lokal = (t0 + timedelta(minutes=i)).astimezone(ort)
            punkte.append((lokal.date(), lokal.hour * 60 + lokal.minute))
    else:
        if b < v:
            b += 1440
        punkte = [(tag + timedelta(days=m // 1440), m % 1440) for m in range(v, b)]
    for d, minute in punkte:
        if d not in arten:
            arten[d] = tagesart(d, c, cache)
        ta = arten[d]
        nacht = ist_nacht(minute, c)
        p = zuschlag_prozent(art, ta, nacht, c)
        erg["min"] += 1
        if nacht:
            erg["nacht"] += 1
        if ta == "Samstag":
            erg["samstag"] += 1
        elif ta == "Sonntag":
            erg["sonntag"] += 1
        elif ta == "Feiertag":
            erg["feiertag"] += 1
        erg["zuschlag"] += p / 100.0
        erg["je_satz"][p] = erg["je_satz"].get(p, 0) + 1
    return erg


def zeiten_auswerten(zeilen: list, cfg_text: dict) -> dict:
    """zeilen: [{kuerzel, datum, reise_beginn, reise_ende, arbeit_beginn, arbeit_ende,
    optional land_code und <feld>_zone je Uhrzeit}, ...].
    Ergebnis: {"tage": [je Zeile: Reise/Arbeit-Ergebnis + Hinweis], "personen": {kuerzel: Summe}, "gesamt": Summe}."""
    c = konfiguration(cfg_text)
    cache: dict = {}
    tage, personen = [], {}
    for z in zeilen:
        d = z["datum"]
        if isinstance(d, str):
            try: d = date.fromisoformat(d[:10])
            except ValueError: continue
        lc = z.get("land_code")
        ort = zz.land_zone(lc)

        def _zone(feld, _z=z, _lc=lc):
            code = zz.zone_normalisieren(_z.get(feld + "_zone"), _lc)
            return zz.zone_aufloesen(code, _lc) if code else None
        reise = intervall_berechnen(d, z.get("reise_beginn"), z.get("reise_ende"), "reise", c, cache,
                                    _zone("reise_beginn"), _zone("reise_ende"), ort)
        arbeit = intervall_berechnen(d, z.get("arbeit_beginn"), z.get("arbeit_ende"), "arbeit", c, cache,
                                     _zone("arbeit_beginn"), _zone("arbeit_ende"), ort)
        hinweise = []
        for name, von, bis, erg in (("Reisezeit", z.get("reise_beginn"), z.get("reise_ende"), reise),
                                    ("Arbeitszeit", z.get("arbeit_beginn"), z.get("arbeit_ende"), arbeit)):
            if bool(von) != bool(bis):
                hinweise.append(f"{name}: Beginn oder Ende fehlt")
            elif von and bis and erg["min"] == 0:
                hinweise.append(f"{name}: Zeit nicht lesbar oder Beginn gleich Ende")
            elif erg["min"] > MAX_STUNDEN_OHNE_HINWEIS * 60:
                hinweise.append(f"{name}: mehr als {MAX_STUNDEN_OHNE_HINWEIS} Stunden, bitte prüfen")
        tage.append({"kuerzel": z.get("kuerzel"), "datum": d, "reise": reise, "arbeit": arbeit,
                     "tagesart": tagesart(d, c, cache), "hinweise": hinweise})
        p = personen.setdefault(z.get("kuerzel"), {"reise": _leer(), "arbeit": _leer()})
        _addieren(p["reise"], reise)
        _addieren(p["arbeit"], arbeit)
    gesamt = {"reise": _leer(), "arbeit": _leer()}
    for p in personen.values():
        _addieren(gesamt["reise"], p["reise"])
        _addieren(gesamt["arbeit"], p["arbeit"])
    return {"tage": tage, "personen": personen, "gesamt": gesamt, "konfiguration": c}


# ── Darstellung ─────────────────────────────────────────────────────────────────

def stunden_text(minuten: int) -> str:
    """Dauer als H:MM, z. B. 8:30."""
    return f"{minuten // 60}:{minuten % 60:02d}"


def zuschlag_stunden_text(zuschlag_min: float) -> str:
    """Zuschlagsstunden als Dezimalzahl, z. B. 2.13."""
    return f"{zuschlag_min / 60:.2f}"


def je_satz_text(je_satz: dict) -> str:
    """'25 %: 8:00 · 50 %: 4:00' (nur Sätze über 0 %)."""
    teile = [f"{_zahl_text(p)} %: {stunden_text(mi)}" for p, mi in sorted(je_satz.items()) if p > 0 and mi > 0]
    return " · ".join(teile) if teile else "–"


def regeln_text(cfg_text: dict) -> str:
    """Einzeiler mit den geltenden Einstellungen (für Abschluss und PDF)."""
    c = konfiguration(cfg_text)
    nb, tb = c["nacht_beginn"], c["tag_beginn"]
    return (f"Zuschläge: Reisezeit {_zahl_text(c['zuschlag_reise'])} %, Arbeitszeit {_zahl_text(c['zuschlag_arbeit'])} % (Mo–Fr), "
            f"Samstag {_zahl_text(c['zuschlag_samstag'])} %, Sonntag {_zahl_text(c['zuschlag_sonntag'])} %, "
            f"Feiertag {_zahl_text(c['zuschlag_feiertag'])} % ({BUNDESLAENDER[c['bundesland']]}), "
            f"Nacht {_zahl_text(c['zuschlag_nacht'])} % von {nb // 60:02d}:{nb % 60:02d} bis {tb // 60:02d}:{tb % 60:02d} Uhr, "
            f"{'addiert' if c['kombination'] == 'addieren' else 'nur der höchste Zuschlag'}. "
            "Uhrzeiten gelten als Ortszeit des Reiselandes, außer es ist eine andere Zeitzone angegeben.")
