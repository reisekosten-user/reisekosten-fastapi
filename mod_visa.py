"""
mod_visa.py – Visa-Datenbank je Mitarbeiter (v3.44)

Im Mitarbeiterprofil lassen sich Visa erfassen: Land, gültig von/bis, Notiz und (optional) die Kopie des Visums
als PDF oder Foto. Daraus folgt:
- Dashboard: Hinweis, wenn ein Visum in den nächsten 4 Wochen abläuft oder abgelaufen ist, bis jemand „OK“ klickt.
- Portal und Reisemappe: Tritt ein Mitarbeiter eine Reise in ein Land an, für das er ein gültiges Visum hat,
  liegt die Kopie im Portal zum Anklicken bereit und hängt hinten an seiner Reisemappe.
Dateien liegen im S3-Speicher (wie die Belege und Reise-PDFs), die Liste in der Tabelle visa.
Datumswerte stehen als Text im Format JJJJ-MM-TT in der Datenbank (läuft in Postgres und SQLite gleich).
"""
from __future__ import annotations
import uuid
from datetime import date, datetime, timedelta

from mod_db import get_db, ph, is_postgres
import mod_laender
import mod_reisepdf

TABELLE = "visa"
WARN_TAGE = 28          # so viele Tage vor Ablauf erscheint der Hinweis im Dashboard


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def datum_lesen(v):
    """Text (JJJJ-MM-TT oder TT.MM.JJJJ), date oder datetime -> date; sonst None."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt, n in (("%Y-%m-%d", 10), ("%d.%m.%Y", 10)):
        try:
            return datetime.strptime(s[:n], fmt).date()
        except Exception:
            pass
    return None


def datum_text(d) -> str:
    return d.strftime("%d.%m.%Y") if d else ""


# ── Tabelle ───────────────────────────────────────────────────────

def tabelle_sichern() -> None:
    """Legt die Tabelle an, falls sie fehlt (läuft in Postgres und SQLite gleich)."""
    try:
        db = get_db(); cur = db.cursor()
        key = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} ("
                    f"id {key}, kuerzel TEXT NOT NULL, land_code TEXT NOT NULL, gueltig_von TEXT, gueltig_bis TEXT, "
                    f"notiz TEXT, dateiname TEXT, s3_key TEXT, seiten INTEGER, groesse INTEGER, ok_am TEXT, erstellt TEXT)")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[Visa] Tabelle {TABELLE} nicht angelegt: {ex}")


_SPALTEN = ("id", "kuerzel", "land_code", "gueltig_von", "gueltig_bis", "notiz", "dateiname", "s3_key", "seiten",
            "groesse", "ok_am", "erstellt")
_SELECT = ("SELECT " + ", ".join(f"v.{c}" for c in _SPALTEN) + ", m.klarname, m.aktiv FROM " + TABELLE +
           " v LEFT JOIN mitarbeiter m ON m.kuerzel = v.kuerzel")


def _zeile(r) -> dict:
    d = {k: _g(r, k, i) for i, k in enumerate(_SPALTEN)}
    d["klarname"] = _g(r, "klarname", len(_SPALTEN)) or d["kuerzel"]
    akt = _g(r, "aktiv", len(_SPALTEN) + 1)
    d["aktiv"] = True if akt is None else bool(akt)
    d["land_code"] = (d["land_code"] or "").upper()
    d["land_name"] = mod_laender.land_name(d["land_code"]) or d["land_code"]
    d["von"] = datum_lesen(d["gueltig_von"])
    d["bis"] = datum_lesen(d["gueltig_bis"])
    d["hat_datei"] = bool(d["s3_key"])
    d["notiz"] = d["notiz"] or ""
    return d


def _sortiert(rows: list) -> list:
    return sorted(rows, key=lambda v: (v["land_name"].lower(), -(v["bis"].toordinal() if v["bis"] else 0), v["id"]))


def liste(kuerzel: str | None = None) -> list:
    """Alle Visa (eines Mitarbeiters oder aller), sortiert nach Land, neueste Gültigkeit zuerst."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        if kuerzel:
            cur.execute(f"{_SELECT} WHERE v.kuerzel={P} ORDER BY v.id", (kuerzel.strip().upper(),))
        else:
            cur.execute(f"{_SELECT} ORDER BY v.id")
        rows = [_zeile(r) for r in cur.fetchall()]
    except Exception:
        try: db.rollback()
        except Exception: pass
        rows = []
    finally:
        cur.close(); db.close()
    return _sortiert(rows)


def holen(vid: int) -> dict | None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"{_SELECT} WHERE v.id={P}", (vid,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    return _zeile(r) if r else None


# ── Hinzufügen / Ändern / Löschen ─────────────────────────────────

def felder_pruefen(land: str, von, bis) -> tuple:
    """Prüft die Eingaben. Rückgabe: (land_code, von_date, bis_date) oder ValueError mit klarem Text."""
    code = (land or "").strip().upper()
    if not code or code not in mod_laender.LAND_NAMEN:
        raise ValueError("Bitte ein Land aus der Liste wählen.")
    d_von, d_bis = datum_lesen(von), datum_lesen(bis)
    if not d_von:
        raise ValueError("Bitte das Datum „gültig von“ eingeben.")
    if not d_bis:
        raise ValueError("Bitte das Datum „gültig bis“ eingeben.")
    if d_bis < d_von:
        raise ValueError("„Gültig bis“ liegt vor „gültig von“.")
    return code, d_von, d_bis


def _datei_speichern(daten: bytes, dateiname: str, content_type: str = "") -> tuple:
    """Macht aus dem Upload ein PDF und legt es im S3-Speicher ab. Rückgabe: (s3_key, seiten, groesse)."""
    pdf, seiten = mod_reisepdf.pruefen(daten, dateiname, content_type)
    from mod_beleg import s3_upload
    key = f"visa/{uuid.uuid4().hex}.pdf"
    s3_upload(key, pdf)
    return key, seiten, len(pdf)


def hinzufuegen(kuerzel: str, land: str, von, bis, notiz: str = "", daten: bytes | None = None,
                dateiname: str = "", content_type: str = "") -> int:
    """Legt ein Visum an (Datei ist optional). Gibt die ID zurück; ungültige Eingaben -> ValueError."""
    code, d_von, d_bis = felder_pruefen(land, von, bis)
    kz = (kuerzel or "").strip().upper()
    if not kz:
        raise ValueError("Mitarbeiter fehlt.")
    key = seiten = groesse = None
    if daten:
        key, seiten, groesse = _datei_speichern(daten, dateiname, content_type)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        sql = (f"INSERT INTO {TABELLE} (kuerzel, land_code, gueltig_von, gueltig_bis, notiz, dateiname, s3_key, seiten, "
               f"groesse, ok_am, erstellt) VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})")
        vals = (kz, code, d_von.isoformat(), d_bis.isoformat(), (notiz or "").strip()[:200] or None,
                (dateiname or "")[:200] if key else None, key, seiten, groesse, None,
                datetime.now().strftime("%Y-%m-%d %H:%M"))
        if is_postgres():
            cur.execute(sql + " RETURNING id", vals)
            neu = cur.fetchone()
            vid = neu[0] if not hasattr(neu, "keys") else neu["id"]
        else:
            cur.execute(sql, vals)
            vid = cur.lastrowid
        db.commit()
    finally:
        cur.close(); db.close()
    return vid


def aendern(vid: int, land: str, von, bis, notiz: str = "", daten: bytes | None = None, dateiname: str = "",
            content_type: str = "", datei_entfernen: bool = False) -> None:
    """Ändert ein Visum. Neue Datei ersetzt die alte (die alte bleibt als Sicherung im Speicher liegen).
    Ändert sich „gültig bis“, ist ein früheres „OK“ im Dashboard wieder offen."""
    alt = holen(vid)
    if not alt:
        raise ValueError("Visum nicht gefunden.")
    code, d_von, d_bis = felder_pruefen(land, von, bis)
    sets = ["land_code", "gueltig_von", "gueltig_bis", "notiz"]
    vals = [code, d_von.isoformat(), d_bis.isoformat(), (notiz or "").strip()[:200] or None]
    if daten:
        key, seiten, groesse = _datei_speichern(daten, dateiname, content_type)
        sets += ["dateiname", "s3_key", "seiten", "groesse"]
        vals += [(dateiname or "")[:200], key, seiten, groesse]
    elif datei_entfernen:
        sets += ["dateiname", "s3_key", "seiten", "groesse"]
        vals += [None, None, None, None]
    if alt["bis"] != d_bis:
        sets.append("ok_am")
        vals.append(None)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET " + ", ".join(f"{c}={P}" for c in sets) + f" WHERE id={P}", tuple(vals) + (vid,))
        db.commit()
    finally:
        cur.close(); db.close()


def loeschen(vid: int) -> None:
    """Entfernt den Eintrag. Die Datei im Speicher bleibt als Sicherung liegen."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {TABELLE} WHERE id={P}", (vid,))
        db.commit()
    finally:
        cur.close(); db.close()


def ok_setzen(vid: int) -> None:
    """Der Ablauf-Hinweis im Dashboard ist gelesen („OK“ geklickt)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET ok_am={P} WHERE id={P}", (datetime.now().strftime("%Y-%m-%d %H:%M"), vid))
        db.commit()
    finally:
        cur.close(); db.close()


def pdf_bytes(vid: int) -> bytes | None:
    v = holen(vid)
    if not v or not v["s3_key"]:
        return None
    from mod_beleg import s3_download
    return s3_download(v["s3_key"])


# ── Status und Dashboard-Hinweis ──────────────────────────────────

def status(v: dict, heute: date | None = None) -> tuple:
    """(Status, Tage): abgelaufen (Tage seit Ablauf) | bald (Tage bis Ablauf) | zukunft (Tage bis Beginn) | gueltig."""
    heute = heute or date.today()
    bis, von = v.get("bis"), v.get("von")
    if bis and bis < heute:
        return "abgelaufen", (heute - bis).days
    if von and von > heute:
        return "zukunft", (von - heute).days
    if bis and (bis - heute).days <= WARN_TAGE:
        return "bald", (bis - heute).days
    return "gueltig", ((bis - heute).days if bis else None)


def ablauf_hinweise(kuerzel: str | None = None, heute: date | None = None) -> list:
    """
    Visa, die in den nächsten 4 Wochen ablaufen oder schon abgelaufen sind und noch nicht mit „OK“ bestätigt wurden.
    Nicht gemeldet wird ein Visum, wenn derselbe Mitarbeiter für dasselbe Land ein anderes hat, das länger und
    noch gültig ist (Verlängerung ist schon erfasst). Inaktive Mitarbeiter bleiben außen vor.
    Rückgabe: Liste von Visa (wie liste()), ergänzt um 'status' und 'tage', nach Ablaufdatum sortiert.
    """
    heute = heute or date.today()
    grenze = heute + timedelta(days=WARN_TAGE)
    alle = liste(kuerzel)
    erg = []
    for v in alle:
        if v["ok_am"] or not v["bis"] or v["bis"] > grenze or not v["aktiv"]:
            continue
        ersetzt = any(w["id"] != v["id"] and w["kuerzel"] == v["kuerzel"] and w["land_code"] == v["land_code"]
                      and w["bis"] and w["bis"] > v["bis"] and w["bis"] >= heute for w in alle)
        if ersetzt:
            continue
        st, tage = status(v, heute)
        erg.append({**v, "status": st, "tage": tage})
    return sorted(erg, key=lambda v: (v["bis"], v["id"]))


# ── Reise: passende Visa ──────────────────────────────────────────

def reise_laender(reise_code: str) -> set:
    """Ländercodes einer Reise (aus den VMA-Tagen und den Land-Einträgen der Reise)."""
    P = ph()
    codes = set()
    db = get_db(); cur = db.cursor()
    try:
        for sql in (f"SELECT DISTINCT land_code FROM vma_tage WHERE reise_code={P}",
                    f"SELECT DISTINCT land_code FROM reise_laender WHERE reise_code={P}"):
            try:
                cur.execute(sql, (reise_code,))
                for r in cur.fetchall():
                    c = str(_g(r, "land_code", 0) or "").strip().upper()
                    if c:
                        codes.add(c)
            except Exception:
                try: db.rollback()
                except Exception: pass
    finally:
        cur.close(); db.close()
    return codes


def _reisezeitraum(reise_code: str) -> tuple:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    return (datum_lesen(_g(r, "abreise", 0)), datum_lesen(_g(r, "rueckkehr", 1))) if r else (None, None)


def fuer_reise(reise_code: str, kuerzel: str) -> list:
    """
    Visa dieses Mitarbeiters, die zu der Reise passen: Das Land kommt auf der Reise vor und das Visum ist
    mindestens an einem Tag der Reise gültig. Jedes Visum trägt 'ganze_reise' (True = deckt die ganze Reise ab).
    """
    kz = (kuerzel or "").strip().upper()
    if not kz:
        return []
    ab, zu = _reisezeitraum(reise_code)
    if not ab or not zu:
        return []
    laender = reise_laender(reise_code)
    if not laender:
        return []
    erg = []
    for v in liste(kz):
        if v["land_code"] not in laender or not v["von"] or not v["bis"]:
            continue
        if v["von"] > zu or v["bis"] < ab:
            continue
        erg.append({**v, "ganze_reise": v["von"] <= ab and v["bis"] >= zu})
    return erg


def titel_fuer_mappe(v: dict, klarname: str = "") -> str:
    wer = f" ({klarname})" if klarname else ""
    return f"Visum {v['land_name']}{wer} – gültig {datum_text(v['von'])} bis {datum_text(v['bis'])}"


def anlagen_fuer(reise_code: str, kuerzel: str, klarname: str = "") -> list:
    """Visa-Kopien als Anlagen der Reisemappe: [{titel, bytes, quelle, fehler}] (fehlende Datei wird gemeldet)."""
    erg = []
    for v in fuer_reise(reise_code, kuerzel):
        titel = titel_fuer_mappe(v, klarname)
        if not v["hat_datei"]:
            erg.append({"titel": titel, "bytes": None, "quelle": "Visum", "fehler": "Kopie liegt noch nicht vor"})
            continue
        try:
            daten = pdf_bytes(v["id"])
        except Exception as ex:
            erg.append({"titel": titel, "bytes": None, "quelle": "Visum", "fehler": str(ex)[:80]})
            continue
        erg.append({"titel": titel, "bytes": daten, "quelle": "Visum", "fehler": None})
    return erg
