"""
mod_portal.py – Reisenden-Selbstbedienungsportal (Zeiten & Verpflegung je Person)
"""
from __future__ import annotations
import os, re, secrets
from datetime import date, datetime, timedelta

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_vma_tage import land_fuer_tag, vma_berechnen, vma_tage_generieren
from mod_vma import VMA_SAETZE
import mod_zeitzone as zz
from mod_mail import sende_mail, smtp_sitzung

PORTAL_BASE_URL = os.getenv("PORTAL_BASE_URL", "").rstrip("/")
PORTAL_TAGE_VORHER = int(os.getenv("PORTAL_TAGE_VORHER", "5") or "5")

# Bestandsreisen: Reisen, die VOR diesem Datum angelegt wurden (Einführung der
# automatischen Portal-Mails), bekommen vom Cron KEINE Mail. Der Link lässt sich
# dort weiterhin von Hand auf der Reise-Seite senden.
PORTAL_AUTOMATIK_AB = "2026-10-16"


def token_erzeugen() -> str:
    return secrets.token_urlsafe(24)


def zugang_holen_oder_erstellen(reise_code: str, kuerzel: str) -> str:
    """Gibt den Token für (Reise, Mitarbeiter) zurück, legt ihn bei Bedarf an."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT token FROM reise_zugang WHERE reise_code={P} AND kuerzel={P}",
                (reise_code, kuerzel))
    row = cur.fetchone()
    if row:
        token = row[0] if isinstance(row, tuple) else row["token"]
        cur.close(); db.close()
        return token

    token = token_erzeugen()
    cur.execute(f"INSERT INTO reise_zugang (reise_code, kuerzel, token) VALUES ({P},{P},{P})",
                (reise_code, kuerzel, token))
    db.commit(); cur.close(); db.close()
    return token


def basis_url(ersatz: str = "") -> str:
    """Öffentliche Adresse der App, immer mit http(s):// am Anfang.
    Reihenfolge: PORTAL_BASE_URL, die von Render gesetzte RENDER_EXTERNAL_URL, sonst `ersatz`
    (z. B. die Adresse der aktuellen Anfrage). Leer, wenn nichts davon bekannt ist."""
    for wert in (PORTAL_BASE_URL, os.getenv("RENDER_EXTERNAL_URL", ""), ersatz):
        wert = (wert or "").strip().rstrip("/")
        if wert:
            if not wert.lower().startswith(("http://", "https://")):
                wert = "https://" + wert
            return wert
    return ""


def portal_link(token: str, ersatz: str = "") -> str:
    return f"{basis_url(ersatz)}/portal/{token}"


def zugang_aus_token(token: str) -> dict | None:
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT rz.reise_code, rz.kuerzel, r.titel, r.abreise, r.rueckkehr, m.klarname
                    FROM reise_zugang rz
                    JOIN reisen r ON r.code = rz.reise_code
                    JOIN mitarbeiter m ON m.kuerzel = rz.kuerzel
                    WHERE rz.token={P}""", (token,))
    row = cur.fetchone()
    cur.close(); db.close()
    if not row:
        return None
    g = lambda k, i: row[k] if hasattr(row, "keys") else row[i]
    return {
        "reise_code": g("reise_code", 0), "kuerzel": g("kuerzel", 1),
        "titel": g("titel", 2), "abreise": g("abreise", 3), "rueckkehr": g("rueckkehr", 4),
        "klarname": g("klarname", 5),
    }


def _to_date(v):
    if not v: return None
    if isinstance(v, str): return date.fromisoformat(v[:10])
    return v


def _tag_text(v):
    """Datum als 'YYYY-MM-DD' (Datenbank liefert je nach System Text oder date)."""
    d = _to_date(v)
    return d.isoformat() if d else ""


def _vma_tage_der_reise(cur, reise_code: str) -> dict:
    """Maßgebliche Tageswerte der Reise-Seite (vma_tage): 'YYYY-MM-DD' -> Werte."""
    P = ph()
    cur.execute(f"""SELECT datum, land_code, land_name, vma_satz_voll, vma_satz_halb, ist_halber_satz,
                    fruehstueck, mittagessen, abendessen FROM vma_tage WHERE reise_code={P}""", (reise_code,))
    out = {}
    for r in cur.fetchall():
        out[_tag_text(_w(r, "datum", 0))] = {
            "land_code": _w(r, "land_code", 1), "land_name": _w(r, "land_name", 2),
            "voll": _w(r, "vma_satz_voll", 3), "halb": _w(r, "vma_satz_halb", 4),
            "ist_halb": bool(_w(r, "ist_halber_satz", 5)),
            "frueh": bool(_w(r, "fruehstueck", 6)), "mittag": bool(_w(r, "mittagessen", 7)),
            "abend": bool(_w(r, "abendessen", 8))}
    return out


def tage_sicherstellen(reise_code: str, kuerzel: str) -> None:
    """
    Legt für jeden Tag der Reise (falls noch nicht vorhanden) einen Eintrag in
    reisetage_person an. Land, Sätze und halber Satz kommen aus den VMA-Tagen der Reise-Seite
    (vma_tage) – fehlen die dort noch, werden sie zuerst berechnet. Danach wird alles Bestehende
    mit der Reise-Seite abgeglichen (siehe von_reise_uebernehmen).
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
    r = cur.fetchone()
    if not r:
        cur.close(); db.close()
        return
    ab = _to_date(r[0] if isinstance(r, tuple) else r["abreise"])
    zu = _to_date(r[1] if isinstance(r, tuple) else r["rueckkehr"])
    tage = (zu - ab).days + 1
    eintaegig = (tage == 1)

    vma = _vma_tage_der_reise(cur, reise_code)
    if any((ab + timedelta(days=i)).isoformat() not in vma for i in range(tage)):
        try:                                   # Reise-Seite hat noch nicht alle Tage berechnet
            vma_tage_generieren(reise_code, db)
            vma = _vma_tage_der_reise(cur, reise_code)
        except Exception as e:
            try: db.rollback()
            except Exception: pass
            print(f"[Portal] VMA-Tage für {reise_code} nicht berechnet: {e}")

    for i in range(tage):
        tag = ab + timedelta(days=i)
        cur.execute(f"SELECT id FROM reisetage_person WHERE reise_code={P} AND kuerzel={P} AND datum={P}",
                    (reise_code, kuerzel, tag.isoformat()))
        if cur.fetchone():
            continue
        ist_letzter_tag = (i == tage - 1)
        v = vma.get(tag.isoformat())
        if v and v["voll"] is not None and v["halb"] is not None:
            lcode, lname, voll, halb, ist_halb = v["land_code"], v["land_name"], v["voll"], v["halb"], v["ist_halb"]
            frueh, mittag, abend = v["frueh"], v["mittag"], v["abend"]
        else:                                  # Rückfall: wie der interne VMA-Lauf
            lcode, lname, quelle, override = land_fuer_tag(reise_code, tag, db, ist_letzter_tag, eintaegig)
            if override:
                voll, halb = override["voll"], override["halb"]
            else:
                satz = VMA_SAETZE.get(lcode, VMA_SAETZE["DE"])
                voll, halb = satz["voll"], satz["halb"]
            ist_halb = (i == 0 or i == tage - 1)
            frueh = mittag = abend = False
        brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, mittag, abend)
        cur.execute(f"""INSERT INTO reisetage_person
            (reise_code, kuerzel, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
             ist_halber_satz, fruehstueck, mittagessen, abendessen, vma_netto, bearbeitet)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
            (reise_code, kuerzel, tag.isoformat(), lcode, lname, voll, halb, ist_halb,
             frueh, mittag, abend, netto, False))
    db.commit(); cur.close(); db.close()
    von_reise_uebernehmen(reise_code, mahlzeiten="neue")


def von_reise_uebernehmen(reise_code: str, datum=None, mahlzeiten: str = "neue") -> int:
    """Reise-Seite (vma_tage) -> Portal (reisetage_person), für alle Reisenden der Reise.
    Immer: Land, Sätze, halber Satz und VMA netto (die Reise-Seite ist hier maßgeblich).
    mahlzeiten: "alle"  = Mahlzeiten überall überschreiben (bewusste Änderung auf der Reise-Seite),
                "neue"  = nur Tage, die der Reisende im Portal noch nie gespeichert hat (z. B. Frühstück aus Hotelbeleg),
                "nein"  = Mahlzeiten nicht anfassen.
    datum: nur dieser Tag, sonst alle Tage. Rückgabe: Anzahl geänderter Portal-Tage. Fehler brechen nie ab."""
    n = 0
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        try:
            vma = _vma_tage_der_reise(cur, reise_code)
            nur = _tag_text(datum) if datum else None
            cur.execute(f"""SELECT id, datum, land_code, land_name, vma_satz_voll, vma_satz_halb, ist_halber_satz,
                            fruehstueck, mittagessen, abendessen, vma_netto, bearbeitet
                            FROM reisetage_person WHERE reise_code={P}""", (reise_code,))
            for r in cur.fetchall():
                tag = _tag_text(_w(r, "datum", 1))
                v = vma.get(tag)
                if not v or (nur and tag != nur) or v["voll"] is None or v["halb"] is None:
                    continue
                bearbeitet = bool(_w(r, "bearbeitet", 11))
                if mahlzeiten == "alle" or (mahlzeiten == "neue" and not bearbeitet):
                    frueh, mittag, abend = v["frueh"], v["mittag"], v["abend"]
                else:
                    frueh, mittag, abend = (bool(_w(r, "fruehstueck", 7)), bool(_w(r, "mittagessen", 8)),
                                            bool(_w(r, "abendessen", 9)))
                _b, netto = vma_berechnen(v["voll"], v["halb"], v["ist_halb"], frueh, mittag, abend)
                alt = (_w(r, "land_code", 2), _w(r, "land_name", 3), float(_w(r, "vma_satz_voll", 4) or 0),
                       float(_w(r, "vma_satz_halb", 5) or 0), bool(_w(r, "ist_halber_satz", 6)),
                       bool(_w(r, "fruehstueck", 7)), bool(_w(r, "mittagessen", 8)), bool(_w(r, "abendessen", 9)),
                       round(float(_w(r, "vma_netto", 10) or 0), 2))
                neu = (v["land_code"], v["land_name"], float(v["voll"]), float(v["halb"]), v["ist_halb"],
                       frueh, mittag, abend, round(float(netto), 2))
                if alt == neu:
                    continue
                cur.execute(f"""UPDATE reisetage_person SET land_code={P}, land_name={P}, vma_satz_voll={P},
                                vma_satz_halb={P}, ist_halber_satz={P}, fruehstueck={P}, mittagessen={P},
                                abendessen={P}, vma_netto={P} WHERE id={P}""", neu + (_w(r, "id", 0),))
                n += 1
            db.commit()
        finally:
            cur.close(); db.close()
    except Exception as e:
        print(f"[Portal] Abgleich Reise-Seite → Portal für {reise_code} fehlgeschlagen: {e}")
    return n


def tatsaechliche_zeit_uebernehmen(reise_code: str, datum, uhrzeit) -> int:
    """Reise-Seite: „Reiseantritt/Reiseende (tatsächlich)“ -> Portal-Zeit aller Reisenden.
    Erster Reisetag -> Reisebeginn, letzter Reisetag -> Reiseende (Zone leer = Ortszeit des Tageslandes).
    Leere Uhrzeit ändert im Portal nichts. Fehler brechen nie ab."""
    zeit, ok = zeit_normalisieren(uhrzeit)
    if not zeit or not ok:
        return 0
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        try:
            cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
            r = cur.fetchone()
            if not r:
                return 0
            ab, zu = _tag_text(r[0] if isinstance(r, tuple) else r["abreise"]), _tag_text(r[1] if isinstance(r, tuple) else r["rueckkehr"])
            tag = _tag_text(datum)
            if tag == ab:
                feld = "reise_beginn"
            elif tag == zu:
                feld = "reise_ende"
            else:
                return 0
            cur.execute(f"UPDATE reisetage_person SET {feld}={P}, {feld}_zone=NULL WHERE reise_code={P} AND datum={P}",
                        (zeit, reise_code, tag))
            n = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            db.commit()
            return n
        finally:
            cur.close(); db.close()
    except Exception as e:
        print(f"[Portal] Zeit Reise-Seite → Portal für {reise_code} fehlgeschlagen: {e}")
        return 0


def bearbeitet_nachtragen() -> None:
    """Einmalig nach Einführung der Spalte reisetage_person.bearbeitet: Tage, in denen der Reisende schon
    etwas eingetragen hat, gelten als bearbeitet (werden von der Reise-Seite nicht überschrieben)."""
    try:
        db = get_db(); cur = db.cursor()
        try:
            cur.execute("""SELECT id, fruehstueck, mittagessen, abendessen, reise_beginn, reise_ende,
                           arbeit_beginn, arbeit_ende, notiz FROM reisetage_person WHERE bearbeitet IS NULL""")
            for r in cur.fetchall():
                hat = any(bool(_w(r, f, i)) for i, f in enumerate(
                    ("id", "fruehstueck", "mittagessen", "abendessen", "reise_beginn", "reise_ende",
                     "arbeit_beginn", "arbeit_ende", "notiz")) if i > 0)
                cur.execute(f"UPDATE reisetage_person SET bearbeitet={ph()} WHERE id={ph()}", (hat, _w(r, "id", 0)))
            db.commit()
        finally:
            cur.close(); db.close()
    except Exception as e:
        print(f"[Portal] Altbestand nicht nachgetragen: {e}")


def tage_laden(reise_code: str, kuerzel: str) -> list:
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT id, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
                    ist_halber_satz, fruehstueck, mittagessen, abendessen, vma_netto,
                    reise_beginn, reise_ende, arbeit_beginn, arbeit_ende, notiz,
                    reise_beginn_zone, reise_ende_zone, arbeit_beginn_zone, arbeit_ende_zone
                    FROM reisetage_person WHERE reise_code={P} AND kuerzel={P}
                    AND datum >= (SELECT abreise FROM reisen WHERE code={P})
                    AND datum <= (SELECT rueckkehr FROM reisen WHERE code={P}) ORDER BY datum""",
                (reise_code, kuerzel, reise_code, reise_code))
    rows = cur.fetchall()
    cur.close(); db.close()
    return rows


def tag_speichern(tag_id: int, frueh: bool, mittag: bool, abend: bool,
                   reise_beginn: str, reise_ende: str,
                   arbeit_beginn: str, arbeit_ende: str, notiz: str,
                   zonen: dict | None = None) -> None:
    """zonen (optional): {"reise_beginn": code|None, "reise_ende": ..., "arbeit_beginn": ..., "arbeit_ende": ...}
    Zeitzone je Uhrzeit (None = Ortszeit). Ohne Angabe bleiben die gespeicherten Zonen unverändert."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT vma_satz_voll, vma_satz_halb, ist_halber_satz, reise_code, datum, land_code
                    FROM reisetage_person WHERE id={P}""", (tag_id,))
    r = cur.fetchone()
    if not r:
        cur.close(); db.close()
        return
    voll = r[0] if isinstance(r, tuple) else r["vma_satz_voll"]
    halb = r[1] if isinstance(r, tuple) else r["vma_satz_halb"]
    ist_halb = bool(r[2] if isinstance(r, tuple) else r["ist_halber_satz"])
    reise_code = r[3] if isinstance(r, tuple) else r["reise_code"]
    datum = r[4] if isinstance(r, tuple) else r["datum"]
    land_code = r[5] if isinstance(r, tuple) else r["land_code"]
    # Maßgeblich für Land, Sätze und halben Satz ist die Reise-Seite (vma_tage). Fehlt der Tag dort noch,
    # wird er zuerst berechnet – sonst gingen die Eingaben für die Reise-Seite verloren.
    if reise_code and datum:
        vt = _vma_tage_der_reise(cur, reise_code)
        if _tag_text(datum) not in vt:
            try:
                vma_tage_generieren(reise_code, db)
                vt = _vma_tage_der_reise(cur, reise_code)
            except Exception as e:
                try: db.rollback()
                except Exception: pass
                print(f"[Portal] VMA-Tage für {reise_code} nicht berechnet: {e}")
        v = vt.get(_tag_text(datum))
        if v and v["voll"] is not None and v["halb"] is not None:
            voll, halb, ist_halb, land_code = v["voll"], v["halb"], v["ist_halb"], v["land_code"]
    # Rückfall auf 0, falls der VMA-Satz für diesen Tag aus irgendeinem Grund
    # nicht gesetzt ist (z.B. Randfall bei bereits abgeschlossener Reise) –
    # sonst würde float(None) mit einer ungefangenen Exception abstürzen und
    # dem Reisenden nur einen 502-Fehler zeigen, ohne dass seine Eingabe
    # (Mahlzeiten/Zeiten) überhaupt gespeichert wird.
    voll = voll if voll is not None else 0.0
    halb = halb if halb is not None else 0.0
    brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, mittag, abend)

    zonen_sql, zonen_val = "", ()
    if zonen is not None:
        zonen_sql = ", reise_beginn_zone={0}, reise_ende_zone={0}, arbeit_beginn_zone={0}, arbeit_ende_zone={0}".format(P)
        zonen_val = tuple(zz.zone_normalisieren(zonen.get(f), land_code) for f in
                          ("reise_beginn", "reise_ende", "arbeit_beginn", "arbeit_ende"))
    cur.execute(f"""UPDATE reisetage_person SET
        fruehstueck={P}, mittagessen={P}, abendessen={P}, vma_netto={P},
        reise_beginn={P}, reise_ende={P}, arbeit_beginn={P}, arbeit_ende={P}, notiz={P},
        bearbeitet={P}{zonen_sql}
        WHERE id={P}""",
        (frueh, mittag, abend, netto, reise_beginn or None, reise_ende or None,
         arbeit_beginn or None, arbeit_ende or None, notiz or None, True) + zonen_val + (tag_id,))

    # WICHTIG: Portal-Eingaben (reisetage_person) sind bisher NIE in die
    # eigentliche, für Reise-Ansicht/Abschluss/PDF maßgebliche vma_tage-
    # Tabelle übernommen worden – zwei getrennte, nie verbundene
    # Datenspeicher. Hier direkt mitschreiben, damit der Reisende sein
    # Ergebnis auch tatsächlich in der Reise wiederfindet. quelle wird auf
    # "manuell" gesetzt, damit "VMA neu berechnen" diese bewusste
    # Reisenden-Eingabe nicht versehentlich wieder überschreibt.
    if reise_code and datum:
        if (reise_ende or "").strip():
            tatsaechliche_zeit = (reise_ende or "").strip()
            zeit_zone = (zonen or {}).get("reise_ende")
        else:
            tatsaechliche_zeit = (reise_beginn or "").strip() or None
            zeit_zone = (zonen or {}).get("reise_beginn")
        if tatsaechliche_zeit and zeit_zone:       # in Ortszeit des Tageslandes umrechnen
            try:
                tatsaechliche_zeit = zz.in_ortszeit(_to_date(datum), tatsaechliche_zeit, zeit_zone, land_code) or None
            except Exception:
                pass
        zeit_sql = f", tatsaechliche_uhrzeit={P}" if tatsaechliche_zeit else ""
        zeit_val = (tatsaechliche_zeit,) if tatsaechliche_zeit else ()
        cur.execute(f"""UPDATE vma_tage SET
            fruehstueck={P}, mittagessen={P}, abendessen={P},
            vma_brutto={P}, vma_netto={P}, quelle='manuell'{zeit_sql}
            WHERE reise_code={P} AND datum={P}""",
            (frueh, mittag, abend, brutto, netto) + zeit_val + (reise_code, datum))
    db.commit(); cur.close(); db.close()


# ── Sammel-Speichern (Portal-Startseite) ──────────────────────────────────────
_ZEIT_RE = re.compile(r"^(\d{1,2}):([0-5]\d)(?::[0-5]\d)?$")
_ZEIT_FELDER = (("reise_beginn", "Reisebeginn"), ("reise_ende", "Reiseende"),
                ("arbeit_beginn", "Arbeitsbeginn"), ("arbeit_ende", "Arbeitsende"))
KM_MAX = 99999


def zeit_normalisieren(text):
    """Gibt (wert, gueltig) zurück. Leer -> ("", True); "8:30" -> ("08:30", True);
    nicht lesbar oder außerhalb 00:00–23:59 -> ("", False)."""
    t = (text or "").strip()
    if not t:
        return "", True
    m = _ZEIT_RE.match(t)
    if not m or int(m.group(1)) > 23:
        return "", False
    return "%02d:%s" % (int(m.group(1)), m.group(2)), True


def _w(r, name, idx):
    return r[name] if hasattr(r, "keys") else r[idx]


def portal_aenderungen_speichern(info: dict, form) -> dict:
    """Speichert ALLE Angaben der Portal-Startseite in einem Schritt:
    Mahlzeiten/Zeiten/Notiz je Reisetag, neue Kilometer-Fahrten, zu löschende Fahrten.
    Es werden nur Tage geschrieben, die sich wirklich geändert haben (unveränderte Tage
    bleiben automatisch berechnet). Nur Tage und Fahrten des eigenen Zugangs sind möglich.
    Rückgabe: {"tage": n, "km_neu": n, "km_geloescht": n, "fehler": [code, ...]}
    Fehlercodes: "tYYYYMMDD" (Zeit nicht lesbar), "kN" (Fahrt N unvollständig/ungültig),
    "sYYYYMMDD" (Speichern des Tages fehlgeschlagen)."""
    erg = {"tage": 0, "km_neu": 0, "km_geloescht": 0, "fehler": []}
    reise, kz = info["reise_code"], info["kuerzel"]

    for r in tage_laden(reise, kz):
        tid = _w(r, "id", 0)
        if not form.get("tag_%s" % tid):          # Tag war nicht im abgeschickten Formular
            continue
        datum = _to_date(_w(r, "datum", 1))
        code = datum.strftime("%Y%m%d")
        neu_zeiten, lesbar = [], True
        for feld, _label in _ZEIT_FELDER:
            wert, ok = zeit_normalisieren(form.get("%s_%s" % (feld, tid)))
            lesbar = lesbar and ok
            neu_zeiten.append(wert)
        if not lesbar:
            erg["fehler"].append("t" + code)
            continue
        lc = _w(r, "land_code", 2)
        neu_zonen = [zz.zone_normalisieren(form.get("zone_%s_%s" % (feld, tid)), lc) for feld, _l in _ZEIT_FELDER]
        alt_zonen = [zz.zone_normalisieren(_w(r, feld + "_zone", i), lc)
                     for (feld, _l), i in zip(_ZEIT_FELDER, (16, 17, 18, 19))]
        neu = (bool(form.get("fruehstueck_%s" % tid)), bool(form.get("mittagessen_%s" % tid)),
               bool(form.get("abendessen_%s" % tid)), *neu_zeiten, *neu_zonen,
               (form.get("notiz_%s" % tid) or "").strip())
        alt = (bool(_w(r, "fruehstueck", 7)), bool(_w(r, "mittagessen", 8)), bool(_w(r, "abendessen", 9)),
               *[zeit_normalisieren(_w(r, f, i))[0] for f, i in
                 (("reise_beginn", 11), ("reise_ende", 12), ("arbeit_beginn", 13), ("arbeit_ende", 14))],
               *alt_zonen,
               (_w(r, "notiz", 15) or "").strip())
        if alt == neu:
            continue
        try:
            tag_speichern(tid, *neu[:3], *neu[3:7], neu[11],
                          zonen=dict(zip(("reise_beginn", "reise_ende", "arbeit_beginn", "arbeit_ende"), neu[7:11])))
            erg["tage"] += 1
        except Exception as e:
            print(f"[Portal] Tag {tid} nicht gespeichert: {e}")
            erg["fehler"].append("s" + code)

    # Kilometer: löschen (nur eigene) und neu anlegen
    loeschen = []
    for v in form.getlist("km_loeschen"):
        try:
            loeschen.append(int(v))
        except (TypeError, ValueError):
            pass
    kennz = form.getlist("km_kennzeichen_neu")
    kms = form.getlist("km_km_neu")
    neue = []
    for i in range(max(len(kennz), len(kms))):
        k = (kennz[i] if i < len(kennz) else "").strip()
        m = (kms[i] if i < len(kms) else "").strip()
        if not k and not m:
            continue
        try:
            wert = float(m.replace(",", "."))
        except ValueError:
            wert = 0
        if not k or not (0 < wert <= KM_MAX):
            erg["fehler"].append("k%d" % (i + 1))
            continue
        neue.append((k[:30], wert))
    if loeschen or neue:
        P = ph()
        db = get_db(); cur = db.cursor()
        try:
            for kid in loeschen:
                cur.execute(f"DELETE FROM kilometer_fahrten WHERE id={P} AND reise_code={P} AND kuerzel={P}",
                            (kid, reise, kz))
                if cur.rowcount and cur.rowcount > 0:
                    erg["km_geloescht"] += 1
            for k, wert in neue:
                # datum ist NOT NULL, wird dem Reisenden aber nicht abgefragt -> heutiges Datum
                cur.execute(f"""INSERT INTO kilometer_fahrten (reise_code, kuerzel, datum, kennzeichen, km)
                                VALUES ({P},{P},{P},{P},{P})""", (reise, kz, date.today().isoformat(), k, wert))
                erg["km_neu"] += 1
            db.commit()
        except Exception as e:
            db.rollback()
            print(f"[Portal] Kilometer nicht gespeichert: {e}")
            erg["km_neu"] = erg["km_geloescht"] = 0
            erg["fehler"].append("k0")
        finally:
            cur.close(); db.close()
    return erg


def reisende_der_reise(reise_code: str) -> list:
    """Alle Mitarbeiter, die dieser Reise zugeordnet sind."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT m.kuerzel, m.klarname, m.email
                    FROM reise_mitarbeiter rm JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel
                    WHERE rm.reise_code={P} ORDER BY m.klarname""", (reise_code,))
    rows = cur.fetchall()
    cur.close(); db.close()
    return rows


def zugaenge_der_reise(reise_code: str) -> dict:
    """Kürzel -> {token, email_gesendet_am} für alle bereits erstellten Zugänge dieser Reise."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT kuerzel, token, email_gesendet_am FROM reise_zugang WHERE reise_code={P}",
                (reise_code,))
    rows = cur.fetchall()
    cur.close(); db.close()
    out = {}
    for r in rows:
        k = r[0] if isinstance(r, tuple) else r["kuerzel"]
        out[k] = {
            "token": r[1] if isinstance(r, tuple) else r["token"],
            "email_gesendet_am": r[2] if isinstance(r, tuple) else r["email_gesendet_am"],
        }
    return out


def _portal_mail_text(reise_code: str, klarname: str, reise_titel: str, abreise, rueckkehr, link: str) -> tuple:
    """Betreff und Text der Portal-Mail (Betreff, Text)."""
    betreff = f"Deine Reise {reise_code} – Zeiten & Verpflegung eintragen"
    text = (f"Hallo {klarname},\n\n"
            f"für deine Reise {reise_code} ({reise_titel}, {fmt_date(abreise)} – {fmt_date(rueckkehr)}) "
            f"kannst du hier deine Reise-/Arbeitszeiten und Verpflegung eintragen:\n\n{link}\n\n"
            f"Der Link ist persönlich und nur für dich bestimmt – bitte nicht weitergeben.\n\n"
            f"Viele Grüße")
    return betreff, text


def portal_mail_senden(reise_code: str, kuerzel: str, klarname: str, email: str,
                        reise_titel: str, abreise, rueckkehr, basis: str = "") -> dict:
    if not email:
        return {"fehler": "Keine E-Mail-Adresse hinterlegt"}
    token = zugang_holen_oder_erstellen(reise_code, kuerzel)
    schon_gesendet = bool((zugaenge_der_reise(reise_code).get(kuerzel) or {}).get("email_gesendet_am"))
    link = portal_link(token, basis)
    betreff, text = _portal_mail_text(reise_code, klarname, reise_titel, abreise, rueckkehr, link)
    if schon_gesendet:
        betreff = "Erneut: " + betreff                       # erneut gesendete Mail soll nicht wie ein Doppeltes aussehen
    result = sende_mail(email, betreff, text)
    if result.get("ok"):
        P = ph()
        db = get_db(); cur = db.cursor()
        if is_postgres():
            cur.execute(f"UPDATE reise_zugang SET email_gesendet_am=NOW() WHERE reise_code={P} AND kuerzel={P}",
                        (reise_code, kuerzel))
        else:
            cur.execute(f"UPDATE reise_zugang SET email_gesendet_am=datetime('now') WHERE reise_code={P} AND kuerzel={P}",
                        (reise_code, kuerzel))
        db.commit(); cur.close(); db.close()
    return result


def cron_portal_mails() -> dict:
    """
    Für alle Reisen, deren Abreise in genau PORTAL_TAGE_VORHER Tagen liegt:
    an alle zugeordneten Reisenden ohne bisherigen Mailversand den Portal-Link schicken.
    Bestandsreisen (angelegt vor PORTAL_AUTOMATIK_AB) werden übersprungen.
    Für einen täglichen externen Cron-Aufruf gedacht (z.B. Render Cron Job oder
    ein kostenloser externer Pinger auf /cron/portal-mails?key=...).
    """
    ziel_datum = (date.today() + timedelta(days=PORTAL_TAGE_VORHER)).isoformat()
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT code, titel, abreise, rueckkehr, erstellt FROM reisen WHERE abreise={P}", (ziel_datum,))
    reisen = cur.fetchall()
    cur.close(); db.close()

    gesendet = 0; bestand = 0; fehler = []
    with smtp_sitzung():                                     # alle Portal-Mails über eine Mail-Verbindung
        for r in reisen:
            code = r[0] if isinstance(r, tuple) else r["code"]
            titel = r[1] if isinstance(r, tuple) else r["titel"]
            abreise = r[2] if isinstance(r, tuple) else r["abreise"]
            rueckkehr = r[3] if isinstance(r, tuple) else r["rueckkehr"]
            erstellt = r[4] if isinstance(r, tuple) else r["erstellt"]
            if not erstellt or str(erstellt)[:10] < PORTAL_AUTOMATIK_AB:
                bestand += 1                                 # Bestandsreise: kein automatischer Versand
                continue

            bereits = zugaenge_der_reise(code)
            for ma in reisende_der_reise(code):
                kuerzel = ma[0] if isinstance(ma, tuple) else ma["kuerzel"]
                klarname = ma[1] if isinstance(ma, tuple) else ma["klarname"]
                email = ma[2] if isinstance(ma, tuple) else ma["email"]
                info = bereits.get(kuerzel)
                if info and info.get("email_gesendet_am"):
                    continue
                result = portal_mail_senden(code, kuerzel, klarname, email, titel, abreise, rueckkehr)
                if result.get("ok"):
                    gesendet += 1
                else:
                    fehler.append(f"{code}/{kuerzel}: {result.get('fehler')}")

    return {"ziel_datum": ziel_datum, "gesendet": gesendet, "bestandsreisen_uebersprungen": bestand, "fehler": fehler}
