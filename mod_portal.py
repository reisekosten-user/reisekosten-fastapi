"""
mod_portal.py – Reisenden-Selbstbedienungsportal (Zeiten & Verpflegung je Person)
"""
from __future__ import annotations
import os, secrets
from datetime import date, datetime, timedelta

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_vma_tage import land_fuer_tag, vma_berechnen
from mod_vma import VMA_SAETZE
from mod_mail import sende_mail

PORTAL_BASE_URL = os.getenv("PORTAL_BASE_URL", "").rstrip("/")
PORTAL_TAGE_VORHER = int(os.getenv("PORTAL_TAGE_VORHER", "5") or "5")


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


def portal_link(token: str) -> str:
    base = PORTAL_BASE_URL or ""
    return f"{base}/portal/{token}"


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


def tage_sicherstellen(reise_code: str, kuerzel: str) -> None:
    """
    Legt für jeden Tag der Reise (falls noch nicht vorhanden) einen Eintrag in
    reisetage_person an – Land/VMA-Satz automatisch ermittelt wie beim internen
    VMA-Lauf, Mahlzeiten/Zeiten zunächst leer, vom Reisenden selbst zu befüllen.
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

    for i in range(tage):
        tag = ab + timedelta(days=i)
        cur.execute(f"SELECT id FROM reisetage_person WHERE reise_code={P} AND kuerzel={P} AND datum={P}",
                    (reise_code, kuerzel, tag.isoformat()))
        if cur.fetchone():
            continue
        lcode, lname, quelle, override = land_fuer_tag(reise_code, tag, db)
        if override:
            voll, halb = override["voll"], override["halb"]
        else:
            satz = VMA_SAETZE.get(lcode, VMA_SAETZE["DE"])
            voll, halb = satz["voll"], satz["halb"]
        ist_halb = (i == 0 or i == tage - 1)
        brutto, netto = vma_berechnen(voll, halb, ist_halb, False, False, False)
        cur.execute(f"""INSERT INTO reisetage_person
            (reise_code, kuerzel, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
             ist_halber_satz, vma_netto)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P})""",
            (reise_code, kuerzel, tag.isoformat(), lcode, lname, voll, halb, ist_halb, netto))
    db.commit(); cur.close(); db.close()


def tage_laden(reise_code: str, kuerzel: str) -> list:
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT id, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
                    ist_halber_satz, fruehstueck, mittagessen, abendessen, vma_netto,
                    reise_beginn, reise_ende, arbeit_beginn, arbeit_ende, notiz
                    FROM reisetage_person WHERE reise_code={P} AND kuerzel={P} ORDER BY datum""",
                (reise_code, kuerzel))
    rows = cur.fetchall()
    cur.close(); db.close()
    return rows


def tag_speichern(tag_id: int, frueh: bool, mittag: bool, abend: bool,
                   reise_beginn: str, reise_ende: str,
                   arbeit_beginn: str, arbeit_ende: str, notiz: str) -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT vma_satz_voll, vma_satz_halb, ist_halber_satz FROM reisetage_person WHERE id={P}",
                (tag_id,))
    r = cur.fetchone()
    if not r:
        cur.close(); db.close()
        return
    voll = r[0] if isinstance(r, tuple) else r["vma_satz_voll"]
    halb = r[1] if isinstance(r, tuple) else r["vma_satz_halb"]
    ist_halb = bool(r[2] if isinstance(r, tuple) else r["ist_halber_satz"])
    brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, mittag, abend)

    cur.execute(f"""UPDATE reisetage_person SET
        fruehstueck={P}, mittagessen={P}, abendessen={P}, vma_netto={P},
        reise_beginn={P}, reise_ende={P}, arbeit_beginn={P}, arbeit_ende={P}, notiz={P}
        WHERE id={P}""",
        (frueh, mittag, abend, netto, reise_beginn or None, reise_ende or None,
         arbeit_beginn or None, arbeit_ende or None, notiz or None, tag_id))
    db.commit(); cur.close(); db.close()


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


def portal_mail_senden(reise_code: str, kuerzel: str, klarname: str, email: str,
                        reise_titel: str, abreise, rueckkehr) -> dict:
    if not email:
        return {"fehler": "Keine E-Mail-Adresse hinterlegt"}
    token = zugang_holen_oder_erstellen(reise_code, kuerzel)
    link = portal_link(token)
    betreff = f"Deine Reise {reise_code} – Zeiten & Verpflegung eintragen"
    text = (f"Hallo {klarname},\n\n"
            f"für deine Reise {reise_code} ({reise_titel}, {fmt_date(abreise)} – {fmt_date(rueckkehr)}) "
            f"kannst du hier deine Reise-/Arbeitszeiten und Verpflegung eintragen:\n\n{link}\n\n"
            f"Der Link ist persönlich und nur für dich bestimmt – bitte nicht weitergeben.\n\n"
            f"Viele Grüße")
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
    Für einen täglichen externen Cron-Aufruf gedacht (z.B. Render Cron Job oder
    ein kostenloser externer Pinger auf /cron/portal-mails?key=...).
    """
    ziel_datum = (date.today() + timedelta(days=PORTAL_TAGE_VORHER)).isoformat()
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT code, titel, abreise, rueckkehr FROM reisen WHERE abreise={P}", (ziel_datum,))
    reisen = cur.fetchall()
    cur.close(); db.close()

    gesendet = 0; fehler = []
    for r in reisen:
        code = r[0] if isinstance(r, tuple) else r["code"]
        titel = r[1] if isinstance(r, tuple) else r["titel"]
        abreise = r[2] if isinstance(r, tuple) else r["abreise"]
        rueckkehr = r[3] if isinstance(r, tuple) else r["rueckkehr"]

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

    return {"ziel_datum": ziel_datum, "gesendet": gesendet, "fehler": fehler}
