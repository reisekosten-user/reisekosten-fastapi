"""
mod_auth.py – Login pro Mitarbeiter (Passwort-Hashing, Session-Check)
"""
from __future__ import annotations
import hashlib, os, secrets

from mod_db import get_db, ph, is_postgres

# Pfade, die IMMER ohne Login erreichbar sein müssen
OFFENE_PFADE = ("/login", "/logout", "/setup", "/static", "/favicon.ico", "/init",
                 "/portal", "/cron")


def passwort_hashen(passwort: str) -> str:
    """PBKDF2-SHA256 mit zufälligem Salt, gespeichert als 'salt$hash' (beides hex)."""
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", passwort.encode("utf-8"), bytes.fromhex(salt), 200_000)
    return f"{salt}${h.hex()}"


def passwort_pruefen(passwort: str, gespeicherter_hash: str) -> bool:
    if not gespeicherter_hash or "$" not in gespeicherter_hash:
        return False
    salt, h = gespeicherter_hash.split("$", 1)
    try:
        neu = hashlib.pbkdf2_hmac("sha256", passwort.encode("utf-8"), bytes.fromhex(salt), 200_000)
        return secrets.compare_digest(neu.hex(), h)
    except Exception:
        return False


def login_pruefen(kuerzel: str, passwort: str) -> dict | None:
    """Prüft Kürzel+Passwort gegen die Mitarbeiter-Tabelle. Gibt Mitarbeiter-Infos zurück oder None."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT kuerzel, klarname, passwort_hash, aktiv FROM mitarbeiter WHERE kuerzel={P}",
                (kuerzel.strip().upper(),))
    r = cur.fetchone()
    cur.close(); db.close()
    if not r:
        return None
    g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
    aktiv = g("aktiv", 3)
    if aktiv is False or aktiv == 0:
        return None
    hash_ = g("passwort_hash", 2)
    if not hash_ or not passwort_pruefen(passwort, hash_):
        return None
    return {"kuerzel": g("kuerzel", 0), "klarname": g("klarname", 1)}


def hat_bereits_passwoerter() -> bool:
    """True, wenn schon mindestens ein Mitarbeiter ein Passwort gesetzt hat (Bootstrap-Sperre)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM mitarbeiter WHERE passwort_hash IS NOT NULL")
    row = cur.fetchone()
    cur.close(); db.close()
    count = row[0] if isinstance(row, tuple) else row["count"]
    return count > 0


def pfad_ist_offen(pfad: str) -> bool:
    return any(pfad == p or pfad.startswith(p + "/") for p in OFFENE_PFADE)


def ist_organisator(request) -> bool:
    """Prüft, ob der aktuell eingeloggte Benutzer als Organisator angelegt ist."""
    kuerzel = request.session.get("kuerzel")
    if not kuerzel:
        return False
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"SELECT ist_organisator FROM mitarbeiter WHERE kuerzel={P}", (kuerzel,))
    row = cur.fetchone()
    cur.close(); db.close()
    if not row:
        return False
    val = row[0] if isinstance(row, tuple) else row["ist_organisator"]
    return bool(val)
