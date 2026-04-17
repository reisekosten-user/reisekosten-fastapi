import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ist nicht gesetzt.")

    url = DATABASE_URL
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"

    return psycopg.connect(url, row_factory=dict_row)


def normalize_search_text(text: str) -> str:
    if not text:
        return ""
    t = text.strip().lower()
    t = (
        t.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    t = t.replace("ae", "a").replace("oe", "o").replace("ue", "u")
    return t


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS belege (
                    id SERIAL PRIMARY KEY
                )
            """)
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS belegdatum TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS art TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS kosten TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS waehrung TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS fingerprint TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS duplicate_key TEXT")
            cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS created_at TEXT")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS mitarbeiter (
                    id SERIAL PRIMARY KEY,
                    kuerzel TEXT NOT NULL,
                    vorname TEXT NOT NULL,
                    nachname TEXT NOT NULL,
                    klarname TEXT NOT NULL,
                    geburtsdatum TEXT,
                    email TEXT,
                    aktiv BOOLEAN DEFAULT TRUE,
                    such_normalisiert TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS reisen (
                    id SERIAL PRIMARY KEY,
                    reise_code TEXT NOT NULL UNIQUE,
                    reise_jahr INTEGER NOT NULL,
                    laufende_nummer INTEGER NOT NULL,
                    reise_name TEXT NOT NULL,
                    startdatum TEXT,
                    enddatum TEXT,
                    anzahl_reisende INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS reise_reisende (
                    id SERIAL PRIMARY KEY,
                    reise_id INTEGER NOT NULL,
                    mitarbeiter_id INTEGER NOT NULL,
                    alias_name TEXT,
                    created_at TEXT
                )
            """)
        conn.commit()


def insert_beleg(data):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO belege
                (belegdatum, art, kosten, waehrung, fingerprint, duplicate_key, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data.get("belegdatum"),
                data.get("art"),
                data.get("kosten"),
                data.get("waehrung"),
                data.get("fingerprint"),
                data.get("duplicate_key"),
                now_iso(),
            ))
        conn.commit()


def list_belege():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, belegdatum, art, kosten, waehrung, fingerprint, duplicate_key, created_at
                FROM belege
                ORDER BY id DESC
            """)
            return cur.fetchall()


def check_duplicate(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM belege WHERE duplicate_key = %s", (key,))
            row = cur.fetchone()
            return (row["cnt"] if row else 0) > 0


def db_ping():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 AS ok, current_database() AS dbname, current_user AS dbuser
            """)
            return cur.fetchone()


def create_mitarbeiter(data: dict):
    kuerzel = (data.get("kuerzel") or "").strip()
    vorname = (data.get("vorname") or "").strip()
    nachname = (data.get("nachname") or "").strip()
    klarname = (data.get("klarname") or f"{vorname} {nachname}").strip()
    geburtsdatum = (data.get("geburtsdatum") or "").strip() or None
    email = (data.get("email") or "").strip() or None
    aktiv = bool(data.get("aktiv", True))

    such_basis = " ".join(filter(None, [kuerzel, vorname, nachname, klarname]))
    such_normalisiert = normalize_search_text(such_basis)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mitarbeiter
                (kuerzel, vorname, nachname, klarname, geburtsdatum, email, aktiv, such_normalisiert, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                kuerzel,
                vorname,
                nachname,
                klarname,
                geburtsdatum,
                email,
                aktiv,
                such_normalisiert,
                now_iso(),
                now_iso(),
            ))
            row = cur.fetchone()
        conn.commit()
    return row["id"]


def update_mitarbeiter(mitarbeiter_id: int, data: dict):
    kuerzel = (data.get("kuerzel") or "").strip()
    vorname = (data.get("vorname") or "").strip()
    nachname = (data.get("nachname") or "").strip()
    klarname = (data.get("klarname") or f"{vorname} {nachname}").strip()
    geburtsdatum = (data.get("geburtsdatum") or "").strip() or None
    email = (data.get("email") or "").strip() or None
    aktiv = bool(data.get("aktiv", True))

    such_basis = " ".join(filter(None, [kuerzel, vorname, nachname, klarname]))
    such_normalisiert = normalize_search_text(such_basis)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE mitarbeiter
                SET kuerzel=%s,
                    vorname=%s,
                    nachname=%s,
                    klarname=%s,
                    geburtsdatum=%s,
                    email=%s,
                    aktiv=%s,
                    such_normalisiert=%s,
                    updated_at=%s
                WHERE id=%s
            """, (
                kuerzel,
                vorname,
                nachname,
                klarname,
                geburtsdatum,
                email,
                aktiv,
                such_normalisiert,
                now_iso(),
                mitarbeiter_id,
            ))
        conn.commit()


def search_mitarbeiter(query: str, limit: int = 10):
    q = (query or "").strip()
    qn = normalize_search_text(q)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, kuerzel, vorname, nachname, klarname, geburtsdatum, email, aktiv
                FROM mitarbeiter
                WHERE aktiv = TRUE
                  AND (
                    kuerzel ILIKE %s
                    OR klarname ILIKE %s
                    OR vorname ILIKE %s
                    OR nachname ILIKE %s
                    OR such_normalisiert LIKE %s
                  )
                ORDER BY klarname ASC
                LIMIT %s
            """, (
                f"{q}%",
                f"%{q}%",
                f"%{q}%",
                f"%{q}%",
                f"%{qn}%",
                limit,
            ))
            return cur.fetchall()


def list_mitarbeiter(limit: int = 100):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, kuerzel, vorname, nachname, klarname, geburtsdatum, email, aktiv
                FROM mitarbeiter
                ORDER BY klarname ASC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


def get_next_reise_code(year: int) -> dict:
    short_year = int(str(year)[-2:])
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(MAX(laufende_nummer), 0) AS maxnr
                FROM reisen
                WHERE reise_jahr = %s
            """, (year,))
            row = cur.fetchone()
            next_num = (row["maxnr"] or 0) + 1

    return {
        "jahr": year,
        "laufende_nummer": next_num,
        "reise_code": f"{short_year:02d}-{next_num:03d}",
    }


def create_reise(data: dict):
    year = int(data["reise_jahr"])
    reise_name = (data.get("reise_name") or "").strip()
    startdatum = (data.get("startdatum") or "").strip() or None
    enddatum = (data.get("enddatum") or "").strip() or None
    anzahl_reisende = int(data.get("anzahl_reisende") or 1)
    mitarbeiter_ids = data.get("mitarbeiter_ids") or []

    next_code = get_next_reise_code(year)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reisen
                (reise_code, reise_jahr, laufende_nummer, reise_name, startdatum, enddatum, anzahl_reisende, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, reise_code
            """, (
                next_code["reise_code"],
                year,
                next_code["laufende_nummer"],
                reise_name,
                startdatum,
                enddatum,
                anzahl_reisende,
                now_iso(),
                now_iso(),
            ))
            row = cur.fetchone()
            reise_id = row["id"]

            for idx, mitarbeiter_id in enumerate(mitarbeiter_ids, start=1):
                cur.execute("""
                    INSERT INTO reise_reisende
                    (reise_id, mitarbeiter_id, alias_name, created_at)
                    VALUES (%s, %s, %s, %s)
                """, (
                    reise_id,
                    int(mitarbeiter_id),
                    f"REISENDER_{idx}",
                    now_iso(),
                ))
        conn.commit()

    return {
        "reise_id": reise_id,
        "reise_code": row["reise_code"],
    }


def list_reisen(limit: int = 100):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, reise_code, reise_jahr, laufende_nummer, reise_name, startdatum, enddatum, anzahl_reisende, created_at
                FROM reisen
                ORDER BY reise_jahr DESC, laufende_nummer DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()