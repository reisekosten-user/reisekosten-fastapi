import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ist nicht gesetzt.")

    url = DATABASE_URL

    if "sslmode=" not in url:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}sslmode=require"

    return psycopg.connect(url, row_factory=dict_row)


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
                datetime.utcnow().isoformat()
            ))
        conn.commit()


def list_belege():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    belegdatum,
                    art,
                    kosten,
                    waehrung,
                    fingerprint,
                    duplicate_key,
                    created_at
                FROM belege
                ORDER BY id DESC
            """)
            return cur.fetchall()


def check_duplicate(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM belege WHERE duplicate_key = %s",
                (key,)
            )
            row = cur.fetchone()
            return (row["cnt"] if row else 0) > 0


def db_ping():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    1 AS ok,
                    current_database() AS dbname,
                    current_user AS dbuser
            """)
            return cur.fetchone()