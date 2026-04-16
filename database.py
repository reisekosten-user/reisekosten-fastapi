import os
import psycopg
from psycopg.rows import dict_row
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS belege (
                id SERIAL PRIMARY KEY,
                belegdatum TEXT,
                art TEXT,
                kosten TEXT,
                waehrung TEXT,
                fingerprint TEXT,
                duplicate_key TEXT,
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
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                data["belegdatum"],
                data["art"],
                data["kosten"],
                data["waehrung"],
                data["fingerprint"],
                data["duplicate_key"],
                datetime.utcnow().isoformat()
            ))
        conn.commit()


def list_belege():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM belege ORDER BY id DESC")
            return cur.fetchall()


def check_duplicate(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM belege WHERE duplicate_key=%s", (key,))
            return cur.fetchone()["count"] > 0


def db_ping():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            return cur.fetchone()