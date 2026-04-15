import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ist nicht gesetzt.")

    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
    )


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """
            )
        conn.commit()


def insert_beleg(data):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO belege (
                    belegdatum,
                    art,
                    kosten,
                    waehrung,
                    fingerprint,
                    duplicate_key,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    data["belegdatum"],
                    data["art"],
                    data["kosten"],
                    data["waehrung"],
                    data["fingerprint"],
                    data["duplicate_key"],
                    datetime.utcnow().isoformat(),
                ),
            )
        conn.commit()


def check_duplicate(duplicate_key):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM belege WHERE duplicate_key = %s",
                (duplicate_key,),
            )
            row = cur.fetchone()
            count = row["cnt"] if row else 0
    return count > 0


def list_belege():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """
            )
            rows = cur.fetchall()

    return rows