import os
from datetime import datetime

import psycopg


DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ist nicht gesetzt.")
    return psycopg.connect(DATABASE_URL)


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
                "SELECT COUNT(*) FROM belege WHERE duplicate_key = %s",
                (duplicate_key,),
            )
            row = cur.fetchone()
            count = row[0] if row else 0
    return count > 0


def list_belege():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, belegdatum, art, kosten, waehrung, fingerprint, duplicate_key, created_at
                FROM belege
                ORDER BY id DESC
                """
            )
            rows = cur.fetchall()

    return [
        {
            "id": row[0],
            "belegdatum": row[1],
            "art": row[2],
            "kosten": row[3],
            "waehrung": row[4],
            "fingerprint": row[5],
            "duplicate_key": row[6],
            "created_at": row[7],
        }
        for row in rows
    ]