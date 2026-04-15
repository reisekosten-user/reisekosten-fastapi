import sqlite3
from datetime import datetime

DB_PATH = "reisekosten.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS belege (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    conn.close()

def insert_beleg(data):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO belege (belegdatum, art, kosten, waehrung, fingerprint, duplicate_key, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
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
    conn.close()

def check_duplicate(duplicate_key):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT COUNT(*) FROM belege WHERE duplicate_key = ?
    """, (duplicate_key,))

    count = cur.fetchone()[0]
    conn.close()

    return count > 0