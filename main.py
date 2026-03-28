from fastapi import FastAPI
import os
import psycopg2
from pydantic import BaseModel
import imaplib
import email
import re

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


class EmailInput(BaseModel):
    text: str


def extract_trip_name(text: str) -> str:
    text_lower = text.lower()

    if "münchen" in text_lower and "delhi" in text_lower:
        return "München-Delhi"
    if "indien" in text_lower:
        return "Indien"
    if "delhi" in text_lower:
        return "Delhi"
    if "berlin" in text_lower and "london" in text_lower:
        return "Berlin-London"

    return "Unbekannt"


def extract_dates(text: str):
    matches = re.findall(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", text)

    if len(matches) >= 2:
        return matches[0], matches[1]
    if len(matches) == 1:
        return matches[0], "unbekannt"

    return "unbekannt", "unbekannt"


@app.get("/")
def read_root():
    return {"status": "Reisekosten System läuft 🚀"}


@app.get("/init")
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id SERIAL PRIMARY KEY,
            name TEXT,
            start_date TEXT,
            end_date TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_mails (
            id TEXT PRIMARY KEY
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    return {"status": "Tabellen erstellt"}


@app.post("/trip")
def create_trip(name: str, start: str, end: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trips (name, start_date, end_date) VALUES (%s, %s, %s)",
        (name, start, end)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "Reise gespeichert"}


@app.get("/trips")
def get_trips():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trips ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"trips": rows}


@app.post("/email")
def create_trip_from_email(payload: EmailInput):
    text = payload.text

    name = extract_trip_name(text)
    start, end = extract_dates(text)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trips (name, start_date, end_date) VALUES (%s, %s, %s)",
        (name, start, end)
    )
    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "Reise aus Mail angelegt",
        "recognized_name": name,
        "recognized_start": start,
        "recognized_end": end,
        "raw_text": payload.text
    }


@app.get("/fetch-mails")
def fetch_mails():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("inbox")

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_mails (
                id TEXT PRIMARY KEY
            )
        """)
        conn.commit()

        status, messages = mail.search(None, "ALL")
        count = 0

        for num in messages[0].split()[-5:]:
            mail_id = num.decode()

            cur.execute("SELECT id FROM processed_mails WHERE id = %s", (mail_id,))
            if cur.fetchone():
                continue

            status, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])

            subject = msg["subject"] or ""
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode(errors="ignore")
                            break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode(errors="ignore")

            text = subject + " " + body

            name = extract_trip_name(text)
            start, end = extract_dates(text)

            cur.execute(
                "INSERT INTO trips (name, start_date, end_date) VALUES (%s, %s, %s)",
                (name, start, end)
            )

            cur.execute(
                "INSERT INTO processed_mails (id) VALUES (%s)",
                (mail_id,)
            )

            conn.commit()
            count += 1

        cur.close()
        conn.close()
        mail.logout()

        return {"status": "Mails verarbeitet", "count": count}

    except Exception as e:
        return {"error": str(e)}