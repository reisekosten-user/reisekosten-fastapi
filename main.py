from fastapi import FastAPI
import os
import psycopg2
from pydantic import BaseModel
import imaplib
import email

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

class EmailInput(BaseModel):
    text: str

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
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "Tabelle erstellt"}

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
    cur.execute("SELECT * FROM trips")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"trips": rows}

@app.post("/email")
def create_trip_from_email(payload: EmailInput):
    text = payload.text.lower()

    name = "Unbekannt"
    start = "unbekannt"
    end = "unbekannt"

    if "indien" in text:
        name = "Indien"
    elif "delhi" in text:
        name = "Delhi"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trips (name, start_date, end_date) VALUES (%s, %s, %s)",
        (name, start, end)
    )
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "Reise aus Mail angelegt"}

# 🔥 NEU: echte Mail abholen
@app.get("/fetch-mails")
def fetch_mails():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("inbox")

        status, messages = mail.search(None, "ALL")

        count = 0

        for num in messages[0].split()[-5:]:  # nur letzte 5 Mails
            status, data = mail.fetch(num, "(RFC822)")
            msg = email.message_from_bytes(data[0][1])

            subject = msg["subject"] or ""
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode(errors="ignore")
            else:
                body = msg.get_payload(decode=True).decode(errors="ignore")

            text = (subject + " " + body).lower()

            name = "Unbekannt"
            if "indien" in text:
                name = "Indien"
            elif "delhi" in text:
                name = "Delhi"

            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO trips (name, start_date, end_date) VALUES (%s, %s, %s)",
                (name, "unbekannt", "unbekannt")
            )
            conn.commit()
            cur.close()
            conn.close()

            count += 1

        mail.logout()

        return {"status": "Mails verarbeitet", "count": count}

    except Exception as e:
        return {"error": str(e)}