from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import os
import psycopg2
from datetime import datetime
import re

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# 🧠 Code generieren
def generate_trip_code():
    year = datetime.now().strftime("%y")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT trip_code FROM trips WHERE trip_code LIKE %s ORDER BY id DESC LIMIT 1", (f"{year}-%",))
    last = cur.fetchone()

    if last:
        last_number = int(last[0].split("-")[1])
        new_number = last_number + 1
    else:
        new_number = 1

    code = f"{year}-{str(new_number).zfill(3)}"

    cur.close()
    conn.close()

    return code

# 🔍 Code aus Text extrahieren
def extract_trip_code(text):
    match = re.search(r"\b\d{2}-\d{3}\b", text)
    if match:
        return match.group(0)
    return None

# 🏁 Startseite
@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
    <head>
        <style>
            body { font-family: Arial; background:#f5f8fb; margin:0; }
            .header { background:#003366; color:white; padding:20px; font-size:24px; }
            .container { padding:20px; }
            .card { background:white; padding:20px; border-radius:10px; margin-bottom:20px; }
            input, button { padding:10px; margin:5px; }
            button { background:#0055aa; color:white; border:none; }
        </style>
    </head>
    <body>
        <div class="header">Herrhammer Reisekosten</div>
        <div class="container">
            <div class="card">
                <h3>Neue Reise</h3>
                <form action="/create-trip" method="post">
                    <input name="employee" placeholder="Mitarbeiter">
                    <input name="destination" placeholder="Ziel">
                    <input name="start" placeholder="Startdatum">
                    <input name="end" placeholder="Enddatum">
                    <button>Erstellen</button>
                </form>
            </div>
            <a href="/dashboard">Dashboard</a>
        </div>
    </body>
    </html>
    """

# ➕ Reise erstellen
@app.post("/create-trip")
def create_trip(employee: str = Form(...), destination: str = Form(...), start: str = Form(...), end: str = Form(...)):
    code = generate_trip_code()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO trips (trip_code, employee, name, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
    """, (code, employee, destination, start, end))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "Reise erstellt",
        "code": code,
        "hinweis": f"Bitte immer [{code}] im Betreff verwenden!"
    }

# 📊 Dashboard
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT trip_code, employee, name, start_date, end_date FROM trips ORDER BY id DESC")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
            <td><b>{r[0]}</b></td>
            <td>{r[1]}</td>
            <td>{r[2]}</td>
            <td>{r[3]}</td>
            <td>{r[4]}</td>
        </tr>
        """

    return f"""
    <html>
    <body style="font-family:Arial;background:#f5f8fb;">
        <div style="background:#003366;color:white;padding:20px;">Dashboard</div>
        <div style="padding:20px;">
            <table style="width:100%;background:white;">
                <tr>
                    <th>Code</th><th>Mitarbeiter</th><th>Ziel</th><th>Start</th><th>Ende</th>
                </tr>
                {rows_html}
            </table>
        </div>
    </body>
    </html>
    """

# ✉️ Mail-Verarbeitung mit Code
@app.post("/email")
def email_input(text: str):
    code = extract_trip_code(text)

    if not code:
        return {"error": "Kein Reisecode gefunden!"}

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM trips WHERE trip_code = %s", (code,))
    trip = cur.fetchone()

    if not trip:
        return {"error": "Reisecode existiert nicht!"}

    # hier später Belege / Infos speichern
    cur.close()
    conn.close()

    return {
        "status": "Mail korrekt zugeordnet",
        "trip_code": code
    }

# 🔧 DB erweitern
@app.get("/init")
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id SERIAL PRIMARY KEY,
            trip_code TEXT,
            employee TEXT,
            name TEXT,
            start_date TEXT,
            end_date TEXT
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "DB bereit"}