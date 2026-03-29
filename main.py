from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import os
import psycopg2
from datetime import datetime

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# 🧠 Reisecode erzeugen
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

# 🏁 Startseite
@app.get("/", response_class=HTMLResponse)
def home():
    return f"""
    <html>
    <head>
        <title>Reisekosten System</title>
        <style>
            body {{
                font-family: Arial;
                background: #f5f8fb;
                margin: 0;
            }}
            .header {{
                background: #003366;
                color: white;
                padding: 20px;
                font-size: 24px;
            }}
            .container {{
                padding: 20px;
            }}
            .card {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 20px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }}
            input, button {{
                padding: 10px;
                margin: 5px;
                border-radius: 5px;
                border: 1px solid #ccc;
            }}
            button {{
                background: #0055aa;
                color: white;
                border: none;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            Herrhammer Reisekosten System
        </div>
        <div class="container">

            <div class="card">
                <h3>Neue Reise anlegen</h3>
                <form action="/create-trip" method="post">
                    <input name="employee" placeholder="Mitarbeiter" required>
                    <input name="destination" placeholder="Ziel (z.B. Delhi)" required>
                    <input name="start" placeholder="Startdatum (10.04.2026)" required>
                    <input name="end" placeholder="Enddatum (14.04.2026)" required>
                    <button type="submit">Reise erstellen</button>
                </form>
            </div>

            <div class="card">
                <a href="/dashboard">→ Zum Dashboard</a>
            </div>

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

    return {"status": "Reise erstellt", "code": code}

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
    <head>
        <style>
            body {{ font-family: Arial; background:#f5f8fb; }}
            .header {{ background:#003366; color:white; padding:20px; }}
            table {{ width:100%; background:white; border-collapse:collapse; }}
            td, th {{ padding:10px; border-bottom:1px solid #ddd; }}
            .container {{ padding:20px; }}
        </style>
    </head>
    <body>
        <div class="header">Dashboard – Reisekosten</div>
        <div class="container">
            <table>
                <tr>
                    <th>Code</th>
                    <th>Mitarbeiter</th>
                    <th>Ziel</th>
                    <th>Start</th>
                    <th>Ende</th>
                </tr>
                {rows_html}
            </table>
        </div>
    </body>
    </html>
    """

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