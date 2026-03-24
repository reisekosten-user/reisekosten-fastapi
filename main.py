from fastapi import FastAPI
import os
import psycopg2

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

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