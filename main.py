from fastapi import FastAPI, UploadFile, File, Body
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import json
import requests
import os
from database import get_conn, init_db

APP_VERSION = "7.7a"

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

# -----------------------------
# INIT
# -----------------------------
@app.on_event("startup")
def startup():
    init_db()


# -----------------------------
# HEALTH
# -----------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION
    }


# -----------------------------
# RESET DB (TEMP)
# -----------------------------
@app.post("/admin/reset-db")
def reset_db():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS event_belege")
                cur.execute("DROP TABLE IF EXISTS events")
                cur.execute("DROP TABLE IF EXISTS reise_reisende")
                cur.execute("DROP TABLE IF EXISTS reisen")
                cur.execute("DROP TABLE IF EXISTS mitarbeiter")
                cur.execute("DROP TABLE IF EXISTS belege")
            conn.commit()

        init_db()

        return {"status": "ok", "message": "DB reset done"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# -----------------------------
# DASHBOARD
# -----------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    with open("templates/dashboard.html", "r", encoding="utf-8") as f:
        return f.read()


# -----------------------------
# MITARBEITER
# -----------------------------
class Mitarbeiter(BaseModel):
    kuerzel: str
    vorname: str
    nachname: str
    klarname: str
    geburtsdatum: Optional[str] = None
    email: Optional[str] = None
    aktiv: bool = True


@app.post("/mitarbeiter")
def create_mitarbeiter(m: Mitarbeiter):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mitarbeiter (kuerzel, vorname, nachname, klarname, geburtsdatum, email, aktiv)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (m.kuerzel, m.vorname, m.nachname, m.klarname, m.geburtsdatum, m.email, m.aktiv))
            mid = cur.fetchone()[0]
        conn.commit()
    return {"status": "ok", "id": mid}


@app.get("/mitarbeiter")
def get_mitarbeiter():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, kuerzel, klarname, geburtsdatum, email FROM mitarbeiter ORDER BY id DESC")
            rows = cur.fetchall()

    data = [
        dict(id=r[0], kuerzel=r[1], klarname=r[2], geburtsdatum=r[3], email=r[4])
        for r in rows
    ]
    return {"count": len(data), "mitarbeiter": data}


@app.get("/mitarbeiter/suche")
def suche(q: str):
    q = f"%{q.lower()}%"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, kuerzel, klarname
                FROM mitarbeiter
                WHERE LOWER(kuerzel) LIKE %s OR LOWER(klarname) LIKE %s
                LIMIT 10
            """, (q, q))
            rows = cur.fetchall()

    return {
        "mitarbeiter": [
            dict(id=r[0], kuerzel=r[1], klarname=r[2]) for r in rows
        ]
    }


# -----------------------------
# REISEN
# -----------------------------
class Reise(BaseModel):
    reise_jahr: int
    reise_name: str
    startdatum: Optional[str]
    enddatum: Optional[str]
    anzahl_reisende: int
    mitarbeiter_ids: list


@app.get("/reisen/next-code")
def next_code(jahr: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reisen WHERE reise_jahr=%s", (jahr,))
            count = cur.fetchone()[0] + 1

    return {"reise_code": f"{str(jahr)[-2:]}-{str(count).zfill(3)}"}


@app.post("/reisen")
def create_reise(r: Reise):
    code = next_code(r.reise_jahr)["reise_code"]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reisen (reise_jahr, reise_code, reise_name, startdatum, enddatum, anzahl_reisende)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (r.reise_jahr, code, r.reise_name, r.startdatum, r.enddatum, r.anzahl_reisende))
            rid = cur.fetchone()[0]

            for mid in r.mitarbeiter_ids:
                cur.execute("INSERT INTO reise_reisende (reise_id, mitarbeiter_id) VALUES (%s,%s)", (rid, mid))

        conn.commit()

    return {"status": "ok", "reise_code": code, "id": rid}


@app.get("/reisen")
def reisen():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, reise_code, reise_name, anzahl_reisende FROM reisen ORDER BY id DESC")
            rows = cur.fetchall()

    return {
        "count": len(rows),
        "reisen": [
            dict(id=r[0], reise_code=r[1], reise_name=r[2], anzahl_reisende=r[3])
            for r in rows
        ]
    }


@app.get("/reisen/overview")
def reisen_overview():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, reise_code, reise_name, anzahl_reisende FROM reisen ORDER BY id DESC")
            rows = cur.fetchall()

    data = []
    for r in rows:
        data.append({
            "id": r[0],
            "reise_code": r[1],
            "reise_name": r[2],
            "anzahl_reisende": r[3],
            "flug": False,
            "hotel": False,
            "taxi": False,
            "status": "pruefen",
            "warnungen": []
        })

    return {"count": len(data), "reisen": data}


# -----------------------------
# ANALYZE TEXT
# -----------------------------
class TextInput(BaseModel):
    text: str
    filename: Optional[str] = "text.txt"


@app.post("/analyze/text")
def analyze_text(data: TextInput):

    result = {
        "belegdatum": "nicht vorhanden",
        "art_des_dokuments": "Unbekannt",
        "buchungsnummer_code": "nicht vorhanden",
        "name_des_reisenden": "nicht vorhanden",
        "wie_viele_reisesegmente": 0,
        "ticketnummer": "nicht vorhanden",
        "kosten_mit_steuern": "nicht vorhanden",
        "waehrung_der_kosten": "nicht vorhanden",
        "reisesegmente": [],
        "confidence_score": 0.5
    }

    return result


# -----------------------------
# ANALYZE FILE
# -----------------------------
@app.post("/analyze/file")
async def analyze_file(file: UploadFile = File(...)):
    content = await file.read()

    return {
        "filename": file.filename,
        "size": len(content),
        "status": "ok"
    }


# -----------------------------
# BELEGE
# -----------------------------
@app.get("/belege")
def belege():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, belegdatum, art, kosten, waehrung, created_at FROM belege ORDER BY id DESC")
            rows = cur.fetchall()

    return {
        "count": len(rows),
        "belege": [
            dict(
                id=r[0],
                belegdatum=r[1],
                art=r[2],
                kosten=r[3],
                waehrung=r[4],
                created_at=r[5]
            )
            for r in rows
        ]
    }