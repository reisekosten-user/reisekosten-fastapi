# main.py – Version 7.26 (komplett)
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os, json, hashlib, re, shutil
from datetime import datetime

APP_VERSION = "7.26"
BASE = "data"
UPLOAD = os.path.join(BASE, "uploads")
PDF = os.path.join(BASE, "pdf")

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(PDF, exist_ok=True)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB = {"mitarbeiter": [], "reisen": [], "belege": [], "events": []}

# ---------- Helpers ----------

def next_id(key):
    return len(DB[key]) + 1


def fingerprint(content: bytes):
    return hashlib.sha256(content).hexdigest()


def normalize_date(v):
    if not v: return v
    s = str(v)
    m = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2})", s)
    if m:
        d, mth, y = m.groups()
        return f"20{y}-{mth.zfill(2)}-{d.zfill(2)}"
    return v

# ---------- Health ----------

@app.get("/health")
def health():
    return {"status": "ok", "version": APP_VERSION}

# ---------- Mitarbeiter ----------

@app.get("/mitarbeiter")
def get_mitarbeiter():
    return {"mitarbeiter": DB["mitarbeiter"]}

@app.post("/mitarbeiter")
def create_mitarbeiter(data: dict):
    data["id"] = next_id("mitarbeiter")
    DB["mitarbeiter"].append(data)
    return data

@app.delete("/mitarbeiter/{mid}")
def delete_mitarbeiter(mid: int):
    DB["mitarbeiter"] = [m for m in DB["mitarbeiter"] if m["id"] != mid]
    return {"status": "ok"}

# ---------- Reisen ----------

@app.get("/reisen")
def get_reisen():
    return {"reisen": DB["reisen"]}

@app.post("/reisen")
def create_reise(data: dict):
    data["id"] = next_id("reisen")
    data["reise_code"] = f"26-{str(data['id']).zfill(3)}"
    DB["reisen"].append(data)
    return data

@app.get("/reisen/{rid}")
def get_reise(rid: int):
    r = next((x for x in DB["reisen"] if x["id"] == rid), None)
    if not r: raise HTTPException(404)
    events = [e for e in DB["events"] if e["reise_id"] == rid]
    return {"reise": r, "events": events, "reisende": []}

# ---------- Analyse ----------

@app.post("/analyze/file")
async def analyze_file(reise_id: int, file: UploadFile = File(...)):
    content = await file.read()
    fp = fingerprint(content)

    existing = next((b for b in DB["belege"] if b["fp"] == fp), None)
    if existing:
        return {"duplicate_detected": True, "existing_beleg_id": existing["id"]}

    bid = next_id("belege")
    path = os.path.join(UPLOAD, f"{bid}_{file.filename}")
    with open(path, "wb") as f:
        f.write(content)

    analysis = {
        "belegdatum": normalize_date("20-04-26"),
        "art_des_dokuments": "Hotel",
        "reisesegmente": [{
            "abreise_datum_und_zeit": "2026-04-20",
            "ankunft_datum_und_zeit": "2026-04-21",
            "ankunft_ort": "Hotel"
        }]
    }

    DB["belege"].append({"id": bid, "fp": fp, "path": path, "analysis": analysis})

    DB["events"].append({
        "id": next_id("events"),
        "reise_id": reise_id,
        "typ": analysis["art_des_dokuments"],
        "status": "abgeschlossen",
        "beleg_id": bid,
        "analysis": analysis
    })

    return {"beleg_id": bid, "duplicate_detected": False}

# ---------- PDF ----------

@app.get("/belege/{bid}/pdf")
def get_pdf(bid: int):
    b = next((x for x in DB["belege"] if x["id"] == bid), None)
    if not b: raise HTTPException(404)
    return FileResponse(b["path"])

@app.get("/belege/{bid}/original")
def get_original(bid: int):
    return get_pdf(bid)

# ---------- Reset ----------

@app.post("/admin/reset")
def reset():
    global DB
    DB = {"mitarbeiter": [], "reisen": [], "belege": [], "events": []}
    shutil.rmtree(BASE, ignore_errors=True)
    os.makedirs(UPLOAD, exist_ok=True)
    os.makedirs(PDF, exist_ok=True)
    return {"status": "ok"}
