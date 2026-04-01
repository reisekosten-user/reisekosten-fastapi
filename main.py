"""
Herrhammer Reisekosten – Version 6.1
=====================================
KI-Belegerkennung via Mistral (EU-gehostet, DSGVO-konform):
  Mistral OCR 3  (mistral-ocr-2512)  - PDFs und Bilder lesen
  Mistral Small  (mistral-small-latest) - Extraktion und Zuordnung

DSGVO-Hinweis:
  Mistral La Plateforme EU-gehostet (Paris), AVV abschliessbar
  Keine Nutzung der Daten fuer Modelltraining (Zero Data Retention)
  Belege nur zur Analyse uebertragen, danach beim Anbieter geloescht
  Alle Rohdaten verbleiben im Hetzner-Bucket (Deutschland)

Neue Umgebungsvariable:
  MISTRAL_API_KEY  - API Key von console.mistral.ai

Kosten Mistral (ca.):
  OCR: 1 USD / 1.000 Seiten (Batch)
  Small: 0.10 USD / 1M Tokens
  500 Belege/Monat ~ unter 1 USD
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import os, re, base64, json, httpx, imaplib, email
from email.header import decode_header
from io import BytesIO
from datetime import date, datetime
from typing import Optional
import psycopg2
import boto3

APP_VERSION = "6.1"

app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Umgebungsvariablen
DATABASE_URL          = os.getenv("DATABASE_URL")
IMAP_HOST             = os.getenv("IMAP_HOST")
IMAP_USER             = os.getenv("IMAP_USER")
IMAP_PASS             = os.getenv("IMAP_PASS")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT")
S3_BUCKET             = os.getenv("S3_BUCKET")
S3_ACCESS_KEY         = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY         = os.getenv("S3_SECRET_KEY")
S3_REGION             = os.getenv("S3_REGION")
MISTRAL_API_KEY       = os.getenv("MISTRAL_API_KEY", "")
AMADEUS_CLIENT_ID     = os.getenv("AMADEUS_CLIENT_ID", "")
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET", "")

MISTRAL_BASE          = "https://api.mistral.ai/v1"
MISTRAL_OCR_MODEL     = "mistral-ocr-2512"
MISTRAL_EXTRACT_MODEL = "mistral-small-latest"

# BMF Verpflegungsmehraufwand Para. 9 EStG (Stand 2024)
VMA = {
    "DE": {"full": 28.0, "partial": 14.0},
    "FR": {"full": 40.0, "partial": 20.0},
    "GB": {"full": 54.0, "partial": 27.0},
    "US": {"full": 56.0, "partial": 28.0},
    "IN": {"full": 32.0, "partial": 16.0},
    "AE": {"full": 53.0, "partial": 26.5},
    "AZ": {"full": 37.0, "partial": 18.5},
    "CN": {"full": 44.0, "partial": 22.0},
    "JP": {"full": 48.0, "partial": 24.0},
    "SG": {"full": 45.0, "partial": 22.5},
    "TR": {"full": 35.0, "partial": 17.5},
    "CH": {"full": 55.0, "partial": 27.5},
    "AT": {"full": 35.0, "partial": 17.5},
    "IT": {"full": 37.0, "partial": 18.5},
    "ES": {"full": 35.0, "partial": 17.5},
    "NL": {"full": 39.0, "partial": 19.5},
    "PL": {"full": 24.0, "partial": 12.0},
}
MEAL_DED = {"breakfast": 5.60, "lunch": 11.20, "dinner": 11.20}

def get_vma(cc, day_type, meals):
    r = VMA.get((cc or "DE").upper(), {"full": 28.0, "partial": 14.0})
    base  = r["full"] if day_type == "full" else r["partial"]
    abzug = sum(MEAL_DED.get(m, 0) for m in (meals or []))
    return max(0.0, round(base - abzug, 2))

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_s3():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION)

def compute_status(dep, ret):
    today = date.today()
    if not dep: return "planned"
    if isinstance(dep, str):
        try: dep = date.fromisoformat(dep)
        except: return "planned"
    if isinstance(ret, str):
        try: ret = date.fromisoformat(ret)
        except: ret = None
    if today < dep: return "planned"
    if ret and today > ret: return "done"
    return "active"

def next_trip_code(cur):
    yr = str(date.today().year)[-2:]
    cur.execute("SELECT trip_code FROM trip_meta WHERE trip_code LIKE %s ORDER BY trip_code DESC LIMIT 1", (f"{yr}-%",))
    row = cur.fetchone()
    num = int(row[0].split("-")[1]) + 1 if row else 1
    return f"{yr}-{str(num).zfill(3)}"

# =========================================================
# MISTRAL KI-BELEGERKENNUNG (DSGVO-konform, EU)
# =========================================================

async def mistral_ocr(file_bytes: bytes, filename: str) -> str:
    if not MISTRAL_API_KEY:
        return "KEIN_MISTRAL_KEY"
    ext = filename.lower().split(".")[-1]
    try:
        async with httpx.AsyncClient(timeout=60.0) as cl:
            if ext == "pdf":
                up = await cl.post(f"{MISTRAL_BASE}/files",
                    headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"},
                    files={"file": (filename, file_bytes, "application/pdf")},
                    data={"purpose": "ocr"})
                if up.status_code != 200:
                    return f"OCR_UPLOAD_FEHLER: {up.status_code}"
                fid = up.json().get("id", "")
                ur  = await cl.get(f"{MISTRAL_BASE}/files/{fid}/url?expiry=60",
                    headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"})
                signed = ur.json().get("url", "")
                resp = await cl.post(f"{MISTRAL_BASE}/ocr",
                    headers={"Authorization": f"Bearer {MISTRAL_API_KEY}", "Content-Type": "application/json"},
                    json={"model": MISTRAL_OCR_MODEL, "document": {"type": "document_url", "document_url": signed}, "include_image_base64": False})
                # Datei nach OCR loeschen (DSGVO - minimale Speicherung)
                await cl.delete(f"{MISTRAL_BASE}/files/{fid}", headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"})
            elif ext in ("jpg", "jpeg", "png", "webp"):
                b64  = base64.b64encode(file_bytes).decode()
                mime = "image/jpeg" if ext in ("jpg","jpeg") else f"image/{ext}"
                resp = await cl.post(f"{MISTRAL_BASE}/ocr",
                    headers={"Authorization": f"Bearer {MISTRAL_API_KEY}", "Content-Type": "application/json"},
                    json={"model": MISTRAL_OCR_MODEL,
                          "document": {"type": "image_url", "image_url": f"data:{mime};base64,{b64}"},
                          "include_image_base64": False})
            else:
                return "NICHT_ANALYSIERBAR"
        if resp.status_code != 200:
            return f"OCR_FEHLER: {resp.status_code}"
        pages = resp.json().get("pages", [])
        text  = "\n\n".join(p.get("markdown", "") for p in pages).strip()
        return text[:20000] if text else "KEIN_TEXT_GEFUNDEN"
    except Exception as e:
        return f"ERROR: {e}"


async def mistral_extract(ocr_text: str, known_codes: list) -> dict:
    if not MISTRAL_API_KEY or not ocr_text or ocr_text.startswith(("KEIN","ERROR","OCR_","NICHT")):
        return {}
    codes_str = ", ".join(known_codes) if known_codes else "keine"
    system = """Du bist Spezialist fuer Reisekostenbelege in deutschen Unternehmen.
Analysiere den OCR-Text und extrahiere Felder als JSON.
Antworte NUR mit einem gueltigen JSON-Objekt ohne Markdown-Backticks.

Felder:
- betrag: Dezimalzahl als String z.B. "142.50" (Punkt), oder ""
- waehrung: ISO-Code z.B. "EUR". Standard immer "EUR" wenn kein Fremdwaehrungs-Symbol/Code explizit im Text
- datum: "DD.MM.YYYY" oder ""
- anbieter: Firmenname z.B. "Lufthansa", "Uber", oder ""
- beleg_typ: eines von: Flug, Hotel, Taxi, Bahn, Mietwagen, Essen, Sonstiges
- reisecode: Format YY-NNN z.B. "26-001" falls im Text, sonst ""
- confidence: "hoch" wenn Betrag+Typ+Datum sicher, "mittel" wenn 2 von 3, sonst "niedrig"
- bemerkung: kurze Notiz auf Deutsch wenn unklar, sonst ""

WICHTIG: INR nur wenn Rupien-Symbol oder INR explizit. USD nur wenn Dollar-Symbol oder USD explizit. Sonst immer EUR."""

    user = f"Bekannte Reisecodes: {codes_str}\n\nOCR-Text:\n---\n{ocr_text[:8000]}\n---\nExtrahiere JSON."
    try:
        async with httpx.AsyncClient(timeout=30.0) as cl:
            resp = await cl.post(f"{MISTRAL_BASE}/chat/completions",
                headers={"Authorization": f"Bearer {MISTRAL_API_KEY}", "Content-Type": "application/json"},
                json={"model": MISTRAL_EXTRACT_MODEL,
                      "messages": [{"role":"system","content":system},{"role":"user","content":user}],
                      "temperature": 0.0, "max_tokens": 400,
                      "response_format": {"type": "json_object"}})
        if resp.status_code != 200: return {}
        content = resp.json()["choices"][0]["message"]["content"]
        content = content.strip().strip("```json").strip("```").strip()
        return json.loads(content)
    except Exception as e:
        return {"fehler": str(e)}


async def analyse_ki(att_id, storage_key, filename, conn, known_codes):
    ext = (filename or "").lower().split(".")[-1]
    cur = conn.cursor()
    if ext not in ("pdf","jpg","jpeg","png","webp"):
        cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    ("nicht analysierbar","niedrig","pruefen",att_id))
        cur.close(); return
    try:
        s3  = get_s3()
        obj = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        file_bytes = obj["Body"].read()
    except Exception as e:
        cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (f"s3-fehler: {str(e)[:80]}","niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return

    ocr_text = await mistral_ocr(file_bytes, filename)

    if not ocr_text or ocr_text in ("KEIN_TEXT_GEFUNDEN","NICHT_ANALYSIERBAR","KEIN_MISTRAL_KEY"):
        cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (ocr_text, (ocr_text or "kein text").lower().replace("_"," "),"niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return

    if ocr_text.startswith(("ERROR","OCR_")):
        cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (ocr_text,"analysefehler","niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return

    fields = await mistral_extract(ocr_text, known_codes)

    betrag     = fields.get("betrag","") or ""
    waehrung   = fields.get("waehrung","EUR") or "EUR"
    datum      = fields.get("datum","") or ""
    anbieter   = fields.get("anbieter","") or ""
    beleg_typ  = fields.get("beleg_typ","Sonstiges") or "Sonstiges"
    reisecode  = fields.get("reisecode","") or ""
    confidence = fields.get("confidence","niedrig") or "niedrig"
    bemerkung  = fields.get("bemerkung","") or ""

    betrag_eur = ""
    if betrag:
        try:
            val   = float(betrag.replace(",","."))
            rates = {"EUR":1.0,"USD":0.93,"GBP":1.17,"INR":0.011,"CHF":1.04}
            eur   = round(val * rates.get(waehrung.upper(),1.0), 2)
            betrag_eur = f"{eur:.2f}".replace(".",",")
        except: pass

    review = "ok" if confidence == "hoch" else "pruefen"
    status = "ok" if fields and "fehler" not in fields else "analysefehler"

    if reisecode:
        cur.execute("UPDATE mail_attachments SET trip_code=%s WHERE id=%s AND (trip_code IS NULL OR trip_code='')",
                    (reisecode, att_id))

    cur.execute("""UPDATE mail_attachments SET
        extracted_text=%s, detected_amount=%s, detected_amount_eur=%s, detected_currency=%s,
        detected_date=%s, detected_vendor=%s, detected_type=%s,
        analysis_status=%s, confidence=%s, review_flag=%s, ki_bemerkung=%s
        WHERE id=%s""",
        (ocr_text[:10000], betrag, betrag_eur, waehrung, datum, anbieter, beleg_typ,
         status, confidence, review, bemerkung, att_id))
    conn.commit(); cur.close()


# =========================================================
# MAIL-HILFSFUNKTIONEN
# =========================================================

def extract_trip_code(text):
    m = re.search(r"\b\d{2}-\d{3}\b", text or "")
    return m.group(0) if m else None

def decode_mime_header(value):
    if not value: return ""
    parts = decode_header(value)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            result.append(part)
    return "".join(result)

def detect_mail_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["flug","flight","boarding","pnr","ticket","airline","itinerary","eticket"]): return "Flug"
    if any(x in t for x in ["hotel","booking.com","check-in","reservation","zimmer","accommodation"]): return "Hotel"
    if any(x in t for x in ["taxi","uber","cab","ride"]): return "Taxi"
    if any(x in t for x in ["bahn","zug","train","ice","db "]): return "Bahn"
    if any(x in t for x in ["restaurant","verpflegung","essen","dinner","lunch","breakfast"]): return "Essen"
    if any(x in t for x in ["mietwagen","rental car","hertz","sixt","avis"]): return "Mietwagen"
    return "Unbekannt"

def detect_attachment_type(filename, subject, body):
    text = f"{filename or ''} {subject or ''} {body or ''}".lower()
    if (filename or "").lower().endswith(".ics"): return "Kalendereintrag"
    if (filename or "").lower().endswith(".emz"): return "Inline-Grafik"
    if any(x in text for x in ["boarding","eticket","flight","flug","ticket","pnr","itinerary"]): return "Flug"
    if any(x in text for x in ["hotel","booking","reservation","zimmer","check-in"]): return "Hotel"
    if any(x in text for x in ["taxi","uber","cab","receipt_","ride"]): return "Taxi"
    if any(x in text for x in ["bahn","zug","train","ice"]): return "Bahn"
    if any(x in text for x in ["restaurant","essen","verpflegung","breakfast","lunch","dinner"]): return "Essen"
    if any(x in text for x in ["mietwagen","rental","hertz","sixt","avis"]): return "Mietwagen"
    return "Unbekannt"

def sanitize_filename(name):
    name = (name or "").replace("\\","_").replace("/","_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]","_",name)
    return name[:180] if name else "attachment.bin"


# =========================================================
# HTML SHELL – helles Design, Herrhammer-Logo
# =========================================================

CSS = """
:root{
  --page:#f0f4f9;--white:#fff;
  --b900:#0e2650;--b800:#153178;--b700:#1a3d96;--b600:#2152c4;--b500:#2e63e8;
  --b400:#4d7ef5;--b300:#7aa3fa;--b100:#dde9ff;--b50:#eef4ff;
  --t900:#0d1b33;--t700:#2c3e5e;--t500:#5a6e8a;--t300:#9bafc8;
  --bd:#dde4ef;--bds:#eaeef5;
  --gr6:#0f9e6e;--gr1:#d4f5eb;
  --am6:#c97c0a;--am1:#fef3d6;
  --re6:#dc2626;--re1:#fee2e2;
  --sh-sm:0 1px 3px rgba(14,38,80,.06),0 1px 2px rgba(14,38,80,.04);
  --sh:0 4px 16px rgba(14,38,80,.09),0 1px 4px rgba(14,38,80,.05);
  --sh-lg:0 12px 40px rgba(14,38,80,.14),0 4px 12px rgba(14,38,80,.06);
  --r:10px;--rs:7px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:var(--page);color:var(--t900);min-height:100vh;font-size:13.5px;line-height:1.55;-webkit-font-smoothing:antialiased}
.topbar{position:sticky;top:0;z-index:100;background:var(--white);border-bottom:1px solid var(--bd);box-shadow:var(--sh-sm);height:58px;display:flex;align-items:center;padding:0 24px;gap:0}
.logo-wrap{display:flex;align-items:center;margin-right:28px}
.logo-wrap img{height:40px;width:auto;display:block}
.nav-tabs{display:flex;align-items:center;gap:2px;flex:1}
.nav-tab{padding:6px 14px;border-radius:var(--rs);font-size:13px;font-weight:400;color:var(--t500);cursor:pointer;transition:all .13s;text-decoration:none;border:none;background:none;white-space:nowrap}
.nav-tab:hover{color:var(--t900);background:var(--b50)}
.nav-tab.active{color:var(--b600);background:var(--b50);font-weight:500}
.topbar-right{display:flex;align-items:center;gap:10px;margin-left:auto}
.ki-pill{font-size:11px;padding:3px 9px;border-radius:4px;border:1px solid;font-weight:500}
.ver-pill{font-family:'DM Mono',monospace;font-size:10.5px;color:var(--t300);background:var(--page);border:1px solid var(--bd);border-radius:4px;padding:2px 7px}
.dd-wrap{position:relative}
.add-btn{display:flex;align-items:center;gap:6px;background:var(--b600);color:white;border:none;border-radius:var(--rs);padding:7px 15px;font-family:'Inter',sans-serif;font-size:13px;font-weight:500;cursor:pointer;box-shadow:0 2px 6px rgba(33,82,196,.30);transition:background .13s,transform .1s}
.add-btn:hover{background:var(--b500);transform:translateY(-1px)}
.dd-menu{position:absolute;top:calc(100% + 8px);right:0;background:var(--white);border:1px solid var(--bd);border-radius:var(--r);box-shadow:var(--sh-lg);min-width:228px;overflow:hidden;opacity:0;pointer-events:none;transform:translateY(-6px) scale(.98);transition:opacity .14s,transform .14s;z-index:200}
.dd-menu.open{opacity:1;pointer-events:all;transform:translateY(0) scale(1)}
.dd-item{display:flex;align-items:center;gap:12px;padding:11px 16px;cursor:pointer;transition:background .1s;border:none;background:none;width:100%;text-align:left;color:var(--t900);font-family:'Inter',sans-serif;font-size:13px;text-decoration:none}
.dd-item:hover{background:var(--b50)}
.dd-icon{width:30px;height:30px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}
.di-b{background:var(--b50)} .di-g{background:var(--gr1)} .di-a{background:var(--am1)}
.dd-sub{font-size:11.5px;color:var(--t300);margin-top:1px}
.dd-div{height:1px;background:var(--bds);margin:4px 0}
.wrap{max-width:1380px;margin:0 auto;padding:24px 24px 60px;display:flex;flex-direction:column;gap:28px}
.sum-bar{display:flex;gap:12px;flex-wrap:wrap}
.sum-item{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);padding:14px 20px;box-shadow:var(--sh-sm);min-width:120px;transition:box-shadow .13s}
.sum-item:hover{box-shadow:var(--sh)}
.sum-val{font-family:'DM Mono',monospace;font-size:22px;font-weight:500;color:var(--t900);letter-spacing:-.5px}
.sum-val.blue{color:var(--b600)} .sum-val.green{color:var(--gr6)} .sum-val.red{color:var(--re6)}
.sum-lbl{font-size:11px;color:var(--t300);margin-top:3px}
.sec-hdr{display:flex;align-items:center;gap:10px;margin-bottom:12px}
.sec-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.sec-dot.active{background:var(--re6);box-shadow:0 0 0 3px rgba(220,38,38,.15)}
.sec-dot.planned{background:var(--b500);box-shadow:0 0 0 3px rgba(46,99,232,.15)}
.sec-dot.done{background:var(--gr6);box-shadow:0 0 0 3px rgba(15,158,110,.15)}
.sec-title{font-size:12px;font-weight:600;color:var(--t500);letter-spacing:.06em;text-transform:uppercase}
.sec-cnt{font-size:11px;font-family:'DM Mono',monospace;color:var(--t300);background:var(--white);border:1px solid var(--bd);border-radius:4px;padding:2px 8px;margin-left:auto}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(370px,1fr));gap:12px}
.card{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);box-shadow:var(--sh-sm);overflow:hidden;transition:box-shadow .15s,transform .15s,border-color .15s;cursor:pointer;position:relative}
.card:hover{box-shadow:var(--sh);transform:translateY(-2px);border-color:var(--b300)}
.card.alert{border-color:rgba(220,38,38,.35)}
.card.alert::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--re6),#f87171)}
.c-top{padding:15px 16px 11px;display:flex;align-items:flex-start;gap:11px}
.c-code{font-family:'DM Mono',monospace;font-size:12px;font-weight:500;color:var(--b700);background:var(--b50);border:1px solid var(--b100);border-radius:5px;padding:3px 9px;white-space:nowrap;flex-shrink:0}
.c-info{flex:1;min-width:0}
.c-traveler{font-size:14px;font-weight:600;color:var(--t900);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.c-dest{font-size:12px;color:var(--t500);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sbadge{font-size:11px;font-weight:500;padding:3px 10px;border-radius:20px;white-space:nowrap;flex-shrink:0}
.sb-active{background:var(--re1);color:var(--re6);border:1px solid rgba(220,38,38,.2)}
.sb-planned{background:var(--b50);color:var(--b600);border:1px solid var(--b100)}
.sb-done{background:var(--gr1);color:var(--gr6);border:1px solid rgba(15,158,110,.2)}
.adot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--re6);margin-right:5px;animation:pr 1.5s ease-in-out infinite}
@keyframes pr{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(220,38,38,.4)}50%{box-shadow:0 0 0 4px rgba(220,38,38,0)}}
.alert-bar{margin:0 12px 10px;padding:8px 12px;background:#fff5f5;border:1px solid rgba(220,38,38,.2);border-radius:var(--rs);font-size:12px;color:var(--re6);display:flex;align-items:center;gap:8px;font-weight:500}
.c-div{height:1px;background:var(--bds);margin:0 12px}
.c-meta{padding:10px 12px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.mpill{display:flex;align-items:center;gap:4px;font-size:11.5px;color:var(--t700);background:var(--page);border:1px solid var(--bd);border-radius:5px;padding:3px 8px}
.mpill.ok{color:var(--gr6);background:#f0fdf8;border-color:rgba(15,158,110,.2)}
.mpill.warn{color:var(--am6);background:#fffbeb;border-color:rgba(201,124,10,.2)}
.mpill.err{color:var(--re6);background:#fff5f5;border-color:rgba(220,38,38,.2)}
.mdate{margin-left:auto;font-size:11px;color:var(--t300);white-space:nowrap}
.prog-wrap{padding:0 12px 10px}
.prog-lbl{font-size:11px;color:var(--t300);display:flex;justify-content:space-between;margin-bottom:5px}
.prog-bg{height:4px;background:var(--page);border-radius:2px;overflow:hidden}
.prog-fill{height:100%;border-radius:2px;transition:width .4s}
.pf-full{background:linear-gradient(90deg,var(--b500),var(--b300))}
.pf-mid{background:linear-gradient(90deg,var(--am6),#fbbf24)}
.pf-low{background:var(--re6)}
.c-foot{padding:10px 12px 14px;display:flex;align-items:center;gap:10px}
.c-amt{font-family:'DM Mono',monospace;font-size:15px;font-weight:500;color:var(--t900)}
.c-amt-sub{font-size:10.5px;color:var(--t300);margin-top:1px}
.c-acts{margin-left:auto;display:flex;gap:6px}
.vma-row{padding:0 12px 10px;display:flex;align-items:center;gap:8px}
.vma-tag{font-size:11px;font-weight:500;color:var(--gr6);background:#f0fdf8;border:1px solid rgba(15,158,110,.2);border-radius:4px;padding:2px 8px}
.vma-detail{font-family:'DM Mono',monospace;font-size:11.5px;color:var(--t500)}
.vma-date{margin-left:auto;font-size:11px;color:var(--t300)}
.btn-g{font-size:12px;font-weight:500;color:var(--b600);background:var(--b50);border:1px solid var(--b100);border-radius:5px;padding:5px 12px;cursor:pointer;transition:all .12s;text-decoration:none;font-family:'Inter',sans-serif}
.btn-g:hover{background:var(--b100)}
.btn-s{font-size:12px;font-weight:500;color:white;background:var(--b600);border:none;border-radius:5px;padding:5px 12px;cursor:pointer;transition:background .12s;font-family:'Inter',sans-serif}
.btn-s:hover{background:var(--b500)}
.btn-dg{font-size:12px;font-weight:500;color:var(--re6);background:#fff5f5;border:1px solid rgba(220,38,38,.2);border-radius:5px;padding:5px 12px;cursor:pointer;transition:all .12s;font-family:'Inter',sans-serif}
.btn-dg:hover{background:var(--re1)}
.page-card{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);padding:24px;box-shadow:var(--sh-sm)}
.page-card h2{font-size:1.1rem;font-weight:600;margin-bottom:16px}
.btn{background:var(--b600);color:white;padding:8px 16px;border:none;border-radius:var(--rs);font-size:13px;font-weight:500;cursor:pointer;text-decoration:none;display:inline-block;transition:background .12s;font-family:'Inter',sans-serif}
.btn:hover{background:var(--b500)}
.btn-l{background:var(--white);color:var(--b600);padding:8px 16px;border:1px solid var(--b100);border-radius:var(--rs);font-size:13px;cursor:pointer;text-decoration:none;display:inline-block;transition:all .12s;font-family:'Inter',sans-serif}
.btn-l:hover{background:var(--b50)}
.acts{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
table{width:100%;border-collapse:collapse}
th,td{border:1px solid var(--bd);padding:8px 10px;text-align:left;vertical-align:top;font-size:12.5px}
th{background:var(--b50);font-weight:600;color:var(--t700)}
tr:hover td{background:#fafcff}
.cc{font-family:'DM Mono',monospace;font-weight:600;color:var(--b700)}
.ok-t{color:var(--gr6);font-weight:500} .warn-t{color:var(--am6);font-weight:500} .err-t{color:var(--re6);font-weight:500}
.bdg{padding:2px 8px;border-radius:20px;font-size:11px;font-weight:500}
.bdg-ok{background:var(--gr1);color:var(--gr6)} .bdg-w{background:var(--am1);color:var(--am6)} .bdg-e{background:var(--re1);color:var(--re6)}
.fgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.ff{grid-column:1/-1}
.fgrp{display:flex;flex-direction:column;gap:5px}
.flbl{font-size:11.5px;font-weight:500;color:var(--t700)}
.finp,.fsel{background:var(--page);border:1px solid var(--bd);border-radius:var(--rs);padding:8px 11px;color:var(--t900);font-family:'Inter',sans-serif;font-size:13px;transition:border-color .12s;width:100%}
.finp:focus,.fsel:focus{outline:none;border-color:var(--b400);background:var(--white);box-shadow:0 0 0 3px rgba(78,126,245,.1)}
.finp::placeholder{color:var(--t300)}
.mfooter{display:flex;gap:8px;justify-content:flex-end;padding-top:14px;border-top:1px solid var(--bds);margin-top:14px}
.modal-ov{position:fixed;inset:0;z-index:300;background:rgba(14,38,80,.35);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;opacity:0;pointer-events:none;transition:opacity .18s}
.modal-ov.open{opacity:1;pointer-events:all}
.modal{background:var(--white);border:1px solid var(--bd);border-radius:14px;box-shadow:var(--sh-lg);width:100%;max-width:530px;transform:translateY(10px) scale(.99);transition:transform .2s;max-height:90vh;overflow-y:auto}
.modal-ov.open .modal{transform:translateY(0) scale(1)}
.m-hdr{padding:20px 24px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--bds)}
.m-title{font-size:15px;font-weight:600;color:var(--t900)}
.m-close{width:28px;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--t300);background:none;border:none;font-size:18px;transition:all .12s}
.m-close:hover{background:var(--page);color:var(--t700)}
.m-body{padding:20px 24px 8px}
.code-prev{text-align:center;font-family:'DM Mono',monospace;font-size:22px;font-weight:500;color:var(--b700);background:var(--b50);border:1px solid var(--b100);border-radius:var(--rs);padding:12px 0;margin-bottom:4px;letter-spacing:1px}
.code-sub{text-align:center;font-size:11px;color:var(--t300);margin-bottom:16px}
.btn-mp{background:var(--b600);color:white;border:none;border-radius:var(--rs);padding:9px 22px;font-size:13px;font-weight:500;cursor:pointer;font-family:'Inter',sans-serif;box-shadow:0 2px 6px rgba(33,82,196,.25)}
.btn-mp:hover{background:var(--b500)}
.btn-mc{background:var(--page);color:var(--t700);border:1px solid var(--bd);border-radius:var(--rs);padding:9px 18px;font-size:13px;cursor:pointer;font-family:'Inter',sans-serif}
.btn-mc:hover{background:var(--bds)}
.up-zone{border:2px dashed var(--bd);border-radius:var(--r);padding:28px 20px;text-align:center;color:var(--t300);cursor:pointer;transition:all .15s;background:var(--page)}
.up-zone:hover,.up-zone.drag{border-color:var(--b400);color:var(--t700);background:var(--b50)}
.empty{text-align:center;padding:32px;color:var(--t300);font-size:13px;border:1px dashed var(--bd);border-radius:var(--r);background:var(--white)}
.sub{color:var(--t500);font-size:12px}
::-webkit-scrollbar{width:5px} ::-webkit-scrollbar-thumb{background:var(--bd);border-radius:3px}
@keyframes fu{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.sb{animation:fu .25s ease both}
.sb:nth-child(2){animation-delay:.06s} .sb:nth-child(3){animation-delay:.12s} .sb:nth-child(4){animation-delay:.18s}
"""

JS = """
function toggleDD(e){e.stopPropagation();document.getElementById('dd').classList.toggle('open')}
document.addEventListener('click',()=>document.getElementById('dd').classList.remove('open'));
function openM(t){
  document.getElementById('dd').classList.remove('open');
  if(t==='trip'){fetch('/api/next-code').then(r=>r.json()).then(d=>{document.getElementById('cprev').textContent=d.code}).catch(()=>{})}
  if(t==='event'||t==='upload'){
    fetch('/api/active-codes').then(r=>r.json()).then(d=>{
      const sel=document.getElementById(t==='event'?'ev-code':'up-code');
      if(sel){sel.innerHTML=(t==='upload'?'<option value="">– KI zuordnen lassen –</option>':'');d.codes.forEach(c=>{sel.innerHTML+=`<option>${c}</option>`});}
    }).catch(()=>{});
  }
  document.getElementById('m-'+t).classList.add('open');
  document.body.style.overflow='hidden';
}
function closeM(t){document.getElementById('m-'+t).classList.remove('open');document.body.style.overflow='';}
function submitTrip(){
  const f=new FormData();
  ['traveler_name','colleagues','departure_date','return_date','country_code','nights_planned','flight_numbers','car_rental_info','hotel_mode','notes'].forEach(k=>{
    const el=document.getElementById('fi-'+k.replace('_','-').replace('_','-').replace('_','-'));
    if(el)f.append(k,el.value);
  });
  fetch('/new-trip',{method:'POST',body:f}).then(r=>{window.location.href='/';});
  closeM('trip');
}
function showFile(inp){if(inp.files[0])document.getElementById('fname').textContent='✓ '+inp.files[0].name;}
function dropFile(e){e.preventDefault();document.getElementById('uz').classList.remove('drag');const f=e.dataTransfer.files[0];if(f)document.getElementById('fname').textContent='✓ '+f.name;}
"""

def page_shell(title, content, active_tab=""):
    tabs = [("active","Laufende Reisen","/active"),("planned","Vorplanung","/"),("done","Abgeschlossen","/done")]
    tab_html = "".join(f'<a href="{href}" class="nav-tab{" active" if active_tab==k else ""}">{lbl}</a>' for k,lbl,href in tabs)
    ki_ok    = bool(MISTRAL_API_KEY)
    ki_txt   = "✓ Mistral KI aktiv" if ki_ok else "⚠ Kein Mistral Key"
    ki_style = "color:#0f9e6e;background:#f0fdf8;border-color:rgba(15,158,110,.25)" if ki_ok else "color:#c97c0a;background:#fffbeb;border-color:rgba(201,124,10,.25)"
    country_opts = "\n".join(f'<option value="{c}">{c} – {l}</option>' for c,l in [
        ("DE","Deutschland 28€"),("AZ","Aserbaidschan 37€"),("AE","VAE/Dubai 53€"),
        ("FR","Frankreich 40€"),("GB","Großbritannien 54€"),("US","USA 56€"),
        ("IN","Indien 32€"),("CH","Schweiz 55€"),("AT","Österreich 35€"),
        ("IT","Italien 37€"),("TR","Türkei 35€"),("JP","Japan 48€"),("SG","Singapur 45€")])
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} – Herrhammer Reisekosten</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head>
<body>
<header class="topbar">
  <div class="logo-wrap"><img src="/static/herrhammer-logo.png" alt="Herrhammer Kuerschner Kerzenmaschinen"></div>
  <nav class="nav-tabs">{tab_html}</nav>
  <div class="topbar-right">
    <span class="ki-pill" style="{ki_style}">{ki_txt}</span>
    <span class="ver-pill">v{APP_VERSION}</span>
    <div class="dd-wrap">
      <button class="add-btn" onclick="toggleDD(event)">+ Neu</button>
      <div class="dd-menu" id="dd">
        <button class="dd-item" onclick="openM('trip')"><div class="dd-icon di-b">✈</div><div><div style="font-weight:500">Neue Reise anlegen</div><div class="dd-sub">Code wird automatisch vergeben</div></div></button>
        <button class="dd-item" onclick="openM('event')"><div class="dd-icon di-g">📋</div><div><div style="font-weight:500">Manuelles Ereignis</div><div class="dd-sub">Alert oder Notiz eintragen</div></div></button>
        <div class="dd-div"></div>
        <button class="dd-item" onclick="openM('upload')"><div class="dd-icon di-a">📎</div><div><div style="font-weight:500">Beleg hochladen</div><div class="dd-sub">KI-Analyse via Mistral OCR (EU)</div></div></button>
      </div>
    </div>
  </div>
</header>
<main class="wrap">{content}</main>

<div class="modal-ov" id="m-trip" onclick="closeM('trip')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Neue Reise anlegen</span><button class="m-close" onclick="closeM('trip')">×</button></div>
    <div class="m-body">
      <div class="code-prev" id="cprev">wird vergeben</div>
      <div class="code-sub">Reisecode automatisch vom System</div>
      <div class="fgrid">
        <div class="fgrp ff"><label class="flbl">Reisender</label><input class="finp" id="fi-traveler-name" type="text" placeholder="Vor- und Nachname"></div>
        <div class="fgrp ff"><label class="flbl">Kollegen (optional)</label><input class="finp" id="fi-colleagues" type="text" placeholder="z.B. T. Moser, K. Brenner"></div>
        <div class="fgrp"><label class="flbl">Abflugdatum</label><input class="finp" id="fi-departure-date" type="date"></div>
        <div class="fgrp"><label class="flbl">Rückkehrdatum</label><input class="finp" id="fi-return-date" type="date"></div>
        <div class="fgrp ff"><label class="flbl">Reiseziel</label><input class="finp" id="fi-dest" type="text" placeholder="z.B. Baku, Aserbaidschan"></div>
        <div class="fgrp"><label class="flbl">Land (ISO)</label><select class="fsel" id="fi-country-code">{country_opts}</select></div>
        <div class="fgrp"><label class="flbl">Geplante Nächte</label><input class="finp" id="fi-nights-planned" type="number" min="0" value="0"></div>
        <div class="fgrp ff"><label class="flbl">Flugnummern</label><input class="finp" id="fi-flight-numbers" type="text" placeholder="z.B. AZ770, AZ281"></div>
        <div class="fgrp ff"><label class="flbl">Mietwagen-Info</label><input class="finp" id="fi-car-rental-info" type="text" placeholder="z.B. Hertz FRA T1, Pickup 13:00"></div>
        <div class="fgrp ff"><label class="flbl">Hotel-Status</label>
          <select class="fsel" id="fi-hotel-mode"><option value="">– noch offen –</option><option value="customer">Kunde stellt Hotel</option><option value="own">Eigenes Hotel gebucht</option></select></div>
        <div class="fgrp ff"><label class="flbl">Notizen</label><input class="finp" id="fi-notes" type="text" placeholder="Interne Notiz..."></div>
      </div>
      <div class="mfooter"><button class="btn-mc" onclick="closeM('trip')">Abbrechen</button><button class="btn-mp" onclick="submitTrip()">Reise anlegen</button></div>
    </div>
  </div>
</div>

<div class="modal-ov" id="m-event" onclick="closeM('event')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Manuelles Ereignis</span><button class="m-close" onclick="closeM('event')">×</button></div>
    <div class="m-body">
      <div class="fgrid">
        <div class="fgrp ff"><label class="flbl">Reise</label><select class="fsel" id="ev-code"></select></div>
        <div class="fgrp ff"><label class="flbl">Ereignistyp</label>
          <select class="fsel"><option>Flugverspätung</option><option>Umbuchung Kollege</option><option>Hoteländerung</option><option>Mietwagen-Verlängerung</option><option>Zuganschluss-Änderung</option><option>Sonstige Notiz</option></select></div>
        <div class="fgrp ff"><label class="flbl">Beschreibung</label><input class="finp" type="text" placeholder="z.B. AZ770 +47 Min."></div>
        <div class="fgrp"><label class="flbl">Schweregrad</label>
          <select class="fsel"><option>⚠ Warnung</option><option>🔴 Alert</option><option>ℹ Info</option></select></div>
        <div class="fgrp"><label class="flbl">Datum / Uhrzeit</label><input class="finp" type="datetime-local"></div>
      </div>
      <div class="mfooter"><button class="btn-mc" onclick="closeM('event')">Abbrechen</button><button class="btn-mp">Speichern</button></div>
    </div>
  </div>
</div>

<div class="modal-ov" id="m-upload" onclick="closeM('upload')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Beleg hochladen</span><button class="m-close" onclick="closeM('upload')">×</button></div>
    <div class="m-body">
      <div class="fgrid" style="margin-bottom:14px">
        <div class="fgrp ff"><label class="flbl">Reise zuordnen (optional – KI erkennt Code automatisch)</label>
          <select class="fsel" id="up-code"><option value="">– KI zuordnen lassen –</option></select></div>
      </div>
      <div class="up-zone" id="uz" ondragover="event.preventDefault();this.classList.add('drag')" ondragleave="this.classList.remove('drag')" ondrop="dropFile(event)" onclick="document.getElementById('fi').click()">
        <div style="font-size:26px;margin-bottom:6px">📎</div>
        <div style="font-size:13px;font-weight:500">Datei hierher ziehen oder klicken</div>
        <div style="font-size:11px;margin-top:3px">PDF, JPG, PNG – Mistral OCR 3 analysiert automatisch</div>
      </div>
      <input type="file" id="fi" style="display:none" accept=".pdf,.jpg,.jpeg,.png" onchange="showFile(this)">
      <div id="fname" style="font-size:12px;color:var(--t500);margin-top:8px;min-height:18px"></div>
      <div style="font-size:11px;color:var(--t300);margin-top:6px">🔒 DSGVO: Belege werden via Mistral EU-API (Paris) analysiert. Keine Datenspeicherung beim KI-Anbieter nach Analyse.</div>
      <div class="mfooter"><button class="btn-mc" onclick="closeM('upload')">Abbrechen</button><button class="btn-mp">Hochladen &amp; KI-Analyse</button></div>
    </div>
  </div>
</div>

<script>{JS}</script>
</body>
</html>"""


# =========================================================
# API HELPER
# =========================================================

@app.get("/api/next-code")
def api_next_code():
    try:
        conn = get_conn(); cur = conn.cursor()
        code = next_trip_code(cur)
        cur.close(); conn.close()
        return {"code": code}
    except Exception as e:
        return {"code": "–", "error": str(e)}

@app.get("/api/active-codes")
def api_active_codes():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code DESC LIMIT 30")
        codes = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return {"codes": codes}
    except Exception as e:
        return {"codes": [], "error": str(e)}

@app.get("/version")
def version():
    return {"version": APP_VERSION, "ki": "mistral-eu" if MISTRAL_API_KEY else "keine"}


# =========================================================
# /init
# =========================================================

@app.get("/init")
def init():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS mail_messages (id SERIAL PRIMARY KEY, mail_uid TEXT UNIQUE)")
        for col in ["sender TEXT","subject TEXT","body TEXT","trip_code TEXT","detected_type TEXT","created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS {col}")
        cur.execute("CREATE TABLE IF NOT EXISTS mail_attachments (id SERIAL PRIMARY KEY, mail_uid TEXT)")
        for col in ["trip_code TEXT","original_filename TEXT","saved_filename TEXT","content_type TEXT",
                    "file_path TEXT","detected_type TEXT","extracted_text TEXT","detected_amount TEXT",
                    "detected_amount_eur TEXT","detected_currency TEXT","detected_date TEXT","detected_vendor TEXT",
                    "analysis_status TEXT DEFAULT 'ausstehend'","storage_key TEXT",
                    "confidence TEXT DEFAULT 'niedrig'","review_flag TEXT DEFAULT 'pruefen'",
                    "ki_bemerkung TEXT","created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS {col}")
        cur.execute("""CREATE TABLE IF NOT EXISTS trip_meta (
            trip_code TEXT PRIMARY KEY, hotel_mode TEXT, departure_date DATE, return_date DATE,
            country_code TEXT DEFAULT 'DE', traveler_name TEXT, colleagues TEXT,
            flight_numbers TEXT, car_rental_info TEXT, nights_planned INTEGER DEFAULT 0,
            meals_reimbursed TEXT DEFAULT '', notes TEXT, created_at TIMESTAMP DEFAULT now())""")
        for col in ["departure_date DATE","return_date DATE","country_code TEXT DEFAULT 'DE'",
                    "traveler_name TEXT","colleagues TEXT","flight_numbers TEXT","car_rental_info TEXT",
                    "nights_planned INTEGER DEFAULT 0","meals_reimbursed TEXT DEFAULT ''","notes TEXT","ki_bemerkung TEXT"]:
            cur.execute(f"ALTER TABLE trip_meta ADD COLUMN IF NOT EXISTS {col}")
        cur.execute("""CREATE TABLE IF NOT EXISTS flight_alerts (
            id SERIAL PRIMARY KEY, trip_code TEXT, flight_number TEXT, flight_date TEXT,
            alert_type TEXT, message TEXT, source TEXT, delay_min INTEGER,
            checked_at TIMESTAMP DEFAULT now())""")
        conn.commit(); cur.close(); conn.close()
        return {"status": "ok", "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}


# =========================================================
# TRIPS LADEN
# =========================================================

def load_trips(conn, filter_status=None):
    cur = conn.cursor()
    cur.execute("""SELECT trip_code,hotel_mode,departure_date,return_date,country_code,
                   traveler_name,colleagues,flight_numbers,car_rental_info,nights_planned,
                   meals_reimbursed,notes FROM trip_meta ORDER BY trip_code""")
    raw = cur.fetchall()
    cur.execute("SELECT COALESCE(trip_code,'') tc,detected_type,COALESCE(detected_amount_eur,'') eur,review_flag FROM mail_attachments")
    att_rows = cur.fetchall(); cur.close()
    att = {}
    for tc,dt,eur,rf in att_rows:
        if tc not in att: att[tc] = {"types":[],"sum":0.0,"review":0}
        att[tc]["types"].append(dt)
        if rf=="pruefen": att[tc]["review"]+=1
        if eur:
            try: att[tc]["sum"]+=float(eur.replace(".","").replace(",","."))
            except: pass
    trips = []
    for row in raw:
        tc,hm,dep,ret,cc,traveler,colleagues,fns,car,nights,meals,notes = row
        status = compute_status(dep,ret)
        if filter_status and status!=filter_status: continue
        a = att.get(tc,{"types":[],"sum":0.0,"review":0})
        types = a["types"]
        trips.append(dict(tc=tc,status=status,hm=hm,dep=dep,ret=ret,cc=cc or "DE",
            traveler=traveler or "",colleagues=colleagues or "",fns=fns or "",
            car=car or "",nights=nights or 0,meals=meals or "",notes=notes or "",
            has_flight="Flug" in types,
            has_hotel="Hotel" in types or hm in ("customer","own"),
            has_car="Mietwagen" in types or bool(car),
            has_taxi="Taxi" in types, has_essen="Essen" in types,
            sum_eur=round(a["sum"],2), review=a["review"],
            warnings=[w for w in [
                None if "Flug" in types else "Kein Flugbeleg",
                None if ("Hotel" in types or hm in ("customer","own")) or status=="done" else "Hotel fehlt",
            ] if w]))
    return trips

def _pills(t):
    def p(ok,lbl):
        return f'<div class="mpill {"ok" if ok else "err"}"><span>{"✓" if ok else "✗"}</span> {lbl}</div>'
    dep = str(t["dep"])[:10] if t["dep"] else "–"
    ret = str(t["ret"])[:10] if t["ret"] else "–"
    return p(t["has_flight"],"Flug") + p(t["has_hotel"],"Hotel") + p(t["has_car"],"Mietwagen") + f'<div class="mdate">{dep} – {ret}</div>'

def _progress(t):
    sc  = sum([t["has_flight"],t["has_hotel"],t["has_car"]])
    pct = int(sc/3*100)
    cls = "pf-full" if sc==3 else ("pf-mid" if sc>=1 else "pf-low")
    lc  = "var(--gr6)" if sc==3 else ("var(--am6)" if sc>=1 else "var(--re6)")
    lbl = "vollständig" if sc==3 else (f"{sc}/3 – {t['warnings'][0]}" if t["warnings"] else f"{sc}/3")
    return f'<div class="prog-wrap"><div class="prog-lbl"><span>Vollständigkeit</span><span style="color:{lc};font-weight:500">{lbl}</span></div><div class="prog-bg"><div class="prog-fill {cls}" style="width:{pct}%"></div></div></div>'


# =========================================================
# DASHBOARD – eine Seite, drei Sektionen
# =========================================================

@app.get("/", response_class=HTMLResponse)
@app.get("/active", response_class=HTMLResponse)
@app.get("/done", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        conn = get_conn()
        all_trips = load_trips(conn)
        conn.close()
        active_t  = [t for t in all_trips if t["status"]=="active"]
        planned_t = [t for t in all_trips if t["status"]=="planned"]
        done_t    = [t for t in all_trips if t["status"]=="done"]
        total_eur = sum(t["sum_eur"] for t in all_trips)
        open_alerts = sum(1 for t in active_t if t["warnings"])

        summary = f"""<div class="sum-bar sb">
          <div class="sum-item"><div class="sum-val blue">{len(active_t)}</div><div class="sum-lbl">Aktive Reisen</div></div>
          <div class="sum-item"><div class="sum-val">{len(planned_t)}</div><div class="sum-lbl">In Planung</div></div>
          <div class="sum-item"><div class="sum-val green">{len(done_t)}</div><div class="sum-lbl">Abgeschlossen</div></div>
          <div class="sum-item"><div class="sum-val {"red" if open_alerts else ""}">{open_alerts}</div><div class="sum-lbl">Offene Alerts</div></div>
          <div style="flex:1"></div>
          <div class="sum-item"><div class="sum-val" style="font-size:18px">{total_eur:,.2f} €</div><div class="sum-lbl">Belege gesamt</div></div>
        </div>"""

        # Aktive Reisen
        if active_t:
            ac = ""
            for t in active_t:
                ha = bool(t["warnings"])
                ac += f"""<div class="card {"alert" if ha else ""}" onclick="location.href='/trip/{t["tc"]}'">
                  <div class="c-top"><div class="c-code">{t["tc"]}</div>
                    <div class="c-info"><div class="c-traveler">{t["traveler"] or "–"}</div>
                      <div class="c-dest">{t["cc"]} · {t["fns"] or "–"}</div></div>
                    <div class="sbadge sb-active">{"<span class='adot'></span>Alert" if ha else "Aktiv"}</div>
                  </div>
                  {"<div class='alert-bar'>⚠ " + " · ".join(t["warnings"]) + "</div>" if ha else ""}
                  <div class="c-div"></div>
                  <div class="c-meta">{_pills(t)}</div>
                  <div class="c-foot">
                    <div><div class="c-amt">{t["sum_eur"]:,.2f} €</div><div class="c-amt-sub">Erfasste Belege</div></div>
                    <div class="c-acts">
                      <button class="{"btn-dg" if ha else "btn-g"}" onclick="event.stopPropagation();location.href='/check-flights/{t["tc"]}'">Flüge prüfen</button>
                      <button class="btn-s" onclick="event.stopPropagation();location.href='/trip/{t["tc"]}'">Detail</button>
                    </div>
                  </div>
                </div>"""
            active_html = f'<div class="cards">{ac}</div>'
        else:
            active_html = '<div class="empty">Keine laufenden Reisen.</div>'

        # Vorplanung
        if planned_t:
            pc = ""
            for t in planned_t:
                dep = str(t["dep"])[:10] if t["dep"] else "–"
                pc += f"""<div class="card" onclick="location.href='/trip/{t["tc"]}'">
                  <div class="c-top"><div class="c-code">{t["tc"]}</div>
                    <div class="c-info"><div class="c-traveler">{t["traveler"] or "–"}</div>
                      <div class="c-dest">Ab {dep} · {t["cc"]}</div></div>
                    <div class="sbadge sb-planned">Geplant</div>
                  </div>
                  {_progress(t)}
                  <div class="c-div"></div>
                  <div class="c-meta">{_pills(t)}</div>
                  <div class="c-foot">
                    <div><div class="c-amt">–</div><div class="c-amt-sub">Noch nicht aktiv</div></div>
                    <div class="c-acts"><a class="btn-g" href="/edit-trip/{t["tc"]}">Bearbeiten</a></div>
                  </div>
                </div>"""
            planned_html = f'<div class="cards">{pc}</div>'
        else:
            planned_html = '<div class="empty">Keine geplanten Reisen. Über &ldquo;+ Neu&rdquo; anlegen.</div>'

        # Abgeschlossen
        if done_t:
            dc = ""
            for t in done_t:
                dep_d = t["dep"] if isinstance(t["dep"],date) else (date.fromisoformat(str(t["dep"])) if t["dep"] else None)
                ret_d = t["ret"] if isinstance(t["ret"],date) else (date.fromisoformat(str(t["ret"])) if t["ret"] else None)
                days  = (ret_d-dep_d).days+1 if dep_d and ret_d else 0
                ml    = [m.strip() for m in t["meals"].split(",") if m.strip()]
                vma   = 0.0
                if days>0:
                    if days==1: vma=get_vma(t["cc"],"partial",ml)
                    else:
                        vma+=get_vma(t["cc"],"partial",[])
                        vma+=get_vma(t["cc"],"full",[])*max(0,days-2)
                        vma+=get_vma(t["cc"],"partial",ml)
                dep_s=str(dep_d)[:10] if dep_d else "–"; ret_s=str(ret_d)[:10] if ret_d else "–"
                dc += f"""<div class="card" onclick="location.href='/report/{t["tc"]}'">
                  <div class="c-top"><div class="c-code">{t["tc"]}</div>
                    <div class="c-info"><div class="c-traveler">{t["traveler"] or "–"}</div>
                      <div class="c-dest">{dep_s} – {ret_s} · {days} Tage · {t["cc"]}</div></div>
                    <div class="sbadge sb-done">Abgerechnet</div>
                  </div>
                  <div class="c-div"></div>
                  <div class="vma-row">
                    <span class="vma-tag">VMA §9 EStG</span>
                    <span class="vma-detail">{days} Tage · {t["cc"]} · {vma:.2f} €</span>
                    <span class="vma-date">{dep_s}</span>
                  </div>
                  <div class="c-foot">
                    <div><div class="c-amt">{(t["sum_eur"]+vma):,.2f} €</div><div class="c-amt-sub">Belege + VMA gesamt</div></div>
                    <div class="c-acts"><a class="btn-g" href="/report/{t["tc"]}">Abrechnung</a></div>
                  </div>
                </div>"""
            done_html = f'<div class="cards">{dc}</div>'
        else:
            done_html = '<div class="empty">Keine abgeschlossenen Reisen.</div>'

        content = summary + f"""
        <div class="sb" id="active">
          <div class="sec-hdr"><div class="sec-dot active"></div><span class="sec-title">Laufende Reisen</span><span class="sec-cnt">{len(active_t)} aktiv</span></div>
          {active_html}
        </div>
        <div class="sb" id="planned">
          <div class="sec-hdr"><div class="sec-dot planned"></div><span class="sec-title">Vorplanung</span><span class="sec-cnt">{len(planned_t)} geplant</span></div>
          {planned_html}
        </div>
        <div class="sb" id="done">
          <div class="sec-hdr"><div class="sec-dot done"></div><span class="sec-title">Abgeschlossen</span><span class="sec-cnt">{len(done_t)} Reisen</span></div>
          {done_html}
        </div>
        <div class="sb">
          <div class="acts">
            <a class="btn" href="/fetch-mails">📥 Mails abrufen</a>
            <a class="btn" href="/analyze-attachments">🔍 Belege analysieren (KI)</a>
            <a class="btn-l" href="/attachment-log">Anhang-Log</a>
            <a class="btn-l" href="/mail-log">Mail-Log</a>
            <a class="btn-l" href="/init" style="color:var(--t300)">DB Init</a>
          </div>
        </div>"""

        return page_shell("Dashboard", content, active_tab="active")
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p><a class="btn" href="/init">DB init</a></div>'), status_code=500)


# =========================================================
# NEUE REISE / EDIT
# =========================================================

@app.post("/new-trip")
async def new_trip(request: Request):
    try:
        form = await request.form()
        conn = get_conn(); cur = conn.cursor()
        tc   = next_trip_code(cur)
        cur.execute("""INSERT INTO trip_meta
            (trip_code,traveler_name,colleagues,departure_date,return_date,country_code,
             flight_numbers,nights_planned,car_rental_info,hotel_mode,notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (trip_code) DO NOTHING""",
            (tc, form.get("traveler_name") or None, form.get("colleagues") or None,
             form.get("departure_date") or None, form.get("return_date") or None,
             form.get("country_code") or "DE", form.get("flight_numbers") or None,
             int(form.get("nights_planned") or 0), form.get("car_rental_info") or None,
             form.get("hotel_mode") or None, form.get("notes") or None))
        conn.commit(); cur.close(); conn.close()
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)}, status_code=500)

@app.get("/edit-trip/{tc}", response_class=HTMLResponse)
def edit_trip_form(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT traveler_name,colleagues,departure_date,return_date,country_code,flight_numbers,nights_planned,car_rental_info,meals_reimbursed,notes,hotel_mode FROM trip_meta WHERE trip_code=%s",(tc,))
        row=cur.fetchone();cur.close();conn.close()
        if not row: return HTMLResponse("Nicht gefunden",404)
        traveler,colleagues,dep,ret,cc,fns,nights,car,meals,notes,hm=row
        dep_v=str(dep) if dep else ""; ret_v=str(ret) if ret else ""
        cc_opts="".join(f'<option value="{c}" {"selected" if cc==c else ""}>{c} – {l}</option>' for c,l in [
            ("DE","Deutschland 28€"),("AZ","Aserbaidschan 37€"),("AE","VAE/Dubai 53€"),("FR","Frankreich 40€"),
            ("GB","Großbritannien 54€"),("US","USA 56€"),("IN","Indien 32€"),("CH","Schweiz 55€"),
            ("AT","Österreich 35€"),("IT","Italien 37€"),("TR","Türkei 35€"),("JP","Japan 48€"),("SG","Singapur 45€")])
        meal_chks="".join(f'<label style="margin-right:12px"><input type="checkbox" name="meals_reimbursed" value="{m}" {"checked" if m in (meals or "") else ""}> {m}</label>' for m in ["breakfast","lunch","dinner"])
        hm_opts="".join(f'<option value="{v}" {"selected" if hm==v else ""}>{l}</option>' for v,l in [("","– offen –"),("customer","Kunde stellt Hotel"),("own","Eigenes Hotel")])
        return page_shell(f"Bearbeiten {tc}", f"""
        <div class="page-card" style="max-width:700px">
          <h2>Reise {tc} bearbeiten</h2>
          <form method="post" action="/edit-trip/{tc}">
            <div class="fgrid">
              <div class="fgrp ff"><label class="flbl">Reisender</label><input class="finp" name="traveler_name" value="{traveler or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Kollegen</label><input class="finp" name="colleagues" value="{colleagues or ''}"></div>
              <div class="fgrp"><label class="flbl">Abflug</label><input class="finp" type="date" name="departure_date" value="{dep_v}"></div>
              <div class="fgrp"><label class="flbl">Rückkehr</label><input class="finp" type="date" name="return_date" value="{ret_v}"></div>
              <div class="fgrp"><label class="flbl">Land</label><select class="fsel" name="country_code">{cc_opts}</select></div>
              <div class="fgrp"><label class="flbl">Geplante Nächte</label><input class="finp" type="number" name="nights_planned" value="{nights or 0}"></div>
              <div class="fgrp ff"><label class="flbl">Flugnummern</label><input class="finp" name="flight_numbers" value="{fns or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Mietwagen</label><input class="finp" name="car_rental_info" value="{car or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Hotel</label><select class="fsel" name="hotel_mode">{hm_opts}</select></div>
              <div class="fgrp ff"><label class="flbl">Erstattete Mahlzeiten</label><div style="padding:6px 0">{meal_chks}</div></div>
              <div class="fgrp ff"><label class="flbl">Notizen</label><input class="finp" name="notes" value="{notes or ''}"></div>
            </div>
            <div class="mfooter"><a class="btn-mc" href="/">Abbrechen</a><button type="submit" class="btn-mp">Speichern</button></div>
          </form>
        </div>""")
    except Exception as e:
        return HTMLResponse(page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>'))

@app.post("/edit-trip/{tc}")
async def edit_trip_save(tc: str, request: Request):
    try:
        form=await request.form()
        meals=",".join(form.getlist("meals_reimbursed"))
        conn=get_conn();cur=conn.cursor()
        cur.execute("""UPDATE trip_meta SET traveler_name=%s,colleagues=%s,departure_date=%s,return_date=%s,
            country_code=%s,flight_numbers=%s,nights_planned=%s,car_rental_info=%s,hotel_mode=%s,
            meals_reimbursed=%s,notes=%s WHERE trip_code=%s""",
            (form.get("traveler_name") or None,form.get("colleagues") or None,
             form.get("departure_date") or None,form.get("return_date") or None,
             form.get("country_code") or "DE",form.get("flight_numbers") or None,
             int(form.get("nights_planned") or 0),form.get("car_rental_info") or None,
             form.get("hotel_mode") or None,meals or None,form.get("notes") or None,tc))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


# =========================================================
# KI-BELEGANALYSE
# =========================================================

@app.get("/analyze-attachments", response_class=HTMLResponse)
async def analyze_attachments():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
        known_codes=[r[0] for r in cur.fetchall()]
        cur.execute("SELECT id,storage_key,original_filename FROM mail_attachments WHERE analysis_status IN ('ausstehend','neu') OR analysis_status IS NULL ORDER BY id")
        rows=cur.fetchall();cur.close()
        processed=0
        for row in rows:
            att_id,storage_key,filename=row
            if not storage_key or storage_key.startswith("S3-FEHLER"): continue
            try:
                await analyse_ki(att_id,storage_key,filename or "",conn,known_codes)
                processed+=1
            except Exception as e:
                cur2=conn.cursor()
                cur2.execute("UPDATE mail_attachments SET analysis_status=%s WHERE id=%s",(f"fehler: {str(e)[:80]}",att_id))
                conn.commit();cur2.close()
        conn.close()
        ki_info="via Mistral OCR 3 + Mistral Small (EU-gehostet, DSGVO-konform)" if MISTRAL_API_KEY else "Kein Mistral Key – bitte MISTRAL_API_KEY in Render setzen"
        return page_shell("Analyse",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ {processed} Anhänge analysiert</h2>
          <p class="sub" style="margin-bottom:16px">{ki_info}</p>
          <div class="acts"><a class="btn" href="/">Dashboard</a><a class="btn-l" href="/attachment-log">Anhang-Log</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# MAIL-IMPORT
# =========================================================

@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        s3=get_s3()
        mail=imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER,IMAP_PASS)
        mail.select("INBOX")
        _,data=mail.search(None,"ALL")
        ids=data[0].split()[-20:]
        conn=get_conn();cur=conn.cursor()
        imported=skipped=att_count=0
        for i in ids:
            uid=i.decode()
            cur.execute("SELECT id FROM mail_messages WHERE mail_uid=%s",(uid,))
            if cur.fetchone(): skipped+=1; continue
            _,msg_data=mail.fetch(i,"(RFC822)")
            msg=email.message_from_bytes(msg_data[0][1])
            subject=decode_mime_header(msg.get("Subject",""))
            sender=decode_mime_header(msg.get("From",""))
            body=""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type()=="text/plain" and "attachment" not in str(part.get("Content-Disposition") or "").lower():
                        pl=part.get_payload(decode=True)
                        if pl: body=pl.decode(errors="ignore"); break
            else:
                pl=msg.get_payload(decode=True)
                if pl: body=pl.decode(errors="ignore")
            full=subject+"\n"+body
            code=extract_trip_code(full)
            cur.execute("INSERT INTO mail_messages (mail_uid,sender,subject,body,trip_code,detected_type) VALUES (%s,%s,%s,%s,%s,%s)",
                        (uid,sender,subject,body,code,detect_mail_type(full)))
            if code:
                cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(code,))
            if msg.is_multipart():
                for part in msg.walk():
                    fn=part.get_filename()
                    cd=str(part.get("Content-Disposition") or "")
                    if not fn and "attachment" not in cd.lower(): continue
                    if fn:
                        decoded_fn=decode_mime_header(fn)
                    else:
                        ext_map={"application/pdf":".pdf","image/jpeg":".jpg","image/png":".png","image/webp":".webp","text/calendar":".ics"}
                        decoded_fn="attachment"+ext_map.get(part.get_content_type(),".bin")
                    pl=part.get_payload(decode=True)
                    if not pl: continue
                    safe_fn=sanitize_filename(decoded_fn)
                    storage_key=f"mail_attachments/{uid}_{safe_fn}"
                    try:
                        s3.put_object(Bucket=S3_BUCKET,Key=storage_key,Body=pl,ContentType=part.get_content_type() or "application/octet-stream")
                    except Exception as s3e:
                        storage_key=f"S3-FEHLER: {s3e}"
                    cur.execute("""INSERT INTO mail_attachments
                        (mail_uid,trip_code,original_filename,saved_filename,content_type,file_path,
                         detected_type,analysis_status,storage_key,confidence,review_flag)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (uid,code,safe_fn,f"{uid}_{safe_fn}",part.get_content_type(),storage_key,
                         detect_attachment_type(safe_fn,subject,body),"ausstehend",storage_key,"niedrig","pruefen"))
                    att_count+=1
            imported+=1
        conn.commit();cur.close();conn.close();mail.logout()
        return page_shell("Mails",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ Mailabruf erfolgreich</h2>
          <p style="margin-bottom:16px"><b>Importiert:</b> {imported} &nbsp;|&nbsp; <b>Übersprungen:</b> {skipped} &nbsp;|&nbsp; <b>Anhänge:</b> {att_count}</p>
          <div class="acts"><a class="btn" href="/">Dashboard</a><a class="btn-l" href="/analyze-attachments">Belege analysieren (KI)</a><a class="btn-l" href="/attachment-log">Anhang-Log</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler Mailabruf</h2><p>{e}</p></div>')


# =========================================================
# LOGS / DETAIL / ABRECHNUNG
# =========================================================

@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT trip_code,original_filename,detected_type,detected_amount,detected_amount_eur,
            detected_currency,detected_date,detected_vendor,analysis_status,confidence,review_flag,ki_bemerkung
            FROM mail_attachments ORDER BY id DESC LIMIT 100""")
        rows=cur.fetchall();cur.close();conn.close()
        def b(s,good="ok"):
            if s==good: return f'<span class="bdg bdg-ok">{s}</span>'
            if s in ("niedrig","pruefen","mittel"): return f'<span class="bdg bdg-w">{s}</span>'
            return f'<span class="bdg bdg-e">{s}</span>'
        html="".join(f"""<tr><td class="cc">{r[0] or ''}</td><td>{r[1] or ''}</td><td>{r[2] or ''}</td>
            <td>{r[3] or ''}</td><td><b>{r[4] or ''}</b></td><td>{r[5] or ''}</td>
            <td>{r[6] or ''}</td><td>{r[7] or ''}</td>
            <td>{b(r[8] or '')}</td><td>{b(r[9] or '',"hoch")}</td><td>{b(r[10] or '')}</td>
            <td style="font-size:11px;color:var(--t300)">{r[11] or ''}</td></tr>""" for r in rows)
        return page_shell("Anhang-Log",f"""
        <div class="page-card">
          <h2>Anhang-Log mit KI-Analyse v{APP_VERSION}</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a><a class="btn" href="/analyze-attachments">Erneut analysieren</a></div>
          <div style="overflow-x:auto"><table>
            <tr><th>Code</th><th>Datei</th><th>Typ</th><th>Betrag</th><th>EUR</th><th>Währung</th>
                <th>Datum</th><th>Anbieter</th><th>Status</th><th>Konfidenz</th><th>Review</th><th>KI-Notiz</th></tr>
            {html}</table></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT sender,subject,trip_code,detected_type FROM mail_messages ORDER BY id DESC LIMIT 50")
        rows=cur.fetchall();cur.close();conn.close()
        html="".join(f"<tr><td>{r[0] or ''}</td><td>{r[1] or ''}</td><td class='cc'>{r[2] or ''}</td><td>{r[3] or ''}</td></tr>" for r in rows)
        return page_shell("Mail-Log",f"""
        <div class="page-card"><h2>Mail-Log</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a></div>
          <table><tr><th>Von</th><th>Betreff</th><th>Code</th><th>Typ</th></tr>{html}</table>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/trip/{tc}", response_class=HTMLResponse)
def trip_detail(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT original_filename,detected_type,detected_amount_eur,detected_currency,
            detected_date,detected_vendor,analysis_status,confidence,ki_bemerkung
            FROM mail_attachments WHERE trip_code=%s ORDER BY id DESC""",(tc,))
        atts=cur.fetchall();cur.close();conn.close()
        rows="".join(f"""<tr><td>{a[0] or ''}</td><td>{a[1] or ''}</td>
            <td style="font-family:'DM Mono',monospace"><b>{a[2] or ''}</b> {a[3] or ''}</td>
            <td>{a[4] or ''}</td><td>{a[5] or ''}</td>
            <td><span class="bdg {"bdg-ok" if a[6]=="ok" else "bdg-w"}">{a[6] or ''}</span></td>
            <td><span class="bdg {"bdg-ok" if a[7]=="hoch" else "bdg-w"}">{a[7] or ''}</span></td>
            <td style="font-size:11px;color:var(--t300)">{a[8] or ''}</td></tr>""" for a in atts)
        return page_shell(f"Detail {tc}",f"""
        <div class="page-card"><h2>Reise {tc}</h2>
          <div class="acts"><a class="btn" href="/report/{tc}">Abrechnung</a><a class="btn-l" href="/edit-trip/{tc}">Bearbeiten</a><a class="btn-l" href="/">Zurück</a></div>
          <div style="overflow-x:auto"><table>
            <tr><th>Datei</th><th>Typ</th><th>Betrag EUR</th><th>Datum</th><th>Anbieter</th><th>Status</th><th>Konfidenz</th><th>KI-Notiz</th></tr>
            {rows or '<tr><td colspan="8">Keine Anhänge</td></tr>'}
          </table></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/report/{tc}", response_class=HTMLResponse)
def report(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT traveler_name,departure_date,return_date,country_code,meals_reimbursed,flight_numbers,colleagues,notes FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        if not meta: return HTMLResponse("Nicht gefunden",404)
        traveler,dep,ret,cc,meals_reimb,fns,colleagues,notes=meta
        cur.execute("SELECT original_filename,detected_type,detected_amount,detected_amount_eur,detected_currency,detected_date,detected_vendor,analysis_status FROM mail_attachments WHERE trip_code=%s ORDER BY id",(tc,))
        atts=cur.fetchall();cur.close();conn.close()
        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
        meals=[m.strip() for m in (meals_reimb or "").split(",") if m.strip()]
        vma_total=0.0; vma_rows=""
        if days>0:
            tag_list=([("Anreisetag","partial")]+[("Reisetag","full")]*max(0,days-2)+[("Abreisetag","partial")]) if days>1 else [("Eintägig","partial")]
            for i,(lbl,dtype) in enumerate(tag_list):
                ml=meals if(dtype=="partial" and i==len(tag_list)-1) else []
                v=get_vma(cc or "DE",dtype,ml); vma_total+=v
                vma_rows+=f"<tr><td>{lbl}</td><td>{cc or 'DE'}</td><td>{', '.join(ml) or '–'}</td><td>{v:.2f} €</td></tr>"
        beleg_sum=0.0; beleg_rows=""
        for a in atts:
            fn,dt,amt,amt_eur,curr,d,vendor,stat=a
            if not amt_eur: continue
            try: beleg_sum+=float(amt_eur.replace(".","").replace(",","."))
            except: pass
            beleg_rows+=f"<tr><td>{fn}</td><td>{dt or '–'}</td><td>{vendor or '–'}</td><td>{d or '–'}</td><td>{amt or '–'} {curr or ''}</td><td><b>{amt_eur} €</b></td></tr>"
        gesamt=beleg_sum+vma_total
        return page_shell(f"Abrechnung {tc}",f"""
        <div class="page-card" style="max-width:900px">
          <h2>Reisekostenabrechnung – {tc}</h2>
          <table style="width:auto;border:none;margin-bottom:20px">
            <tr style="border:none"><td style="border:none;padding:3px 12px 3px 0;font-weight:500">Reisender:</td><td style="border:none">{traveler or '–'}</td></tr>
            <tr style="border:none"><td style="border:none;padding:3px 12px 3px 0;font-weight:500">Zeitraum:</td><td style="border:none">{dep or '–'} – {ret or '–'} ({days} Tage)</td></tr>
            <tr style="border:none"><td style="border:none;padding:3px 12px 3px 0;font-weight:500">Land:</td><td style="border:none">{cc or 'DE'}</td></tr>
            <tr style="border:none"><td style="border:none;padding:3px 12px 3px 0;font-weight:500">Flüge:</td><td style="border:none">{fns or '–'}</td></tr>
          </table>
          <h3 style="margin-bottom:10px;color:var(--t700)">Belege</h3>
          <table>
            <tr><th>Datei</th><th>Typ</th><th>Anbieter</th><th>Datum</th><th>Betrag orig.</th><th>Betrag EUR</th></tr>
            {beleg_rows or '<tr><td colspan="6">Keine analysierten Belege</td></tr>'}
            <tr><td colspan="5"><b>Summe Belege</b></td><td><b>{beleg_sum:.2f} €</b></td></tr>
          </table>
          <h3 style="margin:20px 0 10px;color:var(--t700)">Verpflegungsmehraufwand §9 EStG</h3>
          <table>
            <tr><th>Tag</th><th>Land</th><th>Mahlzeiten-Abzug</th><th>VMA</th></tr>
            {vma_rows or '<tr><td colspan="4">Keine Reisezeit erfasst</td></tr>'}
            <tr><td colspan="3"><b>Summe VMA</b></td><td><b>{vma_total:.2f} €</b></td></tr>
          </table>
          <div style="margin-top:20px;padding:16px;background:var(--b50);border-radius:var(--r);border:1px solid var(--b100)">
            <span style="font-size:1.15rem;font-weight:600">Gesamtbetrag: {gesamt:,.2f} €</span>
            <span class="sub" style="margin-left:12px">Belege {beleg_sum:.2f} € + VMA {vma_total:.2f} €</span>
          </div>
          <div style="margin-top:16px"><a class="btn-l" href="/">Zurück</a>
            <span class="sub" style="margin-left:12px">PDF-Export kommt in v6.2</span></div>
        </div>""",active_tab="done")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/check-flights/{tc}", response_class=HTMLResponse)
async def check_flights(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT flight_numbers,departure_date FROM trip_meta WHERE trip_code=%s",(tc,))
        row=cur.fetchone()
        if not row or not row[0]:
            cur.close();conn.close()
            return page_shell("Flugprüfung",f'<div class="page-card"><h2>Keine Flugnummern für {tc}</h2><a class="btn-l" href="/edit-trip/{tc}">Bearbeiten</a></div>')
        fns=[f.strip() for f in (row[0] or "").split(",") if f.strip()]
        dep_date=str(row[1]) if row[1] else str(date.today())
        results_html=""
        for fn in fns:
            si={"source":"–","status":"kein Amadeus Key","delay_min":None}
            if AMADEUS_CLIENT_ID:
                try:
                    async with httpx.AsyncClient(timeout=8) as cl:
                        tr=await cl.post("https://test.api.amadeus.com/v1/security/oauth2/token",
                            data={"grant_type":"client_credentials","client_id":AMADEUS_CLIENT_ID,"client_secret":AMADEUS_CLIENT_SECRET})
                        token=tr.json().get("access_token","")
                        carrier=fn[:2].upper(); num=re.sub(r"[^0-9]","",fn)
                        fr=await cl.get("https://test.api.amadeus.com/v2/schedule/flights",
                            headers={"Authorization":f"Bearer {token}"},
                            params={"carrierCode":carrier,"flightNumber":num,"scheduledDepartureDate":dep_date})
                    if fr.status_code==200:
                        flights=fr.json().get("data",[])
                        if flights:
                            ds=flights[0].get("flightPoints",[{}])[0].get("departure",{}).get("timings",[{}])[0].get("delays",[{}])
                            mins=int(re.sub(r"[^0-9]","",(ds[0].get("duration","PT0M") if ds else "PT0M")) or 0)
                            si={"source":"Amadeus","status":"verspätet" if mins>15 else "pünktlich","delay_min":mins}
                except Exception as e:
                    si={"source":"Amadeus","status":f"Fehler: {e}","delay_min":None}
            alert="delay" if (si.get("delay_min") or 0)>15 else "ok"
            cur.execute("INSERT INTO flight_alerts (trip_code,flight_number,flight_date,alert_type,message,source,delay_min) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (tc,fn,dep_date,alert,si.get("status","–"),si.get("source","–"),si.get("delay_min")))
            cls="bdg-e" if alert=="delay" else "bdg-ok"
            results_html+=f"<tr><td class='cc'>{fn}</td><td>{dep_date}</td><td><span class='bdg {cls}'>{si.get('status','–')}</span></td><td>{si.get('source','–')}</td><td>{si.get('delay_min','–')}</td></tr>"
        conn.commit();cur.close();conn.close()
        return page_shell("Flugprüfung",f"""
        <div class="page-card"><h2>Flugstatus – {tc}</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a></div>
          <table><tr><th>Flug</th><th>Datum</th><th>Status</th><th>Quelle</th><th>Verspätung (Min.)</th></tr>{results_html}</table>
        </div>""",active_tab="active")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/reset-mail-log")
def reset_mail_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
        conn.commit();cur.close();conn.close()
        return {"status":"ok","version":APP_VERSION}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}

@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("INSERT INTO trip_meta (trip_code,hotel_mode) VALUES (%s,%s) ON CONFLICT (trip_code) DO UPDATE SET hotel_mode=%s",(code,mode,mode))
        conn.commit();cur.close();conn.close()
        return {"status":"ok","code":code,"mode":mode}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}
