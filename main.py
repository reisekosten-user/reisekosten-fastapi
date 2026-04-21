# main.py v7.12 – Mail Fetch Test integriert

from __future__ import annotations

import json
import os
import re
import imaplib
import email
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from openai import OpenAI
from pydantic import BaseModel
from pypdf import PdfReader

from database import (
    attach_beleg_to_event,
    create_event,
    create_mitarbeiter,
    create_reise,
    db_ping,
    get_conn,
    get_event_detail,
    get_next_reise_code,
    get_reise_detail,
    init_db,
    insert_beleg,
    list_belege,
    list_mitarbeiter,
    list_reisen,
    search_mitarbeiter,
    update_event_status,
    update_mitarbeiter,
)

APP_VERSION = "7.12"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")

MAIL_HOST = os.getenv("MAIL_HOST")
MAIL_USER = os.getenv("MAIL_USER")
MAIL_PASS = os.getenv("MAIL_PASS")

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup():
    init_db()


class AnalyzeTextRequest(BaseModel):
    text: str


# ---------------- PDF ----------------

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join([p.extract_text() or "" for p in reader.pages])


# ---------------- ANONYMIZE ----------------

def anonymize(text: str) -> str:
    text = re.sub(r"\b[\w\.-]+@[\w\.-]+\.\w+\b", "abc@123.com", text)
    text = re.sub(r"(?i)(mr|herr|frau)\s+[A-Za-zäöüÄÖÜß]+", r"\1 Max Mustermann", text)
    text = re.sub(r"[A-Z][a-z]+\s+[A-Z][a-z]+", "Max Mustermann", text)
    return text


# ---------------- AI ----------------

def call_openai(prompt: str) -> dict:
    client = OpenAI(api_key=OPENAI_API_KEY)
    res = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "Gib nur JSON zurück"},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    return json.loads(res.choices[0].message.content)


def build_prompt(text: str):
    return f"""
Analysiere Beleg und gib JSON:
Belegdatum, Art, Buchungsnummer, Name, Segmente, Kosten

{text[:80000]}
"""


def analyze(text: str):
    anon = anonymize(text)
    result = call_openai(build_prompt(anon))
    return {
        "status": "ok",
        "result": result,
        "anonymized_preview": anon[:2000],
        "version": APP_VERSION,
    }


# ---------------- MAIL FETCH ----------------

@app.get("/mail/test")
def mail_test():
    if not MAIL_HOST:
        return {"status": "error", "detail": "MAIL config fehlt"}

    try:
        mail = imaplib.IMAP4_SSL(MAIL_HOST)
        mail.login(MAIL_USER, MAIL_PASS)
        mail.select("inbox")

        result, data = mail.search(None, "ALL")
        ids = data[0].split()

        latest_ids = ids[-3:]  # nur letzte 3 Mails
        mails = []

        for i in latest_ids:
            res, msg_data = mail.fetch(i, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject = msg.get("Subject", "")
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode(errors="ignore")
                        break
            else:
                body = msg.get_payload(decode=True).decode(errors="ignore")

            mails.append({
                "subject": subject,
                "preview": body[:500]
            })

        return {"status": "ok", "mails": mails}

    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ---------------- API ----------------

@app.get("/")
def root():
    return {"status": "ok", "version": APP_VERSION}


@app.post("/analyze/text")
def analyze_text(req: AnalyzeTextRequest):
    return analyze(req.text)


@app.post("/analyze/file")
async def analyze_file(file: UploadFile = File(...)):
    content = await file.read()
    if file.filename.endswith(".pdf"):
        text = extract_text_from_pdf(content)
    else:
        text = content.decode("utf-8", errors="ignore")
    return analyze(text)
