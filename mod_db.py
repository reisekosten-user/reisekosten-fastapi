"""
mod_db.py – Datenbankverbindung, Schema, Hilfsfunktionen
Stabil / abgeschlossen
"""
from __future__ import annotations
import os, re
from datetime import date, datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "")

def get_db():
    """PostgreSQL wenn DATABASE_URL gesetzt, sonst SQLite lokal."""
    if DATABASE_URL:
        import psycopg2
        return psycopg2.connect(DATABASE_URL)
    else:
        import sqlite3
        conn = sqlite3.connect("reisekosten.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

def is_postgres() -> bool:
    return bool(DATABASE_URL)

def ph() -> str:
    """Placeholder: %s für PostgreSQL, ? für SQLite."""
    return "%s" if is_postgres() else "?"

def fmt_date(d) -> str:
    if not d: return "–"
    if isinstance(d, date): return d.strftime("%d.%m.%Y")
    s = str(d)[:10]
    try: return date.fromisoformat(s).strftime("%d.%m.%Y")
    except: return s

def next_reise_code(cur) -> str:
    """Generiert nächsten Reisecode YY-NNN."""
    year = str(date.today().year)[-2:]
    P = ph()
    cur.execute(f"SELECT code FROM reisen WHERE code LIKE {P} ORDER BY code DESC LIMIT 1",
                (f"{year}-%",))
    row = cur.fetchone()
    if row:
        last = row[0] if isinstance(row, tuple) else row["code"]
        m = re.match(r"\d{2}-(\d{3})", last)
        num = int(m.group(1)) + 1 if m else 1
    else:
        num = 1
    return f"{year}-{num:03d}"

def get_schema() -> list[str]:
    """Schema für PostgreSQL und SQLite."""
    if is_postgres():
        return [
            """CREATE TABLE IF NOT EXISTS mitarbeiter (
                kuerzel     TEXT PRIMARY KEY,
                klarname    TEXT NOT NULL,
                aktiv       BOOLEAN DEFAULT TRUE,
                erstellt    TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS reisen (
                code        TEXT PRIMARY KEY,
                titel       TEXT NOT NULL,
                abreise     DATE NOT NULL,
                rueckkehr   DATE NOT NULL,
                notiz       TEXT,
                erstellt    TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS reise_mitarbeiter (
                reise_code  TEXT NOT NULL,
                kuerzel     TEXT NOT NULL,
                PRIMARY KEY (reise_code, kuerzel),
                CONSTRAINT fk_rm_reise FOREIGN KEY (reise_code)
                    REFERENCES reisen(code) ON DELETE CASCADE,
                CONSTRAINT fk_rm_ma FOREIGN KEY (kuerzel)
                    REFERENCES mitarbeiter(kuerzel) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS reise_laender (
                id          SERIAL PRIMARY KEY,
                reise_code  TEXT NOT NULL,
                datum_von   DATE NOT NULL,
                datum_bis   DATE NOT NULL,
                land_code   TEXT NOT NULL,
                land_name   TEXT NOT NULL,
                vma_voll    NUMERIC(6,2),
                vma_halb    NUMERIC(6,2),
                CONSTRAINT fk_rl_reise FOREIGN KEY (reise_code)
                    REFERENCES reisen(code) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS vma_tage (
                id              SERIAL PRIMARY KEY,
                reise_code      TEXT NOT NULL REFERENCES reisen(code) ON DELETE CASCADE,
                datum           DATE NOT NULL,
                land_code       TEXT NOT NULL,
                land_name       TEXT NOT NULL,
                vma_satz_voll   NUMERIC(6,2) NOT NULL,
                vma_satz_halb   NUMERIC(6,2) NOT NULL,
                ist_halber_satz BOOLEAN DEFAULT FALSE,
                fruehstueck     BOOLEAN DEFAULT FALSE,
                mittagessen     BOOLEAN DEFAULT FALSE,
                abendessen      BOOLEAN DEFAULT FALSE,
                vma_brutto      NUMERIC(6,2),
                vma_netto       NUMERIC(6,2),
                quelle          TEXT DEFAULT 'auto',
                notiz           TEXT,
                UNIQUE(reise_code, datum)
            )""",
            """CREATE TABLE IF NOT EXISTS belege (
                id                    SERIAL PRIMARY KEY,
                reise_code            TEXT REFERENCES reisen(code) ON DELETE SET NULL,
                dateiname             TEXT,
                s3_original           TEXT,
                s3_anon               TEXT,
                s3_analyse            TEXT,
                rohtext               TEXT,
                anon_text             TEXT,
                ki_json               TEXT,
                pflichtfelder_ok      BOOLEAN DEFAULT FALSE,
                fehlende_felder       TEXT,
                belegdatum            DATE,
                belegart              TEXT,
                transportart          TEXT,
                transportart_freitext TEXT,
                anbieter              TEXT,
                rechnungsnummer       TEXT,
                buchungscode          TEXT,
                reisender             TEXT,
                land_beleg            TEXT,
                betrag_brutto         NUMERIC(10,2),
                betrag_netto          NUMERIC(10,2),
                betrag_mwst           NUMERIC(10,2),
                waehrung              TEXT DEFAULT 'EUR',
                event_datum_von       DATE,
                event_datum_bis       DATE,
                event_ort_von         TEXT,
                event_ort_bis         TEXT,
                hotel_name            TEXT,
                hotel_checkin_datum   DATE,
                hotel_checkin_zeit    TEXT,
                hotel_checkout_datum  DATE,
                hotel_checkout_zeit   TEXT,
                hotel_naechte         INTEGER,
                tanken_kraftstoff     TEXT,
                tanken_menge          NUMERIC(8,3),
                tanken_einheit        TEXT,
                tanken_preis_einheit  NUMERIC(8,3),
                tanken_tankstelle     TEXT,
                tanken_kennzeichen    TEXT,
                status                TEXT DEFAULT 'neu',
                fehler                TEXT,
                erstellt              TIMESTAMP DEFAULT NOW()
            )""",
        ]
    else:
        return [
            """CREATE TABLE IF NOT EXISTS mitarbeiter (
                kuerzel     TEXT PRIMARY KEY,
                klarname    TEXT NOT NULL,
                aktiv       INTEGER DEFAULT 1,
                erstellt    TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS reisen (
                code        TEXT PRIMARY KEY,
                titel       TEXT NOT NULL,
                abreise     TEXT NOT NULL,
                rueckkehr   TEXT NOT NULL,
                notiz       TEXT,
                erstellt    TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS reise_mitarbeiter (
                reise_code  TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel     TEXT REFERENCES mitarbeiter(kuerzel) ON DELETE CASCADE,
                PRIMARY KEY (reise_code, kuerzel)
            )""",
            """CREATE TABLE IF NOT EXISTS reise_laender (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code  TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                datum_von   TEXT NOT NULL,
                datum_bis   TEXT NOT NULL,
                land_code   TEXT NOT NULL,
                land_name   TEXT NOT NULL,
                vma_voll    REAL,
                vma_halb    REAL
            )""",
            """CREATE TABLE IF NOT EXISTS vma_tage (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code      TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                datum           TEXT NOT NULL,
                land_code       TEXT NOT NULL,
                land_name       TEXT NOT NULL,
                vma_satz_voll   REAL NOT NULL,
                vma_satz_halb   REAL NOT NULL,
                ist_halber_satz INTEGER DEFAULT 0,
                fruehstueck     INTEGER DEFAULT 0,
                mittagessen     INTEGER DEFAULT 0,
                abendessen      INTEGER DEFAULT 0,
                vma_brutto      REAL,
                vma_netto       REAL,
                quelle          TEXT DEFAULT 'auto',
                notiz           TEXT,
                UNIQUE(reise_code, datum)
            )""",
            """CREATE TABLE IF NOT EXISTS belege (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code            TEXT REFERENCES reisen(code) ON DELETE SET NULL,
                dateiname             TEXT,
                s3_original           TEXT,
                s3_anon               TEXT,
                s3_analyse            TEXT,
                rohtext               TEXT,
                anon_text             TEXT,
                ki_json               TEXT,
                pflichtfelder_ok      INTEGER DEFAULT 0,
                fehlende_felder       TEXT,
                belegdatum            TEXT,
                belegart              TEXT,
                transportart          TEXT,
                transportart_freitext TEXT,
                anbieter              TEXT,
                rechnungsnummer       TEXT,
                buchungscode          TEXT,
                reisender             TEXT,
                land_beleg            TEXT,
                betrag_brutto         REAL,
                betrag_netto          REAL,
                betrag_mwst           REAL,
                waehrung              TEXT DEFAULT 'EUR',
                event_datum_von       TEXT,
                event_datum_bis       TEXT,
                event_ort_von         TEXT,
                event_ort_bis         TEXT,
                hotel_name            TEXT,
                hotel_checkin_datum   TEXT,
                hotel_checkin_zeit    TEXT,
                hotel_checkout_datum  TEXT,
                hotel_checkout_zeit   TEXT,
                hotel_naechte         INTEGER,
                tanken_kraftstoff     TEXT,
                tanken_menge          REAL,
                tanken_einheit        TEXT,
                tanken_preis_einheit  REAL,
                tanken_tankstelle     TEXT,
                tanken_kennzeichen    TEXT,
                status                TEXT DEFAULT 'neu',
                fehler                TEXT,
                erstellt              TEXT DEFAULT (datetime('now'))
            )""",
        ]
