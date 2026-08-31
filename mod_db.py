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

def next_reise_code(cur, abreise_datum=None) -> str:
    """
    Generiert nächsten Reisecode YY-NNN. YY ist das Jahr der ABREISE (nicht das
    heutige Erstellungsdatum) – legt man z.B. Ende 2026 schon eine Reise für
    Anfang 2027 an, lautet der Code 27-001, nicht 26-xxx. Ohne übergebenes
    Abreisedatum (z.B. für die Formular-Vorschau) wird das heutige Jahr genutzt.
    Die laufende Nummer zählt pro Jahrgang hoch.
    """
    if abreise_datum:
        if isinstance(abreise_datum, str):
            try:
                jahr = date.fromisoformat(abreise_datum[:10]).year
            except ValueError:
                jahr = date.today().year
        else:
            jahr = abreise_datum.year
    else:
        jahr = date.today().year
    year = str(jahr)[-2:]
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
                email       TEXT,
                rolle       TEXT DEFAULT 'reisender',
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
                kurs_eur              NUMERIC(10,4),
                betrag_eur            NUMERIC(10,2),
                kurs_datum            DATE,
                kurs_quelle           TEXT,
                status                TEXT DEFAULT 'neu',
                fehler                TEXT,
                erstellt              TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS vma_saetze (
                id          SERIAL PRIMARY KEY,
                land_code   TEXT NOT NULL,
                ort         TEXT,
                land_name   TEXT NOT NULL,
                vma_voll    NUMERIC(6,2) NOT NULL,
                vma_halb    NUMERIC(6,2) NOT NULL,
                uebernachtung NUMERIC(6,2),
                gueltig_ab  TEXT,
                gueltig_bis TEXT,
                quelle      TEXT DEFAULT 'pauschbetrag-api',
                aktualisiert TIMESTAMP DEFAULT NOW(),
                UNIQUE(land_code, ort)
            )""",
            """CREATE TABLE IF NOT EXISTS termine (
                id            SERIAL PRIMARY KEY,
                reise_code    TEXT NOT NULL REFERENCES reisen(code) ON DELETE CASCADE,
                datum         DATE NOT NULL,
                uhrzeit_von   TEXT,
                uhrzeit_bis   TEXT,
                titel         TEXT NOT NULL,
                typ           TEXT DEFAULT 'termin',
                notiz         TEXT,
                erstellt      TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS beleg_gruppen (
                id          SERIAL PRIMARY KEY,
                erstellt_am TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS reise_zugang (
                id                SERIAL PRIMARY KEY,
                reise_code        TEXT NOT NULL REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel           TEXT NOT NULL,
                token             TEXT NOT NULL UNIQUE,
                erstellt_am       TIMESTAMP DEFAULT NOW(),
                email_gesendet_am TIMESTAMP,
                UNIQUE(reise_code, kuerzel)
            )""",
            """CREATE TABLE IF NOT EXISTS reisetage_person (
                id              SERIAL PRIMARY KEY,
                reise_code      TEXT NOT NULL REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel         TEXT NOT NULL,
                datum           DATE NOT NULL,
                land_code       TEXT,
                land_name       TEXT,
                vma_satz_voll   NUMERIC(6,2),
                vma_satz_halb   NUMERIC(6,2),
                ist_halber_satz BOOLEAN DEFAULT FALSE,
                fruehstueck     BOOLEAN DEFAULT FALSE,
                mittagessen     BOOLEAN DEFAULT FALSE,
                abendessen      BOOLEAN DEFAULT FALSE,
                vma_netto       NUMERIC(6,2),
                reise_beginn    TEXT,
                reise_ende      TEXT,
                arbeit_beginn   TEXT,
                arbeit_ende     TEXT,
                notiz           TEXT,
                UNIQUE(reise_code, kuerzel, datum)
            )""",
            """CREATE TABLE IF NOT EXISTS alert_konfiguration (
                id                  SERIAL PRIMARY KEY,
                intervall_24h_min   INTEGER DEFAULT 60,
                intervall_8h_min    INTEGER DEFAULT 10,
                intervall_4h_min    INTEGER DEFAULT 30,
                intervall_2h_min    INTEGER DEFAULT 1,
                intervall_1h_min    INTEGER DEFAULT 15,
                aktualisiert_am     TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS flug_status (
                id                  SERIAL PRIMARY KEY,
                beleg_id            INTEGER NOT NULL REFERENCES belege(id) ON DELETE CASCADE,
                segment_index       INTEGER NOT NULL,
                transport_typ       TEXT,
                transport_nummer    TEXT,
                von_ort             TEXT,
                nach_ort            TEXT,
                abreise_datum       DATE,
                abreise_zeit        TEXT,
                letzter_check_am    TIMESTAMP,
                status              TEXT,
                verspaetung_minuten INTEGER,
                gate                TEXT,
                terminal            TEXT,
                rohdaten            TEXT,
                alert_gesendet_am   TIMESTAMP,
                UNIQUE(beleg_id, segment_index)
            )""",
        ]
    else:
        return [
            """CREATE TABLE IF NOT EXISTS mitarbeiter (
                kuerzel     TEXT PRIMARY KEY,
                klarname    TEXT NOT NULL,
                email       TEXT,
                rolle       TEXT DEFAULT 'reisender',
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
                kurs_eur              REAL,
                betrag_eur            REAL,
                kurs_datum            TEXT,
                kurs_quelle           TEXT,
                status                TEXT DEFAULT 'neu',
                fehler                TEXT,
                erstellt              TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS vma_saetze (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                land_code   TEXT NOT NULL,
                ort         TEXT,
                land_name   TEXT NOT NULL,
                vma_voll    REAL NOT NULL,
                vma_halb    REAL NOT NULL,
                uebernachtung REAL,
                gueltig_ab  TEXT,
                gueltig_bis TEXT,
                quelle      TEXT DEFAULT 'pauschbetrag-api',
                aktualisiert TEXT DEFAULT (datetime('now')),
                UNIQUE(land_code, ort)
            )""",
            """CREATE TABLE IF NOT EXISTS termine (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code    TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                datum         TEXT NOT NULL,
                uhrzeit_von   TEXT,
                uhrzeit_bis   TEXT,
                titel         TEXT NOT NULL,
                typ           TEXT DEFAULT 'termin',
                notiz         TEXT,
                erstellt      TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS beleg_gruppen (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                erstellt_am TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS reise_zugang (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code        TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel           TEXT NOT NULL,
                token             TEXT NOT NULL UNIQUE,
                erstellt_am       TEXT DEFAULT (datetime('now')),
                email_gesendet_am TEXT,
                UNIQUE(reise_code, kuerzel)
            )""",
            """CREATE TABLE IF NOT EXISTS reisetage_person (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code      TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel         TEXT NOT NULL,
                datum           TEXT NOT NULL,
                land_code       TEXT,
                land_name       TEXT,
                vma_satz_voll   REAL,
                vma_satz_halb   REAL,
                ist_halber_satz INTEGER DEFAULT 0,
                fruehstueck     INTEGER DEFAULT 0,
                mittagessen     INTEGER DEFAULT 0,
                abendessen      INTEGER DEFAULT 0,
                vma_netto       REAL,
                reise_beginn    TEXT,
                reise_ende      TEXT,
                arbeit_beginn   TEXT,
                arbeit_ende     TEXT,
                notiz           TEXT,
                UNIQUE(reise_code, kuerzel, datum)
            )""",
            """CREATE TABLE IF NOT EXISTS alert_konfiguration (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                intervall_24h_min   INTEGER DEFAULT 60,
                intervall_8h_min    INTEGER DEFAULT 10,
                intervall_4h_min    INTEGER DEFAULT 30,
                intervall_2h_min    INTEGER DEFAULT 1,
                intervall_1h_min    INTEGER DEFAULT 15,
                aktualisiert_am     TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS flug_status (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                beleg_id            INTEGER REFERENCES belege(id) ON DELETE CASCADE,
                segment_index       INTEGER NOT NULL,
                transport_typ       TEXT,
                transport_nummer    TEXT,
                von_ort             TEXT,
                nach_ort            TEXT,
                abreise_datum       TEXT,
                abreise_zeit        TEXT,
                letzter_check_am    TEXT,
                status              TEXT,
                verspaetung_minuten INTEGER,
                gate                TEXT,
                terminal            TEXT,
                rohdaten            TEXT,
                alert_gesendet_am   TEXT,
                UNIQUE(beleg_id, segment_index)
            )""",
        ]

def get_migrations() -> list[str]:
    """
    Nachträgliche Schema-Änderungen für bestehende Datenbanken.
    Wird über /init ausgeführt. Jede Anweisung läuft einzeln ab –
    schlägt eine fehl (z.B. Spalte existiert schon), wird sie übersprungen.
    """
    return [
        "ALTER TABLE mitarbeiter ADD COLUMN email2 TEXT",
        "ALTER TABLE mitarbeiter ADD COLUMN email3 TEXT",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS reise_code TEXT",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS datum DATE",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS land_code TEXT",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS land_name TEXT",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS vma_satz_voll NUMERIC(6,2)",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS vma_satz_halb NUMERIC(6,2)",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS ist_halber_satz BOOLEAN DEFAULT FALSE",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS fruehstueck BOOLEAN DEFAULT FALSE",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS mittagessen BOOLEAN DEFAULT FALSE",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS abendessen BOOLEAN DEFAULT FALSE",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS vma_brutto NUMERIC(6,2)",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS vma_netto NUMERIC(6,2)",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS quelle TEXT DEFAULT 'auto'",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS notiz TEXT",
        "ALTER TABLE vma_tage ALTER COLUMN reise_id DROP NOT NULL",
        "ALTER TABLE reise_laender ADD COLUMN IF NOT EXISTS ort TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS event_zeit TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS zahlungsart TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS geprueft BOOLEAN DEFAULT FALSE",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS pruef_vermerk TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS geprueft_von TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS geprueft_am TIMESTAMP",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS dms_versendet_am TIMESTAMP",
        "ALTER TABLE mitarbeiter ADD COLUMN IF NOT EXISTS passwort_hash TEXT",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS trennungspauschale NUMERIC(6,2) DEFAULT 0",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS trennungspauschale_quelle TEXT DEFAULT 'auto'",
        "ALTER TABLE vma_tage ADD COLUMN IF NOT EXISTS tatsaechliche_uhrzeit TEXT",
        "ALTER TABLE mitarbeiter ADD COLUMN IF NOT EXISTS kreditkarten_typ TEXT DEFAULT 'privat'",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS kreditkarte_karte TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS betrag_eur_final NUMERIC(10,2)",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS nebenkosten_eur NUMERIC(10,2)",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS nebenkosten_beschreibung TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS verknuepft_mit_id INTEGER",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS beleg_gruppe_id INTEGER",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS ist_erechnung BOOLEAN DEFAULT FALSE",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS erechnung_format TEXT",
        "ALTER TABLE belege ADD COLUMN IF NOT EXISTS s3_erechnung_xml TEXT",
        "ALTER TABLE termine ADD COLUMN IF NOT EXISTS ort TEXT",
        "ALTER TABLE termine ADD COLUMN IF NOT EXISTS ansprechpartner TEXT",
        "ALTER TABLE termine ADD COLUMN IF NOT EXISTS telefon TEXT",
        "ALTER TABLE mitarbeiter ADD COLUMN IF NOT EXISTS ist_organisator BOOLEAN DEFAULT FALSE",
        "ALTER TABLE mitarbeiter ADD COLUMN IF NOT EXISTS ist_reisender BOOLEAN DEFAULT TRUE",
        "UPDATE mitarbeiter SET ist_organisator=TRUE WHERE rolle='organisator'",
        "UPDATE mitarbeiter SET ist_reisender=TRUE WHERE rolle='reisender' OR rolle IS NULL",
        """INSERT INTO alert_konfiguration (intervall_24h_min, intervall_8h_min, intervall_4h_min, intervall_2h_min)
           SELECT 60, 10, 5, 1 WHERE NOT EXISTS (SELECT 1 FROM alert_konfiguration)""",
        "ALTER TABLE alert_konfiguration ADD COLUMN IF NOT EXISTS intervall_1h_min INTEGER DEFAULT 15",
        "UPDATE alert_konfiguration SET intervall_24h_min=60 WHERE intervall_24h_min IS NULL",
        "UPDATE alert_konfiguration SET intervall_4h_min=30 WHERE intervall_4h_min=5",
        "UPDATE alert_konfiguration SET intervall_1h_min=15 WHERE intervall_1h_min IS NULL",
    ]

def repair_legacy_columns():
    """
    Entfernt NOT-NULL-Zwang von alten Spalten in vma_tage, die aus
    früheren App-Versionen stammen und vom aktuellen Code nicht mehr
    befüllt werden (z.B. 'reise_id', 'tag'). Ohne dies schlagen neue
    Einträge mit "null value in column ... violates not-null constraint" fehl.
    Bei SQLite passiert nichts (dort gibt es das Problem nicht).
    """
    if not is_postgres():
        return
    aktuelle_spalten = {
        "id", "reise_code", "datum", "land_code", "land_name",
        "vma_satz_voll", "vma_satz_halb", "ist_halber_satz",
        "fruehstueck", "mittagessen", "abendessen",
        "vma_brutto", "vma_netto", "quelle", "notiz",
    }
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='vma_tage' AND is_nullable='NO' AND column_default IS NULL
    """)
    alte_spalten = [row[0] for row in cur.fetchall() if row[0] not in aktuelle_spalten]
    for col in alte_spalten:
        try:
            cur.execute(f'ALTER TABLE vma_tage ALTER COLUMN "{col}" DROP NOT NULL')
            conn.commit()
        except Exception:
            conn.rollback()
    cur.close(); conn.close()


def migriere_verknuepfungen_zu_gruppen():
    """
    Wandelt alte 1:1-Verknüpfungen (verknuepft_mit_id) in das neue Gruppen-Modell
    (beleg_gruppe_id) um, das auch 3+ Belege gemeinsam gruppieren kann.
    Idempotent – kann gefahrlos mehrfach über /init laufen.
    """
    conn = get_db()
    cur = conn.cursor()
    P = ph()
    try:
        cur.execute("SELECT id, verknuepft_mit_id FROM belege "
                    "WHERE verknuepft_mit_id IS NOT NULL AND beleg_gruppe_id IS NULL")
        paare = cur.fetchall()
        for row in paare:
            bid = row[0] if isinstance(row, tuple) else row["id"]
            andere_id = row[1] if isinstance(row, tuple) else row["verknuepft_mit_id"]
            # Erneut prüfen (könnte durch vorherige Iteration schon gruppiert sein)
            cur.execute(f"SELECT beleg_gruppe_id FROM belege WHERE id={P}", (bid,))
            r2 = cur.fetchone()
            schon = (r2[0] if isinstance(r2, tuple) else r2["beleg_gruppe_id"]) if r2 else None
            if schon:
                continue
            if is_postgres():
                cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES RETURNING id")
                gid = cur.fetchone()[0]
            else:
                cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES")
                gid = cur.lastrowid
            cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gid, bid))
            cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gid, andere_id))
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close(); conn.close()
