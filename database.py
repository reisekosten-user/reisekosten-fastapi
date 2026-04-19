import os
from datetime import datetime

import psycopg

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ist nicht gesetzt.")

    url = DATABASE_URL
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"

    return psycopg.connect(url)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mitarbeiter (
                    id SERIAL PRIMARY KEY,
                    kuerzel TEXT NOT NULL UNIQUE,
                    vorname TEXT NOT NULL,
                    nachname TEXT NOT NULL,
                    geburtsdatum TEXT,
                    email TEXT,
                    aktiv BOOLEAN DEFAULT TRUE,
                    created_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS reisen (
                    id SERIAL PRIMARY KEY,
                    reise_jahr INTEGER NOT NULL,
                    reise_code TEXT NOT NULL UNIQUE,
                    reise_name TEXT NOT NULL,
                    startdatum TEXT,
                    enddatum TEXT,
                    anzahl_reisende INTEGER DEFAULT 1,
                    created_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS reise_reisende (
                    id SERIAL PRIMARY KEY,
                    reise_id INTEGER NOT NULL,
                    mitarbeiter_id INTEGER NOT NULL
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS belege (
                    id SERIAL PRIMARY KEY,
                    belegdatum TEXT,
                    art TEXT,
                    kosten TEXT,
                    waehrung TEXT,
                    created_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    reise_id INTEGER NOT NULL,
                    typ TEXT,
                    titel TEXT,
                    status TEXT DEFAULT 'planung',
                    created_at TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS event_belege (
                    id SERIAL PRIMARY KEY,
                    event_id INTEGER NOT NULL,
                    beleg_id INTEGER NOT NULL
                )
            """)
        conn.commit()


def db_ping():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            return {"ok": row[0]}


def create_mitarbeiter(data: dict) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mitarbeiter
                (kuerzel, vorname, nachname, geburtsdatum, email, aktiv, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                data.get("kuerzel"),
                data.get("vorname"),
                data.get("nachname"),
                data.get("geburtsdatum"),
                data.get("email"),
                data.get("aktiv", True),
                now_iso(),
            ))
            new_id = cur.fetchone()[0]
        conn.commit()
    return new_id


def update_mitarbeiter(mitarbeiter_id: int, data: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE mitarbeiter
                SET kuerzel=%s,
                    vorname=%s,
                    nachname=%s,
                    geburtsdatum=%s,
                    email=%s,
                    aktiv=%s
                WHERE id=%s
            """, (
                data.get("kuerzel"),
                data.get("vorname"),
                data.get("nachname"),
                data.get("geburtsdatum"),
                data.get("email"),
                data.get("aktiv", True),
                mitarbeiter_id,
            ))
        conn.commit()


def list_mitarbeiter(limit: int = 100):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, kuerzel, vorname, nachname, geburtsdatum, email, aktiv
                FROM mitarbeiter
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "kuerzel": r[1],
            "vorname": r[2],
            "nachname": r[3],
            "vollname": f"{r[2]} {r[3]}".strip(),
            "geburtsdatum": r[4],
            "email": r[5],
            "aktiv": r[6],
        }
        for r in rows
    ]


def search_mitarbeiter(query: str, limit: int = 10):
    q = f"%{(query or '').strip().lower()}%"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, kuerzel, vorname, nachname
                FROM mitarbeiter
                WHERE aktiv = TRUE
                  AND (
                    LOWER(kuerzel) LIKE %s
                    OR LOWER(vorname) LIKE %s
                    OR LOWER(nachname) LIKE %s
                    OR LOWER(vorname || ' ' || nachname) LIKE %s
                  )
                ORDER BY nachname ASC, vorname ASC
                LIMIT %s
            """, (q, q, q, q, limit))
            rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "kuerzel": r[1],
            "vollname": f"{r[2]} {r[3]}".strip(),
        }
        for r in rows
    ]


def get_next_reise_code(year: int) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reisen WHERE reise_jahr=%s", (year,))
            count = cur.fetchone()[0] + 1

    return {
        "reise_code": f"{str(year)[-2:]}-{str(count).zfill(3)}",
        "jahr": year,
        "laufende_nummer": count,
    }


def create_reise(data: dict) -> dict:
    info = get_next_reise_code(int(data["reise_jahr"]))
    code = info["reise_code"]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reisen
                (reise_jahr, reise_code, reise_name, startdatum, enddatum, anzahl_reisende, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                int(data["reise_jahr"]),
                code,
                data.get("reise_name"),
                data.get("startdatum"),
                data.get("enddatum"),
                int(data.get("anzahl_reisende", 1)),
                now_iso(),
            ))
            reise_id = cur.fetchone()[0]

            for mid in data.get("mitarbeiter_ids", []):
                cur.execute("""
                    INSERT INTO reise_reisende (reise_id, mitarbeiter_id)
                    VALUES (%s,%s)
                """, (reise_id, int(mid)))

        conn.commit()

    return {"reise_id": reise_id, "reise_code": code}


def list_reisen(limit: int = 100):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, reise_jahr, reise_code, reise_name, startdatum, enddatum, anzahl_reisende, created_at
                FROM reisen
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "reise_jahr": r[1],
            "reise_code": r[2],
            "reise_name": r[3],
            "startdatum": r[4],
            "enddatum": r[5],
            "anzahl_reisende": r[6],
            "created_at": r[7],
        }
        for r in rows
    ]


def create_event(data: dict) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO events (reise_id, typ, titel, status, created_at)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                int(data["reise_id"]),
                data.get("typ"),
                data.get("titel"),
                data.get("status", "planung"),
                now_iso(),
            ))
            event_id = cur.fetchone()[0]
        conn.commit()
    return event_id


def update_event_status(event_id: int, status: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE events SET status=%s WHERE id=%s", (status, event_id))
        conn.commit()


def insert_beleg(data: dict) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO belege
                (belegdatum, art, kosten, waehrung, created_at)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                data.get("belegdatum"),
                data.get("art"),
                data.get("kosten"),
                data.get("waehrung"),
                now_iso(),
            ))
            beleg_id = cur.fetchone()[0]
        conn.commit()
    return beleg_id


def list_belege():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, belegdatum, art, kosten, waehrung, created_at
                FROM belege
                ORDER BY id DESC
            """)
            rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "belegdatum": r[1],
            "art": r[2],
            "kosten": r[3],
            "waehrung": r[4],
            "created_at": r[5],
        }
        for r in rows
    ]


def attach_beleg_to_event(event_id: int, beleg_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO event_belege (event_id, beleg_id)
                VALUES (%s,%s)
            """, (event_id, beleg_id))
        conn.commit()


def get_reise_detail(reise_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, reise_jahr, reise_code, reise_name, startdatum, enddatum, anzahl_reisende, created_at
                FROM reisen
                WHERE id=%s
            """, (reise_id,))
            rr = cur.fetchone()

            cur.execute("""
                SELECT e.id, e.reise_id, e.typ, e.titel, e.status, e.created_at,
                       COALESCE(COUNT(eb.id), 0) AS beleg_anzahl
                FROM events e
                LEFT JOIN event_belege eb ON e.id = eb.event_id
                WHERE e.reise_id=%s
                GROUP BY e.id
                ORDER BY e.id ASC
            """, (reise_id,))
            events = cur.fetchall()

            cur.execute("""
                SELECT m.id, m.kuerzel, m.vorname, m.nachname
                FROM reise_reisende rr
                JOIN mitarbeiter m ON rr.mitarbeiter_id = m.id
                WHERE rr.reise_id=%s
                ORDER BY m.id ASC
            """, (reise_id,))
            reisende = cur.fetchall()

    reise = None
    if rr:
        reise = {
            "id": rr[0],
            "reise_jahr": rr[1],
            "reise_code": rr[2],
            "reise_name": rr[3],
            "startdatum": rr[4],
            "enddatum": rr[5],
            "anzahl_reisende": rr[6],
            "created_at": rr[7],
        }

    return {
        "reise": reise,
        "events": [
            {
                "id": e[0],
                "reise_id": e[1],
                "typ": e[2],
                "titel": e[3],
                "status": e[4],
                "created_at": e[5],
                "beleg_anzahl": e[6],
            }
            for e in events
        ],
        "reisende": [
            {
                "id": r[0],
                "kuerzel": r[1],
                "vollname": f"{r[2]} {r[3]}".strip(),
                "alias_name": f"REISENDER_{i+1}",
            }
            for i, r in enumerate(reisende)
        ],
    }


def get_event_detail(event_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, reise_id, typ, titel, status, created_at
                FROM events
                WHERE id=%s
            """, (event_id,))
            e = cur.fetchone()

            cur.execute("""
                SELECT b.id, b.belegdatum, b.art, b.kosten, b.waehrung, b.created_at
                FROM event_belege eb
                JOIN belege b ON eb.beleg_id = b.id
                WHERE eb.event_id=%s
                ORDER BY b.id DESC
            """, (event_id,))
            rows = cur.fetchall()

    event = None
    if e:
        event = {
            "id": e[0],
            "reise_id": e[1],
            "typ": e[2],
            "titel": e[3],
            "status": e[4],
            "created_at": e[5],
        }

    return {
        "event": event,
        "belege": [
            {
                "id": r[0],
                "belegdatum": r[1],
                "art": r[2],
                "kosten": r[3],
                "waehrung": r[4],
                "created_at": r[5],
            }
            for r in rows
        ],
    }