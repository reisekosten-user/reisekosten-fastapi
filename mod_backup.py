"""
mod_backup.py – Tägliche Datenbank-Sicherung.

Warum überhaupt nötig: Der kostenlose Render-Tarif bietet KEINE automatischen
Datenbank-Backups (das gibt's erst ab bezahltem Postgres-Plan). Ohne eigene
Vorkehrung wäre bei einem Datenverlust (versehentliches Löschen, Render-
Ausfall, fehlgeschlagene Migration) alles weg.

Funktionsweise: Statt auf das Kommandozeilen-Tool "pg_dump" zu setzen (das in
Render's Python-Laufzeitumgebung nicht garantiert verfügbar ist), liest dieses
Modul JEDE Tabelle direkt über die ohnehin vorhandene DB-Verbindung aus,
serialisiert alles als ein JSON-Dokument und lädt es komprimiert in denselben
S3-Speicher hoch, der auch für die Beleg-PDFs genutzt wird (kein zusätzlicher
Dienst nötig). Alte Backups werden automatisch aufgeräumt (nur die letzten
BACKUP_AUFBEWAHRUNG behalten).

Wiederherstellung im Ernstfall: Das Backup-JSON enthält für jede Tabelle die
Spaltennamen und alle Zeilen als Liste von Objekten – im Zweifel reicht schon
das rohe JSON, um Daten manuell/mit einem kurzen Skript zurückzuspielen.
"""
from __future__ import annotations
import json
import gzip
from datetime import date, datetime, timezone
from decimal import Decimal

from mod_db import get_db, is_postgres
from mod_beleg import get_s3, s3_upload, S3_BUCKET

TABELLEN = [
    "mitarbeiter", "reisen", "reise_mitarbeiter", "reise_laender",
    "belege", "beleg_gruppen", "vma_tage", "termine", "reisetage_person",
    "flug_status", "alert_konfiguration", "reise_zugang", "vma_saetze",
]

BACKUP_PREFIX = "backups/"
BACKUP_AUFBEWAHRUNG = 30  # Anzahl Backups, die behalten werden


def _json_default(o):
    """Wandelt Typen um, die json.dumps nicht von sich aus kann
    (Datum/Zeit, Decimal-Beträge aus Postgres NUMERIC-Spalten)."""
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def backup_erstellen() -> dict:
    """
    Liest alle Tabellen komplett aus, verpackt sie als ein komprimiertes
    JSON und lädt es zu S3 hoch. Gibt eine kurze Zusammenfassung zurück.
    """
    db = get_db()
    cur = db.cursor()
    daten = {"erstellt_am": datetime.now(timezone.utc).isoformat(), "tabellen": {}}
    zeilen_gesamt = 0

    for tabelle in TABELLEN:
        try:
            cur.execute(f"SELECT * FROM {tabelle}")
            spalten = [d[0] for d in cur.description]
            rows = cur.fetchall()
            eintraege = []
            for r in rows:
                if hasattr(r, "keys"):
                    eintraege.append(dict(r))
                else:
                    eintraege.append(dict(zip(spalten, r)))
            daten["tabellen"][tabelle] = eintraege
            zeilen_gesamt += len(eintraege)
        except Exception as e:
            # Eine einzelne fehlerhafte/fehlende Tabelle soll nicht das
            # gesamte Backup verhindern – wird nur vermerkt.
            daten["tabellen"][tabelle] = {"fehler": str(e)}

    cur.close(); db.close()

    roh = json.dumps(daten, ensure_ascii=False, default=_json_default).encode("utf-8")
    komprimiert = gzip.compress(roh)

    zeitstempel = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    key = f"{BACKUP_PREFIX}backup_{zeitstempel}.json.gz"
    s3_upload(key, komprimiert, content_type="application/gzip")

    entfernt = _alte_backups_aufraeumen()

    return {
        "ok": True, "key": key,
        "tabellen": len(TABELLEN), "zeilen_gesamt": zeilen_gesamt,
        "groesse_kb": round(len(komprimiert) / 1024, 1),
        "alte_backups_entfernt": entfernt,
    }


def _alte_backups_aufraeumen(max_anzahl: int = BACKUP_AUFBEWAHRUNG) -> int:
    """Behält nur die neuesten max_anzahl Backups, löscht ältere von S3."""
    s3 = get_s3()
    objekte = []
    paginator = s3.get_paginator("list_objects_v2")
    for seite in paginator.paginate(Bucket=S3_BUCKET, Prefix=BACKUP_PREFIX):
        objekte.extend(seite.get("Contents", []))
    if len(objekte) <= max_anzahl:
        return 0
    objekte.sort(key=lambda o: o["LastModified"], reverse=True)
    zu_loeschen = objekte[max_anzahl:]
    for obj in zu_loeschen:
        s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
    return len(zu_loeschen)


def backups_auflisten() -> list:
    """Liste vorhandener Backups (neueste zuerst) für die Anzeige in der App."""
    s3 = get_s3()
    objekte = []
    paginator = s3.get_paginator("list_objects_v2")
    for seite in paginator.paginate(Bucket=S3_BUCKET, Prefix=BACKUP_PREFIX):
        objekte.extend(seite.get("Contents", []))
    objekte.sort(key=lambda o: o["LastModified"], reverse=True)
    return [{"key": o["Key"], "datum": o["LastModified"], "groesse_kb": round(o["Size"]/1024, 1)}
            for o in objekte]
