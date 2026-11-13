"""
mod_reisepdf.py – PDF-Dokumente für die Reisemappe (v3.41)

In den Einstellungen lädt der Organisator PDFs hoch, die jeder Reisende in seiner Reisemappe braucht
(z. B. die Kopie der Krankenversicherung). Jedes Dokument gilt entweder für ALLE Mitarbeiter oder nur für
einen bestimmten Mitarbeiter (z. B. seine persönliche Versicherungskarte).

Die Reisemappe ist das Reiseplan-PDF. Dieses Modul hängt die passenden Dokumente an und setzt vorne eine
Seite „Anlagen“ mit Inhaltsverzeichnis. Zusätzlich bekommen die PDF-Leser Lesezeichen.
Dateien liegen im S3-Speicher (wie die Belege), die Liste in der Tabelle reise_pdfs.

v3.64: Ein Dokument „für alle Mitarbeiter“ kann als „nur QR-Code“ markiert werden (z. B. die Weltpolice). Es wird dann
nicht mehr in die Reisemappe gedruckt; stattdessen steht am Ende des Reiseplans ein QR-Code mit einem dauerhaften Link
(/portal/dokument/<Kennung>). Die Kennung wird einmal erzeugt und bleibt für immer gleich – auch wenn die Datei
später ersetzt wird.
"""
from __future__ import annotations
import io
import secrets
import uuid
from datetime import datetime
from xml.sax.saxutils import escape as _xml

from mod_db import get_db, ph, is_postgres

TABELLE = "reise_pdfs"
MAX_MB = 15
MAX_SEITEN = 60
ENDUNGEN_BILD = (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp", ".heic", ".heif")


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


# ── Tabelle ───────────────────────────────────────────────────────

def tabelle_sichern() -> None:
    """Legt die Tabelle an, falls sie fehlt (läuft in Postgres und SQLite gleich)."""
    try:
        db = get_db(); cur = db.cursor()
        key = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} ("
                    f"id {key}, titel TEXT, kuerzel TEXT, dateiname TEXT, s3_key TEXT, "
                    f"seiten INTEGER, groesse INTEGER, aktiv INTEGER DEFAULT 1, erstellt TEXT, "
                    f"nur_qr INTEGER DEFAULT 0, qr_token TEXT)")
        db.commit()
        # v3.64: Spalten für „nur QR-Code“ in bestehenden Tabellen nachrüsten (einmalig)
        neu_angelegt = False
        try:
            cur.execute(f"SELECT nur_qr, qr_token FROM {TABELLE} LIMIT 1")
            cur.fetchall()
        except Exception:
            try: db.rollback()
            except Exception: pass
            for spalte in ("nur_qr INTEGER DEFAULT 0", "qr_token TEXT"):
                try:
                    cur.execute(f"ALTER TABLE {TABELLE} ADD COLUMN {spalte}"); db.commit()
                except Exception:
                    try: db.rollback()
                    except Exception: pass
            neu_angelegt = True
        if neu_angelegt:
            # Einmalige Umstellung: Die vorhandene Weltpolice (Titel enthält „police“, gilt für alle) wird nur noch als QR-Code gedruckt.
            P = ph()
            cur.execute(f"SELECT id, titel, kuerzel FROM {TABELLE}")
            for r in cur.fetchall():
                titel = str(_g(r, "titel", 1) or "").lower()
                if "police" in titel and not (_g(r, "kuerzel", 2) or ""):
                    cur.execute(f"UPDATE {TABELLE} SET nur_qr=1, qr_token={P} WHERE id={P}",
                                (secrets.token_urlsafe(18), _g(r, "id", 0)))
                    print(f"[ReisePDF] '{_g(r, 'titel', 1)}' wird ab jetzt nur noch als QR-Code in die Reisemappe gedruckt.")
            db.commit()
        cur.close(); db.close()
    except Exception as ex:
        print(f"[ReisePDF] Tabelle {TABELLE} nicht angelegt: {ex}")


_SPALTEN_ALT = ("id", "titel", "kuerzel", "dateiname", "s3_key", "seiten", "groesse", "aktiv", "erstellt")
_SPALTEN = _SPALTEN_ALT + ("nur_qr", "qr_token")


def _zeile(r) -> dict:
    def wert(k, i):
        if hasattr(r, "keys"):
            return r[k] if k in list(r.keys()) else None
        return r[i] if i < len(r) else None
    d = {k: wert(k, i) for i, k in enumerate(_SPALTEN)}
    d["aktiv"] = bool(d["aktiv"]) if d["aktiv"] is not None else True
    d["kuerzel"] = d["kuerzel"] or ""
    d["nur_qr"] = bool(d.get("nur_qr"))
    d["qr_token"] = d.get("qr_token") or ""
    return d


def _abfrage(cur, where: str = "", args=()) -> list:
    """Liest die Dokumentenliste; fehlen die QR-Spalten noch (ältere Tabelle), mit den alten Spalten weiterarbeiten."""
    try:
        cur.execute(f"SELECT {', '.join(_SPALTEN)} FROM {TABELLE} {where} ORDER BY id", args)
    except Exception:
        try: cur.connection.rollback()
        except Exception: pass
        cur.execute(f"SELECT {', '.join(_SPALTEN_ALT)} FROM {TABELLE} {where} ORDER BY id", args)
    return [_zeile(r) for r in cur.fetchall()]


def liste(nur_aktive: bool = False) -> list:
    """Alle Dokumente: zuerst die für alle Mitarbeiter, dann die persönlichen (je nach Anlage-Reihenfolge)."""
    db = get_db(); cur = db.cursor()
    try:
        rows = _abfrage(cur)
    except Exception:
        try: db.rollback()
        except Exception: pass
        rows = []
    finally:
        cur.close(); db.close()
    if nur_aktive:
        rows = [r for r in rows if r["aktiv"]]
    return sorted(rows, key=lambda r: (1 if r["kuerzel"] else 0, r["id"]))


def holen(pid: int) -> dict | None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        rows = _abfrage(cur, f"WHERE id={P}", (pid,))
    finally:
        cur.close(); db.close()
    return rows[0] if rows else None


# ── Hochladen / Ändern ────────────────────────────────────────────

def pruefen(daten: bytes, dateiname: str, content_type: str = "") -> tuple:
    """Macht aus dem Upload ein lesbares PDF. Rückgabe: (pdf_bytes, seiten) oder wirft ValueError mit klarem Text."""
    if not daten:
        raise ValueError("Die Datei ist leer.")
    if len(daten) > MAX_MB * 1024 * 1024:
        raise ValueError(f"Die Datei ist größer als {MAX_MB} MB.")
    name = (dateiname or "").lower()
    ct = (content_type or "").lower()
    if daten[:4] != b"%PDF":
        if ct.startswith("image/") or name.endswith(ENDUNGEN_BILD):
            try:
                from mod_beleg import bild_zu_pdf
                daten = bild_zu_pdf(daten, dateiname or "bild")
            except Exception as ex:
                raise ValueError(f"Das Bild konnte nicht in ein PDF umgewandelt werden ({str(ex)[:80]}).")
        else:
            raise ValueError("Bitte ein PDF oder ein Foto (JPG/PNG) hochladen.")
    try:
        from pypdf import PdfReader
        r = PdfReader(io.BytesIO(daten))
        if r.is_encrypted:
            if not r.decrypt(""):
                raise ValueError("Das PDF ist passwortgeschützt. Bitte ohne Passwort speichern und neu hochladen.")
        seiten = len(r.pages)
    except ValueError:
        raise
    except Exception as ex:
        raise ValueError(f"Das PDF ist nicht lesbar ({str(ex)[:80]}).")
    if seiten < 1:
        raise ValueError("Das PDF hat keine Seiten.")
    if seiten > MAX_SEITEN:
        raise ValueError(f"Das PDF hat {seiten} Seiten (erlaubt sind höchstens {MAX_SEITEN}).")
    return daten, seiten


def hinzufuegen(titel: str, kuerzel: str, daten: bytes, dateiname: str, content_type: str = "", nur_qr: bool = False) -> int:
    """Speichert ein Dokument (S3 + Liste). kuerzel leer = für alle Mitarbeiter. Gibt die ID zurück.
    nur_qr: in der Reisemappe nur als QR-Code (geht nur für Dokumente „für alle Mitarbeiter“)."""
    titel = (titel or "").strip()[:120]
    if not titel:
        raise ValueError("Bitte einen Titel eingeben.")
    if nur_qr and (kuerzel or "").strip():
        raise ValueError("„Nur als QR-Code“ geht nur für Dokumente, die für alle Mitarbeiter gelten.")
    pdf, seiten = pruefen(daten, dateiname, content_type)
    from mod_beleg import s3_upload
    key = f"reise_pdfs/{uuid.uuid4().hex}.pdf"
    s3_upload(key, pdf)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        sql = (f"INSERT INTO {TABELLE} (titel, kuerzel, dateiname, s3_key, seiten, groesse, aktiv, erstellt) "
               f"VALUES ({P},{P},{P},{P},{P},{P},{P},{P})")
        vals = (titel, (kuerzel or "").strip() or None, (dateiname or "")[:200], key, seiten, len(pdf), 1,
                datetime.now().strftime("%Y-%m-%d %H:%M"))
        if is_postgres():
            cur.execute(sql + " RETURNING id", vals)
            neu = cur.fetchone()
            pid = neu[0] if not hasattr(neu, "keys") else neu["id"]
        else:
            cur.execute(sql, vals)
            pid = cur.lastrowid
        db.commit()
    finally:
        cur.close(); db.close()
    if nur_qr:
        qr_setzen(pid, True)
    return pid


def ersetzen(pid: int, daten: bytes, dateiname: str, content_type: str = "") -> None:
    """Tauscht nur die Datei aus. Titel, Zuordnung und die QR-Kennung (der gedruckte Link) bleiben unverändert."""
    d = holen(pid)
    if not d:
        raise ValueError("Dokument nicht gefunden.")
    pdf, seiten = pruefen(daten, dateiname, content_type)
    from mod_beleg import s3_upload
    key = f"reise_pdfs/{uuid.uuid4().hex}.pdf"
    s3_upload(key, pdf)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET dateiname={P}, s3_key={P}, seiten={P}, groesse={P} WHERE id={P}",
                    ((dateiname or "")[:200], key, seiten, len(pdf), pid))
        db.commit()
    finally:
        cur.close(); db.close()


def umbenennen(pid: int, titel: str) -> None:
    """Ändert nur den Titel (z. B. bei einem Tippfehler). Zuordnung, Datei und QR-Link bleiben unverändert."""
    titel = (titel or "").strip()[:120]
    if not titel:
        raise ValueError("Bitte einen Titel eingeben.")
    if not holen(pid):
        raise ValueError("Dokument nicht gefunden.")
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET titel={P} WHERE id={P}", (titel, pid))
        db.commit()
    finally:
        cur.close(); db.close()


def qr_setzen(pid: int, an: bool) -> str:
    """Schaltet „nur als QR-Code“ ein oder aus. Die Kennung entsteht beim ersten Einschalten und bleibt für immer gleich
    (auch nach dem Ausschalten), damit bereits gedruckte QR-Codes gültig bleiben. Rückgabe: die Kennung."""
    d = holen(pid)
    if not d:
        raise ValueError("Dokument nicht gefunden.")
    if an and d["kuerzel"]:
        raise ValueError("„Nur als QR-Code“ geht nur für Dokumente, die für alle Mitarbeiter gelten.")
    token = d["qr_token"] or (secrets.token_urlsafe(18) if an else "")
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET nur_qr={P}, qr_token={P} WHERE id={P}", (1 if an else 0, token or None, pid))
        db.commit()
    finally:
        cur.close(); db.close()
    return token


def holen_per_qr(token: str) -> dict | None:
    """Das Dokument zu einer QR-Kennung – nur wenn es aktiv, „für alle Mitarbeiter“ und als QR-Code freigegeben ist."""
    token = (token or "").strip()
    if len(token) < 16:
        return None
    for d in liste(nur_aktive=True):
        if d["nur_qr"] and not d["kuerzel"] and d["qr_token"] and secrets.compare_digest(d["qr_token"], token):
            return d
    return None


def qr_eintraege(basis_url: str) -> list:
    """Für den Reiseplan: [{id, titel, url}] aller aktiven QR-Dokumente. Ohne bekannte Internetadresse der App
    lässt sich kein gültiger Link bauen – dann leer (die Dokumente werden dann wie bisher angehängt)."""
    basis = (basis_url or "").strip().rstrip("/")
    if not basis:
        return []
    return [{"id": d["id"], "titel": d["titel"], "url": f"{basis}/portal/dokument/{d['qr_token']}"}
            for d in liste(nur_aktive=True) if d["nur_qr"] and not d["kuerzel"] and d["qr_token"]]


def loeschen(pid: int) -> None:
    """Entfernt den Eintrag aus der Liste. Die Datei im Speicher bleibt als Sicherung liegen."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {TABELLE} WHERE id={P}", (pid,))
        db.commit()
    finally:
        cur.close(); db.close()


def aktiv_setzen(pid: int, aktiv: bool) -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET aktiv={P} WHERE id={P}", (1 if aktiv else 0, pid))
        db.commit()
    finally:
        cur.close(); db.close()


def pdf_bytes(pid: int) -> bytes | None:
    d = holen(pid)
    if not d or not d["s3_key"]:
        return None
    from mod_beleg import s3_download
    return s3_download(d["s3_key"])


# ── Anhänge für die Reisemappe ────────────────────────────────────

def anhaenge_fuer(kuerzel: str | None, ohne_ids=()) -> list:
    """Dokumente für diese Reisemappe: alle aktiven „für alle Mitarbeiter“ plus die persönlichen des Mitarbeiters.
    Ohne Kürzel (Reise mit mehreren Reisenden, kein Mitarbeiter gewählt) nur die Dokumente für alle.
    ohne_ids: Dokumente, die stattdessen als QR-Code im Reiseplan stehen (werden nicht angehängt).
    Rückgabe: [{titel, bytes, quelle}] – nicht ladbare Dateien werden übersprungen und in 'fehler' gemeldet."""
    erg = []
    for d in liste(nur_aktive=True):
        if d["id"] in set(ohne_ids or ()):
            continue
        if d["kuerzel"] and d["kuerzel"] != (kuerzel or ""):
            continue
        try:
            daten = pdf_bytes(d["id"])
        except Exception as ex:
            erg.append({"titel": d["titel"], "bytes": None, "quelle": "Einstellungen", "fehler": str(ex)[:80]})
            continue
        erg.append({"titel": d["titel"], "bytes": daten, "quelle": "Einstellungen", "fehler": None})
    return erg


def a1_anlage(beleg_id: int, titel: str) -> dict:
    """Die A1-Bescheinigung (Beleg) als Anlage der Reisemappe."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT s3_original FROM belege WHERE id={P}", (beleg_id,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    key = _g(r, "s3_original", 0) if r else None
    if not key:
        return {"titel": titel, "bytes": None, "quelle": "A1", "fehler": "Datei fehlt"}
    try:
        from mod_beleg import s3_download
        return {"titel": titel, "bytes": s3_download(key), "quelle": "A1", "fehler": None}
    except Exception as ex:
        return {"titel": titel, "bytes": None, "quelle": "A1", "fehler": str(ex)[:80]}


def _index_pdf(titel: str, zeilen: list, probleme: list) -> bytes:
    """Eine Seite „Anlagen“: Nummer, Titel, Seite im Gesamt-PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    st = getSampleStyleSheet()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm, topMargin=20 * mm,
                            bottomMargin=20 * mm, title="Anlagen")
    story = [Paragraph("Anlagen zur Reisemappe", st["Title"]), Paragraph(_xml(titel), st["Normal"]), Spacer(1, 8 * mm)]
    daten = [["Nr.", "Dokument", "Seite"]]
    for i, (t, seite, n) in enumerate(zeilen, 1):
        daten.append([str(i), Paragraph(_xml(t), st["Normal"]), f"{seite}" if n == 1 else f"{seite}–{seite + n - 1}"])
    tab = Table(daten, colWidths=[14 * mm, 126 * mm, 30 * mm])
    tab.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a5f")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f1f5f9")]),
    ]))
    story.append(tab)
    if probleme:
        story += [Spacer(1, 6 * mm), Paragraph("<b>Nicht angehängt:</b>", st["Normal"])]
        for t, grund in probleme:
            story.append(Paragraph(f"• {_xml(t)} – {_xml(grund)}", st["Normal"]))
    doc.build(story)
    return buf.getvalue()


def mappe_zusammenbauen(basis_pdf: bytes, anlagen: list, kopf: str = "") -> tuple:
    """
    Hängt die Anlagen hinter das Reiseplan-PDF (zuerst eine Seite „Anlagen“ mit Inhaltsverzeichnis).
    anlagen: [{titel, bytes, fehler}]. Rückgabe: (pdf_bytes, probleme) – probleme = [(titel, Grund)].
    Ist nichts anzuhängen, kommt das Reiseplan-PDF unverändert zurück.
    """
    from pypdf import PdfReader, PdfWriter
    probleme, gut = [], []
    for a in anlagen:
        if not a.get("bytes"):
            probleme.append((a["titel"], a.get("fehler") or "Datei nicht lesbar"))
            continue
        try:
            r = PdfReader(io.BytesIO(a["bytes"]))
            if r.is_encrypted and not r.decrypt(""):
                raise ValueError("passwortgeschützt")
            gut.append((a["titel"], r, len(r.pages)))
        except Exception as ex:
            probleme.append((a["titel"], str(ex)[:80] or "nicht lesbar"))
    if not gut:
        return basis_pdf, probleme
    basis = PdfReader(io.BytesIO(basis_pdf))
    n0 = len(basis.pages)
    n_index = 1
    for _ in range(2):                              # Inhaltsverzeichnis länger als eine Seite: Seitenzahlen neu rechnen
        seite = n0 + n_index + 1
        zeilen = []
        for t, _r, n in gut:
            zeilen.append((t, seite, n))
            seite += n
        index = PdfReader(io.BytesIO(_index_pdf(kopf, zeilen, probleme)))
        if len(index.pages) == n_index:
            break
        n_index = len(index.pages)
    w = PdfWriter()
    for p in basis.pages:
        w.add_page(p)
    for p in index.pages:
        w.add_page(p)
    for _t, r, _n in gut:
        for p in r.pages:
            w.add_page(p)
    try:
        w.add_outline_item("Reiseplan", 0)
        w.add_outline_item("Anlagen (Übersicht)", n0)
        for t, seite, _n in zeilen:
            w.add_outline_item(t, seite - 1)
    except Exception:
        pass                                         # Lesezeichen sind nur Komfort
    out = io.BytesIO()
    w.write(out)
    return out.getvalue(), probleme
