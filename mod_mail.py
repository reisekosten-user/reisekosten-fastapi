"""
mod_mail.py – IMAP Mail-Import
Stabil / abgeschlossen
"""
from __future__ import annotations
import imaplib, email as _email_mod, re, json, os
from email.header import decode_header as _decode_header

IMAP_HOST = os.getenv("IMAP_HOST", "")
IMAP_USER = os.getenv("IMAP_USER", "")
IMAP_PASS = os.getenv("IMAP_PASS", "")

from mod_db import get_db, ph, is_postgres
from mod_beleg import beleg_verarbeiten
from mod_anon import anonymisieren

def decode_mime_header(val: str) -> str:
    """Dekodiert MIME-kodierten Mail-Header."""
    if not val: return ""
    parts = _decode_header(val)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            result.append(str(part))
    return "".join(result)

def mail_body_text(msg) -> tuple:
    """
    Extrahiert Text-Body und Anhänge aus einer Mail.
    Gibt (body_text, [(filename, bytes, content_type)]) zurück.
    """
    body = ""
    html_body = ""
    attachments = []

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition") or "")
            fn = part.get_filename()
            payload = part.get_payload(decode=True)
            if not payload: continue

            if fn:
                fn = decode_mime_header(fn)
                fn_lower = fn.lower()
                # Nicht-Beleg-Dateien überspringen
                if fn_lower.endswith((".ics",".vcf",".emz",".wmz",".gif")): continue
                # Nur echte Beleg-Dateien
                if not fn_lower.endswith((".pdf",".jpg",".jpeg",".png",".heic",".webp")):
                    continue
                attachments.append((fn, payload, ct))
            elif ct == "text/plain" and not body:
                body = payload.decode(errors="ignore")
            elif ct == "text/html" and not html_body:
                html_body = payload.decode(errors="ignore")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            ct = msg.get_content_type()
            if ct == "text/html":
                html_body = payload.decode(errors="ignore")
            else:
                body = payload.decode(errors="ignore")

    # HTML zu Text wenn kein Plain-Text vorhanden
    if not body and html_body:
        import html as _html
        t = _html.unescape(html_body)
        t = re.sub(r"<style[^>]*>.*?</style>", " ", t, flags=re.DOTALL|re.IGNORECASE)
        t = re.sub(r"<br\s*/?>", "\n", t, flags=re.IGNORECASE)
        t = re.sub(r"<[^>]+>", " ", t)
        t = re.sub(r"[ \t]+", " ", t)
        t = re.sub(r"\n{3,}", "\n\n", t)
        body = t.strip()

    return body[:50000], attachments

async def fetch_mails() -> dict:
    """
    Holt alle Mails aus dem IMAP-Postfach und verarbeitet sie als Belege.
    Mails werden nach Verarbeitung als gelesen markiert (nicht gelöscht).
    """
    if not all([IMAP_HOST, IMAP_USER, IMAP_PASS]):
        return {"fehler": "IMAP nicht konfiguriert (IMAP_HOST/USER/PASS fehlen)"}

    importiert = 0
    belege_erstellt = 0
    fehler_liste = []
    duplikate = 0

    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        # Nur ungelesene Mails
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split() if data and data[0] else []
    except Exception as e:
        return {"fehler": f"IMAP-Verbindung fehlgeschlagen: {e}"}

    for mid in ids:
        try:
            _, msg_data = mail.fetch(mid, "(RFC822)")
            if not msg_data or not msg_data[0]: continue
            msg = _email_mod.message_from_bytes(msg_data[0][1])

            betreff = decode_mime_header(msg.get("Subject", ""))
            absender = decode_mime_header(msg.get("From", ""))
            msg_id = (msg.get("Message-ID") or "").strip()

            # Duplikat-Check via Message-ID
            if msg_id:
                db = get_db(); cur = db.cursor()
                P = ph()
                cur.execute(f"SELECT id FROM belege WHERE buchungscode={P}",
                            (f"MAIL:{msg_id[:80]}",))
                if cur.fetchone():
                    cur.close(); db.close()
                    duplikate += 1
                    mail.store(mid, "+FLAGS", "\\Seen")
                    continue
                cur.close(); db.close()

            body, attachments = mail_body_text(msg)
            full_text = f"Von: {absender}\nBetreff: {betreff}\n\n{body}"

            # Reisecode aus Betreff (z.B. 26-001)
            rc_match = re.search(r"\b(\d{2}-\d{3})\b", betreff + " " + body)
            reise_code = rc_match.group(1) if rc_match else None

            # Reise-Code prüfen ob sie existiert
            if reise_code:
                db = get_db(); cur = db.cursor()
                P = ph()
                cur.execute(f"SELECT code FROM reisen WHERE code={P}", (reise_code,))
                if not cur.fetchone():
                    reise_code = None
                cur.close(); db.close()

            # Regel: PDF/Bild-Anhänge vorhanden → nur Anhänge verarbeiten
            # Kein Anhang → Mail-Body als Beleg
            echte_anhaenge = [(fn, payload, ct) for fn, payload, ct in attachments
                              if fn.lower().endswith((".pdf",".jpg",".jpeg",".png",".heic",".webp"))]

            if echte_anhaenge:
                # Nur Anhänge verarbeiten – Body ist nur Benachrichtigung
                for fn, payload, ct in echte_anhaenge:
                    result = await beleg_verarbeiten(payload, fn, reise_code, ct)
                    belege_erstellt += 1
            else:
                # Kein Anhang – Mail-Body selbst ist der Beleg
                if body and len(body.strip()) > 50:
                    # Betreff anonymisieren für Dateinamen
                    ma_namen_tmp, ma_mails_tmp = lade_ma_daten()
                    betreff_anon = anonymisieren(betreff, ma_namen_tmp, ma_mails_tmp)
                    result = await beleg_verarbeiten(
                        full_text.encode("utf-8"),
                        f"Mail: {betreff_anon[:60]}",
                        reise_code,
                        "text/plain")
                    belege_erstellt += 1
                    if msg_id and result.get("beleg_id"):
                        db = get_db(); cur = db.cursor()
                        P = ph()
                        cur.execute(f"UPDATE belege SET buchungscode={P} WHERE id={P}",
                                    (f"MAIL:{msg_id[:80]}", result["beleg_id"]))
                        db.commit(); cur.close(); db.close()

            # Mail löschen (nach erfolgreicher Verarbeitung)
            mail.store(mid, "+FLAGS", "\\Deleted")
            importiert += 1

        except Exception as e:
            import traceback
            fehler_liste.append(f"{e}")
            print(f"[Mail-Import Fehler] {e}\n{traceback.format_exc()[:200]}")

    try:
        mail.expunge()  # Endgültig löschen
        mail.logout()
    except: pass

    return {
        "importiert": importiert,
        "belege_erstellt": belege_erstellt,
        "duplikate": duplikate,
        "fehler": len(fehler_liste),
        "fehler_details": fehler_liste
    }


