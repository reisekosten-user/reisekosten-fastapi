"""
mod_mail.py – IMAP Mail-Import + SMTP-Versand (DMS-Archivierung)
"""
from __future__ import annotations
import imaplib, smtplib, socket, ssl, threading, time, email as _email_mod, re, json, os
from email.header import decode_header as _decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

IMAP_HOST = os.getenv("IMAP_HOST", "")
IMAP_USER = os.getenv("IMAP_USER", "")
IMAP_PASS = os.getenv("IMAP_PASS", "")

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465") or "465")
SMTP_USER = os.getenv("SMTP_USER", "") or IMAP_USER
SMTP_PASS = os.getenv("SMTP_PASS", "") or IMAP_PASS
DMS_EMAIL_TO = os.getenv("DMS_EMAIL_TO", "")
SMTP_TIMEOUT = int(os.getenv("SMTP_TIMEOUT", "30") or "30")      # Wartezeit beim Senden (große Anhänge)
SMTP_VERBINDUNG_TIMEOUT = 15                                      # Wartezeit für Verbindungsaufbau und Anmeldung

from mod_db import get_db, ph, is_postgres
from mod_beleg import beleg_verarbeiten, get_s3, s3_download, lade_ma_daten
from mod_anon import anonymisieren


def _kalender_nachricht(text: str, ics: str, methode: str) -> MIMEMultipart:
    """Outlook-/Exchange-taugliche Einladung (wie Google Kalender sie sendet):
    mixed[ alternative[ text/plain, text/calendar;method=… ], invite.ics ]."""
    msg = MIMEMultipart("mixed")
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text, "plain", "utf-8"))
    kal = MIMEText(ics, "calendar", "utf-8")
    kal.replace_header("Content-Type", 'text/calendar; charset="UTF-8"; method=%s' % methode)
    alt.attach(kal)
    msg.attach(alt)
    anhang = MIMEApplication(ics.encode("utf-8"), "ics", Name="invite.ics")
    anhang["Content-Disposition"] = 'attachment; filename="invite.ics"'
    msg.attach(anhang)
    return msg


def _absender_domain() -> str:
    """Domain der Absenderadresse (für die Message-ID); Ersatz, falls SMTP_USER keine Adresse ist."""
    return SMTP_USER.split("@", 1)[1] if "@" in (SMTP_USER or "") else "reisekosten.herrhammer"


def smtp_fehlende() -> list:
    """Namen der Einstellungen (Render → Environment), die für den Mailversand noch fehlen."""
    fehlt = []
    if not SMTP_HOST:
        fehlt.append("SMTP_HOST")
    if not SMTP_USER:
        fehlt.append("SMTP_USER (oder ersatzweise IMAP_USER)")
    if not SMTP_PASS:
        fehlt.append("SMTP_PASS (oder ersatzweise IMAP_PASS)")
    return fehlt


def _ist_verbindungsfehler(e: Exception) -> bool:
    """True, wenn der Mailserver gar nicht erreicht wurde (Zeitüberschreitung, Abbruch, DNS) –
    dann ist es sinnlos, weitere Mails im selben Lauf zu versuchen."""
    return isinstance(e, (socket.timeout, TimeoutError, ConnectionError, socket.gaierror, ssl.SSLError,
                          smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError))


def _tcp_test(host: str, port: int, timeout: float = 6.0) -> tuple:
    """(ok, Text) – reiner Verbindungstest ohne Anmeldung."""
    t0 = time.time()
    try:
        ips = sorted({x[4][0] for x in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
    except Exception as e:
        return False, "Servername nicht auflösbar (%s)" % e
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
        return True, "erreichbar (%d ms, %s)" % (int((time.time() - t0) * 1000), ips[0])
    except Exception as e:
        return False, "keine Verbindung nach %d s: %s (%s)" % (int(time.time() - t0), e or type(e).__name__, ips[0])


def _login_test() -> tuple:
    """(ok, Text) – Verbindung, Verschlüsselung und Anmeldung am eingestellten Postausgang, ohne Mail zu senden."""
    server = None
    try:
        server = _verbinden()
        return True, "Anmeldung erfolgreich"
    except Exception as e:
        return False, "%s: %s" % (type(e).__name__, e)
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                try:
                    server.close()
                except Exception:
                    pass


def smtp_diagnose() -> dict:
    """Prüft schrittweise, wo der Mailversand scheitert. Sendet nichts.
    Liefert {'zeilen': [(status, text)], 'fazit': [text]} – status: ok, fehler, warn, info."""
    from concurrent.futures import ThreadPoolExecutor
    zeilen, fazit = [], []
    z = lambda status, text: zeilen.append((status, text))
    z("info", "Einstellung: SMTP_HOST = %s · SMTP_PORT = %s" % (SMTP_HOST or "(leer)", SMTP_PORT))
    if not SMTP_USER:
        z("fehler", "Anmeldename (SMTP_USER / IMAP_USER) fehlt")
    elif "@" in SMTP_USER:
        z("ok", "Anmeldename ist eine volle E-Mail-Adresse: %s" % SMTP_USER)
    else:
        z("warn", "Anmeldename „%s“ ist keine E-Mail-Adresse. Er dient auch als Absender – UDAG verlangt dafür "
                  "eine volle Adresse." % SMTP_USER)
    z("ok" if SMTP_PASS else "fehler", "Passwort ist gesetzt" if SMTP_PASS else "Passwort (SMTP_PASS / IMAP_PASS) fehlt")

    hosts = []
    for h in (SMTP_HOST, "smtp.udag.de", "smtps.udag.de"):
        if h and h not in hosts:
            hosts.append(h)
    ziele = [(h, p) for h in hosts for p in (465, 587)]
    if IMAP_HOST:
        ziele.append((IMAP_HOST, 993))                        # Vergleich: dieser Weg funktioniert (Mail-Import)
    with ThreadPoolExecutor(max_workers=max(1, len(ziele))) as ex:
        ergebnisse = list(ex.map(lambda t: _tcp_test(t[0], t[1]), ziele))
    smtp_ok, imap_ok = [], None
    for (h, p), (ok, txt) in zip(ziele, ergebnisse):
        name = "Mail-Eingang (Vergleich) %s:%d" % (h, p) if p == 993 else "Postausgang %s:%d" % (h, p)
        z("ok" if ok else "fehler", "%s – %s" % (name, txt))
        if p == 993:
            imap_ok = ok
        elif ok:
            smtp_ok.append((h, p))

    if SMTP_HOST and SMTP_USER and SMTP_PASS and (SMTP_HOST, SMTP_PORT) in smtp_ok:
        ok, txt = _login_test()
        z("ok" if ok else "fehler", "Anmeldung an %s:%d – %s" % (SMTP_HOST, SMTP_PORT, txt))
        if ok:
            fazit.append("Verbindung und Anmeldung funktionieren. Fehlt noch etwas, liegt es an der Absenderadresse "
                         "(siehe oben) oder an einzelnen Mails.")
        else:
            fazit.append("Der Server ist erreichbar, aber die Anmeldung scheitert. Prüfen Sie SMTP_USER (volle E-Mail-Adresse) "
                         "und SMTP_PASS.")
    elif smtp_ok:
        h, p = smtp_ok[0]
        fazit.append("Erreichbar ist %s:%d. Stellen Sie SMTP_HOST auf %s und SMTP_PORT auf %d." % (h, p, h, p))
    elif ziele:
        if imap_ok:
            fazit.append("Alle Postausgänge sind von hier aus nicht erreichbar, der Mail-Eingang (993) aber schon. "
                         "Das spricht dafür, dass der Hoster (z. B. Render beim kostenlosen Web Service) die "
                         "Versandports 465 und 587 sperrt. Lösung: bezahlte Instanz oder Versand über einen "
                         "Mail-Dienst mit Web-Schnittstelle.")
        else:
            fazit.append("Kein Server ist erreichbar, auch der Mail-Eingang nicht. Bitte Servernamen und Netzwerk prüfen.")
    return {"zeilen": zeilen, "fazit": fazit}


# ── Versand über EINE Verbindung für mehrere Mails (Sitzung) ───────────────────
_SITZUNG = threading.local()


def _verbinden():
    """Neue, angemeldete SMTP-Verbindung."""
    if SMTP_PORT == 465:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=SMTP_VERBINDUNG_TIMEOUT)
    else:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_VERBINDUNG_TIMEOUT)
    try:
        if SMTP_PORT != 465:
            server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        try:
            server.sock.settimeout(SMTP_TIMEOUT)             # ab jetzt längere Wartezeit fürs Senden
        except Exception:
            pass
    except Exception:
        _schliessen(server)
        raise
    return server


def _schliessen(server) -> None:
    if server is None:
        return
    try:
        server.quit()
    except Exception:
        try:
            server.close()
        except Exception:
            pass


class smtp_sitzung:
    """with smtp_sitzung(): ...  – alle Mails im Block laufen über eine einzige Verbindung (eine Anmeldung statt
    einer je Mail; manche Anbieter bremsen viele Anmeldungen kurz hintereinander)."""
    def __enter__(self):
        _SITZUNG.aktiv, _SITZUNG.verbindung = True, None
        return self

    def __exit__(self, *exc):
        _schliessen(getattr(_SITZUNG, "verbindung", None))
        _SITZUNG.aktiv, _SITZUNG.verbindung = False, None
        return False


def _versenden(empfaenger: str, nachricht: str) -> None:
    """Sendet eine Nachricht. Bricht die Verbindung ab, wird einmal mit einer neuen Verbindung wiederholt."""
    in_sitzung = bool(getattr(_SITZUNG, "aktiv", False))
    for versuch in (0, 1):
        server = getattr(_SITZUNG, "verbindung", None) if in_sitzung else None
        try:
            if server is None:
                server = _verbinden()
                if in_sitzung:
                    _SITZUNG.verbindung = server
            server.sendmail(SMTP_USER, [empfaenger], nachricht)
            if not in_sitzung:
                _schliessen(server)
            return
        except Exception as e:
            _schliessen(server)                              # Verbindung ist unbrauchbar oder soll nicht bleiben
            if in_sitzung:
                _SITZUNG.verbindung = None
            if versuch == 0 and _ist_verbindungsfehler(e):
                time.sleep(1.0)
                continue
            raise


def sende_mail(empfaenger: str, betreff: str, text: str,
                anhang_bytes: bytes | None = None, anhang_name: str | None = None,
                anhaenge: list | None = None,
                ics: str | None = None, ics_methode: str = "REQUEST") -> dict:
    """
    Generischer SMTP-Versand (Text-Mail, optional mit Anhängen).
    anhaenge: Liste von (bytes, dateiname)-Tupeln für mehrere Anhänge.
    anhang_bytes/anhang_name: Abkürzung für genau einen Anhang (Rückwärtskompatibilität).
    ics: iCalendar-Text -> Mail wird als Kalendereinladung gesendet (ics_methode: REQUEST oder CANCEL).
    Benötigt SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS als Umgebungsvariablen.
    """
    fehlt = smtp_fehlende()
    if fehlt:
        return {"fehler": "SMTP nicht konfiguriert – es fehlt: " + ", ".join(fehlt)}
    try:
        alle_anhaenge = list(anhaenge or [])
        if anhang_bytes is not None:
            alle_anhaenge.append((anhang_bytes, anhang_name or "anhang.pdf"))

        if ics:
            from email.utils import formatdate, make_msgid
            msg = _kalender_nachricht(text, ics, ics_methode)
            msg["Date"] = formatdate(localtime=False)
            msg["Message-ID"] = make_msgid(domain="reisekosten.herrhammer")
        else:
            from email.utils import formatdate, make_msgid
            msg = MIMEMultipart()
            msg.attach(MIMEText(text, "plain", "utf-8"))
            # Datum und eindeutige Message-ID gehören in jede Mail. Ohne sie können Empfänger-Server gleiche Mails
            # (z. B. ein erneut gesendeter Portal-Link) als Doppelte verwerfen oder in den Spam sortieren.
            msg["Date"] = formatdate(usegmt=True)
            msg["Message-ID"] = make_msgid(domain=_absender_domain())
        msg["From"] = SMTP_USER
        msg["To"] = empfaenger
        msg["Subject"] = betreff

        for daten, name in alle_anhaenge:
            part = MIMEApplication(daten, Name=name or "anhang.pdf")
            part["Content-Disposition"] = f'attachment; filename="{name or "anhang.pdf"}"'
            msg.attach(part)

        _versenden(empfaenger, msg.as_string())
        return {"ok": True}
    except Exception as e:
        return {"fehler": str(e), "verbindung": _ist_verbindungsfehler(e)}


def dms_konfiguriert() -> bool:
    return bool(SMTP_HOST and SMTP_USER and SMTP_PASS and DMS_EMAIL_TO)


def sende_dms_mail(betreff: str, text: str, anhang_bytes: bytes, anhang_name: str,
                    weitere_anhaenge: list | None = None) -> dict:
    """Verschickt einen (oder mehrere verknüpfte) Beleg(e) per E-Mail an Habel (DMS_EMAIL_TO)."""
    if not dms_konfiguriert():
        fehlt = smtp_fehlende() + ([] if DMS_EMAIL_TO else ["DMS_EMAIL_TO"])
        return {"fehler": "DMS-Mailversand nicht konfiguriert – es fehlt: " + ", ".join(fehlt)}
    anhaenge = [(anhang_bytes, anhang_name)] + list(weitere_anhaenge or [])
    return sende_mail(DMS_EMAIL_TO, betreff, text, anhaenge=anhaenge)


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

_ENDUNG_ZU_TYP = {".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                  ".heic": "image/heic", ".heif": "image/heic", ".webp": "image/webp", ".xml": "application/xml"}
_TYP_ZU_ENDUNG = {"application/pdf": ".pdf", "image/jpeg": ".jpg", "image/png": ".png",
                  "image/heic": ".heic", "image/webp": ".webp", "application/xml": ".xml"}


def _anhang_typ(payload: bytes, fn: str, ct: str):
    """Erkennt, ob ein MIME-Teil ein Beleg-Anhang ist, und gibt den MIME-Typ zurück (sonst None).
    Maßgeblich ist der INHALT (PDF/JPG/PNG/WEBP/HEIC erkennt man an den ersten Bytes), erst danach die
    Dateiendung. Der von der Mail angegebene Typ zählt nicht: Handy-Apps und Weiterleitungen schreiben
    dort oft nur "application/octet-stream"."""
    if payload[:1024].find(b"%PDF") >= 0:
        return "application/pdf"
    if payload[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if payload[:4] == b"\x89PNG":
        return "image/png"
    if payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp"
    if payload[4:8] == b"ftyp" and payload[8:12] in (b"heic", b"heix", b"hevc", b"mif1", b"msf1", b"heif"):
        return "image/heic"
    endung = os.path.splitext((fn or "").lower().strip())[1]
    return _ENDUNG_ZU_TYP.get(endung)


def mail_teile_uebersicht(msg) -> list:
    """Alle MIME-Teile einer Mail als (Typ, Dateiname, Anzeige, Größe in Bytes) – zur Fehlersuche."""
    out = []
    for part in msg.walk():
        if part.is_multipart():
            continue
        fn = part.get_filename()
        cd = str(part.get("Content-Disposition") or "").split(";")[0].strip().lower() or "-"
        if part.get("Content-ID") and cd == "-":
            cd = "cid"
        out.append((part.get_content_type(), decode_mime_header(fn).strip() if fn else "", cd,
                    len(part.get_payload(decode=True) or b"")))
    return out


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
            if part.is_multipart():
                continue          # Container (auch eine als Anhang weitergeleitete Mail): walk() besucht die Teile darin selbst
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition") or "")
            content_id = part.get("Content-ID")
            fn = part.get_filename()
            payload = part.get_payload(decode=True)
            if not payload: continue
            fn = decode_mime_header(fn).strip() if fn else ""

            if fn or not ct.startswith("text/"):
                typ = _anhang_typ(payload, fn, ct)
                if typ is None:
                    continue      # kein Beleg (z.B. .ics, .vcf, .gif, Signatur-Grafiken anderer Art)
                # WICHTIG: In HTML-Mails eingebettete BILDER (Logos, Icons,
                # Deko-Grafiken, Tracking-Pixel) kommen technisch auch als
                # MIME-Teil MIT Dateinamen daher (oft generisch wie
                # "image001.png") – erkennbar an Content-Disposition: inline
                # und/oder einer Content-ID (für <img src="cid:..."> im HTML).
                # ABER: Handy-Mail-Apps markieren beim direkten "Foto teilen
                # -> Mail" ein einzelnes angehängtes FOTO oft GENAUSO als
                # inline mit Content-ID (damit es direkt in der Mail
                # angezeigt wird) – dieselben technischen Merkmale wie ein
                # kleines Deko-Logo! Nur an Content-Disposition/Content-ID
                # festzumachen würde also auch echte Fotos verwerfen.
                # Deshalb zusätzlich die Dateigröße heranziehen: echte Logos/
                # Icons/Tracking-Pixel sind fast immer winzig (wenige KB),
                # eine echte Handyfoto-Aufnahme praktisch nie unter ~40 KB.
                # NUR FÜR BILDER: Ein PDF (oder eine XML-Rechnung) ist nie ein Logo.
                # Weitergeleitete Rechnungen (v.a. vom Handy) sind oft "inline"
                # markiert und klein – die wurden früher fälschlich verworfen.
                ist_bild = typ.startswith("image/")
                ist_inline = cd.lower().startswith("inline") or bool(content_id)
                if ist_bild and ist_inline and len(payload) < 40_000:
                    continue
                if ist_bild and not fn and ist_inline:
                    continue      # unbenanntes eingebettetes Bild = Deko
                name = fn or "anhang"
                if os.path.splitext(name.lower())[1] not in _ENDUNG_ZU_TYP:
                    name += _TYP_ZU_ENDUNG[typ]
                attachments.append((name, payload, typ))
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
    hinweise = []
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
                              if fn.lower().endswith((".pdf",".jpg",".jpeg",".png",".heic",".webp",".xml"))]

            if echte_anhaenge:
                # Nur Anhänge verarbeiten – Body ist nur Benachrichtigung
                for fn, payload, ct in echte_anhaenge:
                    result = await beleg_verarbeiten(payload, fn, reise_code, ct)
                    if result.get("duplikat"):
                        duplikate += 1
                    else:
                        belege_erstellt += 1
            else:
                # Kein Anhang – Mail-Body selbst ist der Beleg
                # Fehlersuche: Steht im Text etwas von einem Anhang oder hat die Mail weitere Teile,
                # zeigen wir die Teile an, damit sichtbar ist, warum nichts erkannt wurde.
                if msg.is_multipart():
                    teile = mail_teile_uebersicht(msg)
                    weitere = [t for t in teile if t[1] or not t[0].startswith("text/")]
                    nennt_anhang = re.search(r"anbei|angeh[aä]ngt|anhang|attach|beigef", body, re.I)
                    if weitere or nennt_anhang:
                        hinweise.append(
                            f"„{betreff[:70]}“: kein Beleg-Anhang erkannt, der Mailtext wurde als Beleg angelegt. "
                            "Teile der Mail: " + ("; ".join(f"{t[0]}" + (f" „{t[1]}“" if t[1] else "") + f" [{t[2]}, {max(1, t[3] // 1024)} KB]"
                                                   for t in teile) or "nur Text"))
                if body and len(body.strip()) > 50:
                    # Betreff anonymisieren für Dateinamen
                    ma_namen_tmp, ma_mails_tmp = lade_ma_daten()
                    betreff_anon = anonymisieren(betreff, ma_namen_tmp, ma_mails_tmp)
                    result = await beleg_verarbeiten(
                        full_text.encode("utf-8"),
                        f"Mail: {betreff_anon[:60]}",
                        reise_code,
                        "text/plain")
                    if result.get("duplikat"):
                        duplikate += 1
                    else:
                        belege_erstellt += 1
                        # Nur beim tatsächlich NEU angelegten Beleg die Message-ID
                        # eintragen – bei einem erkannten Duplikat bleibt der
                        # ORIGINAL-Buchungscode des bestehenden Belegs unangetastet,
                        # sonst würde er hier überschrieben und der Duplikat-Check
                        # für künftige Weiterleitungen wieder unbrauchbar gemacht.
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
            tb = traceback.format_exc()
            # Letzte 2 relevante Zeilen (Datei+Zeile) zusätzlich zur Fehlermeldung,
            # damit die genaue Fehlerstelle sichtbar ist statt nur "str(e)"
            tb_zeilen = [l for l in tb.strip().split("\n") if l.strip().startswith("File ")]
            ort = tb_zeilen[-1].strip() if tb_zeilen else ""
            fehler_liste.append(f"{e}  [{ort}]" if ort else f"{e}")
            print(f"[Mail-Import Fehler] {e}\n{tb[:2000]}")

    try:
        mail.expunge()  # Endgültig löschen
        mail.logout()
    except: pass

    return {
        "importiert": importiert,
        "belege_erstellt": belege_erstellt,
        "duplikate": duplikate,
        "fehler": len(fehler_liste),
        "fehler_details": fehler_liste,
        "hinweise": hinweise
    }


