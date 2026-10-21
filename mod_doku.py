"""
mod_doku.py – Handbuch der Reisekosten-App (Seite + PDF) und Vollständigkeitsprüfung.

EINZIGE QUELLE für die Dokumentation. Alles, was im Handbuch steht, steht hier:
  KAPITEL     Texte (Teil A Reisende, Teil B Organisatoren, Teil C Einstellungen/Betrieb)
  UMGEBUNG    jede Einstellung (Umgebungsvariable) mit Beschreibung; Stand wird live gelesen
  CRON        automatische Aufgaben
  SEITEN      jede Seite/Adresse der App

Aktuell halten (Pflicht bei JEDEM Release):
  1. Texte in KAPITEL anpassen, neue Einstellungen in UMGEBUNG, neue Seiten in SEITEN eintragen.
  2. DOKU_GEPRUEFT_FUER auf die neue Version setzen.
Die Tests schlagen fehl, wenn eine Einstellung oder Seite fehlt oder die Version nicht stimmt.

Die PDF wird bei jedem Abruf frisch erzeugt (immer aktuell).
"""
from __future__ import annotations
import glob
import io
import os
import re
from html import escape as _esc

# Für diese App-Version wurde das Handbuch zuletzt inhaltlich geprüft.
DOKU_GEPRUEFT_FUER = "3.44"

TEILE = {
    "A": "Teil A – Anleitung für Reisende",
    "B": "Teil B – Handbuch für Organisatoren",
    "C": "Teil C – Einstellungen und Betrieb",
}

# ══════════════════════════════════════════════════════════════════════════════
# EINSTELLUNGEN (Umgebungsvariablen)
# (Name, Bereich, Pflicht, Standard, Beschreibung, geheim)
# Pflicht: "ja" = ohne sie läuft die App nicht richtig, "Funktion" = nur für eine Funktion, "nein" = optional
# ══════════════════════════════════════════════════════════════════════════════
UMGEBUNG = [
    ("DATABASE_URL", "Grundbetrieb", "ja", "",
     "Adresse der PostgreSQL-Datenbank. Ohne Angabe läuft die App nur mit einer lokalen Testdatenbank.", True),
    ("SESSION_SECRET", "Grundbetrieb", "ja", "",
     "Geheimer Schlüssel für die Anmeldung (Login-Cookie). Lang und zufällig wählen (mindestens 32 Zeichen, z. B. 64 Zeichen aus Buchstaben und Ziffern). Fehlt er, nutzt die App einen aus anderen geheimen Einstellungen abgeleiteten Ersatzschlüssel: sicher, aber bitte trotzdem eintragen. Nach jeder Änderung müssen sich alle einmal neu anmelden.", True),
    ("CRON_SECRET", "Grundbetrieb", "ja", "",
     "Passwort für die automatischen Aufgaben (Adressen /cron/…?key=…). Ohne ihn lehnen alle Cron-Adressen ab.", True),
    ("PORTAL_BASE_URL", "Grundbetrieb", "nein", "",
     "Öffentliche Adresse der App (z. B. https://reisekosten.onrender.com). Daraus werden die Portal-Links gebaut. Fehlt sie, nimmt die App die von Render gesetzte Adresse. Bei eigener Domain hier eintragen.", False),
    ("RENDER_EXTERNAL_URL", "Grundbetrieb", "nein", "",
     "Wird von Render automatisch gesetzt (Adresse des Dienstes) und dient als Ersatz für PORTAL_BASE_URL. Nicht selbst eintragen.", False),
    ("OPENAI_API_KEY", "KI-Belegauswertung", "ja", "",
     "Zugangsschlüssel zu OpenAI. Ohne ihn werden Belege nicht ausgelesen.", True),
    ("OPENAI_MODEL", "KI-Belegauswertung", "nein", "gpt-4o",
     "Welches KI-Modell Belege liest. Nur ändern, wenn ein anderes Modell gewünscht ist.", False),
    ("OPENAI_REASONING_EFFORT", "KI-Belegauswertung", "nein", "",
     "Nur für neuere Modelle (GPT-5/6): Denkaufwand, z. B. „low“ für schneller und günstiger. Leer = Standard des Modells.", False),
    ("S3_ENDPOINT", "Dateispeicher (S3)", "ja", "",
     "Adresse des S3-Speichers, in dem alle Beleg-PDFs und die Backups liegen.", False),
    ("S3_BUCKET", "Dateispeicher (S3)", "ja", "",
     "Name des Speicher-Behälters (Bucket).", False),
    ("S3_ACCESS_KEY", "Dateispeicher (S3)", "ja", "",
     "Zugangskennung zum S3-Speicher.", True),
    ("S3_SECRET_KEY", "Dateispeicher (S3)", "ja", "",
     "Geheimer Schlüssel zum S3-Speicher.", True),
    ("IMAP_HOST", "Mail-Eingang (IMAP)", "ja", "",
     "Server des Reisekosten-Postfachs (z. B. imap.udag.de). Von dort werden Belege per „Mails abrufen“ geholt.", False),
    ("IMAP_USER", "Mail-Eingang (IMAP)", "ja", "",
     "Anmeldename des Postfachs (vollständige Mailadresse). Dient zugleich als Absender, wenn SMTP_USER fehlt.", False),
    ("IMAP_PASS", "Mail-Eingang (IMAP)", "ja", "",
     "Passwort des Postfachs. Gilt auch für den Versand, wenn SMTP_PASS fehlt.", True),
    ("SMTP_HOST", "Mail-Ausgang (SMTP)", "ja", "",
     "Postausgangsserver (z. B. smtp.udag.de). Für Outlook-Einladungen, Portal-Mails, Alerts und die Habel-Übergabe.", False),
    ("SMTP_PORT", "Mail-Ausgang (SMTP)", "nein", "465",
     "Port des Postausgangs. 465 = verschlüsselt (SSL), 587 = STARTTLS.", False),
    ("SMTP_USER", "Mail-Ausgang (SMTP)", "nein", "",
     "Anmeldename für den Versand – die VOLLSTÄNDIGE Mailadresse (z. B. reisekosten@herrhammer.com). Fehlt er, gilt IMAP_USER.", False),
    ("SMTP_PASS", "Mail-Ausgang (SMTP)", "nein", "",
     "Passwort für den Versand. Fehlt es, gilt IMAP_PASS.", True),
    ("SMTP_TIMEOUT", "Mail-Ausgang (SMTP)", "nein", "30",
     "Wartezeit in Sekunden beim Senden einer Mail (große Anhänge brauchen länger). Der Verbindungsaufbau wartet fest höchstens 15 Sekunden.", False),
    ("DMS_EMAIL_TO", "Habel-Übergabe", "Funktion", "",
     "Empfängeradresse der DMS-Stelle (Habel). Ohne sie ist „An Habel“ gesperrt.", False),
    ("HABEL_MAX_MB", "Habel-Übergabe", "nein", "15",
     "Größte erlaubte Habel-Mail in MB (Summe aller PDF-Anhänge). Base64 macht die Mail etwa ein Drittel größer.", False),
    ("PORTAL_TAGE_VORHER", "Reisenden-Portal", "nein", "5",
     "So viele Tage vor der Abreise geht der Portal-Link automatisch per Mail an die Reisenden.", False),
    ("KALENDER_ABSENDER", "Outlook-Einladungen", "nein", "",
     "Absenderadresse der Einladungen. Leer = SMTP_USER.", False),
    ("KALENDER_RUHE_SEKUNDEN", "Outlook-Einladungen", "nein", "120",
     "Änderungen werden erst gesendet, wenn sie so viele Sekunden unverändert geblieben sind (verhindert eine Mail-Flut beim Korrigieren). 0 = sofort.", False),
    ("AERODATABOX_API_KEY", "Flug-/Bahn-Alerts", "Funktion", "",
     "Zugang zur Flugdaten-Schnittstelle (AeroDataBox über RapidAPI). Ohne ihn keine Flug-Verspätungsalarme.", True),
    ("AERODATABOX_HOST", "Flug-/Bahn-Alerts", "nein", "aerodatabox.p.rapidapi.com",
     "Server der Flugdaten-Schnittstelle. Normalerweise nicht ändern.", False),
    ("DB_TIMETABLES_CLIENT_ID", "Flug-/Bahn-Alerts", "Funktion", "",
     "Client-ID der Deutschen Bahn (Timetables-API). Ohne sie keine Bahn-Verspätungsalarme.", True),
    ("DB_TIMETABLES_API_KEY", "Flug-/Bahn-Alerts", "Funktion", "",
     "API-Schlüssel der Deutschen Bahn (Timetables-API).", True),
    ("GOOGLE_PLACES_API_KEY", "Termine", "nein", "",
     "Schlüssel für Google-Adressvorschläge im Termin-Formular. Ohne ihn wird die Adresse von Hand eingetippt.", True),
]

# Einstellungen, die ersatzweise aus einer anderen Einstellung gelesen werden
_ERSATZ_EINSTELLUNG = {"SMTP_USER": "IMAP_USER", "SMTP_PASS": "IMAP_PASS"}

# ══════════════════════════════════════════════════════════════════════════════
# AUTOMATISCHE AUFGABEN (externer Cron-Dienst, z. B. cron-job.org)
# (Adresse, Takt, Zweck)
# ══════════════════════════════════════════════════════════════════════════════
CRON = [
    ("/cron/flug-alerts?key=…", "jede Minute",
     "Prüft Flüge und Züge auf Verspätung (feste Prüfzeitpunkte) und gleicht die Outlook-Einladungen ab. Die feinste Stufe (15 Minuten vor Abflug) greift nur bei minütlichem Aufruf."),
    ("/cron/mails?key=…", "alle 5 Minuten",
     "Holt neue Belege aus dem Reisekosten-Postfach, genau wie der Knopf „📬 Mails abrufen“. Es läuft immer nur ein Abruf gleichzeitig."),
    ("/cron/portal-mails?key=…", "einmal täglich",
     "Schickt den Portal-Link an alle Reisenden, deren Reise in PORTAL_TAGE_VORHER Tagen beginnt und die noch keinen Link bekommen haben. Bestandsreisen (angelegt vor dem 16.10.2026) werden übersprungen."),
    ("/cron/backup?key=…", "einmal täglich (nachts)",
     "Sichert alle Tabellen komprimiert in den S3-Speicher. Die letzten 30 Sicherungen bleiben erhalten."),
    ("/cron/kalender?key=…", "optional",
     "Gleicht nur die Outlook-Einladungen ab. Nicht nötig, weil das bereits im Lauf von /cron/flug-alerts geschieht."),
    ("/health", "optional",
     "Leichte Wach-Adresse ohne Anmeldung und ohne Datenbank (zeigt nur „ok“). Auf einer bezahlten Render-Instanz nicht nötig."),
]

# ══════════════════════════════════════════════════════════════════════════════
# SEITENVERZEICHNIS – jede GET-Adresse der App (Adresse, Zweck, Kapitel-ID)
# ══════════════════════════════════════════════════════════════════════════════
SEITEN = [
    ("/", "Dashboard: Reisen (aktiv, geplant, abgeschlossen), offene Alerts, Posteingang, Mails abrufen", "b-dashboard"),
    ("/login", "Anmeldung der Organisatoren mit Kürzel und Passwort", "b-rollen"),
    ("/logout", "Abmelden", "b-rollen"),
    ("/setup", "Ersteinrichtung: erstes Passwort setzen (nur solange noch niemand eines hat)", "b-rollen"),
    ("/mitarbeiter", "Liste aller Mitarbeiter", "b-mitarbeiter"),
    ("/mitarbeiter/neu", "Mitarbeiter anlegen", "b-mitarbeiter"),
    ("/mitarbeiter/{kuerzel}/bearbeiten", "Mitarbeiter bearbeiten, Kürzel ändern, Passwort setzen, Visa verwalten", "b-mitarbeiter"),
    ("/mitarbeiter/{kuerzel}/visum/neu", "Visum eines Mitarbeiters erfassen (Land, gültig von/bis, Kopie)", "b-mitarbeiter"),
    ("/mitarbeiter/{kuerzel}/visum/{vid}/bearbeiten", "Visum ändern, Kopie ersetzen oder entfernen", "b-mitarbeiter"),
    ("/mitarbeiter/{kuerzel}/visum/{vid}/ansehen", "Kopie des Visums ansehen", "b-mitarbeiter"),
    ("/reisen", "Liste aller Reisen", "b-reise"),
    ("/reisen/neu", "Neue Reise anlegen", "b-reise"),
    ("/reisen/naechster-code", "Hilfsadresse: nächster freier Reisecode (technisch)", "b-reise"),
    ("/reise/{code}", "Reise-Seite: Kennzahlen, Portal-Zugänge, Tagesverlauf und VMA", "b-reise"),
    ("/reise/{code}/bearbeiten", "Reise bearbeiten (Titel, Daten, Reisende, Notiz)", "b-reise"),
    ("/reise/{code}/land/neu", "Land zur Reise hinzufügen", "b-reise"),
    ("/reise/{code}/land/{lid}/bearbeiten", "Land der Reise ändern", "b-reise"),
    ("/reise/{code}/land/{lid}/loeschen", "Land aus der Reise entfernen", "b-reise"),
    ("/reise/{code}/termin/neu", "Termin oder Ereignis anlegen", "b-plan"),
    ("/reise/{code}/termin/{tid}/bearbeiten", "Termin ändern", "b-plan"),
    ("/reise/{code}/termin/{tid}/loeschen", "Termin löschen", "b-plan"),
    ("/reise/{code}/uebersicht", "Alte Adresse, leitet auf die Reise-Seite um", "b-reise"),
    ("/reise/{code}/reiseplan", "Reiseplan: alle Flüge, Züge und Termine in zeitlicher Reihenfolge", "b-plan"),
    ("/reise/{code}/reiseplan/pdf", "Reiseplan als PDF: Reisemappe (mit Anlagen: Reise-PDFs und A1) oder zum Mailversand (ohne Anlagen)", "b-reisemappe"),
    ("/reise/{code}/ueberschneidungen", "Überschneidende Flug-/Zug-Segmente aus verschiedenen Belegen klären", "b-plan"),
    ("/reise/{code}/vma-generieren", "VMA-Tage der Reise neu berechnen", "b-vma"),
    ("/reise/{code}/vma-debug", "Fehlersuche: Wie das Land des Rückreisetags ermittelt wurde", "b-vma"),
    ("/reise/{code}/abschluss", "Reise abschließen: VMA, Belege, Zeiten, Kilometer", "b-abschluss"),
    ("/reise/{code}/abschluss/pdf", "Reisekostenabrechnung als PDF", "b-abschluss"),
    ("/reise/{code}/habel", "Übergabe an Habel: Prüfstand und Senden", "b-habel"),
    ("/reise/{code}/habel/vorschau", "Vorschau der Habel-Mail (sendet nichts)", "b-habel"),
    ("/reise/{code}/habel/datei/{datei}", "Einzelne Datei der Habel-Mail öffnen", "b-habel"),
    ("/reise/{code}/kalender", "Outlook-Einladungen der Reise: Status und „Jetzt senden“", "b-kalender"),
    ("/reise/{code}/vorschuss", "Reisevorschuss und Bargeld-Konto der Reise", "b-vorschuss"),
    ("/reise/{code}/vorschuss/neu", "Vorschuss-Buchung erfassen (Auszahlung/Rückgabe)", "b-vorschuss"),
    ("/reise/{code}/vorschuss/{bid}/bearbeiten", "Vorschuss-Buchung bearbeiten (nur Entwurf)", "b-vorschuss"),
    ("/vorschuss/{bid}", "Vorschuss-Buchung ansehen", "b-vorschuss"),
    ("/vorschuss/{bid}/pdf", "Vorschuss-Quittung als PDF", "b-vorschuss"),
    ("/bewirtung/{bid}", "Bewirtungsbeleg ansehen (Organisator)", "b-bewirtung"),
    ("/bewirtung/{bid}/pdf", "Bewirtungsbeleg als PDF", "b-bewirtung"),
    ("/belege", "Belegliste mit Filtern", "b-belegliste"),
    ("/beleg/upload", "Beleg von Hand hochladen", "b-eingang"),
    ("/beleg/{bid}", "Beleg-Seite: Analyse, Dokumente, Zuordnung, Prüfung", "b-beleg"),
    ("/beleg/{bid}/daten-bearbeiten", "Beleg-Daten nachtragen oder korrigieren", "b-beleg"),
    ("/beleg/{bid}/betrag/bearbeiten", "Betrag und Währung des Belegs ändern", "b-betrag"),
    ("/beleg/{bid}/segment/{idx}/umbuchen", "Flug-/Zug-Segment umbuchen (Zeiten, Nummer, Sitz)", "b-plan"),
    ("/beleg/{bid}/flugstatus/zuruecksetzen", "Gespeicherten Flug-/Bahnstatus des Belegs löschen", "b-alerts"),
    ("/beleg/{bid}/loeschen", "Beleg löschen (mit Rückfrage)", "b-beleg"),
    ("/beleg/{bid}/pruef-pdf", "Beleg mit Prüf-Deckblatt als PDF", "b-beleg"),
    ("/beleg/{bid}/pdf/{typ}", "Beleg-PDF öffnen: original, anon (anonymisiert) oder analyse", "b-beleg"),
    ("/beleg/{bid}/erechnung-xml", "Eingebettete eRechnung-XML im Original herunterladen", "b-eingang"),
    ("/mails-abrufen", "Mails aus dem Reisekosten-Postfach abrufen und als Belege anlegen", "b-eingang"),
    ("/unzugeordnet", "Posteingang: Belege ohne Reise einer Reise zuordnen", "b-eingang"),
    ("/todo", "Offene Aufgaben: alles, was vor der Abrechnung noch fehlt", "b-dashboard"),
    ("/maps", "Karte: Standorte der Mitarbeiter bei laufenden Reisen", "b-dashboard"),
    ("/vma", "Amtliche Verpflegungssätze einsehen und importieren", "b-vma"),
    ("/vma-saetze/info/{land_code}", "Hilfsadresse: Sätze eines Landes als Daten (technisch)", "b-vma"),
    ("/einstellungen", "Einstellungsübersicht (Kacheln)", "c-einstellungen"),
    ("/einstellungen/versionen", "Änderungsverlauf aller Versionen", "c-verlauf"),
    ("/einstellungen/alerts", "Flug-/Bahn-Alerts: Prüfplan und Testlauf", "b-alerts"),
    ("/einstellungen/backups", "Datensicherungen ansehen und sofort anstoßen", "c-backup"),
    ("/einstellungen/kreditkarten", "Kreditkarten-Abrechnung hochladen und prüfen", "b-kk"),
    ("/einstellungen/kreditkarten/zuordnungen", "Alle Zuordnungen von Abrechnungspositionen zu Belegen", "b-kk"),
    ("/einstellungen/kreditkarten/ausland", "Kreditkarten-Belege Ausland: Liste zum Abgleich mit der VISA-Abrechnung", "b-kk"),
    ("/einstellungen/kreditkarten/ausland/csv", "Dieselbe Liste als CSV (Excel)", "b-kk"),
    ("/einstellungen/zuschlaege", "Zuschläge einstellen: Sätze, Nacht, Feiertage, Probe", "b-zuschlag"),
    ("/einstellungen/reise-pdfs", "Reise-PDFs: Dokumente hochladen, die in jede Reisemappe kommen (z. B. Krankenversicherung)", "b-reisemappe"),
    ("/einstellungen/reise-pdfs/{pid}/ansehen", "Ein hochgeladenes Reise-PDF ansehen", "b-reisemappe"),
    ("/einstellungen/mail-diagnose", "Mail-Diagnose: Postausgang Schritt für Schritt prüfen", "c-mail"),
    ("/einstellungen/dokumentation", "Dieses Handbuch", "c-pflege"),
    ("/einstellungen/dokumentation/pdf", "Dieses Handbuch als PDF", "c-pflege"),
    ("/portal/{token}", "Portal-Startseite der Reisenden: Tage, Kilometer, Bewirtung, Vorschuss", "a-start"),
    ("/portal/{token}/anleitung", "Anleitung für Reisende (Teil A)", "a-start"),
    ("/portal/{token}/anleitung/pdf", "Anleitung für Reisende als PDF", "a-start"),
    ("/portal/{token}/beleg-hochladen", "Reisende laden einen Beleg hoch", "a-belege"),
    ("/portal/{token}/kilometer/{kid}/loeschen", "Kilometer-Eintrag löschen", "a-km"),
    ("/portal/{token}/unterlagen/{pid}", "Reise-PDF (z. B. Krankenversicherung) im Portal ansehen", "a-start"),
    ("/portal/{token}/a1", "Eigene A1-Bescheinigung im Portal ansehen (nur bei Reisen in Europa)", "a-start"),
    ("/portal/{token}/visum/{vid}", "Eigenes Visum im Portal ansehen (nur für ein Land dieser Reise)", "a-start"),
    ("/portal/{token}/bewirtung/neu", "Bewirtungsbeleg im Portal anlegen", "a-bewirtung"),
    ("/portal/{token}/bewirtung/{bid}/bearbeiten", "Bewirtungsbeleg-Entwurf bearbeiten", "a-bewirtung"),
    ("/portal/{token}/bewirtung/{bid}/pruefen", "Bewirtungsbeleg vor dem Bestätigen prüfen", "a-bewirtung"),
    ("/portal/{token}/bewirtung/{bid}", "Bestätigten Bewirtungsbeleg ansehen", "a-bewirtung"),
    ("/portal/{token}/bewirtung/{bid}/pdf", "Bewirtungsbeleg als PDF", "a-bewirtung"),
    ("/portal/{token}/vorschuss/neu", "Geldautomat/Wechselstube eintragen", "a-bargeld"),
    ("/portal/{token}/vorschuss/{bid}/bearbeiten", "Eigene Bargeld-Buchung bearbeiten", "a-bargeld"),
    ("/portal/{token}/vorschuss/{bid}/bestaetigen", "Auszahlung oder Rückgabe bestätigen", "a-bargeld"),
    ("/portal/{token}/vorschuss/{bid}", "Bestätigte Bargeld-Buchung ansehen", "a-bargeld"),
    ("/portal/{token}/vorschuss/{bid}/pdf", "Bargeld-Quittung als PDF", "a-bargeld"),
    ("/cron/flug-alerts", "Automatik: Verspätungsprüfung und Outlook-Abgleich", "c-cron"),
    ("/cron/mails", "Automatik: Mails abrufen und als Belege anlegen", "c-cron"),
    ("/cron/portal-mails", "Automatik: Portal-Links verschicken", "c-cron"),
    ("/cron/kalender", "Automatik: Outlook-Einladungen abgleichen", "c-cron"),
    ("/cron/backup", "Automatik: Datensicherung", "c-cron"),
    ("/health", "Wach-Adresse ohne Anmeldung", "c-system"),
    ("/version", "Zeigt Version und Datenbanktyp (Daten)", "c-system"),
    ("/init", "Legt fehlende Tabellen/Spalten an und führt Reparaturen aus (löscht nichts)", "c-system"),
    ("/init-reset", "NOTFALL-Funktion: löscht ALLE Daten. Nie im Normalbetrieb verwenden", "c-system"),
    ("/test-openai", "Prüft die Verbindung zu OpenAI (Daten)", "c-system"),
    ("/debug-anon", "Zeigt, welche Namen und Mailadressen anonymisiert werden (Daten)", "c-system"),
]

# FastAPI-eigene Adressen, die nicht zur App gehören
_FREMDE_ADRESSEN = {"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"}


# ══════════════════════════════════════════════════════════════════════════════
# KAPITEL – die Texte
# Bausteine: ("p", Text) · ("h3", Überschrift) · ("ul", [..]) · ("ol", [..]) ·
#            ("tab", [Kopf..], [[Zeile..]..], [Spaltenbreiten..]) ·
#            ("hinweis", Text) · ("warnung", Text) · ("live", Name)
# Im Text: **fett**, `Code`, [[Variable]] (wird beim Erzeugen ersetzt)
# ══════════════════════════════════════════════════════════════════════════════
KAPITEL = [
    # ─────────────────────────── TEIL A – Reisende (du) ───────────────────────────
    {"id": "a-start", "teil": "A", "titel": "So funktioniert es", "bloecke": [
        ("p", "Für jede Reise bekommst du per E-Mail einen **persönlichen Link** (das Reisenden-Portal). "
              "Er funktioniert auf Handy und Computer, ohne Anmeldung. Hier trägst du ein, was für die "
              "Reisekostenabrechnung gebraucht wird. Du kannst alle Angaben jederzeit über denselben Link ändern."),
        ("hinweis", "Der Link gehört nur dir. Bitte nicht weitergeben. Er zeigt nur deine eigene Reise."),
        ("p", "Den Link bekommst du automatisch [[portal_tage]] Tage vor der Abreise per Mail. "
              "Ist er nicht angekommen, melde dich bei deinem Organisator."),
        ("h3", "Was du tun musst"),
        ("ol", [
            "**Belege hochladen** (Rechnungen, Quittungen, Buchungen) – am besten gleich nach dem Bezahlen.",
            "**Jeden Reisetag ausfüllen:** welche Mahlzeiten gestellt wurden und deine Zeiten. Danach **einmal unten speichern**.",
            "**Kilometer eintragen**, falls du mit Firmen- oder Privat-Pkw gefahren bist.",
            "**Bewirtungsbeleg erstellen**, wenn du Gäste eingeladen hast.",
            "**Bargeld bestätigen** (Vorschuss, Rückgabe) und selbst abgehobenes Geld eintragen.",
        ]),
        ("h3", "Reiseunterlagen"),
        ("p", "Oben im Portal findest du die Karte **📄 Reiseunterlagen**: deine **Krankenversicherung** und weitere Dokumente zur Reise, bei Reisen in Europa auch deine **A1-Bescheinigung**. Antippen öffnet das PDF, so hast du es unterwegs auf dem Handy dabei. Fehlt die A1 noch, steht dort ein Hinweis; schicke sie dann über „Beleg hochladen“."),
        ("p", "Nach der Reise prüft dein Organisator alles und erstellt die Abrechnung. Diese Anleitung erreichst du im "
              "Portal jederzeit über „📖 Anleitung“."),
    ]},
    {"id": "a-tage", "teil": "A", "titel": "Reisetage ausfüllen", "bloecke": [
        ("p", "Auf der Startseite des Portals steht für **jeden Reisetag** eine Karte mit Datum und Land. "
              "Das Land wird automatisch aus deinen Flügen, Hotels und der Reiseplanung ermittelt."),
        ("tab", ["Feld", "Was du einträgst"], [
            ["🍳 Frühstück / 🍽 Mittagessen / 🌙 Abendessen",
             "Haken setzen, wenn die Mahlzeit **gestellt** wurde (im Hotelpreis enthalten, vom Kunden bezahlt, im Flug serviert o. Ä.). "
             "Mahlzeiten, die du selbst bezahlt hast, bleiben ohne Haken."],
            ["Reisebeginn / Reiseende", "Uhrzeit, wann du tatsächlich losgefahren bzw. angekommen bist. Hinter der Uhrzeit steht die **Zeitzone** (Standard: Ortszeit)."],
            ["Arbeitsbeginn / Arbeitsende", "Uhrzeit, wann die Arbeit am Reiseziel begann und endete."],
            ["Notiz", "Freie Bemerkung, z. B. „Flug 2 Stunden verspätet“."],
        ], [34, 66]),
        ("h3", "Zeitzone der Uhrzeiten"),
        ("p", "Alle Uhrzeiten gelten als **Ortszeit** des Landes, das auf der Tageskarte steht. Du musst dafür nichts auswählen. "
              "Nur wenn du eine Zeit in einer **anderen Zeitzone** angibst, wählst du hinter der Uhrzeit im Menü die passende aus, "
              "z. B. „Deutsche Zeit“. Die Stunden werden dann richtig umgerechnet."),
        ("hinweis", "Beispiel: Du fliegst um 06:00 Uhr in Deutschland ab und landest um 09:00 Uhr Ortszeit in Rumänien. "
                    "Steht auf der Tageskarte **Deutschland**, bleibt der Reisebeginn auf Ortszeit, und beim Reiseende wählst du „Osteuropa … (Bukarest)“. "
                    "Steht auf der Karte **Rumänien**, wählst du beim Reisebeginn „Deutsche Zeit“. Gerechnet werden in beiden Fällen 2 Stunden Reisezeit."),
        ("p", "Du kannst **alle Tage nacheinander ausfüllen**, ohne dass die Seite neu lädt. Ganz am Ende tippst du **einmal** auf "
              "**💾 Alle Änderungen speichern**. Der Knopf bleibt am unteren Bildschirmrand sichtbar. Damit werden alle Tage und die Kilometer zusammen gesichert. "
              "Danach erscheint „✓ Alles gespeichert“. Du kannst jederzeit zurückkommen und ändern."),
        ("hinweis", "Erst mit **Alle Änderungen speichern** ist etwas gesichert. Bis dahin steht unten „⚠ Ungespeicherte Änderungen“. "
                    "Verlässt du die Seite vorher (z. B. über „Beleg hochladen“), fragt der Browser nach."),
        ("hinweis", "Warum die Mahlzeiten wichtig sind: Der Verpflegungsmehraufwand (Tagegeld) wird gekürzt, wenn Mahlzeiten gestellt wurden – "
                    "Frühstück um 20 %, Mittag- und Abendessen um je 40 % des vollen Tagessatzes des Landes."),
        ("p", "Die Markierung **½ Satz** an einem Tag bedeutet, dass an diesem Tag nur der halbe Satz gilt (z. B. An- oder Abreisetag)."),
    ]},
    {"id": "a-belege", "teil": "A", "titel": "Belege hochladen", "bloecke": [
        ("ol", [
            "Im Portal auf **„📎 Beleg hochladen“** tippen.",
            "Datei wählen: PDF oder Foto/Scan (JPG, PNG, TIF, HEIC, WEBP). Mehrseitige Scans werden komplett gelesen (bis zu 4 Seiten).",
            "**Hochladen** tippen. Es erscheint „Danke!“.",
            "Mit „Weiteren Beleg hochladen“ geht es direkt mit dem nächsten Beleg weiter.",
        ]),
        ("p", "Der Beleg wird automatisch ausgelesen und landet bei deinem Organisator zur Prüfung. Er wird deiner Reise zugeordnet – "
              "du musst nichts auswählen."),
        ("h3", "Welche Belege"),
        ("ul", [
            "Rechnungen und Quittungen (Hotel, Taxi, Mietwagen, Tanken, Bahn, Flug …)",
            "Buchungsbestätigungen von Flügen und Zügen",
            "Bewirtungsrechnungen (Originalbeleg – dazu mehr im Kapitel Bewirtungsbeleg)",
        ]),
        ("h3", "Tipps"),
        ("ul", [
            "Fotos: den ganzen Beleg aufnehmen, gut beleuchtet und gerade.",
            "Ein Beleg pro Datei. Doppelt hochgeladene Belege werden erkannt („Duplikat“) und nicht doppelt gezählt.",
            "Buchungsbestätigungen kannst du auch per Mail weiterleiten an **[[postfach]]**. Schreibe den Reisecode (z. B. 26-007) in den Betreff.",
            "Etwas falsch hochgeladen? Sag deinem Organisator Bescheid. Er kann Belege ändern oder löschen.",
        ]),
    ]},
    {"id": "a-km", "teil": "A", "titel": "Gefahrene Kilometer", "bloecke": [
        ("p", "Die Karte **„🚗 Gefahrene Kilometer“** brauchst du nur, wenn du mit einem **Firmen- oder deinem privaten Pkw** gefahren bist. "
              "Bei Mietwagen steht alles auf der Mietwagenrechnung, dort trägst du nichts ein."),
        ("ol", [
            "Kennzeichen eintragen (z. B. WÜ-AB 123).",
            "Kilometer eintragen (z. B. 85).",
            "Mit mehreren Fahrzeugen: **+ Weitere Fahrt** tippen und eine neue Zeile ausfüllen.",
            "Ganz unten **💾 Alle Änderungen speichern** tippen.",
        ]),
        ("p", "Eine gespeicherte Fahrt löschst du so: Haken bei **🗑 löschen** setzen und unten speichern. Ein Datum ist nicht nötig."),
    ]},
    {"id": "a-bewirtung", "teil": "A", "titel": "Bewirtungsbeleg (Gäste eingeladen)", "bloecke": [
        ("p", "Nur wenn du **Gäste** (z. B. Kunden) eingeladen hast. Eigene Mahlzeiten auf der Reise gehören **nicht** hierher. "
              "Die Rechnung oder der Kassenbon bleibt immer der Originalbeleg."),
        ("ol", [
            "**Zuerst den Originalbeleg hochladen** (Kapitel „Belege hochladen“).",
            "Im Portal bei „🍽 Bewirtungsbelege“ auf **+ Bewirtungsbeleg erstellen** tippen.",
            "Den Originalbeleg aus der Liste wählen. Tag, Ort, Betrag und Währung kommen automatisch aus dem Beleg.",
            "**Anlass** und **Teilnehmer** (Gäste, Firma) eintragen.",
            "Nur wenn die Rechnung **mehr als die Bewirtung** enthält (z. B. Hotelrechnung mit Übernachtung und Abendessen): Im Feld **„Davon Bewirtung“** den Betrag der Bewirtung eintragen (z. B. 70,00). Ist die ganze Rechnung Bewirtung, das Feld **leer lassen**.",
            "Speichern, auf der Prüfseite alles kontrollieren und **elektronisch bestätigen** (Haken, Name, optional Unterschrift).",
        ]),
        ("p", "Mit „Davon Bewirtung“ weist der Abschluss Hotel und Bewirtung **getrennt** aus (z. B. Hotel 450 €, Bewirtung 70 €). Die Gesamtsumme der Reise bleibt unverändert."),
        ("p", "Nach dem Bestätigen ist der Beleg **gesperrt**. Eine Änderung ist nur als **neue Version** („Korrektur“) möglich. "
              "Du kannst den Bewirtungsbeleg jederzeit als PDF öffnen."),
        ("hinweis", "Hinweise erscheinen, sperren aber nichts: Bei Rechnungen ab 250 € muss auf der Rechnung der Name unserer Firma (des Bewirtenden) stehen – "
                    "bitte vor dem Bezahlen prüfen lassen. Liegt die Bewirtung mehr als 14 Tage zurück, erscheint eine Warnung (Beleg zeitnah erstellen)."),
    ]},
    {"id": "a-bargeld", "teil": "A", "titel": "Reisevorschuss und Bargeld", "bloecke": [
        ("p", "Die Karte **„💵 Reisevorschuss / Bargeld“** zeigt dein Bargeld-Konto der Reise."),
        ("ul", [
            "**Vorschuss oder Rückgabe:** Dein Organisator erfasst die Auszahlung bzw. Rückgabe. Du siehst sie in der Karte mit "
            "dem Hinweis „Buchung(en) warten auf deine Bestätigung“. Antippen, Angaben prüfen und bestätigen.",
            "**Selbst Geld abgehoben oder gewechselt** (Geldautomat, Wechselstube): **+ Geldautomat / Wechselstube eintragen** tippen "
            "und Betrag sowie den belasteten Euro-Betrag laut Kontoauszug eintragen.",
        ]),
        ("p", "In die Abrechnung zählen **nur bestätigte** Buchungen. Nach dem Bestätigen ist die Buchung gesperrt; Änderungen gehen nur als neue Version."),
        ("p", "Unter den Buchungen steht eine Zusammenfassung (ausgezahlt, selbst beschafft, Rückgabe offen, Erstattung)."),
    ]},
    {"id": "a-nachrichten", "teil": "A", "titel": "Mails, die du bekommst", "bloecke": [
        ("tab", ["Mail", "Wann", "Was du tun musst"], [
            ["Portal-Link", "[[portal_tage]] Tage vor der Abreise", "Link speichern und Reisetage ausfüllen."],
            ["Outlook-Einladung", "Für jeden Flug, Zug und Termin – wenn dein Organisator das für dich aktiviert hat",
             "Nichts. Änderungen und Stornos werden automatisch nachgeführt (aktualisierter oder abgesagter Termin)."],
            ["Verspätungs-Alarm", "Flug oder Zug ist ab 15 Minuten verspätet, storniert oder umgeleitet",
             "Nichts, nur zur Information. Auch die Organisatoren bekommen den Alarm."],
        ], [22, 38, 40]),
    ]},
    {"id": "a-faq", "teil": "A", "titel": "Häufige Fragen", "bloecke": [
        ("tab", ["Frage", "Antwort"], [
            ["„Link ungültig oder abgelaufen“", "Der Link stimmt nicht mehr (z. B. weil dein Kürzel geändert wurde). Der Organisator schickt dir einen neuen."],
            ["Ich kenne eine Zeit nur in deutscher Zeit (z. B. aus Outlook), bin aber im Ausland.", "Uhrzeit so eintragen und hinter der Uhrzeit **Deutsche Zeit** wählen. Ohne Auswahl gilt Ortszeit."],
            ["Ich habe eine Mahlzeit vergessen.", "Link öffnen, Haken setzen, unten **Alle Änderungen speichern**. Das geht jederzeit."],
            ["Ich habe etwas geändert, aber nicht gespeichert.", "Unten auf **💾 Alle Änderungen speichern** tippen. Solange dort „⚠ Ungespeicherte Änderungen“ steht, ist noch nichts gesichert."],
            ["Ich habe einen falschen Beleg hochgeladen.", "Dem Organisator Bescheid geben. Er löscht oder korrigiert den Beleg."],
            ["Der Beleg erscheint nicht im Portal.", "Das ist normal. Hochgeladene Belege sehen nur die Organisatoren."],
            ["Ich war mit dem Mietwagen unterwegs.", "Nichts eintragen. Kilometer sind nur für Firmen- oder Privat-Pkw."],
            ["Ich habe Bargeld vom Organisator bekommen.", "In der Karte „Reisevorschuss / Bargeld“ antippen und bestätigen."],
            ["Was gehört in meine Reisemappe?", "Der Reiseplan, hinten angehängt deine Krankenversicherungs-Kopie und bei Reisen in Europa deine **A1-Bescheinigung**. Die A1 musst du auf der Reise dabeihaben. Fehlt sie noch, sag dem Organisator Bescheid oder schicke sie ihm."],
            ["Wo finde ich meine Krankenversicherung oder die A1-Bescheinigung?", "Im Portal oben in der Karte **📄 Reiseunterlagen**. Die A1 gibt es nur bei Reisen in Europa. Steht dort, dass sie noch fehlt, lade sie über „Beleg hochladen“ hoch oder sag dem Organisator Bescheid."],
            ["Der Organisator hat etwas geändert (z. B. Mahlzeit oder Land), im Portal sehe ich es noch nicht.", "Das Portal neu öffnen bzw. die Seite neu laden. Änderungen des Büros erscheinen dann auch bei dir."],
            ["Wohin mit Fragen?", "An deinen Organisator."],
        ], [38, 62]),
    ]},

    # ────────────────────── TEIL B – Organisatoren (Sie) ──────────────────────
    {"id": "b-rollen", "teil": "B", "titel": "Anmeldung, Rollen und Zugriff", "bloecke": [
        ("p", "In die App anmelden (mit **Kürzel und Passwort**) können sich nur **Organisatoren**. Die Anmeldung bleibt 14 Tage bestehen. "
              "**Reisende bekommen kein Passwort**, sondern nur ihren persönlichen Portal-Link (per Mail)."),
        ("hinweis", "Vergeben Sie Passwörter deshalb nur an Organisatoren. Ein Mitarbeiter ohne Passwort kann sich nicht anmelden."),
        ("h3", "Erstes Passwort"),
        ("p", "Solange noch niemand ein Passwort hat, führt die App automatisch auf `/setup`. Dort wird das erste Passwort gesetzt. "
              "Danach setzt ein Organisator die Passwörter der weiteren Organisatoren: Mitarbeiter bearbeiten › „🔒 Login-Passwort“ (mindestens 8 Zeichen)."),
        ("h3", "Rollen"),
        ("tab", ["Rolle", "Bedeutung"], [
            ["✈ Reisender", "Fährt selbst. Meldet sich nicht an, sondern bekommt nur den Portal-Link sowie Outlook-Einladungen und Verspätungs-Alarme. Wird bei Reisen als Teilnehmer ausgewählt."],
            ["📋 Organisator", "Bucht für andere. Darf Beleg-Daten nachtragen, Belege prüfen, Vorschuss erfassen, an Habel übergeben, Outlook-Einladungen steuern "
                              "und alle Einstellungen verwenden. Erhält Verspätungs-Alarme aller Reisen."],
        ], [24, 76]),
        ("p", "Beide Rollen gleichzeitig sind möglich. Die Seiten unter „⚙ Einstellungen“ sowie Vorschuss-Erfassung, Outlook, Habel und Mail-Diagnose sind Organisatoren vorbehalten."),
    ]},
    {"id": "b-ablauf", "teil": "B", "titel": "Der Ablauf einer Reise im Überblick", "bloecke": [
        ("ol", [
            "**Mitarbeiter anlegen** (einmalig) – Rolle, E-Mail-Adressen, Kreditkarte.",
            "**Reise anlegen** – Titel, Zeitraum, Reisende, Länder. Der Reisecode wird automatisch vergeben.",
            "**Portal-Links:** gehen [[portal_tage]] Tage vor Abreise automatisch raus oder werden auf der Reise-Seite von Hand gesendet.",
            "**Belege sammeln:** per Mail an das Reisekosten-Postfach, per Upload oder durch die Reisenden im Portal. Belege ohne Reise landen im Posteingang.",
            "**Belege ergänzen:** Belegart, Bezahlart, Beträge und Wechselkurse auf der Beleg-Seite.",
            "**Reiseplan pflegen:** Flüge, Züge und Termine; Änderungen und Überschneidungen klären. Outlook-Einladungen und Alarme laufen automatisch.",
            "**Nach der Reise:** Tagesverlauf und VMA prüfen, Kreditkartenabrechnung einspielen, Vorschuss abrechnen.",
            "**Belege prüfen** (Prüfvermerk), dann **Abschluss** öffnen und die Abrechnung als PDF erzeugen.",
            "**An Habel übergeben** – eine Mail je Reise mit Abrechnung und allen geprüften Belegen.",
        ]),
        ("hinweis", "Was noch fehlt, zeigt die Seite „📋 Offene Aufgaben“ (`/todo`)."),
    ]},
    {"id": "b-mitarbeiter", "teil": "B", "titel": "Mitarbeiter verwalten", "bloecke": [
        ("p", "Menü **Mitarbeiter** › „Neu“ bzw. Stift-Symbol zum Bearbeiten."),
        ("tab", ["Feld", "Bedeutung"], [
            ["Kürzel", "2–5 Buchstaben, eindeutig (z. B. RD). Später änderbar über „Kürzel ändern“: Das Kürzel wird überall ersetzt (Reisen, Portal-Zugänge, Login), "
                       "die Historie bleibt. **Alte Portal-Links werden ungültig** und müssen neu gesendet werden."],
            ["Klarname", "Voller Name. Wird auch bei der Anonymisierung der Belege berücksichtigt."],
            ["E-Mail-Adresse 1", "Hauptadresse: Empfang von Portal-Link, Outlook-Einladungen und Alarmen. Muss eingetragen sein, sonst ist „Link senden“ gesperrt."],
            ["E-Mail-Adresse 2 / 3", "Weitere Adressen (optional). Sie werden bei der Anonymisierung der Belege mit berücksichtigt."],
            ["📅 Outlook-Einladung", "Haken: Der Mitarbeiter bekommt für jeden Flug, Zug und manuellen Termin eine Einladung an E-Mail-Adresse 1."],
            ["Rolle", "✈ Reisender und/oder 📋 Organisator (siehe „Anmeldung, Rollen und Zugriff“)."],
            ["Kreditkartentyp", "Privat- oder Firmenkreditkarte. Bei Bezahlart „Kreditkarte“ wird die Karte als „Privatkreditkarte-KÜRZEL“ bzw. „Firmenkreditkarte-KÜRZEL“ angeboten."],
            ["Auslandseinsatzentgelt (%)", "Gebühr der Bank bei Zahlungen in Fremdwährung (Standard 1,5). Wird bei Kreditkartenbelegen in Fremdwährung automatisch berechnet und zum Betrag addiert. 0, wenn die Karte keine Gebühr hat."],
            ["Mitarbeiter aktiv", "Inaktive Mitarbeiter können sich nicht anmelden und stehen bei neuen Reisen nicht zur Auswahl. Statt Löschen deaktivieren."],
            ["🔒 Login-Passwort", "Nur für Organisatoren. Setzt ein neues Passwort (mindestens 8 Zeichen, überschreibt das alte). Reisende erhalten kein Passwort."],
        ], [28, 72]),
        ("h3", "🛂 Visa"),
        ("p", "Im Profil steht unter dem Formular die Karte **🛂 Visa**. Mit **+ Hinzufügen** erfassen Sie ein Visum: **Land** aus der Liste wählen, **gültig von** und **gültig bis**, "
              "optional eine Notiz (z. B. Art des Visums) und die **Kopie des Visums** als PDF oder Foto (höchstens 15 MB; ein Foto wird in ein PDF umgewandelt). "
              "Die Datei ist freiwillig; ohne sie wird nur der Ablauf überwacht. Pro Visum gibt es **Ansehen**, **Bearbeiten** (Daten ändern, Kopie ersetzen oder entfernen) und **Löschen**. "
              "Ein Land kann mehrere Visa haben (z. B. altes und verlängertes). Sehen und ändern dürfen Organisatoren und der Mitarbeiter selbst."),
        ("ul", [
            "**Hinweis im Dashboard:** Läuft ein Visum in den nächsten **4 Wochen** ab oder ist es schon abgelaufen, steht oben im Dashboard die Karte **„🛂 Visa laufen ab“** mit Name, Land und Datum. "
            "Sie bleibt, bis jemand **OK** klickt. Organisatoren sehen alle Visa, andere Mitarbeiter nur ihre eigenen.",
            "**Verlängerung:** Erfassen Sie ein neues Visum für dasselbe Land mit späterem Ablauf, meldet die App das alte nicht mehr. Ändern Sie bei einem Visum das Datum „gültig bis“, "
            "erscheint der Hinweis zum neuen Ablaufdatum wieder (auch wenn vorher OK geklickt wurde). Inaktive Mitarbeiter werden nicht gemeldet.",
            "**Reise in das Land:** Reist der Mitarbeiter in ein Land, für das er ein Visum hat, das an mindestens einem Reisetag gültig ist, liegt die Kopie im **Portal** (Karte „Reiseunterlagen“) bereit "
            "und hängt hinten an **seiner Reisemappe** (siehe „Reisemappe, Reise-PDFs und A1-Bescheinigung“). Deckt das Visum nicht die ganze Reise ab, steht im Portal ein Warnhinweis.",
        ]),
    ]},
    {"id": "b-reise", "teil": "B", "titel": "Reisen anlegen und bearbeiten", "bloecke": [
        ("h3", "Neue Reise"),
        ("p", "Menü **Reisen** › „Neue Reise“. Der **Reisecode** hat die Form `JJ-NNN` (z. B. 26-007). Das Jahr ist das Jahr der **Abreise**, "
              "die laufende Nummer zählt je Jahr hoch."),
        ("tab", ["Feld", "Bedeutung"], [
            ["Titel", "Kurzbeschreibung, z. B. „ECMA Lyon“."],
            ["Abreise / Rückkehr", "Zeitraum der Reise (Pflicht)."],
            ["Mitarbeiter", "Ein oder mehrere Reisende (Mehrfachauswahl mit Strg-/Cmd-Klick)."],
            ["Notiz", "Optional, z. B. Kundenprojekt oder Messe."],
            ["🌍 Länder & VMA-Sätze", "Länder mit Von/Bis-Datum. Daraus wird die VMA berechnet. Beim Auswählen erscheint der Satz (voll / halb)."],
        ], [28, 72]),
        ("h3", "Die Reise-Seite"),
        ("p", "Oben stehen Reisecode, Titel, Status (**In n Tagen** / **Aktiv** / **Abgeschlossen**) und Kennzahlen: VMA gesamt netto, Trennungspauschale, Kosten (Rechnungen)."),
        ("tab", ["Knopf", "Funktion"], [
            ["🧾 Abschluss", "Abrechnung der Reise ansehen und als PDF erzeugen."],
            ["📤 An Habel", "Übergabe an die DMS-Stelle."],
            ["💵 Vorschuss", "Reisevorschuss und Bargeld-Konto."],
            ["🗺 Reiseplan", "Zeitliche Übersicht aller Flüge, Züge und Termine."],
            ["📅 Outlook", "Outlook-Einladungen der Reise."],
            ["🔄 VMA neu berechnen", "Rechnet die Tage der Reise neu. Normalerweise nicht nötig, die App rechnet automatisch (siehe Verpflegungsmehraufwand)."],
            ["✏ Bearbeiten", "Titel, Daten, Reisende und Notiz ändern."],
            ["🌍 + Land", "Weiteres Land mit Zeitraum hinzufügen. Vorhandene Länder lassen sich über ✏ ändern oder löschen."],
        ], [28, 72]),
        ("p", "Darunter folgen die Karte „🔗 Reisenden-Zugänge (Portal)“ und der „📅 Tagesverlauf & VMA“ mit allen Belegen und Terminen je Tag. "
              "Rechts oben im Tagesverlauf setzt **↺ Alle Tage auf Auto** bei allen Tagen die Manuell-Markierung zurück und berechnet Land und Sätze neu."),
    ]},
    {"id": "b-portal", "teil": "B", "titel": "Portal-Zugänge für Reisende", "bloecke": [
        ("p", "Jeder Reisende einer Reise hat einen **persönlichen Link** (Token). Er öffnet nur die eigene Reise: Die Reise kommt immer aus dem Link, "
              "nie aus einem Formularfeld. Was Reisende dort sehen, steht in Teil A."),
        ("p", "Die Karte **„🔗 Reisenden-Zugänge (Portal)“** auf der Reise-Seite zeigt je Reisendem den Status:"),
        ("ul", [
            "„Noch kein Zugang erstellt“ – der Link entsteht beim ersten Senden.",
            "„Link erstellt, noch nicht gesendet“.",
            "„✓ gesendet am …“ – dann heißt der Knopf „Erneut senden“.",
        ]),
        ("p", "**📧 Link senden** verschickt die Mail sofort. Der Knopf ist gesperrt, solange keine E-Mail-Adresse 1 hinterlegt ist."),
        ("p", "**Automatisch:** Der tägliche Aufruf von `/cron/portal-mails` verschickt den Link [[portal_tage]] Tage vor der Abreise an alle Reisenden, "
              "die ihn noch nicht bekommen haben (Teil C, „Automatische Aufgaben“). Das gilt nur für Reisen, die **ab dem 16.10.2026** angelegt wurden. "
              "**Bestandsreisen** (früher angelegt) bekommen keine automatische Mail; dort bleibt **📧 Link senden** von Hand. "
              "Der Link beginnt immer mit `https://`: Er wird aus `PORTAL_BASE_URL` gebaut, sonst aus der von Render gesetzten Adresse."),
        ("p", "**Erneut senden** verschickt dieselbe Mail noch einmal, der Betreff beginnt dann mit „Erneut:“."),
        ("p", "**Speichern im Portal:** Die Reisenden speichern alle Tage und Kilometer mit **einem** Knopf. Geschrieben werden nur Tage, die sich "
              "wirklich geändert haben. Ein unberührter Tag bleibt dadurch automatisch berechnet und wird nicht als „manuell“ eingefroren. "
              "Eine unlesbare Uhrzeit überspringt nur diesen Tag; der Rest wird gespeichert und die Reisenden sehen einen Hinweis."),
        ("h3", "Abgleich Portal ↔ Reise-Seite"),
        ("p", "Portal und Reise-Seite zeigen dieselben Tage und gleichen sich in beide Richtungen ab. **Maßgeblich für Land, Satz und halben Satz ist die Reise-Seite**; das Portal übernimmt sie bei jedem Öffnen."),
        ("ul", [
            "**Portal → Reise-Seite:** Mahlzeiten und Reisezeit der Reisenden stehen sofort im Tagesverlauf (der Tag gilt dort als „manuell“). Fehlen auf der Reise-Seite die VMA-Tage noch, werden sie dabei zuerst berechnet. Zusätzlich zeigt der Tagesverlauf unter jedem Tag die Portal-Angaben mit Kürzel (📱 Reise- und Arbeitszeit, Notiz); die Kilometer stehen als eigene Zeile unter den Ländern.",
            "**Reise-Seite → Portal:** Ändern Sie dort Mahlzeiten, Land oder Satz eines Tages, sehen alle Reisenden das beim nächsten Öffnen des Portals (auch Reisende, die den Tag schon ausgefüllt haben, denn Ihre Änderung ist bewusst). Die Zeit „Reiseantritt/Reiseende (tatsächlich)“ erscheint im Portal als Reisebeginn (erster Tag) bzw. Reiseende (letzter Tag).",
            "**Automatische Änderungen** (neuer Hotelbeleg mit Frühstück, anderes Land aus Flug oder Hotel, geänderte Reisedaten) gehen ebenfalls ins Portal. Mahlzeiten übernimmt das Portal dabei nur für Tage, die der Reisende noch nie gespeichert hat.",
            "**Reise verlängert oder verkürzt:** Der erste und letzte Tag bekommen den richtigen Satz (auch wenn sie schon „manuell“ sind). Tage außerhalb der neuen Reisedauer verschwinden aus dem Portal; die Eingaben bleiben gespeichert und sind wieder da, wenn die Reise erneut verlängert wird.",
            "**Mehrere Reisende:** Die VMA-Tage der Reise-Seite gibt es nur einmal je Reise. Die Mahlzeiten, die ein Reisender zuletzt gespeichert hat, stehen dort; Änderungen der Reise-Seite gelten für alle Reisenden.",
        ]),
        ("h3", "Karte „📄 Reiseunterlagen“ im Portal"),
        ("p", "Oben im Portal steht die Karte **Reiseunterlagen**. Sie enthält die **Reise-PDFs** aus den Einstellungen (zum Beispiel die Krankenversicherung; alle für alle Mitarbeiter plus die persönlichen des Reisenden, pausierte nicht) und bei **Reisen in Europa** die **A1-Bescheinigung** des Reisenden. Liegt sie noch nicht vor, steht dort ein Hinweis, sie über „Beleg hochladen“ zu schicken; ist sie als „nicht nötig“ markiert, steht der Grund da. Zeigt die A1 einen zu kurzen Zeitraum, gibt es einen Warnhinweis. Hat der Reisende für ein Land der Reise ein gültiges **Visum** mit hinterlegter Kopie, steht dort auch ein Knopf **🛂 Visum Land**. Jeder Reisende sieht nur seine eigenen Dokumente; die Links funktionieren nur mit seinem persönlichen Portal-Link."),
    ]},
    {"id": "b-eingang", "teil": "B", "titel": "Belege hereinbekommen", "bloecke": [
        ("p", "Belege kommen auf drei Wegen in die App. Jeder Beleg wird ausgelesen: Text lesen, Namen und Mailadressen der Mitarbeiter unkenntlich machen "
              "(Anonymisierung), KI-Analyse, Ablage der PDFs im S3-Speicher (Original, anonymisiert, Analyse)."),
        ("h3", "1. Per E-Mail an das Reisekosten-Postfach"),
        ("ul", [
            "Der Reisecode (z. B. `26-007`) muss im Betreff oder Text stehen. Nur eine bestehende Reise gilt als Treffer.",
            "Abruf von Hand über **📬 Mails** (Menü, Dashboard oder Belegliste) **oder automatisch alle 5 Minuten** über den Cron (Teil C, „Automatische Aufgaben“). "
            "Es läuft immer nur ein Abruf; ein zweiter Klick während eines Laufs zeigt einen Hinweis. Der Abruf läuft im Hintergrund, die Seite bleibt bedienbar. "
            "Antwortet der Mailserver nicht, bricht der Abruf nach 30 Sekunden mit einer Fehlermeldung ab.",
            "Geholt werden nur **ungelesene** Mails. Jeder Anhang (PDF, JPG, JPEG, PNG, TIF, HEIC, WEBP, XML) wird ein eigener Beleg. "
            "Ohne Anhang wird der Mailtext (mehr als 50 Zeichen) selbst zum Beleg.",
            "Ob ein Anhang ein Beleg ist, entscheidet sein **Inhalt** (PDF, JPG, PNG, WEBP, HEIC, TIF), nicht die Angabe der Mail. Auch kleine, vom Handy weitergeleitete PDFs werden erkannt. "
            "Nur kleine eingebettete **Bilder** (Logos in der Signatur) und Kalender-/Kontaktdateien werden ignoriert. Ein PDF in einer als Anhang weitergeleiteten Mail wird ebenfalls gefunden.",
            "Verarbeitete Mails werden im Postfach **gelöscht**. Doppelte Weiterleitungen werden über die Message-ID erkannt.",
            "Das Ergebnis zeigt: Mails verarbeitet, Belege erstellt, Duplikate, Fehler (mit Details). Wurde der Mailtext zum Beleg, obwohl die Mail weitere Teile hat oder im Text einen Anhang nennt, "
            "steht darunter **„⚠ Mails ohne erkannten Anhang“** mit der Liste der Mail-Teile (Typ, Name, Größe). Dann die Mail erneut weiterleiten und die Liste mitschicken.",
        ]),
        ("h3", "2. Upload von Hand"),
        ("p", "Seite „Beleg hochladen“ (`/beleg/upload`): PDF oder Bild wählen, optional gleich einer Reise zuordnen."),
        ("h3", "3. Durch die Reisenden im Portal"),
        ("p", "Der Beleg landet automatisch bei der Reise des Reisenden und wartet auf die Prüfung."),
        ("h3", "Posteingang (Belege ohne Reise)"),
        ("p", "Belege ohne gültigen Reisecode erscheinen unter **📬 Posteingang** (`/unzugeordnet`; Kachel in den Einstellungen; das Dashboard zeigt die Anzahl). "
              "Dort die Reise wählen und zuordnen, oder den Beleg öffnen."),
        ("p", "**Doppelte Belege** (gleiche Datei) werden erkannt und nicht erneut angelegt. Enthält ein PDF eine **eRechnung** (ZUGFeRD/XML), ist die XML-Datei "
              "auf der Beleg-Seite im Original und unverändert abrufbar."),
        ("p", "**A1-Bescheinigungen** erkennt die App am Inhalt (Überschrift „Bescheinigung über die anzuwendenden Rechtsvorschriften“, „Portable Document A1“, Verordnung 883/2004) "
              "und legt sie als Belegart **🛂 A1-Bescheinigung** ab, ohne Betrag und ohne Kosten. Bei Scans ohne Text entscheidet die KI, ein Dateiname mit „A1“ hilft. "
              "Siehe Kapitel „Reisemappe, Reise-PDFs und A1-Bescheinigung“."),
        ("p", "**Buchung und Rechnung sind keine Duplikate.** Tragen eine Buchungsbestätigung und die dazugehörigen Rechnungen denselben Buchungscode, bleiben es getrennte Belege. "
              "Zwei Rechnungen derselben Buchung gelten nur dann als doppelt, wenn auch die Rechnungsnummer (ohne Nummer: der Betrag) übereinstimmt. "
              "Beim Eingang werden Buchungsbestätigung und Rechnungen mit **gleichem Buchungscode in derselben Reise automatisch verknüpft** (siehe „Beleg prüfen und ergänzen“)."),
    ]},
    {"id": "b-belegliste", "teil": "B", "titel": "Die Belegliste", "bloecke": [
        ("p", "Menü **Belege** zeigt alle Belege. Filter: Jahr, Monat, Reise, Geprüft, Problem (z. B. fehlende Pflichtfelder) und Suche "
              "(Anbieter, Dateiname, Nummer). „Zurücksetzen“ löscht die Filter. Ein Klick auf einen Beleg öffnet die Beleg-Seite."),
    ]},
    {"id": "b-beleg", "teil": "B", "titel": "Beleg prüfen und ergänzen", "bloecke": [
        ("p", "Die Beleg-Seite (`/beleg/…`) zeigt von oben nach unten:"),
        ("ul", [
            "**📊 KI-Analyse:** erkannte Daten (Anbieter, Datum, Betrag, MwSt, Währung, Transportart, Segmente). Fehlt etwas, steht dort „Fehlende Pflichtfelder“. "
            "Die **MwSt.** wird nur übernommen, wenn sie auf dem Beleg steht. Bei Flug, Bahn-Fernverkehr, Fernbus, Schiff und Reisebüro-Rechnungen rechnet die App sie nie selbst aus (bei einem Reisebüro gilt sie oft nur für die Servicepauschale). "
            "Nur bei einfachen deutschen Belegen (z. B. Parkschein, Tankquittung, ÖPNV) ergänzt die KI eine fehlende MwSt. selbst. "
            "Bei **Scans und Fotos** liest die KI alle Seiten (höchstens 4); ein Scanner-PDF ohne Text wird per Bilderkennung gelesen. Bei mehreren Steuersätzen gilt: MwSt. = Summe der ausgewiesenen Steuerbeträge, Netto = Brutto minus MwSt. Mit **Beleg neu auslesen** lässt sich ein älterer Scan mit allen Seiten neu analysieren. "
            "Mit **✏ Daten nachtragen** (Organisator) ergänzen oder korrigieren. Das **Land** wählen Sie aus einer Liste mit Code und Klarnamen (z. B. „DE – Deutschland“, „IT – Italien“, „MX – Mexiko“). "
            "Häufige Reiseländer stehen oben, darunter alle Länder von A bis Z.",
            "**Belegart** und **Bezahlart** (siehe Tabellen unten). Bei Kreditkarte wählen Sie „Welche Karte?“.",
            "**📎 Dokumente:** Original-PDF, 🔒 Anonymisiert, 🔍 Analyse-PDF; Knöpfe zum erneuten Analysieren und erneuten Anonymisieren; Beleg löschen; Reise zuordnen.",
            "**🔗 Verknüpfte Belege:** Zusammengehörige Belege (z. B. Buchungsbestätigung und Rechnungen) lassen sich verknüpfen und wieder trennen. "
            "Die Karte zeigt, ob die Buchung durch Rechnungen belegt ist (siehe „Buchung und Rechnungen verknüpfen“ unten).",
            "**💰 Eurobetrag – Endabrechnung** und Kurs (siehe „Beträge, Fremdwährung und Kurse“).",
            "**✅ Prüfung & Übergabe an Habel:** Prüfvermerk eintragen und bestätigen. Danach steht dort „Geprüft von …“, und das Prüf-Deckblatt ist als PDF abrufbar. "
            "Darunter der **Habel-Stand der ganzen Reise**: Fortschrittsbalken („n von m Belegen geprüft“), noch offene Belege als Links und der Knopf zur Reise-Übergabe. "
            "Die Übergabe erfolgt für die ganze Reise. Einzelne Rechnungen und Quittungen lassen sich zusätzlich von Hand senden (Karte auf der Reise-Seite).",
        ]),
        ("hinweis", "Von der App selbst erzeugte Belege (Bewirtungsbeleg, Vorschuss-, Rückgabequittung, Bargeldbeschaffung) lassen sich nicht neu analysieren oder anonymisieren "
                    "und brauchen keine Rechnungs-Pflichtfelder."),
        ("h3", "Belegarten"),
        ("tab", ["Belegart", "An Habel (aktuell)", "Hinweis"], [
            ["Rechnung", "ja", "Standardfall."],
            ["Quittung", "ja", "Kassenbon, Quittung."],
            ["Bewirtungsbeleg (Eigenbeleg)", "ja", "Vom Reisenden im Portal erstellt, siehe Kapitel Bewirtung."],
            ["Vorschussquittung / Bargeldbeschaffung / Rückgabequittung", "ja", "Selbst erzeugt aus dem Bargeld-Konto, ohne eigenen Betrag."],
            ["Buchungsbestätigung", "nein", "Betrag ist hier optional. Liefert Flug-/Zugdaten. Braucht Rechnungen oder Quittungen (siehe unten)."],
            ["✈ Flugänderung/Umbuchung", "nein", "Löst die Änderungsklärung aus (siehe „Flug, Bahn und Termine“)."],
            ["🛂 A1-Bescheinigung", "nein", "Kein Kostenbeleg: ohne Betrag, nicht in Abschluss und Kosten, keine Pflichtfelder. Siehe „Reisemappe, Reise-PDFs und A1-Bescheinigung“."],
            ["Tankbeleg (Rechnung), Hotel, Taxi, Bewirtung, Sonstige Kosten, Sonstiges", "nein", "Zur Einordnung der Ausgabe. Nicht prüfpflichtig für die Habel-Übergabe. Liefert die KI für einen Beleg mit Betrag „Sonstiges“, setzt die App automatisch „Quittung“. Bei vorhandenen Belegen zeigt die Beleg-Seite den Knopf „Als Quittung einstufen“."],
        ], [42, 16, 42]),
        ("h3", "Bezahlarten"),
        ("p", "💳 Kreditkarte · 💵 Bar · 🏦 Überweisung · 🅿 PayPal · 🤝 Durch Kunde · ❓ Unbekannt."),
        ("p", "**🤝 Durch Kunde:** Hat der Kunde direkt bezahlt, wählen Sie diese Bezahlart. Der Beleg **zählt nie in der Summe** (Abschluss, PDF, Kosten-Kachel, Tagessumme) "
              "und braucht **keine Rechnung oder Quittung**. Er wird nur vermerkt: im Abschluss und im PDF unter „Durch Kunde direkt bezahlt“, im Tagesverlauf mit dem Hinweis „durch Kunde direkt bezahlt“. "
              "Auch die Hinweise „Betrag final offen“ und „Kurs fehlt“ entfallen. Gehört eine Buchungsbestätigung dazu, setzen Sie auch dort „Durch Kunde“."),
        ("h3", "Buchung und Rechnungen verknüpfen"),
        ("p", "**Jede Buchung braucht Rechnungen oder Quittungen.** Die Buchungsbestätigung (Betrag x) ist nur der Vorab-Beleg; abgerechnet wird mit den echten Rechnungen "
              "(x1 + x2 = x). Dazu werden die Belege zu einer **Gruppe** verknüpft. Die Karte „🔗 Verknüpfte Belege“ zeigt dann Buchung, Rechnungen und Summe, zum Beispiel "
              "„Buchung 300.00 EUR · Rechnungen 120.00 + 180.00 = 300.00 EUR · ✓ vollständig belegt“."),
        ("ul", [
            "**Automatisch:** Beim Eingang werden Buchungsbestätigung und Rechnung/Quittung mit **gleichem Buchungscode** in derselben Reise verknüpft (Groß/Kleinschreibung, Leer- und Sonderzeichen egal, mindestens 4 Zeichen). Die Meldung beim Abruf nennt es.",
            "**💡 Passender Vorschlag:** Ergeben ungebundene Rechnungen der Reise genau den Betrag der Buchung (oder den noch fehlenden Rest), schlägt die Karte die Belege vor. "
            "Ein Klick auf **Alle verknüpfen** übernimmt sie. Der Vorschlag erscheint an der Buchung und auch an jeder der beteiligten Rechnungen.",
            "**Von Hand:** Unten in der Karte die gewünschten Belege **ankreuzen** und **+ Ausgewählte verknüpfen** wählen (mehrere auf einmal). "
            "Belege mit gleichem Buchungscode oder Anbieter stehen oben; Buchungen zeigen ihren offenen Rest. Nur Belege derselben Reise sind möglich.",
            "**Lösen:** **Entfernen** neben einem Beleg nimmt ihn aus der Gruppe.",
            "**Rechnung ohne Betrag:** Wurde bei einer Rechnung kein Betrag erkannt, nennt die Karte den Beleg („Beleg #232 hat keinen Betrag“) und zeigt direkt ein Feld **Betrag speichern**. "
            "Fehlt genau ein Betrag, ist der **Rest der Buchung** vorausgefüllt (z. B. 19725 − 9606 = 10119 INR). Bitte mit der Rechnung vergleichen. Bei Fremdwährung wird der Kurs vom Schwester-Beleg übernommen. "
            "**Beleg neu auslesen** startet die KI-Analyse noch einmal.",
        ]),
        ("tab", ["Stand", "Bedeutung", "Kostensumme"], [
            ["✓ vollständig belegt", "Rechnungen decken die Buchung (auf den Cent; bei Fremdwährung 5 % Kursspielraum). Übersteigen sie die Buchung, steht der Mehrbetrag dabei.", "Nur die Rechnungen zählen. Die Buchung zählt nicht doppelt."],
            ["⚠ teilweise belegt", "Die Rechnungen decken nur einen Teil, es fehlt ein Rest (z. B. 120 von 300).", "Rechnungen plus der **offene Rest** als „vorläufig“."],
            ["⚠ noch keine Rechnung/Quittung", "Zur Buchung ist noch nichts verknüpft (oder nur Belege ohne Betrag).", "Die ganze Buchung zählt „vorläufig“ mit."],
            ["Währungen ohne Kurs", "Beträge in verschiedenen Währungen lassen sich ohne Kurs nicht vergleichen.", "Nur die Rechnungen. Bitte selbst prüfen."],
        ], [24, 48, 28]),
        ("p", "**Zahlungsnachweis:** Ein Kartenbeleg oder eine Zahlungsquittung (z. B. PayU über 194.50 EUR zur Buchung über 19725 INR) belegt nur die Zahlung. "
              "Neben echten Rechnungen derselben Gruppe zählt er **nicht zusätzlich**. Die App erkennt ihn selbst, wenn die Gruppe mindestens eine Rechnung mit Betrag enthält und der Beleg "
              "keine Rechnung ist (Quittung, Sonstiges …) und (a) in anderer Währung als die Buchung lautet, während die Rechnungen die Buchung schon voll decken, "
              "oder (b) sein Betrag genau der ganzen Buchung oder der Rechnungssumme entspricht. Andere Beträge, etwa eine zusätzliche Restaurant-Quittung, zählen wie normale Belege. "
              "Der Zahlungsnachweis liefert den echten Euro-Betrag (siehe „Beträge, Fremdwährung und Kurse“) und geht weiterhin als eigenes PDF an Habel. "
              "Mit **Entfernen** nimmt man einen Beleg aus der Gruppe; er zählt dann wieder normal."),
        ("hinweis", "Eigenbelege der App (z. B. Bewirtungsbeleg) decken keine Buchung ab. Gruppen ohne Buchungsbestätigung (z. B. Bewirtungsbeleg mit Rechnung) werden nicht auf Beträge geprüft. "
                    "Eine Hotelrechnung, die höher ist als die Buchung (z. B. wegen Bewirtung oder Extras), gilt als vollständig belegt; der Mehrbetrag steht dabei."),
    ]},
    {"id": "b-betrag", "teil": "B", "titel": "Beträge, Fremdwährung und Kurse", "bloecke": [
        ("p", "Der Euro-Betrag eines Belegs wird immer nach derselben Regel gebildet:"),
        ("ol", [
            "Ist ein **finaler Euro-Betrag** eingetragen („Betrag Beleg – final, in EUR, gemäß Abrechnung“), gilt dieser.",
            "Sonst der von der KI **geschätzte Euro-Gegenwert** (bei Fremdwährung).",
            "Sonst der Bruttobetrag selbst, wenn er schon in EUR ist.",
            "Dazu kommen immer **Nebenkosten** (von Hand, mit Bezeichnung) und das **Auslandsentgelt** (automatisch).",
        ]),
        ("tab", ["Feld", "Bedeutung"], [
            ["Kurs", "Schreibweise am Beleg: **1 Fremdwährung = ? EUR** (Euro je eine Einheit Fremdwährung), dazu Kursdatum und Quelle (z. B. EZB, Bank). "
                     "Am Bargeld-Konto (Vorschuss) gilt dagegen die Schreibweise der Wechselbelege: 1 EUR = ? Fremdwährung."],
            ["Betrag Beleg – final in EUR", "Endgültiger Betrag laut Abrechnung. Hat Vorrang vor Schätzung und Kurs."],
            ["Nebenkosten / Bezeichnung", "Zusätzliche Kosten in EUR, z. B. Kreditkartengebühr."],
            ["Auslandsentgelt", "Wird automatisch berechnet, wenn Bezahlart Kreditkarte und Fremdwährung vorliegen. Prozentsatz aus dem Profil des Karteninhabers."],
        ], [28, 72]),
        ("p", "Die Hinweise **„Auslandsbeleg ohne Wechselkurs – Kurs nachtragen“** (Dashboard, Abrechnung) erscheinen nur, wenn wirklich ein Kurs fehlt. "
              "Nicht gemeldet werden Bewirtungs- und Vorschussbelege sowie Belege mit bestätigtem Euro-Betrag. "
              "„Betrag final noch nicht bestätigt“ steht in den offenen Aufgaben."),
        ("p", "Die endgültigen Euro-Beträge und echten Auslandsgebühren liefert am genauesten die **Kreditkarten-Abrechnung** (eigenes Kapitel)."),
    ]},
    {"id": "b-plan", "teil": "B", "titel": "Flug, Bahn und Termine", "bloecke": [
        ("p", "Flüge und Züge kommen aus den Buchungsbelegen (die KI liest die Segmente mit Zeiten, Nummer, Sitz, Terminal/Gleis). "
              "Alles andere tragen Sie als **Termin** ein."),
        ("h3", "Termine"),
        ("p", "Reise-Seite: **+ Termin** am Tag oder im Reiseplan **+ Ereignis**. Typen: Termin, Kundenbesuch, Montage, Fahrt/Taxi, Mietwagen, Hotel, Sonstiges. "
              "Felder: Titel, Typ, Datum, Uhrzeit von/bis, Ort/Adresse (mit Google-Vorschlägen, wenn GOOGLE_PLACES_API_KEY gesetzt ist), Ansprechpartner, Telefon, Notiz. "
              "Termine lassen sich ändern und löschen."),
        ("p", "**🔧 Montage über mehrere Tage:** Beim Typ „Montage“ erscheint das Feld **„Montage bis (letzter Tag)“**. Die App legt dann für **jeden Tag** von „Datum“ bis zu diesem Tag einen Termin an, "
              "Standard **07:00–16:00** (die Uhrzeiten oben lassen sich ändern). Für den **Anreisetag** und den **Abreisetag** gibt es eigene Zeiten; ein leeres Feld nimmt die Standardzeit. "
              "Die Montage muss im Reisezeitraum liegen (höchstens 60 Tage). Ist „Montage bis“ leer, entsteht nur ein Termin. Jeder Tag ist danach ein eigener Termin und lässt sich einzeln ändern oder löschen."),
        ("p", "**In Outlook eine Terminserie:** Alle Tage einer mehrtägigen Montage gehören zu einer Serie. Die Mitarbeiter mit Outlook-Haken bekommen dafür **eine einzige Einladung** (eine Mail) statt einer je Tag. Abweichende Zeiten am Anreise-/Abreisetag stehen darin als Ausnahmen, ebenso ein einzeln geänderter Tag. Wird ein Tag gelöscht, kommt eine Aktualisierung mit diesem Tag als Ausnahme; wird die ganze Montage gelöscht, kommt eine Absage der Serie. Beim Bearbeiten eines Tages zeigt die Seite den Hinweis „Teil einer Montage-Serie“. Ändern Sie bei einem Tag den Typ weg von „Montage“, wird er ein eigener Einzeltermin. **Ältere Montagen** (vor Version 3.42 angelegt, mit je Tag einer Einladung) bleiben Einzeltermine; Serien gibt es nur für neu angelegte Montagen."),
        ("p", "**👥 Auch für andere Reisen anlegen:** Sind Kollegen zur selben Zeit unterwegs, zeigt das Formular unten **„Auch für andere Reisen anlegen“**. "
              "Dort stehen nur Reisen, die sich mit dem eingetragenen Datum (bei Montage: mit dem ganzen Zeitraum) überlagern. Haken setzen, speichern: "
              "Der Termin wird für **jede gewählte Reise einzeln** angelegt, deren Reisende bekommen ihre **eigene Outlook-Einladung**. "
              "Bei einer mehrtägigen Montage entsteht je Reise eine eigene Terminserie. Liegt nur ein Teil der Montage in einer anderen Reise, ist sie nicht wählbar (grauer Hinweis). "
              "Die Termine sind danach unabhängig: Änderungen oder Löschen gelten nur für die jeweilige Reise."),
        ("h3", "Reiseplan"),
        ("p", "**🗺 Reiseplan** zeigt alle Flüge, Züge und Termine in zeitlicher Reihenfolge. Zwei PDF-Varianten: „für Reisemappe Mitarbeiter“ (mit Anlagen) und „zum Emailversand“ (ohne Buchungscodes, nie mit Anlagen). Mehr dazu im Kapitel „Reisemappe, Reise-PDFs und A1-Bescheinigung“."),
        ("h3", "Änderungen und Umbuchungen"),
        ("ul", [
            "Wird ein Beleg als **Flugänderung/Umbuchung** eingestuft, erscheint oben auf der Reise-Seite „n Änderungen warten auf Klärung“. "
            "Die Reise zeigt bis dahin die bisherigen Daten. Pro Änderung die richtigen Werte wählen: **anwenden** oder **verwerfen**.",
            "Ein Segment lässt sich auch von Hand **umbuchen** (Zeiten, Nummer, Sitz). Der Sitzplatz wird von der KI aus dem Beleg gelesen und lässt sich dort eintragen.",
            "**Überschneidungen:** Enthalten zwei Belege dieselben Flug-/Zugsegmente (z. B. Buchung und Umbuchung), erscheint ein Hinweis. Auf der Seite wählen Sie je Fall: "
            "A behalten, B behalten oder beide behalten (z. B. zwei Reisende).",
            "**Flugstatus zurücksetzen:** löscht den gespeicherten Flug-/Bahnstatus eines Belegs, wenn ein falscher Endstatus (z. B. falsche Verspätung) auf dem Dashboard hängen bleibt.",
        ]),
    ]},
    {"id": "b-reisemappe", "teil": "B", "titel": "Reisemappe, Reise-PDFs und A1-Bescheinigung", "bloecke": [
        ("p", "Die **Reisemappe** ist das Reiseplan-PDF (Reiseplan › „PDF für Reisemappe“). Seit Version 3.41 hängt die App hinten automatisch die nötigen Dokumente an "
              "und setzt eine Seite **Anlagen** mit Inhaltsverzeichnis und Seitenzahlen davor. Im PDF-Leser gibt es dazu Lesezeichen."),
        ("h3", "Reise-PDFs (Einstellungen › 📎 Reise-PDFs)"),
        ("ul", [
            "Hier laden Sie PDFs hoch, die in jede Reisemappe gehören, zum Beispiel die **Kopie der Krankenversicherung**. Auch ein Foto (JPG/PNG) geht; es wird in ein PDF umgewandelt. "
            "Höchstens 15 MB und 60 Seiten, kein Passwortschutz.",
            "**Gilt für:** „Alle Mitarbeiter“ oder ein bestimmter Mitarbeiter (z. B. seine persönliche Versicherungskarte). Der **Titel** erscheint im Inhaltsverzeichnis.",
            "**Pausieren** nimmt ein Dokument vorübergehend aus allen Mappen, **Entfernen** löscht den Eintrag. **Ansehen** öffnet das PDF.",
        ]),
        ("p", "Dieselben Dokumente (Reise-PDFs und die eigene A1) sehen die Reisenden auch im Portal in der Karte **📄 Reiseunterlagen**, ohne die Mappe zu öffnen. Pausierte Dokumente erscheinen dort ebenfalls nicht."),
        ("h3", "Welche Anlagen kommen in welche Mappe?"),
        ("ul", [
            "Hat die Reise **einen** Reisenden, bekommt die Mappe seine Dokumente automatisch: alle Dokumente „für alle Mitarbeiter“, seine persönlichen Dokumente, seine A1 "
            "und die Kopien seiner **Visa** für Länder der Reise (Mitarbeiterprofil › 🛂 Visa). Fehlt zu einem passenden Visum die Kopie, steht es unter „Nicht angehängt“.",
            "Bei **mehreren Reisenden** zeigt der Reiseplan je Mitarbeiter einen eigenen Knopf („📄 Reisemappe Name“). Ohne Auswahl enthält die Mappe nur die Dokumente für alle Mitarbeiter, "
            "keine persönlichen Dokumente und keine A1.",
            "Die Variante **„PDF zum Emailversand“** enthält **nie** Anlagen (Versicherungsdaten gehören nicht zu Kunden oder Dritten).",
            "Ist ein Dokument nicht lesbar oder die A1 fehlt noch, kommt die Mappe trotzdem. Auf der Anlagen-Seite steht dann unter **„Nicht angehängt“** was fehlt, mit Grund.",
            "Mit `?anlagen=0` an der Adresse (`/reise/…/reiseplan/pdf?anlagen=0`) erzeugt die App das reine Reiseplan-PDF ohne Anlagen.",
        ]),
        ("h3", "A1-Bescheinigung bei Reisen in Europa"),
        ("p", "Wer in ein EU-/EWR-Land, die Schweiz oder das Vereinigte Königreich reist, muss eine **A1-Bescheinigung** mitführen. Die App legt dafür von selbst eine Aufgabe an:"),
        ("ul", [
            "**Wann nötig?** Wenn die Reise (Länder der Reise oder Reisetage) mindestens ein solches Land außer Deutschland enthält. Länder: Belgien, Bulgarien, Dänemark, Estland, Finnland, Frankreich, Griechenland, Irland, "
            "Island, Italien, Kroatien, Lettland, Liechtenstein, Litauen, Luxemburg, Malta, Niederlande, Norwegen, Österreich, Polen, Portugal, Rumänien, Schweden, Schweiz, Slowakei, Slowenien, Spanien, "
            "Tschechien, Ungarn, Zypern und das Vereinigte Königreich. Die A1 braucht **jeder Reisende einzeln**.",
            "**Aufgabe:** Auf **📋 Offene Aufgaben** (Karte 🛂) und auf dem Dashboard steht jede noch nicht beendete Europa-Reise, für die eine A1 fehlt oder zu prüfen ist. "
            "Bei Abreise in 14 Tagen oder früher ist die Aufgabe rot.",
            "**Karte „🛂 A1-Bescheinigung“ auf der Reise-Seite:** Status je Reisendem (✅ vorhanden mit Gültigkeit, ⚠ prüfen, ❌ fehlt, ➖ nicht nötig) und der Knopf zum **Hochladen**. "
            "Eine so hochgeladene Datei wird immer als A1 abgelegt.",
            "**Per Mail:** Die A1 als PDF (oder Foto) an das Reisekosten-Postfach schicken, mit dem Reisecode (z. B. `26-012`) im Betreff. Die App erkennt sie am Inhalt, legt sie als Belegart "
            "**🛂 A1-Bescheinigung** ab und ordnet sie über den **Namen** dem Reisenden zu. Auch der normale Upload (Belege › Hochladen) funktioniert.",
            "**Auslesen:** Name, Gültigkeitszeitraum, Ausstellungsdatum, Länder und ausstellende Stelle werden aus dem Text gelesen (bei Scans von der KI) und können über „✏ Daten nachtragen“ korrigiert werden. "
            "Die App prüft, ob der **Gültigkeitszeitraum die ganze Reise abdeckt**; sonst steht „⚠ prüfen“. Fehlt der Zeitraum, ergänzen Sie Von/Bis im Beleg.",
            "**Name passt nicht?** Die A1 erscheint in der Karte unter „A1 ohne passenden Reisenden“. Dort wählen Sie den Mitarbeiter und tippen **Zuordnen**.",
            "**Ist keine A1 nötig** (z. B. der Mitarbeiter lebt und arbeitet im Reiseland)? Bei „❌ fehlt“ tragen Sie einen Grund ein und tippen **nicht nötig** (nur Organisatoren). Die Aufgabe verschwindet; **A1 doch erforderlich** macht es rückgängig.",
            "**Mehrere Reisen:** Eine A1 gehört zu einer Reise. Gilt sie für mehrere Reisen, laden Sie sie bei jeder Reise hoch (die gleiche Datei zweimal bei **derselben** Reise wird als Duplikat erkannt).",
            "Eine A1 ist **kein Kostenbeleg**: ohne Betrag, nicht im Abschluss, in den Kosten oder bei Habel, keine Pflichtfeld-Meldung.",
        ]),
        ("p", "Die Erkennung beruht auf den Überschriften der amtlichen Bescheinigung (deutsch, englisch, französisch, italienisch, spanisch). Eine Rechnung des Anbieters „A1 Telekom“ wird nicht verwechselt. "
              "Sieht ein Dokument anders aus, stellen Sie auf der Beleg-Seite die Belegart von Hand auf „A1-Bescheinigung“."),
    ]},
    {"id": "b-alerts", "teil": "B", "titel": "Verspätungs-Alarme (Flug und Bahn)", "bloecke": [
        ("p", "Die App überwacht Flüge (AeroDataBox) und Züge (Deutsche Bahn, Timetables) nach einem **festen Prüfplan** – nicht editierbar:"),
        ("tab", ["Zeitpunkt", "Prüfung"], [
            ["Vor der geplanten Abreise", "4 h · 3 h · 2 h · 1 h · 30 Min · 15 Min"],
            ["Bei erkannter Verspätung zusätzlich", "15 Min vor der neuen erwarteten Abreise"],
            ["Während Flug/Fahrt", "keine Prüfung"],
            ["Vor der erwarteten Ankunft", "30 Min vorher"],
            ["Nach der Ankunft", "keine Prüfung mehr"],
        ], [45, 55]),
        ("p", "Ab **15 Minuten Verspätung** (oder bei Stornierung/Umleitung) geht sofort eine Mail an alle Reisenden der Reise **und** alle Organisatoren. "
              "Offene Alarme zeigt das Dashboard."),
        ("p", "Seite **Einstellungen › Flug-/Bahn-Alerts:** zeigt den Prüfplan und hat den Knopf **„🧪 Jetzt testweise abrufen“**. "
              "Er führt einen Prüflauf sofort aus und zeigt eine Diagnose (welche Belege im Zeitfenster liegen, was mit jedem Segment passiert ist)."),
        ("hinweis", "Voraussetzung: Der externe Cron-Dienst ruft `/cron/flug-alerts` jede Minute auf, und die API-Schlüssel (AERODATABOX_API_KEY, "
                    "DB_TIMETABLES_CLIENT_ID, DB_TIMETABLES_API_KEY) sind gesetzt. Siehe Teil C."),
    ]},
    {"id": "b-vma", "teil": "B", "titel": "Verpflegungsmehraufwand (VMA) und Trennungspauschale", "bloecke": [
        ("p", "Der **Tagesverlauf & VMA** auf der Reise-Seite zeigt je Tag: Datum, Land, Satz, Abzüge, **VMA netto** und alle Belege und Termine des Tages. Unter dem Tag steht die **Summe Belege in Euro**: Beträge in Fremdwährung bleiben führend und zeigen den Euro-Wert daneben (≈ geschätzt, = laut Abrechnung); fehlt der Kurs, steht „Kurs fehlt“ und der Beleg fehlt in der Summe. Durch Rechnungen belegte Buchungen und Zahlungsnachweise zählen nicht doppelt, ein Hinweis unter dem Eintrag sagt warum."),
        ("h3", "Land und Satz"),
        ("ul", [
            "Das **Land je Tag** wird automatisch aus Flügen, Hotelbelegen und der Länder-Zeitleiste der Reise ermittelt. Grundlage sind Abreise und Rückkehr der Reise; **Termine werden nicht benötigt**.",
            "**Automatisch aktuell:** Die Tage werden schon beim **Anlegen der Reise** erzeugt (Termine lassen sich sofort eintragen) und danach bei jeder Änderung neu berechnet: geänderte Reisedaten, Länder, neue, geänderte oder gelöschte Hotel- und Flugbelege, einer Reise zugeordnete Belege. Wird der Zeitraum verkürzt, entfallen überzählige Tage; manuell geänderte Tage bleiben.",
            "**🔔 Satz geändert:** Ändert sich Land oder Satz eines Tages gegenüber vorher (z. B. nach einem Hotelbeleg im Ausland), steht am Tag „vorher … → jetzt …“. Mit **OK** ist der Hinweis erledigt.",
            "**Land nicht erkannt:** Gibt es für einen Tag weder Flug noch Hotel noch Land-Eintrag, gilt der Deutschland-Satz. Der Tag zeigt dann den Hinweis „Land nicht erkannt“. Bei einer Auslandsreise den Hotel- oder Flugbeleg hochladen oder **🌍 + Land** eintragen; die Tage werden dann automatisch neu berechnet.",
            "**Rückreisetag:** Hier zählt der letzte Tätigkeitsort vor der Heimreise (Abflugort der ersten Etappe des Tages), nicht der Ankunftsort. "
            "Bei eintägigen Reisen das zuletzt besuchte ausländische Land. Wie die App entschieden hat, zeigt die Diagnose-Seite `/reise/<Code>/vma-debug` (ohne Knopf, nur noch per Adresse erreichbar).",
            "Städte mit eigenem Sondersatz (z. B. Los Angeles, New York) werden berücksichtigt.",
            "An- und Abreisetage erhalten je nach Fall den **halben Satz** (Markierung „½ Satz“).",
        ]),
        ("h3", "Mahlzeiten-Abzüge"),
        ("p", "Haken bei Frühstück, Mittag, Abend kürzen den vollen Tagessatz: **Frühstück 20 %, Mittag 40 %, Abend 40 %**. Das Ergebnis wird nie negativ. "
              "Die Reisenden tragen die Haken im Portal ein; Organisatoren können sie hier ändern."),
        ("h3", "Von Hand eingreifen"),
        ("ul", [
            "**Satz manuell überschreiben** (aufklappbar am Tag): voller und halber Satz in EUR, wenn die Automatik daneben liegt. Der Tag ist dann als „manuell“ markiert.",
            "**↺ Auto** am Tag: nimmt die manuelle Markierung zurück; Land und Sätze werden wieder automatisch ermittelt.",
            "**🔄 VMA neu berechnen** (Reise-Seite): rechnet die Tage der Reise neu. Normalerweise nicht nötig, weil die App automatisch rechnet. Tage mit „manuell“ bleiben unverändert (dort zuerst ↺ Auto).",
        ]),
        ("h3", "Trennungspauschale (betriebliche Sonderregelung)"),
        ("tab", ["Fall", "Betrag"], [
            ["Voller Reisetag am Samstag oder Sonntag", "80 EUR"],
            ["An- oder Abreisetag (halber Satz) am Wochenende", "40 EUR – aber nur, wenn die 12-Uhr-Grenze erfüllt ist: "
                                                                   "Antritt-Tag nur bei Abfahrt **vor** 12 Uhr, End-Tag nur bei Ende **nach** 12 Uhr"],
            ["Werktag", "0 EUR"],
        ], [45, 55]),
        ("p", "Ist die tatsächliche Uhrzeit noch nicht erfasst, gilt vorläufig der Betrag ohne Prüfung. Am ersten und letzten Tag gibt es das Feld "
              "**„Reiseantritt (tatsächlich)“ / „Reiseende (tatsächlich)“**. Sobald die Zeit eingetragen ist, wird automatisch (auch rückwirkend) nachgerechnet. "
              "An Wochenendtagen kann die Pauschale außerdem von Hand gewählt werden."),
        ("h3", "VMA-Sätze"),
        ("p", "Menü **VMA-Sätze** (`/vma`) zeigt die amtlichen Sätze je Land und Städte-Sonderfälle. Mit **Import** werden die aktuellen Sätze übernommen."),
    ]},
    {"id": "b-bewirtung", "teil": "B", "titel": "Bewirtungsbelege", "bloecke": [
        ("p", "Der Reisende erstellt den Bewirtungsbeleg im Portal (siehe Teil A). Sie sehen und prüfen ihn wie jeden Beleg; er erscheint in Prüfung, offenen Aufgaben und Abschluss."),
        ("ul", [
            "Er braucht **immer einen Originalbeleg** (Rechnung oder Kassenbon, auch ein selbst erstellter Eigenbeleg ist als Original möglich).",
            "**Betrag, Trinkgeld und Wechselkurs** stehen nur am Originalbeleg. Der Bewirtungsbeleg hat keinen eigenen Betrag und keinen eigenen Kurs.",
            "**Davon Bewirtung (Teilbetrag):** Enthält die Rechnung mehr als die Bewirtung (z. B. Hotel), trägt der Reisende den Bewirtungsanteil ein. Abschluss, Abschluss-PDF und Reise-Seite zeigen dann Rechnung und Bewirtung getrennt (Hotel 450 € / Bewirtung 70 €); die Gesamtsumme bleibt gleich. Bei Fremdwährung wird der Anteil im gleichen Verhältnis in Euro umgerechnet. Leer = ganze Rechnung ist Bewirtung.",
            "**Elektronische Bestätigung:** Haken, Name, Zeitpunkt, optional Unterschrift, SHA-256-Prüfsumme. Danach ist der Beleg gesperrt; Änderungen nur als neue Version.",
            "Weiche Hinweise (sperren nichts): Rechnung ab 250 € (Name der Firma muss auf der Rechnung stehen); Bewirtung mehr als 14 Tage her.",
            "Der Bewirtungsbeleg ist prüfpflichtig und geht mit an Habel. Das Prüf-Deckblatt enthält einen Querverweis auf den zugehörigen Beleg.",
        ]),
        ("p", "Ansicht: `/bewirtung/…` und als PDF."),
    ]},
    {"id": "b-vorschuss", "teil": "B", "titel": "Reisevorschuss und Bargeld-Konto", "bloecke": [
        ("p", "Button **💵 Vorschuss** auf der Reise-Seite. Je Reisendem gibt es ein Bargeld-Konto je Währung."),
        ("tab", ["Buchungsart", "Wer erfasst", "Was"], [
            ["Vorschuss-Auszahlung", "Organisator", "Währung, Betrag und Kurs von Hand. Der Mitarbeiter quittiert im Portal."],
            ["Bargeld selbst beschafft", "Mitarbeiter im Portal", "Geldautomat, Wechselstube, Bank: Betrag und belasteter Euro-Betrag laut Auszug."],
            ["Rückgabe Bargeld", "Organisator", "Betrag und Abgabekurs von Hand. Der Mitarbeiter bestätigt zusätzlich."],
        ], [28, 22, 50]),
        ("p", "Jede Buchung wird **bestätigt** (Haken, Name, Zeitstempel, optional Unterschrift, SHA-256-Prüfsumme) und danach **gesperrt**; Änderung nur als neue Version "
              "(Korrigieren). Nach dem Bestätigen entsteht ein PDF-Beleg und ein Belegdatensatz ohne Betrag (zählt nicht als Kosten). "
              "**In die Abrechnung zählen nur bestätigte Buchungen.**"),
        ("p", "**Kursschreibweise** hier: 1 EUR = ? Fremdwährung (wie auf Wechselbelegen). Währungen: EUR, USD, GBP, CHF und viele weitere (u. a. MXN)."),
        ("h3", "Abrechnung je Währung"),
        ("ul", [
            "Firmengeld beim Mitarbeiter = Auszahlung − Rückgabe.",
            "**Rückgabe offen** = Firmengeld − Bar-Ausgaben (falls positiv).",
            "**Eigenanteil** = Bar-Ausgaben − Firmengeld (falls positiv): aus eigenem Geld bezahlt, wird zum Kurs der Eigenbeschaffung erstattet.",
            "**Bewertung der Bar-Belege:** Firmenanteil zum Auszahlungskurs, Eigenanteil zum Kurs der Eigenbeschaffung (jeweils gewichtet). "
            "Der Knopf **„Bewertung in die Belege übernehmen“** schreibt die Euro-Beträge in die Belege (überschrieben werden nur Belege ohne Euro-Endbetrag "
            "oder bereits aus dem Bargeld-Konto bewertete).",
        ]),
        ("p", "Solange Buchungen unbestätigt sind, ist die Abrechnung als „vorläufig“ gekennzeichnet."),
    ]},
    {"id": "b-kalender", "teil": "B", "titel": "Outlook-Einladungen", "bloecke": [
        ("p", "Bekommt ein Mitarbeiter den Haken **„📅 Reisetermine als Outlook-Einladung senden“**, erhält er für jeden **Flug, Zug und manuellen Termin** eine "
              "Kalender-Einladung an E-Mail-Adresse 1 (Zeiten, Nummer, Sitz, Terminal/Gleis). Erinnerungen: Flug 3 Stunden, Bahn 45 Minuten, Termin 30 Minuten vorher."),
        ("ul", [
            "**Änderungen und Stornos** werden automatisch nachgeführt (aktualisierter bzw. abgesagter Termin). Der Abgleich läuft im Takt von `/cron/flug-alerts`.",
            "**Wartezeit:** Änderungen werden erst gesendet, wenn sie [[ruhe_sekunden]] Sekunden unverändert geblieben sind (Einstellung KALENDER_RUHE_SEKUNDEN). Das verhindert eine Mail-Flut beim Korrigieren.",
            "Die Einladung enthält absolute Zeiten (Outlook rechnet in die eigene Zeitzone um); die Ortszeiten stehen zusätzlich im Text.",
            "**Montage über mehrere Tage** geht als **eine Terminserie** in einer Mail raus (mit Ausnahmen für abweichende Tage). Serien laufen in Berliner Zeit, damit die Uhrzeit über die Zeitumstellung gleich bleibt. Siehe Kapitel „Flug, Bahn und Termine“.",
            "Für **bereits vergangene** Termine werden keine Einladungen mehr gesendet. Sie bleiben „noch nicht gesendet“.",
        ]),
        ("p", "Seite **📅 Outlook** je Reise: Tabelle mit Reisendem, Termin, Status und zuletzt gesendet. **„Jetzt senden / aktualisieren“** überspringt die Wartezeit "
              "und zeigt: neu gesendet, aktualisiert, abgesagt, Fehler. Unter „Nicht berücksichtigt“ stehen Termine, die nicht eingeladen werden konnten (mit Grund)."),
        ("hinweis", "Ist der Mailserver nicht erreichbar, bricht der Lauf nach dem ersten Fehler ab und meldet das. Ursachensuche: Mail-Diagnose (Teil C)."),
    ]},
    {"id": "b-abschluss", "teil": "B", "titel": "Reise abschließen", "bloecke": [
        ("p", "Button **🧾 Abschluss** auf der Reise-Seite. Die Seite fasst alles für die Abrechnung zusammen:"),
        ("ul", [
            "**🌍 Verpflegungsmehraufwand:** Datum, Land, Satz, Abzüge, Netto je Tag (plus Trennungspauschale).",
            "**🧾 Belege (Rechnungen & Quittungen):** Datum, Art, Anbieter, Rechnungsnr., Betrag, MwSt, Hinweis, Final (Euro-Betrag bestätigt), Geprüft, Beleg.",
            "**⏱ Reise-/Arbeitszeiten** mit Stunden, Nachtstunden und **Zuschlägen** (je Tag und Summe je Mitarbeiter, siehe „Arbeitsstunden und Zuschläge“) und **🚗 Gefahrene Kilometer** aus den Eingaben der Reisenden im Portal.",
            "**✈ Geflogene Flugsegmente:** Summe, Liste (Abflug, Reisender, Flug, Strecke) und Aufteilung je Reisendem – auch im PDF.",
            "**🔗 Buchungen und zugehörige Rechnungen:** Je Buchung die Rechnungen mit Summe und Stand („vollständig belegt“ oder „es fehlen noch …“). Rechnungen tragen in der Belegtabelle den Verweis „🔗 Buchung #…“.",
            "Gelbe/rote Hinweise zeigen, was noch offen ist (z. B. ungeprüfte Belege, fehlende Kurse, **Buchungen ohne vollständige Rechnung/Quittung**).",
        ]),
        ("p", "**Buchung und Rechnungen in der Summe:** Eine Buchungsbestätigung zählt nicht doppelt. Sind ihre Rechnungen/Quittungen verknüpft und decken den Betrag, zählen nur die Rechnungen. "
              "Fehlt ein Teil, steht der **offene Rest** als orange Zeile „Rest der Buchung“ in der Tabelle und ist als „vorläufig“ in der Summe; ohne jede Rechnung zählt die ganze Buchung vorläufig. "
              "Ein Warnkasten nennt jede betroffene Buchung mit dem fehlenden Betrag. Dasselbe steht im PDF (Zeile „Rest Buchung #…“ und Hinweistext). "
              "Die Kachel „Kosten aus Belegen“ auf der Reise-Seite rechnet genauso."),
        ("p", "**Euro-Betrag bei Buchung mit Teilrechnungen:** Der echte Euro-Betrag und das Auslandsentgelt aus der Kreditkartenabrechnung hängen meist an der Buchung (gleicher Betrag in Fremdwährung). "
              "Damit nichts verloren geht, bekommt jede Rechnung ohne eigenen bestätigten Euro-Betrag ihren **Anteil** (Rechnungsbetrag geteilt durch Buchungsbetrag), ebenso der noch fehlende Rest. "
              "In der Tabelle steht dann „= … EUR (Anteil der Buchung)“. Die Summe entspricht damit dem abgerechneten Betrag plus Entgelt. "
              "Hat eine Rechnung schon einen eigenen bestätigten Euro-Betrag, gilt dieser. Fehlt der bestätigte Euro-Betrag an der Buchung, nimmt die App ihn aus dem Zahlungsnachweis in Euro. "
              "Gibt es keins von beidem, bleibt es bei der KI-Schätzung."),
        ("p", "**Was als Flugsegment zählt:** Jede Teilstrecke eines Flugbelegs. **Nicht gezählt** werden Zugfahrten – auch Rail&Fly und Lufthansa Express Rail, die eine LH-Nummer tragen. "
              "Ein Segment gilt als Zug, wenn die Gesellschaft Bahn/Rail/ICE o. ä. nennt, der Ort ein Bahnhof ist (z. B. „Köln Hbf“) oder der Code ein Bahnhofs-Code ist (z. B. QKL, ZWS, QDU). "
              "Stornierte Flüge (Status aus der Flugüberwachung) zählen ebenfalls nicht. Unter der Liste steht, wie viele Zugfahrten und stornierte Flüge nicht gezählt wurden."),
        ("p", "Fehlt bei einem Flug ein Flughafen-Code, wird er mitgezählt und mit „⚠ Flughafencode fehlt – bitte prüfen“ markiert. "
              "Stehen dieselben Flüge in zwei Belegen, entfernt die App sie **nicht** selbst: Ein roter Hinweis führt zu **Überschneidungen klären** (siehe Reiseplan), erst danach stimmt die Summe."),
        ("p", "**PDF** erzeugt die Reisekostenabrechnung. **📤 An Habel** führt zur Übergabe."),
    ]},
    {"id": "b-zuschlag", "teil": "B", "titel": "Arbeitsstunden und Zuschläge", "bloecke": [
        ("p", "Die Reisenden tragen im Portal je Tag **Reisezeit** und **Arbeitszeit** (von–bis) ein. Daraus rechnet die App **Stunden und Zuschläge** aus. "
              "Das Ergebnis steht im **Abschluss** und im **Abschluss-PDF**: je Tag Stunden, Nachtstunden und Zuschlagsstunden, darunter die Summe je Mitarbeiter mit den Stunden je Zuschlagssatz. "
              "Gerechnet wird **minutengenau**."),
        ("h3", "So wird gerechnet"),
        ("ul", [
            "**Tagesart:** Montag bis Freitag ist ein **Werktag**: Es gilt der Zuschlag für Reisezeit bzw. Arbeitszeit. An **Samstag, Sonntag und Feiertagen** gilt statt dessen der Satz für diesen Tag (für Reise- und Arbeitszeit).",
            "**Nacht:** Stunden zwischen „Beginn der Nacht“ und „Beginn des Tages“ bekommen den Nachtzuschlag. Ob er zum Tageszuschlag **addiert** wird oder nur **der höchste** gilt, steht in den Einstellungen.",
            "**Über Mitternacht:** Liegt das Ende vor dem Beginn (z. B. 20:00–02:00), läuft die Zeit in den nächsten Tag. Jede Minute zählt für den Kalendertag, an dem sie liegt (nach Mitternacht also z. B. für den Sonntag).",
            "**Zuschlagsstunden = Stunden × Zuschlag.** Beispiel: 8:00 Stunden Arbeit am Werktag mit 25 % = 2,00 Zuschlagsstunden.",
            "**Feiertage** sind die gesetzlichen Feiertage des eingestellten Bundeslandes (auch bei Arbeit im Ausland). Örtliche Sonderfälle wie das Augsburger Friedensfest sind nicht enthalten.",
            "**Zeitzonen:** Uhrzeiten gelten als **Ortszeit** des Landes der Tageskarte im Portal. Gibt ein Reisender eine Zeit in einer anderen Zeitzone an (Menü hinter der Uhrzeit), "
            "rechnet die App die **echte verstrichene Zeit** (z. B. Abflug 06:00 deutsche Zeit, Landung 09:00 Ortszeit Rumänien = 2:00). Nacht, Wochentag und Feiertag werden nach der Ortszeit bestimmt. "
            "Im Abschluss steht hinter abweichenden Zeiten die Zeitzone, im PDF ein Absatz „Zeitzone …“ unter der Tabelle.",
            "Fehlt Beginn oder Ende, ist die Zeit nicht lesbar oder dauert sie über 16 Stunden, steht in der Zeile ein **Hinweis**.",
        ]),
        ("tab", ["Beispiel (Standardwerte)", "Ergebnis"], [
            ["Arbeit Montag 08:00–16:00", "8:00 Stunden zu 25 % = 2,00 Zuschlagsstunden"],
            ["Reise Montag 20:00–23:30", "3:30 Stunden: bis 22:00 0 %, danach 25 % (Nacht) = 0,38 Zuschlagsstunden"],
            ["Arbeit Samstag 20:00–02:00", "2:00 zu 50 %, 2:00 zu 75 % (Samstag + Nacht), 2:00 zu 125 % (Sonntag + Nacht) = 5,00 Zuschlagsstunden"],
            ["Arbeit Sonntag 08:00–12:00", "4:00 Stunden zu 100 % = 4,00 Zuschlagsstunden"],
        ], [38, 62]),
        ("h3", "Einstellungen"),
        ("p", "**Einstellungen › ⏱ Zuschläge** (nur Organisatoren). Alle Werte sind hier änderbar; unten zeigt eine **Probe**, was die gespeicherten Werte für vier Beispiele ergeben."),
        ("tab", ["Einstellung", "Standard", "Bedeutung"], [
            ["Reisezeit (Reisetage, Mo–Fr)", "0 %", "Zuschlag auf Reisezeit an Werktagen"],
            ["Arbeitszeit (Arbeitstage, Mo–Fr)", "25 %", "Zuschlag auf Arbeitszeit an Werktagen"],
            ["Samstag", "50 %", "für Reise- und Arbeitszeit"],
            ["Sonntag", "100 %", "für Reise- und Arbeitszeit"],
            ["Feiertag", "100 %", "wie Sonntag"],
            ["Nacht", "25 %", "für Stunden in der Nacht"],
            ["Beginn der Nacht / des Tages", "22:00 / 06:00", "Die Nacht läuft vom einen zum anderen Zeitpunkt"],
            ["Treffen mehrere Zuschläge zu", "addieren", "Addieren (Samstag nachts = 50 % + 25 %) oder nur der höchste (50 %)"],
            ["Bundesland", "Bayern", "bestimmt die Feiertage"],
        ], [34, 16, 50]),
        ("hinweis", "Neue Werte gelten sofort für Abschluss und PDF aller Reisen, auch für bereits erfasste Zeiten. Die Zeiten selbst werden nicht verändert."),
    ]},
    {"id": "b-habel", "teil": "B", "titel": "Übergabe an Habel (DMS)", "bloecke": [
        ("p", "Button **📤 An Habel** (Reise-Seite oder Abschluss). Es geht **eine E-Mail je Reise** an `DMS_EMAIL_TO`: die Reisekostenabrechnung und "
              "**jeder Beleg als eigenes PDF** mit Prüf-Deckblatt."),
        ("ul", [
            "Übergeben werden die prüfpflichtigen Belegarten: Rechnung, Quittung, Bewirtungsbeleg, Vorschussquittung, Rückgabequittung, Bargeldbeschaffung.",
            "Die Seite zeigt vorab, welche Belege geprüft sind. **Senden ist gesperrt**, solange ein Beleg nicht geprüft ist oder das Original fehlt.",
            "**Erneutes Senden** braucht einen Haken.",
            "**Vorschau: Dateien der Mail:** zeigt Empfänger, Betreff, Text und alle Dateien zum Anklicken (Seiten, Größe) – ohne etwas zu senden. Funktioniert auch, solange noch nicht alles geprüft ist.",
            "Größtes erlaubtes Mailvolumen: [[habel_max_mb]] MB (Summe aller PDF-Anhänge, Einstellung HABEL_MAX_MB).",
        ]),
        ("p", "Am Beleg steht der Stand der ganzen Reise: Fortschritt der Prüfung, noch offene Belege und der Link zur Übergabe. Nach dem Senden steht dort „Mit der Reise an Habel gesendet am …“."),
        ("h3", "Einzelne Belege von Hand senden"),
        ("p", "Auf der **Reise-Seite** (unter dem Tagesverlauf) listet die Karte **📤 Belege einzeln an Habel senden** alle Rechnungen und Quittungen der Reise. "
              "Der Knopf **Einzeln senden** schickt **eine Mail mit genau diesem Beleg** (mit Prüf-Deckblatt) an `DMS_EMAIL_TO`. Es geschieht nie automatisch."),
        ("ul", [
            "Nur **Rechnungen und Quittungen**. Bewirtungsbelege, Vorschuss- und Bargeldbelege gehen nur mit der Reise-Übergabe.",
            "Nur **geprüfte** Belege mit gespeichertem Original. Sonst steht dort „erst prüfen“ oder „kein Original“.",
            "Vor dem Senden fragt der Browser nach. Ein schon gesendeter Beleg zeigt **Erneut einzeln senden** und fragt ausdrücklich nach.",
            "Der Einzelversand steht in der Karte und am Beleg („Einzeln an Habel gesendet am …“). Er zählt **nicht** als Reise-Übergabe: Der Stand „an Habel gesendet“ der Reise ändert sich nicht.",
            "Die Übergabe der ganzen Reise enthält diese Belege trotzdem noch einmal.",
        ]),
        ("p", "Fehlen zu einer Buchung noch Rechnungen oder Quittungen, zeigt die Übergabe-Seite einen **Hinweis** mit der Buchung und dem fehlenden Betrag. Er sperrt das Senden **nicht**."),
        ("p", "Auf dem **Dashboard** und unter **📋 Offene Aufgaben** erscheinen Reisen, die zur Übergabe bereit sind (zurückgekehrt, alle Belege geprüft, noch nicht gesendet). "
              "Die Liste der abgeschlossenen Reisen zeigt je Reise den Habel-Stand und einen Knopf „📤 Habel“."),
    ]},
    {"id": "b-kk", "teil": "B", "titel": "Kreditkarten-Abrechnung einspielen", "bloecke": [
        ("p", "**Einstellungen › 💳 Kreditkarten-Abrechnung.** Die Monatsabrechnung der Karte (PDF, Format qards BusinessCard / Sparkasse) wird mit den Belegen abgeglichen. "
              "So bekommen die Belege den **endgültigen Euro-Betrag** und die **echte Auslandsgebühr**."),
        ("ol", [
            "Abrechnung (PDF) wählen, Karteninhaber wählen.",
            "**Abrechnung prüfen:** Die App schlägt Zuordnungen von Abrechnungspositionen zu Belegen vor.",
            "Treffer kontrollieren und **übernehmen**.",
            "Unter **📋 Alle Zuordnungen** sehen Sie alle Belege mit Abgleich; eine Zuordnung lässt sich dort wieder lösen.",
        ]),
        ("hinweis", "Datenschutz: Es wird **keine KI** und kein externer Dienst genutzt. Das PDF wird nur im Arbeitsspeicher gelesen und nie gespeichert. "
                    "Gespeichert wird nur das Ergebnis am Beleg: endgültiger Euro-Betrag, echte Auslandsgebühr, Abrechnungsdatum."),
        ("h3", "Liste „Kreditkarten-Belege Ausland“ (FIWE)"),
        ("p", "**Einstellungen › 🌍 Kreditkarten-Belege Ausland.** Die Liste enthält **jede Rechnung und Quittung, die mit Kreditkarte im Ausland bezahlt wurde**: "
              "a) in Fremdwährung und b) in EUR, wenn das Land des Belegs nicht Deutschland ist. Jeder dieser Belege muss mit der VISA-Abrechnung abgeglichen werden, "
              "damit die Kosten stimmen."),
        ("ul", [
            "**Bis zum Abgleich** bleibt der Betrag in Originalwährung führend; daneben steht der geschätzte Euro-Wert mit „≈“. Nach dem Abgleich steht dort „=“ mit dem Betrag laut Abrechnung.",
            "**Nachtragen:** Datum der Kartenabrechnung wählen, je Beleg den **Euro-Betrag lt. VISA** und die **Gebühr** eintragen (Gebühr leer = bleibt unverändert) und **Eingaben speichern**. "
            "Bei EUR-Belegen genügt der Haken **abgeglichen**. Gespeichert werden nur Zeilen mit Eingabe oder Haken; Kommazahlen („12,40“) sind erlaubt.",
            "Der Abgleich wirkt wie der PDF-Abgleich: Der Beleg gilt als abgeglichen und verschwindet aus der Ansicht „Offen“. Lösen lässt er sich unter „Alle Zuordnungen“.",
            "**Filter** nach Status (offen, abgeglichen, alle) und nach Reise; **⬇ CSV (Excel)** exportiert die gefilterte Liste.",
            "Unter **📋 Offene Aufgaben** steht, wie viele Belege noch abzugleichen sind. Die Seite ist Organisatoren vorbehalten; wer sie nutzen soll, braucht diese Rolle.",
        ]),
    ]},
    {"id": "b-dashboard", "teil": "B", "titel": "Dashboard, Offene Aufgaben und Karte", "bloecke": [
        ("h3", "Dashboard"),
        ("p", "Startseite. Reisen in drei Gruppen: **Aktiv**, **Geplant** (nächste zuerst), **Abgeschlossen** (mit Link zum Abschluss). Dazu: offene Verspätungs-Alarme, "
              "Hinweise (z. B. Kurs nachtragen), Anzahl Belege im Posteingang und der Knopf **📬 Mails abrufen**. "
              "Läuft ein **Visum** in den nächsten 4 Wochen ab, steht oben die Karte **„🛂 Visa laufen ab“**, bis OK geklickt wird (siehe „Mitarbeiter verwalten“)."),
        ("h3", "📋 Offene Aufgaben (`/todo`)"),
        ("p", "Alles, was vor der finalen Abrechnung noch offen ist, über alle Reisen: Beträge in Fremdwährung ohne bestätigten Euro-Betrag, Belege ohne Prüfung, "
              "Buchungen, zu denen nach der Rückkehr noch Rechnung oder Quittung fehlt („🧾 Rechnung/Quittung fehlt noch“, mit Betrag), "
              "nicht abgerechnete Reisen mit dem Habel-Stand je Reise (geprüft, bereit, gesendet). "
              "Ganz oben steht die Karte **🛂 A1-Bescheinigung – Reisen in Europa** mit allen Reisen, bei denen eine A1 fehlt, zu prüfen oder nicht zugeordnet ist."),
        ("h3", "🗺 Karte (`/maps`)"),
        ("p", "Zeigt, wo sich zugeordnete Mitarbeiter bei laufenden Reisen aktuell befinden."),
    ]},

    # ─────────────────── TEIL C – Einstellungen und Betrieb (Sie) ───────────────────
    {"id": "c-einstellungen", "teil": "C", "titel": "Die Einstellungsseite", "bloecke": [
        ("p", "Das Zahnrad **⚙** im Menü öffnet die Einstellungsübersicht (nur Organisatoren). Die Kacheln:"),
        ("tab", ["Kachel", "Inhalt"], [
            ["🆕 Versionen", "Änderungsverlauf: Was sich in jeder Version geändert hat. Die Versionsnummer oben rechts führt ebenfalls dorthin."],
            ["📋 Offene Aufgaben", "Was vor der Abrechnung noch fehlt."],
            ["✈ Flug-/Bahn-Alerts", "Prüfplan und Testlauf der Verspätungs-Überwachung."],
            ["💾 Backups", "Tägliche Datensicherung, Liste, Knopf zum sofortigen Sichern."],
            ["📬 Posteingang", "Belege, die noch keiner Reise zugeordnet sind."],
            ["💰 VMA-Sätze", "Amtliche Sätze einsehen und importieren."],
            ["🗺 Karte", "Standorte der Mitarbeiter bei laufenden Reisen."],
            ["💳 Kreditkarten-Abrechnung", "Monatsabrechnung einspielen."],
            ["🌍 Kreditkarten-Belege Ausland", "Liste der im Ausland mit Karte bezahlten Belege zum Abgleich mit der VISA-Abrechnung."],
            ["⏱ Zuschläge", "Zuschläge für Reisezeit, Arbeitszeit, Nacht, Samstag, Sonntag, Feiertag; Beginn von Nacht und Tag; Bundesland."],
            ["📎 Reise-PDFs", "PDF-Dokumente für die Reisemappe, z. B. Kopie der Krankenversicherung (für alle oder einen Mitarbeiter)."],
            ["🔧 Mail-Diagnose", "Prüft den Postausgang Schritt für Schritt."],
            ["📖 Dokumentation", "Dieses Handbuch (Seite und PDF)."],
        ], [30, 70]),
    ]},
    {"id": "c-umgebung", "teil": "C", "titel": "Einstellungen (Umgebungsvariablen)", "bloecke": [
        ("p", "Die App wird über **Umgebungsvariablen** eingestellt. Sie werden beim Hosting-Dienst (Render) unter **Environment** eingetragen. "
              "Nach dem Speichern startet der Dienst neu. Passwörter und Schlüssel erscheinen hier nie im Klartext, nur „gesetzt“ oder „fehlt“."),
        ("live", "umgebung"),
    ]},
    {"id": "c-cron", "teil": "C", "titel": "Automatische Aufgaben (Cron)", "bloecke": [
        ("p", "Die App löst nichts von selbst aus. Ein **externer Cron-Dienst** (z. B. cron-job.org) ruft in festen Abständen Adressen der App auf. "
              "Jede Adresse braucht den Schlüssel: `https://IHRE-APP/cron/…?key=IHR_CRON_SECRET`."),
        ("live", "cron"),
        ("p", "Ohne gesetztes `CRON_SECRET` oder mit falschem Schlüssel antwortet die App mit „Ungültiger oder fehlender Schlüssel“."),
        ("h3", "Mails alle 5 Minuten abholen"),
        ("p", "Läuft ein eigener Server (z. B. bei Hetzner), reicht eine Zeile in dessen `crontab -e`:"),
        ("p", "`*/5 * * * * curl -fsS -m 300 \"https://IHRE-APP/cron/mails?key=IHR_CRON_SECRET\" > /dev/null`"),
        ("ul", [
            "**Kosten und Datenverkehr:** Ein Abruf ohne neue Mails kostet fast nichts (wenige KB, eine kurze Anfrage an das Postfach). 288 Abrufe am Tag sind vernachlässigbar. "
            "Kosten entstehen nur je verarbeitetem Beleg (KI-Analyse), genau wie beim Knopf.",
            "Die Adresse antwortet mit dem Ergebnis (verarbeitet, Belege erstellt, Duplikate, Fehler). Bei einem Fehler, etwa wenn das Postfach nicht erreichbar ist, antwortet sie mit HTTP 500; ein Cron-Dienst wie cron-job.org kann das melden.",
            "Läuft ein Abruf noch, wenn der nächste startet, antwortet die App „läuft noch“ und tut nichts. So werden Mails nie doppelt verarbeitet.",
        ]),
    ]},
    {"id": "c-mail", "teil": "C", "titel": "Mail-Versand und Mail-Diagnose", "bloecke": [
        ("p", "**Eingang** (IMAP): holt Belege aus dem Reisekosten-Postfach. **Ausgang** (SMTP): sendet Outlook-Einladungen, Portal-Links, Verspätungsalarme "
              "und die Habel-Übergabe."),
        ("h3", "Richtige Werte (Beispiel United Domains / UDAG)"),
        ("tab", ["Einstellung", "Wert"], [
            ["SMTP_HOST", "smtp.udag.de"],
            ["SMTP_PORT", "465 (SSL)"],
            ["SMTP_USER", "die **vollständige** Mailadresse, z. B. reisekosten@herrhammer.com"],
            ["SMTP_PASS", "Postfach-Passwort (leer lassen, wenn IMAP_PASS gelten soll)"],
        ], [30, 70]),
        ("warnung", "Kostenlose Render-Instanzen **blockieren ausgehende Mail-Ports** (25, 465, 587). Der Versand braucht eine bezahlte Instanz."),
        ("p", "Pro Lauf wird **eine** SMTP-Verbindung für alle Mails genutzt. Bricht sie ab, wird die Mail einmal mit neuer Verbindung wiederholt. "
              "Der Verbindungsaufbau wartet höchstens 15 Sekunden, das Senden selbst SMTP_TIMEOUT Sekunden."),
        ("h3", "Mail-Diagnose"),
        ("p", "**Einstellungen › 🔧 Mail-Diagnose › Diagnose starten.** Es wird keine Mail gesendet; die Prüfung dauert bis zu einer Minute. Sie prüft der Reihe nach: "
              "eingestellte Werte, ob SMTP_USER eine vollständige Adresse ist, ob ein Passwort gesetzt ist, die Erreichbarkeit der Ports 465 und 587 "
              "(für den eingestellten Server und für smtp.udag.de/smtps.udag.de), zum Vergleich den Eingang (IMAP, Port 993) und die Anmeldung. Am Ende steht die wahrscheinliche Ursache."),
        ("h3", "Typische Fehlermeldungen"),
        ("tab", ["Meldung", "Ursache und Lösung"], [
            ["SMTP nicht konfiguriert (… fehlt)", "Die genannte Einstellung ist nicht gesetzt. Eintragen, speichern, Neustart abwarten."],
            ["timed out / Verbindung nicht möglich", "Ports blockiert (kostenlose Render-Instanz) oder falscher Servername. Mail-Diagnose starten."],
            ["Sender address rejected: need fully-qualified address", "SMTP_USER ist keine vollständige Adresse. Die ganze Mailadresse eintragen."],
            ["Anmeldung fehlgeschlagen", "Anmeldename oder Passwort falsch. Mit den Zugangsdaten des Postfachs vergleichen."],
        ], [38, 62]),
    ]},
    {"id": "c-backup", "teil": "C", "titel": "Datensicherung", "bloecke": [
        ("p", "Täglich (durch `/cron/backup`) werden **alle Tabellen** als komprimiertes JSON in den S3-Speicher gesichert (Ordner `backups/`). "
              "Die **letzten 30** Sicherungen bleiben erhalten, ältere werden gelöscht."),
        ("p", "**Einstellungen › 💾 Backups:** zeigt die vorhandenen Sicherungen und hat den Knopf zum sofortigen Sichern."),
        ("p", "Ein Zurückspielen per Knopf gibt es nicht. Die Sicherung enthält für jede Tabelle die Spaltennamen und alle Zeilen und lässt sich damit im Ernstfall zurückspielen."),
        ("p", "Die Beleg-PDFs selbst liegen im S3-Speicher und sind nicht Teil der JSON-Sicherung."),
    ]},
    {"id": "c-system", "teil": "C", "titel": "Systemadressen", "bloecke": [
        ("tab", ["Adresse", "Zweck"], [
            ["`/init`", "Legt fehlende Tabellen und Spalten an und führt Reparaturen aus. **Löscht nichts.** Nach einer neuen Version aufrufen, wenn in den Änderungen darauf hingewiesen wird "
                        "oder eine Seite „Spalte fehlt“ meldet. Ohne Anmeldung erreichbar."],
            ["`/health`", "Antwortet „ok“, ohne Datenbank. Zum Wachhalten oder Überwachen."],
            ["`/version`", "Zeigt Version und Datenbanktyp."],
            ["`/test-openai`", "Prüft die Verbindung zu OpenAI und den Schlüssel."],
            ["`/debug-anon`", "Zeigt die Namen und Mailadressen, die anonymisiert werden. Ist die Liste leer, fehlen Mitarbeiter."],
            ["`/init-reset`", "NOTFALL: löscht **alle** Daten. Nur mit `?confirm=ja`. Im Normalbetrieb nie verwenden."],
        ], [24, 76]),
    ]},
    {"id": "c-fehler", "teil": "C", "titel": "Fehlersuche", "bloecke": [
        ("tab", ["Beobachtung", "Prüfen / Lösung"], [
            ["Beleg landet im Posteingang", "Reisecode fehlt oder ist falsch (Form JJ-NNN, Reise muss existieren). Beleg dort einer Reise zuordnen."],
            ["„IMAP nicht konfiguriert“", "IMAP_HOST, IMAP_USER, IMAP_PASS setzen."],
            ["„IMAP-Verbindung fehlgeschlagen“", "Zugangsdaten und Server des Postfachs prüfen."],
            ["Beleg: „Fehlende Pflichtfelder“", "Auf der Beleg-Seite „✏ Daten nachtragen“."],
            ["Beleg wird nicht ausgelesen", "OPENAI_API_KEY prüfen; `/test-openai` aufrufen."],
            ["Portal-Link „ungültig“", "Kürzel wurde geändert. Link auf der Reise-Seite neu senden."],
            ["Portal-Link beginnt nicht mit https://", "Weder PORTAL_BASE_URL noch die Render-Adresse sind bekannt. PORTAL_BASE_URL in Render eintragen (z. B. https://reisekosten.onrender.com)."],
            ["Mail „gesendet“, kommt aber nicht an", "Spam-/Junk-Ordner und Quarantäne des Empfängers prüfen. Jede Mail trägt Datum und eine eindeutige Message-ID. Postausgang mit der Mail-Diagnose prüfen."],
            ["„Link senden“ gesperrt", "Beim Mitarbeiter fehlt E-Mail-Adresse 1."],
            ["Outlook-Einladung fehlt", "Haken im Profil? E-Mail-Adresse 1? Termin schon vorbei? Mail-Diagnose starten."],
            ["„An Habel“ gesperrt", "Ein Beleg ist nicht geprüft oder das Original fehlt. DMS_EMAIL_TO gesetzt?"],
            ["Habel-Mail zu groß", "Summe der Anhänge über HABEL_MAX_MB. Wert erhöhen, soweit der Mailserver es erlaubt."],
            ["Kein Verspätungs-Alarm", "Cron `/cron/flug-alerts` jede Minute eingerichtet? API-Schlüssel gesetzt? „Jetzt testweise abrufen“ zeigt die Diagnose."],
            ["Falsches Land oder falscher VMA-Satz", "Am Tag **↺ Auto** klicken oder oben im Tagesverlauf **↺ Alle Tage auf Auto**. Notfalls den Satz am Tag manuell überschreiben. Die Diagnose steht unter `/reise/<Code>/vma-debug`."],
            ["„Kurs nachtragen“ bleibt stehen", "Kurs am Beleg eintragen oder Euro-Betrag final setzen (am besten über die Kreditkarten-Abrechnung)."],
            ["Seite meldet „Spalte fehlt“", "Einmal `/init` aufrufen."],
        ], [34, 66]),
    ]},
    {"id": "c-verlauf", "teil": "C", "titel": "Änderungsverlauf", "bloecke": [
        ("p", "Neueste Version zuerst. Der Verlauf wird direkt aus der App gelesen und ist deshalb immer vollständig."),
        ("live", "verlauf"),
    ]},
    {"id": "c-seiten", "teil": "C", "titel": "Seitenverzeichnis", "bloecke": [
        ("p", "Alle Adressen der App. Die Zahl hinter dem Zweck verweist auf das Kapitel dieses Handbuchs. Platzhalter in geschweiften Klammern "
              "(z. B. {code}) stehen für Reisecode, Beleg-Nummer usw."),
        ("live", "seiten"),
    ]},
    {"id": "c-pflege", "teil": "C", "titel": "Dokumentation und wie sie aktuell bleibt", "bloecke": [
        ("p", "Dieses Handbuch gibt es als **Seite** (Einstellungen › 📖 Dokumentation) und als **PDF** (Knopf „PDF herunterladen“). "
              "Die PDF wird bei jedem Abruf frisch erzeugt und enthält deshalb immer den aktuellen Stand, auch bei Version und Einstellungen."),
        ("tab", ["Ausgabe", "Inhalt", "Wer"], [
            ["Handbuch gesamt (Seite/PDF)", "Teil A, B und C", "Organisatoren"],
            ["Reisenden-Anleitung (PDF)", "Nur Teil A. Zum Weitergeben an Reisende.", "Organisatoren; Reisende im Portal über „📖 Anleitung“"],
        ], [30, 40, 30]),
        ("h3", "So bleibt es aktuell"),
        ("ul", [
            "Alle Texte stehen in **einer** Datei der App (`mod_doku.py`). Bei **jeder neuen Version** wird sie mit angepasst und die Prüfversion gesetzt.",
            "Eine **neue Einstellung** oder eine **neue Seite** ohne Eintrag im Handbuch lässt die automatischen Tests der App fehlschlagen – sie kann also nicht vergessen werden.",
            "Der **Änderungsverlauf** und der **Stand der Einstellungen** (gesetzt/fehlt) werden live aus der App gelesen.",
            "Weicht die Prüfversion des Handbuchs von der App-Version ab, zeigt die Seite einen Warnhinweis.",
        ]),
    ]},
]

_KAPITEL_NACH_ID = {k["id"]: k for k in KAPITEL}


# ══════════════════════════════════════════════════════════════════════════════
# KONTEXT, NUMMERIERUNG, TEXTE
# ══════════════════════════════════════════════════════════════════════════════
def kapitel_nummern() -> dict:
    """Kapitel-ID -> Nummer wie 'A1', 'B7', 'C3' (nach Reihenfolge in KAPITEL)."""
    zaehler: dict = {}
    nummern = {}
    for k in KAPITEL:
        zaehler[k["teil"]] = zaehler.get(k["teil"], 0) + 1
        nummern[k["id"]] = "%s%d" % (k["teil"], zaehler[k["teil"]])
    return nummern


def kontext_bauen(version: str, versionen: list, jetzt: str) -> dict:
    """Werte, die in Texte per [[name]] eingesetzt werden, plus Version und Verlauf."""
    def _zahl(name, std):
        try:
            return int(os.getenv(name, "") or std)
        except ValueError:
            return std
    try:
        habel = float(os.getenv("HABEL_MAX_MB", "") or 15)
    except ValueError:
        habel = 15
    return {
        "version": version,
        "versionen": versionen or [],
        "jetzt": jetzt,
        "postfach": os.getenv("IMAP_USER", "") or "das Reisekosten-Postfach",
        "portal_tage": _zahl("PORTAL_TAGE_VORHER", 5),
        "ruhe_sekunden": _zahl("KALENDER_RUHE_SEKUNDEN", 120),
        "habel_max_mb": ("%g" % habel).replace(".", ","),
    }


def teile_normalisieren(teile: str) -> str:
    """'ABC' / 'A' ... -> nur gültige Teile, in fester Reihenfolge."""
    t = "".join(x for x in "ABC" if x in (teile or "").upper())
    return t or "A"


def kapitel_fuer(teile: str) -> list:
    t = teile_normalisieren(teile)
    return [k for k in KAPITEL if k["teil"] in t]


def titel_fuer(teile: str) -> str:
    t = teile_normalisieren(teile)
    if t == "A":
        return "Anleitung für Reisende"
    return "Handbuch Reisekosten-App"


def _ersetzen(text: str, ctx: dict) -> str:
    return re.sub(r"\[\[(\w+)\]\]", lambda m: str(ctx.get(m.group(1), "")), text)


def _datum_de(zeit: str) -> str:
    """'2026-10-15 13:25' -> '15.10.2026 13:25'"""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}:\d{2}))?", zeit or "")
    if not m:
        return zeit or ""
    return "%s.%s.%s%s" % (m.group(3), m.group(2), m.group(1), (" " + m.group(4)) if m.group(4) else "")


# ══════════════════════════════════════════════════════════════════════════════
# LIVE-DATEN
# ══════════════════════════════════════════════════════════════════════════════
def umgebung_status() -> list:
    """Eine Zeile je Einstellung mit aktuellem Stand (Werte von Passwörtern/Schlüsseln werden nie gezeigt)."""
    zeilen = []
    for name, bereich, pflicht, standard, text, geheim in UMGEBUNG:
        wert = (os.getenv(name, "") or "").strip()
        ersatz = ""
        if not wert and name in _ERSATZ_EINSTELLUNG:
            wert = (os.getenv(_ERSATZ_EINSTELLUNG[name], "") or "").strip()
            ersatz = _ERSATZ_EINSTELLUNG[name]
        if wert:
            stand = "gesetzt" if geheim else (wert if len(wert) <= 46 else wert[:43] + "…")
            if ersatz:
                stand += " (über %s)" % ersatz
            ok = True
        elif standard:
            stand = "Standard: %s" % standard
            ok = True
        else:
            stand = "fehlt" if pflicht == "ja" else "nicht gesetzt"
            ok = pflicht != "ja"
        zeilen.append({"name": name, "bereich": bereich, "pflicht": pflicht, "standard": standard,
                       "text": text, "stand": stand, "ok": ok, "gesetzt": bool(wert)})
    return zeilen


def fehlende_pflicht_einstellungen() -> list:
    return [z["name"] for z in umgebung_status() if not z["ok"]]


# ══════════════════════════════════════════════════════════════════════════════
# VOLLSTÄNDIGKEITSPRÜFUNG (für die Tests)
# ══════════════════════════════════════════════════════════════════════════════
_RE_ENV = re.compile(r"""(?:getenv|environ\.get)\(\s*["']([A-Z][A-Z0-9_]+)["']|environ\[\s*["']([A-Z][A-Z0-9_]+)["']\s*\]""")


def einstellungen_im_code(verzeichnis: str | None = None) -> set:
    verz = verzeichnis or os.path.dirname(os.path.abspath(__file__))
    gefunden = set()
    for pfad in glob.glob(os.path.join(verz, "*.py")):
        try:
            with open(pfad, encoding="utf-8") as f:
                quelltext = f.read()
        except OSError:
            continue
        for m in _RE_ENV.finditer(quelltext):
            gefunden.add(m.group(1) or m.group(2))
    return gefunden


def undokumentierte_einstellungen(verzeichnis: str | None = None) -> list:
    """Einstellungen, die der Code liest, die aber nicht in UMGEBUNG stehen. Muss leer sein."""
    return sorted(einstellungen_im_code(verzeichnis) - {u[0] for u in UMGEBUNG})


def veraltete_einstellungen(verzeichnis: str | None = None) -> list:
    """Einträge in UMGEBUNG, die der Code gar nicht mehr liest. Muss leer sein."""
    return sorted({u[0] for u in UMGEBUNG} - einstellungen_im_code(verzeichnis))


def _get_adressen(app) -> set:
    adressen = set()
    for r in getattr(app, "routes", []):
        pfad = getattr(r, "path", None)
        methoden = getattr(r, "methods", None) or set()
        if pfad and "GET" in methoden and pfad not in _FREMDE_ADRESSEN:
            adressen.add(pfad)
    return adressen


def undokumentierte_seiten(app) -> list:
    """GET-Adressen der App ohne Eintrag in SEITEN. Muss leer sein."""
    return sorted(_get_adressen(app) - {s[0] for s in SEITEN})


def veraltete_seiten(app) -> list:
    """Einträge in SEITEN, die es in der App nicht mehr gibt. Muss leer sein."""
    return sorted({s[0] for s in SEITEN} - _get_adressen(app))


def kaputte_verweise() -> list:
    """SEITEN mit unbekannter Kapitel-ID. Muss leer sein."""
    return [s[0] for s in SEITEN if s[2] not in _KAPITEL_NACH_ID]


# ══════════════════════════════════════════════════════════════════════════════
# HTML
# ══════════════════════════════════════════════════════════════════════════════
_FETT = re.compile(r"\*\*(.+?)\*\*")
_CODE = re.compile(r"`([^`]+)`")

DOKU_CSS = """<style>
.doku{max-width:900px}
.doku h2{font-size:19px;margin:34px 0 4px;padding-top:6px;border-top:2px solid var(--border);color:var(--blue,#1d4ed8)}
.doku h2.teil{font-size:22px;margin-top:44px;border-top:3px solid var(--blue,#1d4ed8)}
.doku h3{font-size:15px;margin:18px 0 6px}
.doku p,.doku li{font-size:14px;line-height:1.55}
.doku li{margin-bottom:4px}
.doku code{background:#f1f5f9;padding:1px 5px;border-radius:4px;font-size:12.5px}
.doku .tocteil{font-weight:700;margin:12px 0 4px}
.doku .toc a{display:inline-block;margin:2px 14px 2px 0;font-size:13px;text-decoration:none}
.doku td,.doku th{vertical-align:top;font-size:13px}
@media print{nav,.nodruck{display:none!important}}
</style>"""


def _html_inline(text: str, ctx: dict) -> str:
    t = _esc(_ersetzen(text, ctx), quote=False)
    t = _FETT.sub(r"<b>\1</b>", t)
    t = _CODE.sub(r"<code>\1</code>", t)
    return t


def _html_tabelle(kopf: list, zeilen: list, ctx: dict, breiten: list | None = None) -> str:
    colgroup = ""
    if breiten:
        colgroup = "<colgroup>" + "".join('<col style="width:%s%%">' % b for b in breiten) + "</colgroup>"
    th = "".join("<th>%s</th>" % _html_inline(k, ctx) for k in kopf)
    tr = "".join("<tr>%s</tr>" % "".join("<td>%s</td>" % _html_inline(str(c), ctx) for c in z) for z in zeilen)
    return '<div class="table-wrap"><table>%s<thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>' % (colgroup, th, tr)


def _live_zeilen(name: str, ctx: dict, mit_stand: bool):
    """Gibt (Kopf, Zeilen, Breiten) für die Live-Tabellen zurück."""
    if name == "umgebung":
        kopf = ["Einstellung", "Pflicht", "Bedeutung"] + (["Stand jetzt"] if mit_stand else [])
        zeilen = []
        for z in umgebung_status():
            pflicht = {"ja": "ja", "Funktion": "für die Funktion", "nein": "optional"}[z["pflicht"]]
            text = z["text"] + (" Standard: %s." % z["standard"] if z["standard"] and "Standard" not in z["text"] else "")
            zeile = ["`%s`" % z["name"], pflicht, text]
            if mit_stand:
                zeile.append(("⚠ " if not z["ok"] else "") + z["stand"])
            zeilen.append(zeile)
        return kopf, zeilen, ([28, 9, 41, 22] if mit_stand else [26, 12, 62])
    if name == "cron":
        kopf = ["Adresse", "Takt", "Zweck"]
        return kopf, [["`%s`" % a, t, z] for a, t, z in CRON], [28, 16, 56]
    if name == "seiten":
        nummern = kapitel_nummern()
        kopf = ["Adresse", "Zweck", "Kapitel"]
        return kopf, [["`%s`" % p, z, nummern.get(k, "")] for p, z, k in SEITEN], [48, 40, 12]
    return None


def _verlauf_bloecke(ctx: dict) -> list:
    out = []
    for v in ctx.get("versionen") or []:
        out.append(("h3", "Version %s – %s" % (v.get("version", ""), _datum_de(v.get("zeit", "")))))
        out.append(("ul", list(v.get("aenderungen") or [])))
    return out


def _block_html(b: tuple, ctx: dict, live: bool) -> str:
    typ = b[0]
    if typ == "p":
        return "<p>%s</p>" % _html_inline(b[1], ctx)
    if typ == "h3":
        return "<h3>%s</h3>" % _html_inline(b[1], ctx)
    if typ in ("ul", "ol"):
        return "<%s>%s</%s>" % (typ, "".join("<li>%s</li>" % _html_inline(x, ctx) for x in b[1]), typ)
    if typ == "tab":
        return _html_tabelle(b[1], b[2], ctx, b[3] if len(b) > 3 else None)
    if typ == "hinweis":
        return '<div class="alert alert-info" style="font-size:13px">%s</div>' % _html_inline(b[1], ctx)
    if typ == "warnung":
        return '<div class="alert alert-warn" style="font-size:13px">%s</div>' % _html_inline(b[1], ctx)
    if typ == "live":
        if b[1] == "verlauf":
            return "".join(_block_html(x, ctx, live) for x in _verlauf_bloecke(ctx))
        kopf, zeilen, breiten = _live_zeilen(b[1], ctx, live)
        return _html_tabelle(kopf, zeilen, ctx, breiten)
    return ""


def html_inhalt(teile: str, ctx: dict, live: bool = True) -> str:
    """Handbuch-Text als HTML (ohne Seitenrahmen). live=True zeigt den Stand der Einstellungen."""
    t = teile_normalisieren(teile)
    nummern = kapitel_nummern()
    kap = kapitel_fuer(t)
    toc = ""
    for teil in "ABC":
        if teil not in t:
            continue
        links = "".join('<a href="#%s">%s %s</a>' % (k["id"], nummern[k["id"]], _esc(k["titel"]))
                        for k in kap if k["teil"] == teil)
        toc += '<div class="tocteil">%s</div><div>%s</div>' % (_esc(TEILE[teil]), links)
    teile_html = ""
    letzter_teil = ""
    for k in kap:
        if k["teil"] != letzter_teil:
            teile_html += '<h2 class="teil">%s</h2>' % _esc(TEILE[k["teil"]])
            letzter_teil = k["teil"]
        teile_html += '<section id="%s"><h2>%s · %s</h2>%s</section>' % (
            k["id"], nummern[k["id"]], _esc(k["titel"]),
            "".join(_block_html(b, ctx, live) for b in k["bloecke"]))
    kopf = ('<p style="color:var(--muted);font-size:13px;margin:0 0 8px">Herrhammer Reisekosten · Version %s · Stand %s</p>'
            % (_esc(str(ctx.get("version", ""))), _esc(str(ctx.get("jetzt", "")))))
    return '%s<div class="doku">%s<div class="card"><div class="card-body toc">%s</div></div>%s</div>' % (
        DOKU_CSS, kopf, toc, teile_html)


# ══════════════════════════════════════════════════════════════════════════════
# PDF (reportlab)
# ══════════════════════════════════════════════════════════════════════════════
_PDF_ERSATZ = {"→": "›", "←": "‹", "✓": "ok", "✔": "ok", "✖": "x", "⚠": "!", "ℹ": "i", "≥": ">=", "≤": "<=",
               " ": " ", "‑": "-", "−": "-", "−": "-", "·": "·"}


def _pdf_zeichen(text: str) -> str:
    """Behält nur Zeichen, die die Standard-PDF-Schrift (Windows-1252) darstellen kann. Emojis entfallen."""
    out = []
    for c in text:
        try:
            c.encode("cp1252")
            out.append(c)
        except UnicodeEncodeError:
            out.append(_PDF_ERSATZ.get(c, ""))
    if not text.strip():
        return ""
    t = re.sub(r"[ ]{2,}", " ", "".join(out)).strip()
    t = re.sub(r"([„(])\s+", r"\1", t)         # Lücke nach entferntem Emoji direkt hinter öffnendem Anführungszeichen/Klammer
    return re.sub(r"(<b>)\s+", r"\1", t)


def _pdf_inline(text: str, ctx: dict) -> str:
    t = _esc(_ersetzen(text, ctx), quote=False)
    t = _FETT.sub(r"<b>\1</b>", t)
    t = _CODE.sub(r'<font face="Courier">\1</font>', t)
    # Zeichen bereinigen, ohne die Tags zu beschädigen (Tags enthalten nur ASCII)
    return _pdf_zeichen(t)


def pdf_erzeugen(teile: str, ctx: dict, live: bool = True) -> bytes:
    """Erzeugt das Handbuch als PDF (Titelseite, Inhaltsverzeichnis, Kopf-/Fußzeile, Lesezeichen)."""
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import (BaseDocTemplate, Frame, Image, NextPageTemplate, PageBreak,
                                    PageTemplate, Paragraph, Spacer, Table, TableStyle)
    from reportlab.platypus.tableofcontents import TableOfContents

    t = teile_normalisieren(teile)
    nummern = kapitel_nummern()
    titel = titel_fuer(t)
    version = str(ctx.get("version", ""))
    stand = str(ctx.get("jetzt", ""))
    blau = colors.HexColor("#1e3a8a")
    grau = colors.HexColor("#64748b")

    basis = ParagraphStyle("Basis", fontName="Helvetica", fontSize=9.5, leading=13.5, alignment=TA_LEFT, spaceAfter=5)
    st_h1 = ParagraphStyle("H1", parent=basis, fontName="Helvetica-Bold", fontSize=20, leading=24, textColor=blau,
                           spaceBefore=0, spaceAfter=10, keepWithNext=1)
    st_h2 = ParagraphStyle("H2", parent=basis, fontName="Helvetica-Bold", fontSize=14, leading=18, textColor=blau,
                           spaceBefore=14, spaceAfter=6, keepWithNext=1)
    st_h3 = ParagraphStyle("H3", parent=basis, fontName="Helvetica-Bold", fontSize=10.5, leading=14,
                           spaceBefore=8, spaceAfter=3, keepWithNext=1)
    st_li = ParagraphStyle("LI", parent=basis, leftIndent=16, bulletIndent=4, spaceAfter=2.5)
    st_zelle = ParagraphStyle("Zelle", parent=basis, fontSize=8.5, leading=11.5, spaceAfter=0)
    st_kopf = ParagraphStyle("ZelleKopf", parent=st_zelle, fontName="Helvetica-Bold")
    st_toc1 = ParagraphStyle("TOC1", parent=basis, fontName="Helvetica-Bold", fontSize=10.5, leading=16, spaceBefore=6)
    st_toc2 = ParagraphStyle("TOC2", parent=basis, fontSize=9.5, leading=14, leftIndent=14)

    breite = A4[0] - 40 * mm

    def kopf_fuss(canv, doc):
        canv.saveState()
        canv.setFont("Helvetica", 8)
        canv.setFillColor(grau)
        canv.drawString(20 * mm, 12 * mm, "Herrhammer Reisekosten – %s" % _pdf_zeichen(titel))
        canv.drawCentredString(A4[0] / 2, 12 * mm, "Version %s" % version)
        canv.drawRightString(A4[0] - 20 * mm, 12 * mm, "Seite %d" % doc.page)
        canv.setStrokeColor(colors.HexColor("#cbd5e1"))
        canv.line(20 * mm, 16 * mm, A4[0] - 20 * mm, 16 * mm)
        canv.restoreState()

    class _Doc(BaseDocTemplate):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self._lesezeichen = 0

        def beforeDocument(self):
            self._lesezeichen = 0          # je Durchlauf neu zählen, sonst ändern sich die Schlüssel und das Inhaltsverzeichnis wird nie fertig

        def afterFlowable(self, fl):
            if isinstance(fl, Paragraph) and fl.style.name in ("H1", "H2"):
                ebene = 0 if fl.style.name == "H1" else 1
                text = fl.getPlainText()
                schluessel = "kap%d" % self._lesezeichen
                self._lesezeichen += 1
                self.canv.bookmarkPage(schluessel)
                self.canv.addOutlineEntry(text, schluessel, level=ebene, closed=(ebene == 1))
                self.notify("TOCEntry", (ebene, text, self.page, schluessel))

    puffer = io.BytesIO()
    doc = _Doc(puffer, pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm, topMargin=18 * mm, bottomMargin=22 * mm,
               title="%s – Version %s" % (_pdf_zeichen(titel), version), author="Herrhammer Reisekosten")
    rahmen = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="f")
    doc.addPageTemplates([
        PageTemplate(id="Titel", frames=[rahmen]),
        PageTemplate(id="Normal", frames=[rahmen], onPage=kopf_fuss),
    ])

    def tabelle(kopf, zeilen, breiten=None):
        daten = [[Paragraph(_pdf_inline(k, ctx), st_kopf) for k in kopf]]
        for z in zeilen:
            daten.append([Paragraph(_pdf_inline(str(c), ctx), st_zelle) for c in z])
        if breiten:
            summe = float(sum(breiten))
            cw = [breite * b / summe for b in breiten]
        else:
            cw = [breite / len(kopf)] * len(kopf)
        tb = Table(daten, colWidths=cw, repeatRows=1)
        tb.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return [tb, Spacer(1, 6)]

    def kasten(text, farbe, rand):
        tb = Table([[Paragraph(_pdf_inline(text, ctx), st_zelle)]], colWidths=[breite])
        tb.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), farbe),
            ("LINEBEFORE", (0, 0), (0, -1), 2.5, rand),
            ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        return [tb, Spacer(1, 6)]

    def baustein(b):
        typ = b[0]
        if typ == "p":
            return [Paragraph(_pdf_inline(b[1], ctx), basis)]
        if typ == "h3":
            return [Paragraph(_pdf_inline(b[1], ctx), st_h3)]
        if typ == "ul":
            return [Paragraph(_pdf_inline(x, ctx), st_li, bulletText="•") for x in b[1]] + [Spacer(1, 3)]
        if typ == "ol":
            return [Paragraph(_pdf_inline(x, ctx), st_li, bulletText="%d." % (i + 1)) for i, x in enumerate(b[1])] + [Spacer(1, 3)]
        if typ == "tab":
            return tabelle(b[1], b[2], b[3] if len(b) > 3 else None)
        if typ == "hinweis":
            return kasten(b[1], colors.HexColor("#eff6ff"), colors.HexColor("#3b82f6"))
        if typ == "warnung":
            return kasten(b[1], colors.HexColor("#fffbeb"), colors.HexColor("#f59e0b"))
        if typ == "live":
            if b[1] == "verlauf":
                out = []
                for x in _verlauf_bloecke(ctx):
                    out += baustein(x)
                return out
            kopf, zeilen, breiten = _live_zeilen(b[1], ctx, live)
            return tabelle(kopf, zeilen, breiten)
        return []

    # ── Titelseite ──
    story = []
    logo = None
    for kandidat in (os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "logo3.png"),
                     os.path.join("static", "logo3.png")):
        if os.path.exists(kandidat):
            logo = kandidat
            break
    story.append(Spacer(1, 30 * mm))
    if logo:
        try:
            iw, ih = ImageReader(logo).getSize()
            b = 60 * mm
            story.append(Image(logo, width=b, height=b * ih / float(iw)))
            story.append(Spacer(1, 14 * mm))
        except Exception:
            pass
    st_titel = ParagraphStyle("Titel", parent=basis, fontName="Helvetica-Bold", fontSize=30, leading=36, textColor=blau)
    st_unter = ParagraphStyle("Untertitel", parent=basis, fontSize=13, leading=18, textColor=grau)
    story.append(Paragraph(_pdf_zeichen(titel), st_titel))
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph("Herrhammer Reisekosten-App", st_unter))
    story.append(Paragraph("Version %s · Stand %s" % (_esc(version), _esc(stand)), st_unter))
    story.append(Spacer(1, 10 * mm))
    inhalt_teile = "<br/>".join(_pdf_zeichen(TEILE[x]) for x in "ABC" if x in t)
    story.append(Paragraph(inhalt_teile, st_unter))
    story.append(NextPageTemplate("Normal"))
    story.append(PageBreak())

    # ── Inhaltsverzeichnis ──
    story.append(Paragraph("Inhalt", st_h1.clone("InhaltTitel")))
    toc = TableOfContents()
    toc.levelStyles = [st_toc1, st_toc2]
    toc.dotsMinLevel = 0
    story.append(toc)
    story.append(PageBreak())

    # ── Inhalt ──
    letzter_teil = ""
    for k in kapitel_fuer(t):
        if k["teil"] != letzter_teil:
            if letzter_teil:
                story.append(PageBreak())
            story.append(Paragraph(_pdf_zeichen(TEILE[k["teil"]]), st_h1))
            letzter_teil = k["teil"]
        story.append(Paragraph("%s  %s" % (nummern[k["id"]], _pdf_zeichen(k["titel"])), st_h2))
        for b in k["bloecke"]:
            story += baustein(b)

    doc.multiBuild(story)
    return puffer.getvalue()
