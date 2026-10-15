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
DOKU_GEPRUEFT_FUER = "3.24.1"

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
     "Geheimer Schlüssel für die Anmeldung (Login-Cookie). Lang und zufällig wählen. Ohne ihn gilt ein unsicherer Standardwert.", True),
    ("CRON_SECRET", "Grundbetrieb", "ja", "",
     "Passwort für die automatischen Aufgaben (Adressen /cron/…?key=…). Ohne ihn lehnen alle Cron-Adressen ab.", True),
    ("PORTAL_BASE_URL", "Grundbetrieb", "ja", "",
     "Öffentliche Adresse der App (z. B. https://reisekosten.onrender.com), ohne Schrägstrich am Ende. Daraus werden die Portal-Links in den Mails gebaut.", False),
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
    ("/cron/portal-mails?key=…", "einmal täglich",
     "Schickt den Portal-Link an alle Reisenden, deren Reise in PORTAL_TAGE_VORHER Tagen beginnt und die noch keinen Link bekommen haben."),
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
    ("/mitarbeiter/{kuerzel}/bearbeiten", "Mitarbeiter bearbeiten, Kürzel ändern, Passwort setzen (nur Organisatoren)", "b-mitarbeiter"),
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
    ("/reise/{code}/reiseplan/pdf", "Reiseplan als PDF (Reisemappe oder zum Mailversand)", "b-plan"),
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
    ("/einstellungen/mail-diagnose", "Mail-Diagnose: Postausgang Schritt für Schritt prüfen", "c-mail"),
    ("/einstellungen/dokumentation", "Dieses Handbuch", "c-pflege"),
    ("/einstellungen/dokumentation/pdf", "Dieses Handbuch als PDF", "c-pflege"),
    ("/portal/{token}", "Portal-Startseite der Reisenden: Tage, Kilometer, Bewirtung, Vorschuss", "a-start"),
    ("/portal/{token}/anleitung", "Anleitung für Reisende (Teil A)", "a-start"),
    ("/portal/{token}/anleitung/pdf", "Anleitung für Reisende als PDF", "a-start"),
    ("/portal/{token}/beleg-hochladen", "Reisende laden einen Beleg hoch", "a-belege"),
    ("/portal/{token}/kilometer/{kid}/loeschen", "Kilometer-Eintrag löschen", "a-km"),
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
            "**Jeden Reisetag ausfüllen:** welche Mahlzeiten gestellt wurden und deine Zeiten.",
            "**Kilometer eintragen**, falls du mit Firmen- oder Privat-Pkw gefahren bist.",
            "**Bewirtungsbeleg erstellen**, wenn du Gäste eingeladen hast.",
            "**Bargeld bestätigen** (Vorschuss, Rückgabe) und selbst abgehobenes Geld eintragen.",
        ]),
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
            ["Reisebeginn / Reiseende", "Uhrzeit, wann du tatsächlich losgefahren bzw. angekommen bist."],
            ["Arbeitsbeginn / Arbeitsende", "Uhrzeit, wann die Arbeit am Reiseziel begann und endete."],
            ["Notiz", "Freie Bemerkung, z. B. „Flug 2 Stunden verspätet“."],
        ], [34, 66]),
        ("p", "Mit **Speichern** (je Tag) wird die Karte gesichert. Du kannst jederzeit zurückkommen und ändern."),
        ("hinweis", "Warum die Mahlzeiten wichtig sind: Der Verpflegungsmehraufwand (Tagegeld) wird gekürzt, wenn Mahlzeiten gestellt wurden – "
                    "Frühstück um 20 %, Mittag- und Abendessen um je 40 % des vollen Tagessatzes des Landes."),
        ("p", "Die Markierung **½ Satz** an einem Tag bedeutet, dass an diesem Tag nur der halbe Satz gilt (z. B. An- oder Abreisetag)."),
    ]},
    {"id": "a-belege", "teil": "A", "titel": "Belege hochladen", "bloecke": [
        ("ol", [
            "Im Portal auf **„📎 Beleg hochladen“** tippen.",
            "Datei wählen: PDF oder Foto (JPG, PNG, HEIC, WEBP).",
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
            "**+ Hinzufügen** tippen.",
        ]),
        ("p", "Ein Eintrag lässt sich mit 🗑 wieder löschen. Ein Datum ist nicht nötig."),
    ]},
    {"id": "a-bewirtung", "teil": "A", "titel": "Bewirtungsbeleg (Gäste eingeladen)", "bloecke": [
        ("p", "Nur wenn du **Gäste** (z. B. Kunden) eingeladen hast. Eigene Mahlzeiten auf der Reise gehören **nicht** hierher. "
              "Die Rechnung oder der Kassenbon bleibt immer der Originalbeleg."),
        ("ol", [
            "**Zuerst den Originalbeleg hochladen** (Kapitel „Belege hochladen“).",
            "Im Portal bei „🍽 Bewirtungsbelege“ auf **+ Bewirtungsbeleg erstellen** tippen.",
            "Den Originalbeleg aus der Liste wählen. Tag, Ort, Betrag und Währung kommen automatisch aus dem Beleg.",
            "**Anlass** und **Teilnehmer** (Gäste, Firma) eintragen.",
            "Speichern, auf der Prüfseite alles kontrollieren und **elektronisch bestätigen** (Haken, Name, optional Unterschrift).",
        ]),
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
            ["Ich habe eine Mahlzeit vergessen.", "Link öffnen, Haken setzen, Speichern. Das geht jederzeit."],
            ["Ich habe einen falschen Beleg hochgeladen.", "Dem Organisator Bescheid geben. Er löscht oder korrigiert den Beleg."],
            ["Der Beleg erscheint nicht im Portal.", "Das ist normal. Hochgeladene Belege sehen nur die Organisatoren."],
            ["Ich war mit dem Mietwagen unterwegs.", "Nichts eintragen. Kilometer sind nur für Firmen- oder Privat-Pkw."],
            ["Ich habe Bargeld vom Organisator bekommen.", "In der Karte „Reisevorschuss / Bargeld“ antippen und bestätigen."],
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
            ["🔄 VMA neu berechnen", "Erzeugt die Tage der Reise neu."],
            ["🔍 VMA-Debug", "Zeigt, wie das Land des Rückreisetags ermittelt wurde."],
            ["✏ Bearbeiten", "Titel, Daten, Reisende und Notiz ändern."],
            ["🌍 + Land", "Weiteres Land mit Zeitraum hinzufügen. Vorhandene Länder lassen sich über ✏ ändern oder löschen."],
        ], [28, 72]),
        ("p", "Darunter folgen die Karte „🔗 Reisenden-Zugänge (Portal)“ und der „📅 Tagesverlauf & VMA“ mit allen Belegen und Terminen je Tag."),
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
              "die ihn noch nicht bekommen haben (Teil C, „Automatische Aufgaben“). Die Links werden mit `PORTAL_BASE_URL` gebaut."),
    ]},
    {"id": "b-eingang", "teil": "B", "titel": "Belege hereinbekommen", "bloecke": [
        ("p", "Belege kommen auf drei Wegen in die App. Jeder Beleg wird ausgelesen: Text lesen, Namen und Mailadressen der Mitarbeiter unkenntlich machen "
              "(Anonymisierung), KI-Analyse, Ablage der PDFs im S3-Speicher (Original, anonymisiert, Analyse)."),
        ("h3", "1. Per E-Mail an das Reisekosten-Postfach"),
        ("ul", [
            "Der Reisecode (z. B. `26-007`) muss im Betreff oder Text stehen. Nur eine bestehende Reise gilt als Treffer.",
            "Abruf von Hand über **📬 Mails** (Menü, Dashboard oder Belegliste). Es gibt keinen automatischen Abruf.",
            "Geholt werden nur **ungelesene** Mails. Jeder Anhang (PDF, JPG, JPEG, PNG, HEIC, WEBP, XML) wird ein eigener Beleg. "
            "Ohne Anhang wird der Mailtext (mehr als 50 Zeichen) selbst zum Beleg.",
            "Verarbeitete Mails werden im Postfach **gelöscht**. Doppelte Weiterleitungen werden über die Message-ID erkannt.",
            "Das Ergebnis zeigt: Mails verarbeitet, Belege erstellt, Duplikate, Fehler (mit Details).",
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
    ]},
    {"id": "b-belegliste", "teil": "B", "titel": "Die Belegliste", "bloecke": [
        ("p", "Menü **Belege** zeigt alle Belege. Filter: Jahr, Monat, Reise, Geprüft, Problem (z. B. fehlende Pflichtfelder) und Suche "
              "(Anbieter, Dateiname, Nummer). „Zurücksetzen“ löscht die Filter. Ein Klick auf einen Beleg öffnet die Beleg-Seite."),
    ]},
    {"id": "b-beleg", "teil": "B", "titel": "Beleg prüfen und ergänzen", "bloecke": [
        ("p", "Die Beleg-Seite (`/beleg/…`) zeigt von oben nach unten:"),
        ("ul", [
            "**📊 KI-Analyse:** erkannte Daten (Anbieter, Datum, Betrag, MwSt, Währung, Transportart, Segmente). Fehlt etwas, steht dort „Fehlende Pflichtfelder“. "
            "Mit **✏ Daten nachtragen** (Organisator) ergänzen oder korrigieren.",
            "**Belegart** und **Bezahlart** (siehe Tabellen unten). Bei Kreditkarte wählen Sie „Welche Karte?“.",
            "**📎 Dokumente:** Original-PDF, 🔒 Anonymisiert, 🔍 Analyse-PDF; Knöpfe zum erneuten Analysieren und erneuten Anonymisieren; Beleg löschen; Reise zuordnen.",
            "**🔗 Verknüpfte Belege:** Zusammengehörige Belege (z. B. Rechnung und Buchungsbestätigung) lassen sich verknüpfen und wieder trennen.",
            "**💰 Eurobetrag – Endabrechnung** und Kurs (siehe „Beträge, Fremdwährung und Kurse“).",
            "**✅ Prüfung & Archivierung:** Prüfvermerk eintragen und bestätigen. Danach steht dort „Geprüft von …“, und das Prüf-Deckblatt ist als PDF abrufbar.",
        ]),
        ("hinweis", "Von der App selbst erzeugte Belege (Bewirtungsbeleg, Vorschuss-, Rückgabequittung, Bargeldbeschaffung) lassen sich nicht neu analysieren oder anonymisieren "
                    "und brauchen keine Rechnungs-Pflichtfelder."),
        ("h3", "Belegarten"),
        ("tab", ["Belegart", "An Habel (aktuell)", "Hinweis"], [
            ["Rechnung", "ja", "Standardfall."],
            ["Quittung", "ja", "Kassenbon, Quittung."],
            ["Bewirtungsbeleg (Eigenbeleg)", "ja", "Vom Reisenden im Portal erstellt, siehe Kapitel Bewirtung."],
            ["Vorschussquittung / Bargeldbeschaffung / Rückgabequittung", "ja", "Selbst erzeugt aus dem Bargeld-Konto, ohne eigenen Betrag."],
            ["Buchungsbestätigung", "nein", "Betrag ist hier optional. Liefert Flug-/Zugdaten."],
            ["✈ Flugänderung/Umbuchung", "nein", "Löst die Änderungsklärung aus (siehe „Flug, Bahn und Termine“)."],
            ["Tankbeleg (Rechnung), Hotel, Taxi, Bewirtung, Sonstige Kosten, Sonstiges", "nein", "Zur Einordnung der Ausgabe. Nicht prüfpflichtig für die Habel-Übergabe."],
        ], [42, 16, 42]),
        ("h3", "Bezahlarten"),
        ("p", "💳 Kreditkarte · 💵 Bar · 🏦 Überweisung · 🅿 PayPal · 🤝 Durch Kunde · ❓ Unbekannt."),
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
        ("p", "Reise-Seite: **+ Termin** am Tag oder im Reiseplan **+ Ereignis**. Typen: Termin, Kundenbesuch, Fahrt/Taxi, Mietwagen, Hotel, Sonstiges. "
              "Felder: Titel, Typ, Datum, Uhrzeit von/bis, Ort/Adresse (mit Google-Vorschlägen, wenn GOOGLE_PLACES_API_KEY gesetzt ist), Ansprechpartner, Telefon, Notiz. "
              "Termine lassen sich ändern und löschen."),
        ("h3", "Reiseplan"),
        ("p", "**🗺 Reiseplan** zeigt alle Flüge, Züge und Termine in zeitlicher Reihenfolge. Zwei PDF-Varianten: „für Reisemappe Mitarbeiter“ und „zum Emailversand“ (ohne Buchungscodes)."),
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
        ("p", "Der **Tagesverlauf & VMA** auf der Reise-Seite zeigt je Tag: Datum, Land, Satz, Abzüge, **VMA netto** und alle Belege und Termine des Tages."),
        ("h3", "Land und Satz"),
        ("ul", [
            "Das **Land je Tag** wird automatisch aus Flügen, Hotelbelegen und der Länder-Zeitleiste der Reise ermittelt.",
            "**Rückreisetag:** Hier zählt der letzte Tätigkeitsort vor der Heimreise (Abflugort der ersten Etappe des Tages), nicht der Ankunftsort. "
            "Bei eintägigen Reisen das zuletzt besuchte ausländische Land. Wie die App entschieden hat, zeigt **🔍 VMA-Debug**.",
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
            "**🔄 VMA neu berechnen** (Reise-Seite): erzeugt die Tage der Reise neu, z. B. nach geänderten Ländern oder Belegen.",
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
            "**⏱ Reise-/Arbeitszeiten** und **🚗 Gefahrene Kilometer** aus den Eingaben der Reisenden im Portal.",
            "Gelbe/rote Hinweise zeigen, was noch offen ist (z. B. ungeprüfte Belege, fehlende Kurse).",
        ]),
        ("p", "**PDF** erzeugt die Reisekostenabrechnung. **📤 An Habel** führt zur Übergabe."),
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
        ("p", "Am Beleg selbst steht nur der Status („An Habel übertragen am …“) mit Link zur Reise-Übergabe."),
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
    ]},
    {"id": "b-dashboard", "teil": "B", "titel": "Dashboard, Offene Aufgaben und Karte", "bloecke": [
        ("h3", "Dashboard"),
        ("p", "Startseite. Reisen in drei Gruppen: **Aktiv**, **Geplant** (nächste zuerst), **Abgeschlossen** (mit Link zum Abschluss). Dazu: offene Verspätungs-Alarme, "
              "Hinweise (z. B. Kurs nachtragen), Anzahl Belege im Posteingang und der Knopf **📬 Mails abrufen**."),
        ("h3", "📋 Offene Aufgaben (`/todo`)"),
        ("p", "Alles, was vor der finalen Abrechnung noch offen ist, über alle Reisen: Beträge in Fremdwährung ohne bestätigten Euro-Betrag, Belege ohne Prüfung, "
              "unversendete Belege (Habel), nicht abgerechnete Reisen."),
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
            ["„Link senden“ gesperrt", "Beim Mitarbeiter fehlt E-Mail-Adresse 1."],
            ["Outlook-Einladung fehlt", "Haken im Profil? E-Mail-Adresse 1? Termin schon vorbei? Mail-Diagnose starten."],
            ["„An Habel“ gesperrt", "Ein Beleg ist nicht geprüft oder das Original fehlt. DMS_EMAIL_TO gesetzt?"],
            ["Habel-Mail zu groß", "Summe der Anhänge über HABEL_MAX_MB. Wert erhöhen, soweit der Mailserver es erlaubt."],
            ["Kein Verspätungs-Alarm", "Cron `/cron/flug-alerts` jede Minute eingerichtet? API-Schlüssel gesetzt? „Jetzt testweise abrufen“ zeigt die Diagnose."],
            ["Falsches Land oder falscher VMA-Satz", "🔍 VMA-Debug ansehen. Notfalls den Satz am Tag manuell überschreiben."],
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
