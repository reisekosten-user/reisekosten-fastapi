"""
# v2.2-f – Dashboard Alarm, Organisatoren fix, Anonymisierung E-Mails
Modulare Struktur – Einstiegspunkt
"""
# v2.2-a – Modularisierung komplett
from __future__ import annotations
import os, re, json, io, base64
import httpx
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

# ── Module importieren ────────────────────────────────────────────────────────
from mod_db import get_db, is_postgres, ph, fmt_date, next_reise_code, get_schema, get_migrations, repair_legacy_columns, migriere_verknuepfungen_zu_gruppen
from mod_vma import (VMA_SAETZE, IATA_TO_LAND, LAENDER_LISTE, vma_fuer_land,
                      importiere_aktuelle_saetze, vma_fuer_land_erweitert, STADT_ZU_LAND)
from mod_anon import anonymisieren
from mod_kk import (abrechnung_parsen, abrechnung_zuordnen, mitarbeiter_finden, auswahl_wert,
                    zuordnungen_laden as kk_zuordnungen_laden,
                    belege_laden as kk_belege_laden, treffer_speichern as kk_treffer_speichern)
import mod_bewirtung as bw
import mod_vorschuss as vs
import mod_kalender as kal
import mod_doku
import mod_beleg
from mod_beleg import (beleg_verarbeiten, gpt_analyse, gpt_analyse_bild,
                        lade_ma_daten, get_s3, s3_upload, s3_download,
                        bild_zu_pdf, text_zu_pdf, pdf_text_lesen,
                        beleg_neu_anonymisieren, beleg_neu_analysieren,
                        pruefkopf_pdf_erzeugen, beleg_mit_pruefkopf,
                        OPENAI_KEY, OPENAI_MODEL, OPENAI_URL,
                        S3_ENDPOINT, S3_BUCKET,
                        _gleicher_reisender, _namens_tokens, _SEG_FELDER, _wert_gleich)
import mod_belegdeckung as deckung_mod
from mod_mail import fetch_mails, sende_dms_mail, dms_konfiguriert, DMS_EMAIL_TO, smtp_diagnose
from mod_vma_tage import (vma_berechnen, land_fuer_tag, trennungspauschale_berechnen,
                           fruehstueck_aus_beleg, vma_tage_generieren, land_fuer_letzten_tag,
                           vma_auto_aktualisieren, vma_auto_fuer_beleg)
from mod_auth import (passwort_hashen, passwort_pruefen, login_pruefen,
                       hat_bereits_passwoerter, pfad_ist_offen, ist_organisator)
from mod_portal import (zugang_holen_oder_erstellen, portal_link, zugang_aus_token,
                         tage_sicherstellen, tage_laden, tag_speichern,
                         reisende_der_reise, zugaenge_der_reise, portal_mail_senden,
                         cron_portal_mails, PORTAL_TAGE_VORHER)
from mod_flugalert import (konfiguration_laden,
                            cron_flug_alerts, offene_alerts_fuer_dashboard)
from mod_geo import koordinaten_fuer_land, adresse_geokodieren
CRON_SECRET = os.getenv("CRON_SECRET", "")
# Größte erlaubte Habel-Mail (Summe aller PDF-Anhänge in MB; Base64 macht die Mail ca. ein Drittel größer)
HABEL_MAX_MB = float(os.getenv("HABEL_MAX_MB", "15") or 15)
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# ── Konfiguration ─────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")
IMAP_HOST    = os.getenv("IMAP_HOST", "")
IMAP_USER    = os.getenv("IMAP_USER", "")
IMAP_PASS    = os.getenv("IMAP_PASS", "")
def _session_secret_bestimmen() -> tuple:
    """Schlüssel zum Signieren des Login-Cookies. Gibt (Schlüssel, Herkunft) zurück.
    1. Eingetragenes SESSION_SECRET (empfohlen).
    2. Fehlt es: stabiler Ersatz, abgeleitet aus anderen geheimen Einstellungen (Datenbank-Zugang usw.).
       Er ist nicht öffentlich bekannt und bleibt bei jedem Neustart gleich.
    3. Auch das nicht vorhanden (nur lokaler Test): zufälliger Schlüssel je Start.
    Einen festen, im Quelltext stehenden Standardwert gibt es bewusst nicht mehr."""
    import hashlib, secrets
    wert = (os.getenv("SESSION_SECRET", "") or "").strip()
    if wert:
        return wert, "eingetragen"
    basis = "|".join((os.getenv(n, "") or "").strip()
                     for n in ("DATABASE_URL", "CRON_SECRET", "S3_SECRET_KEY", "IMAP_PASS", "OPENAI_API_KEY"))
    if basis.strip("|"):
        return hashlib.sha256(("reisekosten-session|" + basis).encode("utf-8")).hexdigest(), "abgeleitet"
    return secrets.token_hex(32), "zufällig"

SESSION_SECRET, SESSION_SECRET_HERKUNFT = _session_secret_bestimmen()

# Versionsverlauf (neueste zuerst). Bei jedem Release OBEN einen Eintrag ergänzen:
# "zeit" = Datum und Uhrzeit der Auslieferung (Ortszeit), "aenderungen" = 2-3 kurze Zeilen
# zur Vorgängerversion. APP_VERSION wird automatisch aus dem obersten Eintrag gelesen.
VERSIONEN = [
    {"version": "3.35", "zeit": "2026-10-19 16:56", "aenderungen": [
        "Neu: Die VMA-Tage werden jetzt automatisch angelegt, sobald eine Reise erstellt wird, und danach bei jeder Änderung aktualisiert (Reisedaten, Länder, neue, geänderte oder gelöschte Hotel-/Flugbelege, Beleg einer Reise zugeordnet). „VMA berechnen“ ist vor dem Eintragen von Terminen nicht mehr nötig. Wird der Reisezeitraum verkürzt, entfallen die überzähligen Tage (manuell geänderte Tage bleiben).",
        "Neu: Ändert sich Land oder Satz eines Tages gegenüber vorher, zeigt der Tag den Hinweis „Satz geändert: vorher … → jetzt …“ mit Knopf OK. Kann für einen Tag kein Land erkannt werden, steht dort „Land nicht erkannt – es gilt Deutschland“. Die Spalte satz_hinweis wird beim Start automatisch angelegt."]},
    {"version": "3.34", "zeit": "2026-10-19 16:46", "aenderungen": [
        "Neu: Am Bewirtungsbeleg gibt es das Feld „Davon Bewirtung“ (Teilbetrag). Enthält eine Rechnung mehr als die Bewirtung (z. B. Hotelrechnung 520 EUR mit 70 EUR Abendessen für Gäste), trägt der Reisende nur den Bewirtungsanteil ein. Leer = ganze Rechnung ist Bewirtung (wie bisher).",
        "Abschluss, Abschluss-PDF und Reise-Seite weisen Hotel und Bewirtung getrennt aus (z. B. Hotel 450 EUR, Bewirtung 70 EUR); die Gesamtsumme bleibt unverändert. Bei Fremdwährung wird der Anteil im gleichen Verhältnis umgerechnet. Die Spalte wird beim Start automatisch angelegt."]},
    {"version": "3.33", "zeit": "2026-10-19 16:38", "aenderungen": [
        "Neu: Liste „Kreditkarten-Belege Ausland“ (Einstellungen › 🌍) für den Abgleich mit der VISA-Abrechnung. Sie enthält jede mit Kreditkarte bezahlte Rechnung und Quittung aus dem Ausland, in Fremdwährung oder in EUR mit Land außerhalb Deutschlands. Bis zum Abgleich bleibt der Betrag in Originalwährung führend, der Euro-Wert daneben ist geschätzt (≈).",
        "Nachtragen direkt in der Liste: Datum der Kartenabrechnung, Euro-Betrag und Gebühr eintragen oder bei EUR-Belegen „abgeglichen“ ankreuzen. Filter nach Status und Reise, CSV-Export für Excel. Unter „Offene Aufgaben“ steht die Zahl der noch abzugleichenden Belege."]},
    {"version": "3.32", "zeit": "2026-10-19 16:31", "aenderungen": [
        "Neu: Rechnungen und Quittungen lassen sich einzeln an Habel senden, und zwar nur von Hand. Auf der Reise-Seite listet die neue Karte „Belege einzeln an Habel senden“ alle Rechnungen und Quittungen der Reise; der Knopf „Einzeln senden“ schickt eine Mail mit genau diesem Beleg (mit Prüf-Deckblatt). Voraussetzung: Beleg geprüft und Original gespeichert. Ein schon gesendeter Beleg lässt sich nur nach ausdrücklicher Bestätigung erneut senden.",
        "Der Einzelversand wird in einer eigenen Spalte vermerkt (belege.habel_einzel_am, wird beim Start automatisch angelegt) und ändert den Stand „an Habel gesendet“ der Reise nicht. Die Übergabe der ganzen Reise bleibt unverändert und enthält alle Belege."]},
    {"version": "3.31", "zeit": "2026-10-19 16:26", "aenderungen": [
        "Neu: Belege mit der Bezahlart „Durch Kunde“ zählen nie in der Summe (Abschluss, PDF, Kosten-Kachel, Tagessumme) und brauchen keine Rechnung oder Quittung. Eine solche Buchungsbestätigung löst keine Warnung „Jede Buchung braucht eine Rechnung“ mehr aus, und die Hinweise „Betrag final offen“ und „Kurs fehlt“ entfallen.",
        "Vermerk: Im Abschluss und im PDF stehen diese Belege in einer eigenen Zeile „Durch Kunde direkt bezahlt – zählt nicht in der Summe“, im Tagesverlauf mit dem Hinweis „durch Kunde direkt bezahlt“ und ohne Euro-Umrechnung."]},
    {"version": "3.30.1", "zeit": "2026-10-19 16:21", "aenderungen": [
        "Ursache der falschen MwSt. bei Fly Away (Beleg #56) gefunden: Bei Scans und Fotos las die KI nur die erste Seite. Die Rechnung weist die MwSt. aber auf Seite 2 aus („Enthaltene Mehrwertsteuer“: 12,35 EUR, nur 19 % auf die Ticketvermittlung; der Flug hat 0 %). Jetzt liest die KI alle Seiten eines Scans (höchstens 4), beim Hochladen, bei mehrseitigen TIF-Scans und bei „Beleg neu auslesen“.",
        "Ein gescanntes PDF ohne Text (Scanner-PDF) wird beim Hochladen jetzt per Bilderkennung gelesen. Bisher meldete die Analyse „Kein Text vorhanden“. Bei mehreren Steuersätzen auf einem Beleg gilt: MwSt. = Summe der ausgewiesenen Steuerbeträge, Netto = Brutto minus MwSt. Beleg #56: bitte einmal „Beleg neu auslesen“."]},
    {"version": "3.30", "zeit": "2026-10-19 16:15", "aenderungen": [
        "Fehler behoben: Die MwSt. bei Flug- und Bahn-Belegen war falsch (Fall Fly Away, Beleg #56: Die KI rechnete 19 % auf den ganzen Rechnungsbetrag hoch, obwohl die MwSt. bei einem Reisebüro nur für die Servicepauschale gilt, nicht für den Flugpreis). Neue Regel für die KI: MwSt. und Netto nur übernehmen, wenn sie auf dem Beleg stehen. Bei Flug, Bahn-Fernverkehr, Fernbus, Schiff, Reisebüro-Rechnungen und Visa-Gebühren wird nie selbst gerechnet.",
        "Zusätzlich prüft die App selbst: Bei Flug und Bahn werden Netto und MwSt. geleert, wenn die KI sie berechnet hat oder der MwSt.-Betrag nicht im Belegtext steht. Ältere Flug-/Bahn-Belege mit hochgerechneter MwSt. werden beim Start einmalig bereinigt (Foto-Belege bleiben unberührt). „Beleg neu auslesen“ holt eine aufgedruckte MwSt. wieder."]},
    {"version": "3.29.3", "zeit": "2026-10-19 16:09", "aenderungen": [
        "Fehler behoben: Die „Summe Belege“ je Tag im Tagesverlauf addierte Beträge in Fremdwährung als Euro (Fall Reise 26-007: 81012.00 INR + 746.30 EUR + 115.90 EUR ergab 81874.20 EUR). Jetzt rechnet die Summe in Euro nach denselben Regeln wie Abschluss und Kosten-Kachel: Euro-Betrag laut Abrechnung, sonst geschätzter Euro-Wert, Anteil an der Buchung, Gebühren. Buchungen, die durch Rechnungen belegt sind, und Zahlungsnachweise zählen nicht doppelt.",
        "Anzeige: Der Fremdwährungsbetrag bleibt der führende Betrag, der Euro-Wert steht daneben („81012.00 INR (≈ 972.14 EUR)“; ≈ = geschätzt, = laut Abrechnung). Fehlt der Kurs, steht „Kurs fehlt“ und die Summenzeile nennt, wie viele Belege fehlen. Ein Hinweis erklärt, warum ein Betrag nicht oder nur als Rest zählt."]},
    {"version": "3.29.2", "zeit": "2026-10-19 15:36", "aenderungen": [
        "Sicherheit: Die Anmeldung wurde bisher mit einem festen, im Programm stehenden Schlüssel gesichert, wenn SESSION_SECRET nicht eingetragen war (Meldung „Fehlende Pflicht-Einstellungen: SESSION_SECRET“ in der Dokumentation). Wer diesen Standardwert kennt, könnte ein gültiges Login-Cookie bauen. Jetzt leitet die App ohne SESSION_SECRET einen nicht öffentlichen Ersatzschlüssel aus anderen geheimen Einstellungen ab; einen festen Standardwert gibt es nicht mehr.",
        "Einmalig müssen sich alle Benutzer neu anmelden (der Schlüssel ändert sich). Die Dokumentation erklärt, wie SESSION_SECRET in Render eingetragen wird; auch dann ist einmal neues Anmelden nötig."]},
    {"version": "3.29.1", "zeit": "2026-10-19 13:53", "aenderungen": [
        "Fehler behoben: Bei manchen Belegen (Fall Uber-Stornoquittung #216) erschien keine Prüfung. Die KI hatte die Belegart „Sonstiges“ gesetzt, und diese Art ist nicht prüfpflichtig. Jetzt wird ein Beleg mit Betrag, bei dem die KI „Sonstiges“ oder nichts liefert, als Quittung eingestuft; die KI-Anweisung dazu ist präzisiert (Zahlungsbelege, auch Stornogebühren, sind Quittungen).",
        "Bei bereits vorhandenen Belegen mit nicht prüfpflichtiger Belegart zeigt die Beleg-Seite jetzt einen Hinweis mit dem Knopf „Als Quittung einstufen“, statt die Prüfung stillschweigend auszublenden."]},
    {"version": "3.29", "zeit": "2026-10-19 13:41", "aenderungen": [
        "Bildformat TIF/TIFF wird erkannt: beim Hochladen (Beleg-Seite, Portal, Bewirtung) und als Mail-Anhang, auch wenn Browser oder Mail nur „octet-stream“ melden oder die Endung fehlt (Erkennung am Inhalt). Der Scan wird wie ein Foto zu PDF gewandelt und per Bilderkennung gelesen; mehrseitige TIF-Scans ergeben ein PDF mit allen Seiten (höchstens 20), die KI liest die erste Seite.",
        "Bei Bildern mit Transparenz (PNG) ist der Hintergrund im PDF jetzt weiß statt schwarz."]},
    {"version": "3.28", "zeit": "2026-10-16 19:40", "aenderungen": [
        "Zahlungsnachweis: Ein Beleg, der nur die Zahlung belegt (Fall PayU über 194.50 EUR zur IndiGo-Buchung über 19725 INR und ihren Teilrechnungen), wurde bisher zusätzlich zu den Rechnungen gezählt (424.50 statt 194.50 EUR, „Rechnungen liegen über der Buchung“). Jetzt erkennt die App ihn selbst: Eine Quittung oder ein Kartenbeleg neben echten Rechnungen derselben Gruppe zählt nicht extra, wenn er in anderer Währung lautet und die Rechnungen die Buchung schon voll decken, oder wenn sein Betrag genau der Buchung bzw. der Rechnungssumme entspricht. Andere Beträge (z. B. eine zusätzliche Restaurant-Quittung) zählen wie bisher.",
        "Der Zahlungsnachweis liefert den echten Euro-Betrag: Hat die Buchung keinen bestätigten Euro-Betrag, verteilt die App die 194.50 EUR anteilig auf die Rechnungen (94.72 + 99.78). Beleg-Seite, Abschluss und PDF benennen den Zahlungsnachweis („zählt nicht extra“); er geht weiterhin als eigenes PDF an Habel."]},
    {"version": "3.27", "zeit": "2026-10-16 18:54", "aenderungen": [
        "Buchung mit Teilrechnungen, von denen eine keinen erkannten Betrag hat (Fall IndiGo: Buchung 19725 INR, Rechnung 9606 INR, Rechnung ohne Betrag): Der Beleg ohne Betrag wird jetzt namentlich genannt (Beleg-Seite, Abschluss, PDF, Hinweise). Auf der Beleg-Seite steht direkt ein Feld „Betrag speichern“ mit dem Rest der Buchung als Vorschlag (10119.00 INR) und ein Knopf „Beleg neu auslesen“. Bei Fremdwährung wird der Kurs vom Schwester-Beleg übernommen.",
        "Abrechnung: Der echte Euro-Betrag und das Auslandsentgelt aus der Kreditkartenabrechnung liegen meist an der Buchung, nicht an den Teilrechnungen. Bisher gingen sie verloren, sobald die Rechnungen die Buchung belegten. Jetzt bekommt jede Rechnung ohne eigenen bestätigten Euro-Betrag ihren Anteil (Rechnungsbetrag / Buchungsbetrag), auch der noch fehlende Rest. Abschluss, PDF und Kosten-Kachel summieren damit genau den abgerechneten Betrag; in der Tabelle steht „Anteil der Buchung“."]},
    {"version": "3.26.1", "zeit": "2026-10-16 08:50", "aenderungen": [
        "Fehler behoben: Die Seite konnte komplett einfrieren („Antwort hat zu lange gedauert“), bis der Dienst neu gestartet wurde. Ursache: Der Mail-Abruf (Knopf und Cron alle 5 Minuten) wartete ohne Zeitlimit auf den Mailserver und hielt dabei die ganze App an.",
        "Der Abruf läuft jetzt im Hintergrund (eigener Thread), die Seite bleibt währenddessen bedienbar. Am Mailserver gilt ein Zeitlimit von 30 Sekunden je Schritt; danach meldet der Abruf einen Fehler (HTTP 500 beim Cron), statt zu hängen."]},
    {"version": "3.26", "zeit": "2026-10-15 20:02", "aenderungen": [
        "Buchung und Rechnungen: Eine Buchungsbestätigung (Betrag x) mit mehreren Rechnungen/Quittungen (x1 + x2 = x) wird jetzt sauber verknüpft, gerechnet und angezeigt. Die Beleg-Seite zeigt „Buchung 300.00 EUR · Rechnungen 120.00 + 180.00 = 300.00 EUR · vollständig belegt“, macht Vorschläge, wenn die Summe genau passt (ein Klick: „Alle verknüpfen“), und verknüpft mehrere Belege auf einmal per Ankreuzen. Beim Eingang werden Belege mit gleichem Buchungscode in derselben Reise automatisch verknüpft.",
        "Abschluss, PDF und Kosten-Kachel: Eine vollständig belegte Buchung zählt nicht doppelt. Decken die Rechnungen nur einen Teil, steht der offene Rest als Zeile „Rest der Buchung“ in der Summe (vorläufig), ohne Rechnung zählt die ganze Buchung vorläufig. Warnkasten und neue Karte „Buchungen und zugehörige Rechnungen“ zeigen, was fehlt; auch in „Offene Aufgaben“ (nach Rückkehr) und als Hinweis bei der Habel-Übergabe.",
        "Fehler behoben: Eine Rechnung mit demselben Buchungscode wie die Buchungsbestätigung (oder gleichem Betrag, Anbieter und Datum) wurde bisher als Duplikat verworfen oder als „Änderung“ behandelt. Buchung und Rechnung sind jetzt getrennte Belege, ebenso zwei Rechnungen mit verschiedener Rechnungsnummer."]},
    {"version": "3.25", "zeit": "2026-10-15 15:50", "aenderungen": [
        "Neu: Die Adresse /cron/mails holt neue Belege aus dem Postfach (gedacht für einen Cron-Aufruf alle 5 Minuten, z. B. per crontab auf dem Hetzner-Server). Gleiche Verarbeitung wie der Knopf „Mails abrufen“. Es läuft immer nur ein Abruf; bei einem Fehler antwortet die Adresse mit HTTP 500, damit der Cron-Dienst es meldet.",
        "Habel-Info neu gestaltet: Am Beleg steht jetzt der Stand der ganzen Reise (Fortschrittsbalken „n von m Belegen geprüft“, noch offene Belege als Links, Knopf zur Reise-Übergabe). Dashboard und „Offene Aufgaben“ melden Reisen, die zur Übergabe bereit sind, statt einzelner ungesendeter Belege. Die Liste der abgeschlossenen Reisen zeigt den Habel-Stand je Reise."]},
    {"version": "3.24.4", "zeit": "2026-10-15 15:30", "aenderungen": [
        "Mail-Abruf: Ein weitergeleitetes PDF wurde nicht als Anhang erkannt (Fall IndiGo-Rechnung), stattdessen wurde nur der Mailtext zum Beleg. Ursache: Kleine Anhänge, die die Mail als „inline“ markiert (typisch bei Weiterleitung vom Handy), wurden wie Signatur-Logos verworfen. Diese Regel gilt jetzt nur noch für Bilder, nicht mehr für PDF und XML.",
        "Anhänge werden jetzt am Inhalt erkannt (PDF, JPG, PNG, WEBP, HEIC), auch mit falschem Mail-Typ, fehlender Endung oder fehlendem Namen. WEBP- und HEIF-Bilder werden korrekt als Bild verarbeitet.",
        "Neu: Wird nur der Mailtext zum Beleg, obwohl die Mail weitere Teile hat oder im Text einen Anhang nennt, zeigt „Mails abrufen“ die Teile der Mail (Typ, Name, Größe) an."]},
    {"version": "3.24.3", "zeit": "2026-10-15 14:50", "aenderungen": [
        "Abschluss (Seite und PDF): neue Summe der geflogenen Flugsegmente, mit Liste (Datum, Reisender, Flug, Strecke) und Aufteilung je Reisendem. Zugfahrten zählen nicht – auch nicht mit LH-Nummer (Lufthansa Express Rail / Rail&Fly, erkannt an Bahnhof im Ortsnamen, Bahnhofs-Code wie QKL/ZWS oder Zug in der Gesellschaft). Stornierte Flüge zählen nicht.",
        "Der automatische Portal-Link (Cron) geht nur noch an Reisen, die ab dem 16.10.2026 angelegt wurden. Bestandsreisen bekommen keine automatische Mail. Von Hand senden geht weiter."]},
    {"version": "3.24.2", "zeit": "2026-10-15 14:40", "aenderungen": [
        "Portal-Link beginnt jetzt immer mit https:// (aus PORTAL_BASE_URL, sonst aus der von Render gesetzten Adresse, sonst aus der Adresse der aufgerufenen Seite). Vorher stand nur „/portal/…“ in Mail und Reise-Seite. Auf der Reise-Seite ist der Link anklickbar.",
        "Alle Mails tragen jetzt Datum und eine eindeutige Message-ID. Ein erneut gesendeter Portal-Link kam bisher nicht an, vermutlich weil der Empfänger-Server ihn als Doppeltes verworfen hat. „Erneut senden“ setzt zusätzlich „Erneut:“ vor den Betreff."]},
    {"version": "3.24.1", "zeit": "2026-10-15 14:35", "aenderungen": [
        "Handbuch korrigiert: Anmelden können sich nur Organisatoren. Reisende bekommen kein Passwort, sondern nur ihren Portal-Link. Betrifft die Kapitel „Anmeldung, Rollen und Zugriff“, „Mitarbeiter verwalten“ und das Seitenverzeichnis.",
        "Sonst keine Änderung am Programm."]},
    {"version": "3.24", "zeit": "2026-10-15 14:20", "aenderungen": [
        "Neu: Handbuch unter Einstellungen › „📖 Dokumentation“ – als Seite und als PDF. Teil A für Reisende (im Portal über „📖 Anleitung“ und als eigene PDF zum Weitergeben), Teil B für Organisatoren, Teil C Einstellungen und Betrieb mit dem aktuellen Stand aller Einstellungen, Cron-Aufgaben, Fehlersuche, Änderungsverlauf und Seitenverzeichnis.",
        "Das Handbuch wird bei jedem Release mitgepflegt. Automatische Tests schlagen fehl, wenn eine neue Einstellung oder Seite nicht darin steht.",
        "Die Testversand-Seite (Knopf „🧪 Test“) ist entfernt. Die Mail-Diagnose hat jetzt eine eigene Kachel in den Einstellungen."]},
    {"version": "3.23.3", "zeit": "2026-10-15 13:25", "aenderungen": [
        "Mailversand: Alle Einladungen und Portal-Mails eines Laufs gehen jetzt über eine einzige Verbindung (eine Anmeldung statt einer je Mail). Vorher bremste UDAG nach den ersten Mails mit „timed out“.",
        "Bricht die Verbindung ab, wird die Mail einmal mit einer neuen Verbindung wiederholt. Verbindungsaufbau wartet höchstens 15 Sekunden, das Senden selbst weiter SMTP_TIMEOUT (Standard 30)."]},
    {"version": "3.23.2", "zeit": "2026-10-15 13:05", "aenderungen": [
        "Neu: Seite „Mail-Diagnose“ (Knopf auf der Testversand-Seite). Sie prüft Schritt für Schritt Servername, Erreichbarkeit der Postausgangs-Ports, Anmeldename und Anmeldung und nennt am Ende die wahrscheinliche Ursache. Es wird keine Mail gesendet.",
        "Ist der Mailserver nicht erreichbar, bricht der Versand nach dem ersten Fehler ab (statt je Termin 30 Sekunden zu warten). Die SMTP-Verbindung wird nach jedem Versand sauber geschlossen.",
        "Optional: SMTP_TIMEOUT (Sekunden, Standard 30)."]},
    {"version": "3.23.1", "zeit": "2026-10-15 14:35", "aenderungen": [
        "Mailversand: Fehlt eine Einstellung, nennt die Meldung jetzt genau, welche (SMTP_HOST, SMTP_USER/IMAP_USER, SMTP_PASS/IMAP_PASS, bei Habel auch DMS_EMAIL_TO). Vorher stand nur „SMTP nicht konfiguriert“.",
        "Gilt für Outlook-Einladungen, Portal-Mails, Testversand und die Habel-Übergabe."]},
    {"version": "3.23", "zeit": "2026-10-14 16:20", "aenderungen": [
        "Neu: Seite „Testversand“ je Reise (Knopf „🧪 Test“ auf der Reise-Seite). Sie nutzt die vorhandenen Reisen und Belege aus dem IMAP-Bestand und schickt alles nur an eine Testadresse (vorausgefüllt mit Ihrer eigenen).",
        "Outlook-Test: alle Flüge, Züge und Termine der Reise als Test-Einladung – unabhängig vom Haken im Profil. Portal-Test: die Portal-Mail jedes Reisenden mit dessen echtem Link.",
        "Der echte Versandstatus bleibt unberührt: nichts wird als „gesendet“ vermerkt, der automatische Versand läuft später normal."]},
    {"version": "3.22.1", "zeit": "2026-10-14 16:40", "aenderungen": [
        "Habel-Übergabe: Neuer Knopf „Vorschau: Dateien der Mail“. Die Seite zeigt Empfänger, Betreff und Text und listet alle Dateien der Mail zum Anklicken (mit Seiten und Größe) – ohne etwas zu senden. Funktioniert auch, solange noch nicht alle Belege geprüft sind.",
        "Prüf-Deckblatt: „&“ in Anbieternamen wird richtig dargestellt (stand zuvor als „&amp;“)."]},
    {"version": "3.22", "zeit": "2026-10-14 16:10", "aenderungen": [
        "Habel-Übergabe neu: Eine E-Mail je Reise mit der Reisekostenabrechnung und jedem Beleg als eigenem PDF (mit Prüf-Deckblatt). Der Knopf „An Habel“ sitzt jetzt auf der Reise-Seite neben „Abschluss“.",
        "Vor dem Senden zeigt die Seite, welche Belege geprüft sind. Solange ein Beleg nicht geprüft ist oder das Original fehlt, ist das Senden gesperrt. Erneutes Senden braucht einen Haken.",
        "Der Einzel-Knopf „Jetzt an Habel übertragen“ am Beleg entfällt; der Beleg zeigt nur noch den Status und führt zur Reise-Übergabe. Grenze für die Mailgröße: HABEL_MAX_MB (Standard 15)."]},
    {"version": "3.21.3", "zeit": "2026-10-14 15:30", "aenderungen": [
        "Dashboard und Abrechnung: „Auslandsbeleg ohne Wechselkurs – Kurs nachtragen“ erscheint nur noch, wenn wirklich ein Kurs nachzutragen ist.",
        "Nicht mehr gemeldet werden: Bewirtungs- und Vorschussbelege (haben keinen eigenen Kurs) sowie Belege, deren Euro-Betrag schon bestätigt ist."]},
    {"version": "3.21.2", "zeit": "2026-10-14 15:10", "aenderungen": [
        "Bewirtungsbeleg: Er braucht immer einen Originalbeleg (auch einen selbst erstellten Eigenbeleg). Die Option „kein Originalbeleg“ und das Feld Trinkgeld entfallen; Betrag, Trinkgeld und Wechselkurs stehen nur am Originalbeleg. Der Bewirtungsbeleg hat keinen eigenen Betrag und keinen eigenen Kurs mehr.",
        "Wechselkurs der KI-Schätzung war verdreht gespeichert (18,2 MXN je EUR statt 0,0549 EUR je MXN). Neue Analysen speichern ihn richtig, bestehende Belege werden automatisch korrigiert.",
        "Reste aus 3.21.1 (Trinkgeld als Betrag am Bewirtungsbeleg) werden beim Start, über /init und nach jeder Änderung entfernt."]},
    {"version": "3.21.1", "zeit": "2026-10-14 10:20", "aenderungen": [
        "Das Trinkgeld einer Bewirtung zählt jetzt in der Reisekostenabrechnung. Bei Fremdwährung wird der Euro-Wert nach dem Kurs des Rechnungsbelegs gerechnet und folgt automatisch, wenn dieser Kurs später geändert wird.",
        "Selbst erzeugte Belege (Bewirtung, Vorschuss, Bargeld) lassen sich nicht mehr per KI neu analysieren oder anonymisieren – das hatte den Bewirtungsbeleg überschrieben.",
        "Bereits überschriebene Bewirtungs- und Vorschussbelege werden beim Start, über /init und nach jeder Änderung automatisch aus den Buchungsdaten wiederhergestellt."]},
    {"version": "3.21", "zeit": "2026-10-14 10:05", "aenderungen": [
        "Neu: Im Profil gibt es den Haken „Reisetermine als Outlook-Einladung senden“. Dann bekommt der Reisende für jeden Flug, Zug und manuellen Termin eine Einladung (Zeiten, Nummer, Sitz, Terminal/Gleis).",
        "Änderungen und Stornos werden automatisch nachgeführt (aktualisierter bzw. abgesagter Termin). Je Reise: Seite „Outlook-Einladungen“ mit Status und Knopf „Jetzt senden“.",
        "Sitzplatz wird von der KI aus dem Beleg gelesen und lässt sich bei der Umbuchung eintragen. Beim Kürzel-Ändern bleiben jetzt Rollen und Kartendaten erhalten."]},
    {"version": "3.20.1", "zeit": "2026-10-12 09:35", "aenderungen": [
        "Fehler behoben: Bewirtungs- und Vorschussbelege wurden nach „Daten nachtragen“ fälschlich als „Pflichtfelder fehlen“ (Betrag, Eventdatum) markiert.",
        "Bereits falsch markierte Belege werden beim Start bzw. über /init automatisch wieder auf vollständig gesetzt.",
        "Bei diesen selbst erzeugten Belegen entfallen die KI-Knöpfe und das Wechselkurs-Formular."]},
    {"version": "3.20", "zeit": "2026-10-12 08:33", "aenderungen": [
        "Bewirtungsbeleg vereinfacht: nur Beleg wählen, Anlass und Gast eintragen. Tag, Ort, Betrag und Währung kommen aus dem Beleg.",
        "Die Versionsnummer in der oberen Leiste ist jetzt ein Link zu dieser Änderungsliste."]},
    {"version": "3.19.1", "zeit": "2026-10-12 08:20", "aenderungen": [
        "Fehler behoben: Kurs nachtragen bei Belegen ohne Betrag (z. B. Quittungen) brach mit „float() … NoneType“ ab.",
        "Belege ohne Betrag zeigen keinen Hinweis „Kurs fehlt“ mehr."]},
    {"version": "3.19", "zeit": "2026-10-12 08:01", "aenderungen": [
        "Währungsliste erweitert (u. a. MXN) und aus dem gewählten Beleg übernommen.",
        "Beim Bestätigen ist der Name mit dem angemeldeten Mitarbeiter vorausgefüllt.",
        "Neu: Kachel „Versionen“ in den Einstellungen mit diesem Änderungsverlauf."]},
    {"version": "3.18", "zeit": "2026-10-11 01:07", "aenderungen": [
        "Neu: Reisevorschuss / Bargeld-Konto (Auszahlung, Rückgabe, Geldautomat, Kurse von Hand, Bestätigung im Portal).",
        "Bewirtung: Hinweis ab 250 €, Warnung bei mehr als 14 Tagen, Querverweis im Prüf-Deckblatt.",
        "Datensicherung enthält jetzt Bewirtungsbelege und Bargeld-Buchungen."]},
    {"version": "3.17", "zeit": "2026-10-11 00:22", "aenderungen": [
        "Neu: Bewirtungsbeleg im Portal (Eigenbeleg mit elektronischer Bestätigung, PDF, Versionen, Prüfsumme).",
        "Bewirtungsbelege erscheinen in Prüfung, ToDo-Liste und Abschluss."]},
]
APP_VERSION  = VERSIONEN[0]["version"]

# Belegarten, die der Organisator prüft (Prüf-Deckblatt) und die an die DMS-Stelle (Habel) gehen
PRUEF_BELEGARTEN = ("Rechnung", "Quittung", "Bewirtungsbeleg",
                    "Vorschussquittung", "Rueckgabequittung", "Bargeldbeschaffung")
PRUEF_SQL = "(" + ",".join(f"'{a}'" for a in PRUEF_BELEGARTEN) + ")"

# Von der App selbst erzeugte Belege (Eigenbelege): kein Betrag, keine KI-Analyse, keine Rechnungs-Pflichtfelder
EIGENBELEG_ARTEN = ("Bewirtungsbeleg", "Vorschussquittung", "Rueckgabequittung", "Bargeldbeschaffung")
EIGENBELEG_SQL = "(" + ",".join(f"'{a}'" for a in EIGENBELEG_ARTEN) + ")"

# ── CSS + HTML Shell ──────────────────────────────────────────────────────────
# ── CSS + HTML Shell ───────────────────────────────────────────────────────────
CSS = """
:root {
    --bg: #f0f4f8;
    --white: #ffffff;
    --border: #dde3ea;
    --border-strong: #c4cdd8;
    --text: #0d1b2a;
    --muted: #5a6a7a;
    --light: #8fa0b0;
    --blue: #1a56db;
    --blue-d: #1344b8;
    --blue-l: #eff4ff;
    --blue-nav: #0f2d6e;
    --green: #047857;
    --green-l: #ecfdf5;
    --amber: #b45309;
    --amber-l: #fffbeb;
    --red: #c81e1e;
    --red-l: #fef2f2;
    --radius: 10px;
    --radius-s: 6px;
    --shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
    --shadow-md: 0 4px 12px rgba(0,0,0,.08);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
}

/* Navigation */
nav {
    background: linear-gradient(135deg, #0f2d6e 0%, #1a56db 100%);
    padding: 0 28px;
    display: flex;
    align-items: center;
    gap: 0;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 12px rgba(15,45,110,.3);
    height: 60px;
}
.nav-brand {
    display: flex;
    align-items: center;
    margin-right: 32px;
    padding: 4px 0;
    text-decoration: none;
    flex-shrink: 0;
}
.nav-brand img {
    height: 42px;
    width: auto;
    filter: brightness(0) invert(1);
    opacity: 0.95;
}
.nav-link {
    color: rgba(255,255,255,0.7);
    text-decoration: none;
    font-size: 13px;
    font-weight: 500;
    padding: 20px 14px;
    border-bottom: 3px solid transparent;
    transition: color .15s, border-color .15s;
    white-space: nowrap;
    letter-spacing: .01em;
}
.nav-link:hover { color: white; border-bottom-color: rgba(255,255,255,0.4); }
.nav-link.active { color: white; border-bottom-color: #60a5fa; }
.nav-right { margin-left: auto; font-size: 11px; color: rgba(255,255,255,0.4);
             font-family: monospace; letter-spacing:.05em; }

/* Layout */
main { padding: 28px 28px; max-width: 1200px; margin: 0 auto; }
.page-title { font-size: 22px; font-weight: 700; color: var(--text); margin-bottom: 20px;
              letter-spacing: -.02em; }

/* Karten */
.card {
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    margin-bottom: 16px;
}
.card-header {
    padding: 14px 20px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #fafbfc;
    border-radius: var(--radius) var(--radius) 0 0;
}
.card-title { font-size: 14px; font-weight: 600; color: var(--text); }
.card-body { padding: 20px; }

/* Stat-Karten */
.stat-card {
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    padding: 20px;
    text-align: center;
    transition: box-shadow .15s, transform .15s;
}
.stat-card:hover { box-shadow: var(--shadow-md); transform: translateY(-1px); }
.stat-num { font-size: 32px; font-weight: 700; letter-spacing: -.03em; }
.stat-label { font-size: 12px; color: var(--muted); margin-top: 4px; font-weight: 500; }

/* Buttons */
.btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    border-radius: var(--radius-s);
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    border: none;
    transition: all .15s;
    white-space: nowrap;
    letter-spacing: .01em;
}
.btn-primary { background: var(--blue); color: white; }
.btn-primary:hover { background: var(--blue-d); box-shadow: 0 2px 8px rgba(26,86,219,.3); }
.btn-success { background: var(--green); color: white; }
.btn-success:hover { background: #036545; }
.btn-secondary { background: white; color: #374151; border: 1px solid var(--border); }
.btn-secondary:hover { background: #f9fafb; border-color: var(--border-strong); }
.btn-danger { background: var(--red); color: white; }
.btn-danger:hover { background: #a31717; }
.btn-sm { padding: 5px 10px; font-size: 12px; }

/* Formulare */
.form-grid { display: grid; gap: 16px; }
.form-grid-2 { grid-template-columns: 1fr 1fr; }
.form-grid-3 { grid-template-columns: 1fr 1fr 1fr; }
.form-group { display: flex; flex-direction: column; gap: 4px; }
.form-group.full { grid-column: 1 / -1; }
label { font-size: 12px; font-weight: 600; color: #374151; letter-spacing: .01em; }
.required { color: var(--red); margin-left: 2px; }
input[type="text"], input[type="date"], input[type="email"],
input[type="number"], select, textarea {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: var(--radius-s);
    font-size: 13px;
    background: white;
    color: var(--text);
    transition: border-color .15s, box-shadow .15s;
}
input:focus, select:focus, textarea:focus {
    outline: none;
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(26,86,219,.1);
}
.form-hint { font-size: 11px; color: var(--muted); margin-top: 2px; }
.form-actions {
    display: flex;
    gap: 8px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
    margin-top: 20px;
}

/* Tabellen */
.table-wrap { overflow-x: auto; border-radius: 0 0 var(--radius) var(--radius); }
table { width: 100%; border-collapse: collapse; }
th {
    text-align: left;
    padding: 9px 14px;
    font-size: 11px;
    font-weight: 700;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .06em;
    border-bottom: 1px solid var(--border);
    background: #fafbfc;
    white-space: nowrap;
}
td {
    padding: 11px 14px;
    font-size: 13px;
    border-bottom: 1px solid #f3f4f6;
    vertical-align: middle;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafbfe; }
.td-mono { font-family: "SF Mono", "Fira Code", monospace; font-size: 12px; }

/* Badges */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: .02em;
}
.badge-blue { background: #dbeafe; color: #1e40af; }
.badge-green { background: #d1fae5; color: #065f46; }
.badge-amber { background: #fef3c7; color: #92400e; }
.badge-red { background: var(--red-l); color: #991b1b; }
.badge-gray { background: #f3f4f6; color: #6b7280; }
.badge-purple { background: #ede9fe; color: #5b21b6; }

/* Alerts */
.alert {
    padding: 12px 16px;
    border-radius: var(--radius-s);
    font-size: 13px;
    margin-bottom: 16px;
    border-left: 4px solid;
}
.alert-ok { background: var(--green-l); border-color: #34d399; color: #065f46; }
.alert-warn { background: var(--amber-l); border-color: #fbbf24; color: #92400e; }
.alert-err { background: var(--red-l); border-color: #f87171; color: #991b1b; }
.alert-info { background: var(--blue-l); border-color: #93c5fd; color: #1e40af; }

/* Leerer Zustand */
.empty-state { text-align: center; padding: 40px 20px; color: var(--light); }
.empty-state p { margin-top: 8px; font-size: 13px; }

/* Reise-Sektion Header */
.sektion-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 24px 0 12px;
    padding-bottom: 8px;
    border-bottom: 2px solid var(--border);
}
.sektion-titel {
    font-size: 16px;
    font-weight: 700;
    color: var(--text);
}
.sektion-count {
    background: var(--blue-l);
    color: var(--blue);
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 700;
}

@media (max-width: 640px) {
    .form-grid-2, .form-grid-3 { grid-template-columns: 1fr; }
    main { padding: 16px; }
    nav { padding: 0 16px; }
}
"""

def shell(title: str, content: str, page: str = "") -> str:
    def nav(p, label, url):
        cls = "nav-link active" if page == p else "nav-link"
        return f'<a href="{url}" class="{cls}">{label}</a>'
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} – Herrhammer Reisekosten</title>
<style>{CSS}</style>
</head>
<body>
<nav>
  <a href="/" class="nav-brand" style="display:flex;align-items:center;gap:10px;padding:6px 0">
    <img src="/static/logo3.png" alt="Herrhammer" style="height:32px;width:auto">
  </a>
  {nav("start", "Dashboard", "/")}
  {nav("mitarbeiter", "Mitarbeiter", "/mitarbeiter")}
  {nav("reisen", "Reisen", "/reisen")}
  {nav("belege", "Belege", "/belege")}
  {nav("mails", "📬 Mails", "/mails-abrufen")}
  {nav("vma", "VMA-Sätze", "/vma")}
  {nav("einstellungen", "⚙", "/einstellungen")}
  <div class="nav-right"><a href="/einstellungen/versionen" title="Änderungsverlauf" style="color:inherit">v{APP_VERSION}</a> &nbsp;·&nbsp; <a href="/logout" style="color:inherit">🚪 Logout</a></div>
</nav>
<main>
{content}
</main>
</body>
</html>"""

def segmente_aus_ki_json(ki_json_str: str | None) -> list:
    """
    Einzige Stelle im Code, die "segmente" aus dem KI-JSON eines Flug-/Bahn-
    Belegs ausliest. WIRD SOWOHL vom Tagesverlauf (Reise-Detailseite) ALS AUCH
    vom Reiseplan genutzt – vorher gab es zwei fast identische, aber getrennte
    Implementierungen, die dadurch auseinanderlaufen konnten (ein Beleg zeigte
    im Tagesverlauf nur eine zusammengefasste Zeile, im Reiseplan aber korrekt
    alle Einzelsegmente). Jede künftige Stelle, die Segmente braucht, soll
    diese Funktion aufrufen statt eigenes json.loads(...).get("segmente").
    """
    if not ki_json_str:
        return []
    try:
        return json.loads(ki_json_str).get("segmente") or []
    except Exception:
        return []


# ── Geflogene Flugsegmente (Summe im Abschluss) ───────────────────────────────
# Rail&Fly / Lufthansa Express Rail: Die Bahnstrecke steht im Beleg als "Flug"
# mit LH-Nummer (Zug mit Flugnummer). Sie ist KEIN Flug. Ein Segment gilt als
# Zug, sobald EINE dieser Bedingungen zutrifft:
#   1. Belegart ist "Bahn"
#   2. Gesellschaft nennt Bahn/Rail/Zug/ICE/TGV usw.  (nur transport_name, nicht Freitext)
#   3. Zugnummer beginnt mit ICE/TGV/RJ(X)
#   4. Von-/Nach-Ort ist ein Bahnhof (Hbf, Bahnhof, Bf, Gare, Centraal, Station)
#   5. Von-/Nach-IATA ist ein Bahnhofs-Code (QKL Köln Hbf, ZWS Stuttgart Hbf usw.)
_ZUG_NAME = re.compile(r"\b(bahn|train|zug|rail|sncf|ice|tgv|railjet|intercity|eurostar|thalys|flixtrain)\b", re.I)
_ZUG_NUMMER = re.compile(r"^(ICE|TGV|RJX?)\s*\d", re.I)
_BAHNHOF_ORT = re.compile(r"(hbf|bahnhof|\bbf\b|\bgare\b|centraal|railway|\bstation\b)", re.I)
_BAHNHOF_IATA = {"QKL", "QDU", "ZVR", "ZMB", "XIT", "ZMU", "ZAQ", "ZWS", "XIR", "QPP", "ZQU", "ZBA"}


def segment_ist_zug(s: dict, transportart: str = "Flug") -> bool:
    """True, wenn das Segment eine Zugfahrt ist (auch mit LH-Nummer)."""
    if (transportart or "") == "Bahn":
        return True
    if _ZUG_NAME.search(str(s.get("transport_name") or "")):
        return True
    if _ZUG_NUMMER.search(str(s.get("transport_nummer") or "").strip()):
        return True
    if _BAHNHOF_ORT.search(f'{s.get("von_ort") or ""} {s.get("nach_ort") or ""}'):
        return True
    for k in ("von_iata", "nach_iata"):
        if str(s.get(k) or "").strip().upper() in _BAHNHOF_IATA:
            return True
    return False


def _flug_ort_text(s: dict, p: str) -> str:
    ort = str(s.get(f"{p}_ort") or "").strip()
    iata = str(s.get(f"{p}_iata") or "").strip().upper()
    if ort and iata:
        return f"{ort} ({iata})"
    return ort or iata or "?"


def flugsegmente_zaehlen(reise_code: str) -> dict:
    """
    Zählt die tatsächlich GEFLOGENEN Flugsegmente einer Reise (für den Abschluss).
    Nicht gezählt: Zugfahrten (auch mit LH-Nummer, siehe segment_ist_zug),
    stornierte Flüge (Status "cancel" aus der Flugüberwachung), Änderungs-Belege
    (deren Daten stehen nach dem Übernehmen im Original-Beleg).
    Dubletten aus mehreren Belegen werden NICHT automatisch entfernt, sondern über
    die Überschneidungs-Prüfung der Reise geklärt (ueberschneidungen = Anzahl offener Fälle).
    Rückgabe: {summe, je_person [(Name, Anzahl)], gezaehlt [..], zuege, storniert, ueberschneidungen}
    """
    P = ph()
    leer = {"summe": 0, "je_person": [], "gezaehlt": [], "zuege": 0, "storniert": 0, "ueberschneidungen": 0}
    try:
        db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT id, transportart, reisender, ki_json FROM belege
                        WHERE reise_code={P} AND transportart IN ('Flug','Bahn')
                        AND aenderung_status IS NULL ORDER BY id""", (reise_code,))
        rows = cur.fetchall()
        cur.execute(f"""SELECT fs.beleg_id, fs.segment_index, fs.status FROM flug_status fs
                        JOIN belege b ON b.id = fs.beleg_id WHERE b.reise_code={P}""", (reise_code,))
        stati = {(r[0] if isinstance(r, tuple) else r["beleg_id"],
                  r[1] if isinstance(r, tuple) else r["segment_index"]):
                 str((r[2] if isinstance(r, tuple) else r["status"]) or "").lower()
                 for r in cur.fetchall()}
        cur.execute(f"""SELECT m.klarname FROM mitarbeiter m JOIN reise_mitarbeiter rm ON rm.kuerzel=m.kuerzel
                        WHERE rm.reise_code={P}""", (reise_code,))
        mitarbeiter = [(r[0] if isinstance(r, tuple) else r["klarname"]) for r in cur.fetchall()]
        cur.close(); db.close()
    except Exception:
        return leer

    gezaehlt, zuege, storniert = [], 0, 0
    for r in rows:
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        bid, art = g("id", 0), g("transportart", 1)
        try:
            ki = json.loads(g("ki_json", 3) or "{}")
        except Exception:
            continue
        reisender = (g("reisender", 2) or ki.get("reisender") or "").strip()
        for idx, s in enumerate(ki.get("segmente") or []):
            if segment_ist_zug(s, art):
                zuege += 1
                continue
            if "cancel" in stati.get((bid, idx), ""):
                storniert += 1
                continue
            d = _datum_parsen(s.get("abreise_datum"))
            gezaehlt.append({"beleg": bid, "idx": idx, "datum": d, "zeit": str(s.get("abreise_zeit") or ""),
                             "nummer": str(s.get("transport_nummer") or "").strip(),
                             "von": _flug_ort_text(s, "von"), "nach": _flug_ort_text(s, "nach"),
                             "reisender": reisender,
                             "iata_fehlt": not (str(s.get("von_iata") or "").strip()
                                                and str(s.get("nach_iata") or "").strip())})
    gezaehlt.sort(key=lambda x: (x["datum"] or date.max, x["zeit"], x["beleg"], x["idx"]))

    # Je Person zusammenfassen: Name aus der Reise-Zuordnung, sonst der Name vom Beleg
    gruppen = []                                   # [Anzeigename, Namensteile, Anzahl]
    for f in gezaehlt:
        tok = _namens_tokens(f["reisender"])
        anzeige = f["reisender"] or "ohne Namen"
        if not tok and len(mitarbeiter) == 1:         # Beleg ohne Namen, Reise mit nur einer Person
            anzeige = mitarbeiter[0]; tok = _namens_tokens(anzeige)
        ziel = None
        for gr in gruppen:
            if (tok and gr[1] and (tok & gr[1])) or (not tok and not gr[1]):
                ziel = gr; break
        if ziel is None:
            for kl in mitarbeiter:
                if tok & _namens_tokens(kl):
                    anzeige = kl; break
            ziel = [anzeige, set(tok), 0]
            gruppen.append(ziel)
        ziel[2] += 1
        f["person"] = ziel[0]

    try:
        offen = len(_segment_ueberschneidungen(reise_code))
    except Exception:
        offen = 0
    return {"summe": len(gezaehlt), "je_person": [(gr[0], gr[2]) for gr in gruppen],
            "gezaehlt": gezaehlt, "zuege": zuege, "storniert": storniert, "ueberschneidungen": offen}


def gesamtbetrag_berechnen(betrag_brutto, waehrung, betrag_eur_geschaetzt,
                            betrag_eur_final, nebenkosten_eur, auslandsentgelt_eur=None) -> float:
    """
    Einheitliche Eurobetrag-Logik für einen Beleg:
    1. Betrag Beleg final (manuell, gemäß Abrechnung) hat Vorrang, falls gesetzt.
    2. Sonst der von der KI geschätzte Euro-Gegenwert (bei Fremdwährung).
    3. Sonst der Bruttobetrag selbst, falls bereits in EUR.
    4. Nebenkosten (manuell, z.B. sonstige Zusatzkosten) werden immer addiert.
    5. Auslandsentgelt (automatisch berechnet, siehe auslandsentgelt_neu_berechnen)
       wird ebenfalls immer addiert.
    """
    if betrag_eur_final is not None:
        basis = float(betrag_eur_final)
    elif betrag_eur_geschaetzt is not None:
        basis = float(betrag_eur_geschaetzt)
    elif (waehrung or "EUR") == "EUR" and betrag_brutto is not None:
        basis = float(betrag_brutto)
    else:
        basis = 0.0
    return basis + float(nebenkosten_eur or 0) + float(auslandsentgelt_eur or 0)


def beleg_eur_mit_anteil(betrag_brutto, waehrung, betrag_eur_geschaetzt, betrag_eur_final,
                         nebenkosten_eur, auslandsentgelt_eur, anteil=None) -> float:
    """
    Wie gesamtbetrag_berechnen. Hat die Rechnung einen Anteil an einer Buchung (Euro-Betrag und
    Auslandsentgelt aus der Kreditkartenabrechnung, siehe mod_belegdeckung.anteile_aus_buchung),
    ersetzen die Anteilswerte den Euro-Betrag und das Entgelt; Nebenkosten der Buchung kommen dazu.
    """
    if anteil:
        betrag_eur_final = anteil["eur_final"]
        nebenkosten_eur = float(nebenkosten_eur or 0) + anteil["nebenkosten"]
        if anteil["entgelt"] is not None:
            auslandsentgelt_eur = anteil["entgelt"]
    return gesamtbetrag_berechnen(betrag_brutto, waehrung, betrag_eur_geschaetzt, betrag_eur_final,
                                  nebenkosten_eur, auslandsentgelt_eur)


def auslandsentgelt_neu_berechnen(bid: int, db) -> None:
    """
    Berechnet automatisch die Auslandseinsatzentgelt-Gebühr der Kreditkarte
    (typischerweise 1-2%, je nach Karte unterschiedlich) und speichert sie in
    belege.kreditkarte_auslandsentgelt_eur. Wird bei jeder Änderung der
    Kartenwahl oder des finalen Eurobetrags neu aufgerufen, damit der Wert
    nie veraltet ist. Greift NUR bei Fremdwährungsbelegen (bei EUR-Belegen
    fällt üblicherweise keine gesonderte Auslandsgebühr an) und nur, wenn
    eine Karte zugeordnet ist, die auf ein bekanntes Mitarbeiterkürzel endet.
    Bewusst getrennt von "nebenkosten_eur" (das bleibt ein rein manuelles
    Feld für sonstige Zusatzkosten) – so geht der automatisch berechnete Wert
    nie verloren, nur weil jemand die Nebenkosten aus einem anderen Grund
    bearbeitet.
    """
    P = ph()
    cur = db.cursor()
    cur.execute(f"""SELECT waehrung, betrag_brutto, betrag_eur, betrag_eur_final, kreditkarte_karte,
                           kk_abrechnung_datum
                    FROM belege WHERE id={P}""", (bid,))
    row = cur.fetchone()
    if not row:
        cur.close(); return
    g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
    if g("kk_abrechnung_datum",5) is not None:
        # Betrag und Gebühr stammen aus der echten Kreditkartenabrechnung -> nie neu berechnen
        cur.close(); return
    waehrung = g("waehrung",0) or "EUR"
    kreditkarte_karte = g("kreditkarte_karte",4)

    wert = None
    if waehrung != "EUR" and kreditkarte_karte:
        basis = g("betrag_eur_final",3)
        if basis is None: basis = g("betrag_eur",2)
        if basis is None: basis = g("betrag_brutto",1)  # letzter Rückfall, ungenau bei Fremdwährung
        # Mitarbeiterkürzel aus "Firmenkreditkarte-RD"/"Privatkreditkarte-RD" extrahieren
        kuerzel = kreditkarte_karte.rsplit("-", 1)[-1].strip() if "-" in kreditkarte_karte else None
        if kuerzel and basis is not None:
            cur.execute(f"""SELECT kreditkarten_auslandsentgelt_prozent
                            FROM mitarbeiter WHERE kuerzel={P}""", (kuerzel,))
            mrow = cur.fetchone()
            if mrow:
                prozent = mrow[0] if isinstance(mrow, tuple) else mrow["kreditkarten_auslandsentgelt_prozent"]
                if prozent is not None:
                    wert = round(float(basis) * float(prozent) / 100, 2)

    cur.execute(f"UPDATE belege SET kreditkarte_auslandsentgelt_eur={P} WHERE id={P}", (wert, bid))
    cur.close()

# ── FastAPI App ────────────────────────────────────────────────────────────────
app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)

# ── FastAPI App ────────────────────────────────────────────────────────────────
app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)

@app.middleware("http")
async def login_erforderlich(request: Request, call_next):
    antwort = await _zugang_pruefen(request, call_next)
    if request.method == "POST" and antwort.status_code < 400 and not request.url.path.startswith("/cron"):
        try:   # beschädigte App-Belege zurücksetzen, Kurs-Schreibweise vereinheitlichen
            from starlette.concurrency import run_in_threadpool
            await run_in_threadpool(bewirtung_kosten_reparieren)
        except Exception:
            pass
    return antwort


async def _zugang_pruefen(request: Request, call_next):
    pfad = request.url.path
    if pfad_ist_offen(pfad):
        return await call_next(request)
    if not hat_bereits_passwoerter():
        # Noch kein Passwort im System vergeben → Ersteinrichtung erlauben
        if pfad != "/setup":
            return RedirectResponse("/setup", status_code=303)
        return await call_next(request)
    if not request.session.get("kuerzel"):
        return RedirectResponse(f"/login?next={pfad}", status_code=303)
    return await call_next(request)

# WICHTIG: erst NACH der eigenen Middleware registrieren, damit SessionMiddleware
# beim Ausführen "außen" liegt und request.session bereits gesetzt ist, bevor
# login_erforderlich darauf zugreift (Starlette: zuletzt hinzugefügt = läuft zuerst).
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET,
                    same_site="lax", max_age=60*60*24*14)

@app.on_event("startup")
async def startup():
    print(f"[Startup] Herrhammer Reisekosten {APP_VERSION} gestartet")
    if SESSION_SECRET_HERKUNFT != "eingetragen":
        print(f"[Startup] SESSION_SECRET fehlt – Ersatzschlüssel ({SESSION_SECRET_HERKUNFT}) aktiv. Bitte in den Einstellungen eintragen.")
    eigenbelege_pflichtfelder_reparieren()
    bewirtung_kosten_reparieren()
    habel_einzel_spalte_sichern()
    spalte_sichern("bewirtungen", "teilbetrag", "NUMERIC(10,2)")
    spalte_sichern("vma_tage", "satz_hinweis", "TEXT")
    try:
        n_mwst = mod_beleg.mwst_flug_bahn_reparieren()
        if n_mwst:
            print(f"[Startup] MwSt. bei {n_mwst} Flug-/Bahn-Beleg(en) entfernt (war hochgerechnet, nicht aufgedruckt)")
    except Exception as e:
        print(f"[Startup] MwSt.-Bereinigung übersprungen: {e}")

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── System-Routen ──────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
# SCHRITT B) – BELEGE VERARBEITEN
# ═══════════════════════════════════════════════════════════════════════════════



# ── App ───────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    print(f"[Startup] Herrhammer Reisekosten {APP_VERSION} gestartet")
    eigenbelege_pflichtfelder_reparieren()
    bewirtung_kosten_reparieren()

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Beleg-Routen ──────────────────────────────────────────────────────────────
@app.get("/beleg/upload", response_class=HTMLResponse)
def beleg_upload_form():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT code, titel, abreise FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
        unzugeordnet = cur.fetchone()[0]
        cur.close(); db.close()
    except: reisen = []

    def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

    opts = '<option value="">– Reise wählen (optional) –</option>'
    for r in reisen:
        code = get(r,"code",0); titel = get(r,"titel",1); ab = get(r,"abreise",2)
        opts += f'<option value="{code}">{code} – {titel} ({fmt_date(ab)})</option>'

    content = f"""
    <h1 class="page-title">Beleg hochladen</h1>
    <div class="card" style="max-width:560px">
      <div class="card-body">
        <div class="alert alert-warn" style="margin-bottom:20px">
          Der Beleg wird automatisch:<br>
          1. Zu PDF konvertiert (bei Foto/Bild)<br>
          2. Anonymisiert (Namen, E-Mails, Herrhammer)<br>
          3. Von GPT-4o analysiert (Typ, Betrag, Datum, Segmente)
        </div>
        <form method="post" action="/beleg/upload" enctype="multipart/form-data">
          <div class="form-grid">
            <div class="form-group">
              <label>Reise zuordnen</label>
              <select name="reise_code" class="sel">{opts}</select>
              <div class="form-hint">Oder leer lassen und später zuordnen</div>
            </div>
            <div class="form-group">
              <label>Datei <span class="required">*</span></label>
              <input type="file" name="datei" required
                     accept=".pdf,.jpg,.jpeg,.png,.heic,.webp,.tif,.tiff,.xml"
                     style="width:100%;padding:8px;border:1px solid var(--border);
                            border-radius:var(--radius-s);background:white">
              <div class="form-hint">PDF, JPG, PNG, TIF, HEIC, WebP</div>
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">
              Hochladen & Analysieren
            </button>
            <a href="/belege" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell("Beleg hochladen", content))

@app.post("/beleg/upload")
async def beleg_upload(request: Request,
                       datei: UploadFile = File(...),
                       reise_code: str = Form("")):
    try:
        datei_bytes = await datei.read()
        ct = datei.content_type or "application/octet-stream"
        rc = reise_code.strip() or None

        result = await beleg_verarbeiten(datei_bytes, datei.filename or "upload", rc, ct)
        return RedirectResponse(f"/beleg/{result['beleg_id']}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err"><b>Fehler:</b> {e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'
            '<a href="/beleg/upload" class="btn btn-secondary">Zurück</a>'))

def _verknuepfung_karte(bid: int, rcode) -> str:
    """
    Karte „Verknüpfte Belege“ an der Beleg-Seite. Jede Buchung braucht Rechnungen oder Quittungen:
    zeigt Buchung x gegen Rechnungen x1 + x2, was noch fehlt, passende Vorschläge (Summe stimmt genau)
    und die Auswahl weiterer Belege der Reise. Die Rechenregeln stehen in mod_belegdeckung.
    """
    from html import escape as e
    D = deckung_mod

    def karte(inhalt: str) -> str:
        return ('<div class="card"><div class="card-header"><span class="card-title">🔗 Verknüpfte Belege</span></div>'
                f'<div class="card-body">{inhalt}</div></div>')

    leise = 'font-size:11px;color:var(--muted)'
    if not rcode:
        return karte(f'<div style="{leise};font-style:italic">Beleg muss zuerst einer Reise zugeordnet sein.</div>')
    try:
        belege = D.belege_der_reise(rcode)
    except Exception as ex:
        return karte(f'<div class="alert alert-err" style="font-size:12px">Verknüpfungen nicht ladbar: {e(str(ex))}</div>')
    by_id = {b["id"]: b for b in belege}
    me = by_id.get(bid)
    if not me:
        return karte(f'<div style="{leise};font-style:italic">Beleg gehört nicht zu dieser Reise.</div>')

    deckung = D.kategorisieren(belege)["deckung"]
    gid = me.get("beleg_gruppe_id")
    info = deckung.get(gid) if gid else None
    html = ""

    def betrag_s(b):
        return f'{float(b["betrag_brutto"]):.2f} {(b.get("waehrung") or "EUR")}' if D.hat_betrag(b) else "–"

    def zeile_kurz(b):
        art = "Buchung" if D.ist_buchung(b.get("belegart")) else D.art_name(b.get("belegart"))
        return f'<a href="/beleg/{b["id"]}" style="color:#2563eb">#{b["id"]}</a> · {e(art)} · {e(b.get("anbieter") or "–")} · {betrag_s(b)}'

    # 0) dieser Beleg ist ein Zahlungsnachweis (z. B. PayU-Beleg über 194.50 EUR zur Buchung in Rupien)
    if info and bid in info["nachweis_ids"]:
        html += ('<div style="background:#f1f5f9;color:#334155;border-radius:6px;padding:8px 10px;margin-bottom:10px;'
                 'font-size:12px">ℹ <b>Zahlungsnachweis:</b> Dieser Beleg weist die Zahlung der Buchung nach und zählt '
                 'nicht zusätzlich zu den Rechnungen. Er liefert den Euro-Betrag. Er geht trotzdem als eigenes PDF an Habel.</div>')

    # 1) Rechnung: Buchung x gegen Rechnungen x1 + x2
    me_dk = D.ist_durch_kunde(me)
    if me_dk:
        html += ('<div style="background:#eff6ff;color:#1e3a8a;border-radius:6px;padding:8px 10px;margin-bottom:10px;'
                 'font-size:12px">🤝 <b>Durch Kunde direkt bezahlt:</b> Dieser Beleg zählt nicht in der Summe und '
                 'braucht keine Rechnung oder Quittung. Er wird nur vermerkt.</div>')
    elif info and D.status_anzeige(info):
        html += D.zusammenfassung_html(info, by_id)
    elif D.ist_buchung(me.get("belegart")) and not (info and info["finale_ids"]):
        html += ('<div style="background:#fff7ed;color:#9a3412;border-radius:6px;padding:8px 10px;margin-bottom:10px;'
                 'font-size:12px;font-weight:600">⚠ Jede Buchung braucht eine Rechnung oder Quittung. '
                 'Zu dieser Buchung ist noch keine verknüpft.</div>')

    # 1b) Rechnungen/Quittungen ohne erkannten Betrag: direkt hier eintragen (Vorschlag = Rest der Buchung)
    ohne_betrag = [b for b in belege if D.ist_deckender_beleg(b.get("belegart")) and not D.hat_betrag(b)
                   and not D.ist_durch_kunde(b)
                   and "aenderung" not in (b.get("belegart") or "").lower()
                   and "änderung" not in (b.get("belegart") or "").lower()
                   and (b["id"] == bid or (gid and b.get("beleg_gruppe_id") == gid))]
    vorschlag_betrag = None
    if info and len(ohne_betrag) == 1 and info["status"] in ("teilweise", "offen") and info["fehlt"] > 0:
        vorschlag_betrag = (info["fehlt"], info["waehrung"])
    for ob in ohne_betrag:
        wert, w_ob = "", (ob.get("waehrung") or "EUR").upper()
        erklaerung = 'Bitte den Gesamtbetrag der Rechnung/Quittung eintragen.'
        if vorschlag_betrag and len(ohne_betrag) == 1:
            wert, w_ob = f"{vorschlag_betrag[0]:.2f}", vorschlag_betrag[1]
            erklaerung = (f'Vorschlag: Rest der Buchung ({wert} {e(w_ob)}). '
                          'Bitte mit dem Betrag auf der Rechnung vergleichen.')
        html += ('<div style="background:#fff7ed;border-radius:6px;padding:8px 10px;margin-bottom:10px;font-size:12px">'
                 f'<div style="font-weight:600;color:#9a3412"><a href="/beleg/{ob["id"]}" style="color:#9a3412">#{ob["id"]}</a> · '
                 f'{e(D.art_name(ob.get("belegart")))} · {e(ob.get("anbieter") or "–")} hat keinen Betrag.</div>'
                 f'<div style="{leise};margin:2px 0 6px">{erklaerung}</div>'
                 f'<form method="post" action="/beleg/{ob["id"]}/betrag/bearbeiten" '
                 'style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin:0">'
                 f'<input type="hidden" name="zurueck" value="/beleg/{bid}">'
                 f'<input type="number" step="0.01" name="betrag_brutto" value="{wert}" placeholder="Betrag" '
                 'style="width:120px" required>'
                 f'<input type="text" name="waehrung" value="{e(w_ob)}" maxlength="3" style="width:56px">'
                 '<button type="submit" class="btn btn-primary btn-sm">Betrag speichern</button></form>'
                 f'<form method="post" action="/beleg/{ob["id"]}/neu-analysieren" style="margin:6px 0 0">'
                 '<button type="submit" class="btn btn-secondary btn-sm">Beleg neu auslesen</button></form></div>')

    # 2) Gruppenmitglieder
    mitglieder = [b for b in belege if gid and b.get("beleg_gruppe_id") == gid and b["id"] != bid]
    if mitglieder:
        html += (f'<div style="{leise};margin-bottom:8px">Dieser Beleg gehört zu einer Gruppe von {len(mitglieder) + 1} Belegen. '
                 'Beim Senden an Habel geht jeder Beleg einzeln als eigenes PDF in der Mail der Reise mit.</div>')
        for m in mitglieder:
            gp = "✓ geprüft" if m.get("geprueft") else "○ noch nicht geprüft"
            if info and m["id"] in info["nachweis_ids"]:
                gp += " · <b>Zahlungsnachweis</b> (zählt nicht extra)"
            html += ('<div style="display:flex;justify-content:space-between;align-items:center;'
                     'background:var(--bg);border-radius:6px;padding:8px 10px;margin-bottom:6px">'
                     f'<span style="font-size:12px">{zeile_kurz(m)} · {gp}</span>'
                     f'<form method="post" action="/beleg/{bid}/aus-gruppe-entfernen/{m["id"]}">'
                     '<button type="submit" class="btn btn-secondary btn-sm">Entfernen</button></form></div>')

    # 3) Vorschläge: Auswahl, deren Summe genau zur Buchung passt (z. B. 120 + 180 = 300)
    def offener_betrag(buchung):
        g = buchung.get("beleg_gruppe_id")
        d = deckung.get(g) if g else None
        if d is None:
            return float(buchung["betrag_brutto"]), (buchung.get("waehrung") or "EUR").upper()
        if d["status"] in ("offen", "teilweise"):
            return d["fehlt"], d["waehrung"]
        return None, None

    frei = [b for b in belege if b["id"] != bid and D.ist_deckender_beleg(b.get("belegart"))
            and D.hat_betrag(b) and not b.get("beleg_gruppe_id") and not D.ist_durch_kunde(b)]
    vorschlaege = []   # (buchung, [beleg_ids], summe, waehrung)
    if D.ist_buchung(me.get("belegart")) and D.hat_betrag(me):
        ziel, w = offener_betrag(me)
        if ziel:
            ids = D.kombination_vorschlag(ziel, [(b["id"], b["betrag_brutto"]) for b in frei
                                                   if D.waehrung_von(b) == w])
            if ids:
                vorschlaege.append((me, ids, ziel, w))
    elif D.ist_deckender_beleg(me.get("belegart")) and D.hat_betrag(me) and not gid:
        pool = frei + [me]
        for buchung in belege:
            if not (D.ist_buchung(buchung.get("belegart")) and D.hat_betrag(buchung)):
                continue
            ziel, w = offener_betrag(buchung)
            if not ziel:
                continue
            ids = D.kombination_vorschlag(ziel, [(b["id"], b["betrag_brutto"]) for b in pool if D.waehrung_von(b) == w],
                                          muss_id=bid)
            if ids:
                vorschlaege.append((buchung, ids, ziel, w))
    for buchung, ids, ziel, w in vorschlaege[:3]:
        teile = " + ".join(f'<a href="/beleg/{i}" style="color:#2563eb">#{i}</a> ({float(by_id[i]["betrag_brutto"]):.2f})' for i in ids)
        html += ('<div style="background:#eff6ff;color:#1e3a8a;border-radius:6px;padding:8px 10px;margin-bottom:10px;font-size:12px">'
                 f'💡 <b>Passender Vorschlag:</b> Buchung <a href="/beleg/{buchung["id"]}" style="color:#1e3a8a">#{buchung["id"]}</a> '
                 f'({e(buchung.get("anbieter") or "–")}): {teile} = <b>{ziel:.2f} {e(w)}</b> – die Summe stimmt genau.'
                 f'<form method="post" action="/beleg/{buchung["id"]}/verknuepfen-mehrere" style="margin-top:6px">'
                 + "".join(f'<input type="hidden" name="ids" value="{i}">' for i in ids)
                 + '<button type="submit" class="btn btn-primary btn-sm">Alle verknüpfen</button></form></div>')

    # 4) Weitere Belege der Reise zum Verknüpfen (Auswahl, mehrere auf einmal)
    cur_norm = None
    for b in belege:
        if b["id"] == bid:
            cur_norm = D.code_normalisieren(b.get("buchungscode"))
    kandidaten = [b for b in belege if b["id"] != bid and not (gid and b.get("beleg_gruppe_id") == gid)]

    def rang(b):
        gleicher_code = bool(cur_norm) and len(cur_norm) >= D.MIN_CODE_LAENGE and D.code_normalisieren(b.get("buchungscode")) == cur_norm
        gleicher_anbieter = bool(me.get("anbieter")) and (b.get("anbieter") or "").strip().lower() == (me.get("anbieter") or "").strip().lower()
        return (0 if gleicher_code else 1 if gleicher_anbieter else 2, -b["id"]), gleicher_code, gleicher_anbieter

    kandidaten.sort(key=lambda b: rang(b)[0])
    zeilen = ""
    for b in kandidaten[:30]:
        _, gleicher_code, gleicher_anbieter = rang(b)
        tags = ""
        if gleicher_code:
            tags += ' <span style="color:#059669;font-weight:600">· gleicher Buchungscode</span>'
        elif gleicher_anbieter:
            tags += ' <span style="color:#2563eb">· gleicher Anbieter</span>'
        if D.ist_durch_kunde(b):
            tags += ' <span style="color:#1e40af">· 🤝 durch Kunde bezahlt</span>'
        elif D.ist_buchung(b.get("belegart")) and D.hat_betrag(b):
            rest, w = offener_betrag(b)
            if rest:
                tags += f' <span style="color:#c2410c">· offen: {rest:.2f} {e(w)}</span>'
        zeilen += ('<label style="display:flex;gap:8px;align-items:center;padding:6px 0;'
                   'border-bottom:1px solid var(--border);font-size:12px;cursor:pointer">'
                   f'<input type="checkbox" name="ids" value="{b["id"]}">'
                   f'<span>{zeile_kurz(b)}{tags}</span></label>')
    if zeilen:
        html += (f'<div style="{leise};margin:10px 0 4px">{"Weitere Belege" if mitglieder else "Belege dieser Reise"} '
                 'verknüpfen (mehrere möglich):</div>'
                 f'<form method="post" action="/beleg/{bid}/verknuepfen-mehrere">{zeilen}'
                 '<button type="submit" class="btn btn-secondary btn-sm" style="margin-top:8px">'
                 '+ Ausgewählte verknüpfen</button></form>')
    if not html:
        html = f'<div style="{leise};font-style:italic">Keine weiteren Belege in dieser Reise gefunden.</div>'
    return karte(html)


# ── Beleg Detailseite ──────────────────────────────────────────────────────────
@app.get("/beleg/{bid}", response_class=HTMLResponse)
def beleg_detail(bid: int, request: Request):
    try:
        benutzer_ist_organisator = ist_organisator(request)
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"""SELECT id, reise_code, transportart, dateiname,
            s3_original, s3_anon, s3_analyse, rohtext, anon_text, ki_json,
            pflichtfelder_ok, fehlende_felder,
            belegdatum, belegart, anbieter, rechnungsnummer, buchungscode,
            reisender, land_beleg,
            betrag_brutto, betrag_netto, betrag_mwst, waehrung,
            event_datum_von, event_datum_bis, event_ort_von, event_ort_bis,
            hotel_name, hotel_checkin_datum, hotel_checkin_zeit,
            hotel_checkout_datum, hotel_checkout_zeit, hotel_naechte,
            tanken_kraftstoff, tanken_menge, tanken_einheit,
            tanken_preis_einheit, tanken_tankstelle, tanken_kennzeichen,
            status, fehler, erstellt,
            kurs_eur, betrag_eur, kurs_datum, kurs_quelle,
            zahlungsart, geprueft, pruef_vermerk, geprueft_von, geprueft_am, dms_versendet_am,
            beleg_gruppe_id, ist_erechnung, erechnung_format, s3_erechnung_xml, kreditkarte_karte,
            betrag_eur_final, nebenkosten_eur, nebenkosten_beschreibung,
            kreditkarte_auslandsentgelt_eur,
            aenderung_status, aenderung_zu_beleg_id, aenderung_diff_json
            FROM belege WHERE id={P}""", (bid,))
        r = cur.fetchone()
        # Reisen für Zuordnung
        cur.execute("SELECT code,titel FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        # Mitarbeiter für Kreditkarten-Auswahl (nur bei Bezahlart "Kreditkarte" genutzt)
        cur.execute("SELECT kuerzel, klarname, kreditkarten_typ FROM mitarbeiter WHERE aktiv=" +
                    ("TRUE" if is_postgres() else "1") + " ORDER BY klarname")
        ma_kreditkarte_rows = cur.fetchall()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Beleg nicht gefunden.</div>'))

        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        bid2=get(r,"id",0); rcode=get(r,"reise_code",1); typ=get(r,"transportart",2)
        dateiname=get(r,"dateiname",3); s3o=get(r,"s3_original",4)
        s3a=get(r,"s3_anon",5); s3an=get(r,"s3_analyse",6)
        rohtext=get(r,"rohtext",7); anon_text=get(r,"anon_text",8)
        ki_json_str=get(r,"ki_json",9)
        pf_ok=get(r,"pflichtfelder_ok",10); fehlend=get(r,"fehlende_felder",11)
        belegdatum=get(r,"belegdatum",12); belegart=get(r,"belegart",13)
        vendor=get(r,"anbieter",14); rechnr=get(r,"rechnungsnummer",15)
        buchungscode=get(r,"buchungscode",16); reisender=get(r,"reisender",17)
        land=get(r,"land_beleg",18)
        betrag_brutto=get(r,"betrag_brutto",19); betrag_netto=get(r,"betrag_netto",20)
        betrag_mwst=get(r,"betrag_mwst",21); waehrung=get(r,"waehrung",22)
        ev_von=get(r,"event_datum_von",23); ev_bis=get(r,"event_datum_bis",24)
        ev_ort_von=get(r,"event_ort_von",25); ev_ort_bis=get(r,"event_ort_bis",26)
        hotel_name=get(r,"hotel_name",27)
        hotel_ci_dat=get(r,"hotel_checkin_datum",28); hotel_ci_zeit=get(r,"hotel_checkin_zeit",29)
        hotel_co_dat=get(r,"hotel_checkout_datum",30); hotel_co_zeit=get(r,"hotel_checkout_zeit",31)
        hotel_naechte=get(r,"hotel_naechte",32)
        tank_kraft=get(r,"tanken_kraftstoff",33); tank_menge=get(r,"tanken_menge",34)
        tank_einh=get(r,"tanken_einheit",35); tank_preis=get(r,"tanken_preis_einheit",36)
        tank_stelle=get(r,"tanken_tankstelle",37); tank_kfz=get(r,"tanken_kennzeichen",38)
        status=get(r,"status",39); fehler=get(r,"fehler",40); erstellt=get(r,"erstellt",41)
        kurs_eur=get(r,"kurs_eur",42); betrag_eur=get(r,"betrag_eur",43)
        kurs_datum=get(r,"kurs_datum",44); kurs_quelle=get(r,"kurs_quelle",45)
        zahlungsart=get(r,"zahlungsart",46); geprueft=bool(get(r,"geprueft",47))
        pruef_vermerk=get(r,"pruef_vermerk",48); geprueft_von=get(r,"geprueft_von",49)
        geprueft_am=get(r,"geprueft_am",50); dms_versendet_am=get(r,"dms_versendet_am",51)
        gruppe_id=get(r,"beleg_gruppe_id",52)
        ist_erechnung=bool(get(r,"ist_erechnung",53)); erechnung_format=get(r,"erechnung_format",54)
        s3_erechnung_xml=get(r,"s3_erechnung_xml",55)
        kreditkarte_karte=get(r,"kreditkarte_karte",56)
        betrag_eur_final=get(r,"betrag_eur_final",57)
        nebenkosten_eur=get(r,"nebenkosten_eur",58)
        nebenkosten_beschreibung=get(r,"nebenkosten_beschreibung",59)
        auslandsentgelt_eur=get(r,"kreditkarte_auslandsentgelt_eur",60)
        aenderung_status=get(r,"aenderung_status",61)
        aenderung_zu=get(r,"aenderung_zu_beleg_id",62)
        aenderung_diff_json=get(r,"aenderung_diff_json",63)
        aenderung_html = _aenderung_panel_html(bid2, aenderung_status, aenderung_zu, aenderung_diff_json)
        aenderung_hinweis_html = _aenderung_hinweis_fuer_original_html(bid2)
        zusammenfassung = f"{typ}: {vendor} – {betrag_brutto} {waehrung}" if vendor else ""

        # KI-JSON parsen
        ki = {}
        try: ki = json.loads(ki_json_str or "{}")
        except: pass

        segmente = ki.get("segmente") or []

        # Typ-Badge
        typ_farben = {
            "Flug":"#dbeafe:#1e40af","Hotel":"#dcfce7:#166534",
            "Bahn":"#e0e7ff:#3730a3","Taxi":"#fef3c7:#92400e",
            "Mietwagen":"#fce7f3:#9d174d","Bewirtung":"#fff7ed:#9a3412",
            "Tanken":"#f0fdf4:#14532d","Sonstiges":"#f1f5f9:#475569"
        }
        tc = typ_farben.get(typ or "Sonstiges","#f1f5f9:#475569").split(":")
        typ_badge = (f'<span style="background:{tc[0]};color:{tc[1]};'
                     f'padding:3px 10px;border-radius:4px;font-size:12px;'
                     f'font-weight:700">{typ}</span>')

        # Segmente Tabelle
        seg_html = ""
        if segmente:
            rows = ""
            for s in segmente:
                ab_tz = s.get("abreise_zeitzone","") or ""
                an_tz = s.get("ankunft_zeitzone","") or ""
                von_iata = s.get("von_iata","") or ""
                von_ort  = s.get("von_ort","") or ""
                nach_iata= s.get("nach_iata","") or ""
                nach_ort = s.get("nach_ort","") or ""
                t_name   = s.get("transport_name","") or ""
                t_nr     = s.get("transport_nummer","") or ""
                ab_dat   = s.get("abreise_datum","") or ""
                ab_zeit  = s.get("abreise_zeit","") or ""
                an_dat   = s.get("ankunft_datum","") or ab_dat
                an_zeit  = s.get("ankunft_zeit","") or ""
                klasse   = s.get("klasse","") or ""
                if s.get("sitzplatz"):
                    klasse = (klasse + " · " if klasse else "") + "Sitz " + str(s.get("sitzplatz"))
                hinweis  = s.get("hinweis","") or ""
                rows += (f'<tr>'
                    f'<td style="text-align:center;color:var(--muted);font-size:12px">{s.get("nr","")}</td>'
                    f'<td style="font-weight:700;color:var(--blue);font-family:monospace;white-space:nowrap">'
                    f'{t_name} {t_nr}</td>'
                    f'<td><b>{von_iata}</b>'
                    f'<div style="font-size:11px;color:var(--muted)">{von_ort}</div></td>'
                    f'<td style="color:var(--muted)">→</td>'
                    f'<td><b>{nach_iata}</b>'
                    f'<div style="font-size:11px;color:var(--muted)">{nach_ort}</div></td>'
                    f'<td style="font-family:monospace;font-size:12px;white-space:nowrap">'
                    f'<b>{ab_dat}</b><br>'
                    f'<span style="color:var(--blue)">{ab_zeit} {ab_tz}</span></td>'
                    f'<td style="font-family:monospace;font-size:12px;white-space:nowrap">'
                    f'<b>{an_dat}</b><br>'
                    f'<span style="color:var(--green)">{an_zeit} {an_tz}</span></td>'
                    f'<td style="font-size:11px;color:var(--muted)">{klasse}</td>'
                    f'<td style="font-size:11px;color:var(--muted);max-width:120px">'
                    f'{hinweis}</td>'
                    f'</tr>')
            seg_html = (f'<div class="card" style="margin-top:16px">'
                f'<div class="card-header"><span class="card-title">'
                f'✈ Reisesegmente ({len(segmente)})</span></div>'
                f'<div class="table-wrap"><table>'
                f'<thead><tr><th>#</th><th>Transport</th><th>Von</th><th></th><th>Nach</th>'
                f'<th>Abflug</th><th>Ankunft</th><th>Klasse</th><th>Hinweis</th></tr></thead>'
                f'<tbody>{rows}</tbody></table></div></div>')

        # Reise-Dropdown
        r_opts = '<option value="">– Keine –</option>'
        for rv in reisen:
            rc2 = rv[0] if isinstance(rv,tuple) else rv["code"]
            rt2 = rv[1] if isinstance(rv,tuple) else rv["titel"]
            sel = ' selected' if rc2==rcode else ""
            r_opts += f'<option value="{rc2}"{sel}>{rc2} – {rt2}</option>'

        status_badge = ('<span class="badge badge-green">OK</span>' if status=="ok"
                        else '<span class="badge badge-red">Fehler</span>' if status=="fehler"
                        else '<span class="badge badge-amber">Ausstehend</span>')

        # Prüfung, Habel-Versand & Verknüpfung (nur bei Umsatzbelegen) – als
        # Variablen vorbereitet, um verschachtelte f-strings zu vermeiden.
        def fmt_zeitstempel(v):
            if not v: return "–"
            if isinstance(v, str):
                try:
                    from datetime import datetime as _dt
                    v = _dt.fromisoformat(v[:19])
                except Exception:
                    return v[:16]
            return v.strftime("%d.%m.%Y, %H:%M Uhr")

        erechnung_karte_html = ""
        if ist_erechnung:
            erechnung_karte_html = f"""<div class="card" style="border:1px solid #86efac;background:#f0fdf4">
            <div class="card-body">
              <div style="display:flex;align-items:center;gap:8px">
                <span style="font-size:18px">📄✓</span>
                <div>
                  <div style="font-weight:700;color:#166534;font-size:13px">eRechnung erkannt ({erechnung_format or 'strukturiertes Format'})</div>
                  <div style="font-size:11px;color:#4b5f4f">Die eingebettete Rechnungsdatei wird unverändert mitgeführt und archiviert.</div>
                </div>
              </div>
              {f'<a href="/beleg/{bid2}/erechnung-xml" target="_blank" class="btn btn-secondary" style="width:100%;text-align:center;display:block;margin-top:10px">⬇ eRechnung-XML herunterladen (Original, unverändert)</a>' if s3_erechnung_xml else ''}
            </div>
          </div>"""

        # Kreditkarten-Auswahl: nur relevant, wenn Bezahlart = Kreditkarte
        kreditkarte_dropdown_html = ""
        if zahlungsart == "Kreditkarte":
            kk_opts = '<option value="">– Karte wählen –</option>'
            ma_kreditkarte_sortiert = sorted(
                ma_kreditkarte_rows,
                key=lambda m: (get(m,"kreditkarten_typ",2) != "firma", get(m,"klarname",1)))
            for m in ma_kreditkarte_sortiert:
                mk = get(m,"kuerzel",0); mn = get(m,"klarname",1)
                mtyp = get(m,"kreditkarten_typ",2) or "privat"
                label_typ = "Privatkreditkarte" if mtyp == "privat" else "Firmenkreditkarte"
                wert = f"{label_typ}-{mk}"
                sel = " selected" if wert == kreditkarte_karte else ""
                kk_opts += f'<option value="{wert}"{sel}>{mk} – {mn} ({label_typ})</option>'
            kreditkarte_dropdown_html = f"""
                <form method="post" action="/beleg/{bid2}/kreditkarte" style="margin-top:6px">
                  <label style="font-size:11px;color:var(--muted)">Welche Karte?</label><br>
                  <select name="kreditkarte_karte" onchange="this.form.submit()"
                          style="padding:4px 8px;font-size:12px;border:1px solid var(--border);
                                 border-radius:6px;background:white;margin-top:2px">
                    {kk_opts}
                  </select>
                </form>
                {f'<div style="font-size:12px;color:var(--muted);margin-top:4px">💳 {kreditkarte_karte}</div>' if kreditkarte_karte else ''}
                {_kk_abgerechnet_hinweis(bid2)}"""

        # Verknüpfung: Buchungsbestätigung (Betrag x) ↔ Rechnungen/Quittungen (x1 + x2 = x), siehe mod_belegdeckung
        verknuepfung_karte_html = _verknuepfung_karte(bid2, rcode)

        bewirtung_karte_html = _bewirtung_karte(bid2) + _vorschuss_karte(bid2)

        pruef_karte_html = ""
        if belegart in PRUEF_BELEGARTEN:
            aktueller_user = request.session.get("klarname") or request.session.get("kuerzel") or "–"
            if geprueft:
                vermerk_teil = f' · "{pruef_vermerk}"' if pruef_vermerk else ""
                pruef_status_html = (
                    '<div class="alert alert-ok" style="font-size:12px">Geprüft von <b>'
                    + (geprueft_von or "–") + "</b> am " + fmt_zeitstempel(geprueft_am) + vermerk_teil + "</div>")
            else:
                pruef_status_html = ('<div class="alert alert-warn" style="font-size:12px">Noch nicht geprüft. '
                                      f'Wird gespeichert unter deinem Login: <b>{aktueller_user}</b></div>')

            pruef_pdf_link_html = (
                f'<a href="/beleg/{bid2}/pruef-pdf" target="_blank" class="btn btn-secondary" '
                f'style="width:100%;text-align:center;display:block;margin-bottom:8px">'
                f'👁 Prüf-PDF ansehen (Deckblatt + Original)</a>') if geprueft else ""

            if dms_versendet_am:
                gesendet_text = fmt_zeitstempel(dms_versendet_am)
            else:
                gesendet_text = ""
            dms_block_html = pruef_pdf_link_html + (
                habel_beleg_block_html(bid2, rcode, gesendet_text) if rcode else
                '<div style="font-size:12px;color:var(--muted)">Beleg zuerst einer Reise zuordnen. '
                'Die Übergabe an Habel erfolgt für die ganze Reise.</div>')

            pruef_button_text = "✓ Prüfung aktualisieren" if geprueft else "✓ Als geprüft markieren"
            pruef_karte_html = f"""<div class="card">
            <div class="card-header"><span class="card-title">✅ Prüfung & Übergabe an Habel</span></div>
            <div class="card-body">
              {pruef_status_html}
              <form method="post" action="/beleg/{bid2}/pruefen" style="margin-top:10px">
                <div class="form-group">
                  <label>Prüfvermerk</label>
                  <input type="text" name="pruef_vermerk" value="{pruef_vermerk or ''}"
                         placeholder="z.B. sachlich korrekt, Reise bestätigt">
                </div>
                <button type="submit" class="btn btn-primary" style="width:100%">{pruef_button_text}</button>
              </form>
              <hr style="border:none;border-top:1px solid var(--border);margin:14px 0">
              {dms_block_html}
            </div>
          </div>"""
        elif (benutzer_ist_organisator and betrag_brutto and belegart not in EIGENBELEG_ARTEN
              and "buchungsbest" not in (belegart or "").lower()
              and "aenderung" not in (belegart or "").lower()):
            # Zahlungsbeleg mit Betrag, aber Belegart nicht prüfpflichtig (z. B. „Sonstiges“): erklären statt schweigen
            pruef_karte_html = f"""<div class="card">
            <div class="card-header"><span class="card-title">✅ Prüfung & Übergabe an Habel</span></div>
            <div class="card-body">
              <div class="alert alert-warn" style="font-size:12px">Dieser Beleg ist noch nicht prüfbar, weil die Belegart
                „{belegart or 'leer'}“ nicht prüfpflichtig ist. Ist es ein Zahlungsbeleg (Kassenbon, Taxi, Gebühr),
                bitte als Quittung einstufen. Danach erscheint hier die Prüfung.</div>
              <form method="post" action="/beleg/{bid2}/belegart" style="margin-top:10px">
                <input type="hidden" name="belegart" value="Quittung">
                <button type="submit" class="btn btn-primary" style="width:100%">Als Quittung einstufen</button>
              </form>
            </div>
          </div>"""

        content = f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap">
          <a href="/belege" class="btn btn-secondary">← Belege</a>
          {f'<a href="/reise/{rcode}" class="btn btn-secondary">🧳 Reise {rcode}</a>' if rcode else ''}
          <h1 class="page-title" style="margin:0">Beleg #{bid2}</h1>
          {typ_badge}
          {status_badge}
        </div>
        {aenderung_html}{aenderung_hinweis_html}
        {f'''<div class="alert" style="background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;
             padding:12px 16px;border-radius:8px;margin-bottom:16px">
          ✈ Als <b>Flugänderung/Umbuchung</b> erkannt – die neuen Flugdaten stehen unten im Beleg,
          sind aber noch NICHT automatisch mit dem ursprünglichen Flug-Segment in der Reise verknüpft.
          Bitte beim betroffenen Segment auf der Reiseseite auf
          <a href="/beleg/{bid2}/segment/0/umbuchen" style="color:#9a3412;font-weight:700">"✏ Umgebucht"</a>
          gehen (oder über das Dashboard, falls dort schon ein Alarm dazu läuft), um die neuen Zeiten/
          Flugnummer zu übernehmen.</div>''' if belegart == "Flugaenderung" and not aenderung_status else ''}

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
          <div class="card">
            <div class="card-header">
              <span class="card-title">📊 KI-Analyse</span>
              {f'<a href="/beleg/{bid2}/daten-bearbeiten" class="btn btn-secondary btn-sm">✏ Daten nachtragen</a>' if benutzer_ist_organisator else ''}
            </div>
            <div class="card-body">
              <dl style="display:grid;grid-template-columns:160px 1fr;gap:4px 12px">
                <dt style="color:var(--muted);font-size:12px">Datei</dt>
                <dd style="font-size:12px;color:var(--muted)">{dateiname}</dd>
                <dt style="color:var(--muted);font-size:12px">Transportart</dt>
                <dd>{typ_badge}{f' – {ki.get("transportart_freitext")}' if ki.get("transportart_freitext") else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Belegart</dt>
                <dd>
                  <form method="post" action="/beleg/{bid2}/belegart" style="display:inline-block">
                    <select name="belegart" onchange="this.form.submit()"
                            style="padding:4px 8px;font-size:12px;border:1px solid var(--border);
                                   border-radius:6px;background:white">
                      <option value=""{' selected' if not belegart else ''}>– wählen –</option>
                      <option value="Rechnung"{' selected' if belegart=='Rechnung' else ''}>Rechnung</option>
                      <option value="Quittung"{' selected' if belegart=='Quittung' else ''}>Quittung</option>
                      <option value="Buchungsbestaetigung"{' selected' if belegart=='Buchungsbestaetigung' else ''}>Buchungsbestätigung</option>
                      <option value="Flugaenderung"{' selected' if belegart=='Flugaenderung' else ''}>✈ Flugänderung/Umbuchung</option>
                      <option value="Tankbeleg"{' selected' if belegart=='Tankbeleg' else ''}>Tankbeleg (Rechnung)</option>
                      <option value="Hotel"{' selected' if belegart=='Hotel' else ''}>Hotel</option>
                      <option value="Taxi"{' selected' if belegart=='Taxi' else ''}>Taxi</option>
                      <option value="Bewirtung"{' selected' if belegart=='Bewirtung' else ''}>Bewirtung</option>
                      <option value="Bewirtungsbeleg"{' selected' if belegart=='Bewirtungsbeleg' else ''}>Bewirtungsbeleg (Eigenbeleg)</option>
                      <option value="Vorschussquittung"{' selected' if belegart=='Vorschussquittung' else ''}>Vorschussquittung (Bargeld-Konto)</option>
                      <option value="Bargeldbeschaffung"{' selected' if belegart=='Bargeldbeschaffung' else ''}>Bargeldbeschaffung (Bargeld-Konto)</option>
                      <option value="Rueckgabequittung"{' selected' if belegart=='Rueckgabequittung' else ''}>Rückgabequittung (Bargeld-Konto)</option>
                      <option value="Sonstige Kosten"{' selected' if belegart=='Sonstige Kosten' else ''}>Sonstige Kosten (z.B. Extragepäck)</option>
                      <option value="Sonstiges"{' selected' if belegart=='Sonstiges' else ''}>Sonstiges</option>
                    </select>
                  </form>
                </dd>
                <dt style="color:var(--muted);font-size:12px">Anbieter</dt>
                <dd style="font-weight:600">{vendor or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Reisender</dt>
                <dd>{reisender or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Land</dt>
                <dd>{land or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Betrag brutto</dt>
                <dd style="font-weight:700;color:var(--green);font-size:15px">
                  {f"{float(betrag_brutto):.2f}" if betrag_brutto else "–"} {waehrung}
                  <a href="/beleg/{bid2}/betrag/bearbeiten" style="font-size:12px;color:var(--muted);
                     text-decoration:none;font-weight:400;margin-left:6px">✏</a></dd>
                {'<div class="alert alert-warn" style="margin:8px 0;font-size:12px">Bei Buchungsbestätigungen ist noch kein Betrag nötig – kann später über das ✏ nachgetragen werden.</div>' if not betrag_brutto and belegart == 'Buchungsbestaetigung' else ''}
                <dt style="color:var(--muted);font-size:12px">Bezahlart</dt>
                <dd>
                  <form method="post" action="/beleg/{bid2}/zahlungsart" style="display:inline-block">
                    <select name="zahlungsart" onchange="this.form.submit()"
                            style="padding:4px 8px;font-size:12px;border:1px solid var(--border);
                                   border-radius:6px;background:white">
                      <option value=""{' selected' if not zahlungsart else ''}>– wählen –</option>
                      <option value="Kreditkarte"{' selected' if zahlungsart=='Kreditkarte' else ''}>💳 Kreditkarte</option>
                      <option value="Bar"{' selected' if zahlungsart=='Bar' else ''}>💵 Bar</option>
                      <option value="Ueberweisung"{' selected' if zahlungsart=='Ueberweisung' else ''}>🏦 Überweisung</option>
                      <option value="PayPal"{' selected' if zahlungsart=='PayPal' else ''}>🅿 PayPal</option>
                      <option value="DurchKunde"{' selected' if zahlungsart=='DurchKunde' else ''}>🤝 Durch Kunde</option>
                      <option value="Unbekannt"{' selected' if zahlungsart=='Unbekannt' else ''}>❓ Unbekannt</option>
                    </select>
                  </form>
                  {'' if zahlungsart or belegart == 'Buchungsbestaetigung' else '<span style="font-size:11px;color:#b45309;margin-left:6px">⚠ bitte nachtragen</span>'}
                  {kreditkarte_dropdown_html}
                </dd>
                {f'<dt style="color:var(--muted);font-size:12px">Netto</dt><dd>{float(betrag_netto):.2f} {waehrung}</dd>' if betrag_netto else ""}
                {f'<dt style="color:var(--muted);font-size:12px">{"MwSt." if land == "DE" else "VAT"}</dt><dd>{float(betrag_mwst):.2f} {waehrung}</dd>' if betrag_mwst else ""}
                <dt style="color:var(--muted);font-size:12px">Belegdatum</dt>
                <dd>{fmt_date(belegdatum)}</dd>
                <dt style="color:var(--muted);font-size:12px">Event</dt>
                <dd>{fmt_date(ev_von)}{f" – {fmt_date(ev_bis)}" if ev_bis else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Strecke</dt>
                <dd>{ev_ort_von or "–"}{f" → {ev_ort_bis}" if ev_ort_bis else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Buchungscode</dt>
                <dd style="font-family:monospace">{buchungscode or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Rechnungsnr.</dt>
                <dd style="font-family:monospace">{rechnr or "–"}</dd>
                {f'<dt style="color:var(--muted);font-size:12px">Betrag in Euro (geschätzt)</dt><dd style="font-weight:600;color:var(--green)">{float(betrag_eur):.2f} EUR</dd>' if betrag_eur else ('<dt style="color:var(--amber);font-size:12px">Betrag in Euro (geschätzt)</dt><dd style="color:var(--amber);font-size:12px">⚠ Kurs fehlt – bitte nachtragen</dd>' if waehrung and waehrung != "EUR" and betrag_brutto and zahlungsart != "DurchKunde" else "")}
                {f'<dt style="color:var(--muted);font-size:12px">Kurs</dt><dd style="font-family:monospace">{kurs_eur} ({kurs_datum or ""} {kurs_quelle or ""})</dd>' if kurs_eur else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Hotel</dt><dd style="font-weight:600">{hotel_name}</dd>' if hotel_name else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Check-in</dt><dd>{fmt_date(hotel_ci_dat)} {hotel_ci_zeit or ""}</dd>' if hotel_ci_dat else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Check-out</dt><dd>{fmt_date(hotel_co_dat)} {hotel_co_zeit or ""}</dd>' if hotel_co_dat else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Nächte</dt><dd>{hotel_naechte}</dd>' if hotel_naechte else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Kraftstoff</dt><dd>{tank_kraft}</dd>' if tank_kraft else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Menge</dt><dd>{tank_menge} {tank_einh or ""}</dd>' if tank_menge else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Preis/Einheit</dt><dd>{tank_preis} {waehrung}</dd>' if tank_preis else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Tankstelle</dt><dd>{tank_stelle}</dd>' if tank_stelle else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Kennzeichen</dt><dd style="font-family:monospace">{tank_kfz}</dd>' if tank_kfz else ""}
              </dl>
              {f'<div class="alert alert-err" style="margin-top:12px"><b>Fehlende Pflichtfelder:</b> {fehlend}</div>' if not pf_ok else ""}
              {f'<div class="alert alert-err" style="margin-top:8px">{fehler}</div>' if fehler else ""}
            </div>
          </div>

          {erechnung_karte_html}

          <div class="card">
            <div class="card-header"><span class="card-title">📎 Dokumente</span></div>
            <div class="card-body">
              <div style="display:flex;flex-direction:column;gap:8px">
                <a href="/beleg/{bid2}/pdf/original" target="_blank"
                   class="btn btn-secondary">📄 Original-PDF öffnen</a>
                <a href="/beleg/{bid2}/pdf/anon" target="_blank"
                   class="btn btn-secondary">🔒 Anonymisiert öffnen</a>
                <a href="/beleg/{bid2}/pdf/analyse" target="_blank"
                   class="btn btn-secondary">🔍 Analyse-PDF öffnen</a>
              </div>
              {"" if belegart in EIGENBELEG_ARTEN else f'''<form method="post" action="/beleg/{bid2}/neu-anonymisieren" style="margin-top:8px">
                <button type="submit" class="btn btn-secondary" style="width:100%">
                  🔄 Neu anonymisieren
                </button>
              </form>
              <form method="post" action="/beleg/{bid2}/neu-analysieren" style="margin-top:8px">
                <button type="submit" class="btn btn-secondary" style="width:100%">
                  🧠 Neu analysieren
                </button>
              </form>'''}
              <a href="/beleg/{bid2}/loeschen" class="btn btn-secondary"
                 style="display:block;margin-top:8px;text-align:center;color:#b91c1c;border-color:#fca5a5"
                 onclick="return confirm('Diesen Beleg unwiderruflich löschen?')">
                🗑 Beleg löschen
              </a>
              <hr style="border:none;border-top:1px solid var(--border);margin:16px 0">
              <form method="post" action="/beleg/{bid2}/zuordnen">
                <div class="form-group">
                  <label>Reise zuordnen</label>
                  <select name="reise_code">{r_opts}</select>
                </div>
                <button type="submit" class="btn btn-primary" style="margin-top:8px;width:100%">
                  Speichern
                </button>
              </form>
              {"" if waehrung == "EUR" or belegart in EIGENBELEG_ARTEN else f'''
              <hr style="border:none;border-top:1px solid var(--border);margin:16px 0">
              <div style="font-size:12px;font-weight:600;color:var(--muted);margin-bottom:8px">
                💱 Wechselkurs ({waehrung} → EUR)
              </div>
              <form method="post" action="/beleg/{bid2}/kurs">
                <div class="form-grid form-grid-2" style="gap:8px">
                  <div class="form-group" style="margin:0">
                    <label>Kurs (1 {waehrung} = ? EUR)</label>
                    <input type="number" step="0.0001" name="kurs_eur"
                           value="{kurs_eur or ""}" placeholder="z.B. 0.9200"
                           class="inp" style="font-family:monospace">
                  </div>
                  <div class="form-group" style="margin:0">
                    <label>Kursdatum</label>
                    <input type="date" name="kurs_datum" class="inp"
                           value="{str(kurs_datum)[:10] if kurs_datum else ""}">
                  </div>
                  <div class="form-group full" style="margin:0">
                    <label>Quelle (z.B. EZB, Bank)</label>
                    <input type="text" name="kurs_quelle" class="inp"
                           value="{kurs_quelle or ""}" placeholder="EZB / Kreditkarte">
                  </div>
                </div>
                <div style="margin-top:8px;padding:8px;background:var(--blue-l);
                            border-radius:var(--radius-s);font-size:12px;color:var(--blue)">
                  EUR-Betrag: <b>{f"{float(betrag_brutto)*float(kurs_eur):.2f} EUR" if betrag_brutto and kurs_eur else "wird berechnet"}</b>
                </div>
                <button type="submit" class="btn btn-success" style="margin-top:8px;width:100%">
                  💱 Kurs speichern
                </button>
              </form>'''}
            </div>
          </div>

          <div class="card">
            <div class="card-header"><span class="card-title">💰 Eurobetrag – Endabrechnung</span></div>
            <div class="card-body">
              <p style="font-size:12px;color:var(--muted);margin-bottom:10px">
                Der geschätzte Euro-Betrag oben dient als Arbeitsgrundlage. Sobald die echte
                Abrechnung vorliegt (z.B. Kreditkartenbeleg), hier den tatsächlichen Betrag
                eintragen – das hat Vorrang vor der Schätzung.</p>
              <form method="post" action="/beleg/{bid2}/gesamtbetrag">
                <div class="form-grid form-grid-2" style="gap:8px">
                  <div class="form-group" style="margin:0">
                    <label>Betrag Beleg – final, in EUR (gemäß Abrechnung)</label>
                    <input type="number" step="0.01" name="betrag_eur_final"
                           value="{betrag_eur_final if betrag_eur_final is not None else ''}"
                           placeholder="z.B. 47.30" class="inp">
                  </div>
                  <div class="form-group" style="margin:0">
                    <label>Nebenkosten Beleg, in EUR (z.B. Kreditkartengebühr)</label>
                    <input type="number" step="0.01" name="nebenkosten_eur"
                           value="{nebenkosten_eur if nebenkosten_eur is not None else ''}"
                           placeholder="z.B. 1.20" class="inp">
                  </div>
                  <div class="form-group full" style="margin:0">
                    <label>Bezeichnung Nebenkosten</label>
                    <input type="text" name="nebenkosten_beschreibung" class="inp"
                           value="{nebenkosten_beschreibung or ''}" placeholder="Kreditkartengebühr">
                  </div>
                </div>
                <div style="margin-top:10px;padding:10px;background:var(--green-l,#f0fdf4);
                            border-radius:var(--radius-s);font-size:14px;font-weight:700;color:var(--green)">
                  Gesamtbetrag: {gesamtbetrag_berechnen(betrag_brutto, waehrung, betrag_eur, betrag_eur_final, nebenkosten_eur, auslandsentgelt_eur):.2f} EUR
                  {f'<div style="font-size:11px;font-weight:400;color:var(--muted);margin-top:2px">davon {auslandsentgelt_eur:.2f} EUR automatisch berechnetes Auslandsentgelt der Kreditkarte</div>' if auslandsentgelt_eur else ''}
                </div>
                <button type="submit" class="btn btn-success" style="margin-top:8px;width:100%">
                  Speichern
                </button>
              </form>
            </div>
          </div>

          {bewirtung_karte_html}
          {verknuepfung_karte_html}
          {pruef_karte_html}
        </div>

        {seg_html}

        <div class="card" style="margin-top:16px">
          <div class="card-header"><span class="card-title">📄 Rohtext (original)</span></div>
          <div class="card-body">
            <pre style="font-size:11px;white-space:pre-wrap;color:var(--muted);
                        max-height:200px;overflow-y:auto;background:var(--bg);
                        padding:12px;border-radius:var(--radius-s)">{(rohtext or "").replace("<","&lt;")[:3000]}</pre>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Beleg #{bid2}", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))

@app.get("/beleg/{bid}/daten-bearbeiten", response_class=HTMLResponse)
def beleg_daten_bearbeiten_form(bid: int, request: Request):
    """Formular zum vollständigen Nachtragen/Korrigieren aller von der KI
    erkannten Felder – nur für Organisatoren zugänglich."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen Beleg-Daten nachtragen.</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">Zurück</a>'), status_code=403)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT transportart, transportart_freitext, anbieter, reisender, land_beleg,
                        belegdatum, event_datum_von, event_datum_bis, event_ort_von, event_ort_bis,
                        buchungscode, rechnungsnummer, event_zeit
                        FROM belege WHERE id={P}""", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Beleg nicht gefunden.</div>'))
        g = lambda k,i: r[k] if hasattr(r,'keys') else r[i]
        def v(x): return x if x else ""
        transportart_v = g("transportart",0); freitext_v = v(g("transportart_freitext",1))
        anbieter_v = v(g("anbieter",2)); reisender_v = v(g("reisender",3)); land_v = v(g("land_beleg",4))
        belegdatum_v = str(g("belegdatum",5))[:10] if g("belegdatum",5) else ""
        ev_von_v = str(g("event_datum_von",6))[:10] if g("event_datum_von",6) else ""
        ev_bis_v = str(g("event_datum_bis",7))[:10] if g("event_datum_bis",7) else ""
        ort_von_v = v(g("event_ort_von",8)); ort_bis_v = v(g("event_ort_bis",9))
        buchungscode_v = v(g("buchungscode",10)); rechnr_v = v(g("rechnungsnummer",11))
        event_zeit_v = v(g("event_zeit",12))

        transportarten = ["Hotel","Flug","Bahn","Mietwagen","Taxi","Tanken","Verpflegung","Bewirtung","Sonstiges"]
        typ_opts = "".join(f'<option value="{t}"{" selected" if t==transportart_v else ""}>{t}</option>' for t in transportarten)

        content = f"""
        <h1 class="page-title">Beleg-Daten nachtragen – #{bid}</h1>
        <div class="alert alert-warn" style="margin-bottom:16px">
          Nur für Organisatoren: Felder, die die KI nicht erkennen konnte, hier manuell ergänzen.
        </div>
        <div class="card" style="max-width:560px">
          <div class="card-body">
            <form method="post" action="/beleg/{bid}/daten-bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group">
                  <label>Transportart</label>
                  <select name="transportart"><option value="">– wählen –</option>{typ_opts}</select>
                </div>
                <div class="form-group">
                  <label>Freitext (bei Sonstiges)</label>
                  <input type="text" name="transportart_freitext" value="{freitext_v}">
                </div>
                <div class="form-group">
                  <label>Anbieter</label>
                  <input type="text" name="anbieter" value="{anbieter_v}">
                </div>
                <div class="form-group">
                  <label>Reisender</label>
                  <input type="text" name="reisender" value="{reisender_v}">
                </div>
                <div class="form-group">
                  <label>Land (ISO-Code)</label>
                  <input type="text" name="land_beleg" value="{land_v}" maxlength="2" placeholder="z.B. DE">
                </div>
                <div class="form-group">
                  <label>Belegdatum</label>
                  <input type="date" name="belegdatum" value="{belegdatum_v}">
                </div>
                <div class="form-group">
                  <label>Event-Datum von</label>
                  <input type="date" name="event_datum_von" value="{ev_von_v}">
                </div>
                <div class="form-group">
                  <label>Event-Datum bis</label>
                  <input type="date" name="event_datum_bis" value="{ev_bis_v}">
                </div>
                <div class="form-group">
                  <label>Uhrzeit (bei Tanken/Maut etc.)</label>
                  <input type="time" name="event_zeit" value="{event_zeit_v}">
                </div>
                <div class="form-group"></div>
                <div class="form-group">
                  <label>Strecke – von</label>
                  <input type="text" name="event_ort_von" value="{ort_von_v}">
                </div>
                <div class="form-group">
                  <label>Strecke – bis</label>
                  <input type="text" name="event_ort_bis" value="{ort_bis_v}">
                </div>
                <div class="form-group">
                  <label>Buchungscode</label>
                  <input type="text" name="buchungscode" value="{buchungscode_v}">
                </div>
                <div class="form-group">
                  <label>Rechnungsnummer</label>
                  <input type="text" name="rechnungsnummer" value="{rechnr_v}">
                </div>
              </div>
              <div class="form-hint" style="margin:8px 0">
                Betrag, Belegart und Bezahlart werden weiterhin über die eigenen Felder im Beleg-Detail gepflegt.</div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/beleg/{bid}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Beleg #{bid} – Daten nachtragen", content))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/daten-bearbeiten")
async def beleg_daten_bearbeiten(bid: int, request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen Beleg-Daten nachtragen.</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">Zurück</a>'), status_code=403)
    form = await request.form()
    def s(name): return (form.get(name) or "").strip() or None

    transportart = s("transportart")
    transportart_freitext = s("transportart_freitext")
    anbieter = s("anbieter")
    reisender = s("reisender")
    land_beleg = (s("land_beleg") or "").upper() or None
    belegdatum = s("belegdatum")
    event_datum_von = s("event_datum_von")
    event_datum_bis = s("event_datum_bis")
    event_zeit = s("event_zeit")
    event_ort_von = s("event_ort_von")
    event_ort_bis = s("event_ort_bis")
    buchungscode = s("buchungscode")
    rechnungsnummer = s("rechnungsnummer")

    try:
        P = ph()
        db = get_db(); cur = db.cursor()

        # Bei Hotel-Belegen sind "Event"-Zeitraum und Check-in/Check-out
        # inhaltlich dasselbe (Aufenthaltszeitraum) – beim manuellen Nachtragen
        # bisher nur "Event" aktualisiert, Check-in/Check-out blieben auf dem
        # alten (ggf. falschen) KI-Wert stehen. Jetzt generell mitgezogen.
        hotel_checkin_datum = None
        hotel_checkout_datum = None
        hotel_naechte = None
        zusatz_sql = ""
        zusatz_vals = ()
        if transportart == "Hotel" and (event_datum_von or event_datum_bis):
            hotel_checkin_datum = event_datum_von
            hotel_checkout_datum = event_datum_bis
            try:
                d1 = date.fromisoformat(event_datum_von) if event_datum_von else None
                d2 = date.fromisoformat(event_datum_bis) if event_datum_bis else None
                if d1 and d2 and d2 > d1:
                    hotel_naechte = (d2 - d1).days
            except Exception:
                pass
            zusatz_sql = f", hotel_checkin_datum={P}, hotel_checkout_datum={P}, hotel_naechte={P}"
            zusatz_vals = (hotel_checkin_datum, hotel_checkout_datum, hotel_naechte)

        cur.execute(f"""UPDATE belege SET
            transportart={P}, transportart_freitext={P}, anbieter={P}, reisender={P},
            land_beleg={P}, belegdatum={P}, event_datum_von={P}, event_datum_bis={P},
            event_zeit={P}, event_ort_von={P}, event_ort_bis={P},
            buchungscode={P}, rechnungsnummer={P}{zusatz_sql}
            WHERE id={P}""",
            (transportart, transportart_freitext, anbieter, reisender, land_beleg,
             belegdatum, event_datum_von, event_datum_bis, event_zeit,
             event_ort_von, event_ort_bis, buchungscode, rechnungsnummer)
            + zusatz_vals + (bid,))

        # Pflichtfelder neu bewerten (Betrag/Belegart/Bezahlart bleiben wie gepflegt)
        cur.execute(f"""SELECT belegdatum, transportart, anbieter, betrag_brutto, waehrung,
                        event_datum_von, belegart, zahlungsart FROM belege WHERE id={P}""", (bid,))
        r = cur.fetchone()
        if r:
            g = lambda k,i: r[k] if hasattr(r,'keys') else r[i]
            pflicht_werte = {
                "belegdatum": g("belegdatum",0), "transportart": g("transportart",1),
                "anbieter": g("anbieter",2), "betrag_brutto": g("betrag_brutto",3),
                "waehrung": g("waehrung",4), "event_datum_von": g("event_datum_von",5),
                "zahlungsart": g("zahlungsart",7),
            }
            pflicht = list(pflicht_werte.keys())
            if g("belegart",6) == "Buchungsbestaetigung":
                pflicht = [f for f in pflicht if f not in ("betrag_brutto","waehrung","zahlungsart")]
            elif g("belegart",6) in EIGENBELEG_ARTEN:
                # Eigenbelege (Bewirtung, Vorschuss) sind von der App vollständig erzeugt: nur Datum und Titel nötig
                pflicht = [f for f in pflicht if f in ("belegdatum","anbieter")]
            fehlend = [f for f in pflicht if not pflicht_werte.get(f)]
            pflicht_ok = len(fehlend) == 0
            status = "ok" if pflicht_ok else "fehlerhaft"
            cur.execute(f"""UPDATE belege SET pflichtfelder_ok={P}, fehlende_felder={P}, status={P}
                            WHERE id={P}""",
                        (pflicht_ok, json.dumps(fehlend, ensure_ascii=False), status, bid))

        db.commit(); cur.close(); db.close()
        vma_auto_fuer_beleg(bid)          # Land/Hotel-Zeitraum können sich geändert haben
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/beleg/{bid}/betrag/bearbeiten", response_class=HTMLResponse)
def beleg_betrag_bearbeiten_form(bid: int):
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT betrag_brutto, waehrung, anbieter, belegart FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Beleg nicht gefunden.</div>'))
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        betrag_v = g("betrag_brutto",0); waehrung_v = g("waehrung",1) or "EUR"
        anbieter_v = g("anbieter",2) or ""; belegart_v = g("belegart",3) or ""
        hinweis = ('<div class="alert alert-warn" style="margin-bottom:12px">Bei Buchungsbestätigungen ist '
                   'ein Betrag optional.</div>') if belegart_v == "Buchungsbestaetigung" else ""
        content = f"""
        <h1 class="page-title">Betrag bearbeiten – Beleg #{bid}</h1>
        <div class="card" style="max-width:420px">
          <div class="card-body">
            {hinweis}
            <p style="font-size:13px;color:var(--muted);margin-bottom:12px">{anbieter_v}</p>
            <form method="post" action="/beleg/{bid}/betrag/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group">
                  <label>Betrag brutto</label>
                  <input type="number" step="0.01" name="betrag_brutto"
                         value="{betrag_v if betrag_v is not None else ''}" placeholder="z.B. 107.20">
                </div>
                <div class="form-group">
                  <label>Währung</label>
                  <input type="text" name="waehrung" value="{waehrung_v}" maxlength="3">
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/beleg/{bid}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Betrag – Beleg #{bid}", content))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/betrag/bearbeiten")
async def beleg_betrag_bearbeiten(bid: int, request: Request):
    form = await request.form()
    betrag_raw = (form.get("betrag_brutto") or "").strip().replace(",", ".")
    waehrung = (form.get("waehrung") or "EUR").strip().upper()[:3] or "EUR"
    zurueck = (form.get("zurueck") or "").strip()
    ziel = zurueck if re.fullmatch(r"/beleg/\d+", zurueck) else f"/beleg/{bid}"   # nur interne Beleg-Seiten
    betrag = None
    if betrag_raw:
        try: betrag = float(betrag_raw)
        except ValueError:
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Betrag ist keine gültige Zahl.</div>'
                f'<a href="/beleg/{bid}/betrag/bearbeiten" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        # Wenn ein Betrag manuell eingetragen wird, ist der Beleg vollständig
        if betrag is not None:
            cur.execute(f"UPDATE belege SET betrag_brutto={P}, waehrung={P}, "
                        f"pflichtfelder_ok=TRUE, fehlende_felder='[]', status='ok' WHERE id={P}",
                        (betrag, waehrung, bid))
            # Fremdwährung ohne Euro-Schätzung: Kurs vom Beleg derselben Buchungsgruppe übernehmen
            # (z. B. Teilrechnung 2 von 2), damit der Beleg nicht mit 0 € in der Summe steht
            if waehrung != "EUR" and betrag > 0:
                cur.execute(f"SELECT betrag_eur FROM belege WHERE id={P}", (bid,))
                r_eur = cur.fetchone()
                if r_eur and (r_eur[0] if not hasattr(r_eur, "keys") else r_eur["betrag_eur"]) is None:
                    cur.execute(f"""SELECT betrag_eur, betrag_brutto, kurs_eur FROM belege
                        WHERE beleg_gruppe_id=(SELECT beleg_gruppe_id FROM belege WHERE id={P})
                        AND id!={P} AND waehrung={P} AND betrag_brutto>0 AND betrag_eur IS NOT NULL
                        ORDER BY (kurs_eur IS NULL), id LIMIT 1""", (bid, bid, waehrung))
                    sib = cur.fetchone()
                    if sib:
                        sg = (lambda k, i: sib[k] if hasattr(sib, "keys") else sib[i])
                        cur.execute(f"UPDATE belege SET betrag_eur={P}, kurs_eur={P} WHERE id={P}",
                                    (round(betrag * float(sg("betrag_eur", 0)) / float(sg("betrag_brutto", 1)), 2),
                                     sg("kurs_eur", 2), bid))
        else:
            cur.execute(f"UPDATE belege SET betrag_brutto={P}, waehrung={P} WHERE id={P}",
                        (betrag, waehrung, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/beleg/{bid}/flugstatus/zuruecksetzen")
def beleg_flugstatus_zuruecksetzen(bid: int):
    """
    Löscht den gespeicherten Flug-/Bahnstatus (flug_status) für alle Segmente
    dieses Belegs. Für Fälle, in denen ein einmal gespeicherter Endstatus
    (z.B. "Arrived" mit einer falsch berechneten Verspätung) auf dem
    Dashboard hängen bleibt, weil ein bereits vergangener Flug/Zug laut
    Sicherheitslogik nicht mehr automatisch neu geprüft wird. Nach dem
    Zurücksetzen verschwindet die Meldung vom Dashboard; beim nächsten
    fälligen Checkpoint (oder manuellen Testlauf) wird ggf. neu geprüft.
    """
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"DELETE FROM flug_status WHERE beleg_id={P}", (bid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.get("/beleg/{bid}/segment/{idx}/umbuchen", response_class=HTMLResponse)
def beleg_segment_umbuchen_form(bid: int, idx: int):
    """Formular, um ein einzelnes Flug-/Bahnsegment nach einer Umbuchung
    manuell zu korrigieren (neue Fluggesellschaft/Zugnummer, neue Zeiten)."""
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT reise_code, transportart, ki_json FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Beleg nicht gefunden.</div>'))
        g = lambda row,k,i: row[k] if hasattr(row,'keys') else row[i]
        reise_code = g(r,"reise_code",0)
        typ = g(r,"transportart",1) or "Flug"
        ki_str = g(r,"ki_json",2) or "{}"
        try:
            segs = json.loads(ki_str).get("segmente") or []
        except Exception:
            segs = []
        if idx >= len(segs):
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Segment nicht gefunden.</div>'))
        s = segs[idx]

        def v(feld): return (s.get(feld) or "")

        content = f"""
        <h1 class="page-title">✏ Umbuchung eintragen</h1>
        <p style="color:var(--muted);font-size:13px;margin-bottom:16px">
          Beleg #{bid}, Segment {idx}. Änderungen werden in den Reiseplan, die Karte,
          die Flug-/Bahn-Überwachung und die VMA-Berechnung übernommen.</p>
        <div class="card" style="max-width:560px">
          <div class="card-body">
            <form method="post" action="/beleg/{bid}/segment/{idx}/umbuchen">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Gesellschaft ({"Airline" if typ=="Flug" else "Bahnunternehmen"})</label>
                  <input type="text" name="transport_name" class="inp" value="{v('transport_name')}">
                </div>
                <div class="form-group">
                  <label>Neue {"Flugnummer" if typ=="Flug" else "Zugnummer"}</label>
                  <input type="text" name="transport_nummer" class="inp" value="{v('transport_nummer')}" required>
                </div>
                <div class="form-group">
                  <label>{"IATA-Code" if typ=="Flug" else "Bahnhof"} – von</label>
                  <input type="text" name="von_ort" class="inp" value="{v('von_ort')}" required>
                </div>
                <div class="form-group">
                  <label>{"IATA-Code" if typ=="Flug" else "Bahnhof"} – nach</label>
                  <input type="text" name="nach_ort" class="inp" value="{v('nach_ort')}" required>
                </div>
                <div class="form-group">
                  <label>Sitzplatz (optional)</label>
                  <input type="text" name="sitzplatz" class="inp" value="{v('sitzplatz')}" placeholder="z.B. 12A oder Wagen 23, Platz 61">
                </div>
                <div class="form-group">
                  <label>Neues Abreisedatum</label>
                  <input type="text" name="abreise_datum" class="inp" placeholder="TT.MM.JJJJ" value="{v('abreise_datum')}" required>
                </div>
                <div class="form-group">
                  <label>Neue Abreisezeit</label>
                  <input type="time" name="abreise_zeit" class="inp" value="{v('abreise_zeit')}" required>
                </div>
                <div class="form-group">
                  <label>Neues Ankunftsdatum</label>
                  <input type="text" name="ankunft_datum" class="inp" placeholder="TT.MM.JJJJ" value="{v('ankunft_datum')}" required>
                </div>
                <div class="form-group">
                  <label>Neue Ankunftszeit</label>
                  <input type="time" name="ankunft_zeit" class="inp" value="{v('ankunft_zeit')}" required>
                </div>
              </div>
              <div class="form-hint" style="margin:10px 0">
                Zeitzonen-Offsets und Koordinaten werden zurückgesetzt und beim nächsten
                Check automatisch neu bestimmt (bei rein innerdeutschen Umbuchungen ohne Belang).
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern & VMA neu berechnen</button>
                <a href="/beleg/{bid}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell("Umbuchung", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


@app.post("/beleg/{bid}/segment/{idx}/umbuchen")
async def beleg_segment_umbuchen_speichern(bid: int, idx: int, request: Request):
    """Speichert die manuell korrigierten Umbuchungs-Daten im KI-JSON,
    verwirft den alten Flug-/Bahnstatus und berechnet die VMA der Reise neu."""
    form = await request.form()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT reise_code, ki_json FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Beleg nicht gefunden.</div>'))
        g = lambda row,k,i: row[k] if hasattr(row,'keys') else row[i]
        reise_code = g(r,"reise_code",0)
        ki = json.loads(g(r,"ki_json",1) or "{}")
        segs = ki.get("segmente") or []
        if idx >= len(segs):
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Segment nicht gefunden.</div>'))

        s = segs[idx]
        for feld in ("transport_name", "transport_nummer", "von_ort", "nach_ort",
                     "abreise_datum", "abreise_zeit", "ankunft_datum", "ankunft_zeit", "sitzplatz"):
            wert = (form.get(feld) or "").strip()
            if wert: s[feld] = wert
        # Zeitzonen-Offsets/Koordinaten/IATA-Codes zurücksetzen – die galten für
        # die ALTE Verbindung, bei einer Umbuchung ggf. nicht mehr korrekt.
        # Rückfall-Logik (MESZ, Städtenamen-Erkennung) greift automatisch weiter.
        for feld in ("abreise_utc_offset", "ankunft_utc_offset",
                     "von_iata", "nach_iata", "von_lat", "von_lon", "nach_lat", "nach_lon"):
            s[feld] = None
        ki["segmente"][idx] = s

        cur.execute(f"UPDATE belege SET ki_json={P} WHERE id={P}",
                    (json.dumps(ki, ensure_ascii=False), bid))
        _event_daten_in_db_aktualisieren(cur, bid, ki)
        # Alten (jetzt veralteten) Status für dieses Segment verwerfen
        cur.execute(f"DELETE FROM flug_status WHERE beleg_id={P} AND segment_index={P}", (bid, idx))
        db.commit(); cur.close(); db.close()

        # VMA der betroffenen Reise neu berechnen, falls sich Datum/Land geändert hat
        if reise_code:
            db2 = get_db()
            try:
                vma_tage_generieren(reise_code, db2)
            finally:
                db2.close()

        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


def _event_daten_in_db_aktualisieren(cur, bid: int, ki: dict) -> None:
    """Setzt event_datum_von/bis eines Flug-/Bahn-/Mietwagen-Belegs aus seinen
    Segmenten neu. Wichtig, weil z.B. die VMA-Länderermittlung Flüge über diese
    beiden Spalten findet – nach einer Umbuchung auf ein anderes Datum würde der
    Beleg sonst am neuen Tag nicht mehr gefunden."""
    daten = []
    for s in ki.get("segmente") or []:
        for k in ("abreise_datum", "ankunft_datum"):
            d = _datum_parsen(s.get(k))
            if d: daten.append(d)
    if not daten:
        return
    P = ph()
    cur.execute(f"""UPDATE belege SET event_datum_von={P}, event_datum_bis={P}
                    WHERE id={P} AND transportart IN ('Flug','Bahn','Mietwagen')""",
                (min(daten).isoformat(), max(daten).isoformat(), bid))


def _aenderung_panel_html(bid: int, status, zu_id, diff_json) -> str:
    """Vergleichsansicht für einen Änderungs-Beleg: je Unterschied alt/neu/eigener Wert."""
    from html import escape as e
    if not status:
        return ""
    if status != "offen":
        txt = "übernommen" if status == "uebernommen" else "verworfen"
        return ('<div class="alert" style="background:#f1f5f9;border:1px solid #cbd5e1;color:#475569;'
                'padding:12px 16px;border-radius:8px;margin-bottom:16px">'
                f'Änderungs-Beleg zu <a href="/beleg/{zu_id}">Beleg #{zu_id}</a> – Änderungen wurden {txt}.</div>')
    try:
        diffs = json.loads(diff_json or "[]")
    except Exception:
        diffs = []
    zeilen = ""
    for i, d in enumerate(diffs):
        typ = d.get("typ")
        neu_chk = "" if typ == "entfaellt" else " checked"
        alt_chk = " checked" if typ == "entfaellt" else ""
        eigen = ""
        if typ == "aenderung":
            eigen = ('<br><input type="text" name="eigen_%d" placeholder="oder eigener Wert" '
                     'style="width:140px;font-size:12px;margin-top:4px">' % i)
        zeilen += (
            '<tr style="border-top:1px solid #fed7aa">'
            f'<td style="padding:6px 8px;font-size:13px">{e(str(d.get("label", "")))}</td>'
            f'<td style="padding:6px 8px;background:#fef2f2;font-size:13px">{e(str(d.get("alt") or "–"))}</td>'
            f'<td style="padding:6px 8px;background:#f0fdf4;font-size:13px;font-weight:600">{e(str(d.get("neu") or "–"))}</td>'
            '<td style="padding:6px 8px;font-size:12px;white-space:nowrap">'
            f'<label><input type="radio" name="wahl_{i}" value="neu"{neu_chk}> neu</label> &nbsp;'
            f'<label><input type="radio" name="wahl_{i}" value="alt"{alt_chk}> alt</label>{eigen}</td></tr>')
    if not zeilen:
        zeilen = '<tr><td colspan="4" style="padding:8px">Keine Unterschiede gespeichert.</td></tr>'
    return (
        '<div class="card" style="margin-bottom:16px;border:2px solid #fb923c">'
        '<div class="card-header"><span class="card-title">⚠ Änderung zu '
        f'<a href="/beleg/{zu_id}">Beleg #{zu_id}</a> – bitte prüfen</span></div>'
        '<div class="card-body">'
        '<p style="font-size:13px;color:#9a3412;margin:0 0 10px 0">Dieses Dokument weicht vom bereits erfassten Beleg ab. '
        'Es zählt in Reiseplan, Karte und Kosten <b>nicht</b>, bis entschieden ist, welche Werte stimmen.</p>'
        f'<form method="post" action="/beleg/{bid}/aenderung/anwenden">'
        '<div class="table-wrap"><table style="width:100%;border-collapse:collapse"><thead><tr style="text-align:left;font-size:12px">'
        '<th style="padding:6px 8px">Was</th><th style="padding:6px 8px">Bisher</th>'
        '<th style="padding:6px 8px">Neu im Dokument</th><th style="padding:6px 8px">Wert nehmen</th></tr></thead>'
        f'<tbody>{zeilen}</tbody></table></div>'
        '<div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap">'
        '<button type="submit" class="btn btn-primary">✅ Auswahl übernehmen</button></div></form>'
        f'<form method="post" action="/beleg/{bid}/aenderung/verwerfen" style="margin-top:8px" '
        'onsubmit="return confirm(\'Alle Änderungen verwerfen und den bisherigen Beleg unverändert lassen?\')">'
        '<button type="submit" class="btn btn-secondary">✖ Alles verwerfen (Bisheriges bleibt)</button></form>'
        '</div></div>')


def _aenderung_hinweis_fuer_original_html(bid: int) -> str:
    """Hinweis am ORIGINAL-Beleg, wenn zu ihm noch ungeklärte Änderungen vorliegen."""
    try:
        P = ph(); db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT id FROM belege WHERE aenderung_zu_beleg_id={P} AND aenderung_status='offen'", (bid,))
        ids = [(r[0] if isinstance(r, tuple) else r["id"]) for r in cur.fetchall()]
        cur.close(); db.close()
    except Exception:
        return ""
    if not ids:
        return ""
    links = ", ".join(f'<a href="/beleg/{i}" style="color:#9a3412;font-weight:700">#{i}</a>' for i in ids)
    return ('<div class="alert" style="background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;'
            'padding:12px 16px;border-radius:8px;margin-bottom:16px">'
            f'⚠ Zu diesem Beleg liegen <b>ungeklärte Änderungen</b> vor (Beleg {links}) – '
            'bitte dort die richtigen Werte auswählen.</div>')


@app.post("/beleg/{bid}/aenderung/anwenden")
async def beleg_aenderung_anwenden(bid: int, request: Request):
    """Übernimmt die gewählten Werte eines Änderungs-Belegs in den Original-Beleg."""
    form = await request.form()
    try:
        P = ph(); db = get_db(); cur = db.cursor()
        g = lambda row, k, i: row[k] if hasattr(row, 'keys') else row[i]
        cur.execute(f"SELECT aenderung_zu_beleg_id, aenderung_diff_json, aenderung_status FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        if not r or g(r, "aenderung_status", 2) != "offen":
            cur.close(); db.close()
            return RedirectResponse(f"/beleg/{bid}", status_code=303)
        orig_id = g(r, "aenderung_zu_beleg_id", 0)
        diffs = json.loads(g(r, "aenderung_diff_json", 1) or "[]")
        cur.execute(f"SELECT reise_code, ki_json FROM belege WHERE id={P}", (orig_id,))
        o = cur.fetchone()
        if not o:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Original-Beleg nicht gefunden.</div>'))
        reise_code = g(o, "reise_code", 0)
        ki = json.loads(g(o, "ki_json", 1) or "{}")
        segs = ki.get("segmente") or []
        # Geografie-/Zeitzonen-Zwischenwerte gelten nur für die alte Strecke
        GEO = ("abreise_utc_offset", "ankunft_utc_offset", "von_iata", "nach_iata",
               "von_lat", "von_lon", "nach_lat", "nach_lon")
        spalten, entfernen = {}, []
        for i, d in enumerate(diffs):
            typ, pfad = d.get("typ"), d.get("pfad", "")
            wahl = form.get(f"wahl_{i}") or ("alt" if typ == "entfaellt" else "neu")
            eigen = (form.get(f"eigen_{i}") or "").strip()
            if typ == "aenderung":
                if eigen: wert = eigen
                elif wahl == "neu": wert = d.get("neu")
                else: continue
                if pfad.startswith("seg:"):
                    _, oi, feld = pfad.split(":", 2); oi = int(oi)
                    if oi < len(segs):
                        segs[oi][feld] = wert
                        if feld in ("von_ort", "nach_ort"):
                            for gf in GEO: segs[oi][gf] = None
                elif pfad.startswith("feld:"):
                    feld = pfad.split(":", 1)[1]
                    ki[feld] = wert; spalten[feld] = wert
            elif typ == "neu_segment" and wahl == "neu":
                segs.append(d.get("neu_segment") or {})
            elif typ == "entfaellt" and wahl == "neu":
                entfernen.append(int(pfad.split(":", 1)[1]))
        for oi in sorted(set(entfernen), reverse=True):
            if oi < len(segs): segs.pop(oi)
        if segs or "segmente" in ki:
            ki["segmente"] = segs

        set_teile, werte = [f"ki_json={P}"], [json.dumps(ki, ensure_ascii=False)]
        TEXT_SPALTEN = ("hotel_name", "hotel_adresse", "hotel_checkin_zeit", "hotel_checkout_zeit", "waehrung")
        DATUM_SPALTEN = ("event_datum_von", "event_datum_bis", "hotel_checkin_datum", "hotel_checkout_datum")
        for feld, wert in spalten.items():
            if feld in TEXT_SPALTEN:
                set_teile.append(f"{feld}={P}"); werte.append(wert)
            elif feld == "betrag_brutto":
                try:
                    werte.append(float(str(wert).replace(",", "."))); set_teile.append(f"{feld}={P}")
                except ValueError:
                    pass
            elif feld in DATUM_SPALTEN:
                dd = _datum_parsen(wert)
                if dd:
                    set_teile.append(f"{feld}={P}"); werte.append(dd.isoformat())
        cur.execute("UPDATE belege SET " + ", ".join(set_teile) + f" WHERE id={P}", tuple(werte) + (orig_id,))
        if segs:
            _event_daten_in_db_aktualisieren(cur, orig_id, ki)
        # Alter Flug-/Bahnstatus gilt für die alten Zeiten nicht mehr
        cur.execute(f"DELETE FROM flug_status WHERE beleg_id={P}", (orig_id,))
        cur.execute(f"UPDATE belege SET aenderung_status='uebernommen' WHERE id={P}", (bid,))
        db.commit(); cur.close(); db.close()

        if reise_code:
            db2 = get_db()
            try:
                vma_tage_generieren(reise_code, db2)
            finally:
                db2.close()
        return RedirectResponse(f"/beleg/{orig_id}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


@app.post("/beleg/{bid}/aenderung/verwerfen")
def beleg_aenderung_verwerfen(bid: int):
    try:
        P = ph(); db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE belege SET aenderung_status='verworfen' WHERE id={P} AND aenderung_status='offen'", (bid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.get("/beleg/{bid}/loeschen")
def beleg_loeschen(bid: int):
    """Löscht einen Beleg unwiderruflich aus der Datenbank (Dateien in S3 bleiben)."""
    sperre = _bewirtung_sperre(bid) or _vorschuss_sperre(bid)
    if sperre:
        return HTMLResponse(shell("Löschen nicht möglich",
            f'<div class="alert alert-err">{sperre}</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">← Zurück zum Beleg</a>'), status_code=409)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT reise_code FROM belege WHERE id={P}", (bid,))
        rc_row = cur.fetchone()
        rc_alt = (rc_row[0] if isinstance(rc_row, tuple) else rc_row["reise_code"]) if rc_row else None
        cur.execute(f"DELETE FROM belege WHERE id={P}", (bid,))
        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(rc_alt)    # ein gelöschter Hotel-/Flugbeleg ändert Land und Satz
        return RedirectResponse("/belege", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/neu-analysieren")
async def beleg_neu_analysieren_route(bid: int):
    """Führt die KI-Analyse für diesen Beleg erneut aus (z.B. um event_zeit nachzuladen)."""
    result = await beleg_neu_analysieren(bid)
    if result.get("fehler"):
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{result["fehler"]}</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">Zurück</a>'))
    return RedirectResponse(f"/beleg/{bid}", status_code=303)

@app.post("/beleg/{bid}/neu-anonymisieren")
async def beleg_neu_anonymisieren_route(bid: int):
    """Führt die Anonymisierung für diesen Beleg erneut aus (z.B. nach Bugfix)."""
    result = await beleg_neu_anonymisieren(bid)
    if result.get("fehler"):
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{result["fehler"]}</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">Zurück</a>'))
    return RedirectResponse(f"/beleg/{bid}", status_code=303)

@app.post("/beleg/{bid}/pruefen")
async def beleg_pruefen(bid: int, request: Request):
    """Markiert einen Umsatzbeleg als geprüft, mit Prüfvermerk. Person + Zeitstempel
    kommen automatisch aus dem eingeloggten Benutzer."""
    form = await request.form()
    vermerk = (form.get("pruef_vermerk") or "").strip() or None
    geprueft_von = request.session.get("kuerzel") or "?"
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        if is_postgres():
            cur.execute(f"UPDATE belege SET geprueft=TRUE, pruef_vermerk={P}, "
                        f"geprueft_von={P}, geprueft_am=NOW() WHERE id={P}",
                        (vermerk, geprueft_von, bid))
        else:
            cur.execute(f"UPDATE belege SET geprueft=1, pruef_vermerk={P}, "
                        f"geprueft_von={P}, geprueft_am=datetime('now') WHERE id={P}",
                        (vermerk, geprueft_von, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/verknuepfen/{andere_id}")
def beleg_verknuepfen(bid: int, andere_id: int):
    """
    Fügt zwei Belege zu einer gemeinsamen Gruppe zusammen (z.B. Buchungsbestätigung
    + Rechnung, oder auch 3+ zusammengehörige Belege einer Buchung).
    - Hat noch keiner der beiden eine Gruppe → neue Gruppe für beide anlegen.
    - Hat einer schon eine Gruppe → der andere tritt dieser Gruppe bei.
    - Haben beide schon (unterschiedliche) Gruppen → Gruppen werden zusammengeführt.
    """
    try:
        db = get_db(); cur = db.cursor()
        deckung_mod.gruppe_zusammenfuehren(db, cur, bid, andere_id)
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/verknuepfen-mehrere")
async def beleg_verknuepfen_mehrere(bid: int, request: Request):
    """Verknüpft den Beleg mit mehreren ausgewählten Belegen DERSELBEN Reise auf einmal
    (z.B. Buchungsbestätigung 300 € mit zwei Rechnungen über 120 € und 180 €)."""
    form = await request.form()
    ids = []
    for x in form.getlist("ids"):
        try:
            ids.append(int(x))
        except (TypeError, ValueError):
            pass
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT reise_code FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        rcode = (r[0] if isinstance(r, tuple) else r["reise_code"]) if r else None
        if not rcode:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Beleg muss zuerst einer Reise zugeordnet sein.</div>'))
        for andere in sorted(set(ids) - {bid}):
            cur.execute(f"SELECT reise_code FROM belege WHERE id={P}", (andere,))
            ra = cur.fetchone()
            if ra and (ra[0] if isinstance(ra, tuple) else ra["reise_code"]) == rcode:
                deckung_mod.gruppe_zusammenfuehren(db, cur, bid, andere)
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/aus-gruppe-entfernen/{ziel_id}")
def beleg_aus_gruppe_entfernen(bid: int, ziel_id: int):
    """Entfernt EIN Mitglied (ziel_id) aus der Gruppe – der Rest der Gruppe bleibt bestehen."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE belege SET beleg_gruppe_id=NULL WHERE id={P}", (ziel_id,))
        # Bleibt nur noch 1 Mitglied in der Gruppe übrig, Gruppe komplett auflösen
        cur.execute(f"SELECT beleg_gruppe_id FROM belege WHERE id={P}", (bid,))
        row = cur.fetchone()
        gid = (row[0] if isinstance(row, tuple) else row["beleg_gruppe_id"]) if row else None
        if gid:
            cur.execute(f"SELECT COUNT(*) FROM belege WHERE beleg_gruppe_id={P}", (gid,))
            anzahl = cur.fetchone()[0]
            if anzahl <= 1:
                cur.execute(f"UPDATE belege SET beleg_gruppe_id=NULL WHERE beleg_gruppe_id={P}", (gid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Habel-Übergabe: EINE E-Mail je Reise (Reisekostenabrechnung + jeder Beleg einzeln als PDF) ───────────
# Gesendet wird von der Reise-Seite aus (Knopf neben „Abschluss“). Vorher prüft die Seite, ob alle Belege
# geprüft sind; sonst ist das Senden gesperrt. Welche Belege mitgehen, bestimmt PRUEF_BELEGARTEN.

def _datei_ascii(text, maxlen: int) -> str:
    import unicodedata
    t = str(text or "").translate(str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "Ä": "Ae", "Ö": "Oe", "Ü": "Ue", "ß": "ss"}))
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    t = re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_")
    return t[:maxlen].strip("_") or "Beleg"


_ART_KURZ = {"Bewirtungsbeleg": "Bewirtung", "Vorschussquittung": "Vorschuss", "Rueckgabequittung": "Rueckgabe",
            "Bargeldbeschaffung": "Bargeld"}


def habel_paket_pruefen(rcode: str) -> dict:
    """Liest, welche Belege der Reise an Habel gehen und ob alles bereit ist (ändert nichts)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT code, titel, abreise, rueckkehr FROM reisen WHERE code={P}", (rcode,))
        r = cur.fetchone()
        if not r:
            return {"reise": None}
        g = lambda row, k, i: row[k] if hasattr(row, "keys") else row[i]
        reise = {"code": g(r, "code", 0), "titel": g(r, "titel", 1) or "", "abreise": g(r, "abreise", 2),
                 "rueckkehr": g(r, "rueckkehr", 3)}
        cur.execute(f"""SELECT m.klarname FROM mitarbeiter m JOIN reise_mitarbeiter rm ON rm.kuerzel=m.kuerzel
                        WHERE rm.reise_code={P} ORDER BY m.klarname""", (rcode,))
        namen = [g(x, "klarname", 0) for x in cur.fetchall()]
        cur.execute(f"""SELECT id, belegart, anbieter, belegdatum, betrag_brutto, waehrung, geprueft, s3_original,
                        dms_versendet_am, habel_einzel_am FROM belege WHERE reise_code={P} AND belegart IN {PRUEF_SQL}
                        ORDER BY belegdatum NULLS LAST, id""", (rcode,))
        zeilen = cur.fetchall()
    finally:
        cur.close(); db.close()
    belege = []
    for nr, z in enumerate(zeilen, start=1):
        bid, art, anb = g(z, "id", 0), g(z, "belegart", 1) or "", g(z, "anbieter", 2) or ""
        datum = str(g(z, "belegdatum", 3) or "")[:10]
        belege.append({
            "id": bid, "art": art, "anbieter": anb, "datum": datum, "betrag": g(z, "betrag_brutto", 4),
            "waehrung": g(z, "waehrung", 5) or "EUR", "geprueft": bool(g(z, "geprueft", 6)),
            "original": bool(g(z, "s3_original", 7)), "gesendet_am": g(z, "dms_versendet_am", 8),
            "einzel_am": g(z, "habel_einzel_am", 9),
            "name": f"{nr:02d}_{_datei_ascii(_ART_KURZ.get(art, art), 14)}_{_datei_ascii(anb, 22)}_{datum}_Nr{bid}.pdf".replace("__", "_")})
    offen = [b for b in belege if not b["geprueft"]]
    ohne_original = [b for b in belege if b["geprueft"] and not b["original"]]
    try:   # Buchungen ohne (vollständige) Rechnung/Quittung: nur ein Hinweis, kein Sendehindernis
        luecken_h = deckung_mod.luecken(deckung_mod.belege_der_reise(rcode))
    except Exception:
        luecken_h = []
    return {"reise": reise, "mitarbeiter": namen, "belege": belege, "offen": offen, "ohne_original": ohne_original,
            "luecken": luecken_h, "bereits": bool(belege) and all(b["gesendet_am"] for b in belege)}


def habel_hindernisse(info: dict) -> list:
    """Gründe, warum jetzt nicht gesendet werden darf (leer = bereit)."""
    h = []
    if not dms_konfiguriert():
        h.append("Der Mail-Versand an Habel ist nicht eingerichtet (SMTP_HOST, SMTP_USER, SMTP_PASS oder DMS_EMAIL_TO fehlen).")
    if info["offen"]:
        h.append("Es sind noch nicht alle Belege geprüft (%d offen: %s)."
                 % (len(info["offen"]), ", ".join("#%s" % b["id"] for b in info["offen"])))
    if info["ohne_original"]:
        h.append("Für diese Belege ist kein Original-Dokument gespeichert: %s."
                 % ", ".join("#%s" % b["id"] for b in info["ohne_original"]))
    return h


def _zeitstempel_text(v) -> str:
    """Zeitstempel aus der Datenbank als „19.10.2026, 16:30 Uhr“."""
    if not v:
        return "–"
    if isinstance(v, str):
        try:
            from datetime import datetime as _dt
            v = _dt.fromisoformat(v[:19])
        except Exception:
            return v[:16]
    return v.strftime("%d.%m.%Y, %H:%M Uhr")


# ── Einzelversand an Habel: nur von Hand, nur Rechnungen und Quittungen ───────────────────────────────────
EINZEL_ARTEN = ("Rechnung", "Quittung")


def spalte_sichern(tabelle: str, spalte: str, typ: str) -> None:
    """Legt eine neue Spalte an, falls /init noch nicht gelaufen ist (ein Neustart genügt). Ändert sonst nichts.
    Nur feste Namen aus dem Programm übergeben (kein Benutzereingabe-Text)."""
    try:
        db = get_db(); cur = db.cursor()
        if is_postgres():
            cur.execute(f"ALTER TABLE {tabelle} ADD COLUMN IF NOT EXISTS {spalte} {typ}")
        else:
            cur.execute(f"PRAGMA table_info({tabelle})")
            if spalte not in {(r["name"] if hasattr(r, "keys") else r[1]) for r in cur.fetchall()}:
                cur.execute(f"ALTER TABLE {tabelle} ADD COLUMN {spalte} {typ}")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[Startup] Spalte {tabelle}.{spalte} nicht angelegt (bitte /init aufrufen): {ex}")


def habel_einzel_spalte_sichern():
    """Spalte für den Einzelversand an Habel (belege.habel_einzel_am)."""
    spalte_sichern("belege", "habel_einzel_am", "TIMESTAMP")


def habel_einzel_hindernisse(b: dict) -> list:
    """Gründe, warum dieser Beleg jetzt nicht einzeln gesendet werden darf (leer = bereit)."""
    h = []
    if b["art"] not in EINZEL_ARTEN:
        h.append("Einzeln gesendet werden nur Rechnungen und Quittungen.")
    if not dms_konfiguriert():
        h.append("Der Mail-Versand an Habel ist nicht eingerichtet (SMTP_HOST, SMTP_USER, SMTP_PASS oder DMS_EMAIL_TO fehlen).")
    if not b["geprueft"]:
        h.append("Der Beleg ist noch nicht geprüft.")
    elif not b["original"]:
        h.append("Für den Beleg ist kein Original-Dokument gespeichert.")
    return h


def habel_einzel_karte_html(rcode: str) -> str:
    """Karte auf der Reise-Seite: Rechnungen und Quittungen der Reise mit Knopf „Einzeln senden“ (nur von Hand)."""
    from html import escape as e
    try:
        info = habel_paket_pruefen(rcode)
    except Exception:
        return ""
    liste = [b for b in info.get("belege", []) if b["art"] in EINZEL_ARTEN]
    if not liste:
        return ""
    konfiguriert = dms_konfiguriert()
    zeilen = ""
    for b in liste:
        bet = f'{float(b["betrag"]):.2f} {e(b["waehrung"])}' if b["betrag"] else "–"
        if not b["geprueft"]:
            aktion = f'<a href="/beleg/{b["id"]}" style="color:#b91c1c;font-size:12px">✗ erst prüfen</a>'
        elif not b["original"]:
            aktion = '<span style="color:#b91c1c;font-size:12px">✗ kein Original</span>'
        elif not konfiguriert:
            aktion = ('<button type="button" class="btn btn-secondary btn-sm" disabled style="opacity:.4;cursor:not-allowed" '
                      'title="Mail-Versand an Habel ist nicht eingerichtet">📤 Einzeln senden</button>')
        else:
            schon = bool(b["einzel_am"] or b["gesendet_am"])
            frage = (f"Beleg #{b['id']} wurde schon an Habel gesendet. Trotzdem erneut einzeln senden?" if schon
                     else f"Beleg #{b['id']} jetzt einzeln an Habel senden?")
            aktion = (f'<form method="post" action="/reise/{e(rcode)}/habel/beleg/{b["id"]}/senden" style="margin:0" '
                      f'onsubmit="return confirm(\'{frage}\')">'
                      f'<input type="hidden" name="erneut" value="{"1" if schon else ""}">'
                      f'<button type="submit" class="btn btn-secondary btn-sm">📤 {"Erneut einzeln senden" if schon else "Einzeln senden"}</button></form>')
        stand = []
        if b["einzel_am"]:
            stand.append(f'einzeln gesendet {e(_zeitstempel_text(b["einzel_am"]))}')
        if b["gesendet_am"]:
            stand.append(f'mit der Reise gesendet {e(_zeitstempel_text(b["gesendet_am"]))}')
        zeilen += (f'<tr><td><a href="/beleg/{b["id"]}">#{b["id"]}</a></td><td>{e(b["art"])}</td>'
                   f'<td>{e(b["anbieter"] or "–")}</td><td>{fmt_date(b["datum"]) if b["datum"] else "–"}</td>'
                   f'<td style="text-align:right">{bet}</td>'
                   f'<td style="font-size:12px;color:var(--muted)">{" · ".join(stand) or "–"}</td>'
                   f'<td style="text-align:right">{aktion}</td></tr>')
    return ('<div class="card" style="margin-top:16px"><div class="card-header">'
            '<span class="card-title">📤 Belege einzeln an Habel senden</span></div>'
            '<div class="card-body" style="padding-bottom:0">'
            '<p style="font-size:12px;color:var(--muted);margin:0 0 8px 0">Nur von Hand, nur Rechnungen und Quittungen, '
            'nur geprüfte Belege. Jeder Knopf schickt <b>eine</b> Mail mit diesem Beleg (mit Prüf-Deckblatt) an '
            f'<b>{e(DMS_EMAIL_TO or "– nicht eingerichtet –")}</b>. Die Übergabe der ganzen Reise (Knopf „An Habel“ oben) '
            'enthält diese Belege trotzdem noch einmal.</p></div>'
            '<div class="table-wrap"><table><thead><tr><th>Beleg</th><th>Art</th><th>Anbieter</th><th>Datum</th>'
            '<th style="text-align:right">Betrag</th><th>Stand</th><th></th></tr></thead>'
            f'<tbody>{zeilen}</tbody></table></div></div>')


def habel_beleg_block_html(bid: int, rcode: str, gesendet_text: str = "") -> str:
    """Habel-Info am einzelnen Beleg. Einzelne Belege gehen nicht an Habel: Die Übergabe erfolgt einmal für die
    ganze Reise, sobald alle prüfpflichtigen Belege geprüft sind. Der Block zeigt den Stand der Reise.
    Rechnungen und Quittungen lassen sich zusätzlich von Hand einzeln senden (Karte auf der Reise-Seite)."""
    from html import escape as e
    try:
        info = habel_paket_pruefen(rcode)
    except Exception:
        return ""
    if not info.get("reise"):
        return ""
    n = len(info["belege"]); n_ok = sum(1 for b in info["belege"] if b["geprueft"])
    prozent = int(round(100 * n_ok / n)) if n else 0
    bereit = n > 0 and not info["offen"] and not info["ohne_original"]
    def link(b):
        mark = " (dieser Beleg)" if b["id"] == bid else ""
        return f'<a href="/beleg/{b["id"]}" style="color:#2563eb">#{b["id"]}</a>{mark}'
    einzel = next((b.get("einzel_am") for b in info["belege"] if b["id"] == bid), None)
    einzel_html = ""
    if einzel:
        einzel_html = ('<div class="alert alert-ok" style="font-size:12px;margin:8px 0">📤 Einzeln an Habel gesendet am '
                       f'<b>{e(_zeitstempel_text(einzel))}</b></div>')
    if gesendet_text:
        status = ('<div class="alert alert-ok" style="font-size:12px;margin:8px 0">📤 Mit der Reise an Habel gesendet am '
                  f'<b>{e(gesendet_text)}</b></div>')
    elif bereit:
        status = ('<div class="alert alert-ok" style="font-size:12px;margin:8px 0">✓ Alle Belege der Reise sind geprüft. '
                  'Die Reise kann jetzt an Habel übergeben werden.</div>')
    else:
        punkte = ""
        if info["offen"]:
            punkte += ('<div>• %d Beleg%s noch nicht geprüft: %s</div>'
                       % (len(info["offen"]), "" if len(info["offen"]) == 1 else "e", ", ".join(link(b) for b in info["offen"])))
        if info["ohne_original"]:
            punkte += '<div>• Kein Original gespeichert: %s</div>' % ", ".join(link(b) for b in info["ohne_original"])
        if not punkte:
            punkte = "<div>• Die Reise hat noch keine prüfpflichtigen Belege.</div>"
        status = ('<div class="alert alert-warn" style="font-size:12px;margin:8px 0"><b>Übergabe noch nicht möglich:</b>%s</div>' % punkte)
    knopf_klasse = "btn-primary" if (bereit and not gesendet_text) else "btn-secondary"
    return (
        f'<div style="font-weight:700;font-size:13px;margin-bottom:4px">📤 Übergabe an Habel – Reise {e(rcode)}</div>'
        '<div style="font-size:12px;color:var(--muted);margin-bottom:8px">Die Übergabe an Habel erfolgt für die ganze Reise: '
        'Sind alle Belege geprüft, geht sie in <b>einer</b> Mail raus (Reisekostenabrechnung und jeder Beleg als eigenes PDF). '
        'Einzelne Rechnungen und Quittungen können Sie auf der Reise-Seite von Hand senden.</div>'
        '<div style="background:var(--bg);border-radius:6px;height:8px;overflow:hidden">'
        f'<div style="width:{prozent}%;height:8px;background:#16a34a"></div></div>'
        f'<div style="font-size:12px;margin-top:4px">{n_ok} von {n} Belegen der Reise geprüft</div>'
        f'{status}{einzel_html}'
        f'<a href="/reise/{e(rcode)}/habel" class="btn {knopf_klasse}" '
        'style="width:100%;text-align:center;display:block;margin-top:8px">'
        f'📤 {"Zur Übergabe an Habel" if (bereit and not gesendet_text) else "Übergabe ansehen"}</a>')


def habel_reise_status_html(code: str) -> str:
    """Kurzer Habel-Stand einer Reise (für die Liste „Offene Aufgaben“)."""
    try:
        info = habel_paket_pruefen(code)
    except Exception:
        return "–"
    if not info.get("reise") or not info["belege"]:
        return '<span style="color:var(--muted)">keine prüfpflichtigen Belege</span>'
    n = len(info["belege"]); n_ok = sum(1 for b in info["belege"] if b["geprueft"])
    if info["bereits"]:
        return '<span style="color:#166534">✓ an Habel gesendet</span>'
    if n_ok == n and not info["ohne_original"]:
        return '<span style="color:#166534;font-weight:600">✓ bereit zur Übergabe</span>'
    return f'<span style="color:#b45309">{n_ok} von {n} Belegen geprüft</span>'


def habel_paket_bauen(info: dict, grenze_pruefen: bool = True) -> list:
    """Erzeugt die Mail-Anhänge [(bytes, dateiname)]: Reisekostenabrechnung + jeder Beleg einzeln (mit Prüf-Deckblatt).
    Wirft ValueError mit lesbarer Meldung."""
    rcode = info["reise"]["code"]
    namen = {b["id"]: b["name"] for b in info["belege"]}
    antwort = abschluss_pdf_antwort(rcode, mit_originalen=False, anlagen_namen=namen)
    if getattr(antwort, "media_type", "") != "application/pdf":
        raise ValueError("Die Reisekostenabrechnung konnte nicht erzeugt werden.")
    anhaenge = [(antwort.body, f"Reisekostenabrechnung_{rcode}.pdf")]
    for b in info["belege"]:
        try:
            anhaenge.append((pruef_pdf_fuer_beleg(b["id"]), b["name"]))
        except Exception as e:
            raise ValueError(f"Beleg #{b['id']} konnte nicht vorbereitet werden: {e}")
    groesse = sum(len(x[0]) for x in anhaenge)
    if grenze_pruefen and groesse > HABEL_MAX_MB * 1024 * 1024:
        raise ValueError(f"Die Mail wäre {groesse / 1048576:.1f} MB groß (Grenze {HABEL_MAX_MB:g} MB). "
                         "Bitte besonders große Belege verkleinern oder die Grenze (HABEL_MAX_MB) anheben.")
    return anhaenge


def habel_mail_inhalt(info: dict) -> tuple:
    """Betreff und Text der Habel-Mail (wird beim Senden und in der Vorschau gleich verwendet)."""
    reise = info["reise"]; rcode = reise["code"]; n = len(info["belege"])
    abr_name = f"Reisekostenabrechnung_{rcode}.pdf"
    liste = "\n".join(" - %s  (%s, %s, %s, %s)" % (
        b["name"], b["art"], b["anbieter"] or "–", fmt_date(b["datum"]) if b["datum"] else "ohne Datum",
        ("%.2f %s" % (float(b["betrag"]), b["waehrung"])) if b["betrag"] else "ohne Betrag") for b in info["belege"])
    betreff = "Reisekostenabrechnung %s – %s (%d Beleg%s)" % (rcode, reise["titel"], n, "" if n == 1 else "e")
    text = ("Reisekostenabrechnung %s – %s\nZeitraum: %s – %s\nReisende: %s\n\n"
            "Anhänge (%d PDF-Dateien):\n - %s  (Abrechnung)\n%s\n\n"
            "Alle Belege sind geprüft (Prüf-Deckblatt im jeweiligen PDF).\n"
            % (rcode, reise["titel"], fmt_date(reise["abreise"]), fmt_date(reise["rueckkehr"]),
               ", ".join(info["mitarbeiter"]) or "–", n + 1, abr_name, liste))
    return betreff, text


def _habel_seite(info: dict, fehler: list | None = None, ok: bool = False, versendet: int = 0) -> str:
    from html import escape as e
    reise = info["reise"]; rcode = reise["code"]
    hind = habel_hindernisse(info)
    zeilen = ""
    for b in info["belege"]:
        pruef = ('<span style="color:#166534">✓ geprüft</span>' if b["geprueft"] else
                 f'<a href="/beleg/{b["id"]}" style="color:#b91c1c;font-weight:600">✗ nicht geprüft – jetzt prüfen</a>')
        if b["geprueft"] and not b["original"]:
            pruef = '<span style="color:#b91c1c">✗ kein Original gespeichert</span>'
        bet = f'{float(b["betrag"]):.2f} {e(b["waehrung"])}' if b["betrag"] else "–"
        gesendet = fmt_date(b["gesendet_am"]) if b["gesendet_am"] else "–"
        zeilen += (f'<tr><td><a href="/beleg/{b["id"]}">#{b["id"]}</a></td><td>{e(b["art"])}</td>'
                   f'<td>{e(b["anbieter"] or "–")}</td><td>{fmt_date(b["datum"]) if b["datum"] else "–"}</td>'
                   f'<td style="text-align:right">{bet}</td><td>{pruef}</td>'
                   f'<td style="font-size:12px;color:var(--muted)">{gesendet}</td></tr>')
    if not zeilen:
        zeilen = ('<tr><td colspan="7"><div class="empty-state">Keine Belege zum Senden. '
                  'Es geht nur die Reisekostenabrechnung raus.</div></td></tr>')
    n = len(info["belege"]); n_ok = sum(1 for b in info["belege"] if b["geprueft"])
    banner = ""
    if ok:
        banner += ('<div class="alert alert-ok" style="margin-bottom:16px">✓ Gesendet an <b>%s</b>: '
                   'Reisekostenabrechnung und %d Beleg%s als einzelne PDF-Dateien in einer E-Mail.</div>'
                   % (e(DMS_EMAIL_TO or "Habel"), n, "" if n == 1 else "e"))
    if fehler:
        banner += ('<div class="alert alert-err" style="margin-bottom:16px"><b>Nicht gesendet:</b>%s</div>'
                   % "".join("<div>• %s</div>" % e(x) for x in fehler))
    elif hind and not ok:
        banner += ('<div class="alert alert-warn" style="margin-bottom:16px"><b>Senden noch gesperrt:</b>%s</div>'
                   % "".join("<div>• %s</div>" % e(x) for x in hind))
    if info.get("luecken") and not ok:
        banner += ('<div class="alert alert-warn" style="margin-bottom:16px"><b>Hinweis (hindert das Senden nicht):</b> '
                   'Jede Buchung braucht Rechnungen oder Quittungen. Es fehlt noch etwas zu:%s</div>'
                   % "".join("<div>• %s</div>" % e(x) for x in deckung_mod.luecken_texte(info["luecken"])))
    erneut = ""
    if info["bereits"]:
        erneut = ('<label style="display:block;margin:10px 0;font-size:13px"><input type="checkbox" name="erneut" value="1" '
                  'style="width:auto;margin-right:6px"> Alle Belege sind schon einmal gesendet worden – '
                  'trotzdem erneut senden</label>')
    teilweise = ""
    if not info["bereits"] and any(b["gesendet_am"] for b in info["belege"]):
        teilweise = ('<div style="font-size:12px;color:var(--muted);margin-top:8px">Ein Teil der Belege wurde früher schon '
                     'gesendet. Die Mail enthält trotzdem immer alle Belege der Reise.</div>')
    knopf_attr = ('disabled title="Erst alle Hinweise oben erledigen" style="width:100%;opacity:.4;cursor:not-allowed"'
                  if hind else 'style="width:100%"')
    dateien = n + 1
    content = (
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px">'
        f'<div><h1 class="page-title" style="margin:0">📤 Übergabe an Habel {e(rcode)}</h1>'
        f'<div style="color:var(--muted);font-size:13px">{e(reise["titel"])}</div></div>'
        '<div style="display:flex;gap:8px;flex-wrap:wrap">'
        f'<a href="/reise/{e(rcode)}/habel/vorschau" class="btn btn-primary">👁 Vorschau: Dateien der Mail</a>'
        f'<a href="/reise/{e(rcode)}" class="btn btn-secondary">← Reise</a>'
        f'<a href="/reise/{e(rcode)}/abschluss" class="btn btn-secondary">🧾 Abschluss</a></div></div>'
        f'{banner}'
        '<div class="card" style="margin-bottom:16px"><div class="card-header">'
        f'<span class="card-title">1. Sind alle Belege geprüft? ({n_ok} von {n})</span></div>'
        '<div class="table-wrap"><table><thead><tr><th>Beleg</th><th>Art</th><th>Anbieter</th><th>Datum</th>'
        '<th style="text-align:right">Betrag</th><th>Prüfung</th><th>Früher gesendet</th></tr></thead>'
        f'<tbody>{zeilen}</tbody></table></div></div>'
        '<div class="card"><div class="card-header"><span class="card-title">2. Senden</span></div><div class="card-body">'
        f'<p style="margin:0 0 8px 0;font-size:13px">Eine E-Mail an <b>{e(DMS_EMAIL_TO or "– nicht eingerichtet –")}</b> '
        f'mit <b>{dateien} PDF-Datei{"" if dateien == 1 else "en"}</b>: die Reisekostenabrechnung '
        'und jeder Beleg einzeln (mit Prüf-Deckblatt).</p>'
        f'<form method="post" action="/reise/{e(rcode)}/habel/senden" style="margin:0">{erneut}'
        f'<button type="submit" class="btn btn-success" {knopf_attr}>📤 Jetzt an Habel senden</button></form>'
        f'{teilweise}'
        '</div></div>')
    return shell("Übergabe an Habel %s" % rcode, content, "reisen")


@app.get("/reise/{code}/habel", response_class=HTMLResponse)
def reise_habel_anzeigen(code: str, request: Request, ok: str = ""):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    info = habel_paket_pruefen(code.upper())
    if not info["reise"]:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
    return HTMLResponse(_habel_seite(info, ok=bool(ok)))


def _habel_vorschau_seite(info: dict) -> str:
    """Zeigt Mail (Empfänger, Betreff, Text) und alle Dateien zum Anklicken – ohne etwas zu senden."""
    from html import escape as e
    import pypdf
    rcode = info["reise"]["code"]
    betreff, text = habel_mail_inhalt(info)
    zeilen = ""; gesamt = 0; fehler_html = ""
    try:
        anhaenge = habel_paket_bauen(info, grenze_pruefen=False)
    except ValueError as ex:
        anhaenge = []
        fehler_html = '<div class="alert alert-err" style="margin-bottom:16px">%s</div>' % e(str(ex))
    for i, (daten, name) in enumerate(anhaenge):
        gesamt += len(daten)
        try:
            seiten = len(pypdf.PdfReader(io.BytesIO(daten)).pages)
        except Exception:
            seiten = "?"
        if i == 0:
            link, was, hinweis = f"/reise/{e(rcode)}/habel/datei/abrechnung", "Reisekostenabrechnung", ""
        else:
            b = info["belege"][i - 1]
            link = f"/reise/{e(rcode)}/habel/datei/{b['id']}"
            bet = ("%.2f %s" % (float(b["betrag"]), b["waehrung"])) if b["betrag"] else "ohne Betrag"
            was = "%s · %s · %s" % (e(b["art"]), e(b["anbieter"] or "–"), bet)
            hinweis = "" if b["geprueft"] else '<span style="color:#b91c1c">noch nicht geprüft</span>'
        zeilen += ('<tr><td>%d</td><td><a href="%s" target="_blank" style="font-family:monospace;font-size:12px">%s</a></td>'
                   '<td style="font-size:12px">%s</td><td style="text-align:right">%s</td>'
                   '<td style="text-align:right;font-size:12px">%.0f KB</td><td style="font-size:12px">%s</td></tr>'
                   % (i + 1, link, e(name), was, seiten, len(daten) / 1024, hinweis))
    grenze = HABEL_MAX_MB * 1024 * 1024
    gr_txt = ("%s von höchstens %g MB" % (("%.1f MB" % (gesamt / 1048576)) if gesamt >= 1048576 else ("%.0f KB" % (gesamt / 1024)),
                                         HABEL_MAX_MB))
    gr_farbe = "#b91c1c" if gesamt > grenze else "var(--muted)"
    offen = ""
    if info["offen"] or info["ohne_original"]:
        offen = ('<div class="alert alert-warn" style="margin-bottom:16px">Vorschau – gesendet werden darf erst, wenn alle Belege '
                 'geprüft sind (noch offen: %s).</div>'
                 % e(", ".join("#%s" % b["id"] for b in info["offen"] + info["ohne_original"])))
    content = (
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:8px">'
        f'<div><h1 class="page-title" style="margin:0">👁 Vorschau Habel-Mail {e(rcode)}</h1>'
        f'<div style="color:var(--muted);font-size:13px">{e(info["reise"]["titel"])}</div></div>'
        '<div style="display:flex;gap:8px">'
        f'<a href="/reise/{e(rcode)}/habel" class="btn btn-secondary">← Übergabe an Habel</a>'
        f'<a href="/reise/{e(rcode)}" class="btn btn-secondary">Reise</a></div></div>'
        '<div class="alert alert-ok" style="margin-bottom:16px">Nur eine Vorschau: Es wird nichts gesendet. '
        'Die Dateien sind genau die, die später in der Mail hängen.</div>'
        f'{offen}{fehler_html}'
        '<div class="card" style="margin-bottom:16px"><div class="card-header"><span class="card-title">Die Mail</span></div>'
        '<div class="card-body" style="font-size:13px">'
        f'<div><b>An:</b> {e(DMS_EMAIL_TO or "– nicht eingerichtet –")}</div>'
        f'<div style="margin-top:4px"><b>Betreff:</b> {e(betreff)}</div>'
        f'<pre style="margin:10px 0 0 0;padding:10px;background:#f8fafc;border:1px solid var(--border);border-radius:8px;'
        f'white-space:pre-wrap;font-size:12px">{e(text)}</pre></div></div>'
        '<div class="card"><div class="card-header">'
        f'<span class="card-title">Dateien in der Mail ({len(anhaenge)}) – zum Öffnen anklicken</span>'
        f'<span style="font-size:12px;color:{gr_farbe}">Gesamt {gr_txt}</span></div>'
        '<div class="table-wrap"><table><thead><tr><th>Nr.</th><th>Datei</th><th>Inhalt</th>'
        '<th style="text-align:right">Seiten</th><th style="text-align:right">Größe</th><th></th></tr></thead>'
        f'<tbody>{zeilen}</tbody></table></div></div>')
    return shell("Vorschau Habel-Mail %s" % rcode, content, "reisen")


@app.get("/reise/{code}/habel/vorschau", response_class=HTMLResponse)
def reise_habel_vorschau(code: str, request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    info = habel_paket_pruefen(code.upper())
    if not info["reise"]:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
    return HTMLResponse(_habel_vorschau_seite(info))


@app.get("/reise/{code}/habel/datei/{datei}")
def reise_habel_datei(code: str, datei: str, request: Request):
    """Eine Datei der Habel-Mail zur Ansicht ('abrechnung' oder Beleg-Nr.). Nur Belege, die auch gesendet würden."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    rcode = code.upper()
    info = habel_paket_pruefen(rcode)
    if not info["reise"]:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
    from fastapi.responses import Response
    try:
        if datei == "abrechnung":
            namen = {b["id"]: b["name"] for b in info["belege"]}
            antwort = abschluss_pdf_antwort(rcode, mit_originalen=False, anlagen_namen=namen)
            if getattr(antwort, "media_type", "") != "application/pdf":
                raise ValueError("Die Reisekostenabrechnung konnte nicht erzeugt werden.")
            daten, name = antwort.body, f"Reisekostenabrechnung_{rcode}.pdf"
        else:
            b = next((x for x in info["belege"] if str(x["id"]) == datei), None)
            if not b:
                return HTMLResponse(shell("Nicht gefunden", '<div class="alert alert-err">Diese Datei gehört nicht zur Habel-Mail dieser Reise.</div>'), status_code=404)
            daten, name = pruef_pdf_fuer_beleg(b["id"]), b["name"]
    except Exception as ex:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{ex}</div>'), status_code=500)
    return Response(content=daten, media_type="application/pdf", headers={"Content-Disposition": f'inline; filename="{name}"'})


@app.post("/reise/{code}/habel/senden")
def reise_habel_senden(code: str, request: Request, erneut: str = Form("")):
    """Eine Mail an Habel: Reisekostenabrechnung + jeder Beleg einzeln. Geprüft wird hier erneut (nicht nur auf der Seite)."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    rcode = code.upper()
    info = habel_paket_pruefen(rcode)
    if not info["reise"]:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
    fehler = habel_hindernisse(info)
    if not fehler and info["bereits"] and not erneut:
        fehler.append("Diese Reise wurde schon an Habel gesendet. Zum erneuten Senden bitte den Haken setzen.")
    anhaenge = []
    if not fehler:
        try:
            anhaenge = habel_paket_bauen(info)
        except ValueError as ex:
            fehler.append(str(ex))
    if fehler:
        return HTMLResponse(_habel_seite(info, fehler=fehler), status_code=409)
    betreff, text = habel_mail_inhalt(info)
    ergebnis = sende_dms_mail(betreff, text, anhaenge[0][0], anhaenge[0][1], weitere_anhaenge=anhaenge[1:])
    if ergebnis.get("fehler"):
        return HTMLResponse(_habel_seite(info, fehler=["Der Versand ist fehlgeschlagen: %s" % ergebnis["fehler"]]), status_code=502)
    if info["belege"]:
        P = ph()
        db = get_db(); cur = db.cursor()
        jetzt = "NOW()" if is_postgres() else "datetime('now')"
        for b in info["belege"]:
            cur.execute(f"UPDATE belege SET dms_versendet_am={jetzt} WHERE id={P}", (b["id"],))
        db.commit(); cur.close(); db.close()
    return RedirectResponse(f"/reise/{rcode}/habel?ok=1", status_code=303)


def _habel_einzel_ergebnis_seite(rcode: str, bid: int, fehler: list | None = None, ok_text: str = "") -> str:
    from html import escape as e
    if fehler:
        banner = ('<div class="alert alert-err" style="margin-bottom:16px"><b>Nicht gesendet:</b>%s</div>'
                  % "".join("<div>• %s</div>" % e(x) for x in fehler))
    else:
        banner = '<div class="alert alert-ok" style="margin-bottom:16px">%s</div>' % ok_text
    content = (f'<h1 class="page-title">📤 Beleg #{bid} einzeln an Habel</h1>{banner}'
               f'<a href="/reise/{e(rcode)}" class="btn btn-primary">← Zurück zur Reise {e(rcode)}</a> '
               f'<a href="/beleg/{bid}" class="btn btn-secondary">Zum Beleg</a>')
    return shell("Einzelversand an Habel", content, "reisen")


@app.post("/reise/{code}/habel/beleg/{bid}/senden")
def reise_habel_beleg_senden(code: str, bid: int, request: Request, erneut: str = Form("")):
    """Sendet EINEN Beleg (nur Rechnung oder Quittung, geprüft, mit Original) von Hand an Habel.
    Die Reise-Übergabe (dms_versendet_am) bleibt unberührt; der Einzelversand steht in belege.habel_einzel_am."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    rcode = code.upper()
    info = habel_paket_pruefen(rcode)
    if not info["reise"]:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
    b = next((x for x in info["belege"] if x["id"] == bid), None)
    if not b:
        return HTMLResponse(_habel_einzel_ergebnis_seite(rcode, bid, fehler=[
            "Dieser Beleg gehört nicht zu dieser Reise oder ist keine Rechnung/Quittung."]), status_code=404)
    fehler = habel_einzel_hindernisse(b)
    if not fehler and (b["einzel_am"] or b["gesendet_am"]) and not erneut:
        fehler.append("Dieser Beleg wurde schon an Habel gesendet. Zum erneuten Senden bitte den Knopf erneut bestätigen.")
    daten = None
    if not fehler:
        try:
            daten = pruef_pdf_fuer_beleg(bid)
        except Exception as ex:
            fehler.append(f"Beleg #{bid} konnte nicht vorbereitet werden: {ex}")
        if daten is not None and len(daten) > HABEL_MAX_MB * 1024 * 1024:
            fehler.append(f"Die Mail wäre {len(daten) / 1048576:.1f} MB groß (Grenze {HABEL_MAX_MB:g} MB).")
    if fehler:
        return HTMLResponse(_habel_einzel_ergebnis_seite(rcode, bid, fehler=fehler), status_code=409)
    reise = info["reise"]
    bet = ("%.2f %s" % (float(b["betrag"]), b["waehrung"])) if b["betrag"] else "ohne Betrag"
    betreff = "Beleg %s – %s %s (Nr. %s)" % (rcode, b["art"], b["anbieter"] or "–", bid)
    text = ("Einzelbeleg zur Reise %s – %s\nZeitraum: %s – %s\nReisende: %s\n\n"
            "Anhang:\n - %s  (%s, %s, %s, %s)\n\n"
            "Der Beleg ist geprüft (Prüf-Deckblatt im PDF). Einzelversand: Die Reisekostenabrechnung ist nicht enthalten.\n"
            % (rcode, reise["titel"], fmt_date(reise["abreise"]), fmt_date(reise["rueckkehr"]),
               ", ".join(info["mitarbeiter"]) or "–", b["name"], b["art"], b["anbieter"] or "–",
               fmt_date(b["datum"]) if b["datum"] else "ohne Datum", bet))
    ergebnis = sende_dms_mail(betreff, text, daten, b["name"])
    if ergebnis.get("fehler"):
        return HTMLResponse(_habel_einzel_ergebnis_seite(rcode, bid, fehler=[
            "Der Versand ist fehlgeschlagen: %s" % ergebnis["fehler"]]), status_code=502)
    P = ph()
    db = get_db(); cur = db.cursor()
    jetzt = "NOW()" if is_postgres() else "datetime('now')"
    cur.execute(f"UPDATE belege SET habel_einzel_am={jetzt} WHERE id={P}", (bid,))
    db.commit(); cur.close(); db.close()
    from html import escape as e
    return HTMLResponse(_habel_einzel_ergebnis_seite(rcode, bid, ok_text=(
        "✓ Beleg #%s gesendet an <b>%s</b> (eine Mail, eine PDF-Datei: %s)." % (bid, e(DMS_EMAIL_TO or "Habel"), e(b["name"])))))


@app.post("/beleg/{bid}/zahlungsart")
async def beleg_zahlungsart_speichern(bid: int, request: Request):
    """Speichert manuell gewählte Bezahlart (Kreditkarte/Bar/Überweisung)."""
    form = await request.form()
    zahlungsart = (form.get("zahlungsart") or "").strip() or None
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"UPDATE belege SET zahlungsart={P} WHERE id={P}", (zahlungsart, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/beleg/{bid}/kreditkarte")
async def beleg_kreditkarte_speichern(bid: int, request: Request):
    """Speichert, welche Kreditkarte (Privat-/Firmenkreditkarte + Mitarbeiterkürzel)
    für die Zahlung genutzt wurde – nur relevant, wenn Bezahlart = Kreditkarte."""
    form = await request.form()
    kreditkarte_karte = (form.get("kreditkarte_karte") or "").strip() or None
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"UPDATE belege SET kreditkarte_karte={P} WHERE id={P}", (kreditkarte_karte, bid))
        auslandsentgelt_neu_berechnen(bid, db)
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/beleg/{bid}/belegart")
async def beleg_belegart_speichern(bid: int, request: Request):
    """Speichert manuell gewählte Belegart (Rechnung/Quittung/etc.)."""
    form = await request.form()
    belegart = (form.get("belegart") or "").strip() or None
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"UPDATE belege SET belegart={P} WHERE id={P}", (belegart, bid))
        if belegart == "Buchungsbestaetigung":
            # Betrag ist bei Buchungsbestätigungen optional – fehlender Betrag
            # allein macht den Beleg nicht mehr unvollständig.
            cur.execute(f"""SELECT betrag_brutto, event_datum_von, belegdatum, transportart, anbieter
                            FROM belege WHERE id={P}""", (bid,))
            r = cur.fetchone()
            g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
            if r and g("event_datum_von",1) and g("belegdatum",2) and g("transportart",3) and g("anbieter",4):
                cur.execute(f"UPDATE belege SET pflichtfelder_ok=TRUE, fehlende_felder='[]', "
                            f"status='ok' WHERE id={P}", (bid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/beleg/{bid}/zuordnen")
async def beleg_zuordnen(bid: int, request: Request):
    form = await request.form()
    rcode = (form.get("reise_code") or "").strip() or None
    sperre = _bewirtung_sperre(bid) or _vorschuss_sperre(bid)
    if sperre:
        return HTMLResponse(shell("Zuordnung nicht möglich",
            f'<div class="alert alert-err">{sperre}</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">← Zurück zum Beleg</a>'), status_code=409)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT reise_code FROM belege WHERE id={P}", (bid,))
        rc_row = cur.fetchone()
        rc_alt = (rc_row[0] if isinstance(rc_row, tuple) else rc_row["reise_code"]) if rc_row else None
        cur.execute(f"UPDATE belege SET reise_code={P} WHERE id={P}", (rcode, bid))
        db.commit(); cur.close(); db.close()
        for rc_x in {rc_alt, rcode}:      # alte und neue Reise aktualisieren
            vma_auto_aktualisieren(rc_x)
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

def pruef_pdf_fuer_beleg(bid: int) -> bytes:
    """Baut das Prüf-PDF (Deckblatt + Original) für einen geprüften Beleg."""
    P = ph()
    db = get_db(); cur = db.cursor()
    cur.execute(f"""SELECT reise_code, geprueft_von, geprueft_am, pruef_vermerk,
                    anbieter, betrag_brutto, waehrung, belegdatum, s3_original
                    FROM belege WHERE id={P}""", (bid,))
    r = cur.fetchone()
    cur.close(); db.close()
    if not r:
        raise ValueError("Beleg nicht gefunden")
    g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
    rcode = g("reise_code",0); geprueft_von = g("geprueft_von",1); geprueft_am = g("geprueft_am",2)
    vermerk = g("pruef_vermerk",3); anbieter = g("anbieter",4)
    zusatz = _bewirtung_vermerk_zusatz(bid)
    if zusatz:
        vermerk = (str(vermerk) + "\n" if vermerk else "") + zusatz
    betrag = g("betrag_brutto",5); waehrung = g("waehrung",6) or "EUR"
    belegdat = g("belegdatum",7); s3o = g("s3_original",8)
    if not s3o:
        raise ValueError("Kein Original-Dokument in S3 vorhanden")

    deckblatt = pruefkopf_pdf_erzeugen(rcode, geprueft_von, geprueft_am, vermerk,
                                        anbieter, float(betrag) if betrag else None,
                                        waehrung, belegdat)
    original = s3_download(s3o)
    return beleg_mit_pruefkopf(original, deckblatt)

@app.get("/beleg/{bid}/erechnung-xml")
def beleg_erechnung_xml(bid: int):
    """Liefert die eingebettete eRechnung-XML unverändert im Original."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT s3_erechnung_xml, dateiname FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return JSONResponse({"fehler": "Nicht gefunden"}, status_code=404)
        g = lambda k,i: r[k] if hasattr(r,'keys') else r[i]
        key = g("s3_erechnung_xml",0); dateiname = g("dateiname",1) or f"beleg_{bid}"
        if not key:
            return JSONResponse({"fehler": "Keine eRechnung-XML vorhanden"}, status_code=404)
        from fastapi.responses import Response
        data = s3_download(key)
        xml_name = re.sub(r"\.pdf$", "", dateiname, flags=re.IGNORECASE) + ".xml"
        return Response(content=data, media_type="application/xml",
                        headers={"Content-Disposition": f"attachment; filename={xml_name}"})
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/beleg/{bid}/pruef-pdf")
def beleg_pruef_pdf(bid: int):
    """Zeigt das Prüf-PDF (Deckblatt + Original) zur Kontrolle, bevor an Habel übertragen wird."""
    try:
        data = pruef_pdf_fuer_beleg(bid)
        from fastapi.responses import Response
        return Response(content=data, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename=beleg_{bid}_pruef.pdf"})
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/beleg/{bid}/pdf/{typ}")
def beleg_pdf(bid: int, typ: str):
    """Liefert Original-, Anon- oder Analyse-PDF aus S3."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT s3_original,s3_anon,s3_analyse FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r: return JSONResponse({"fehler": "Nicht gefunden"}, status_code=404)
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        keys = {"original": get(r,"s3_original",0),
                "anon": get(r,"s3_anon",1),
                "analyse": get(r,"s3_analyse",2)}
        key = keys.get(typ)
        if not key: return JSONResponse({"fehler": "Ungültiger Typ"}, status_code=400)
        from fastapi.responses import Response
        data = s3_download(key)
        return Response(content=data, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename=beleg_{bid}_{typ}.pdf"})
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/unzugeordnet", response_class=HTMLResponse)
def belege_unzugeordnet():
    """Alle Belege ohne Reisezuordnung – müssen zugeordnet werden."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT id, transportart, transportart_freitext,
            dateiname, anbieter, betrag_brutto, waehrung,
            belegdatum, status, erstellt
            FROM belege WHERE reise_code IS NULL
            ORDER BY erstellt DESC""")
        rows = cur.fetchall()
        cur.execute("SELECT code, titel, abreise FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        if not rows:
            return HTMLResponse(shell("Alle Belege zugeordnet", """
            <div style="text-align:center;padding:60px 20px">
              <div style="font-size:48px;margin-bottom:16px">✅</div>
              <h1 style="font-size:20px;font-weight:700;margin-bottom:8px">
                Alle Belege zugeordnet</h1>
              <p style="color:#64748b;margin-bottom:20px">
                Es gibt keine offenen Belege.</p>
              <a href="/" class="btn btn-secondary">← Dashboard</a>
            </div>"""))

        # Reisen-Optionen für Dropdown
        r_opts = '<option value="">– Reise wählen –</option>'
        for rv in reisen:
            rc = get(rv,"code",0); rt = get(rv,"titel",1); ab = get(rv,"abreise",2)
            r_opts += f'<option value="{rc}">{rc} – {rt} ({fmt_date(ab)})</option>'

        typ_farben = {
            "Flug":"#dbeafe:#1e40af","Hotel":"#dcfce7:#166534",
            "Bahn":"#e0e7ff:#3730a3","Taxi":"#fef3c7:#92400e",
            "Mietwagen":"#fce7f3:#9d174d","Bewirtung":"#fff7ed:#9a3412",
            "Tanken":"#f0fdf4:#14532d","Sonstiges":"#f1f5f9:#475569"
        }

        karten = ""
        for r in rows:
            bid=get(r,"id",0); typ=get(r,"transportart",1)
            freitext=get(r,"transportart_freitext",2) or ""
            datei=get(r,"dateiname",3); vendor=get(r,"anbieter",4)
            betrag=get(r,"betrag_brutto",5); waehrung=get(r,"waehrung",6)
            bd=get(r,"belegdatum",7); zusamm=get(r,"status",8)

            tc = typ_farben.get(typ or "Sonstiges","#f1f5f9:#475569").split(":")
            typ_badge = (f'<span style="background:{tc[0]};color:{tc[1]};'
                        f'padding:2px 8px;border-radius:4px;font-size:11px;'
                        f'font-weight:700">{typ or "?"}</span>')
            bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else "–"

            karten += f"""
            <div class="card" style="border-left:4px solid #ef4444">
              <div class="card-body">
                <div style="display:flex;justify-content:space-between;
                            align-items:flex-start;gap:16px;flex-wrap:wrap">
                  <div style="flex:1;min-width:200px">
                    <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
                      {typ_badge}
                      <span style="font-size:12px;color:#64748b">{datei[:40]}</span>
                    </div>
                    <div style="font-weight:700;font-size:15px;margin-bottom:4px">
                      {vendor or "Unbekannter Anbieter"}</div>
                    <div style="display:flex;gap:16px;flex-wrap:wrap">
                      <span style="font-weight:700;color:#059669">{bet_s}</span>
                      <span style="color:#64748b">{fmt_date(bd)}</span>
                    </div>
                    {f'<div style="font-size:12px;color:#94a3b8;margin-top:4px">{zusamm}</div>' if zusamm else ''}
                  </div>
                  <div style="display:flex;flex-direction:column;gap:8px;min-width:280px">
                    <form method="post" action="/beleg/{bid}/zuordnen"
                          style="display:flex;gap:8px">
                      <select name="reise_code" style="flex:1;padding:7px 10px;
                              border:1px solid #d1d5db;border-radius:6px;font-size:13px">
                        {r_opts}
                      </select>
                      <button type="submit" class="btn btn-success btn-sm"
                              style="white-space:nowrap">✓ Zuordnen</button>
                    </form>
                    <a href="/beleg/{bid}" class="btn btn-secondary btn-sm"
                       style="text-align:center">Detail ansehen</a>
                  </div>
                </div>
              </div>
            </div>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;
                    margin-bottom:20px">
          <div>
            <h1 class="page-title" style="margin:0">⚠ Unzugeordnete Belege</h1>
            <p style="color:#64748b;margin-top:4px;font-size:13px">
              {len(rows)} Beleg{"e" if len(rows)!=1 else ""} ohne Reisezuordnung.
              Bitte jeden Beleg einer Reise zuordnen.
            </p>
          </div>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>
        {karten}"""
        return HTMLResponse(shell("Unzugeordnete Belege", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))


_MONATSNAMEN = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August",
                "September", "Oktober", "November", "Dezember"]


def _beleg_datum(x):
    """Datum aus date/datetime/Text (SQLite) oder None."""
    if not x:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return None


@app.get("/belege", response_class=HTMLResponse)
def belege_liste(request: Request):
    """Belegliste mit Filtern: Jahr, Monat, Reise (auch 'nicht zugeordnet'),
    Prüfstatus, Problem-Art, Freitextsuche und Sortierung. Gefiltert wird in Python,
    damit es mit PostgreSQL und SQLite gleich funktioniert."""
    from html import escape as esc
    from urllib.parse import urlencode
    try:
        qp = request.query_params
        f_jahr = (qp.get("jahr") or "").strip()
        f_monat = (qp.get("monat") or "").strip()
        f_reise = (qp.get("reise") or "").strip()
        f_geprueft = (qp.get("geprueft") or "").strip()
        f_problem = (qp.get("problem") or "").strip()
        f_suche = (qp.get("suche") or "").strip()
        f_sort = (qp.get("sort") or "neu").strip()
        f_alle = (qp.get("alle") or "") == "1"
        if not f_jahr.isdigit():
            f_jahr = ""
        if not (f_monat.isdigit() and 1 <= int(f_monat) <= 12):
            f_monat = ""

        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT b.id, b.reise_code, b.transportart, b.anbieter, b.betrag_brutto,
            b.waehrung, b.belegdatum, b.status, b.dateiname, b.pflichtfelder_ok, b.ist_erechnung,
            b.geprueft, b.zahlungsart, b.belegart, b.aenderung_status, b.event_datum_von,
            b.erstellt, b.rechnungsnummer
            FROM belege b ORDER BY b.id DESC""")
        rows = cur.fetchall()
        cur.execute("SELECT code, titel FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()

        # Kreditkarten-Abgleich separat laden: die Spalte entsteht erst mit /init
        kk_abgeglichen = set()
        try:
            db2 = get_db(); cur2 = db2.cursor()
            cur2.execute("SELECT id FROM belege WHERE kk_abrechnung_datum IS NOT NULL")
            kk_abgeglichen = {(r[0] if isinstance(r, tuple) else r["id"]) for r in cur2.fetchall()}
            cur2.close(); db2.close()
        except Exception:
            kk_abgeglichen = set()

        def get(r, k, i): return r[k] if hasattr(r, 'keys') else r[i]

        items = []
        for r in rows:
            bd = _beleg_datum(get(r, "belegdatum", 6))
            sortdatum = bd or _beleg_datum(get(r, "event_datum_von", 15)) or _beleg_datum(get(r, "erstellt", 16))
            items.append({
                "id": get(r, "id", 0), "reise": get(r, "reise_code", 1), "typ": get(r, "transportart", 2),
                "anbieter": get(r, "anbieter", 3), "betrag": get(r, "betrag_brutto", 4),
                "waehrung": get(r, "waehrung", 5), "datum": bd, "sortdatum": sortdatum,
                "status": get(r, "status", 7), "datei": get(r, "dateiname", 8) or "",
                "pf_ok": bool(get(r, "pflichtfelder_ok", 9)), "erechnung": bool(get(r, "ist_erechnung", 10)),
                "geprueft": bool(get(r, "geprueft", 11)), "zahlungsart": get(r, "zahlungsart", 12),
                "belegart": get(r, "belegart", 13), "aenderung": get(r, "aenderung_status", 14),
                "rechnungsnr": get(r, "rechnungsnummer", 17) or "",
            })

        jahre = sorted({it["sortdatum"].year for it in items if it["sortdatum"]}, reverse=True)
        if f_jahr and int(f_jahr) not in jahre:
            jahre.append(int(f_jahr)); jahre.sort(reverse=True)

        def passt(it):
            sd = it["sortdatum"]
            if f_jahr and (not sd or sd.year != int(f_jahr)):
                return False
            if f_monat and (not sd or sd.month != int(f_monat)):
                return False
            if f_reise == "_ohne" and it["reise"]:
                return False
            if f_reise and f_reise != "_ohne" and it["reise"] != f_reise:
                return False
            if f_geprueft == "nein" and it["geprueft"]:
                return False
            if f_geprueft == "ja" and not it["geprueft"]:
                return False
            if f_problem == "pflicht" and it["pf_ok"]:
                return False
            if f_problem == "fehler" and it["status"] != "fehler":
                return False
            if f_problem == "zahlungsart" and (it["zahlungsart"] or it["belegart"] == "Buchungsbestaetigung"):
                return False
            if f_problem == "kreditkarte" and not (it["zahlungsart"] == "Kreditkarte" and it["id"] not in kk_abgeglichen):
                return False
            if f_problem == "aenderung" and it["aenderung"] != "offen":
                return False
            if f_suche:
                s = f_suche.lstrip("#").lower()
                heu = " ".join([str(it["id"]), it["anbieter"] or "", it["datei"], it["rechnungsnr"],
                                it["reise"] or ""]).lower()
                if s not in heu:
                    return False
            return True

        treffer = [it for it in items if passt(it)]
        if f_sort == "datum":
            treffer.sort(key=lambda it: (it["sortdatum"] or date.min, it["id"]), reverse=True)
        limit = 300
        gekuerzt = (not f_alle) and len(treffer) > limit
        anzeige = treffer if f_alle else treffer[:limit]

        typ_farben = {
            "Flug": "badge-blue", "Hotel": "badge-green", "Bahn": "badge-blue",
            "Taxi": "badge-amber", "Mietwagen": "badge-red", "Tanken": "badge-green",
            "Verpflegung": "badge-amber", "Bewirtung": "badge-amber", "Sonstiges": "badge-gray"
        }
        zeilen = ""
        for it in anzeige:
            bid = it["id"]; rcode = it["reise"]; typ = it["typ"]
            bc = typ_farben.get(typ or "", "badge-gray")
            bet_s = f"{float(it['betrag']):.2f} {it['waehrung']}" if it["betrag"] else "–"
            stat_b = ('<span class="badge badge-green">✓</span>' if it["status"] == "ok"
                      else '<span class="badge badge-red">✗</span>' if it["status"] == "fehler"
                      else '<span class="badge badge-amber">…</span>')
            geprueft_b = ('<span class="badge badge-green" title="geprüft">✓</span>' if it["geprueft"]
                          else '<span style="color:var(--muted)">–</span>')
            erechnung_badge = ' <span class="badge badge-green" title="eRechnung">📄✓</span>' if it["erechnung"] else ""
            aenderung_badge = (' <span class="badge badge-amber" title="Änderung offen">⚠ Änderung</span>'
                               if it["aenderung"] == "offen" else "")
            name = esc((it["anbieter"] or it["datei"] or "")[:30])
            reise_zelle = (f'<a href="/reise/{esc(rcode)}" style="font-family:monospace;font-size:12px;'
                           f'color:var(--blue)">{esc(rcode)}</a>') if rcode else (
                           '<span class="badge badge-amber">nicht zugeordnet</span>')
            zeilen += (f'<tr>'
                f'<td><a href="/beleg/{bid}" style="color:var(--blue);font-weight:600">#{bid}</a></td>'
                f'<td><span class="badge {bc}">{esc(typ or "?")}</span>{erechnung_badge}{aenderung_badge}</td>'
                f'<td style="font-weight:500">{name}</td>'
                f'<td style="font-weight:600;color:var(--green)">{bet_s}</td>'
                f'<td>{fmt_date(it["datum"])}</td>'
                f'<td>{reise_zelle}</td>'
                f'<td>{stat_b}</td>'
                f'<td>{geprueft_b}</td>'
                f'<td><a href="/beleg/{bid}" class="btn btn-secondary btn-sm">Detail</a></td>'
                f'</tr>')

        def sel(name, optionen, aktuell):
            opts = ""
            for wert, label in optionen:
                markiert = " selected" if str(wert) == str(aktuell) else ""
                opts += f'<option value="{esc(str(wert))}"{markiert}>{esc(label)}</option>'
            return (f'<select name="{name}" onchange="this.form.submit()" '
                    f'style="width:auto;max-width:240px;padding:6px 8px;font-size:13px;'
                    f'border:1px solid var(--border);border-radius:6px;background:white">{opts}</select>')

        jahr_opts = [("", "Alle Jahre")] + [(j, str(j)) for j in jahre]
        monat_opts = [("", "Alle Monate")] + [(i + 1, n) for i, n in enumerate(_MONATSNAMEN)]
        reise_opts = [("", "Alle Reisen"), ("_ohne", "Nicht zugeordnet")] + [
            (get(r, "code", 0), f'{get(r, "code", 0)} – {(get(r, "titel", 1) or "")[:40]}') for r in reisen]
        pruef_opts = [("", "Prüfung: alle"), ("nein", "Nicht geprüft"), ("ja", "Geprüft")]
        problem_opts = [("", "Problem: alle"), ("pflicht", "Pflichtfelder fehlen"),
                        ("fehler", "Fehler bei Analyse"), ("zahlungsart", "Zahlungsart fehlt"),
                        ("kreditkarte", "Kreditkarte nicht abgeglichen"), ("aenderung", "Änderung offen")]
        sort_opts = [("neu", "Sortierung: zuletzt eingegangen"), ("datum", "Sortierung: Belegdatum")]

        def chip(label, **params):
            return (f'<a href="/belege?{urlencode(params)}" class="btn btn-secondary btn-sm">{label}</a>'
                    if params else f'<a href="/belege" class="btn btn-secondary btn-sm">{label}</a>')

        filter_aktiv = any([f_jahr, f_monat, f_reise, f_geprueft, f_problem, f_suche])
        filter_html = f"""
        <form method="get" action="/belege" class="card" style="margin-bottom:16px">
          <div class="card-body" style="display:flex;flex-wrap:wrap;gap:8px;align-items:center">
            {sel("jahr", jahr_opts, f_jahr)}
            {sel("monat", monat_opts, f_monat)}
            {sel("reise", reise_opts, f_reise)}
            {sel("geprueft", pruef_opts, f_geprueft)}
            {sel("problem", problem_opts, f_problem)}
            {sel("sort", sort_opts, f_sort)}
            <input type="text" name="suche" value="{esc(f_suche)}" placeholder="Suche: Anbieter, Datei, Nr."
                   style="width:auto;padding:6px 8px;font-size:13px;border:1px solid var(--border);border-radius:6px;min-width:220px">
            <button type="submit" class="btn btn-primary btn-sm">Filtern</button>
            {'<a href="/belege" class="btn btn-secondary btn-sm">Zurücksetzen</a>' if filter_aktiv else ''}
          </div>
          <div class="card-body" style="padding-top:0;display:flex;flex-wrap:wrap;gap:6px;align-items:center">
            <span style="font-size:12px;color:var(--muted)">Schnellauswahl:</span>
            {chip("Nicht zugeordnet", reise="_ohne")}
            {chip("Nicht geprüft", geprueft="nein")}
            {chip("Pflichtfelder fehlen", problem="pflicht")}
            {chip("Änderung offen", problem="aenderung")}
            {chip("Kreditkarte nicht abgeglichen", problem="kreditkarte")}
            {chip("Dieses Jahr", jahr=date.today().year)}
          </div>
        </form>"""

        hinweis = ""
        if gekuerzt:
            rest_params = {k: v for k, v in (("jahr", f_jahr), ("monat", f_monat), ("reise", f_reise),
                           ("geprueft", f_geprueft), ("problem", f_problem), ("suche", f_suche),
                           ("sort", f_sort)) if v}
            rest_params["alle"] = "1"
            hinweis = (f'<div class="alert alert-warn" style="margin-bottom:12px">Es werden die ersten {limit} '
                       f'von {len(treffer)} Treffern angezeigt. '
                       f'<a href="/belege?{urlencode(rest_params)}">Alle anzeigen</a></div>')

        anzahl_txt = (f"{len(treffer)} von {len(items)}" if filter_aktiv else f"{len(items)}")
        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Belege ({anzahl_txt})</h1>
          <div style="display:flex;gap:8px">
            <a href="/mails-abrufen" class="btn btn-success">📬 Mails abrufen</a>
            <a href="/beleg/upload" class="btn btn-primary">+ Beleg hochladen</a>
          </div>
        </div>
        {filter_html}
        {hinweis}
        <div class="card">
          <div class="table-wrap"><table>
            <thead><tr>
              <th>#</th><th>Typ</th><th>Anbieter</th><th>Betrag</th>
              <th>Datum</th><th>Reise</th><th>Status</th><th>Geprüft</th><th></th>
            </tr></thead>
            <tbody>
              {zeilen or '<tr><td colspan="9"><div class="empty-state">Keine Belege für diese Auswahl.</div></td></tr>'}
            </tbody>
          </table></div>
        </div>"""
        return HTMLResponse(shell("Belege", content, "belege"))
    except Exception as ex:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{ex}</div>'))


@app.get("/debug-anon")
def debug_anon():
    """Zeigt welche Namen für Anonymisierung geladen werden."""
    namen, mails = lade_ma_daten()
    return {"namen": namen, "mails": mails,
            "anzahl": len(namen),
            "hinweis": "Wenn leer: Mitarbeiter neu anlegen unter /mitarbeiter/neu"}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIL-IMPORT
# ═══════════════════════════════════════════════════════════════════════════════

import imaplib, email as _email_mod
from email.header import decode_header as _decode_header


# ── Mail-Route ────────────────────────────────────────────────────────────────
_MAIL_ABRUF = {"aktiv": False}


async def _mail_abruf_ausfuehren():
    """Holt die Mails. Es läuft immer nur EIN Abruf (Knopf und Cron teilen sich die Sperre), sonst würden zwei
    Läufe dieselben Mails gleichzeitig verarbeiten. Gibt None zurück, wenn schon ein Abruf läuft."""
    if _MAIL_ABRUF["aktiv"]:
        return None
    _MAIL_ABRUF["aktiv"] = True
    try:
        # WICHTIG: fetch_mails nutzt blockierendes IMAP. Direkt in dieser async-Funktion würde ein langsamer oder
        # nicht antwortender Mailserver die GANZE Webseite einfrieren. Darum läuft der Abruf in einem eigenen Thread
        # (mit eigener Ereignisschleife); die Seite bleibt währenddessen bedienbar.
        import asyncio
        return await asyncio.to_thread(lambda: asyncio.run(fetch_mails()))
    finally:
        _MAIL_ABRUF["aktiv"] = False


@app.get("/mails-abrufen", response_class=HTMLResponse)
async def mails_abrufen():
    """Holt ungelesene Mails und verarbeitet sie als Belege."""
    result = await _mail_abruf_ausfuehren()
    if result is None:
        return HTMLResponse(shell("Mails abrufen",
            '<h1 class="page-title">📬 Mails abrufen</h1>'
            '<div class="alert alert-warn">Es läuft gerade schon ein Abruf (z. B. der automatische). '
            'Bitte in einer Minute noch einmal versuchen.</div>'
            '<a href="/belege" class="btn btn-secondary">← Zurück</a>'))

    if "fehler" in result and "importiert" not in result:
        body = f'''
        <div class="alert alert-err">
          <b>Fehler:</b> {result["fehler"]}
        </div>
        <a href="/belege" class="btn btn-secondary">← Zurück</a>'''
    else:
        fehler_html = ""
        if result.get("fehler_details"):
            items = "".join(f'<li style="white-space:pre-wrap;font-family:monospace;font-size:12px">{d}</li>'
                             for d in result["fehler_details"])
            fehler_html = f'<div class="alert alert-warn" style="margin-top:12px"><b>Fehlerdetails:</b><ul>{items}</ul></div>'
        if result.get("hinweise"):
            from html import escape as _he
            items_h = "".join(f'<li style="margin:4px 0;font-size:12px">{_he(d)}</li>' for d in result["hinweise"])
            fehler_html += (f'<div class="alert alert-warn" style="margin-top:12px"><b>⚠ Mails ohne erkannten Anhang:</b>'
                            f'<ul style="padding-left:16px">{items_h}</ul></div>')

        body = f'''
        <h1 class="page-title">📬 Mails abgerufen</h1>
        <div class="alert alert-ok" style="margin-bottom:16px">
          ✓ {result.get("importiert",0)} Mails verarbeitet &nbsp;·&nbsp;
          {result.get("belege_erstellt",0)} Belege erstellt &nbsp;·&nbsp;
          {result.get("duplikate",0)} Duplikate &nbsp;·&nbsp;
          {result.get("fehler",0)} Fehler
        </div>
        {fehler_html}
        <div style="display:flex;gap:8px;margin-top:16px">
          <a href="/belege" class="btn btn-primary">📋 Belege ansehen</a>
          <a href="/unzugeordnet" class="btn btn-secondary">📬 Posteingang</a>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>'''

    return HTMLResponse(shell("Mails abrufen", body))


# ═══════════════════════════════════════════════════════════════════════════════
# VMA-TAGE LOGIK
# ═══════════════════════════════════════════════════════════════════════════════


# ── VMA-Routen ────────────────────────────────────────────────────────────────
@app.get("/reise/{code}/vma-generieren")
async def vma_generieren(code: str):
    """Generiert VMA-Tage aus Belegen und Länder-Einträgen."""
    try:
        db = get_db()
        n = vma_tage_generieren(code.upper(), db)
        db.close()
        return RedirectResponse(f"/reise/{code.upper()}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/reise/{code}/vma/{vid}/speichern")
async def vma_tag_speichern(code: str, vid: int, request: Request):
    """Speichert manuell geänderten VMA-Tag."""
    form = await request.form()
    frueh   = bool(form.get("fruehstueck"))
    mittag  = bool(form.get("mittagessen"))
    abend   = bool(form.get("abendessen"))
    lcode   = (form.get("land_code") or "DE").strip().upper()
    ist_halb= bool(form.get("ist_halber_satz"))
    notiz   = (form.get("notiz") or "").strip()
    # Fluchtklappe: falls die automatische Stadt-/Länder-Erkennung für einen
    # Sonderfall mal nicht ausreicht, kann hier ein eigener Satz eingetragen
    # werden, der dann in jedem Fall Vorrang hat (echte manuelle Übersteuerung,
    # kein Rätselraten mehr nötig).
    voll_manuell = (form.get("voll_manuell") or "").strip()
    halb_manuell = (form.get("halb_manuell") or "").strip()

    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        rcode = code.upper()

        if voll_manuell and halb_manuell:
            # Expliziter manueller Satz -> hat immer Vorrang, keine weitere
            # Automatik-Erkennung nötig.
            voll = float(voll_manuell); halb = float(halb_manuell)
            lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)
        else:
            # WICHTIG (allgemeingültig): Die vollständige Tages-Erkennung
            # (land_fuer_tag) erneut durchlaufen, statt nur den nackten
            # Länder-Satz nachzuschlagen – sonst geht eine bereits erkannte
            # Städte-Zuordnung (z.B. "Spanien – Madrid") bei jedem einfachen
            # Häkchen-Setzen verloren und springt auf den Landesdurchschnitt
            # zurück. Nur wenn der/die Nutzer:in das Land im Dropdown
            # tatsächlich auf ein ANDERES Land als das automatisch erkannte
            # ändert, gilt das als bewusste Übersteuerung -> dann schlichter
            # Länder-Satz ohne Stadt-Rätselraten.
            cur.execute(f"SELECT reise_code, datum FROM vma_tage WHERE id={P}", (vid,))
            r = cur.fetchone()
            g = lambda k,i: r[k] if hasattr(r,'keys') else r[i]
            tag_datum = _datum_parsen(g("datum",1)) if r else None
            auto_override = None
            auto_lcode = None
            if tag_datum:
                cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (rcode,))
                rr = cur.fetchone()
                gr = lambda k,i: rr[k] if hasattr(rr,'keys') else rr[i]
                ab_d = _datum_parsen(gr("abreise",0)); zu_d = _datum_parsen(gr("rueckkehr",1))
                tage_ges = (zu_d - ab_d).days + 1
                ist_letzter = (tag_datum == zu_d)
                eintaegig_ges = (tage_ges == 1)
                auto_lcode, _, _, auto_override = land_fuer_tag(rcode, tag_datum, db, ist_letzter, eintaegig_ges)

            if lcode == auto_lcode and auto_override:
                voll = auto_override["voll"]; halb = auto_override["halb"]
                ort_txt = auto_override.get("ort")
                lname = (f'{VMA_SAETZE.get(lcode, {}).get("name", lcode)} – {ort_txt}'
                         if ort_txt else VMA_SAETZE.get(lcode, {}).get("name", lcode))
            else:
                # WICHTIG: Immer zuerst die importierte Liste (offizielle
                # BMF-Quelle) fragen, genau wie bei der Automatik – die
                # statische VMA_SAETZE im Code ist nur ein Rückfall.
                info = vma_fuer_land_erweitert(cur, lcode, ort=None)
                voll = info["voll"]; halb = info["halb"]
                lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)

        brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, mittag, abend)
        cur.execute(f"""UPDATE vma_tage SET
            land_code={P}, land_name={P}, vma_satz_voll={P}, vma_satz_halb={P},
            ist_halber_satz={P}, fruehstueck={P}, mittagessen={P}, abendessen={P},
            vma_brutto={P}, vma_netto={P}, quelle='manuell', notiz={P}
            WHERE id={P}""",
            (lcode, lname, voll, halb, ist_halb, frueh, mittag, abend,
             brutto, netto, notiz or None, vid))
        db.commit(); cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{code.upper()}"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/reise/{code}/vma-alle-zuruecksetzen")
def vma_alle_zuruecksetzen(code: str, request: Request):
    """
    Setzt ALLE Tage der Reise auf einmal von "manuell" zurück auf
    "automatisch" und berechnet danach neu – statt jeden betroffenen Tag
    einzeln über "↺ Auto" zurücksetzen zu müssen. Sinnvoll, wenn nach einer
    Reihe von Fixes mehrere Tage noch alte, eingefrorene Werte zeigen (z.B.
    weil sie vor einem Fix schon manuell/automatisch verändert wurden) und
    uneinheitliche Sätze für denselben Ort anzeigen.
    """
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE vma_tage SET quelle='auto' WHERE reise_code={P}", (rcode,))
        db.commit()
        vma_tage_generieren(rcode, db)
        cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{rcode}/vma-debug"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.post("/reise/{code}/vma/{vid}/reset-und-neu")
def vma_tag_reset_und_neu(code: str, vid: int, request: Request):
    """
    Kombiniert Zurücksetzen (quelle -> auto) und die komplette Neuberechnung
    der Reise in EINEM Server-Aufruf, ohne Zwischenschritt/Redirect – schließt
    jede Möglichkeit aus, dass ein Klick zwischen zwei Einzelschritten verloren
    geht oder die Reihenfolge durcheinanderkommt.
    """
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE vma_tage SET quelle='auto' WHERE id={P}", (vid,))
        db.commit()
        vma_tage_generieren(rcode, db)
        cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{rcode}/vma-debug"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.post("/reise/{code}/vma/{vid}/hinweis-ok")
def vma_hinweis_ok(code: str, vid: int, request: Request):
    """Bestätigt den Hinweis „Satz geändert“ an einem Tag (der Hinweis verschwindet)."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE vma_tage SET satz_hinweis=NULL WHERE id={P} AND reise_code={P}", (vid, code.upper()))
        db.commit(); cur.close(); db.close()
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)
    return RedirectResponse(request.headers.get("referer") or f"/reise/{code.upper()}", status_code=303)


@app.post("/reise/{code}/vma/{vid}/zuruecksetzen")
def vma_tag_zuruecksetzen(code: str, vid: int, request: Request):
    """
    Setzt einen einzelnen VMA-Tag von "manuell" zurück auf "automatisch" –
    damit greift bei der nächsten "VMA neu berechnen" wieder die normale
    Erkennungslogik (Flug-/Bahn-Segment, Hotel, Rückreisetag-Regel) für
    GENAU DIESEN Tag. Nötig, weil ein einmal manuell gesetzter/über das
    Portal befüllter Tag sonst dauerhaft von der Neuberechnung übersprungen
    wird, selbst wenn sich die zugrunde liegende Logik später verbessert.
    """
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE vma_tage SET quelle='auto' WHERE id={P}", (vid,))
        db.commit(); cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{code.upper()}"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.get("/reise/{code}/vma-debug", response_class=HTMLResponse)
def vma_debug(code: str):
    """Zeigt für jeden Tag der Reise, was die Rückreisetag-Länderregel sieht
    und warum sie zu ihrem Ergebnis kommt – zum gezielten Fehlersuchen."""
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (rcode,))
        r = cur.fetchone()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'))
        g = lambda row,k,i: row[k] if hasattr(row,'keys') else row[i]
        ab = _datum_parsen(g(r,"abreise",0)); zu = _datum_parsen(g(r,"rueckkehr",1))
        tage = (zu - ab).days + 1
        eintaegig = (tage == 1)

        bloecke = ""
        for i in range(tage):
            tag = ab + timedelta(days=i)
            ist_letzter_tag = (i == tage - 1)

            # Gespeicherten Zustand dieses Tages immer mit anzeigen – zeigt
            # sofort, ob der Tag als "manuell" eingefroren ist und deshalb
            # von "VMA neu berechnen" übersprungen wird.
            cur.execute(f"""SELECT id, quelle, land_code, land_name, vma_satz_voll, vma_satz_halb
                            FROM vma_tage WHERE reise_code={P} AND datum={P}""",
                        (rcode, tag.isoformat()))
            gerow = cur.fetchone()
            if gerow:
                vid_tag = g(gerow,"id",0)
                q = g(gerow,"quelle",1) or "?"
                lc = g(gerow,"land_code",2) or "?"
                ln = g(gerow,"land_name",3) or "?"
                vv = g(gerow,"vma_satz_voll",4); vh = g(gerow,"vma_satz_halb",5)
                farbe_q = "#ef4444" if q == "manuell" else "#059669"
                fix_btn = (f'<form method="post" action="/reise/{rcode}/vma/{vid_tag}/reset-und-neu" style="display:inline">'
                           f'<button type="submit" style="font-size:12px;color:white;background:#2563eb;'
                           f'border:none;border-radius:6px;padding:4px 10px;cursor:pointer;margin-left:8px">'
                           f'🔧 Diesen Tag jetzt zurücksetzen &amp; neu berechnen</button></form>'
                           if q == "manuell" else "")
                gespeichert_txt = (f'<div style="font-size:12px;margin-top:4px">Gespeichert: '
                                    f'<b style="color:{farbe_q}">quelle={q}</b> · {ln} ({lc}) · '
                                    f'voll {float(vv):.2f} € / halb {float(vh):.2f} €'
                                    f'{" ⚠ manuell -> wird bei Neuberechnung ÜBERSPRUNGEN" if q=="manuell" else ""}'
                                    f'{fix_btn}</div>')
            else:
                gespeichert_txt = '<div style="font-size:12px;color:#ef4444;margin-top:4px">Kein vma_tage-Eintrag vorhanden</div>'

            if not ist_letzter_tag:
                # Hotel-Beleg-Abgleich nachvollziehbar machen: welche Hotels
                # wurden für diesen Tag geprüft, welches hat gematcht (falls
                # keins matcht, greift der Länder-Rückfall bzw. Deutschland).
                cur.execute(f"""SELECT id, anbieter, hotel_name, land_beleg,
                                hotel_checkin_datum, hotel_checkout_datum
                                FROM belege WHERE reise_code={P} AND transportart='Hotel'""",
                            (rcode,))
                hotel_kandidaten = cur.fetchall()
                hotel_diag = ""
                treffer_gefunden = False
                for hb in hotel_kandidaten:
                    hci = _datum_parsen(g(hb,"hotel_checkin_datum",4))
                    hco = _datum_parsen(g(hb,"hotel_checkout_datum",5))
                    matcht = bool(hci and hco and hci <= tag <= hco)
                    if matcht: treffer_gefunden = True
                    farbe_h = "#059669" if matcht else "#94a3b8"
                    hotel_diag += (f'<div style="font-size:12px;color:{farbe_h};margin:2px 0">'
                                    f'Beleg #{g(hb,"id",0)}: {g(hb,"hotel_name",2) or g(hb,"anbieter",1)} · '
                                    f'Land "{g(hb,"land_beleg",3)}" · '
                                    f'Checkin {hci.strftime("%d.%m.") if hci else "?"} – '
                                    f'Checkout {hco.strftime("%d.%m.") if hco else "?"} '
                                    f'-> Treffer: {matcht}</div>')
                if not hotel_kandidaten:
                    hotel_diag = '<i style="font-size:12px;color:#ef4444">Keine Hotel-Belege für diese Reise gefunden</i>'
                elif not treffer_gefunden:
                    hotel_diag += ('<div style="font-size:12px;color:#ef4444;margin-top:4px">'
                                    'Kein Hotel deckt diesen Tag ab -> fällt auf "Standard" (Deutschland) zurück, '
                                    'außer ein Flugsegment oder ein manueller Reise-Land-Eintrag greift zuerst.</div>')
                bloecke += f"""<div class="card" style="margin-bottom:12px"><div class="card-body">
                  <b>{tag.strftime("%d.%m.%Y")}</b> – kein Rückreisetag, normale Logik (Flug-Segment/Hotel) greift.
                  <div style="margin-top:6px;font-size:12px;font-weight:600">Hotel-Abgleich:</div>
                  {hotel_diag}
                  {gespeichert_txt}
                </div></div>"""
                continue
            land, override, diag = land_fuer_letzten_tag(rcode, tag, db, eintaegig, debug=True)
            seg_html = ""
            for s in diag["segmente_heute"]:
                farbe = "#059669" if s["treffer"] else "#94a3b8"
                seg_html += (f'<div style="font-size:12px;color:{farbe};margin:2px 0">'
                             f'Beleg #{s["beleg_id"]}: abreise_datum="{s["abreise_datum_im_segment"]}" '
                             f'von_iata="{s["von_iata"]}" von_ort="{s["von_ort"]}" '
                             f'abreise_zeit="{s["abreise_zeit"]}" -> Treffer: {s["treffer"]}</div>')
            override_txt = (f' · Städte-Satz: {override["ort"]} (voll {override["voll"]:.2f} € / '
                             f'halb {override["halb"]:.2f} €)') if override else ' · kein Städte-Satz gefunden, Landesdurchschnitt greift'
            bloecke += f"""<div class="card" style="margin-bottom:12px">
              <div class="card-body">
                <b>{tag.strftime("%d.%m.%Y")} (Rückreisetag)</b><br>
                <span style="font-size:12px;color:var(--muted)">Gesucht: {diag['gesuchtes_datum']}</span><br>
                <span style="font-size:12px">Belege mit passendem event_datum_von/bis: {diag['belege_gefunden']}
                &nbsp;·&nbsp; Segmente insgesamt in diesen Belegen: {diag['segmente_gesamt']}</span>
                <div style="margin-top:6px">{seg_html or '<i style="font-size:12px;color:#ef4444">Keine Segmente gefunden</i>'}</div>
                <div style="margin-top:6px;font-weight:700">Berechnet: {land or 'None (Rückfall auf normale Logik)'}{override_txt if land else ''}</div>
                {f'<div style="font-size:12px;color:#ef4444;margin-top:4px">{diag["hinweis"]}</div>' if diag["hinweis"] else ''}
                {gespeichert_txt}
              </div>
            </div>"""
        cur.close(); db.close()
        content = f"""<h1 class="page-title">🔍 VMA-Debug {rcode}</h1>
        <form method="post" action="/reise/{rcode}/vma-alle-zuruecksetzen" style="margin-bottom:16px">
          <button type="submit" class="btn btn-primary"
                  onclick="return confirm('ALLE Tage dieser Reise von manuell zurück auf automatisch setzen und neu berechnen?')">
            🔄 Alle Tage zurücksetzen &amp; neu berechnen
          </button>
        </form>
        {bloecke}"""
        return HTMLResponse(shell(f"VMA-Debug {rcode}", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:1500]}</pre>'))


@app.post("/reise/{code}/vma/{vid}/trennungspauschale")
async def vma_trennungspauschale_speichern(code: str, vid: int, request: Request):
    """Speichert eine manuell korrigierte Trennungspauschale für einen Wochenend-Reisetag."""
    form = await request.form()
    try:
        wert = float((form.get("trennungspauschale") or "0").strip())
    except ValueError:
        wert = 0.0
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""UPDATE vma_tage SET trennungspauschale={P},
                        trennungspauschale_quelle='manuell' WHERE id={P}""", (wert, vid))
        db.commit(); cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{code.upper()}"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/reise/{code}/vma/{vid}/tatsaechliche-zeit")
async def vma_tatsaechliche_zeit_speichern(code: str, vid: int, request: Request):
    """
    Speichert die tatsächliche Uhrzeit des Reiseantritts (erster Reisetag) bzw.
    der Reisebeendigung (letzter Reisetag) – kann vom Abflug/Ankunft abweichen,
    z.B. wenn der Mitarbeiter schon vor dem Flug losfährt oder nach der
    Zugankunft noch mit dem eigenen Auto nach Hause fährt.
    Rechnet direkt danach auch die Trennungspauschale neu (12-Uhr-Regel an
    Wochenend-An-/Abreisetagen), statt erst auf die nächste manuelle
    "VMA neu berechnen" zu warten – sonst sähe der Reisende nach dem
    Eintragen der Uhrzeit fälschlich weiter den alten Pauschal-Wert.
    """
    form = await request.form()
    uhrzeit = (form.get("tatsaechliche_uhrzeit") or "").strip() or None
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT reise_code, datum, ist_halber_satz, trennungspauschale_quelle
                        FROM vma_tage WHERE id={P}""", (vid,))
        row = cur.fetchone()
        g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
        cur.execute(f"UPDATE vma_tage SET tatsaechliche_uhrzeit={P} WHERE id={P}", (uhrzeit, vid))

        if row and (g("trennungspauschale_quelle",3) or "auto") != "manuell":
            rcode2 = g("reise_code",0)
            tag = _datum_parsen(g("datum",1))
            ist_halb2 = bool(g("ist_halber_satz",2))
            cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (rcode2,))
            rr = cur.fetchone()
            gr = lambda k,i: rr[k] if hasattr(rr,'keys') else rr[i]
            abreise_d = _datum_parsen(gr("abreise",0)); rueckkehr_d = _datum_parsen(gr("rueckkehr",1))
            ist_erster = (tag == abreise_d); ist_letzter = (tag == rueckkehr_d)
            neue_trennung = trennungspauschale_berechnen(tag, ist_halb2, ist_erster, ist_letzter, uhrzeit)
            cur.execute(f"UPDATE vma_tage SET trennungspauschale={P} WHERE id={P}", (neue_trennung, vid))

        db.commit(); cur.close(); db.close()
        ziel = request.headers.get("referer") or f"/reise/{code.upper()}"
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/reise/{code}/uebersicht", response_class=HTMLResponse)
def reise_uebersicht_redirect(code: str):
    """Die separate Tages-Übersicht wurde in die Reise-Detailseite integriert
    (Tagesverlauf & VMA) – alte Links/Lesezeichen leiten hierher um."""
    return RedirectResponse(f"/reise/{code.upper()}", status_code=301)


# ── System-Routen ─────────────────────────────────────────────────────────────
@app.post("/beleg/{bid}/kurs")
async def beleg_kurs_speichern(bid: int, request: Request):
    """Speichert Wechselkurs für Auslandsbeleg."""
    form = await request.form()
    try:
        kurs = float(form.get("kurs_eur") or 0) or None
        kurs_datum = (form.get("kurs_datum") or "").strip() or None
        kurs_quelle = (form.get("kurs_quelle") or "").strip() or None
        P = ph()
        db = get_db(); cur = db.cursor()
        # Betrag_eur berechnen
        cur.execute(f"SELECT betrag_brutto FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        roh = (r[0] if isinstance(r,tuple) else r["betrag_brutto"]) if r else None
        brutto = float(roh) if roh is not None else 0     # Belege ohne Betrag (z. B. Quittungen): nur Kurs speichern
        betrag_eur = round(brutto * kurs, 2) if kurs and brutto else None
        cur.execute(f"""UPDATE belege SET
            kurs_eur={P}, betrag_eur={P}, kurs_datum={P}, kurs_quelle={P}
            WHERE id={P}""",
            (kurs, betrag_eur, kurs_datum, kurs_quelle, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/beleg/{bid}/gesamtbetrag")
async def beleg_gesamtbetrag_speichern(bid: int, request: Request):
    """Speichert den finalen Euro-Betrag (gemäß Abrechnung) und Nebenkosten
    (z.B. Kreditkartengebühr) – hat Vorrang vor dem KI-geschätzten Euro-Betrag."""
    form = await request.form()
    try:
        def parse_num(feld):
            v = (form.get(feld) or "").strip()
            return float(v) if v else None
        betrag_eur_final = parse_num("betrag_eur_final")
        nebenkosten_eur = parse_num("nebenkosten_eur")
        nebenkosten_beschreibung = (form.get("nebenkosten_beschreibung") or "").strip() or None
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""UPDATE belege SET
            betrag_eur_final={P}, nebenkosten_eur={P}, nebenkosten_beschreibung={P}
            WHERE id={P}""",
            (betrag_eur_final, nebenkosten_eur, nebenkosten_beschreibung, bid))
        auslandsentgelt_neu_berechnen(bid, db)
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


def reiseplan_daten_laden(reise_code: str):
    """
    Sammelt alle Reiseplan-relevanten Ereignisse für eine Reise – BEWUSST OHNE
    jegliche Preisangaben, nur Zeiten, Buchungsnummern, Adressen und
    Ansprechpartner. Gedacht für den Versand an den Reisenden vor Abreise.
    Gibt {"titel","abreise","rueckkehr","tage": {datum: [ereignis,...]}} zurück,
    oder None wenn die Reise nicht existiert.
    """
    db = get_db(); cur = db.cursor()
    P = ph()
    cur.execute(f"SELECT titel, abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
    r = cur.fetchone()
    if not r:
        cur.close(); db.close()
        return None
    g = lambda row,k,i: row[k] if hasattr(row,'keys') else row[i]
    titel = g(r,"titel",0); abreise = g(r,"abreise",1); rueckkehr = g(r,"rueckkehr",2)

    ereignisse = []  # (datum, sortierzeit, ereignis-dict)

    cur.execute(f"""SELECT transportart, anbieter, buchungscode, rechnungsnummer,
                    ki_json, hotel_name, hotel_adresse, hotel_checkin_datum, hotel_checkin_zeit,
                    hotel_checkout_datum, hotel_checkout_zeit, transportart_freitext,
                    event_datum_von, event_datum_bis, event_ort_von
                    FROM belege WHERE reise_code={P}
                    AND transportart IN ('Flug','Bahn','Mietwagen','Hotel','Sonstiges')""", (reise_code,))
    for b in cur.fetchall():
        typ = g(b,"transportart",0); anbieter = g(b,"anbieter",1) or ""
        buchungscode = g(b,"buchungscode",2) or g(b,"rechnungsnummer",3) or ""
        if typ == "Sonstiges":
            # Allgemeingültig: JEDE "Sonstiges"-Position mit einem echten
            # Datums-ZEITRAUM (von != bis, z.B. Parkplatzreservierung am
            # Flughafen: Einstellen am Abreisetag, Abholen bei Rückkehr) wird
            # wie ein Hotel mit Start-/Ende-Eintrag behandelt – nicht nur
            # Parken speziell, auch andere mehrtägige "Sonstiges"-Belege.
            # Belege mit nur einem Einzeldatum (die meisten Tank-/Maut-/
            # Parkscheine) werden bewusst NICHT aufgenommen, sonst würde der
            # Reiseplan mit lauter Kleinbelegen überladen.
            freitext = g(b,"transportart_freitext",11) or "Sonstiges"
            s_von = _datum_parsen(g(b,"event_datum_von",12))
            s_bis = _datum_parsen(g(b,"event_datum_bis",13))
            s_ort = g(b,"event_ort_von",14) or ""
            if s_von and s_bis and s_von != s_bis:
                ist_park = "park" in freitext.lower()
                ereignisse.append((s_von, "", {
                    "icon": "🅿️" if ist_park else "📌", "typ": "parken" if ist_park else "sonstiges",
                    "titel": f"{freitext}: Beginn" + (f" – {anbieter}" if anbieter else ""),
                    "zeit": "", "sub": s_ort,
                    "extra": f"Buchungsnr.: {buchungscode}" if buchungscode else "", "ist_buchungscode": True}))
                ereignisse.append((s_bis, "", {
                    "icon": "🅿️" if ist_park else "📌", "typ": "parken" if ist_park else "sonstiges",
                    "titel": f"{freitext}: Ende" + (f" – {anbieter}" if anbieter else ""),
                    "zeit": "", "sub": s_ort, "extra": "", "ist_buchungscode": False}))
            continue
        if typ == "Hotel":
            hname = g(b,"hotel_name",5) or anbieter or "Hotel"
            hadresse = g(b,"hotel_adresse",6) or ""
            ci_d = _datum_parsen(g(b,"hotel_checkin_datum",7)); ci_z = g(b,"hotel_checkin_zeit",8) or ""
            co_d = _datum_parsen(g(b,"hotel_checkout_datum",9)); co_z = g(b,"hotel_checkout_zeit",10) or ""
            if ci_d:
                ereignisse.append((ci_d, ci_z or "14:00", {
                    "icon": "🏨", "typ": "hotel", "titel": f"Check-in: {hname}", "zeit": ci_z, "sub": hadresse,
                    "extra": f"Buchungsnr.: {buchungscode}" if buchungscode else "", "ist_buchungscode": True}))
            if co_d:
                ereignisse.append((co_d, co_z or "11:00", {
                    "icon": "🏨", "typ": "hotel", "titel": f"Check-out: {hname}", "zeit": co_z, "sub": hadresse,
                    "extra": "", "ist_buchungscode": False}))
        else:
            segs = segmente_aus_ki_json(g(b,"ki_json",4))
            icon = {"Flug": "✈", "Bahn": "🚆", "Mietwagen": "🚗"}.get(typ, "📍")
            typ_key = {"Flug": "flug", "Bahn": "bahn", "Mietwagen": "mietwagen"}.get(typ, "sonstiges")
            for s in segs:
                d_ab = _datum_parsen(s.get("abreise_datum"))
                if not d_ab: continue
                von_ort = s.get("von_ort") or s.get("von_iata") or ""
                nach_ort = s.get("nach_ort") or s.get("nach_iata") or ""
                nummer = s.get("transport_nummer") or anbieter
                terminal = s.get("abreise_terminal")
                zeit_txt = f'{s.get("abreise_zeit","")}–{s.get("ankunft_zeit","")}'.strip("–")
                ereignisse.append((d_ab, s.get("abreise_zeit") or "", {
                    "icon": icon, "typ": typ_key, "titel": f"{nummer}: {von_ort} → {nach_ort}", "zeit": zeit_txt,
                    "sub": f"Terminal {terminal}" if terminal else "",
                    "extra": f"Buchungsnr.: {buchungscode}" if buchungscode else "", "ist_buchungscode": True}))

    TERMIN_ICON = {"termin": "🤝", "kundenbesuch": "🤝", "fahrt": "🚕",
                   "mietwagen": "🚗", "hotel": "🏨", "sonstiges": "📌"}
    cur.execute(f"""SELECT datum, uhrzeit_von, uhrzeit_bis, titel, typ, ort, ansprechpartner, telefon, notiz
                    FROM termine WHERE reise_code={P} ORDER BY datum, uhrzeit_von""", (reise_code,))
    for t in cur.fetchall():
        d = _datum_parsen(g(t,"datum",0))
        if not d: continue
        von = g(t,"uhrzeit_von",1) or ""; bis = g(t,"uhrzeit_bis",2) or ""
        titel_t = g(t,"titel",3) or "Termin"; typ_t = g(t,"typ",4) or "termin"
        ort_t = g(t,"ort",5) or ""; ansprech_t = g(t,"ansprechpartner",6) or ""
        tel_t = g(t,"telefon",7) or ""; notiz_t = g(t,"notiz",8) or ""
        sub_parts = [p for p in (ort_t, f"👤 {ansprech_t}" if ansprech_t else "",
                                  f"📞 {tel_t}" if tel_t else "") if p]
        ereignisse.append((d, von, {
            "icon": TERMIN_ICON.get(typ_t, "📌"), "typ": "termin", "titel": titel_t,
            "zeit": f"{von}–{bis}" if von and bis else von,
            # WICHTIG: "extra" ist hier eine NOTIZ, kein Buchungscode – bleibt
            # deshalb auch in der Buchungscode-freien PDF-Variante erhalten.
            "sub": " · ".join(sub_parts), "extra": notiz_t, "ist_buchungscode": False}))
    cur.close(); db.close()

    tage = {}
    for d, _, ev in ereignisse:
        tage.setdefault(d, []).append(ev)
    for d in tage:
        tage[d].sort(key=lambda e: e.get("zeit") or "")
    return {"titel": titel, "abreise": abreise, "rueckkehr": rueckkehr, "tage": tage}


@app.get("/reise/{code}/reiseplan", response_class=HTMLResponse)
def reiseplan_anzeigen(code: str):
    """Reiseplan für den Reisenden – nur Zeiten/Orte/Buchungsnummern, KEINE Preise."""
    rcode = code.upper()
    daten = reiseplan_daten_laden(rcode)
    if not daten:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'))
    wochentage = ["Mo","Di","Mi","Do","Fr","Sa","So"]
    tage_html = ""
    for d in sorted(daten["tage"].keys()):
        datum_txt = f"{wochentage[d.weekday()]} {d.strftime('%d.%m.%Y')}"
        zeilen = ""
        for ev in daten["tage"][d]:
            sub_html = f'<div style="font-size:12px;color:var(--muted);margin-top:2px">{ev["sub"]}</div>' if ev["sub"] else ""
            extra_html = f'<div style="font-size:11px;color:#7c3aed;margin-top:2px">{ev["extra"]}</div>' if ev["extra"] else ""
            zeilen += f"""<div style="display:flex;gap:12px;padding:10px 16px;border-bottom:1px solid var(--border)">
                <div style="width:90px;flex-shrink:0;font-size:13px;font-weight:600;color:var(--muted)">{ev["zeit"] or "–"}</div>
                <div style="flex:1">
                  <div style="font-size:14px"><span style="margin-right:6px">{ev["icon"]}</span>{ev["titel"]}</div>
                  {sub_html}{extra_html}
                </div>
              </div>"""
        tage_html += f"""<div class="card" style="margin-bottom:16px">
          <div class="card-header"><span class="card-title">{datum_txt}</span></div>
          <div>{zeilen}</div>
        </div>"""
    content = f"""
    {_ueberschneidungs_banner_fuer_reise(rcode)}
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
      <div>
        <h1 class="page-title" style="margin:0">🗺 Reiseplan {rcode}</h1>
        <div style="color:var(--muted);font-size:13px">{daten["titel"]} · {fmt_date(daten["abreise"])} – {fmt_date(daten["rueckkehr"])}</div>
      </div>
      <div style="display:flex;gap:8px">
        <a href="/reise/{rcode}/termin/neu?zurueck=reiseplan" class="btn btn-primary">+ Ereignis</a>
        <a href="/reise/{rcode}/reiseplan/pdf" class="btn btn-secondary">📄 PDF für Reisemappe Mitarbeiter</a>
        <a href="/reise/{rcode}/reiseplan/pdf?ohne_buchungscode=1" class="btn btn-secondary">📄 PDF zum Emailversand</a>
        <a href="/reise/{rcode}/kalender" class="btn btn-secondary">📅 Outlook-Einladungen</a>
        <a href="/reise/{rcode}" class="btn btn-secondary">← Reise</a>
      </div>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-bottom:16px">
      Reine Zeit-/Orts-/Buchungsübersicht ohne Preisangaben – zum Versand an den Reisenden vor Abreise.
      Über "+ Ereignis" lassen sich Kundenbesuche (mit Adresse/Ansprechpartner/Telefon), Taxifahrten,
      Messebesuche o.ä. manuell ergänzen.</p>
    {tage_html or '<div class="alert alert-warn">Noch keine Flug-/Bahn-/Hotel-/Termindaten vorhanden.</div>'}
    """
    return HTMLResponse(shell(f"Reiseplan {rcode}", content, "reisen"))


def _reiseplan_pdf_bauen(rcode: str, daten: dict, ohne_buchungscode: bool) -> bytes:
    """
    Baut das Reiseplan-PDF. Zwei Varianten über ohne_buchungscode steuerbar:
    - False: für den Reisenden selbst, mit allen Buchungscodes (wie bisher).
    - True: identischer Inhalt, aber ohne Buchungscodes – z.B. zum Weitergeben
      an Kunden/Dritte, ohne dass die eigenen Buchungsreferenzen sichtbar sind.
    Tabellen-Layout mit leichten Hintergrundfarben je Ereignistyp, damit
    Flug/Bahn/Hotel/Termin auf einen Blick unterscheidbar sind, und einem
    farbigen Tagesbalken für klare Tagesumbrüche.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors

    def esc(s):
        return (str(s) if s is not None else "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    # Leichte, helle Hintergrundfarben je Ereignistyp
    FARBEN = {
        "flug":      colors.HexColor("#dbeafe"),
        "bahn":      colors.HexColor("#dcfce7"),
        "mietwagen": colors.HexColor("#ede9fe"),
        "hotel":     colors.HexColor("#fef3c7"),
        "termin":    colors.HexColor("#fce7f3"),
        "parken":    colors.HexColor("#e2e8f0"),
        "sonstiges": colors.HexColor("#f1f5f9"),
    }
    TAGESBALKEN_FARBE = colors.HexColor("#1e3a5f")

    # WICHTIG: reportlab/PDF-Standardschriften stellen komplexe, mehrfarbige
    # Emojis (🏨🅿️📌🤝 etc.) nicht zuverlässig dar – landet oft als "■"
    # (fehlendes Glyphen-Rechteck). Statt Emojis daher feste, klar lesbare
    # Text-Kürzel je Ereignistyp; die HTML-Ansicht im Browser behält die
    # echten Emojis, das betrifft NUR das PDF.
    PDF_TYP_LABEL = {
        "flug": "FLUG", "bahn": "BAHN", "mietwagen": "MIETWAGEN",
        "hotel": "HOTEL", "termin": "TERMIN", "parken": "PARKEN", "sonstiges": "SONSTIGES",
    }

    def zeit_formatieren(zeit_roh: str) -> str:
        """
        Formatiert eine Zeit-/Zeitraumangabe für die schmale Zeitspalte.
        WICHTIG: Bei einem Zeitraum (z.B. "12:00–13:40") NIE dem Paragraph
        das automatische Umbrechen überlassen – das bricht sonst mitten in
        der Uhrzeit um (z.B. "13:" / "40"), weil der Bindestrich als
        Umbruchstelle gilt und der Rest dann nicht mehr passt. Stattdessen
        gezielt NACH der ersten Uhrzeit umbrechen, sieht sauber aus und ist
        bei jeder Spaltenbreite vorhersehbar.
        """
        if not zeit_roh:
            return "–"
        if "–" in zeit_roh:
            ab, an = zeit_roh.split("–", 1)
            return f"{esc(ab)}<br/>–{esc(an)}"
        return esc(zeit_roh)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=16*mm, rightMargin=16*mm, topMargin=16*mm, bottomMargin=16*mm)
    styles = getSampleStyleSheet()
    titel_stil = ParagraphStyle("EvTitel", parent=styles["Normal"], fontSize=10, fontName="Helvetica-Bold", leading=13)
    sub_stil = ParagraphStyle("EvSub", parent=styles["Normal"], fontSize=8.5, textColor=colors.HexColor("#475569"), leading=11)
    zeit_stil = ParagraphStyle("EvZeit", parent=styles["Normal"], fontSize=9, fontName="Helvetica-Bold", alignment=1, leading=11)

    story = [Paragraph(f"Reiseplan {esc(rcode)}", styles["Title"]),
             Paragraph(esc(daten["titel"]), styles["Heading2"]),
             Paragraph(f"{fmt_date(daten['abreise'])} – {fmt_date(daten['rueckkehr'])}", styles["Normal"]),
             Spacer(1, 5*mm)]

    wochentage = ["Montag","Dienstag","Mittwoch","Donnerstag","Freitag","Samstag","Sonntag"]
    for d in sorted(daten["tage"].keys()):
        tagesblock = []
        # Farbiger Tagesbalken als eigene Mini-Tabelle (Paragraphs selbst
        # können keinen Hintergrund haben) – sorgt für einen klaren,
        # auffälligen Umbruch zwischen den Tagen.
        tag_tbl = Table([[Paragraph(f'<font color="white"><b>{wochentage[d.weekday()]}, {d.strftime("%d.%m.%Y")}</b></font>',
                                     styles["Normal"])]], colWidths=[178*mm])
        tag_tbl.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1), TAGESBALKEN_FARBE),
            ("TOPPADDING",(0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ("LEFTPADDING",(0,0),(-1,-1), 8),
        ]))
        tagesblock.append(tag_tbl)
        tagesblock.append(Spacer(1, 2*mm))

        for ev in daten["tage"][d]:
            farbe = FARBEN.get(ev.get("typ"), FARBEN["sonstiges"])
            label = PDF_TYP_LABEL.get(ev.get("typ"), "SONSTIGES")
            inhalt = f'<font size="7" color="#475569"><b>{label}</b></font>&nbsp;&nbsp;{esc(ev["titel"])}'
            text_teile = [Paragraph(inhalt, titel_stil)]
            if ev.get("sub"):
                text_teile.append(Paragraph(esc(ev["sub"]), sub_stil))
            # Buchungscode nur zeigen, wenn NICHT die buchungscode-freie
            # Variante gewählt ist UND dieses "extra"-Feld tatsächlich ein
            # Buchungscode ist (bei Terminen ist "extra" nur eine Notiz und
            # bleibt in jeder Variante erhalten).
            if ev.get("extra") and not (ohne_buchungscode and ev.get("ist_buchungscode")):
                text_teile.append(Paragraph(esc(ev["extra"]), sub_stil))

            zeile = Table([[Paragraph(zeit_formatieren(ev["zeit"]), zeit_stil), text_teile]],
                           colWidths=[24*mm, 154*mm])
            zeile.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,-1), farbe),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
                ("TOPPADDING",(0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
                ("LEFTPADDING",(0,0),(0,0), 4), ("LEFTPADDING",(1,0),(1,0), 8),
                ("LINEBELOW",(0,0),(-1,-1), 0.5, colors.white),
            ]))
            tagesblock.append(zeile)
        tagesblock.append(Spacer(1, 4*mm))
        # KeepTogether: Tagesbalken soll nicht alleine am Seitenende hängen,
        # ohne dass noch Inhalt des Tages mit auf dieselbe Seite passt.
        story.append(KeepTogether(tagesblock[:2] + [tagesblock[2]] if len(tagesblock) > 2 else tagesblock))
        story.extend(tagesblock[3:] if len(tagesblock) > 3 else [])

    doc.build(story)
    return buf.getvalue()


@app.get("/reise/{code}/reiseplan/pdf")
def reiseplan_pdf(code: str, ohne_buchungscode: bool = False):
    """
    Reiseplan als PDF. Zwei Varianten über den Query-Parameter:
    - /reise/{code}/reiseplan/pdf  -> für den Reisenden, mit Buchungscodes
    - /reise/{code}/reiseplan/pdf?ohne_buchungscode=1 -> identisch, aber ohne
      Buchungscodes (z.B. zum Weitergeben an Kunden/Dritte)
    """
    rcode = code.upper()
    daten = reiseplan_daten_laden(rcode)
    if not daten:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>'))
    try:
        pdf_bytes = _reiseplan_pdf_bauen(rcode, daten, ohne_buchungscode)
        from fastapi.responses import Response
        dateiname = f"Reiseplan_{rcode}" + ("_Emailversand" if ohne_buchungscode else "_Reisemappe") + ".pdf"
        return Response(content=pdf_bytes, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename={dateiname}"})
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:800]}</pre>'))


@app.get("/reise/{code}/abschluss", response_class=HTMLResponse)
def reise_abschluss(code: str):
    """
    Abschluss-Übersicht einer Reise:
    - VMA-Tabelle komplett
    - Kostenaufstellung (nur Rechnungen/Quittungen)
    - Hinweise bei fehlenden Rechnungen
    - Wechselkurs-Status
    """
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()

        cur.execute(f"""SELECT code,titel,abreise,rueckkehr,notiz
            FROM reisen WHERE code={P}""", (rcode,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Reise nicht gefunden</div>'))
        def g(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        titel=g(r,"titel",1); ab=g(r,"abreise",2); zu=g(r,"rueckkehr",3)

        # Mitarbeiter
        cur.execute(f"""SELECT m.kuerzel, m.klarname FROM mitarbeiter m
            JOIN reise_mitarbeiter rm ON rm.kuerzel=m.kuerzel
            WHERE rm.reise_code={P} ORDER BY m.klarname""", (rcode,))
        ma_rows = cur.fetchall()

        # VMA-Tage
        cur.execute(f"""SELECT datum,land_code,land_name,vma_satz_voll,vma_satz_halb,
            ist_halber_satz,fruehstueck,mittagessen,abendessen,vma_brutto,vma_netto,
            trennungspauschale,tatsaechliche_uhrzeit
            FROM vma_tage WHERE reise_code={P} ORDER BY datum""", (rcode,))
        vma_rows = cur.fetchall()

        # Tatsächliche Reisezeit: Uhrzeit vom ersten bzw. letzten Reisetag
        antritt_zeit = g(vma_rows[0],"tatsaechliche_uhrzeit",12) if vma_rows else None
        ende_zeit = g(vma_rows[-1],"tatsaechliche_uhrzeit",12) if vma_rows else None

        # Reise-/Arbeitszeiten je Person und Tag (vom Reisenden im Portal erfasst)
        cur.execute(f"""SELECT kuerzel, datum, reise_beginn, reise_ende, arbeit_beginn, arbeit_ende
            FROM reisetage_person WHERE reise_code={P} ORDER BY kuerzel, datum""", (rcode,))
        zeiten_rows = cur.fetchall()

        # Gefahrene Kilometer mit Firmen-/Privat-Pkw (vom Reisenden im Portal erfasst)
        cur.execute(f"""SELECT kuerzel, datum, kennzeichen, km
            FROM kilometer_fahrten WHERE reise_code={P} ORDER BY kuerzel, datum""", (rcode,))
        km_rows = cur.fetchall()

        # Alle Belege
        cur.execute(f"""SELECT id,belegart,transportart,transportart_freitext,
            anbieter,rechnungsnummer,belegdatum,
            betrag_brutto,betrag_netto,betrag_mwst,waehrung,
            land_beleg,betrag_eur,kurs_eur,kurs_datum,kurs_quelle,
            s3_original,status,beleg_gruppe_id,betrag_eur_final,nebenkosten_eur,
            nebenkosten_beschreibung,geprueft,kreditkarte_auslandsentgelt_eur,zahlungsart
            FROM belege WHERE reise_code={P}
            ORDER BY belegdatum NULLS LAST, id""", (rcode,))
        belege = cur.fetchall()
        cur.close(); db.close()
        try:   # Bewirtung mit Teilbetrag (z. B. 70 von 520 EUR auf einer Hotelrechnung): getrennt ausweisen
            _db_t = get_db()
            try:
                teil_map = bw.teilbetraege_der_reise(_db_t, rcode)
            finally:
                _db_t.close()
        except Exception:
            teil_map = {}

        # VMA berechnen
        vma_total_netto = sum(float(g(v,"vma_netto",10) or 0) for v in vma_rows)
        trennung_total = sum(float(g(v,"trennungspauschale",11) or 0) for v in vma_rows)

        # Belege kategorisieren – die Regeln stehen in mod_belegdeckung (Buchung x gegen Rechnungen x1 + x2):
        #   rechnungen    endgültige Belege (Rechnung, Quittung, Tankbeleg ...) – zählen immer
        #   vorlaeufig    Buchungsbestätigung mit Betrag, aber ohne Rechnung/Quittung – zählt vorläufig voll mit
        #   bestaetigung  Buchungsbestätigung ohne Betrag und ohne Rechnung – fehlt in der Summe
        #   rest_zeilen   Rechnungen decken eine Buchung nur teilweise – der offene Rest zählt vorläufig mit
        #   kurs_fehlt    Auslandsbelege ohne Kurs
        # Eine vollständig durch Rechnungen belegte Buchung zählt NICHT mit (kein Doppelzählen).
        def kurs_fehlt_bei(b) -> bool:
            """Kurs nachzutragen? Nur bei Fremdwährung mit Betrag, ohne Kurs und ohne bestätigten Euro-Betrag.
            Selbst erzeugte Belege (Bewirtungsbeleg, Vorschuss) haben keinen eigenen Kurs."""
            return bool((g(b,"waehrung",10) or "EUR") != "EUR" and not g(b,"kurs_eur",13)
                        and g(b,"betrag_brutto",7) and g(b,"betrag_eur_final",19) is None
                        and (g(b,"belegart",1) or "") not in EIGENBELEG_ARTEN)

        deck_belege = [{"id": g(b,"id",0), "belegart": g(b,"belegart",1), "beleg_gruppe_id": g(b,"beleg_gruppe_id",18),
                        "betrag_brutto": g(b,"betrag_brutto",7), "waehrung": g(b,"waehrung",10),
                        "betrag_eur": g(b,"betrag_eur",12), "betrag_eur_final": g(b,"betrag_eur_final",19),
                        "nebenkosten_eur": g(b,"nebenkosten_eur",20),
                        "kreditkarte_auslandsentgelt_eur": g(b,"kreditkarte_auslandsentgelt_eur",23),
                        "anbieter": g(b,"anbieter",4), "transportart": g(b,"transportart",2),
                        "zahlungsart": g(b,"zahlungsart",24)} for b in belege]
        deck_by_id = {d["id"]: d for d in deck_belege}
        kat = deckung_mod.kategorisieren(deck_belege)
        ids_final, ids_vorl, ids_ohne = set(kat["final"]), set(kat["vorlaeufig"]), set(kat["ohne_betrag"])
        rechnungen, bestaetigung, vorlaeufig, kurs_fehlt = [], [], [], []
        for b in belege:
            bid_k = g(b,"id",0)
            if bid_k in ids_final:
                rechnungen.append(b)
            elif bid_k in ids_vorl:
                vorlaeufig.append(b)
            elif bid_k in ids_ohne:
                bestaetigung.append(b)
                continue
            else:
                continue   # durch Rechnungen in derselben Gruppe belegt
            if kurs_fehlt_bei(b):
                kurs_fehlt.append(b)
        rest_zeilen = kat["rest"]
        luecken_liste = deckung_mod.luecken(deck_belege, kat)

        # Rechnungen einer Buchung bekommen ihren Anteil am echten Euro-Betrag/Auslandsentgelt der Buchung
        kosten_eur = sum(
            beleg_eur_mit_anteil(g(b,"betrag_brutto",7), g(b,"waehrung",10), g(b,"betrag_eur",12),
                                 g(b,"betrag_eur_final",19), g(b,"nebenkosten_eur",20),
                                 g(b,"kreditkarte_auslandsentgelt_eur",23), kat["anteile"].get(g(b,"id",0)))
            for b in (rechnungen + vorlaeufig)) + sum(((r["fehlt_eur"] or 0.0) + r["gebuehr_eur"]) for r in rest_zeilen)

        # Wochentage
        wt = ["Mo","Di","Mi","Do","Fr","Sa","So"]
        mo = ["Jan","Feb","Mär","Apr","Mai","Jun",
              "Jul","Aug","Sep","Okt","Nov","Dez"]

        def fdat(d):
            if not d: return "–"
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            try: return date.fromisoformat(str(d)[:10]).strftime("%d.%m.%Y")
            except: return str(d)[:10]

        # ── VMA-Tabelle ───────────────────────────────────────────────────────
        vma_html = ""
        for v in vma_rows:
            dat = g(v,"datum",0)
            if isinstance(dat,str): dat = date.fromisoformat(dat[:10])
            lcode=g(v,"land_code",1); lname=g(v,"land_name",2)
            voll=float(g(v,"vma_satz_voll",3) or 0)
            halb=float(g(v,"vma_satz_halb",4) or 0)
            ist_halb=bool(g(v,"ist_halber_satz",5))
            frueh=bool(g(v,"fruehstueck",6))
            mitt=bool(g(v,"mittagessen",7))
            abend=bool(g(v,"abendessen",8))
            netto=float(g(v,"vma_netto",10) or 0)
            trennung_v = float(g(v,"trennungspauschale",11) or 0)

            abzuege = []
            if frueh: abzuege.append("Frühstück")
            if mitt: abzuege.append("Mittagessen")
            if abend: abzuege.append("Abendessen")

            vma_html += f"""<tr>
                <td>{wt[dat.weekday()]} {dat.strftime("%d.%m.%Y")}</td>
                <td><span style="font-family:monospace;font-size:11px;
                    background:#f1f5f9;padding:1px 6px;border-radius:4px">{lcode}</span>
                    {lname}</td>
                <td style="text-align:right;font-family:monospace">
                    {"½ " if ist_halb else ""}{halb if ist_halb else voll:.2f} €</td>
                <td style="font-size:12px;color:#64748b">
                    {", ".join(abzuege) if abzuege else "–"}</td>
                <td style="text-align:right;font-weight:600;color:#059669;
                    font-family:monospace">{netto:.2f} €</td>
                <td style="text-align:right;font-weight:600;color:#7c3aed;
                    font-family:monospace">{f"{trennung_v:.2f} €" if trennung_v else "–"}</td>
            </tr>"""

        # ── Kosten-Tabelle ────────────────────────────────────────────────────
        kosten_html = ""
        summen = {}  # pro Transportart
        vorlaeufig_ids = {g(b,"id",0) for b in vorlaeufig}
        for b in rechnungen + vorlaeufig:
            bid2=g(b,"id",0); art=g(b,"belegart",1) or "–"
            ist_vorlaeufig = bid2 in vorlaeufig_ids
            typ=g(b,"transportart",2) or "Sonstiges"
            freitext=g(b,"transportart_freitext",3) or ""
            anbieter=g(b,"anbieter",4) or "–"
            rechnr=g(b,"rechnungsnummer",5) or "–"
            bd=g(b,"belegdatum",6)
            brutto=g(b,"betrag_brutto",7); netto_b=g(b,"betrag_netto",8)
            mwst=g(b,"betrag_mwst",9); waehrung=g(b,"waehrung",10) or "EUR"
            land=g(b,"land_beleg",11) or ""
            betrag_eur_b=g(b,"betrag_eur",12); kurs=g(b,"kurs_eur",13)
            betrag_eur_final_b=g(b,"betrag_eur_final",19); nebenkosten_b=g(b,"nebenkosten_eur",20)
            nebenkosten_besch_b=g(b,"nebenkosten_beschreibung",21)
            auslandsentgelt_b=g(b,"kreditkarte_auslandsentgelt_eur",23)
            an_b = kat["anteile"].get(bid2)    # Anteil an der Buchung (echter Euro-Betrag, Entgelt)
            if an_b:
                betrag_eur_final_b = an_b["eur_final"]
                nebenkosten_b = float(nebenkosten_b or 0) + an_b["nebenkosten"]
                if an_b["entgelt"] is not None:
                    auslandsentgelt_b = an_b["entgelt"]

            typ_label = typ + (f" – {freitext}" if freitext else "")
            if ist_vorlaeufig:
                typ_label += ' <span style="color:#c2410c;font-weight:600">· vorläufig</span>'
            else:
                d_b = kat["deckung"].get(g(b,"beleg_gruppe_id",18))
                if d_b and d_b["buchung_ids"] and bid2 not in d_b["buchung_ids"]:
                    typ_label += (f' <a href="/beleg/{d_b["buchung_ids"][0]}" style="color:#2563eb;font-size:10px;'
                                  f'text-decoration:none" title="Gehört zu dieser Buchungsbestätigung">🔗 Buchung #{d_b["buchung_ids"][0]}</a>')

            eur_val = gesamtbetrag_berechnen(brutto, waehrung, betrag_eur_b, betrag_eur_final_b, nebenkosten_b, auslandsentgelt_b)

            # Betrag-Spalte
            if waehrung == "EUR":
                bet_s = f"{float(brutto):.2f} EUR" if brutto else "–"
                mwst_s = (f"MwSt: {float(mwst):.2f} EUR"
                          if mwst and land == "DE" else
                          "VAT: nicht abzugsfähig (Ausland)" if mwst and land != "DE"
                          else "")
            else:
                bet_s = f"{float(brutto):.2f} {waehrung}" if brutto else "–"
                if betrag_eur_final_b:
                    bet_s += f" = {float(betrag_eur_final_b):.2f} EUR" + (" (Anteil der Buchung)" if an_b else "")
                elif betrag_eur_b:
                    bet_s += f" ≈ {float(betrag_eur_b):.2f} EUR (Kurs: {kurs})"
                else:
                    bet_s += f' <span style="color:#ef4444">⚠ Kurs fehlt</span>'
                mwst_s = "Ausland"
            if nebenkosten_b:
                bet_s += f'<br><span style="font-size:10px;color:#64748b">+ {float(nebenkosten_b):.2f} EUR {nebenkosten_besch_b or "Nebenkosten"}</span>'
            if auslandsentgelt_b:
                bet_s += f'<br><span style="font-size:10px;color:#64748b">+ {float(auslandsentgelt_b):.2f} EUR Auslandsentgelt</span>'
            if ist_vorlaeufig:
                mwst_s += (' <span style="color:#c2410c;font-size:10px">(nur Buchungsbestätigung)</span>')

            # Spalte "Final": bei Fremdwährung zeigt sie, ob der endgültige Betrag
            # schon bestätigt ist oder noch auf einer KI-Schätzung beruht
            if waehrung == "EUR":
                final_s = '<span style="color:#059669">✓ EUR</span>'
            elif betrag_eur_final_b:
                final_s = '<span style="color:#059669" title="Finaler Betrag bestätigt">✓ final</span>'
            elif betrag_eur_b:
                final_s = '<span style="color:#b45309" title="Nur KI-Schätzung, noch nicht final bestätigt">⚠ geschätzt</span>'
            else:
                final_s = '<span style="color:#ef4444">✗ fehlt</span>'

            geprueft_b = bool(g(b,"geprueft",22))
            geprueft_s = ('<span style="color:#059669;font-weight:700" title="Geprüft">✓</span>'
                          if geprueft_b else
                          '<span style="color:#ef4444;font-weight:700" title="Noch nicht geprüft">✗</span>')

            tb = teil_map.get(bid2)
            if (tb and brutto and waehrung == tb["waehrung"] and 0 < tb["teilbetrag"] < float(brutto) - 0.004
                    and typ != "Bewirtung"):
                eur_bw = eur_val * tb["teilbetrag"] / float(brutto)
                summen[typ] = summen.get(typ, 0) + (eur_val - eur_bw)
                summen["Bewirtung"] = summen.get("Bewirtung", 0) + eur_bw
                bet_s += (f'<br><span style="font-size:10px;color:#7c3aed">davon Bewirtung {tb["teilbetrag"]:.2f} {waehrung}'
                          + (f' = {eur_bw:.2f} EUR' if waehrung != "EUR" else "")
                          + f' ({tb["nummer"]}) · übrige Leistungen {eur_val - eur_bw:.2f} EUR</span>')
            else:
                summen[typ] = summen.get(typ, 0) + eur_val

            kosten_html += f"""<tr{' style="background:#fff7ed"' if ist_vorlaeufig else ''}>
                <td>{fdat(bd)}</td>
                <td><span style="font-size:11px;background:#f1f5f9;padding:1px 6px;
                    border-radius:4px">{typ_label}</span></td>
                <td><b>{anbieter}</b></td>
                <td style="font-family:monospace;font-size:11px;color:#64748b">{rechnr}</td>
                <td style="text-align:right;font-family:monospace">
                    {bet_s}</td>
                <td style="text-align:right;font-family:monospace;font-size:11px">
                    {f"{float(mwst):.2f} €" if mwst and waehrung=="EUR" and land=="DE" else "–"}</td>
                <td style="font-size:11px;color:#64748b">{mwst_s}</td>
                <td style="text-align:center;font-size:11px">{final_s}</td>
                <td style="text-align:center">{geprueft_s}</td>
                <td>
                  <a href="/beleg/{bid2}" style="color:var(--blue);font-weight:600;
                     text-decoration:none;margin-right:8px">#{bid2}</a>
                  <a href="/beleg/{bid2}/pdf/original" target="_blank"
                     class="btn btn-secondary btn-sm">PDF</a>
                </td>
            </tr>"""

        # Buchung nur teilweise durch Rechnungen belegt: der offene Rest steht als eigene (orange) Zeile
        # in der Summe, bis die fehlende Rechnung/Quittung verknüpft ist
        for r_rest in rest_zeilen:
            bu_id = r_rest["buchung_ids"][0]
            bu = deck_by_id[bu_id]
            bu_row = next(x for x in belege if g(x,"id",0) == bu_id)
            typ_r = bu.get("transportart") or "Sonstiges"
            fe = r_rest["fehlt_eur"]
            if fe is not None:
                fe += r_rest["gebuehr_eur"]      # inkl. Anteil am Auslandsentgelt der Buchung
            summen[typ_r] = summen.get(typ_r, 0) + (fe or 0.0)
            w_r = r_rest["waehrung"]
            bet_r = f"{r_rest['fehlt']:.2f} {w_r}"
            if w_r != "EUR":
                bet_r += (f" ≈ {fe:.2f} EUR" if fe is not None
                          else ' <span style="color:#ef4444">⚠ Kurs fehlt</span>')
            kosten_html += f"""<tr style="background:#fff7ed">
                <td>{fdat(g(bu_row,"belegdatum",6))}</td>
                <td><span style="font-size:11px;background:#f1f5f9;padding:1px 6px;border-radius:4px">{typ_r}
                    <span style="color:#c2410c;font-weight:600">· vorläufig</span></span></td>
                <td><b>{bu.get("anbieter") or "–"}</b></td>
                <td style="font-size:11px;color:#c2410c">Rest der Buchung</td>
                <td style="text-align:right;font-family:monospace">{bet_r}</td>
                <td style="text-align:right;font-family:monospace;font-size:11px">–</td>
                <td style="font-size:11px;color:#c2410c">Rechnung/Quittung fehlt noch</td>
                <td style="text-align:center;font-size:11px">–</td>
                <td style="text-align:center">–</td>
                <td><a href="/beleg/{bu_id}" style="color:var(--blue);font-weight:600;text-decoration:none">#{bu_id}</a></td>
            </tr>"""

        # Prüfstatus-Zusammenfassung (für den grünen/roten Gesamthinweis)
        alle_geprueft = all(bool(g(b,"geprueft",22)) for b in (rechnungen + vorlaeufig)) if (rechnungen or vorlaeufig) else True
        n_ungeprueft = sum(1 for b in (rechnungen + vorlaeufig) if not bool(g(b,"geprueft",22)))

        # Summen pro Kategorie
        summen_html = ""
        for typ, summe in sorted(summen.items()):
            summen_html += f"""<tr style="background:#f8fafc">
                <td colspan="4" style="text-align:right;font-size:12px;
                    color:#64748b;padding:6px 14px">Summe {typ}</td>
                <td style="text-align:right;font-family:monospace;
                    font-weight:600;padding:6px 14px">{summe:.2f} EUR</td>
                <td colspan="5"></td>
            </tr>"""

        # Buchungsbestätigungen (Hinweis)
        best_html = ""
        if bestaetigung:
            items = ""
            for b in bestaetigung:
                typ=g(b,"transportart",2) or "?"
                anbieter=g(b,"anbieter",4) or "–"
                bid2=g(b,"id",0)
                items += (f'<li style="margin:4px 0">{typ}: {anbieter} – ' 
                          f'<a href="/beleg/{bid2}" style="color:var(--blue)">'
                          f'Beleg #{bid2}</a> ' 
                          f'<span style="color:#ef4444;font-size:11px">'
                          f'⚠ Keine Rechnung, kein Betrag erkannt</span></li>')
            best_html = f"""
            <div class="alert alert-warn" style="margin-top:16px">
              <b>⚠ Nur Buchungsbestätigung ohne erkennbaren Betrag – fehlt in der Kostensumme:</b>
              <ul style="margin-top:8px;padding-left:16px">{items}</ul>
            </div>"""

        # Belege mit Bezahlart „Durch Kunde“: zählen nie in der Summe, werden nur vermerkt
        kunde_html = ""
        if kat["durch_kunde"]:
            from html import escape as _h_escape
            items = ""
            for kid in kat["durch_kunde"]:
                kb = deck_by_id[kid]
                betrag_k = (f' – {float(kb["betrag_brutto"]):.2f} {kb.get("waehrung") or "EUR"}'
                            if kb.get("betrag_brutto") else "")
                items += (f'<li style="margin:4px 0"><a href="/beleg/{kid}" style="color:var(--blue)">'
                          f'Beleg #{kid}</a>: {_h_escape(str(kb.get("anbieter") or "–"))}{betrag_k}</li>')
            kunde_html = f"""
            <div class="alert alert-info" style="margin-top:8px">
              <b>🤝 Durch Kunde direkt bezahlt – zählt nicht in der Summe:</b>
              <ul style="margin-top:8px;padding-left:16px">{items}</ul>
            </div>"""

        # Fehlende Kurse
        kurs_html = ""
        if kurs_fehlt:
            items = ""
            for b in kurs_fehlt:
                bid2=g(b,"id",0); w=g(b,"waehrung",10); brutto=g(b,"betrag_brutto",7)
                anbieter=g(b,"anbieter",4) or "–"
                items += (f'<li style="margin:4px 0">' 
                          f'<a href="/beleg/{bid2}" style="color:var(--blue)">' 
                          f'Beleg #{bid2}</a>: {anbieter} – {brutto} {w} ' 
                          f'<span style="color:#ef4444">⚠ Kurs fehlt</span></li>')
            kurs_html = f"""
            <div class="alert alert-err" style="margin-top:8px">
              <b>❌ Wechselkurs fehlt – bitte nachtragen:</b>
              <ul style="margin-top:8px;padding-left:16px">{items}</ul>
            </div>"""

        ma_str = " · ".join(f"{g(m,'kuerzel',0)} – {g(m,'klarname',1)}"
                             for m in ma_rows)

        # Start/Ende der Reise mit tatsächlicher Uhrzeit (falls erfasst)
        start_txt = fdat(ab) + (f" · {antritt_zeit} Uhr" if antritt_zeit else "")
        ende_txt = fdat(zu) + (f" · {ende_zeit} Uhr" if ende_zeit else "")

        # Reise-/Arbeitszeiten (aus dem Reisenden-Portal erfasst) – v.a. für
        # Monteure relevant, die ihre Zeiten dort eintragen
        zeiten_html = ""
        if zeiten_rows:
            zeilen_z = ""
            for z in zeiten_rows:
                kuerzel_z = g(z,"kuerzel",0); datum_z = g(z,"datum",1)
                rb = g(z,"reise_beginn",2) or "–"; re_ = g(z,"reise_ende",3) or "–"
                ab_z = g(z,"arbeit_beginn",4) or "–"; ae_z = g(z,"arbeit_ende",5) or "–"
                zeilen_z += f"""<tr>
                    <td>{fdat(datum_z)}</td><td>{kuerzel_z}</td>
                    <td style="text-align:right;font-family:monospace">{rb} – {re_}</td>
                    <td style="text-align:right;font-family:monospace">{ab_z} – {ae_z}</td>
                </tr>"""
            zeiten_html = f"""<div class="card" style="margin-bottom:16px">
              <div class="card-header"><span class="card-title">⏱ Reise-/Arbeitszeiten</span></div>
              <div class="table-wrap">
                <table>
                  <thead><tr><th>Datum</th><th>Kürzel</th>
                    <th style="text-align:right">Reisezeit</th>
                    <th style="text-align:right">Arbeitszeit</th></tr></thead>
                  <tbody>{zeilen_z}</tbody>
                </table>
              </div>
            </div>"""

        # Gefahrene Kilometer (Firmen-/Privat-Pkw) – vom Reisenden im Portal
        # erfasst; bei Mietwagen steht die Info bereits auf der Mietwagen-
        # rechnung, taucht hier also bewusst nicht separat auf.
        km_html = ""
        if km_rows:
            zeilen_km = ""
            km_summe = 0.0
            for k in km_rows:
                kuerzel_k = g(k,"kuerzel",0)
                kennz_k = g(k,"kennzeichen",2); km_k = float(g(k,"km",3) or 0)
                km_summe += km_k
                zeilen_km += f"""<tr>
                    <td>{kuerzel_k}</td>
                    <td>{kennz_k}</td>
                    <td style="text-align:right;font-family:monospace">{km_k:.0f} km</td>
                </tr>"""
            km_html = f"""<div class="card" style="margin-bottom:16px">
              <div class="card-header">
                <span class="card-title">🚗 Gefahrene Kilometer (Firmen-/Privat-Pkw)</span>
                <span style="font-size:13px;font-weight:600">{km_summe:.0f} km gesamt</span>
              </div>
              <div class="table-wrap">
                <table>
                  <thead><tr><th>Kürzel</th><th>Kennzeichen</th>
                    <th style="text-align:right">Kilometer</th></tr></thead>
                  <tbody>{zeilen_km}</tbody>
                </table>
              </div>
            </div>"""

        # Geflogene Flugsegmente (ohne Züge, auch nicht mit LH-Nummer)
        from html import escape as _h
        flug = flugsegmente_zaehlen(rcode)
        flug_html = ""
        if flug["summe"] or flug["zuege"] or flug["storniert"]:
            zeilen_f = ""
            for f in flug["gezaehlt"]:
                warn = (' <span style="color:#b45309;font-size:11px">⚠ Flughafencode fehlt – bitte prüfen</span>'
                        if f["iata_fehlt"] else "")
                zeilen_f += f"""<tr>
                    <td>{fdat(f["datum"])} {_h(f["zeit"])}</td>
                    <td>{_h(f["person"])}</td>
                    <td style="font-family:monospace">{_h(f["nummer"]) or "–"}</td>
                    <td>{_h(f["von"])} → {_h(f["nach"])}{warn}</td>
                    <td><a href="/beleg/{f["beleg"]}" style="color:var(--blue);font-weight:600;
                        text-decoration:none">#{f["beleg"]}</a></td>
                </tr>"""
            je_person_f = " · ".join(f"{_h(n)}: {c}" for n, c in flug["je_person"]) if len(flug["je_person"]) > 1 else ""
            nicht_f = []
            if flug["zuege"]:
                nicht_f.append(f'{flug["zuege"]} Zugfahrt{"en" if flug["zuege"] != 1 else ""} (auch mit LH-Nummer)')
            if flug["storniert"]:
                nicht_f.append("1 stornierter Flug" if flug["storniert"] == 1
                               else f'{flug["storniert"]} stornierte Flüge')
            ueb_f = (f'<div style="padding:8px 16px;background:#fef2f2;color:#991b1b;font-size:12px;font-weight:600">'
                     f'⚠ {flug["ueberschneidungen"]} ungeklärte Überschneidung(en) – die Summe kann Dubletten enthalten. '
                     f'<a href="/reise/{rcode}/ueberschneidungen" style="color:#991b1b">Jetzt klären →</a></div>'
                     if flug["ueberschneidungen"] else "")
            flug_html = f"""<div class="card" style="margin-bottom:16px">
              <div class="card-header">
                <span class="card-title">✈ Geflogene Flugsegmente</span>
                <span style="font-size:13px;font-weight:600">{flug["summe"]} gesamt{f" · {je_person_f}" if je_person_f else ""}</span>
              </div>
              {ueb_f}
              <div class="table-wrap">
                <table>
                  <thead><tr><th>Abflug</th><th>Reisender</th><th>Flug</th><th>Strecke</th><th>Beleg</th></tr></thead>
                  <tbody>{zeilen_f or '<tr><td colspan="5" class="empty-state">Keine Flugsegmente</td></tr>'}</tbody>
                </table>
              </div>
              {f'<div style="padding:8px 16px;font-size:12px;color:var(--muted)">Nicht mitgezählt: {", ".join(nicht_f)}</div>' if nicht_f else ''}
            </div>"""

        # Warnkasten: Buchungen ohne (vollständige) Rechnung/Quittung. „Kein Betrag erkannt“ steht schon in best_html.
        luecken_box_html = deckung_mod.luecken_html([l for l in luecken_liste if l["art"] != "ohne_betrag"], deck_by_id)
        gruppen_html = deckung_mod.gruppen_uebersicht_html(kat["deckung"], deck_by_id)
        gruppen_block_html = f'<div style="margin-top:16px">{gruppen_html}</div>' if gruppen_html else ""

        content = f"""
        <div style="display:flex;align-items:flex-start;justify-content:space-between;
                    margin-bottom:20px;flex-wrap:wrap;gap:12px">
          <div style="display:flex;gap:14px;align-items:flex-start">
            <img src="/static/logo3.png" alt="Herrhammer" style="height:44px;width:auto;margin-top:2px">
            <div>
              <div style="font-family:monospace;font-size:12px;color:#64748b">{rcode}</div>
              <h1 class="page-title" style="margin:4px 0">{titel}</h1>
              <div style="font-size:13px;color:#64748b">
                🚀 Start: {start_txt} &nbsp;·&nbsp; 🏁 Ende: {ende_txt}<br>
                👤 {ma_str}{f" &nbsp;·&nbsp; ✈ {flug['summe']} Flugsegment{'e' if flug['summe'] != 1 else ''}" if flug["summe"] else ""}
              </div>
            </div>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <a href="/reise/{rcode}/abschluss/pdf" class="btn btn-primary">
              📄 PDF-Export
            </a>
            <a href="/reise/{rcode}/habel" class="btn btn-secondary">📤 An Habel</a>
            <a href="/reise/{rcode}" class="btn btn-secondary">← Reise</a>
          </div>
        </div>

        <!-- Zusammenfassung -->
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600;color:#059669">{vma_total_netto:.2f} €</div>
            <div style="font-size:12px;color:#64748b">VMA gesamt (netto)</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600;color:#7c3aed">{trennung_total:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Trennungspauschale</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600">{kosten_eur:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Kosten (Rechnungen)</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600;color:#2563eb">
              {vma_total_netto + trennung_total + kosten_eur:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Gesamt zur Abrechnung</div>
          </div></div>
        </div>

        {kurs_html}
        {best_html}
        {kunde_html}
        {luecken_box_html}
        {zeiten_html}
        {km_html}
        {flug_html}
        {_vorschuss_abschluss_html(rcode)}

        <!-- VMA-Tabelle -->
        <div class="card" style="margin-bottom:16px">
          <div class="card-header">
            <span class="card-title">🌍 Verpflegungsmehraufwand</span>
            <span style="font-size:13px;font-weight:600;color:#059669">
              {vma_total_netto:.2f} EUR{f' <span style="color:#7c3aed">+ {trennung_total:.2f} EUR Trennungspauschale</span>' if trennung_total else ''}</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Datum</th><th>Land</th><th style="text-align:right">Satz</th>
                <th>Abzüge</th><th style="text-align:right">Netto</th>
                <th style="text-align:right">Trennungspauschale</th>
              </tr></thead>
              <tbody>
                {vma_html or '<tr><td colspan="6" class="empty-state">Keine VMA-Daten – bitte Übersicht aufrufen</td></tr>'}
              </tbody>
              <tfoot><tr style="border-top:2px solid var(--border)">
                <td colspan="4" style="text-align:right;font-weight:600;padding:10px 14px">
                  Gesamt:</td>
                <td style="text-align:right;font-weight:700;font-size:15px;
                    color:#059669;padding:10px 14px">{vma_total_netto:.2f} EUR</td>
                <td style="text-align:right;font-weight:700;font-size:15px;
                    color:#7c3aed;padding:10px 14px">{trennung_total:.2f} EUR</td>
              </tr></tfoot>
            </table>
          </div>
        </div>

        <!-- Kosten-Tabelle -->
        <div class="card">
          <div class="card-header">
            <span class="card-title">🧾 Belege (Rechnungen & Quittungen)</span>
            <span style="font-size:13px;font-weight:600">{kosten_eur:.2f} EUR</span>
          </div>
          {f'<div style="padding:8px 16px;background:#fef2f2;color:#991b1b;font-size:12px;font-weight:600">⚠ {n_ungeprueft} Beleg{"e" if n_ungeprueft!=1 else ""} noch nicht geprüft</div>' if n_ungeprueft else '<div style="padding:8px 16px;background:#ecfdf5;color:#065f46;font-size:12px;font-weight:600">✓ Alle Belege geprüft</div>'}
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Datum</th><th>Art</th><th>Anbieter</th>
                <th>Rechnungsnr.</th><th style="text-align:right">Betrag</th>
                <th style="text-align:right">MwSt</th><th>Hinweis</th>
                <th style="text-align:center">Final</th><th style="text-align:center">Geprüft</th><th>Beleg</th>
              </tr></thead>
              <tbody>
                {kosten_html or '<tr><td colspan="10" class="empty-state">Keine Rechnungen vorhanden</td></tr>'}
                {summen_html}
              </tbody>
              <tfoot><tr style="border-top:2px solid var(--border);background:#f0fdf4">
                <td colspan="4" style="text-align:right;font-weight:600;padding:10px 14px">
                  Gesamt Kosten:</td>
                <td style="text-align:right;font-weight:700;font-size:15px;
                    color:#059669;padding:10px 14px">{kosten_eur:.2f} EUR</td>
                <td colspan="5"></td>
              </tr></tfoot>
            </table>
          </div>
        </div>
        {gruppen_block_html}"""

        return HTMLResponse(shell(f"Abschluss {rcode}", content, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


@app.get("/reise/{code}/abschluss/pdf")
def reise_abschluss_pdf(code: str):
    """Erzeugt den Abschlussbericht (VMA + Kosten) als PDF zum Download/Öffnen."""
    return abschluss_pdf_antwort(code)


def abschluss_pdf_antwort(code: str, mit_originalen: bool = True, anlagen_namen: dict | None = None):
    """Abschlussbericht als PDF-Antwort. mit_originalen=False: nur der Bericht, die Belege liegen dann als eigene
    Dateien bei (Habel-Mail); anlagen_namen = {beleg_id: Dateiname} für das Anlagenverzeichnis."""
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()

        cur.execute(f"SELECT code,titel,abreise,rueckkehr FROM reisen WHERE code={P}", (rcode,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden</div>'))
        def g(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        titel = g(r,"titel",1); ab = g(r,"abreise",2); zu = g(r,"rueckkehr",3)

        cur.execute(f"""SELECT m.kuerzel, m.klarname FROM mitarbeiter m
            JOIN reise_mitarbeiter rm ON rm.kuerzel=m.kuerzel
            WHERE rm.reise_code={P} ORDER BY m.klarname""", (rcode,))
        ma_rows = cur.fetchall()

        cur.execute(f"""SELECT datum,land_code,land_name,vma_satz_voll,vma_satz_halb,
            ist_halber_satz,fruehstueck,mittagessen,abendessen,vma_netto,trennungspauschale,
            tatsaechliche_uhrzeit
            FROM vma_tage WHERE reise_code={P} ORDER BY datum""", (rcode,))
        vma_rows = cur.fetchall()
        antritt_zeit = g(vma_rows[0],"tatsaechliche_uhrzeit",11) if vma_rows else None
        ende_zeit = g(vma_rows[-1],"tatsaechliche_uhrzeit",11) if vma_rows else None

        cur.execute(f"""SELECT kuerzel, datum, reise_beginn, reise_ende, arbeit_beginn, arbeit_ende
            FROM reisetage_person WHERE reise_code={P} ORDER BY kuerzel, datum""", (rcode,))
        zeiten_rows = cur.fetchall()

        cur.execute(f"""SELECT kuerzel, datum, kennzeichen, km
            FROM kilometer_fahrten WHERE reise_code={P} ORDER BY kuerzel, datum""", (rcode,))
        km_rows = cur.fetchall()

        cur.execute(f"""SELECT id,belegart,anbieter,belegdatum,betrag_brutto,waehrung,betrag_eur,
            beleg_gruppe_id,s3_original,dateiname,betrag_eur_final,nebenkosten_eur,
            betrag_mwst,land_beleg,geprueft,kreditkarte_auslandsentgelt_eur,zahlungsart
            FROM belege WHERE reise_code={P} ORDER BY belegdatum""", (rcode,))
        belege = cur.fetchall()
        cur.close(); db.close()
        try:   # Bewirtung mit Teilbetrag: getrennt ausweisen (Gesamtsumme unverändert)
            _db_t = get_db()
            try:
                teil_map = bw.teilbetraege_der_reise(_db_t, rcode)
            finally:
                _db_t.close()
        except Exception:
            teil_map = {}

        vma_total_netto = sum(float(g(v,"vma_netto",9) or 0) for v in vma_rows)
        trennung_total = sum(float(g(v,"trennungspauschale",10) or 0) for v in vma_rows)

        # Gleiche Logik wie /abschluss (HTML), Regeln in mod_belegdeckung: Eine Buchungsbestätigung zählt
        # nicht mit, wenn Rechnungen/Quittungen sie vollständig belegen (Buchung x = Rechnungen x1 + x2).
        # Ohne Rechnung zählt sie vorläufig voll mit, bei nur teilweiser Deckung der offene Rest.
        deck_belege = [{"id": g(b,"id",0), "belegart": g(b,"belegart",1), "beleg_gruppe_id": g(b,"beleg_gruppe_id",7),
                        "betrag_brutto": g(b,"betrag_brutto",4), "waehrung": g(b,"waehrung",5),
                        "betrag_eur": g(b,"betrag_eur",6), "betrag_eur_final": g(b,"betrag_eur_final",10),
                        "nebenkosten_eur": g(b,"nebenkosten_eur",11),
                        "kreditkarte_auslandsentgelt_eur": g(b,"kreditkarte_auslandsentgelt_eur",15),
                        "anbieter": g(b,"anbieter",2), "zahlungsart": g(b,"zahlungsart",16)} for b in belege]
        deck_by_id = {d["id"]: d for d in deck_belege}
        kat = deckung_mod.kategorisieren(deck_belege)
        ids_zaehlen = set(kat["final"]) | set(kat["vorlaeufig"])
        rechnungen = [b for b in belege if g(b,"id",0) in ids_zaehlen]   # inkl. vorläufig mitgezählter Buchungen
        rest_zeilen = kat["rest"]
        luecken_liste = deckung_mod.luecken(deck_belege, kat)

        kosten_eur = sum(
            beleg_eur_mit_anteil(g(b,"betrag_brutto",4), g(b,"waehrung",5), g(b,"betrag_eur",6),
                                 g(b,"betrag_eur_final",10), g(b,"nebenkosten_eur",11),
                                 g(b,"kreditkarte_auslandsentgelt_eur",15), kat["anteile"].get(g(b,"id",0)))
            for b in rechnungen) + sum(((r["fehlt_eur"] or 0.0) + r["gebuehr_eur"]) for r in rest_zeilen)

        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.lib import colors

        def esc(s):
            return (str(s) if s is not None else "–").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

        def zelle(s):    # Tabellenzellen sind reiner Text (kein Markup): nicht escapen, sonst steht „&amp;“ im PDF
            return str(s) if s is not None else "–"

        def tbl_style(fusszeile=True):
            style = [
                ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#f1f5f9")),
                ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
                ("FONTSIZE",(0,0),(-1,-1),8),
                ("LINEBELOW",(0,0),(-1,0),0.5,colors.HexColor("#cbd5e1")),
                ("ALIGN",(2,0),(-1,-1),"RIGHT"),
                ("BOTTOMPADDING",(0,0),(-1,-1),3), ("TOPPADDING",(0,0),(-1,-1),3),
            ]
            if fusszeile:
                style += [("LINEABOVE",(0,-1),(-1,-1),1,colors.HexColor("#94a3b8")),
                          ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold")]
            return TableStyle(style)

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
            leftMargin=16*mm, rightMargin=16*mm, topMargin=14*mm, bottomMargin=14*mm)
        styles = getSampleStyleSheet()
        story = []

        ma_namen = ", ".join(g(m,"klarname",1) for m in ma_rows) or "–"
        start_txt = fmt_date(ab) + (f" · {antritt_zeit} Uhr" if antritt_zeit else "")
        ende_txt = fmt_date(zu) + (f" · {ende_zeit} Uhr" if ende_zeit else "")

        kopf_zeilen = [Paragraph(f"Reisekostenabrechnung {esc(rcode)}", styles["Title"]),
                       Paragraph(esc(titel), styles["Heading2"])]
        if os.path.exists("static/logo3.png"):
            try:
                logo_img = Image("static/logo3.png", width=32*mm, height=13*mm)
                kopf_tbl = Table([[kopf_zeilen, logo_img]], colWidths=[130*mm, 34*mm])
                kopf_tbl.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),
                                               ("ALIGN",(1,0),(1,0),"RIGHT")]))
                story.append(kopf_tbl)
            except Exception:
                story.extend(kopf_zeilen)
        else:
            story.extend(kopf_zeilen)
        story.append(Spacer(1, 3*mm))
        story.append(Paragraph(f"🚀 Start: {esc(start_txt)} &nbsp;&nbsp; 🏁 Ende: {esc(ende_txt)}", styles["Normal"]))
        story.append(Paragraph(f"Reisende: {esc(ma_namen)}", styles["Normal"]))
        story.append(Spacer(1, 6*mm))

        if zeiten_rows:
            story.append(Paragraph("Reise-/Arbeitszeiten", styles["Heading2"]))
            zeiten_daten = [["Datum","Kürzel","Reisezeit","Arbeitszeit"]]
            for z in zeiten_rows:
                zeiten_daten.append([
                    fmt_date(g(z,"datum",1)), zelle(g(z,"kuerzel",0)),
                    f"{g(z,'reise_beginn',2) or '–'} – {g(z,'reise_ende',3) or '–'}",
                    f"{g(z,'arbeit_beginn',4) or '–'} – {g(z,'arbeit_ende',5) or '–'}"])
            zeiten_tbl = Table(zeiten_daten, colWidths=[28*mm,20*mm,45*mm,45*mm])
            zeiten_tbl.setStyle(tbl_style(fusszeile=False))
            story.append(zeiten_tbl)
            story.append(Spacer(1, 5*mm))

        if km_rows:
            story.append(Paragraph("Gefahrene Kilometer (Firmen-/Privat-Pkw)", styles["Heading2"]))
            km_daten = [["Kürzel","Kennzeichen","Kilometer"]]
            km_summe_pdf = 0.0
            for k in km_rows:
                km_wert = float(g(k,"km",3) or 0); km_summe_pdf += km_wert
                km_daten.append([
                    zelle(g(k,"kuerzel",0)), zelle(g(k,"kennzeichen",2)), f"{km_wert:.0f} km"])
            km_daten.append(["", "Gesamt:", f"{km_summe_pdf:.0f} km"])
            km_tbl = Table(km_daten, colWidths=[30*mm,50*mm,38*mm])
            km_tbl.setStyle(tbl_style())
            story.append(km_tbl)
            story.append(Spacer(1, 5*mm))

        flug_pdf = flugsegmente_zaehlen(rcode)
        if flug_pdf["summe"] or flug_pdf["zuege"] or flug_pdf["storniert"]:
            story.append(Paragraph("Geflogene Flugsegmente", styles["Heading2"]))
            flug_daten = [["Abflug","Reisender","Flug","Strecke"]]
            for f in flug_pdf["gezaehlt"]:
                flug_daten.append([
                    (fmt_date(f["datum"]) if f["datum"] else "–") + (f" {f['zeit']}" if f["zeit"] else ""),
                    zelle(f["person"]), zelle(f["nummer"] or "–"), zelle(f"{f['von']} – {f['nach']}")])
            je_p = " · ".join(f"{n}: {c}" for n, c in flug_pdf["je_person"]) if len(flug_pdf["je_person"]) > 1 else ""
            flug_daten.append(["", "", "Gesamt:", f"{flug_pdf['summe']} Flugsegmente" + (f" ({je_p})" if je_p else "")])
            flug_tbl = Table(flug_daten, colWidths=[30*mm,38*mm,20*mm,90*mm])
            flug_tbl.setStyle(tbl_style())
            flug_tbl.setStyle(TableStyle([("ALIGN",(0,0),(-1,-1),"LEFT")]))
            story.append(flug_tbl)
            hinweise_f = []
            if flug_pdf["zuege"]:
                hinweise_f.append(f"{flug_pdf['zuege']} Zugfahrt(en), auch mit LH-Nummer")
            if flug_pdf["storniert"]:
                hinweise_f.append(f"{flug_pdf['storniert']} stornierte(r) Flug/Flüge")
            if hinweise_f:
                story.append(Paragraph("Nicht mitgezählt: " + esc(", ".join(hinweise_f)), styles["Normal"]))
            if flug_pdf["ueberschneidungen"]:
                story.append(Paragraph(f"Hinweis: {flug_pdf['ueberschneidungen']} Überschneidung(en) ungeklärt – "
                                       "die Summe kann Dubletten enthalten.", styles["Normal"]))
            story.append(Spacer(1, 5*mm))

        story.append(Paragraph("Verpflegungsmehraufwand", styles["Heading2"]))
        vma_daten = [["Datum","Land","Satz","Abzüge","Netto","Trennung"]]
        for v in vma_rows:
            dat = g(v,"datum",0)
            if isinstance(dat,str): dat = date.fromisoformat(dat[:10])
            lcode = g(v,"land_code",1) or "–"
            voll = float(g(v,"vma_satz_voll",3) or 0); halb = float(g(v,"vma_satz_halb",4) or 0)
            ist_halb = bool(g(v,"ist_halber_satz",5))
            frueh = bool(g(v,"fruehstueck",6)); mitt = bool(g(v,"mittagessen",7)); abend = bool(g(v,"abendessen",8))
            netto = float(g(v,"vma_netto",9) or 0)
            trenn = float(g(v,"trennungspauschale",10) or 0)
            abz = [x for x, ok in (("F",frueh),("M",mitt),("A",abend)) if ok]
            vma_daten.append([
                dat.strftime("%d.%m.%Y") if dat else "–", zelle(lcode),
                f"{'½ ' if ist_halb else ''}{(halb if ist_halb else voll):.2f} €",
                "/".join(abz) if abz else "–", f"{netto:.2f} €",
                f"{trenn:.2f} €" if trenn else "–"
            ])
        vma_daten.append(["", "", "", "Gesamt:", f"{vma_total_netto:.2f} €", f"{trennung_total:.2f} €"])
        vma_tbl = Table(vma_daten, colWidths=[25*mm,15*mm,25*mm,20*mm,25*mm,25*mm])
        vma_tbl.setStyle(tbl_style())
        story.append(vma_tbl)
        story.append(Spacer(1, 6*mm))

        story.append(Paragraph("Kosten (Rechnungen/Quittungen)", styles["Heading2"]))
        n_ungeprueft_pdf = sum(1 for b in rechnungen if not bool(g(b,"geprueft",14)))
        if n_ungeprueft_pdf:
            story.append(Paragraph(f"⚠ {n_ungeprueft_pdf} Beleg(e) noch nicht geprüft",
                                    styles["Normal"]))
        else:
            story.append(Paragraph("✓ Alle Belege geprüft", styles["Normal"]))
        story.append(Spacer(1, 2*mm))
        if rechnungen or rest_zeilen:
            kosten_daten = [["Datum","Anbieter","Betrag","MwSt","Final","Geprüft"]]
            for b in rechnungen:
                bd = g(b,"belegdatum",3)
                if isinstance(bd,str):
                    try: bd = date.fromisoformat(bd[:10])
                    except Exception: bd = None
                betrag = g(b,"betrag_brutto",4); waehrung = g(b,"waehrung",5) or "EUR"
                betrag_eur = g(b,"betrag_eur",6); betrag_eur_final = g(b,"betrag_eur_final",10)
                an_b = kat["anteile"].get(g(b,"id",0))
                if an_b:
                    betrag_eur_final = an_b["eur_final"]      # Anteil am echten Euro-Betrag der Buchung
                mwst_b = g(b,"betrag_mwst",12); land_b = g(b,"land_beleg",13) or ""
                geprueft_b = bool(g(b,"geprueft",14))
                bet_txt = f"{float(betrag):.2f} {waehrung}" if betrag else "–"
                if waehrung != "EUR" and (betrag_eur_final or betrag_eur):
                    bet_txt += f" ({float(betrag_eur_final or betrag_eur):.2f} €)"
                mwst_txt = f"{float(mwst_b):.2f} €" if mwst_b and waehrung=="EUR" and land_b=="DE" else "–"
                if waehrung == "EUR":
                    final_txt = "✓"
                elif betrag_eur_final:
                    final_txt = "✓"
                elif betrag_eur:
                    final_txt = "⚠ Schätz."
                else:
                    final_txt = "✗"
                kosten_daten.append([bd.strftime("%d.%m.%Y") if bd else "–",
                                      zelle(g(b,"anbieter",2) or "–"), bet_txt, mwst_txt,
                                      final_txt, "✓" if geprueft_b else "✗"])
            for r_rest in rest_zeilen:
                bu = deck_by_id[r_rest["buchung_ids"][0]]
                bu_row = next(x for x in belege if g(x,"id",0) == bu["id"])
                bd_r = g(bu_row,"belegdatum",3)
                if isinstance(bd_r,str):
                    try: bd_r = date.fromisoformat(bd_r[:10])
                    except Exception: bd_r = None
                bet_r = f"{r_rest['fehlt']:.2f} {r_rest['waehrung']}"
                if r_rest["waehrung"] != "EUR" and r_rest["fehlt_eur"] is not None:
                    bet_r += f" ({r_rest['fehlt_eur'] + r_rest['gebuehr_eur']:.2f} €)"
                kosten_daten.append([bd_r.strftime("%d.%m.%Y") if bd_r else "–",
                                      zelle(f"Rest Buchung #{bu['id']}: {bu.get('anbieter') or '–'}"),
                                      bet_r, "–", "vorl.", "–"])
            kosten_daten.append(["", "Gesamt:", f"{kosten_eur:.2f} €", "", "", ""])
            kosten_tbl = Table(kosten_daten, colWidths=[22*mm,58*mm,38*mm,18*mm,16*mm,18*mm])
            kosten_tbl.setStyle(tbl_style())
            story.append(kosten_tbl)
            teil_zeilen = []
            for b in rechnungen:
                tb = teil_map.get(g(b,"id",0))
                brutto_t = g(b,"betrag_brutto",4); w_t = g(b,"waehrung",5) or "EUR"
                if tb and brutto_t and w_t == tb["waehrung"] and 0 < tb["teilbetrag"] < float(brutto_t) - 0.004:
                    eur_ges = beleg_eur_mit_anteil(brutto_t, w_t, g(b,"betrag_eur",6), g(b,"betrag_eur_final",10),
                                                   g(b,"nebenkosten_eur",11), g(b,"kreditkarte_auslandsentgelt_eur",15),
                                                   kat["anteile"].get(g(b,"id",0)))
                    eur_bw = eur_ges * tb["teilbetrag"] / float(brutto_t)
                    teil_zeilen.append(
                        f"Beleg #{g(b,'id',0)} ({g(b,'anbieter',2) or '–'}, {float(brutto_t):.2f} {w_t}): "
                        f"davon Bewirtung {tb['teilbetrag']:.2f} {w_t} = {eur_bw:.2f} € ({tb['nummer']}), "
                        f"übrige Leistungen {eur_ges - eur_bw:.2f} €")
            if teil_zeilen:
                story.append(Spacer(1, 2*mm))
                story.append(Paragraph("Aufteilung Rechnung und Bewirtung (die Gesamtsumme bleibt unverändert):", styles["Normal"]))
                for zeile_t in teil_zeilen:
                    story.append(Paragraph("– " + esc(zeile_t), styles["Normal"]))
            for nid in kat["nachweis"]:
                nb = deck_by_id[nid]
                story.append(Spacer(1, 2*mm))
                story.append(Paragraph(esc(f"Zahlungsnachweis #{nid} ({nb.get('anbieter') or '–'}, "
                                           f"{float(nb['betrag_brutto']):.2f} {nb.get('waehrung') or 'EUR'}) "
                                           "belegt die Zahlung und zählt nicht zusätzlich."), styles["Normal"]))
            if luecken_liste:
                story.append(Spacer(1, 2*mm))
                story.append(Paragraph("Hinweis: Jede Buchung braucht Rechnungen oder Quittungen. Es fehlen:", styles["Normal"]))
                for zeile_l in deckung_mod.luecken_texte(luecken_liste):
                    story.append(Paragraph("– " + esc(zeile_l), styles["Normal"]))
        else:
            story.append(Paragraph("Keine Rechnungen/Quittungen erfasst.", styles["Normal"]))

        if kat["durch_kunde"]:
            story.append(Spacer(1, 2*mm))
            story.append(Paragraph("Durch Kunde direkt bezahlt (zählt nicht in der Summe):", styles["Normal"]))
            for kid in kat["durch_kunde"]:
                kb = deck_by_id[kid]
                betrag_k = (f", {float(kb['betrag_brutto']):.2f} {kb.get('waehrung') or 'EUR'}"
                            if kb.get("betrag_brutto") else "")
                story.append(Paragraph("– " + esc(f"Beleg #{kid} ({kb.get('anbieter') or '–'}{betrag_k})"),
                                       styles["Normal"]))

        vs_daten = _vorschuss_abschluss_pdf_daten(rcode)
        if vs_daten:
            story.append(Spacer(1, 4*mm))
            story.append(Paragraph("Reisevorschuss / Bargeld-Konto (Beträge in der jeweiligen Währung)", styles["Heading3"]))
            vs_tbl = Table(vs_daten, colWidths=[34*mm,12*mm,24*mm,24*mm,22*mm,24*mm,38*mm])
            vs_tbl.setStyle(tbl_style(fusszeile=False))
            story.append(vs_tbl)
            story.append(Paragraph("Ein ausgezahlter Vorschuss ist in „Gesamt zur Abrechnung“ nicht abgezogen.",
                                   styles["Normal"]))

        story.append(Spacer(1, 6*mm))
        gesamt = vma_total_netto + trennung_total + kosten_eur
        story.append(Paragraph(f"<b>Gesamt zur Abrechnung: {gesamt:.2f} €</b>", styles["Heading2"]))

        # Anlagenverzeichnis für die im Folgenden angehängten Original-Belege
        anhaenge = [b for b in rechnungen if g(b,"s3_original",8)]
        ohne_original = [b for b in rechnungen if not g(b,"s3_original",8)]
        if anhaenge or ohne_original:
            story.append(Spacer(1, 10*mm))
            namen_mail = anlagen_namen or {}
            story.append(Paragraph("Anlagen: Original-Belege" if mit_originalen
                                   else "Anlagen: Belege als eigene PDF-Dateien in dieser E-Mail", styles["Heading2"]))
            anlagen_daten = [["Nr.", "Anbieter", "Betrag", "Seite folgt" if mit_originalen else "Datei in dieser E-Mail"]]
            for idx, b in enumerate(anhaenge, start=1):
                brutto = g(b,"betrag_brutto",4); waehrung = g(b,"waehrung",5) or "EUR"
                bet_s = f"{float(brutto):.2f} {waehrung}" if brutto else "–"
                if mit_originalen:
                    letzte = "ja"
                else:
                    letzte = zelle(namen_mail.get(g(b,"id",0)) or "nicht in dieser E-Mail")
                anlagen_daten.append([str(idx), zelle(g(b,"anbieter",2) or "–"), bet_s, letzte])
            for b in ohne_original:
                brutto = g(b,"betrag_brutto",4); waehrung = g(b,"waehrung",5) or "EUR"
                bet_s = f"{float(brutto):.2f} {waehrung}" if brutto else "–"
                anlagen_daten.append(["–", zelle(g(b,"anbieter",2) or "–"), bet_s, "kein Original vorhanden"])
            anlagen_tbl = Table(anlagen_daten, colWidths=([15*mm,80*mm,35*mm,40*mm] if mit_originalen
                                                          else [10*mm,46*mm,28*mm,82*mm]))
            anlagen_tbl.setStyle(tbl_style(fusszeile=False))
            story.append(anlagen_tbl)

        doc.build(story)
        report_bytes = buf.getvalue()

        # Original-Belege als Anhang anfügen (jeweils alle Seiten des Originals)
        import pypdf
        writer = pypdf.PdfWriter()
        for seite in pypdf.PdfReader(io.BytesIO(report_bytes)).pages:
            writer.add_page(seite)
        anhang_fehler = []
        for b in (anhaenge if mit_originalen else []):
            try:
                original_bytes = s3_download(g(b,"s3_original",8))
                for seite in pypdf.PdfReader(io.BytesIO(original_bytes)).pages:
                    writer.add_page(seite)
            except Exception as e:
                anhang_fehler.append(f'Beleg #{g(b,"id",0)}: {e}')
        out = io.BytesIO()
        writer.write(out)
        pdf_bytes = out.getvalue()

        from fastapi.responses import Response
        return Response(content=pdf_bytes, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename=Abschluss_{rcode}.pdf"})
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:800]}</pre>'))


@app.get("/test-openai")
async def test_openai():
    """Testet die OpenAI API-Verbindung."""
    import httpx, os
    if not OPENAI_KEY:
        return {"status": "fehler", "detail": "OPENAI_API_KEY nicht gesetzt"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {OPENAI_KEY}",
                         "Content-Type": "application/json"},
                json={"model": OPENAI_MODEL,
                      "messages": [{"role": "user",
                                    "content": "Antworte nur mit: OK"}],
                      "max_tokens": 5})
            if resp.status_code == 200:
                antwort = resp.json()["choices"][0]["message"]["content"]
                return {"status": "ok", "antwort": antwort, "modell": OPENAI_MODEL}
            else:
                return {"status": "fehler", "http": resp.status_code,
                        "detail": resp.text[:200]}
    except Exception as e:
        import traceback
        return {"status": "fehler", "detail": str(e),
                "trace": traceback.format_exc()[:500]}


# ── Login / Logout / Ersteinrichtung ───────────────────────────────────────────
def login_seite(fehler: str = "", next_url: str = "/") -> str:
    return f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Login – Herrhammer Reisekosten</title><style>{CSS}</style></head>
<body>
<main style="max-width:380px;margin:80px auto">
  <div style="text-align:center;margin-bottom:24px">
    <img src="/static/logo3.png" alt="Herrhammer" style="height:40px">
  </div>
  <div class="card"><div class="card-body">
    <h1 class="page-title" style="font-size:18px;margin-bottom:16px">Anmelden</h1>
    {f'<div class="alert alert-err" style="margin-bottom:12px">{fehler}</div>' if fehler else ''}
    <form method="post" action="/login">
      <input type="hidden" name="next" value="{next_url}">
      <div class="form-group">
        <label>Kürzel</label>
        <input type="text" name="kuerzel" required autofocus maxlength="5" style="text-transform:uppercase">
      </div>
      <div class="form-group">
        <label>Passwort</label>
        <input type="password" name="passwort" required>
      </div>
      <button type="submit" class="btn btn-primary" style="width:100%;margin-top:8px">Anmelden</button>
    </form>
  </div></div>
</main>
</body></html>"""

@app.get("/login", response_class=HTMLResponse)
def login_form(next: str = "/"):
    return HTMLResponse(login_seite(next_url=next))

@app.post("/login")
async def login_post(request: Request):
    form = await request.form()
    kuerzel = (form.get("kuerzel") or "").strip()
    passwort = (form.get("passwort") or "")
    next_url = (form.get("next") or "/").strip() or "/"
    ma = login_pruefen(kuerzel, passwort)
    if not ma:
        return HTMLResponse(login_seite("Kürzel oder Passwort falsch.", next_url))
    request.session["kuerzel"] = ma["kuerzel"]
    request.session["klarname"] = ma["klarname"]
    return RedirectResponse(next_url if next_url.startswith("/") else "/", status_code=303)

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

@app.get("/setup", response_class=HTMLResponse)
def setup_form():
    if hat_bereits_passwoerter():
        return HTMLResponse(shell("Gesperrt",
            '<div class="alert alert-err">Die Ersteinrichtung ist bereits abgeschlossen. '
            'Bitte über <a href="/login">Login</a> anmelden.</div>'))
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv=TRUE ORDER BY klarname")
        rows = cur.fetchall()
        cur.close(); db.close()
        opts = "".join(
            f'<option value="{(r[0] if isinstance(r,tuple) else r["kuerzel"])}">'
            f'{(r[0] if isinstance(r,tuple) else r["kuerzel"])} – {(r[1] if isinstance(r,tuple) else r["klarname"])}</option>'
            for r in rows)
    except Exception:
        opts = ""
    content = f"""
    <div style="max-width:420px;margin:0 auto">
      <div class="card"><div class="card-body">
        <h1 class="page-title" style="font-size:18px">Ersteinrichtung – erstes Passwort vergeben</h1>
        <p style="font-size:13px;color:var(--muted);margin-bottom:16px">
          Es ist noch kein Login im System hinterlegt. Wähle einen bestehenden Mitarbeiter
          und vergib das erste Passwort. Danach ist diese Seite gesperrt.</p>
        {'<div class="alert alert-warn">Es sind noch keine Mitarbeiter angelegt – lege zuerst über die Datenbank/Import einen an, oder wende dich an den Entwickler.</div>' if not opts else f'''
        <form method="post" action="/setup">
          <div class="form-group">
            <label>Mitarbeiter</label>
            <select name="kuerzel" required>{opts}</select>
          </div>
          <div class="form-group">
            <label>Neues Passwort</label>
            <input type="password" name="passwort" required minlength="8">
          </div>
          <div class="form-group">
            <label>Passwort wiederholen</label>
            <input type="password" name="passwort2" required minlength="8">
          </div>
          <button type="submit" class="btn btn-primary" style="width:100%">Passwort setzen</button>
        </form>'''}
      </div></div>
    </div>"""
    return HTMLResponse(shell("Ersteinrichtung", content))

@app.post("/setup")
async def setup_post(request: Request):
    if hat_bereits_passwoerter():
        return HTMLResponse(shell("Gesperrt", '<div class="alert alert-err">Ersteinrichtung bereits abgeschlossen.</div>'))
    form = await request.form()
    kuerzel = (form.get("kuerzel") or "").strip().upper()
    pw1 = form.get("passwort") or ""; pw2 = form.get("passwort2") or ""
    if not kuerzel or len(pw1) < 8 or pw1 != pw2:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Passwörter fehlen, stimmen nicht überein oder sind zu kurz (min. 8 Zeichen).</div>'
            '<a href="/setup" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE mitarbeiter SET passwort_hash={P} WHERE kuerzel={P}",
                    (passwort_hashen(pw1), kuerzel))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/login", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

def eigenbelege_pflichtfelder_reparieren() -> None:
    """Setzt Eigenbelege (Bewirtung, Vorschuss), die fälschlich als 'fehlende Pflichtfelder' markiert wurden,
    wieder auf vollständig. Idempotent, ändert nur diese Belegarten."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute(f"""UPDATE belege SET pflichtfelder_ok = {'TRUE' if is_postgres() else '1'},
                        fehlende_felder = '[]', status = 'ok'
                        WHERE belegart IN {EIGENBELEG_SQL}
                        AND pflichtfelder_ok = {'FALSE' if is_postgres() else '0'}""")
        db.commit(); cur.close(); db.close()
    except Exception:
        pass


def bewirtung_kosten_reparieren() -> None:
    """Hält Belege in Ordnung (idempotent, günstig):
    1. beschädigte Bewirtungs-/Vorschussbelege aus den Buchungsdaten wiederherstellen,
    2. Bewirtungsbeleg ohne eigenen Betrag/Kurs (siehe bw.kosten_abgleichen),
    3. Wechselkurse in der Schreibweise 'Fremdwährung je 1 EUR' auf 'EUR je 1 Fremdwährung' umstellen."""
    try:
        db = get_db()
        try:
            bw.eigenbelege_wiederherstellen(db)
            vs.eigenbelege_wiederherstellen(db)
            bw.kosten_abgleichen(db)
            mod_beleg.kurse_vereinheitlichen(db)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    except Exception:
        pass


@app.get("/init")
def init():
    """Legt Tabellen an. Bestehende Tabellen werden NICHT gelöscht."""
    try:
        db = get_db(); cur = db.cursor()
        for sql in get_schema():
            cur.execute(sql)
        db.commit()
        for sql in get_migrations():
            try:
                cur.execute(sql)
                db.commit()
            except Exception:
                db.rollback()
        cur.close(); db.close()
        repair_legacy_columns()
        migriere_verknuepfungen_zu_gruppen()
        eigenbelege_pflichtfelder_reparieren()
        bewirtung_kosten_reparieren()
        return {"status": "ok", "version": APP_VERSION,
                "db": "postgresql" if is_postgres() else "sqlite"}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}

@app.get("/init-reset")
def init_reset(confirm: str = ""):
    """
    Löscht ALLE Tabellen und legt sie neu an.
    Nur aufrufen mit ?confirm=ja
    """
    if confirm != "ja":
        return {"status": "warten",
                "hinweis": "Aufruf mit ?confirm=ja um alle Daten zu löschen und neu anzulegen"}
    try:
        db = get_db(); cur = db.cursor()
        # Tabellen in richtiger Reihenfolge löschen (Foreign Keys beachten)
        for tbl in ["belege", "reise_laender", "reise_mitarbeiter", "reisen", "mitarbeiter"]:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
            except: pass
        db.commit()
        # Neu anlegen
        for sql in get_schema():
            cur.execute(sql)
        db.commit(); cur.close(); db.close()
        return {"status": "ok", "aktion": "reset+init", "version": APP_VERSION,
                "db": "postgresql" if is_postgres() else "sqlite"}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}

@app.get("/health")
def health():
    """
    Bewusst extrem leichter, öffentlicher Endpunkt OHNE Datenbankzugriff –
    gedacht zum reinen "Wach halten" des Servers (Render-Free-Tier schläft
    nach 15 Min. ganz ohne Traffic ein). Unabhängig von der Flug-/Bahn-Alert-
    Logik, damit die App auch wach bleibt, falls jener Cron-Job mal ausfällt
    oder deaktiviert wird.
    """
    return {"status": "ok"}

@app.get("/version")
def version():
    return {"version": APP_VERSION,
            "db": "postgresql" if is_postgres() else "sqlite"}

# ── Dashboard ──────────────────────────────────────────────────────────────────

def todo_liste_laden() -> list:
    """
    Konsolidierte Aufgabenliste für Organisatoren: alles, was noch offen ist,
    bis eine Reise final abgerechnet werden kann. Zeigt IMMER alle Belege/
    Reisen (nicht auf einzelne Mitarbeiter gefiltert) – nur für Organisatoren
    gedacht. Alle Zeilen verlinken auf die zentrale /todo-Seite (dort als
    anklickbare Tabelle statt pauschal zur ganzen Belegliste).
    """
    db = get_db(); cur = db.cursor()
    P = ph()
    today = date.today().isoformat()
    aufgaben = []

    try:
        # 1. Betrag final noch nicht bestätigt (Fremdwährung, nur Schätzung vorhanden)
        cur.execute(f"""SELECT COUNT(*) FROM belege
            WHERE waehrung != 'EUR' AND betrag_eur_final IS NULL AND betrag_brutto IS NOT NULL
            AND COALESCE(zahlungsart,'') != 'DurchKunde'""")
        n = cur.fetchone()[0]
        if n:
            aufgaben.append({"icon": "💶", "prio": "warn",
                "text": f'Betrag final noch nicht bestätigt ({n} Beleg{"e" if n!=1 else ""})',
                "sub": "Fremdwährung, nur KI-Schätzung vorhanden",
                "url": "/todo"})
    except Exception: pass

    try:
        # 1b. Kreditkarten-Belege aus dem Ausland, noch nicht mit der VISA-Abrechnung abgeglichen
        cur.execute(f"SELECT COUNT(*) FROM belege b WHERE {_KK_AUSLAND_WHERE} AND b.kk_abrechnung_datum IS NULL")
        n = cur.fetchone()[0]
        if n:
            aufgaben.append({"icon": "🌍", "prio": "warn",
                "text": f'Kreditkarte Ausland: {n} Beleg{"e" if n!=1 else ""} mit der VISA-Abrechnung abgleichen',
                "sub": "Fremdwährung oder EUR im Ausland, Euro-Betrag und Gebühr nachtragen",
                "url": "/einstellungen/kreditkarten/ausland"})
    except Exception: pass

    try:
        # 2. Belege noch nicht geprüft, obwohl die Reise bereits zurück ist
        cur.execute(f"""SELECT COUNT(*) FROM belege b
            JOIN reisen r ON r.code = b.reise_code
            WHERE b.belegart IN {PRUEF_SQL} AND b.geprueft = {'FALSE' if is_postgres() else '0'}
            AND r.rueckkehr < {P}""", (today,))
        n = cur.fetchone()[0]
        if n:
            aufgaben.append({"icon": "✅", "prio": "danger",
                "text": f'Belege noch nicht geprüft ({n})',
                "sub": "Reise bereits abgeschlossen",
                "url": "/todo"})
    except Exception: pass

    try:
        # 3. Reisen, bei denen alle prüfpflichtigen Belege geprüft sind, die aber noch nicht an Habel gesendet wurden
        #    (an Habel geht die ganze Reise in einer Mail, nicht der einzelne Beleg)
        cur.execute(f"""SELECT r.code FROM reisen r
            WHERE r.rueckkehr < {P}
            AND EXISTS (SELECT 1 FROM belege b WHERE b.reise_code = r.code
                        AND b.belegart IN {PRUEF_SQL} AND b.dms_versendet_am IS NULL)
            AND NOT EXISTS (SELECT 1 FROM belege b WHERE b.reise_code = r.code
                        AND b.belegart IN {PRUEF_SQL} AND b.geprueft = {'FALSE' if is_postgres() else '0'})
            ORDER BY r.rueckkehr""", (today,))
        codes_habel = [(x[0] if isinstance(x, tuple) else x["code"]) for x in cur.fetchall()]
        if codes_habel:
            n_h = len(codes_habel)
            aufgaben.append({"icon": "📤", "prio": "info",
                "text": f'{n_h} Reise{"n" if n_h != 1 else ""} bereit zur Übergabe an Habel',
                "sub": ", ".join(codes_habel[:5]) + (" …" if n_h > 5 else ""),
                "url": f"/reise/{codes_habel[0]}/habel" if n_h == 1 else "/todo"})
    except Exception: pass

    try:
        # 4. Fehlende Pflichtfelder
        cur.execute(f"""SELECT COUNT(*) FROM belege
            WHERE pflichtfelder_ok = {'FALSE' if is_postgres() else '0'}""")
        n = cur.fetchone()[0]
        if n:
            aufgaben.append({"icon": "⚠", "prio": "danger",
                "text": f'Pflichtfelder fehlen ({n})',
                "sub": "",
                "url": "/todo"})
    except Exception: pass

    try:
        # 5. Reisen abgeschlossen (Rückkehr liegt bereits zurück), aber noch
        # mindestens ein Rechnungs-/Quittungsbeleg ungeprüft oder ungesendet
        cur.execute(f"""SELECT DISTINCT r.code, r.titel FROM reisen r
            WHERE r.rueckkehr < {P} AND EXISTS (
                SELECT 1 FROM belege b WHERE b.reise_code = r.code
                AND b.belegart IN {PRUEF_SQL}
                AND (b.geprueft = {'FALSE' if is_postgres() else '0'} OR b.dms_versendet_am IS NULL)
            )""", (today,))
        rows = cur.fetchall()
        if rows:
            aufgaben.append({"icon": "🧾", "prio": "info",
                "text": f'Reisen abgeschlossen, aber noch nicht final abgerechnet ({len(rows)})',
                "sub": "Rückkehr liegt bereits zurück",
                "url": "/todo"})
    except Exception: pass

    try:
        # 6. Bargeld-Konto / Reisevorschuss: unbestätigte Buchungen, offene Rückgabe, Bar-Belege ohne Zuordnung
        vs_reisen = _vorschuss_todo_laden()
        if vs_reisen:
            aufgaben.append({"icon": "💵", "prio": "warn",
                "text": f'Reisevorschuss: offene Punkte bei {len(vs_reisen)} Reise{"n" if len(vs_reisen)!=1 else ""}',
                "sub": "Bestätigung, Rückgabe oder Bar-Belege klären",
                "url": "/todo"})
    except Exception: pass

    cur.close(); db.close()
    return aufgaben


def todo_belege_laden() -> list:
    """
    Liefert JEDEN betroffenen Beleg genau einmal, mit ALLEN auf ihn
    zutreffenden Problemen als Liste – für die anklickbare Tabelle auf /todo.
    """
    db = get_db(); cur = db.cursor()
    P = ph()
    today = date.today().isoformat()

    cur.execute(f"""SELECT b.id, b.reise_code, b.anbieter, b.betrag_brutto, b.waehrung,
            b.belegdatum, b.betrag_eur_final, b.geprueft, b.dms_versendet_am,
            b.pflichtfelder_ok, b.belegart, r.rueckkehr, b.zahlungsart
        FROM belege b LEFT JOIN reisen r ON r.code = b.reise_code
        ORDER BY b.erstellt DESC""")
    rows = cur.fetchall()
    cur.close(); db.close()

    def g(row, k, i): return row[k] if hasattr(row, 'keys') else row[i]

    def rueckkehr_vorbei(rk) -> bool:
        if isinstance(rk, str):
            try: rk = date.fromisoformat(rk[:10])
            except Exception: return False
        return bool(rk) and rk < date.today()

    # Buchungen ohne (vollständige) Rechnung/Quittung – erst nach der Rückkehr ein Problem
    luecken_je_beleg = {}
    for code in {g(r,"reise_code",1) for r in rows
                 if g(r,"reise_code",1) and deckung_mod.ist_buchung(g(r,"belegart",10)) and rueckkehr_vorbei(g(r,"rueckkehr",11))}:
        try:
            for l in deckung_mod.luecken(deckung_mod.belege_der_reise(code)):
                luecken_je_beleg[l["buchung_id"]] = l
        except Exception:
            pass

    ergebnis = []
    for r in rows:
        probleme = []
        waehrung = g(r,"waehrung",4) or "EUR"
        betrag_brutto = g(r,"betrag_brutto",3)
        betrag_eur_final = g(r,"betrag_eur_final",6)
        geprueft = bool(g(r,"geprueft",7))
        dms_versendet = g(r,"dms_versendet_am",8)
        pflicht_ok = g(r,"pflichtfelder_ok",9)
        belegart = g(r,"belegart",10) or ""
        rueckkehr = g(r,"rueckkehr",11)

        if (g(r,"zahlungsart",12) or "") != "DurchKunde" and waehrung != "EUR" \
                and betrag_eur_final is None and betrag_brutto is not None:
            probleme.append("💶 Betrag final offen")
        if belegart in PRUEF_BELEGARTEN:
            rueckkehr_d = rueckkehr
            if isinstance(rueckkehr_d, str):
                try: rueckkehr_d = date.fromisoformat(rueckkehr_d[:10])
                except Exception: rueckkehr_d = None
            if not geprueft and rueckkehr_d and rueckkehr_d < date.today():
                probleme.append("✅ Nicht geprüft")
        if pflicht_ok is False or pflicht_ok == 0:
            probleme.append("⚠ Pflichtfelder fehlen")
        l_b = luecken_je_beleg.get(g(r,"id",0))
        if l_b:
            probleme.append(f"🧾 Rechnung/Quittung fehlt noch ({l_b['fehlt']:.2f} {l_b['waehrung']})"
                            if l_b["fehlt"] else "🧾 Rechnung/Quittung fehlt noch")

        if probleme:
            ergebnis.append({
                "id": g(r,"id",0), "reise_code": g(r,"reise_code",1) or "–",
                "anbieter": g(r,"anbieter",2) or "–", "betrag": betrag_brutto,
                "waehrung": waehrung, "belegdatum": g(r,"belegdatum",5),
                "probleme": probleme,
            })
    return ergebnis


def todo_reisen_laden() -> list:
    """Reisen, die zurück, aber noch nicht final abgerechnet sind (für /todo)."""
    db = get_db(); cur = db.cursor()
    P = ph()
    today = date.today().isoformat()
    cur.execute(f"""SELECT DISTINCT r.code, r.titel, r.rueckkehr FROM reisen r
        WHERE r.rueckkehr < {P} AND EXISTS (
            SELECT 1 FROM belege b WHERE b.reise_code = r.code
            AND b.belegart IN {PRUEF_SQL}
            AND (b.geprueft = {'FALSE' if is_postgres() else '0'} OR b.dms_versendet_am IS NULL)
        ) ORDER BY r.rueckkehr DESC""", (today,))
    rows = cur.fetchall()
    cur.close(); db.close()
    return rows



# ── Dashboard + Mitarbeiter ───────────────────────────────────────────────────
@app.get("/todo", response_class=HTMLResponse)
def todo_seite(request: Request):
    """Zentrale ToDo-Seite für Organisatoren: jeder betroffene Beleg als eigene
    Zeile mit allen zutreffenden Problemen, direkt anklickbar."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren haben Zugriff auf die ToDo-Liste.</div>'), status_code=403)
    try:
        belege = todo_belege_laden()
        reisen = todo_reisen_laden()

        def get(r, k, i): return r[k] if hasattr(r, 'keys') else r[i]

        beleg_zeilen = ""
        for b in belege:
            bet_s = f"{float(b['betrag']):.2f} {b['waehrung']}" if b['betrag'] else "–"
            probleme_html = " ".join(
                f'<span style="font-size:11px;background:var(--bg);color:var(--muted);'
                f'padding:2px 8px;border-radius:10px;white-space:nowrap">{p}</span>'
                for p in b["probleme"])
            beleg_zeilen += f"""<tr>
                <td><a href="/beleg/{b['id']}" style="color:var(--blue);font-weight:600;
                    text-decoration:none">#{b['id']}</a></td>
                <td style="font-family:monospace;font-size:12px;color:var(--blue)">{b['reise_code']}</td>
                <td>{b['anbieter']}</td>
                <td style="text-align:right;font-family:monospace">{bet_s}</td>
                <td>{fmt_date(b['belegdatum'])}</td>
                <td><div style="display:flex;gap:4px;flex-wrap:wrap">{probleme_html}</div></td>
            </tr>"""

        vs_zeilen = ""
        for v in _vorschuss_todo_laden():
            vs_zeilen += (f'<tr><td><a href="/reise/{v["reise_code"]}/vorschuss" style="color:var(--blue);font-weight:600;'
                          f'text-decoration:none">{v["reise_code"]}</a></td><td>{v["titel"]}</td>'
                          f'<td>{"<br>".join(v["punkte"])}</td>'
                          f'<td><a href="/reise/{v["reise_code"]}/vorschuss" class="btn btn-secondary btn-sm">Öffnen</a></td></tr>')

        reisen_zeilen = ""
        for r in reisen:
            code = get(r,"code",0); titel = get(r,"titel",1); rueckkehr = get(r,"rueckkehr",2)
            reisen_zeilen += f"""<tr>
                <td><a href="/reise/{code}/abschluss" style="color:var(--blue);font-weight:600;
                    text-decoration:none">{code}</a></td>
                <td>{titel}</td>
                <td>{fmt_date(rueckkehr)}</td>
                <td style="font-size:12px">{habel_reise_status_html(code)}</td>
                <td style="white-space:nowrap">
                  <a href="/reise/{code}/abschluss" class="btn btn-secondary btn-sm">Abschluss ansehen</a>
                  <a href="/reise/{code}/habel" class="btn btn-secondary btn-sm">📤 Habel</a></td>
            </tr>"""

        try:
            n_kk = kk_ausland_offen_anzahl()
        except Exception:
            n_kk = 0
        kk_banner = (f'<div class="alert alert-warn" style="margin-bottom:20px">🌍 <b>{n_kk} Kreditkarten-Beleg{"e" if n_kk != 1 else ""} '
                     f'aus dem Ausland</b> {"warten" if n_kk != 1 else "wartet"} auf den Abgleich mit der VISA-Abrechnung. '
                     '<a href="/einstellungen/kreditkarten/ausland" style="font-weight:600">Liste öffnen →</a></div>') if n_kk else ""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
          <h1 class="page-title" style="margin:0">📋 Offene Aufgaben</h1>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>
        {kk_banner}

        <div class="card" style="margin-bottom:20px">
          <div class="card-header"><span class="card-title">Belege mit offenen Punkten ({len(belege)})</span></div>
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Beleg</th><th>Reise</th><th>Anbieter</th><th style="text-align:right">Betrag</th>
                <th>Datum</th><th>Probleme</th>
              </tr></thead>
              <tbody>
                {beleg_zeilen or '<tr><td colspan="6" class="empty-state">Keine offenen Belege 🎉</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>

        <div class="card" style="margin-bottom:20px">
          <div class="card-header"><span class="card-title">💵 Reisevorschuss / Bargeld-Konto – offene Punkte</span></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Reise</th><th>Titel</th><th>Offen</th><th></th></tr></thead>
              <tbody>
                {vs_zeilen or '<tr><td colspan="4" class="empty-state">Keine offenen Punkte 🎉</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="card-header"><span class="card-title">Reisen abgeschlossen, aber noch nicht final abgerechnet ({len(reisen)})</span></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Reise</th><th>Titel</th><th>Rückkehr</th><th>Übergabe an Habel</th><th></th></tr></thead>
              <tbody>
                {reisen_zeilen or '<tr><td colspan="5" class="empty-state">Keine offenen Reisen 🎉</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>"""
        return HTMLResponse(shell("Offene Aufgaben", content, "start"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:800]}</pre>'))


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        today = date.today()

        # Mitarbeiter
        cur.execute("SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = 1")
        ma_count = cur.fetchone()[0]

        # Alle Reisen mit Mitarbeitern
        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                STRING_AGG(rm.kuerzel, ', ' ORDER BY rm.kuerzel) as ma
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                ORDER BY r.abreise DESC""")
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                GROUP_CONCAT(rm.kuerzel, ', ') as ma
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                ORDER BY r.abreise DESC""")
        alle_reisen = cur.fetchall()

        # Unzugeordnete Belege
        try:
            cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
            unzugeordnet = cur.fetchone()[0]
        except: unzugeordnet = 0
        cur.close(); db.close()

        def to_date(v):
            if isinstance(v, date): return v
            try: return date.fromisoformat(str(v)[:10])
            except: return None

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        # In 3 Gruppen aufteilen
        aktiv = []; geplant = []; abgeschlossen = []
        for r in alle_reisen:
            ab = to_date(get(r,"abreise",2))
            zu = to_date(get(r,"rueckkehr",3))
            if not ab or not zu:
                geplant.append(r)
            elif today < ab:
                geplant.append(r)
            elif today <= zu:
                aktiv.append(r)
            else:
                abgeschlossen.append(r)

        # Geplante Reisen: die nächste zuerst, die am weitesten entfernte zuletzt
        # (alle_reisen kommt insgesamt DESC sortiert, für "geplant" wollen wir ASC)
        geplant.sort(key=lambda r: to_date(get(r,"abreise",2)) or date.max)

        def reise_zeile(r, typ=""):
            code=get(r,"code",0); titel=get(r,"titel",1)
            ab=to_date(get(r,"abreise",2)); zu=to_date(get(r,"rueckkehr",3))
            ma=get(r,"ma",4) or "–"
            tage = (zu-ab).days+1 if ab and zu else "?"
            if typ=="aktiv":
                badge = '<span class="badge badge-green">● Aktiv</span>'
                link_extra = f'<a href="/reise/{code}" style="font-size:11px;color:var(--muted);margin-left:8px">Details</a>'
            elif typ=="geplant":
                delta = (ab-today).days if ab else 0
                badge = f'<span class="badge badge-blue">in {delta} Tagen</span>'
                link_extra = f'<a href="/reise/{code}" style="font-size:11px;color:var(--muted);margin-left:8px">Details</a>'
            else:
                badge = '<span class="badge badge-gray">Abgeschlossen</span>'
                link_extra = f'<a href="/reise/{code}/abschluss" style="font-size:11px;color:var(--muted);margin-left:8px">Abschluss</a>'
            return f"""<tr>
                <td><a href="/reise/{code}" class="td-mono"
                    style="color:var(--blue);font-weight:600">{code}</a>{link_extra}</td>
                <td style="font-weight:500">
                  <a href="/reise/{code}" style="color:inherit;text-decoration:none">{titel}</a>
                </td>
                <td>{fmt_date(ab)}</td>
                <td>{fmt_date(zu)}</td>
                <td style="color:var(--muted)">{ma}</td>
                <td style="text-align:center;color:var(--muted)">{tage}</td>
                <td>{badge}</td>
            </tr>"""

        def sektion(titel_s, emoji, rows, typ, limit=None):
            if not rows:
                return f"""<div class="card" style="margin-bottom:16px">
                  <div class="card-header">
                    <span class="card-title">{emoji} {titel_s} (0)</span>
                    <a href="/reisen/neu" class="btn btn-primary btn-sm">+ Neue Reise</a>
                  </div>
                  <div class="empty-state"><p>Keine {titel_s.lower()}</p></div>
                </div>"""
            anzeige = rows[:limit] if limit else rows
            mehr = len(rows) - len(anzeige)
            zeilen = "".join(reise_zeile(r, typ) for r in anzeige)
            mehr_link = (f'<tr><td colspan="7" style="text-align:center;padding:10px;'
                         f'font-size:12px;color:var(--muted)">'
                         f'<a href="/reisen" style="color:var(--blue)">'
                         f'+ {mehr} weitere anzeigen →</a></td></tr>') if mehr else ""
            return f"""<div class="sektion-header">
              <span class="sektion-titel">{emoji} {titel_s}</span>
              <span class="sektion-count">{len(rows)}</span>
              <div style="margin-left:auto;display:flex;gap:8px">
                {'<a href="/maps" class="btn btn-secondary btn-sm">🗺 Maps</a>' if typ == "aktiv" else ''}
                <a href="/reisen/neu" class="btn btn-primary btn-sm">+ Neue Reise</a>
              </div>
            </div>
            <div class="card" style="margin-bottom:16px">
              <div class="table-wrap"><table>
                <thead><tr>
                  <th>Code</th><th>Titel</th><th>Abreise</th>
                  <th>Rückkehr</th><th>Mitarbeiter</th><th style="text-align:center">Tage</th>
                  <th>Status</th>
                </tr></thead>
                <tbody>{zeilen}{mehr_link}</tbody>
              </table></div>
            </div>"""

        # Weitere Alarme prüfen
        try:
            cur2 = db.cursor() if not db.closed else get_db().cursor()
        except:
            db2 = get_db(); cur2 = db2.cursor()
        
        alarme = []
        if unzugeordnet > 0:
            alarme.append({
                "url": "/unzugeordnet",
                "text": f'⚠ {unzugeordnet} Beleg{"e" if unzugeordnet!=1 else ""} ohne Reisezuordnung',
                "sub": "Jetzt zuordnen →"
            })
        try:
            cur2.execute(
                "SELECT id FROM belege WHERE pflichtfelder_ok = FALSE ORDER BY erstellt DESC"
                if is_postgres() else
                "SELECT id FROM belege WHERE pflichtfelder_ok = 0 ORDER BY erstellt DESC")
            fehler_ids = [row[0] for row in cur2.fetchall()]
            n_fehler = len(fehler_ids)
            if n_fehler > 0:
                alarme.append({
                    "url": f"/beleg/{fehler_ids[0]}" if n_fehler == 1 else "/belege",
                    "text": f'⚠ {n_fehler} Beleg{"e" if n_fehler!=1 else ""} mit fehlenden Pflichtfeldern',
                    "sub": "Beleg öffnen →" if n_fehler == 1 else "Zur Belegliste →"
                })
        except: pass
        try:
            cur2.execute(
                f"""SELECT id FROM belege
                   WHERE waehrung != 'EUR' AND (kurs_eur IS NULL OR kurs_eur = 0)
                   AND betrag_brutto IS NOT NULL AND betrag_eur_final IS NULL
                   AND COALESCE(zahlungsart,'') != 'DurchKunde'
                   AND COALESCE(belegart,'') NOT IN {EIGENBELEG_SQL}
                   ORDER BY erstellt DESC""")
            kurs_ids = [row[0] for row in cur2.fetchall()]
            n_kurs = len(kurs_ids)
            if n_kurs > 0:
                alarme.append({
                    "url": f"/beleg/{kurs_ids[0]}" if n_kurs == 1 else "/belege",
                    "text": f'💱 {n_kurs} Auslandsbeleg{"e" if n_kurs!=1 else ""} ohne Wechselkurs',
                    "sub": "Kurs nachtragen →"
                })
        except: pass
        try:
            for a in offene_alerts_fuer_dashboard():
                icon = "✈" if a["typ"] == "Flug" else "🚆"
                verspaetung_txt = f' · +{a["verspaetung"]} Min' if a.get("verspaetung") else ""
                alarme.append({
                    "url": f"/beleg/{a['beleg_id']}",
                    "text": f'{icon} {a["typ"]} {a["nummer"]} ({a["von"]}→{a["nach"]}): {a["status"]}{verspaetung_txt}',
                    "sub": "Beleg öffnen →",
                    "reset_url": f"/beleg/{a['beleg_id']}/flugstatus/zuruecksetzen",
                    "umbuchen_url": f"/beleg/{a['beleg_id']}/segment/{a['segment_index']}/umbuchen"
                })
        except: pass
        try: cur2.close()
        except: pass

        if alarme:
            warn_items = "".join(
                f'<div style="display:flex;align-items:center;'
                f'justify-content:space-between;padding:10px 16px;'
                f'border-bottom:1px solid #fecaca">'
                f'<a href="{a["url"]}" style="text-decoration:none;color:#991b1b;font-weight:600;flex:1">{a["text"]}</a>'
                f'<span style="display:flex;align-items:center;gap:10px">'
                + (f'<a href="{a["umbuchen_url"]}" style="font-size:11px;color:#7c3aed;text-decoration:none">✏ Umgebucht</a>' if a.get("umbuchen_url") else "")
                + (f'<a href="{a["reset_url"]}" onclick="return confirm(\'Diesen (ggf. veralteten) Status verwerfen und beim nächsten Checkpoint neu prüfen?\')" '
                   f'style="font-size:11px;color:#94a3b8;text-decoration:none">↺ Zurücksetzen</a>' if a.get("reset_url") else "")
                + f'<a href="{a["url"]}" style="font-size:12px;color:#ef4444;text-decoration:none">{a["sub"]}</a>'
                f'</span></div>'
                for a in alarme)
            warn_html = (
                f'<div style="background:#fef2f2;border:1px solid #fca5a5;'
                f'border-radius:var(--radius);margin-bottom:20px;overflow:hidden">'
                f'<div style="padding:8px 16px;background:#fecaca;font-size:11px;'
                f'font-weight:700;color:#991b1b;text-transform:uppercase;letter-spacing:.05em">'
                f'🔔 Aktionen erforderlich ({len(alarme)})</div>'
                f'{warn_items}</div>')
        else:
            warn_html = (
                f'<div style="background:var(--green-l);border:1px solid #6ee7b7;'
                f'border-radius:var(--radius);padding:10px 16px;margin-bottom:20px;'
                f'font-size:13px;color:#065f46;font-weight:500">'
                f'✅ Alles in Ordnung – keine offenen Aktionen</div>')

        # ToDo-Liste – nur für Organisatoren: konsolidierter Überblick über alles,
        # was bis zur finalen Abrechnung noch offen ist (nicht auf den einzelnen
        # Mitarbeiter gefiltert)
        todo_html = ""
        if ist_organisator(request):
            todo_items = todo_liste_laden()
            PRIO_FARBEN = {
                "danger": ("#991b1b", "#ef4444"),
                "warn": ("#92400e", "#d97706"),
                "info": ("#334155", "#64748b"),
            }
            if todo_items:
                zeilen = ""
                for t in todo_items:
                    fg, sub_fg = PRIO_FARBEN.get(t["prio"], PRIO_FARBEN["info"])
                    zeilen += (
                        f'<a href="{t["url"]}" style="display:flex;align-items:center;gap:10px;'
                        f'padding:10px 16px;text-decoration:none;color:{fg};'
                        f'border-bottom:1px solid var(--border)">'
                        f'<span style="font-size:16px">{t["icon"]}</span>'
                        f'<span style="flex:1;font-weight:600;font-size:13px">{t["text"]}</span>'
                        f'<span style="font-size:12px;color:{sub_fg}">{t["sub"]}</span>'
                        f'</a>')
                todo_html = (
                    f'<div style="background:var(--white);border:1px solid var(--border);'
                    f'border-radius:var(--radius);margin-bottom:20px;overflow:hidden">'
                    f'<div style="padding:8px 16px;background:var(--bg);font-size:11px;'
                    f'font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.05em">'
                    f'📋 Offene Aufgaben (Organisator) ({len(todo_items)})</div>'
                    f'{zeilen}</div>')
            else:
                todo_html = (
                    f'<div style="background:var(--green-l);border:1px solid #6ee7b7;'
                    f'border-radius:var(--radius);padding:10px 16px;margin-bottom:20px;'
                    f'font-size:13px;color:#065f46;font-weight:500">'
                    f'✅ Keine offenen Aufgaben – alles final abgerechnet</div>')

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;
                    margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Dashboard</h1>
          <div style="display:flex;gap:8px">
            <a href="/mails-abrufen" class="btn btn-success">📬 Mails abrufen</a>
            <a href="/beleg/upload" class="btn btn-secondary">📎 Beleg hochladen</a>
            <a href="/einstellungen/alerts" class="btn btn-secondary">✈ Alert-Einstellungen</a>
            <a href="/einstellungen/backups" class="btn btn-secondary">💾 Backups</a>
          </div>
        </div>

        {todo_html}
        {warn_html}

        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px">
          <div class="stat-card" style="border-top:3px solid var(--green)">
            <div class="stat-num" style="color:var(--green)">{len(aktiv)}</div>
            <div class="stat-label">🟢 Aktive Reisen</div>
          </div>
          <div class="stat-card" style="border-top:3px solid var(--blue)">
            <div class="stat-num" style="color:var(--blue)">{len(geplant)}</div>
            <div class="stat-label">📋 In Planung</div>
          </div>
          <div class="stat-card" style="border-top:3px solid var(--border-strong)">
            <div class="stat-num" style="color:var(--muted)">{len(abgeschlossen)}</div>
            <div class="stat-label">✓ Abgeschlossen</div>
          </div>
          <div class="stat-card" style="border-top:3px solid #8b5cf6">
            <div class="stat-num" style="color:#5b21b6">{ma_count}</div>
            <div class="stat-label">👤 Mitarbeiter</div>
          </div>
        </div>

        {sektion("Aktuelle Reisen", "🟢", aktiv, "aktiv")}
        {sektion("In Planung", "📋", geplant, "geplant")}
        {sektion("Abgeschlossen", "✓", abgeschlossen, "abgeschlossen", limit=5)}
        """
        return HTMLResponse(shell("Dashboard", content, "start"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f"""
        <div class="alert alert-warn">
            <b>Datenbank nicht initialisiert?</b><br>
            Bitte <a href="/init">/init aufrufen</a> um Tabellen anzulegen.<br>
            Fehler: {e}
        </div>
        <pre style="font-size:11px;color:var(--muted)">{traceback.format_exc()[:500]}</pre>
        """))


def _datum_parsen(wert):
    """Parst DD.MM.YYYY oder YYYY-MM-DD zu date, sonst None."""
    if not wert: return None
    try: return datetime.strptime(str(wert).strip(), "%d.%m.%Y").date()
    except Exception:
        try: return date.fromisoformat(str(wert)[:10])
        except Exception: return None


def aktuelle_position_ermitteln(reise_code: str, db, debug: bool = False):
    """
    Ermittelt den TATSÄCHLICHEN aktuellen Aufenthaltsort/-status für die
    Kartenanzeige – bewusst GETRENNT von der steuerlichen VMA-Länderzuordnung
    (vma_tage.land_code), die am Abreisetag absichtlich den ABFLUGort zeigt
    (rechtlich korrekt für die Verpflegungspauschale), nicht den tatsächlichen
    aktuellen Ort.

    Nutzt die Flughafen-/Bahnhof-Koordinaten, die die KI beim Analysieren des
    Belegs direkt mitliefert (von_lat/von_lon/nach_lat/nach_lon je Segment –
    aus ihrem eigenen Wissen abgeleitet, keine lokale Nachschlagetabelle nötig).

    Gibt eines von zwei Ergebnistypen zurück:
    - {"status": "unterwegs", "von_iata":.., "nach_iata":.., "von_koord":(lat,lon),
       "nach_koord":(lat,lon), "fortschritt": 0..1, "transport_typ":.., "label":..}
      wenn JETZT zwischen Abflug- und Ankunftszeit eines Segments liegt.
    - {"status": "am_ort", "lat":.., "lon":.., "land":.., "ort_name":..}
      für den zuletzt erreichten Ort (Segment-Koordinaten bevorzugt, sonst
      Länder-Zentroid als Rückfall für alte Belege ohne Koordinatenfelder).
    - None, wenn gar nichts ermittelbar ist.

    Mit debug=True wird zusätzlich (result, diagnose_liste) zurückgegeben –
    ein Eintrag pro Segment mit Rohdaten und Grund für Aufnahme/Ausschluss,
    um genau nachvollziehen zu können, welches Segment warum als "aktuell" galt.
    """
    from mod_flugalert import jetzt_lokal
    from mod_geo import koordinaten_fuer_land
    from mod_zeit import segment_zeit_zu_utc
    P = ph()
    cur = db.cursor()
    jetzt = datetime.now(timezone.utc)
    diag = [] if debug else None

    def _ret(ergebnis):
        return (ergebnis, diag) if debug else ergebnis

    def _koord(s, praefix):
        lat, lon = s.get(f"{praefix}_lat"), s.get(f"{praefix}_lon")
        try:
            return (float(lat), float(lon)) if lat is not None and lon is not None else None
        except (TypeError, ValueError):
            return None

    # Bekannte Live-Verspätungen laden (aus den Flug-/Bahn-Alert-Checks) –
    # ohne das würde die Karte bei einer Verspätung fälschlich denken, ein
    # Zug/Flug sei schon angekommen, obwohl er laut Realität noch unterwegs ist
    # (die planmäßige Ankunftszeit allein reicht dafür nicht mehr).
    verspaetungen = {}  # (beleg_id, segment_index) -> Minuten
    cur.execute(f"""SELECT b.id, fs.segment_index, fs.verspaetung_minuten
            FROM flug_status fs JOIN belege b ON b.id = fs.beleg_id
            WHERE b.reise_code={P} AND fs.verspaetung_minuten IS NOT NULL""", (reise_code,))
    for row in cur.fetchall():
        bid_v = row[0] if isinstance(row, tuple) else row["id"]
        idx_v = row[1] if isinstance(row, tuple) else row["segment_index"]
        min_v = row[2] if isinstance(row, tuple) else row["verspaetung_minuten"]
        if min_v:
            verspaetungen[(bid_v, idx_v)] = min_v

    # Alle Flug-/Bahnsegmente mit geparsten Ab-/Ankunftszeitpunkten sammeln
    cur.execute(f"""SELECT id, ki_json, transportart FROM belege
        WHERE reise_code={P} AND transportart IN ('Flug','Bahn')""", (reise_code,))
    segmente = []
    for row in cur.fetchall():
        bid_diag = row[0] if isinstance(row, tuple) else row["id"]
        ki_str = row[1] if isinstance(row, tuple) else row["ki_json"]
        typ = row[2] if isinstance(row, tuple) else row["transportart"]
        if not ki_str: continue
        try:
            segs = json.loads(ki_str).get("segmente") or []
        except Exception:
            continue
        for idx_diag, s in enumerate(segs):
            d_ab = _datum_parsen(s.get("abreise_datum"))
            d_an = _datum_parsen(s.get("ankunft_datum"))
            if debug:
                eintrag = {"beleg_id": bid_diag, "segment_index": idx_diag,
                           "von_ort": s.get("von_ort"), "nach_ort": s.get("nach_ort"),
                           "abreise_datum_roh": s.get("abreise_datum"), "abreise_zeit_roh": s.get("abreise_zeit"),
                           "ankunft_datum_roh": s.get("ankunft_datum"), "ankunft_zeit_roh": s.get("ankunft_zeit"),
                           "von_lat": s.get("von_lat"), "von_lon": s.get("von_lon"),
                           "nach_lat": s.get("nach_lat"), "nach_lon": s.get("nach_lon")}
            if not d_ab or not d_an:
                if debug:
                    eintrag["status"] = f"ÜBERSPRUNGEN – Datum nicht parsbar (abreise_datum={s.get('abreise_datum')!r}, ankunft_datum={s.get('ankunft_datum')!r})"
                    diag.append(eintrag)
                continue
            ab_zeit_roh = s.get("abreise_zeit")
            if not ab_zeit_roh:
                # WICHTIG: Fehlt die Abreisezeit komplett (z.B. bei einem
                # zweiten, unvollständigeren Beleg-Dokument desselben Flugs
                # ohne Uhrzeitangabe), NIEMALS Mitternacht raten – das würde
                # einen Flug, der tatsächlich erst später am Tag stattfindet,
                # fälschlich als "schon mitten in der Nacht abgeflogen und
                # längst angekommen" erscheinen lassen. Ohne echte Zeit ist
                # das Segment für die Positionsbestimmung wertlos -> überspringen,
                # ein anderes (hoffentlich vollständigeres) Segment/Beleg für
                # dieselbe Strecke liefert die echte Position stattdessen.
                if debug:
                    eintrag["status"] = "ÜBERSPRUNGEN – keine Abreisezeit vorhanden, keine Schätzung (verhindert Fehlpositionierung)"
                    diag.append(eintrag)
                continue
            ab_zeit = ab_zeit_roh
            an_zeit_roh = s.get("ankunft_zeit")
            dt_ab = segment_zeit_zu_utc(d_ab, ab_zeit, s.get("abreise_utc_offset"))
            if dt_ab is None:
                if debug:
                    eintrag["status"] = f"ÜBERSPRUNGEN – Abreisezeit nicht parsbar (abreise_zeit={ab_zeit!r})"
                    diag.append(eintrag)
                continue
            dt_an = segment_zeit_zu_utc(d_an, an_zeit_roh, s.get("ankunft_utc_offset")) if an_zeit_roh else None
            if dt_an is None or dt_an <= dt_ab:
                # Ankunftszeit fehlt oder wurde als "00:00" fehlinterpretiert
                # (läge dann VOR dem Abflug!) -> sichere Schätzung: Abflug + 2h,
                # statt fälschlich Mitternacht anzunehmen (das hätte die ganze
                # zeitliche Reihenfolge der Segmente durcheinandergebracht).
                dt_an = dt_ab + timedelta(hours=2)
                if debug:
                    eintrag["ankunft_geschaetzt"] = True

            # Bekannte Live-Verspätung einrechnen: sonst würde die Karte bei
            # einem verspäteten Zug/Flug fälschlich "schon angekommen" zeigen,
            # sobald nur die PLANMÄSSIGE Ankunftszeit verstrichen ist.
            verspaetung_min = verspaetungen.get((bid_diag, idx_diag))
            if verspaetung_min:
                dt_an = dt_an + timedelta(minutes=verspaetung_min)
                if debug:
                    eintrag["verspaetung_beruecksichtigt_min"] = verspaetung_min

            typ_segment = typ
            kombi_text = f'{s.get("transport_name","")} {s.get("hinweis","")}'.lower()
            if any(k in kombi_text for k in ("bahn", "train", "zug", "sncf", "ice", "tgv", "railjet")):
                typ_segment = "Bahn"
            if debug:
                eintrag["dt_ab"] = dt_ab.isoformat(); eintrag["dt_an"] = dt_an.isoformat()
                eintrag["typ_erkannt"] = typ_segment
                eintrag["status"] = "übernommen"
                diag.append(eintrag)
            segmente.append({
                "typ": typ_segment, "dt_ab": dt_ab, "dt_an": dt_an,
                "dt_ab_anzeige": f"{d_ab.strftime('%d.%m.') if d_ab else ''} {ab_zeit}".strip(),
                "dt_an_anzeige": f"{d_an.strftime('%d.%m.') if d_an else ''} {an_zeit_roh}".strip() if an_zeit_roh else "",
                "von_iata": s.get("von_iata"), "nach_iata": s.get("nach_iata"),
                "von_ort": s.get("von_ort"), "nach_ort": s.get("nach_ort"),
                "von_koord": _koord(s, "von"), "nach_koord": _koord(s, "nach"),
                "transport_nummer": s.get("transport_nummer") or "",
            })

    # 1. UNTERWEGS? – ein Segment, dessen Ab-/Ankunftszeit JETZT einschließt
    for s in segmente:
        if s["dt_ab"] <= jetzt <= s["dt_an"] and s["von_koord"] and s["nach_koord"]:
            dauer = (s["dt_an"] - s["dt_ab"]).total_seconds()
            fortschritt = ((jetzt - s["dt_ab"]).total_seconds() / dauer) if dauer > 0 else 0.5
            fortschritt = max(0.0, min(1.0, fortschritt))

            # Nächste Anschluss-Etappe ab dem Zielort dieses Segments (falls
            # vorhanden) – z.B. der Anschlusszug nach der aktuellen Fahrt.
            naechste_etappe = None
            kommende = [k for k in segmente if k["dt_ab"] >= s["dt_an"] and k is not s
                        and k["von_koord"] and k["nach_koord"]]
            if kommende:
                kommende.sort(key=lambda k: k["dt_ab"])
                k = kommende[0]
                detail = f'{k["von_iata"] or k["von_ort"] or "?"}-{k["nach_iata"] or k["nach_ort"] or "?"}'
                naechste_etappe = {"typ": k["typ"], "detail": detail, "datum": k["dt_ab"],
                                    "zeit_lokal": k["dt_ab_anzeige"]}

            cur.close()
            return _ret({
                "status": "unterwegs",
                "von_iata": s["von_iata"], "nach_iata": s["nach_iata"],
                "von_koord": s["von_koord"], "nach_koord": s["nach_koord"],
                "von_name": s["von_ort"] or s["von_iata"], "nach_name": s["nach_ort"] or s["nach_iata"],
                "fortschritt": fortschritt, "transport_typ": s["typ"],
                "dt_ab_anzeige": s["dt_ab_anzeige"], "dt_an_anzeige": s["dt_an_anzeige"],
                "dt_ab": s["dt_ab"], "dt_an": s["dt_an"], "naechste_etappe": naechste_etappe,
                "label": f'{s["transport_nummer"]} {s["von_iata"] or s["von_ort"]} → {s["nach_iata"] or s["nach_ort"]}'.strip(),
            })

    # 2. AM ORT: zeitlich jüngstes bereits erreichtes Ziel (Segment-Ankunft oder Hotel-Check-in)
    kandidaten = []  # (datetime, koord_oder_None, land_code, ort_name, typ, detail, zeit_lokal)
    for s in segmente:
        if s["dt_an"] > jetzt: continue
        land = IATA_TO_LAND.get(s["nach_iata"]) if s["nach_iata"] else None
        if not land:
            land = STADT_ZU_LAND.get((s["nach_ort"] or "").strip().lower())
        if land or s["nach_koord"]:
            detail = f'{s["von_iata"] or s["von_ort"] or "?"}-{s["nach_iata"] or s["nach_ort"] or "?"}'
            kandidaten.append((s["dt_an"], s["nach_koord"], land, s["nach_ort"] or s["nach_iata"],
                                s["typ"], detail, s["dt_an_anzeige"]))

    cur.execute(f"""SELECT land_beleg, hotel_checkin_datum, hotel_checkin_zeit, hotel_name,
        hotel_lat, hotel_lon, hotel_adresse, hotel_checkout_datum, hotel_checkout_zeit
        FROM belege WHERE reise_code={P} AND transportart='Hotel'""", (reise_code,))
    for row in cur.fetchall():
        g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
        land = g("land_beleg",0)
        d = _datum_parsen(g("hotel_checkin_datum",1))
        ci_zeit = g("hotel_checkin_zeit",2) or "14:00"
        hotel_name = g("hotel_name",3)
        hotel_lat = g("hotel_lat",4); hotel_lon = g("hotel_lon",5)
        hotel_adresse = g("hotel_adresse",6)
        d_checkout = _datum_parsen(g("hotel_checkout_datum",7))
        co_zeit = g("hotel_checkout_zeit",8) or "11:00"
        hotel_koord = (float(hotel_lat), float(hotel_lon)) if hotel_lat is not None and hotel_lon is not None else None
        if not land or not d: continue
        # Hotels haben kein UTC-Offset-Feld im Schema -> Rückfall auf MESZ
        # (segment_zeit_zu_utc's Standard), damit der Vergleich mit den jetzt
        # UTC-bewussten Flug-/Bahnzeiten trotzdem konsistent bleibt.
        dt = segment_zeit_zu_utc(d, ci_zeit, None)
        if dt is None: continue
        dt_checkout = segment_zeit_zu_utc(d_checkout, co_zeit, None) if d_checkout else None

        # WICHTIG (allgemeingültig, nicht nur "am selben Tag"): Die
        # Check-in-Uhrzeit ist oft nur ein GENERISCHER Standardwert (z.B.
        # "15:00"), unabhängig davon, wann der Reisende tatsächlich ankommt.
        # Flug/Bahn/Termin sind IMMER die präziseren Quellen und sollen das
        # Hotel als Positionsangabe überstimmen, solange sie zeitlich in den
        # Aufenthalt (Checkin bis Checkout) fallen und später als die
        # pauschale Check-in-Zeit sind – nicht nur an genau diesem Kalendertag,
        # sondern über den GESAMTEN Aufenthalt hinweg (z.B. auch bei einer
        # Zwischenlandung am Folgetag durch Zeitzonen-Verschiebung).
        fenster_ende = dt_checkout or (dt + timedelta(hours=24))
        spaeteste_ankunft_im_aufenthalt = max(
            (s["dt_an"] for s in segmente
             if dt < s["dt_an"] <= fenster_ende),
            default=None)
        if spaeteste_ankunft_im_aufenthalt:
            dt = spaeteste_ankunft_im_aufenthalt

        if dt > jetzt: continue
        zeit_lokal = f"{d.strftime('%d.%m.')} {ci_zeit}"
        # "detail" zeigt jetzt die Adresse statt den Hotelnamen zu wiederholen
        # (Name steht im Popup bereits als "Aktuell in: ..."/Ortsname)
        detail_txt = hotel_adresse or hotel_name or "–"
        # WICHTIG: hotel_koord statt None übergeben, sonst fällt die spätere
        # Positionsermittlung auf einen groben Länder-Mittelpunkt zurück (z.B.
        # Rom als Standard für "Italien", selbst wenn das Hotel in Turin liegt).
        kandidaten.append((dt, hotel_koord, land, hotel_name, "Hotel", detail_txt, zeit_lokal))

    # Termine (Kundenbesuche, Taxifahrten, Messebesuche etc.) – genau wie
    # Hotels, aber mit geokodierten Koordinaten statt Flughafen-/Bahnhofsdaten.
    cur.execute(f"""SELECT datum, uhrzeit_von, titel, typ, lat, lon, land_code, ort
        FROM termine WHERE reise_code={P} AND lat IS NOT NULL""", (reise_code,))
    for row in cur.fetchall():
        g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
        d = _datum_parsen(g("datum",0))
        t_zeit = g("uhrzeit_von",1) or "09:00"
        titel_t = g("titel",2) or "Termin"
        typ_t = g("typ",3) or "termin"
        t_lat = g("lat",4); t_lon = g("lon",5)
        t_land = (g("land_code",6) or "").upper() or None
        t_ort = g("ort",7)
        if not d or not t_land: continue
        dt = segment_zeit_zu_utc(d, t_zeit, None)
        if dt is None or dt > jetzt: continue
        zeit_lokal = f"{d.strftime('%d.%m.')} {t_zeit}"
        kandidaten.append((dt, (float(t_lat), float(t_lon)), t_land, titel_t,
                            "Termin", t_ort or titel_t, zeit_lokal))
    cur.close()

    # Start-Ort ab Mitternacht (lokal) des ersten Reisetags als Kandidat
    # ergänzen: Vor dem allerersten Ereignis der Reise (z.B. morgens vor der
    # Abfahrt) soll die Karte den Reisenden am ABFAHRTSORT zeigen, nicht "ohne
    # Position" – er ist ja real dort, auch wenn er noch nicht losgefahren
    # ist. Nutzt bewusst dieselbe "letzter Kandidat <= jetzt gewinnt"-Logik
    # wie alle anderen Kandidaten: sobald ein echtes Ereignis eintritt (Zug
    # fährt ab), übernimmt das automatisch, ohne Sonderfall-Code.
    if segmente:
        erstes_segment = min(segmente, key=lambda s: s["dt_ab"])
        if erstes_segment["von_koord"]:
            dt_start = erstes_segment["dt_ab"].replace(hour=0, minute=0, second=0, microsecond=0)
            if dt_start <= jetzt:
                start_land = (IATA_TO_LAND.get(erstes_segment["von_iata"])
                              if erstes_segment["von_iata"] else None) or "DE"
                start_ort = erstes_segment["von_ort"] or erstes_segment["von_iata"] or "Start"
                kandidaten.append((dt_start, erstes_segment["von_koord"], start_land, start_ort,
                                    "Start", start_ort, "00:00"))

    if kandidaten:
        kandidaten.sort(key=lambda x: x[0])
        dt, koord, land, ort_name, letzter_typ, letztes_detail, letzte_zeit_lokal = kandidaten[-1]

        # "Zuletzt": direkt aus dem gewonnenen Kandidaten (deckt jetzt auch den
        # Fall ab, dass der aktuelle Ort per Hotel-Check-in erreicht wurde,
        # nicht nur per Flug/Bahn – das fehlte vorher komplett).
        herkunft = {"typ": letzter_typ, "detail": letztes_detail, "datum": dt, "zeit_lokal": letzte_zeit_lokal}

        # "Als nächstes": frühestes künftiges Ereignis – Flug-/Bahnabreise ODER
        # ein noch bevorstehender Hotel-Check-in, je nachdem was zuerst kommt.
        kommende_kandidaten = []
        for s in segmente:
            if s["dt_ab"] > jetzt and s["von_koord"] and s["nach_koord"]:
                detail = f'{s["von_iata"] or s["von_ort"] or "?"}-{s["nach_iata"] or s["nach_ort"] or "?"}'
                kommende_kandidaten.append((s["dt_ab"], s["typ"], detail, s["dt_ab_anzeige"]))
        cur2 = db.cursor()
        cur2.execute(f"""SELECT hotel_checkin_datum, hotel_checkin_zeit, hotel_name, hotel_adresse,
            hotel_checkout_datum, hotel_checkout_zeit
            FROM belege WHERE reise_code={P} AND transportart='Hotel'""", (reise_code,))
        for row in cur2.fetchall():
            g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
            d = _datum_parsen(g("hotel_checkin_datum",0))
            ci_zeit = g("hotel_checkin_zeit",1) or "14:00"
            hotel_name = g("hotel_name",2)
            hotel_adresse = g("hotel_adresse",3)
            d_checkout = _datum_parsen(g("hotel_checkout_datum",4))
            co_zeit = g("hotel_checkout_zeit",5) or "11:00"
            if not d: continue
            dt_hotel = segment_zeit_zu_utc(d, ci_zeit, None)
            if dt_hotel is None: continue
            # WICHTIG – dieselbe Korrektur wie bei "Last" (herkunft): die
            # Check-in-Zeit ist oft nur ein genereller Standardwert. Gibt es
            # an diesem Reisetag noch eine SPÄTERE Flug-/Bahn-Ankunft (z.B.
            # erst Zwischenstopp, dann Weiterflug zum Hotel-Ort), kann der
            # Reisende unmöglich vorher schon einchecken – sonst würde "Next:"
            # fälschlich das Hotel VOR dem eigentlich nächsten Flug zeigen.
            dt_checkout = segment_zeit_zu_utc(d_checkout, co_zeit, None) if d_checkout else None
            fenster_ende = dt_checkout or (dt_hotel + timedelta(hours=24))
            spaeteste_ankunft = max(
                (s["dt_an"] for s in segmente if dt_hotel < s["dt_an"] <= fenster_ende),
                default=None)
            if spaeteste_ankunft:
                dt_hotel = spaeteste_ankunft
            if dt_hotel > jetzt:
                kommende_kandidaten.append((dt_hotel, "Hotel", hotel_adresse or hotel_name or "–",
                                             f"{d.strftime('%d.%m.')} {ci_zeit}"))
        cur2.close()

        cur3 = db.cursor()
        cur3.execute(f"""SELECT datum, uhrzeit_von, titel, typ
            FROM termine WHERE reise_code={P} AND lat IS NOT NULL""", (reise_code,))
        for row in cur3.fetchall():
            g = lambda k,i: row[k] if hasattr(row,'keys') else row[i]
            d = _datum_parsen(g("datum",0))
            t_zeit = g("uhrzeit_von",1) or "09:00"
            titel_t = g("titel",2) or "Termin"
            if not d: continue
            dt_t = segment_zeit_zu_utc(d, t_zeit, None)
            if dt_t and dt_t > jetzt:
                kommende_kandidaten.append((dt_t, "Termin", titel_t, f"{d.strftime('%d.%m.')} {t_zeit}"))
        cur3.close()

        naechste_etappe = None
        if kommende_kandidaten:
            kommende_kandidaten.sort(key=lambda x: x[0])
            naechste_etappe = {"typ": kommende_kandidaten[0][1], "detail": kommende_kandidaten[0][2],
                                "datum": kommende_kandidaten[0][0], "zeit_lokal": kommende_kandidaten[0][3]}

        if koord:
            return _ret({"status": "am_ort", "lat": koord[0], "lon": koord[1],
                    "land": land, "ort_name": ort_name,
                    "herkunft": herkunft, "naechste_etappe": naechste_etappe})
        land_koord = koordinaten_fuer_land(land)
        if land_koord:
            return _ret({"status": "am_ort", "lat": land_koord[0], "lon": land_koord[1],
                    "land": land, "ort_name": ort_name or land,
                    "herkunft": herkunft, "naechste_etappe": naechste_etappe})

    # 3. Rückfall: heutiger VMA-Tag (steuerliche Zuordnung, nicht immer = aktueller Ort)
    # WICHTIG: Nur nutzen, wenn schon MINDESTENS EIN echtes Ereignis (Flug/
    # Bahn/Hotel/Termin) der Reise tatsächlich stattgefunden hat (kandidaten
    # nicht leer). Ist "kandidaten" komplett leer, ist die Reise aus Sicht der
    # Karte noch gar nicht "losgegangen" (z.B. der Reisetag hat begonnen, aber
    # der erste Zug/Flug ist noch gar nicht abgefahren) – dann das steuerliche
    # Zielland des Tages als "aktuellen Ort" anzuzeigen wäre falsch: der
    # Reisende ist ja noch am Ausgangsort, nicht im Zielland. Ohne echtes
    # Ereignis lieber gar keine Position zeigen, als eine falsche zu raten.
    if not kandidaten:
        return _ret(None)

    heute_s = date.today().isoformat()
    cur = db.cursor()
    cur.execute(f"SELECT land_code, land_name FROM vma_tage WHERE reise_code={P} AND datum={P}",
                (reise_code, heute_s))
    row = cur.fetchone()
    cur.close()
    if row:
        land = row[0] if isinstance(row, tuple) else row["land_code"]
        lname = row[1] if isinstance(row, tuple) else row["land_name"]
        land_koord = koordinaten_fuer_land(land)
        if land_koord:
            return _ret({"status": "am_ort", "lat": land_koord[0], "lon": land_koord[1],
                    "land": land, "ort_name": lname})
    return _ret(None)


@app.get("/maps", response_class=HTMLResponse)
def dashboard_maps(debug: str = ""):
    """Zeigt auf einer zoombaren Weltkarte, wo sich Reisende bei aktiven Reisen
    gerade tatsächlich aufhalten (Land-Ebene, aus dem letzten erfolgten Flug/Hotel –
    NICHT aus der steuerlichen VMA-Zuordnung, die am Abreisetag absichtlich anders ist).
    Mit ?debug=1 wird zusätzlich eine Diagnose-Karte pro Segment angezeigt."""
    debug_an = debug in ("1", "true", "ja")
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        today = date.today()

        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                STRING_AGG(DISTINCT m.klarname, ', ' ORDER BY m.klarname) as ma,
                STRING_AGG(DISTINCT m.kuerzel, ', ' ORDER BY m.kuerzel) as kuerzel
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                LEFT JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel
                WHERE r.abreise <= %s AND r.rueckkehr >= %s
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr""", (today.isoformat(), today.isoformat()))
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                GROUP_CONCAT(DISTINCT m.klarname) as ma,
                GROUP_CONCAT(DISTINCT m.kuerzel) as kuerzel
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                LEFT JOIN mitarbeiter m ON m.kuerzel = rm.kuerzel
                WHERE r.abreise <= ? AND r.rueckkehr >= ?
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr""", (today.isoformat(), today.isoformat()))
        aktive_reisen = cur.fetchall()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        marker = []
        strecken = []
        ohne_position = []
        alle_diagnosen = []
        for r in aktive_reisen:
            code = get(r,"code",0); titel = get(r,"titel",1); ma = get(r,"ma",4) or "–"
            kuerzel = get(r,"kuerzel",5) or "?"
            if debug_an:
                pos, seg_diag = aktuelle_position_ermitteln(code, db, debug=True)
                alle_diagnosen.append({"code": code, "titel": titel,
                                        "jetzt_lokal": None, "segmente": seg_diag,
                                        "ergebnis": pos})
            else:
                pos = aktuelle_position_ermitteln(code, db)
            if not pos:
                ohne_position.append({"code": code, "titel": titel, "ma": ma})
            elif pos["status"] == "unterwegs":
                from zoneinfo import ZoneInfo
                BERLIN = ZoneInfo("Europe/Berlin")
                TYP_ICON = {"Flug": "✈", "Bahn": "🚆", "Hotel": "🏨", "Termin": "🤝"}
                icon = TYP_ICON.get(pos["transport_typ"], "✈")

                def zeit_txt(zeit_lokal, dt):
                    zeit_lokal = zeit_lokal or "?"
                    zeit_de = dt.astimezone(BERLIN).strftime("%d.%m. %H:%M") if dt else "?"
                    return (f"{zeit_lokal} Ortszeit / {zeit_de} DE-Zeit"
                            if zeit_lokal != zeit_de else f"{zeit_lokal} Uhr")

                next_txt = ""
                naechste = pos.get("naechste_etappe")
                if naechste:
                    n_icon = TYP_ICON.get(naechste["typ"], "📍")
                    n_zeit = zeit_txt(naechste.get("zeit_lokal"), naechste.get("datum"))
                    next_txt = f'Next: {n_icon} {naechste["typ"]} · {naechste["detail"]} · {n_zeit}'

                strecken.append({
                    "von": pos["von_koord"], "nach": pos["nach_koord"],
                    "von_iata": pos["von_iata"], "nach_iata": pos["nach_iata"],
                    "von_name": pos["von_name"], "nach_name": pos["nach_name"],
                    "fortschritt": pos["fortschritt"], "icon": icon, "kuerzel": kuerzel,
                    "code": code, "titel": titel, "ma": ma, "label": pos["label"],
                    "ab_zeit": zeit_txt(pos.get("dt_ab_anzeige"), pos.get("dt_ab")),
                    "an_zeit": zeit_txt(pos.get("dt_an_anzeige"), pos.get("dt_an")),
                    "next_txt": next_txt,
                })
            else:
                from zoneinfo import ZoneInfo
                BERLIN = ZoneInfo("Europe/Berlin")
                TYP_ICON = {"Flug": "✈", "Bahn": "🚆", "Hotel": "🏨", "Termin": "🤝"}

                def format_zeile(praefix, info):
                    icon = TYP_ICON.get(info["typ"], "📍")
                    zeit_lokal = info.get("zeit_lokal") or "?"
                    zeit_de = info["datum"].astimezone(BERLIN).strftime("%d.%m. %H:%M") if info.get("datum") else "?"
                    zeit_txt = (f"{zeit_lokal} Ortszeit / {zeit_de} DE-Zeit"
                                if zeit_lokal != zeit_de else f"{zeit_lokal} Uhr")
                    return f'{praefix}: {icon} {info["typ"]} · {info["detail"]} · {zeit_txt}'

                popup_zusatz = []
                herkunft = pos.get("herkunft")
                if herkunft:
                    popup_zusatz.append(format_zeile("Last", herkunft))
                naechste = pos.get("naechste_etappe")
                if naechste:
                    popup_zusatz.append(format_zeile("Next", naechste))
                marker.append({
                    "lat": pos["lat"], "lon": pos["lon"], "code": code, "titel": titel,
                    "ma": ma, "land": pos.get("ort_name") or pos.get("land"), "kuerzel": kuerzel,
                    "zusatz": popup_zusatz,
                })
        cur.close(); db.close()

        from mod_flugalert import jetzt_lokal as _jetzt_lokal_fuer_debug
        jetzt_anzeige = _jetzt_lokal_fuer_debug().isoformat()

        debug_html = ""
        if debug_an:
            karten = ""
            for d in alle_diagnosen:
                seg_zeilen = ""
                for s in d["segmente"]:
                    farbe = "#059669" if s["status"] == "übernommen" else "#dc2626"
                    seg_zeilen += f"""<div style="padding:6px 0;border-bottom:1px solid var(--border);font-size:11px">
                        <b>Beleg #{s['beleg_id']} · Segment {s['segment_index']}</b>: {s.get('von_ort')} → {s.get('nach_ort')}<br>
                        Abreise roh: {s.get('abreise_datum_roh')!r} {s.get('abreise_zeit_roh')!r} ·
                        Ankunft roh: {s.get('ankunft_datum_roh')!r} {s.get('ankunft_zeit_roh')!r}<br>
                        {'Geparst: ' + s.get('dt_ab','?') + ' → ' + s.get('dt_an','?') + '<br>' if 'dt_ab' in s else ''}
                        Koordinaten: von=({s.get('von_lat')},{s.get('von_lon')}) nach=({s.get('nach_lat')},{s.get('nach_lon')})<br>
                        <span style="color:{farbe};font-weight:600">→ {s['status']}</span>
                    </div>"""
                ergebnis_txt = json.dumps(d["ergebnis"], ensure_ascii=False, default=str, indent=2) if d["ergebnis"] else "None"
                karten += f"""<div class="card" style="margin-bottom:12px">
                  <div class="card-header"><span class="card-title">{d['code']} – {d['titel']}</span></div>
                  <div class="card-body">
                    {seg_zeilen or '<div style="font-size:12px;color:var(--muted)">Keine Flug-/Bahnsegmente gefunden</div>'}
                    <pre style="font-size:11px;background:var(--bg);padding:8px;border-radius:6px;margin-top:8px;white-space:pre-wrap">Endergebnis: {ergebnis_txt}</pre>
                  </div>
                </div>"""
            debug_html = f"""<div class="alert alert-warn" style="margin-bottom:12px">
              🔍 Debug-Modus aktiv – aktuelle Zeit lokal: {jetzt_anzeige}</div>{karten}"""

        marker_js = json.dumps(marker, ensure_ascii=False)
        strecken_js = json.dumps(strecken, ensure_ascii=False)
        ohne_html = "".join(
            f'<li><a href="/reise/{o["code"]}">{o["code"]} – {o["titel"]}</a> ({o["ma"]}) – '
            f'noch kein Ort ermittelbar (Belege/VMA prüfen)</li>' for o in ohne_position)

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
          <h1 class="page-title" style="margin:0">🗺 Reisende auf der Karte</h1>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>
        <p style="font-size:13px;color:var(--muted);margin-bottom:12px">
          Zeigt, wo sich zugeordnete Mitarbeiter bei aktuell laufenden Reisen befinden –
          am Boden (letzter erreichter Ort) oder unterwegs (gestrichelte Linie zwischen
          Start- und Zielflughafen/-bahnhof, Position anteilig zur verstrichenen Reisezeit
          geschätzt). Kein Live-GPS, sondern aus Belegdaten abgeleitet.
          {'' if debug_an else '<a href="/maps?debug=1" style="color:var(--blue)">🔍 Diagnose anzeigen</a>'}</p>
        {debug_html}
        <div id="reise-map" style="height:70vh;border-radius:var(--radius,10px);
                                    border:1px solid var(--border);overflow:hidden"></div>
        {'<div class="alert alert-warn" style="margin-top:12px"><b>Ohne Standort:</b><ul style="margin:6px 0 0 18px">' + ohne_html + '</ul></div>' if ohne_html else ''}

        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
        const marker = {marker_js};
        const strecken = {strecken_js};
        const map = L.map('reise-map').setView([20, 10], 2);
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; OpenStreetMap-Mitwirkende',
            maxZoom: 18
        }}).addTo(map);
        function personIcon(kuerzel) {{
            return L.divIcon({{
                html: '<div style="position:relative;width:36px;height:46px">' +
                      // Klassische Pin-Form: Kreis mit spitzem unteren Ende
                      // (per CSS-Trick: eine Ecke des Kreises bleibt eckig,
                      // dann um 45° gedreht -> zeigt nach unten auf den Ort)
                      '<div style="position:absolute;top:0;left:1px;width:34px;height:34px;' +
                      'background:linear-gradient(135deg,#3b82f6,#1e40af);' +
                      'border-radius:50% 50% 50% 0;transform:rotate(-45deg);' +
                      'border:2.5px solid #fff;box-shadow:0 3px 8px rgba(0,0,0,.4)">' +
                      '<div style="transform:rotate(45deg);width:100%;height:100%;' +
                      'display:flex;align-items:center;justify-content:center">' +
                      '<span style="color:#fff;font-weight:800;font-size:12px;letter-spacing:.3px;' +
                      'text-shadow:0 1px 2px rgba(0,0,0,.35);font-family:system-ui,sans-serif">' +
                      kuerzel + '</span></div></div>' +
                      // Kleiner heller Punkt in der Spitze, wirkt wie eine
                      // Glanz-/Fokus-Markierung auf der eigentlichen Position
                      '<div style="position:absolute;bottom:2px;left:50%;transform:translateX(-50%);' +
                      'width:6px;height:6px;background:#fff;border-radius:50%;' +
                      'box-shadow:0 0 0 2px rgba(59,130,246,.5)"></div>' +
                      '</div>',
                className: '', iconSize: [36,46], iconAnchor: [18,44]
            }});
        }}
        const flughafenIcon = L.divIcon({{
            html: '<div style="background:white;border:2px solid #64748b;border-radius:50%;width:9px;height:9px"></div>',
            className: '', iconSize: [9,9], iconAnchor: [4,4]
        }});
        const bounds = [];

        // Großkreis-Interpolation (Slerp auf der Kugel) – die Erde ist keine
        // Scheibe, daher folgt die kürzeste Flugstrecke zwischen zwei Punkten
        // einer gekrümmten Linie, keiner Geraden auf der flachen Karte.
        function greatCirclePoint(lat1, lon1, lat2, lon2, f) {{
            const toRad = d => d * Math.PI / 180, toDeg = r => r * 180 / Math.PI;
            const p1 = toRad(lat1), l1 = toRad(lon1), p2 = toRad(lat2), l2 = toRad(lon2);
            const delta = Math.acos(Math.min(1, Math.max(-1,
                Math.sin(p1)*Math.sin(p2) + Math.cos(p1)*Math.cos(p2)*Math.cos(l2-l1))));
            if (delta < 1e-9) return [lat1, lon1];
            const A = Math.sin((1-f)*delta) / Math.sin(delta);
            const B = Math.sin(f*delta) / Math.sin(delta);
            const x = A*Math.cos(p1)*Math.cos(l1) + B*Math.cos(p2)*Math.cos(l2);
            const y = A*Math.cos(p1)*Math.sin(l1) + B*Math.cos(p2)*Math.sin(l2);
            const z = A*Math.sin(p1) + B*Math.sin(p2);
            return [toDeg(Math.atan2(z, Math.sqrt(x*x+y*y))), toDeg(Math.atan2(y, x))];
        }}
        function greatCirclePath(von, nach, teile) {{
            teile = teile || 64;
            const pts = [];
            for (let i = 0; i <= teile; i++) {{
                pts.push(greatCirclePoint(von[0], von[1], nach[0], nach[1], i/teile));
            }}
            return pts;
        }}

        // --- Personen-Marker (am Ort UND unterwegs) einheitlich verwalten ---
        // Überlappende Pins werden in BILDSCHIRM-PIXELN auseinandergezogen
        // (nicht in Grad) und bei jeder Zoomstufe neu berechnet. Ein fester
        // Versatz in Grad ist bei weit herausgezoomter Karte unsichtbar klein
        // (Pins verdecken sich dann trotzdem), bei starkem Zoom dagegen
        // unnötig groß. Gilt auch für Kollegen auf demselben Flug/Zug, die
        // an exakt derselben Stelle der Strecke unterwegs sind.
        const personen = [];

        marker.forEach(m => {{
            const mk = L.marker([m.lat, m.lon], {{icon: personIcon(m.kuerzel)}}).addTo(map);
            let popup = '<b>' + m.code + '</b> – ' + m.titel + '<br>👤 ' + m.ma + '<br>📍 Aktuell in: ' + m.land;
            (m.zusatz || []).forEach(z => {{ popup += '<br><span style="font-size:12px;color:#64748b">' + z + '</span>'; }});
            mk.bindPopup(popup);
            personen.push({{lat: m.lat, lon: m.lon, mk: mk}});
            bounds.push([m.lat, m.lon]);
        }});

        strecken.forEach(s => {{
            L.polyline(greatCirclePath(s.von, s.nach), {{
                color: '#2563eb', weight: 2, dashArray: '6, 8', opacity: 0.8
            }}).addTo(map);
            L.marker(s.von, {{icon: flughafenIcon}}).addTo(map)
                .bindPopup('<b>' + s.von_iata + '</b> – ' + s.von_name);
            L.marker(s.nach, {{icon: flughafenIcon}}).addTo(map)
                .bindPopup('<b>' + s.nach_iata + '</b> – ' + s.nach_name);
            // Aktuelle Position anteilig ENTLANG DES GROSSKREISES interpolieren
            // (nicht linear – das würde bei langen Strecken sichtbar von der
            // gezeichneten gekrümmten Linie abweichen)
            const [lat, lon] = greatCirclePoint(s.von[0], s.von[1], s.nach[0], s.nach[1], s.fortschritt);
            const mk = L.marker([lat, lon], {{icon: personIcon(s.kuerzel)}}).addTo(map).bindPopup(
                '<b>' + s.code + '</b> – ' + s.titel + '<br>' +
                '👤 ' + s.ma + '<br>' + s.icon + ' ' + s.label + ' (unterwegs, ' +
                Math.round(s.fortschritt*100) + '%)<br>' +
                '<span style="font-size:12px;color:#64748b">Ab: ' + s.ab_zeit + '<br>An: ' + s.an_zeit +
                (s.next_txt ? '<br>' + s.next_txt : '') + '</span>'
            );
            personen.push({{lat: lat, lon: lon, mk: mk}});
            bounds.push(s.von, s.nach);
        }});

        const versatzLinien = L.layerGroup().addTo(map);
        function personenVerteilen() {{
            versatzLinien.clearLayers();
            const SCHWELLE = 40;  // Pixel: näher als das gilt als "überlappend"
            const gruppen = [];
            personen.forEach(p => {{
                p.pt = map.latLngToContainerPoint([p.lat, p.lon]);
                const g = gruppen.find(grp => grp.some(q => Math.hypot(q.pt.x - p.pt.x, q.pt.y - p.pt.y) < SCHWELLE));
                if (g) g.push(p); else gruppen.push([p]);
            }});
            gruppen.forEach(g => {{
                if (g.length === 1) {{ g[0].mk.setLatLng([g[0].lat, g[0].lon]); return; }}
                const cx = g.reduce((a, p) => a + p.pt.x, 0) / g.length;
                const cy = g.reduce((a, p) => a + p.pt.y, 0) / g.length;
                // Radius so wählen, dass benachbarte Pins (36px breit) sich
                // nicht berühren – wächst automatisch mit der Gruppengröße.
                const R = Math.max(24, 22 / Math.sin(Math.PI / g.length));
                g.forEach((p, i) => {{
                    const w = (2 * Math.PI * i) / g.length;
                    const ll = map.containerPointToLatLng([cx + R * Math.cos(w), cy + R * Math.sin(w)]);
                    p.mk.setLatLng(ll);
                    L.polyline([[p.lat, p.lon], [ll.lat, ll.lng]], {{
                        color: '#94a3b8', weight: 1, dashArray: '2, 3', opacity: 0.7
                    }}).addTo(versatzLinien);
                }});
            }});
        }}
        map.on('zoomend', personenVerteilen);

        if (bounds.length > 0) {{ map.fitBounds(bounds, {{padding: [40,40], maxZoom: 6}}); }}
        if (personen.length > 0) {{ personenVerteilen(); }}
        </script>
        """
        return HTMLResponse(shell("Karte – Reisende", content, "start"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


# ── Mitarbeiter ────────────────────────────────────────────────────────────────
@app.get("/mitarbeiter", response_class=HTMLResponse)
def mitarbeiter_liste():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT m.kuerzel, m.klarname, m.email, m.email2, m.email3, m.rolle, m.aktiv,
                       COUNT(rm.reise_code) as reise_count, m.ist_reisender, m.ist_organisator, m.kalender_mail
                       FROM mitarbeiter m
                       LEFT JOIN reise_mitarbeiter rm ON rm.kuerzel = m.kuerzel
                       GROUP BY m.kuerzel, m.klarname, m.email, m.email2, m.email3, m.rolle, m.aktiv,
                                m.ist_reisender, m.ist_organisator, m.kalender_mail
                       ORDER BY m.klarname""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r, key, idx):
            return r[key] if hasattr(r, 'keys') else r[idx]

        zeilen = ""
        for r in rows:
            kuerzel = get(r,"kuerzel",0)
            klarname = get(r,"klarname",1)
            email_liste = [get(r,"email",2), get(r,"email2",3), get(r,"email3",4)]
            email   = "<br>".join(e for e in email_liste if e) or "–"
            aktiv   = get(r,"aktiv",6)
            rcnt    = get(r,"reise_count",7)
            is_reisend = bool(get(r,"ist_reisender",8))
            is_org = bool(get(r,"ist_organisator",9))
            aktiv_badge = ('<span class="badge badge-green">Aktiv</span>' if aktiv
                           else '<span class="badge badge-gray">Inaktiv</span>')
            rolle_badges = []
            if is_reisend: rolle_badges.append('<span class="badge badge-blue">✈ Reisender</span>')
            if is_org: rolle_badges.append('<span class="badge badge-purple">📋 Organisator</span>')
            if get(r,"kalender_mail",10): rolle_badges.append('<span class="badge badge-gray">📅 Outlook</span>')
            rolle_badge = " ".join(rolle_badges) or '<span class="badge badge-gray">–</span>'
            zeilen += f"""<tr>
                <td class="td-mono" style="font-weight:700">{kuerzel}</td>
                <td style="font-weight:500">{klarname}</td>
                <td style="font-size:12px;color:var(--muted)">{email}</td>
                <td>{rolle_badge}</td>
                <td>{aktiv_badge}</td>
                <td style="color:var(--muted)">{rcnt}</td>
                <td>
                  <a href="/mitarbeiter/{kuerzel}/bearbeiten"
                     class="btn btn-secondary btn-sm">✏ Bearbeiten</a>
                </td>
            </tr>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Mitarbeiter</h1>
          <a href="/mitarbeiter/neu" class="btn btn-primary">+ Neu anlegen</a>
        </div>
        <div class="card">
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Kürzel</th><th>Name</th><th>E-Mail</th><th>Rolle</th>
                <th>Status</th><th>Reisen</th><th></th>
              </tr></thead>
              <tbody>
                {zeilen or '<tr><td colspan="5"><div class="empty-state">Noch keine Mitarbeiter – <a href="/mitarbeiter/neu">Jetzt anlegen</a></div></td></tr>'}
              </tbody>
            </table>
          </div>
        </div>"""
        return HTMLResponse(shell("Mitarbeiter", content, "mitarbeiter"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/mitarbeiter/neu", response_class=HTMLResponse)
def mitarbeiter_neu_form():
    content = """
    <h1 class="page-title">Mitarbeiter anlegen</h1>
    <div class="card" style="max-width:480px">
      <div class="card-body">
        <form method="post" action="/mitarbeiter/neu">
          <div class="form-grid form-grid-2">
            <div class="form-group">
              <label>Kürzel <span class="required">*</span></label>
              <input type="text" name="kuerzel" maxlength="5" required
                     placeholder="z.B. RD" style="text-transform:uppercase"
                     autofocus>
              <div class="form-hint">2–5 Buchstaben, eindeutig</div>
            </div>
            <div class="form-group">
              <label>Klarname <span class="required">*</span></label>
              <input type="text" name="klarname" required
                     placeholder="z.B. Ralf Diesslin">
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 1</label>
              <input type="email" name="email"
                     placeholder="rdiesslin@herrhammer.de">
              <div class="form-hint">Für automatische Beleg-Erkennung</div>
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 2</label>
              <input type="email" name="email2" placeholder="optional">
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 3</label>
              <input type="email" name="email3" placeholder="optional">
            </div>
            <div class="form-group full">
              <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                <input type="checkbox" name="kalender_mail" value="1" style="width:auto;margin:0">
                📅 Reisetermine als Outlook-Einladung senden
              </label>
              <div class="form-hint">Für jeden Flug, Zug und manuell angelegten Termin geht eine Kalender-Einladung an E-Mail-Adresse 1 (Zeiten, Nummer, Sitz, Terminal/Gleis). Änderungen und Stornierungen werden automatisch nachgeführt.</div>
            </div>
            <div class="form-group full">
              <label>Rolle <span class="required">*</span></label>
              <div style="display:flex;gap:16px;margin-top:4px">
                <label style="display:inline-flex;align-items:center;gap:6px;font-weight:400">
                  <input type="checkbox" name="ist_reisender" value="1" checked style="width:auto"> ✈ Reisender
                </label>
                <label style="display:inline-flex;align-items:center;gap:6px;font-weight:400">
                  <input type="checkbox" name="ist_organisator" value="1" style="width:auto"> 📋 Organisator
                </label>
              </div>
              <div class="form-hint">Reisender = fährt selbst · Organisator = bucht für andere und darf Beleg-Daten nachtragen. Beides gleichzeitig möglich.</div>
            </div>
            <div class="form-group full">
              <label>Kreditkartentyp</label>
              <select name="kreditkarten_typ">
                <option value="privat" selected>💳 Privatkreditkarte</option>
                <option value="firma">🏢 Firmenkreditkarte</option>
              </select>
              <div class="form-hint">Standard: Privatkreditkarte (die meisten zahlen mit eigener Karte). Wird bei "Kreditkarte" als Bezahlart automatisch als "Privatkreditkarte-{Kürzel}" bzw. "Firmenkreditkarte-{Kürzel}" hinterlegt.</div>
            </div>
            <div class="form-group full">
              <label>Auslandseinsatzentgelt der Karte (%)</label>
              <input type="number" step="0.01" name="kreditkarten_auslandsentgelt_prozent" class="inp" value="1.5">
              <div class="form-hint">Gebühr, die die Bank bei Fremdwährungs-Zahlungen berechnet (steht auf der Kartenabrechnung, oft 1-2%). Wird bei Belegen in Fremdwährung automatisch berechnet und zum Gesamtbetrag addiert. 0 eintragen, falls die Karte keine Auslandsgebühr hat.</div>
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Anlegen</button>
            <a href="/mitarbeiter" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell("Mitarbeiter anlegen", content, "mitarbeiter"))

@app.post("/mitarbeiter/neu")
async def mitarbeiter_neu(request: Request):
    form = await request.form()
    kuerzel = (form.get("kuerzel") or "").strip().upper()
    klarname = (form.get("klarname") or "").strip()
    email   = (form.get("email") or "").strip() or None
    email2  = (form.get("email2") or "").strip() or None
    email3  = (form.get("email3") or "").strip() or None
    ist_reisender = bool(form.get("ist_reisender"))
    ist_organisator = bool(form.get("ist_organisator"))
    kalender_mail = bool(form.get("kalender_mail"))
    kreditkarten_typ = (form.get("kreditkarten_typ") or "privat").strip()
    try:
        kreditkarten_auslandsentgelt_prozent = float(form.get("kreditkarten_auslandsentgelt_prozent") or 1.5)
    except ValueError:
        kreditkarten_auslandsentgelt_prozent = 1.5
    if not kuerzel or not klarname:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Kürzel und Name sind Pflichtfelder.</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))
    if not re.match(r'^[A-Z]{1,5}$', kuerzel):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Kürzel: nur Buchstaben, 1–5 Zeichen.</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        rolle_txt = "beides" if (ist_reisender and ist_organisator) else ("organisator" if ist_organisator else "reisender")
        cur.execute(f"""INSERT INTO mitarbeiter
            (kuerzel, klarname, email, email2, email3, rolle, ist_reisender, ist_organisator, kreditkarten_typ,
             kreditkarten_auslandsentgelt_prozent, kalender_mail)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
                    (kuerzel, klarname, email, email2, email3, rolle_txt, ist_reisender, ist_organisator,
                     kreditkarten_typ, kreditkarten_auslandsentgelt_prozent, kalender_mail))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/mitarbeiter", status_code=303)
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower():
            msg = f'Kürzel "{kuerzel}" existiert bereits.'
        else:
            msg = err
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{msg}</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))

@app.get("/mitarbeiter/{kuerzel}/bearbeiten", response_class=HTMLResponse)
def mitarbeiter_bearbeiten_form(kuerzel: str):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT kuerzel, klarname, email, rolle, aktiv, email2, email3, ist_reisender, ist_organisator, kreditkarten_typ, kreditkarten_auslandsentgelt_prozent, kalender_mail FROM mitarbeiter WHERE kuerzel={P}",
                    (kuerzel.upper(),))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Mitarbeiter nicht gefunden.</div>'))
        k = r[0] if isinstance(r, tuple) else r["kuerzel"]
        n = r[1] if isinstance(r, tuple) else r["klarname"]
        em = (r[2] if isinstance(r, tuple) else r.get("email","")) or ""
        a = r[4] if isinstance(r, tuple) else r["aktiv"]
        em2 = (r[5] if isinstance(r, tuple) else r.get("email2","")) or ""
        em3 = (r[6] if isinstance(r, tuple) else r.get("email3","")) or ""
        is_reisend = bool(r[7] if isinstance(r, tuple) else r.get("ist_reisender", True))
        is_org = bool(r[8] if isinstance(r, tuple) else r.get("ist_organisator", False))
        kk_typ = (r[9] if isinstance(r, tuple) else r.get("kreditkarten_typ")) or "privat"
        kk_prozent = r[10] if isinstance(r, tuple) else r.get("kreditkarten_auslandsentgelt_prozent")
        if kk_prozent is None: kk_prozent = 1.5
        kal_mail = bool(r[11] if isinstance(r, tuple) else r["kalender_mail"])
        aktiv_check = "checked" if a else ""
        content = f"""
        <h1 class="page-title">Mitarbeiter bearbeiten</h1>
        <div class="card" style="max-width:480px">
          <div class="card-body">
            <form method="post" action="/mitarbeiter/{k}/bearbeiten">
              <div class="form-grid">
                <div class="form-group">
                  <label>Kürzel</label>
                  <input type="text" value="{k}" disabled
                         style="background:#f8fafc;color:var(--muted)">
                </div>
                <div class="form-group">
                  <label>Klarname <span class="required">*</span></label>
                  <input type="text" name="klarname" value="{n}" required autofocus>
                </div>
                <div class="form-group">
                  <label>E-Mail-Adresse 1</label>
                  <input type="email" name="email" value="{em}"
                         placeholder="rdiesslin@herrhammer.de">
                </div>
                <div class="form-group">
                  <label>E-Mail-Adresse 2</label>
                  <input type="email" name="email2" value="{em2}" placeholder="optional">
                </div>
                <div class="form-group">
                  <label>E-Mail-Adresse 3</label>
                  <input type="email" name="email3" value="{em3}" placeholder="optional">
                </div>
                <div class="form-group full">
                  <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                    <input type="checkbox" name="kalender_mail" value="1" {"checked" if kal_mail else ""}
                           style="width:auto;margin:0">
                    📅 Reisetermine als Outlook-Einladung senden
                  </label>
                  <div class="form-hint">Für jeden Flug, Zug und manuell angelegten Termin geht eine Kalender-Einladung an E-Mail-Adresse 1 (Zeiten, Nummer, Sitz, Terminal/Gleis). Änderungen und Stornierungen werden automatisch nachgeführt.</div>
                </div>
                <div class="form-group full">
                  <label>Rolle</label>
                  <div style="display:flex;gap:16px;margin-top:4px">
                    <label style="display:inline-flex;align-items:center;gap:6px;font-weight:400">
                      <input type="checkbox" name="ist_reisender" value="1" {"checked" if is_reisend else ""} style="width:auto"> ✈ Reisender
                    </label>
                    <label style="display:inline-flex;align-items:center;gap:6px;font-weight:400">
                      <input type="checkbox" name="ist_organisator" value="1" {"checked" if is_org else ""} style="width:auto"> 📋 Organisator
                    </label>
                  </div>
                  <div class="form-hint">Organisator darf KI-Beleg-Daten nachtragen/korrigieren.</div>
                </div>
                <div class="form-group full">
                  <label>Kreditkartentyp</label>
                  <select name="kreditkarten_typ">
                    <option value="privat"{" selected" if kk_typ=="privat" else ""}>💳 Privatkreditkarte</option>
                    <option value="firma"{" selected" if kk_typ=="firma" else ""}>🏢 Firmenkreditkarte</option>
                  </select>
                  <div class="form-hint">Wird bei Belegen mit Bezahlart "Kreditkarte" automatisch als "Privatkreditkarte-{k}" bzw. "Firmenkreditkarte-{k}" angeboten.</div>
                </div>
                <div class="form-group full">
                  <label>Auslandseinsatzentgelt der Karte (%)</label>
                  <input type="number" step="0.01" name="kreditkarten_auslandsentgelt_prozent" class="inp" value="{kk_prozent}">
                  <div class="form-hint">Gebühr, die die Bank bei Fremdwährungs-Zahlungen berechnet (steht auf der Kartenabrechnung, oft 1-2%). Wird bei Belegen in Fremdwährung automatisch berechnet und zum Gesamtbetrag addiert. 0 eintragen, falls die Karte keine Auslandsgebühr hat.</div>
                </div>
                <div class="form-group full">
                  <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                    <input type="checkbox" name="aktiv" value="1" {aktiv_check}
                           style="width:auto;margin:0">
                    Mitarbeiter aktiv
                  </label>
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/mitarbeiter" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="max-width:480px;margin-top:16px">
          <div class="card-header"><span class="card-title">✏ Kürzel ändern</span></div>
          <div class="card-body">
            <p style="font-size:12px;color:var(--muted);margin-bottom:12px">
              Ändert das Kürzel überall (Reisen, Portal-Zugänge, Login) – die
              bisherige Historie bleibt erhalten. Aktive Portal-Links mit dem
              alten Kürzel werden ungültig, neue müssten neu verschickt werden.</p>
            <form method="post" action="/mitarbeiter/{k}/kuerzel-aendern"
                  onsubmit="return confirm('Kürzel {k} wirklich ändern? Dies kann nicht rückgängig gemacht werden.')">
              <div class="form-group">
                <label>Neues Kürzel</label>
                <input type="text" id="neues-kuerzel" name="neues_kuerzel" required
                       maxlength="5" style="text-transform:uppercase" placeholder="z.B. RDI">
              </div>
              <button type="submit" class="btn btn-secondary" style="width:100%">Kürzel ändern</button>
            </form>
          </div>
        </div>

        <div class="card" style="max-width:480px;margin-top:16px">
          <div class="card-header"><span class="card-title">🔒 Login-Passwort</span></div>
          <div class="card-body">
            <p style="font-size:12px;color:var(--muted);margin-bottom:12px">
              Setzt ein neues Passwort für den Login dieses Mitarbeiters (überschreibt ein
              eventuell vorhandenes Passwort).</p>
            <form method="post" action="/mitarbeiter/{k}/passwort">
              <div class="form-group">
                <label>Neues Passwort</label>
                <input type="password" name="passwort" required minlength="8">
              </div>
              <div class="form-group">
                <label>Wiederholen</label>
                <input type="password" name="passwort2" required minlength="8">
              </div>
              <button type="submit" class="btn btn-primary" style="width:100%">Passwort setzen</button>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"MA {k} bearbeiten", content, "mitarbeiter"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/mitarbeiter/{kuerzel}/kuerzel-aendern")
async def mitarbeiter_kuerzel_aendern(kuerzel: str, request: Request):
    """
    Benennt ein Mitarbeiter-Kürzel sicher um: legt einen neuen Mitarbeiter-
    Datensatz mit allen bisherigen Daten an, verschiebt alle Verweise
    (Reisen, Portal-Zugänge, Reisetage) auf das neue Kürzel und löscht den
    alten Datensatz. Läuft in einer Transaktion – entweder ganz oder gar nicht.
    """
    alt = kuerzel.strip().upper()
    form = await request.form()
    neu = (form.get("neues_kuerzel") or "").strip().upper()

    if not re.match(r'^[A-Z]{1,5}$', neu):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Neues Kürzel: nur Buchstaben, 1–5 Zeichen.</div>'
            f'<a href="/mitarbeiter/{alt}/bearbeiten" class="btn btn-secondary">Zurück</a>'))
    if neu == alt:
        return RedirectResponse(f"/mitarbeiter/{alt}/bearbeiten", status_code=303)

    try:
        P = ph()
        db = get_db(); cur = db.cursor()

        cur.execute(f"SELECT kuerzel FROM mitarbeiter WHERE kuerzel={P}", (neu,))
        if cur.fetchone():
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler",
                f'<div class="alert alert-err">Kürzel {neu} ist bereits vergeben.</div>'
                f'<a href="/mitarbeiter/{alt}/bearbeiten" class="btn btn-secondary">Zurück</a>'))

        cur.execute(f"""SELECT klarname, email, email2, email3, rolle, aktiv, passwort_hash,
                               ist_reisender, ist_organisator, kreditkarten_typ,
                               kreditkarten_auslandsentgelt_prozent, kalender_mail
                        FROM mitarbeiter WHERE kuerzel={P}""", (alt,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Mitarbeiter nicht gefunden.</div>'))
        g = lambda k,i: r[k] if hasattr(r,'keys') else r[i]

        cur.execute(f"""INSERT INTO mitarbeiter
            (kuerzel, klarname, email, email2, email3, rolle, aktiv, passwort_hash,
             ist_reisender, ist_organisator, kreditkarten_typ, kreditkarten_auslandsentgelt_prozent, kalender_mail)
            VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
            (neu, g("klarname",0), g("email",1), g("email2",2), g("email3",3),
             g("rolle",4), g("aktiv",5), g("passwort_hash",6), g("ist_reisender",7),
             g("ist_organisator",8), g("kreditkarten_typ",9), g("kreditkarten_auslandsentgelt_prozent",10),
             g("kalender_mail",11)))

        cur.execute(f"UPDATE reise_mitarbeiter SET kuerzel={P} WHERE kuerzel={P}", (neu, alt))
        cur.execute(f"UPDATE reise_zugang SET kuerzel={P} WHERE kuerzel={P}", (neu, alt))
        cur.execute(f"UPDATE reisetage_person SET kuerzel={P} WHERE kuerzel={P}", (neu, alt))
        cur.execute(f"UPDATE kalender_versand SET kuerzel={P} WHERE kuerzel={P}", (neu, alt))
        cur.execute(f"DELETE FROM mitarbeiter WHERE kuerzel={P}", (alt,))

        db.commit(); cur.close(); db.close()

        # Falls der aktuell eingeloggte Nutzer selbst umbenannt wurde, Session nachziehen
        if request.session.get("kuerzel") == alt:
            request.session["kuerzel"] = neu

        return RedirectResponse(f"/mitarbeiter/{neu}/bearbeiten", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/mitarbeiter/{kuerzel}/passwort")
async def mitarbeiter_passwort_setzen(kuerzel: str, request: Request):
    form = await request.form()
    pw1 = form.get("passwort") or ""; pw2 = form.get("passwort2") or ""
    if len(pw1) < 8 or pw1 != pw2:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Passwörter stimmen nicht überein oder sind zu kurz (min. 8 Zeichen).</div>'
            f'<a href="/mitarbeiter/{kuerzel}/bearbeiten" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE mitarbeiter SET passwort_hash={P} WHERE kuerzel={P}",
                    (passwort_hashen(pw1), kuerzel.upper()))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/mitarbeiter/{kuerzel}/bearbeiten", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/mitarbeiter/{kuerzel}/bearbeiten")
async def mitarbeiter_bearbeiten(kuerzel: str, request: Request):
    form = await request.form()
    klarname = (form.get("klarname") or "").strip()
    email    = (form.get("email") or "").strip() or None
    email2   = (form.get("email2") or "").strip() or None
    email3   = (form.get("email3") or "").strip() or None
    ist_reisender = bool(form.get("ist_reisender"))
    ist_organisator = bool(form.get("ist_organisator"))
    kalender_mail = bool(form.get("kalender_mail"))
    kreditkarten_typ = (form.get("kreditkarten_typ") or "privat").strip()
    try:
        kreditkarten_auslandsentgelt_prozent = float(form.get("kreditkarten_auslandsentgelt_prozent") or 1.5)
    except ValueError:
        kreditkarten_auslandsentgelt_prozent = 1.5
    aktiv    = bool(form.get("aktiv"))
    if not klarname:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Name darf nicht leer sein.</div>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        aktiv_val = True if is_postgres() else 1
        inaktiv_val = False if is_postgres() else 0
        rolle_txt = "beides" if (ist_reisender and ist_organisator) else ("organisator" if ist_organisator else "reisender")
        cur.execute(f"""UPDATE mitarbeiter SET klarname={P}, email={P}, email2={P}, email3={P},
                        rolle={P}, ist_reisender={P}, ist_organisator={P}, kreditkarten_typ={P},
                        kreditkarten_auslandsentgelt_prozent={P}, kalender_mail={P},
                        aktiv={P} WHERE kuerzel={P}""",
                    (klarname, email, email2, email3, rolle_txt, ist_reisender, ist_organisator,
                     kreditkarten_typ, kreditkarten_auslandsentgelt_prozent, kalender_mail,
                     aktiv_val if aktiv else inaktiv_val, kuerzel.upper()))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/mitarbeiter", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Reisen ─────────────────────────────────────────────────────────────────────

# ── Reisen + Länder + VMA-Tabelle ─────────────────────────────────────────────
@app.get("/reisen", response_class=HTMLResponse)
def reisen_liste():
    try:
        db = get_db(); cur = db.cursor()
        today = date.today()
        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           STRING_AGG(rm.kuerzel, ', ' ORDER BY rm.kuerzel) as ma,
                           COUNT(DISTINCT rl.id) as laender_count
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           LEFT JOIN reise_laender rl ON rl.reise_code = r.code
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise DESC""")
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           GROUP_CONCAT(rm.kuerzel, ', ') as ma,
                           COUNT(DISTINCT rl.id) as laender_count
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           LEFT JOIN reise_laender rl ON rl.reise_code = r.code
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise DESC""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        def status(ab, zu):
            if isinstance(ab, str): ab = date.fromisoformat(ab)
            if isinstance(zu, str): zu = date.fromisoformat(zu)
            if today < ab: return f'<span class="badge badge-blue">Geplant</span>'
            elif today <= zu: return '<span class="badge badge-green">● Aktiv</span>'
            else: return '<span class="badge badge-gray">Abgeschlossen</span>'

        zeilen = ""
        for r in rows:
            code = get(r,"code",0); titel = get(r,"titel",1)
            ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3)
            ma = get(r,"ma",4); lc = get(r,"laender_count",5)
            vma_ok = "✓" if lc and lc > 0 else '<span style="color:var(--amber)">–</span>'
            zeilen += f"""<tr>
                <td class="td-mono" style="font-weight:700">
                  <a href="/reise/{code}" style="color:var(--blue)">{code}</a></td>
                <td style="font-weight:500">
                  <a href="/reise/{code}" style="color:inherit;text-decoration:none">{titel}</a></td>
                <td>{fmt_date(ab)}</td><td>{fmt_date(zu)}</td>
                <td style="color:var(--muted)">{ma or "–"}</td>
                <td style="text-align:center">{vma_ok}</td>
                <td>{status(ab,zu)}</td>
                <td>
                  <a href="/reise/{code}" class="btn btn-secondary btn-sm">Detail</a>
                </td>
            </tr>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Reisen</h1>
          <a href="/reisen/neu" class="btn btn-primary">+ Neue Reise</a>
        </div>
        <div class="card">
          <div class="table-wrap"><table>
            <thead><tr>
              <th>Code</th><th>Titel</th><th>Abreise</th><th>Rückkehr</th>
              <th>Mitarbeiter</th><th>VMA</th><th>Status</th><th></th>
            </tr></thead>
            <tbody>
              {zeilen or '<tr><td colspan="8"><div class="empty-state">Keine Reisen – <a href="/reisen/neu">Erste Reise anlegen</a></div></td></tr>'}
            </tbody>
          </table></div>
        </div>"""
        return HTMLResponse(shell("Reisen", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reisen/naechster-code")
def reisen_naechster_code(abreise: str = ""):
    """JSON: nächster freier Reisecode für das Jahr des übergebenen Abreisedatums."""
    try:
        db = get_db(); cur = db.cursor()
        code = next_reise_code(cur, abreise or None)
        cur.close(); db.close()
        return JSONResponse({"code": code})
    except Exception as e:
        return JSONResponse({"code": None, "fehler": str(e)})

@app.get("/reisen/neu", response_class=HTMLResponse)
def reise_neu_form():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = 1"
                    " ORDER BY klarname")
        ma_rows = cur.fetchall()
        cur.close(); db.close()
    except: ma_rows = []

    def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

    ma_opts = "".join(
        f'<option value="{get(r,"kuerzel",0)}">'
        f'{get(r,"kuerzel",0)} – {get(r,"klarname",1)}</option>'
        for r in ma_rows)

    land_opts = "".join(
        f'<option value="{code}">{name} ({code})</option>'
        for code, name in LAENDER_LISTE)

    # Vorschau-Code
    try:
        db = get_db(); cur = db.cursor()
        code_vorschau = next_reise_code(cur)
        cur.close(); db.close()
    except: code_vorschau = "–"

    content = f"""
    <h1 class="page-title">Neue Reise anlegen</h1>
    <div class="card" style="max-width:800px">
      <div class="card-body">
        <form method="post" action="/reisen/neu">

          <div style="background:var(--blue-l);border:1px solid #bfdbfe;border-radius:var(--radius);
                      padding:12px 16px;margin-bottom:20px;display:flex;align-items:center;gap:12px">
            <span id="code-vorschau" style="font-size:22px;font-family:monospace;font-weight:700;color:var(--blue)">{code_vorschau}</span>
            <span style="font-size:12px;color:#3b82f6">Reisecode (wird automatisch vergeben)</span>
          </div>

          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Titel / Beschreibung <span class="required">*</span></label>
              <input type="text" name="titel" required autofocus
                     placeholder="z.B. ECMA Lyon oder Costa Rica Kundenbesuch">
            </div>
            <div class="form-group">
              <label>Abreise <span class="required">*</span></label>
              <input type="date" name="abreise" required
                     onchange="updateRueckkehr(this.value); updateCodeVorschau(this.value)">
            </div>
            <div class="form-group">
              <label>Rückkehr <span class="required">*</span></label>
              <input type="date" name="rueckkehr" required id="inp-rueckkehr">
            </div>
            <div class="form-group full">
              <label>Mitarbeiter <span class="required">*</span></label>
              <select name="mitarbeiter" multiple required size="4"
                      style="height:auto">
                {ma_opts or '<option disabled>Erst Mitarbeiter anlegen</option>'}
              </select>
              <div class="form-hint">Mehrfachauswahl: Strg+Klick (Windows) oder Cmd+Klick (Mac)</div>
            </div>
            <div class="form-group full">
              <label>Notiz (optional)</label>
              <textarea name="notiz" rows="2"
                        placeholder="z.B. Kundenprojekt, Messe, internes Meeting"></textarea>
            </div>
          </div>

          <hr style="border:none;border-top:1px solid var(--border);margin:24px 0">

          <h2 style="font-size:15px;font-weight:600;margin-bottom:16px">
            🌍 Länder & VMA-Sätze
          </h2>
          <div class="alert alert-warn" style="margin-bottom:16px">
            Die Länder-Timeline wird für die automatische VMA-Berechnung genutzt.
            Trage alle Länder mit den jeweiligen Aufenthalts-Zeiträumen ein.
          </div>

          <div id="laender-container">
            <div class="laender-zeile" style="display:grid;grid-template-columns:1fr 1fr 1fr auto;
                 gap:8px;margin-bottom:8px;align-items:end">
              <div class="form-group" style="margin:0">
                <label>Land</label>
                <select name="land_code[]" onchange="updateVMA(this)">
                  {land_opts}
                </select>
              </div>
              <div class="form-group" style="margin:0">
                <label>Von (Datum)</label>
                <input type="date" name="land_von[]">
              </div>
              <div class="form-group" style="margin:0">
                <label>Bis (Datum)</label>
                <input type="date" name="land_bis[]">
              </div>
              <div style="padding-bottom:1px">
                <button type="button" onclick="removeLand(this)"
                        class="btn btn-secondary btn-sm">✕</button>
              </div>
            </div>
          </div>

          <button type="button" onclick="addLand()" class="btn btn-secondary btn-sm"
                  style="margin-bottom:20px">+ Land hinzufügen</button>

          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Reise anlegen</button>
            <a href="/reisen" class="btn btn-secondary">Abbrechen</a>
          </div>

        </form>
      </div>
    </div>

    <script>
    const VMA = {json.dumps({k: v for k, v in VMA_SAETZE.items()})};
    const LAND_OPTS = `{land_opts}`;

    function updateRueckkehr(v) {{
        if (!v) return;
        const r = document.getElementById('inp-rueckkehr');
        if (r && !r.value) {{
            const d = new Date(v);
            d.setDate(d.getDate() + 3);
            r.value = d.toISOString().split('T')[0];
        }}
    }}

    async function updateCodeVorschau(abreise) {{
        if (!abreise) return;
        try {{
            const res = await fetch('/reisen/naechster-code?abreise=' + abreise);
            const data = await res.json();
            const el = document.getElementById('code-vorschau');
            if (el && data.code) el.textContent = data.code;
        }} catch (e) {{}}
    }}

    function updateVMA(sel) {{
        const code = sel.value;
        const info = VMA[code];
        if (info) {{
            const row = sel.closest('.laender-zeile');
            let hint = row.querySelector('.vma-hint');
            if (!hint) {{
                hint = document.createElement('div');
                hint.className = 'vma-hint';
                hint.style.cssText = 'grid-column:1/-1;font-size:11px;color:#059669;margin-top:-4px;margin-bottom:4px';
                row.after(hint);
            }}
            hint.textContent = info.name + ': ' + info.voll + ' EUR/Tag (voll) · ' + info.halb + ' EUR/Tag (halber Satz)';
        }}
    }}

    function addLand() {{
        const container = document.getElementById('laender-container');
        const div = document.createElement('div');
        div.className = 'laender-zeile';
        div.style.cssText = 'display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;margin-bottom:8px;align-items:end';
        div.innerHTML = `
          <div class="form-group" style="margin:0">
            <label>Land</label>
            <select name="land_code[]" onchange="updateVMA(this)">${{LAND_OPTS}}</select>
          </div>
          <div class="form-group" style="margin:0">
            <label>Von (Datum)</label>
            <input type="date" name="land_von[]">
          </div>
          <div class="form-group" style="margin:0">
            <label>Bis (Datum)</label>
            <input type="date" name="land_bis[]">
          </div>
          <div style="padding-bottom:1px">
            <button type="button" onclick="removeLand(this)" class="btn btn-secondary btn-sm">✕</button>
          </div>`;
        container.appendChild(div);
    }}

    function removeLand(btn) {{
        const row = btn.closest('.laender-zeile');
        const hint = row.nextElementSibling;
        if (hint && hint.classList.contains('vma-hint')) hint.remove();
        row.remove();
    }}

    // Erste Zeile: VMA-Info anzeigen
    document.querySelectorAll('select[name="land_code[]"]').forEach(updateVMA);
    </script>
    """
    return HTMLResponse(shell("Neue Reise", content, "reisen"))

@app.post("/reisen/neu")
async def reise_neu(request: Request):
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    abreise = (form.get("abreise") or "").strip()
    rueckkehr = (form.get("rueckkehr") or "").strip()
    notiz = (form.get("notiz") or "").strip()
    mitarbeiter = form.getlist("mitarbeiter")
    land_codes = form.getlist("land_code[]")
    land_vons = form.getlist("land_von[]")
    land_bis_list = form.getlist("land_bis[]")

    if not all([titel, abreise, rueckkehr, mitarbeiter]):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Titel, Zeitraum und mindestens ein Mitarbeiter sind Pflicht.</div>'
            '<a href="/reisen/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        code = next_reise_code(cur, abreise)

        cur.execute(
            f"INSERT INTO reisen (code,titel,abreise,rueckkehr,notiz) VALUES ({P},{P},{P},{P},{P})",
            (code, titel, abreise, rueckkehr, notiz or None))

        for ma in mitarbeiter:
            cur.execute(f"INSERT INTO reise_mitarbeiter (reise_code,kuerzel) VALUES ({P},{P})",
                        (code, ma))

        # Länder
        for i, lcode in enumerate(land_codes):
            if not lcode: continue
            lvon = land_vons[i] if i < len(land_vons) else ""
            lbis = land_bis_list[i] if i < len(land_bis_list) else ""
            if not lvon or not lbis: continue
            lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)
            vvoll, vhalb = vma_fuer_land(lcode)
            cur.execute(
                f"INSERT INTO reise_laender (reise_code,datum_von,datum_bis,land_code,land_name,vma_voll,vma_halb) "
                f"VALUES ({P},{P},{P},{P},{P},{P},{P})",
                (code, lvon, lbis, lcode, lname, vvoll, vhalb))

        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(code)      # Tage sofort anlegen, damit Termine gleich eingetragen werden können
        return RedirectResponse(f"/reise/{code}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'
            '<a href="/reisen/neu" class="btn btn-secondary">Zurück</a>'))

# ── Reise Detail ───────────────────────────────────────────────────────────────
# ── Überschneidende Flug-/Zug-Segmente (Dubletten über mehrere Belege) ───────
def _seg_kurz(s: dict) -> str:
    nr = s.get("transport_nummer") or s.get("transport_name") or "Segment"
    zeit = f'{s.get("abreise_zeit") or "?"}–{s.get("ankunft_zeit") or "?"}'
    von = s.get("von_ort") or s.get("von_iata") or "?"
    nach = s.get("nach_ort") or s.get("nach_iata") or "?"
    return f'{nr} · {von} → {nach} · {s.get("abreise_datum") or "?"} {zeit}'


def _gleiche_strecke(sa: dict, sb: dict) -> bool:
    def gleich(p):
        ia, ib = (sa.get(f"{p}_iata") or "").strip().upper(), (sb.get(f"{p}_iata") or "").strip().upper()
        if ia and ib:
            return ia == ib
        oa = re.sub(r"[^a-z0-9]", "", str(sa.get(f"{p}_ort") or "").lower())
        ob = re.sub(r"[^a-z0-9]", "", str(sb.get(f"{p}_ort") or "").lower())
        if not oa or not ob:
            return False
        return oa == ob or (len(oa) >= 4 and len(ob) >= 4 and (oa in ob or ob in oa))
    return gleich("von") and gleich("nach")


def _seg_unterschiede(sa: dict, sb: dict) -> list:
    out = []
    for feld, label in _SEG_FELDER:
        va, vb = sa.get(feld), sb.get(feld)
        la, lb = (va is None or str(va).strip() == ""), (vb is None or str(vb).strip() == "")
        if la and lb:
            continue
        if la or lb:
            out.append({"label": label, "a": "" if la else str(va), "b": "" if lb else str(vb), "art": "ergaenzt"})
        elif not _wert_gleich(feld, va, vb):
            out.append({"label": label, "a": str(va), "b": str(vb), "art": "abweichend"})
    return out


def _segment_ueberschneidungen(reise_code: str) -> list:
    """
    Findet Flug-/Bahn-Segmente VERSCHIEDENER Belege einer Reise, die offenbar
    dieselbe Fahrt beschreiben oder zeitlich übereinanderliegen – typisch, wenn
    eine geänderte Buchung als zweiter Beleg ankommt und die Erkennung auf
    Beleg-Ebene (Buchungscode/Anbieter/Betrag) nicht gegriffen hat.
    Bewusst NICHT geprüft: Hotels (Check-in/-out dürfen sich überschneiden).
    Segmente verschiedener Reisender gelten nicht als Überschneidung.
    """
    from mod_zeit import segment_zeit_zu_utc
    P = ph()
    try:
        db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT id, transportart, anbieter, buchungscode, reisender, ki_json
                        FROM belege WHERE reise_code={P} AND transportart IN ('Flug','Bahn')
                        AND aenderung_status IS NULL ORDER BY id""", (reise_code,))
        rows = cur.fetchall(); cur.close(); db.close()
    except Exception:
        return []
    segs = []
    for r in rows:
        g = lambda k, i: r[k] if hasattr(r, 'keys') else r[i]
        try:
            ki = json.loads(g("ki_json", 5) or "{}")
        except Exception:
            continue
        for idx, s in enumerate(ki.get("segmente") or []):
            d = _datum_parsen(s.get("abreise_datum"))
            if not d:
                continue
            d_an = _datum_parsen(s.get("ankunft_datum")) or d
            dt_ab = segment_zeit_zu_utc(d, s.get("abreise_zeit"), s.get("abreise_utc_offset"))
            dt_an = (segment_zeit_zu_utc(d_an, s.get("ankunft_zeit"), s.get("ankunft_utc_offset"))
                     if s.get("ankunft_zeit") else None)
            if dt_ab and dt_an and dt_an <= dt_ab:
                dt_an = dt_an + timedelta(days=1)   # Nachtflug/-zug
            segs.append({"beleg": g("id", 0), "idx": idx, "typ": g("transportart", 1),
                         "anbieter": g("anbieter", 2) or "", "code": g("buchungscode", 3) or "",
                         "reisender": g("reisender", 4) or ki.get("reisender") or "",
                         "seg": s, "datum": d, "dt_ab": dt_ab, "dt_an": dt_an,
                         "nr": re.sub(r"\s+", "", str(s.get("transport_nummer") or "")).upper()})
    paare = []
    for i in range(len(segs)):
        for j in range(i + 1, len(segs)):
            a, b = segs[i], segs[j]
            if a["beleg"] == b["beleg"] or a["typ"] != b["typ"]:
                continue
            if a["seg"].get("beide_behalten") and b["seg"].get("beide_behalten"):
                continue
            if not _gleicher_reisender(a["reisender"], b["reisender"]):
                continue
            grund = None
            if a["datum"] == b["datum"]:
                if a["nr"] and a["nr"] == b["nr"]:
                    grund = "gleiche Nummer am gleichen Tag"
                elif _gleiche_strecke(a["seg"], b["seg"]):
                    grund = "gleiche Strecke am gleichen Tag"
            if not grund and a["dt_ab"] and a["dt_an"] and b["dt_ab"] and b["dt_an"]:
                minuten = (min(a["dt_an"], b["dt_an"]) - max(a["dt_ab"], b["dt_ab"])).total_seconds() / 60
                if minuten > 15:
                    grund = "Zeiten überlappen"
            if not grund:
                continue
            diffs = _seg_unterschiede(a["seg"], b["seg"])
            paare.append({"a": a, "b": b, "grund": grund, "diffs": diffs,
                          "identisch": not any(d["art"] == "abweichend" for d in diffs)})
    return paare


def _ueberschneidungs_banner_fuer_reise(rcode: str) -> str:
    n = len(_segment_ueberschneidungen(rcode))
    if not n:
        return ""
    return ('<div class="alert" style="background:#fef2f2;border:2px solid #f87171;color:#991b1b;'
            'padding:12px 16px;border-radius:8px;margin-bottom:16px">'
            f'⚠ <b>{n} überschneidende Flug-/Zug-Segmente</b> – vermutlich eine Buchung doppelt (z.B. nach '
            'einer Änderung). Reiseplan, Karte und VMA zeigen bis zur Klärung beide. '
            f'<a href="/reise/{rcode}/ueberschneidungen" style="color:#991b1b;font-weight:700">Jetzt klären →</a></div>')


@app.get("/reise/{code}/ueberschneidungen", response_class=HTMLResponse)
def reise_ueberschneidungen(code: str):
    from html import escape as e
    rcode = code.upper()
    paare = _segment_ueberschneidungen(rcode)

    def seite(x, titel):
        kopf = (f'<a href="/beleg/{x["beleg"]}">Beleg #{x["beleg"]}</a> · {e(x["anbieter"])}'
                + (f' · Code {e(x["code"])}' if x["code"] else "")
                + (f' · 👤 {e(x["reisender"])}' if x["reisender"] else ""))
        return (f'<div style="flex:1;min-width:240px;padding:8px 10px;background:#f8fafc;border-radius:8px">'
                f'<div style="font-size:11px;color:#64748b;font-weight:700">{titel}</div>'
                f'<div style="font-size:12px;margin:2px 0 6px 0">{kopf}</div>'
                f'<div style="font-size:13px;font-weight:600">{e(_seg_kurz(x["seg"]))}</div></div>')

    karten = ""
    for k, p in enumerate(paare):
        a, b = p["a"], p["b"]
        chips = ""
        for d in p["diffs"]:
            if d["art"] == "abweichend":
                chips += (f'<span style="display:inline-block;margin:2px 6px 2px 0;padding:2px 8px;border-radius:10px;'
                          f'background:#fef3c7;font-size:12px">{e(d["label"])}: <s>{e(d["a"])}</s> → <b>{e(d["b"])}</b></span>')
            else:
                chips += (f'<span style="display:inline-block;margin:2px 6px 2px 0;padding:2px 8px;border-radius:10px;'
                          f'background:#e2e8f0;font-size:12px">{e(d["label"])} ergänzt: {e(d["a"] or d["b"])}</span>')
        badge = ('<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px;font-size:12px">'
                 'identisch</span>' if p["identisch"] else
                 '<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:10px;font-size:12px">'
                 'weicht ab</span>')
        # Vorauswahl: identisch -> älteres Dokument (A) behalten; abweichend ->
        # neueres Dokument (B), weil es meist die Änderung ist (änderbar!).
        chk_a, chk_b = (" checked", "") if p["identisch"] else ("", " checked")
        karten += (
            '<div class="card" style="margin-bottom:12px"><div class="card-body">'
            f'<div style="margin-bottom:8px"><b>{"✈ Flug" if a["typ"] == "Flug" else "🚆 Zug"}</b> {badge} '
            f'<span style="font-size:12px;color:#64748b">({e(p["grund"])})</span></div>'
            f'<div style="display:flex;gap:10px;flex-wrap:wrap">{seite(a, "A")}{seite(b, "B")}</div>'
            f'<div style="margin-top:6px">{chips}</div>'
            f'<div style="margin-top:8px;font-size:13px">'
            f'<label><input type="radio" name="p{k}" value="a"{chk_a}> A behalten (B ausblenden)</label> &nbsp; '
            f'<label><input type="radio" name="p{k}" value="b"{chk_b}> B behalten (A ausblenden)</label> &nbsp; '
            f'<label><input type="radio" name="p{k}" value="beide"> beide behalten (z.B. zwei Reisende)</label></div>'
            f'<input type="hidden" name="p{k}_a" value="{a["beleg"]}:{a["idx"]}">'
            f'<input type="hidden" name="p{k}_b" value="{b["beleg"]}:{b["idx"]}">'
            '</div></div>')

    # Belege, die nach früheren Bereinigungen keine aktiven Segmente mehr haben
    hinweis = ""
    try:
        P = ph(); db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT id, anbieter, betrag_brutto, ki_json FROM belege
                        WHERE reise_code={P} AND transportart IN ('Flug','Bahn')
                        AND aenderung_status IS NULL ORDER BY id""", (rcode,))
        leer = []
        for r in cur.fetchall():
            try:
                kj = json.loads((r[3] if isinstance(r, tuple) else r["ki_json"]) or "{}")
            except Exception:
                continue
            if kj.get("segmente_verworfen") and not kj.get("segmente"):
                leer.append((r[0] if isinstance(r, tuple) else r["id"],
                             (r[1] if isinstance(r, tuple) else r["anbieter"]) or "",
                             (r[2] if isinstance(r, tuple) else r["betrag_brutto"])))
        cur.close(); db.close()
        if leer:
            zeilen = ", ".join(f'<a href="/beleg/{i}">#{i}</a> ({e(a)}'
                               + (f", {float(b):.2f} EUR" if b else "") + ")" for i, a, b in leer)
            hinweis = ('<div class="alert" style="background:#fffbeb;border:1px solid #fcd34d;color:#92400e;'
                       'padding:12px 16px;border-radius:8px;margin-bottom:16px">'
                       f'ℹ Diese Belege haben nach der Bereinigung <b>keine aktiven Segmente</b> mehr: {zeilen}. '
                       'Ihr Betrag zählt weiterhin in den Kosten – ist der Beleg eine reine Dublette, '
                       'bitte löschen oder den Betrag prüfen.</div>')
    except Exception:
        pass

    if paare:
        inhalt = (f'<p style="font-size:13px;color:var(--muted)">Diese Segmente stammen aus <b>verschiedenen Belegen</b> '
                  'und liegen übereinander. Wähle je Zeile, welche Version gilt. Das ausgeblendete Segment wird '
                  'nicht gelöscht, sondern am Beleg als "verworfen" aufbewahrt. Hotels sind von der Prüfung ausgenommen.</p>'
                  f'<form method="post" action="/reise/{rcode}/ueberschneidungen/anwenden">'
                  f'<input type="hidden" name="anzahl" value="{len(paare)}">{karten}'
                  '<button type="submit" class="btn btn-primary">✅ Auswahl übernehmen</button></form>')
    else:
        inhalt = '<div class="alert alert-ok">Keine überschneidenden Flug-/Zug-Segmente gefunden. ✔</div>'
    content = (f'<h1 class="page-title">Überschneidungen – {rcode}</h1>'
               f'<a href="/reise/{rcode}" class="btn btn-secondary" style="margin-bottom:16px">← Reise</a>'
               f'{hinweis}{inhalt}')
    return HTMLResponse(shell(f"Überschneidungen – {rcode}", content, "reisen"))


@app.post("/reise/{code}/ueberschneidungen/anwenden")
async def reise_ueberschneidungen_anwenden(code: str, request: Request):
    rcode = code.upper()
    form = await request.form()
    try:
        anzahl = int(form.get("anzahl") or 0)
    except ValueError:
        anzahl = 0
    entfernen, behalten = {}, {}     # beleg_id -> {idx: grund} bzw. {idx, ...}
    try:
        for k in range(anzahl):
            wahl = form.get(f"p{k}")
            ra, rb = form.get(f"p{k}_a") or "", form.get(f"p{k}_b") or ""
            if not wahl or ":" not in ra or ":" not in rb:
                continue
            ba, ia = (int(x) for x in ra.split(":", 1))
            bb, ib = (int(x) for x in rb.split(":", 1))
            if wahl == "a":
                entfernen.setdefault(bb, {})[ib] = f"Überschneidung mit Beleg #{ba} (dort behalten)"
            elif wahl == "b":
                entfernen.setdefault(ba, {})[ia] = f"Überschneidung mit Beleg #{bb} (dort behalten)"
            elif wahl == "beide":
                behalten.setdefault(ba, set()).add(ia)
                behalten.setdefault(bb, set()).add(ib)
        P = ph(); db = get_db(); cur = db.cursor()
        for bid in set(entfernen) | set(behalten):
            cur.execute(f"SELECT ki_json FROM belege WHERE id={P} AND reise_code={P}", (bid, rcode))
            r = cur.fetchone()
            if not r:
                continue
            ki = json.loads((r[0] if isinstance(r, tuple) else r["ki_json"]) or "{}")
            segs = ki.get("segmente") or []
            for idx in behalten.get(bid, set()):
                if idx < len(segs) and idx not in entfernen.get(bid, {}):
                    segs[idx]["beide_behalten"] = True
            verworfen = list(ki.get("segmente_verworfen") or [])
            for idx in sorted(entfernen.get(bid, {}), reverse=True):
                if idx < len(segs):
                    s = segs.pop(idx)
                    s["_grund"] = entfernen[bid][idx]
                    verworfen.append(s)
            ki["segmente"] = segs
            if verworfen:
                ki["segmente_verworfen"] = verworfen
            cur.execute(f"UPDATE belege SET ki_json={P} WHERE id={P}", (json.dumps(ki, ensure_ascii=False), bid))
            if segs:
                _event_daten_in_db_aktualisieren(cur, bid, ki)
            # Segment-Indizes haben sich verschoben -> alter Status passt nicht mehr
            cur.execute(f"DELETE FROM flug_status WHERE beleg_id={P}", (bid,))
        db.commit(); cur.close(); db.close()
        db2 = get_db()
        try:
            vma_tage_generieren(rcode, db2)
        finally:
            db2.close()
        return RedirectResponse(f"/reise/{rcode}/ueberschneidungen", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


def _aenderungs_banner_fuer_reise(rcode: str) -> str:
    """Auffälliger Hinweis oben auf der Reise, solange Änderungs-Belege (z.B. neue
    Flugzeiten, Ersatzzug) noch nicht geklärt sind – die Reise zeigt bis dahin
    bewusst die bisherigen Werte."""
    try:
        P = ph(); db = get_db(); cur = db.cursor()
        cur.execute(f"""SELECT id, aenderung_zu_beleg_id FROM belege
                        WHERE reise_code={P} AND aenderung_status='offen' ORDER BY id""", (rcode,))
        rows = [((r[0], r[1]) if isinstance(r, tuple) else (r["id"], r["aenderung_zu_beleg_id"]))
                for r in cur.fetchall()]
        cur.close(); db.close()
    except Exception:
        return ""
    if not rows:
        return ""
    links = " · ".join(f'<a href="/beleg/{i}" style="color:#9a3412;font-weight:700">Beleg #{i}</a> (zu #{z})'
                       for i, z in rows)
    n = len(rows)
    return ('<div class="alert" style="background:#fff7ed;border:2px solid #fb923c;color:#9a3412;'
            'padding:12px 16px;border-radius:8px;margin-bottom:16px">'
            f'⚠ <b>{n} Änderung{"en" if n != 1 else ""} warten auf Klärung</b> – die Reise zeigt bis dahin die '
            f'bisherigen Daten. Bitte Unterschiede prüfen und die richtigen Werte wählen: {links}</div>')


@app.get("/reise/{code}", response_class=HTMLResponse)
def reise_detail(code: str, request: Request):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT code,titel,abreise,rueckkehr,notiz FROM reisen WHERE code={P}",
                    (code.upper(),))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Nicht gefunden",
                '<div class="alert alert-err">Reise nicht gefunden.</div>'))

        def get(row, k, i): return row[k] if hasattr(row,'keys') else row[i]
        rcode = get(r,"code",0); titel = get(r,"titel",1)
        ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3); notiz = get(r,"notiz",4)

        # Mitarbeiter
        cur.execute(f"""SELECT m.kuerzel, m.klarname FROM mitarbeiter m
                        JOIN reise_mitarbeiter rm ON rm.kuerzel = m.kuerzel
                        WHERE rm.reise_code = {P} ORDER BY m.klarname""", (rcode,))
        ma_rows = cur.fetchall()

        # Länder / VMA
        cur.execute(f"""SELECT id, datum_von, datum_bis, land_code, land_name,
                        vma_voll, vma_halb FROM reise_laender
                        WHERE reise_code = {P} ORDER BY datum_von""", (rcode,))
        land_rows = cur.fetchall()

        # VMA je Tag
        cur.execute(f"""SELECT id, datum, land_code, land_name, ist_halber_satz,
                        fruehstueck, mittagessen, abendessen, vma_netto, vma_satz_voll, vma_satz_halb,
                        trennungspauschale, trennungspauschale_quelle, tatsaechliche_uhrzeit, quelle, satz_hinweis
                        FROM vma_tage WHERE reise_code = {P} ORDER BY datum""", (rcode,))
        vma_tage_rows = cur.fetchall()
        if not vma_tage_rows and vma_auto_aktualisieren(rcode):
            # Reise ohne VMA-Tage (z. B. vor dieser Funktion angelegt): einmal automatisch berechnen
            cur.execute(f"""SELECT id, datum, land_code, land_name, ist_halber_satz,
                        fruehstueck, mittagessen, abendessen, vma_netto, vma_satz_voll, vma_satz_halb,
                        trennungspauschale, trennungspauschale_quelle, tatsaechliche_uhrzeit, quelle, satz_hinweis
                        FROM vma_tage WHERE reise_code = {P} ORDER BY datum""", (rcode,))
            vma_tage_rows = cur.fetchall()

        # Belege für den Tagesverlauf
        cur.execute(f"""SELECT id, transportart, transportart_freitext, anbieter,
                        betrag_brutto, waehrung, belegdatum, hotel_checkin_zeit, ki_json,
                        event_datum_von, event_datum_bis, hotel_checkin_datum,
                        hotel_checkout_datum, hotel_checkout_zeit, event_zeit,
                        belegart, beleg_gruppe_id, betrag_eur, betrag_eur_final, nebenkosten_eur,
                        kreditkarte_auslandsentgelt_eur, zahlungsart
                        FROM belege WHERE reise_code = {P} ORDER BY belegdatum""", (rcode,))
        beleg_rows_tag = cur.fetchall()

        # Manuelle Termine für den Tagesverlauf
        cur.execute(f"""SELECT id, datum, uhrzeit_von, uhrzeit_bis, titel, typ, ort, ansprechpartner, telefon
                        FROM termine WHERE reise_code = {P} ORDER BY datum, uhrzeit_von""", (rcode,))
        termin_rows = cur.fetchall()
        cur.close(); db.close()

        today = date.today()
        ab_d = date.fromisoformat(str(ab)[:10]) if ab else None
        zu_d = date.fromisoformat(str(zu)[:10]) if zu else None

        if not ab_d: status_html = '<span class="badge badge-gray">Kein Datum</span>'
        elif today < ab_d:
            tage = (ab_d - today).days
            status_html = f'<span class="badge badge-blue">In {tage} Tag{"en" if tage!=1 else ""}</span>'
        elif zu_d and today <= zu_d:
            status_html = '<span class="badge badge-green">● Aktiv</span>'
        else:
            status_html = '<span class="badge badge-gray">Abgeschlossen</span>'

        # Länder – kompakte Zeile statt großer Tabelle (VMA je Tag steht unten beim Tagesverlauf)
        laender_kompakt = " · ".join(
            f'{get(lr,"land_code",3)} {get(lr,"land_name",4)} '
            f'({fmt_date(get(lr,"datum_von",1))}–{fmt_date(get(lr,"datum_bis",2))}) '
            f'<a href="/reise/{rcode}/land/{get(lr,"id",0)}/bearbeiten" style="color:var(--muted)">✏</a>'
            for lr in land_rows)

        ma_html = " ".join(
            f'<span class="badge badge-green">{get(m,"kuerzel",0)} – {get(m,"klarname",1)}</span>'
            for m in ma_rows) or "–"

        # Reisenden-Zugänge (Portal-Links) für die Karte vorbereiten
        zugaenge_rows = ""
        reisende_liste = reisende_der_reise(rcode)
        bestehende_zugaenge = zugaenge_der_reise(rcode)
        if reisende_liste:
            for ma in reisende_liste:
                mk = ma[0] if isinstance(ma, tuple) else ma["kuerzel"]
                mn = ma[1] if isinstance(ma, tuple) else ma["klarname"]
                me = ma[2] if isinstance(ma, tuple) else ma["email"]
                info_z = bestehende_zugaenge.get(mk)
                if info_z:
                    link_txt = portal_link(info_z["token"], _basis_aus_anfrage(request))
                    status_txt = (f'✓ gesendet am {fmt_date(info_z["email_gesendet_am"])}'
                                   if info_z.get("email_gesendet_am") else "Link erstellt, noch nicht gesendet")
                    link_html = (f'<div style="font-size:11px;color:var(--muted);word-break:break-all">'
                                 f'<a href="{link_txt}" style="color:inherit">{link_txt}</a></div>')
                else:
                    status_txt = "Noch kein Zugang erstellt"
                    link_html = ""
                zugaenge_rows += f"""<div style="display:flex;justify-content:space-between;align-items:center;
                    gap:10px;padding:8px 0;border-bottom:1px solid var(--border)">
                    <div><b>{mk}</b> – {mn}<div style="font-size:11px;color:var(--muted)">{status_txt}</div>{link_html}</div>
                    <form method="post" action="/reise/{rcode}/zugang/{mk}/senden">
                      <button type="submit" class="btn btn-secondary btn-sm"{' disabled' if not me else ''}>
                        📧 {'Erneut senden' if info_z and info_z.get('email_gesendet_am') else 'Link senden'}
                      </button>
                    </form>
                </div>"""
            zugaenge_html = f"""<div class="card" style="margin-top:16px">
              <div class="card-header"><span class="card-title">🔗 Reisenden-Zugänge (Portal)</span></div>
              <div class="card-body">
                <p style="font-size:12px;color:var(--muted);margin-bottom:8px">
                  Jeder Reisende bekommt einen persönlichen Link zum Eintragen von
                  Verpflegung und Reise-/Arbeitszeiten. Automatischer Versand {PORTAL_TAGE_VORHER}
                  Tage vor Abreise (falls Cron eingerichtet ist; nicht bei Bestandsreisen), oder hier manuell.</p>
                {zugaenge_rows}
              </div>
            </div>"""
        else:
            zugaenge_html = ""

        # Belege und Termine nach Datum gruppieren
        TERMIN_ICON = {
            "termin": "🤝", "fahrt": "🚕", "mietwagen": "🚗",
            "hotel": "🏨", "kundenbesuch": "🏢", "sonstiges": "📌",
        }
        BADGE_DETAIL = {
            "Flug": ("✈", "#dbeafe", "#1e40af"), "Hotel": ("🏨", "#dcfce7", "#166534"),
            "Mietwagen": ("🚗", "#ede9fe", "#5b21b6"), "Taxi": ("🚕", "#fef3c7", "#92400e"),
            "Bahn": ("🚆", "#e0e7ff", "#3730a3"), "Tanken": ("⛽", "#fee2e2", "#991b1b"),
            "Bewirtung": ("🍽", "#fff7ed", "#9a3412"), "Sonstiges": ("📄", "#f1f5f9", "#475569"),
        }

        def zeit_aus_beleg(b):
            zeit = get(b, "hotel_checkin_zeit", 7)
            if zeit: return zeit
            ki_str = get(b, "ki_json", 8) or ""
            try:
                segs = json.loads(ki_str).get("segmente") or []
                if segs: return segs[0].get("abreise_zeit", "") or ""
            except: pass
            return ""

        def to_d(v):
            if not v: return None
            if isinstance(v, str): return date.fromisoformat(v[:10])
            return v

        def to_d_ddmmyyyy(v):
            if not v: return None
            try: return datetime.strptime(str(v).strip(), "%d.%m.%Y").date()
            except: return None

        # Events aus Belegen aufbauen: pro Flug-/Bahn-Segment eine eigene Zeile,
        # Mietwagen/Hotel als Abholen+Abgeben bzw. Check-in+Check-out, sonst eine
        # Zeile mit Uhrzeit aus event_zeit (z.B. Tankbeleg, Mautbeleg). Kosten
        # werden pro Beleg nur EINMAL vergeben (beim ersten Auftreten).
        events_by_date: dict = {}

        def add_event(d, zeit, titel_txt, sub_txt, bid, bet_s, bg, fg):
            if not d: return
            events_by_date.setdefault(d, []).append(
                (zeit or "99:99", "beleg", bid, titel_txt, sub_txt, bet_s, bg, fg))

        # Gruppen-ID → vorhandene Belegarten in der Gruppe, um verknüpfte
        # Buchungsbestätigungen im Zeitstrahl zu unterdrücken (sonst doppelte
        # Events + doppelt gezählte Kosten)
        gruppen_arten_tag = {}
        for b in beleg_rows_tag:
            gid = get(b,"beleg_gruppe_id",16)
            if gid:
                gruppen_arten_tag.setdefault(gid, set()).add(get(b,"belegart",15) or "")

        # Gruppen-ID → hat diese Gruppe eine Buchungsbestätigung MIT echten
        # Reisesegmenten (Flug/Bahn/Mietwagen-Ticket)? Wenn ja, ist DIESE der
        # maßgebliche Beleg (die Segmentdaten stecken sonst nirgendwo anders),
        # und eine verknüpfte Rechnung/Quittung in derselben Gruppe gilt dann
        # als redundantes Duplikat (z.B. durch mehrfaches Nachsenden entstanden)
        # – nicht umgekehrt wie bisher, das war nur bei Hotels korrekt (dort IST
        # die Buchungsbestätigung meist wirklich nur ein Duplikat der Rechnung).
        gruppen_mit_segment_buchung = set()
        for b in beleg_rows_tag:
            gid = get(b,"beleg_gruppe_id",16)
            if not gid: continue
            typ_g = get(b,"transportart",1) or ""
            if (get(b,"belegart",15) or "") == "Buchungsbestaetigung" and typ_g in ("Flug","Bahn","Mietwagen") \
                    and segmente_aus_ki_json(get(b,"ki_json",8)):
                gruppen_mit_segment_buchung.add(gid)

        def beleg_unterdruecken(belegart_x, gruppe_x, hat_segmente_x):
            if not gruppe_x: return False
            if gruppe_x in gruppen_mit_segment_buchung:
                # Diese Gruppe hat eine segmenthaltige Buchungsbestätigung ->
                # die ist maßgeblich, alles andere in der Gruppe (Rechnung/
                # Quittung-Duplikate) wird unterdrückt – außer sie selbst.
                return not hat_segmente_x
            # Kein Segment-Ticket in der Gruppe -> altes Verhalten (Hotel-Fall):
            # Buchungsbestätigung wird unterdrückt, wenn eine Rechnung/Quittung
            # in derselben Gruppe existiert.
            return (belegart_x == "Buchungsbestaetigung"
                    and bool(gruppen_arten_tag.get(gruppe_x, set()) & {"Rechnung", "Quittung"}))

        # Kosten: gleiche Regeln wie im Abschluss (mod_belegdeckung) – eine durch Rechnungen vollständig
        # belegte Buchung zählt nicht mit, eine nur teilweise belegte nur mit ihrem offenen Rest.
        beleg_anzahl_gesamt = 0
        for b in beleg_rows_tag:
            belegart_k = get(b,"belegart",15) or ""
            gruppe_k = get(b,"beleg_gruppe_id",16)
            typ_k = get(b,"transportart",1) or ""
            hat_segmente_k = (typ_k in ("Flug","Bahn","Mietwagen")
                               and bool(segmente_aus_ki_json(get(b,"ki_json",8))))
            if beleg_unterdruecken(belegart_k, gruppe_k, hat_segmente_k):
                continue
            beleg_anzahl_gesamt += 1
        _kat_tag = deckung_mod.kategorisieren([
            {"id": get(b,"id",0), "belegart": get(b,"belegart",15), "beleg_gruppe_id": get(b,"beleg_gruppe_id",16),
             "betrag_brutto": get(b,"betrag_brutto",4), "waehrung": get(b,"waehrung",5),
             "betrag_eur": get(b,"betrag_eur",17), "betrag_eur_final": get(b,"betrag_eur_final",18),
             "nebenkosten_eur": get(b,"nebenkosten_eur",19),
             "kreditkarte_auslandsentgelt_eur": get(b,"kreditkarte_auslandsentgelt_eur",20),
             "zahlungsart": get(b,"zahlungsart",21)}
            for b in beleg_rows_tag])
        _zaehlen_tag = set(_kat_tag["final"]) | set(_kat_tag["vorlaeufig"])
        _kunde_tag = set(_kat_tag["durch_kunde"])       # Bezahlart „Durch Kunde“: zählt nie
        beleg_kosten_gesamt = sum(
            beleg_eur_mit_anteil(get(b,"betrag_brutto",4), get(b,"waehrung",5), get(b,"betrag_eur",17),
                                 get(b,"betrag_eur_final",18), get(b,"nebenkosten_eur",19),
                                 get(b,"kreditkarte_auslandsentgelt_eur",20), _kat_tag["anteile"].get(get(b,"id",0)))
            for b in beleg_rows_tag if get(b,"id",0) in _zaehlen_tag
        ) + sum(((r["fehlt_eur"] or 0.0) + r["gebuehr_eur"]) for r in _kat_tag["rest"])

        # Euro-Wert je Beleg für die Tagessummen: nur, was auch in der Gesamtsumme zählt (gleiche Regeln wie Abschluss).
        _eur_tag = {}
        for b in beleg_rows_tag:
            _bid = get(b,"id",0)
            if _bid in _zaehlen_tag:
                _eur_tag[_bid] = beleg_eur_mit_anteil(
                    get(b,"betrag_brutto",4), get(b,"waehrung",5), get(b,"betrag_eur",17),
                    get(b,"betrag_eur_final",18), get(b,"nebenkosten_eur",19),
                    get(b,"kreditkarte_auslandsentgelt_eur",20), _kat_tag["anteile"].get(_bid))
        try:
            _db_t = get_db()
            try:
                _teil_tag = bw.teilbetraege_der_reise(_db_t, rcode)     # Hauptbeleg-ID -> Bewirtung mit Teilbetrag
            finally:
                _db_t.close()
        except Exception:
            _teil_tag = {}
        _hinweis_tag = {}       # Beleg-ID -> Hinweis, warum der Betrag nicht (voll) zählt
        _ohne_kurs_tag = set()  # Fremdwährung ohne Euro-Wert
        for _bid in _kat_tag["nachweis"]:
            _hinweis_tag[_bid] = "Zahlungsnachweis – zählt nicht extra"
        for _bid in _kunde_tag:
            _hinweis_tag[_bid] = "durch Kunde direkt bezahlt – zählt nicht in der Summe"
        for r in _kat_tag["rest"]:
            if r["buchung_ids"]:
                _rest_eur = (r["fehlt_eur"] or 0.0) + r["gebuehr_eur"]
                _eur_tag[r["buchung_ids"][0]] = _rest_eur
                _hinweis_tag[r["buchung_ids"][0]] = "Buchung – nur der offene Rest zählt (%.2f EUR)" % _rest_eur
        for b in beleg_rows_tag:
            _bid = get(b,"id",0)
            if (_bid not in _eur_tag and _bid not in _hinweis_tag
                    and deckung_mod.ist_buchung(get(b,"belegart",15)) and float(get(b,"betrag_brutto",4) or 0) > 0):
                _hinweis_tag[_bid] = "Buchung – zählt nicht, die Rechnungen/Quittungen zählen"

        def betrag_anzeige(b):
            """Betrag im Tagesverlauf: Fremdwährung führend, Euro-Wert daneben (≈ geschätzt, = laut Abrechnung)."""
            betrag_x = get(b,"betrag_brutto",4)
            if not betrag_x:
                return None
            wk = get(b,"waehrung",5) or "EUR"
            text = f"{float(betrag_x):.2f} {wk}"
            if wk == "EUR" or get(b,"id",0) in _kunde_tag:
                return text
            bid_x = get(b,"id",0)
            eigen_final = get(b,"betrag_eur_final",18); eigen_eur = get(b,"betrag_eur",17)
            anteil = _kat_tag["anteile"].get(bid_x)
            if eigen_final is None and eigen_eur is None and not anteil:
                _ohne_kurs_tag.add(bid_x)
                return text + " (Kurs fehlt)"
            eur_x = _eur_tag[bid_x] if (bid_x in _eur_tag and bid_x not in _hinweis_tag) \
                else float(eigen_final if eigen_final is not None else eigen_eur)
            zeichen = "=" if (eigen_final is not None or anteil) else "≈"
            return f"{text} ({zeichen} {eur_x:.2f} EUR)"

        for b in beleg_rows_tag:
            bid = get(b,"id",0); typ = get(b,"transportart",1) or "Sonstiges"
            belegart_b = get(b,"belegart",15) or ""
            gruppe_b = get(b,"beleg_gruppe_id",16)
            hat_segmente_b = typ in ("Flug","Bahn","Mietwagen") and bool(segmente_aus_ki_json(get(b,"ki_json",8)))
            if beleg_unterdruecken(belegart_b, gruppe_b, hat_segmente_b):
                continue  # redundantes Duplikat innerhalb der Gruppe

            freitext = get(b,"transportart_freitext",2) or ""
            anbieter = get(b,"anbieter",3) or "–"
            betrag = get(b,"betrag_brutto",4); waehrung = get(b,"waehrung",5) or "EUR"
            icon, bg, fg = BADGE_DETAIL.get(typ, BADGE_DETAIL["Sonstiges"])
            bet_s = betrag_anzeige(b)
            kosten_vergeben = False

            segs = []
            if typ in ("Flug", "Bahn"):
                segs = segmente_aus_ki_json(get(b,"ki_json",8))

            if segs:
                for s in segs:
                    d_ab = to_d_ddmmyyyy(s.get("abreise_datum")) or \
                        to_d(get(b,"event_datum_von",9)) or to_d(get(b,"belegdatum",6))
                    d_an = to_d_ddmmyyyy(s.get("ankunft_datum")) or d_ab
                    zeit = s.get("abreise_zeit","") or ""
                    an_zeit = s.get("ankunft_zeit","") or ""
                    von = s.get("von_ort") or s.get("von_iata") or "?"
                    nach = s.get("nach_ort") or s.get("nach_iata") or "?"
                    tname = s.get("transport_name","") or ""
                    tnum = s.get("transport_nummer","") or ""
                    ab_term = s.get("abreise_terminal") or ""
                    an_term = s.get("ankunft_terminal") or ""
                    hinweis = s.get("hinweis") or ""
                    titel_txt = f"{icon} {tname} {tnum} · {von} → {nach}".replace("  "," ").strip()

                    sub_parts = [f"Ab {zeit}" + (f" {ab_term}" if ab_term else "")]
                    an_txt = f"An {an_zeit}" + (f" {an_term}" if an_term else "")
                    if d_an and d_an != d_ab: an_txt += f" ({d_an.day:02d}.{d_an.month:02d}.)"
                    sub_parts.append(an_txt)
                    if hinweis: sub_parts.append(hinweis)
                    sub_txt = " · ".join(p for p in sub_parts if p.strip() not in ("Ab", "An"))

                    zeile_bet = bet_s if not kosten_vergeben else None
                    if zeile_bet: kosten_vergeben = True
                    add_event(d_ab, zeit, titel_txt, sub_txt, bid, zeile_bet, bg, fg)
            elif typ == "Hotel":
                ci_d = to_d(get(b,"hotel_checkin_datum",11)) or to_d(get(b,"event_datum_von",9))
                co_d = to_d(get(b,"hotel_checkout_datum",12)) or to_d(get(b,"event_datum_bis",10))
                ci_z = get(b,"hotel_checkin_zeit",7) or ""
                co_z = get(b,"hotel_checkout_zeit",13) or ""
                add_event(ci_d, ci_z, f"{icon} Hotel einchecken · {anbieter}",
                          f"{fmt_date(ci_d)}" if ci_d else "", bid, bet_s, bg, fg)
                if bet_s: kosten_vergeben = True
                if co_d and co_d != ci_d:
                    add_event(co_d, co_z, f"{icon} Hotel auschecken · {anbieter}",
                              f"{fmt_date(co_d)}" if co_d else "", bid, None, bg, fg)
            elif typ == "Mietwagen":
                ab_d = to_d(get(b,"event_datum_von",9)) or to_d(get(b,"belegdatum",6))
                bis_d = to_d(get(b,"event_datum_bis",10))
                zeit = zeit_aus_beleg(b)
                add_event(ab_d, zeit, f"{icon} Mietwagen abholen · {anbieter}",
                          f"{fmt_date(ab_d)}" if ab_d else "", bid, bet_s, bg, fg)
                if bet_s: kosten_vergeben = True
                if bis_d and bis_d != ab_d:
                    add_event(bis_d, "", f"{icon} Mietwagen abgeben · {anbieter}",
                              f"{fmt_date(bis_d)}" if bis_d else "", bid, None, bg, fg)
            else:
                d = to_d(get(b,"event_datum_von",9)) or to_d(get(b,"belegdatum",6))
                zeit = get(b,"event_zeit",14) or ""
                titel_txt = f"{icon} {typ}" + (f" – {freitext}" if freitext else "") + f" · {anbieter}"
                add_event(d, zeit, titel_txt, f"{fmt_date(d)}" if d else "", bid, bet_s, bg, fg)

        termine_je_tag: dict = {}
        for t in termin_rows:
            td = get(t, "datum", 1)
            if isinstance(td, str): td = date.fromisoformat(td[:10])
            termine_je_tag.setdefault(td, []).append(t)

        def tagesablauf_html(tag_datum, rcode):
            eintraege = list(events_by_date.get(tag_datum, []))
            tages_summe = sum(_eur_tag.get(e[2], 0.0) for e in eintraege if e[1] == "beleg" and e[5])
            n_ohne_kurs = sum(1 for e in eintraege if e[1] == "beleg" and e[5] and e[2] in _ohne_kurs_tag and e[2] in _eur_tag)
            for t in termine_je_tag.get(tag_datum, []):
                tid = get(t,"id",0); von = get(t,"uhrzeit_von",2) or ""; bis = get(t,"uhrzeit_bis",3) or ""
                titel_t = get(t,"titel",4); typ_t = get(t,"typ",5) or "termin"
                ort_t = get(t,"ort",6) or ""; ansprech_t = get(t,"ansprechpartner",7) or ""; tel_t = get(t,"telefon",8) or ""
                icon = TERMIN_ICON.get(typ_t, "📌")
                zeit_txt = f"{von}–{bis}" if von and bis else (von or "")
                sub_parts = [p for p in (ort_t,
                             f"👤 {ansprech_t}" if ansprech_t else "",
                             f"📞 {tel_t}" if tel_t else "") if p]
                sub_t = " · ".join(sub_parts)
                eintraege.append((von or "99:99", "termin", tid, f"{icon} {titel_t}", sub_t, zeit_txt, None, None))

            eintraege.sort(key=lambda x: x[0])
            if not eintraege:
                return '<div style="padding:12px 16px;font-size:12px;color:var(--muted);font-style:italic">Keine Termine oder Belege an diesem Tag.</div>'

            rows = ""
            for zeit, art, eid, titel_txt, sub_txt, rechts, bg, fg in eintraege:
                zeit_disp = "" if zeit == "99:99" else zeit
                if art == "beleg":
                    rechts_html = (f'<span style="background:{bg};color:{fg};padding:2px 8px;'
                                    f'border-radius:12px;font-size:11px;font-weight:500">{rechts}</span>'
                                    if rechts else '<span style="font-size:11px;color:var(--muted);font-style:italic">kein Betrag</span>')
                    link = f'<a href="/beleg/{eid}" style="font-size:11px;color:#2563eb;text-decoration:none">Detail</a>'
                else:
                    rechts_html = f'<span style="font-size:11px;color:#7c3aed">✎ manuell · {rechts}</span>' if rechts else '<span style="font-size:11px;color:#7c3aed">✎ manuell</span>'
                    link = (f'<a href="/reise/{rcode}/termin/{eid}/bearbeiten" style="font-size:11px;color:var(--muted);text-decoration:none">✎</a> '
                            f'<a href="/reise/{rcode}/termin/{eid}/loeschen" style="font-size:11px;color:var(--muted);text-decoration:none" '
                            f'onclick="return confirm(\'Termin löschen?\')">🗑</a>')
                sub_html = f'<div style="font-size:11px;color:var(--muted);margin-top:1px">{sub_txt}</div>' if sub_txt else ""
                if art == "beleg" and eid in _hinweis_tag:
                    sub_html += f'<div style="font-size:11px;color:#b45309;margin-top:1px">ⓘ {_hinweis_tag[eid]}</div>'
                if art == "beleg" and eid in _teil_tag:
                    _tt = _teil_tag[eid]
                    sub_html += (f'<div style="font-size:11px;color:#7c3aed;margin-top:1px">🍽 davon Bewirtung '
                                 f'{_tt["teilbetrag"]:.2f} {_tt["waehrung"]} ({_tt["nummer"]})</div>')
                rows += f"""<div style="display:flex;gap:12px;padding:8px 16px;border-bottom:1px solid var(--border);align-items:flex-start">
                    <div style="width:44px;flex-shrink:0;font-size:12px;color:var(--muted);font-weight:600;padding-top:1px">{zeit_disp}</div>
                    <div style="flex:1;font-size:13px">{titel_txt}{sub_html}</div>
                    <div style="text-align:right;white-space:nowrap;display:flex;gap:8px;align-items:center">{rechts_html}{link}</div>
                </div>"""
            if tages_summe > 0 or n_ohne_kurs:
                kurs_txt = (f' <span style="color:#b45309;font-weight:400">· ohne {n_ohne_kurs} Beleg(e) ohne Kurs</span>'
                            if n_ohne_kurs else "")
                rows += (f'<div style="display:flex;justify-content:flex-end;padding:8px 16px;'
                          f'background:var(--bg)"><span style="font-size:12px;font-weight:600;'
                          f'color:var(--text)">Summe Belege: {tages_summe:.2f} EUR{kurs_txt}</span></div>')
            return rows

        wochentage = ["Mo","Di","Mi","Do","Fr","Sa","So"]
        vma_tage_summe = 0.0
        trennung_summe = 0.0
        tage_blocks = ""
        standard_tage = []      # Tage, für die kein Land erkannt wurde (Deutschland als Rückfall)

        def cb(rcode, vid, name, checked, label, abzug_pct):
            ch = "checked" if checked else ""
            return (f'<label style="display:inline-flex;align-items:center;gap:4px;'
                    f'cursor:pointer;font-size:12px;color:var(--muted);margin-right:10px">'
                    f'<input type="checkbox" name="{name}" value="1" {ch} '
                    f'onchange="this.form.submit()" style="width:auto;margin:0">'
                    f'{label} <span style="color:#c81e1e;font-size:10px">-{abzug_pct}%</span>'
                    f'</label>')

        def trenn_select(rcode, vid, aktuell):
            opts = ""
            for wert, label in ((0.0, "0 EUR – keine"), (40.0, "40 EUR – halb"), (80.0, "80 EUR – voll")):
                sel = " selected" if abs((aktuell or 0) - wert) < 0.01 else ""
                opts += f'<option value="{wert:.0f}"{sel}>{label}</option>'
            return (f'<form method="post" action="/reise/{rcode}/vma/{vid}/trennungspauschale" '
                    f'style="display:inline-flex;align-items:center;gap:4px;margin-left:10px">'
                    f'<span style="font-size:11px;color:var(--muted)">Trennungspauschale:</span>'
                    f'<select name="trennungspauschale" onchange="this.form.submit()" '
                    f'style="font-size:11px;padding:2px 4px;border:1px solid var(--border);border-radius:4px">'
                    f'{opts}</select></form>')

        for i, vt in enumerate(vma_tage_rows):
            vid = get(vt,"id",0)
            vd = get(vt,"datum",1)
            if isinstance(vd, str): vd = date.fromisoformat(vd[:10])
            lcode_t = get(vt,"land_code",2) or "DE"
            lname_t = get(vt,"land_name",3) or "Deutschland"
            ist_halb = bool(get(vt,"ist_halber_satz",4))
            frueh = bool(get(vt,"fruehstueck",5))
            mittag = bool(get(vt,"mittagessen",6))
            abend = bool(get(vt,"abendessen",7))
            netto = float(get(vt,"vma_netto",8) or 0)
            trennung = float(get(vt,"trennungspauschale",11) or 0)
            tatsaechliche_zeit = get(vt,"tatsaechliche_uhrzeit",13) or ""
            quelle_t = get(vt,"quelle",14) or ""
            vma_tage_summe += netto
            trennung_summe += trennung

            ist_erster_tag = (i == 0)
            ist_letzter_tag_schleife = (i == len(vma_tage_rows) - 1)

            wt = wochentage[vd.weekday()] if vd else "–"
            datum_txt = f"{wt} {vd.day:02d}.{vd.month:02d}.{vd.year}" if vd else "–"
            halb_badge = ('<span style="font-size:10px;background:#fef3c7;color:#92400e;'
                           'padding:1px 7px;border-radius:10px">½ Satz</span>') if ist_halb else ""
            trenn_badge = (f'<div style="font-size:12px;color:#7c3aed;font-weight:600;margin-top:2px">'
                            f'+ {trennung:.2f} EUR Trennungspauschale</div>') if trennung else ""

            tatsaechliche_zeit_html = ""
            if ist_erster_tag or ist_letzter_tag_schleife:
                label = "Reiseantritt (tatsächlich)" if ist_erster_tag else "Reiseende (tatsächlich)"
                hinweis = ("z.B. schon losgefahren, bevor der Flug startet" if ist_erster_tag
                           else "z.B. noch Heimfahrt nach der Ankunft")
                tatsaechliche_zeit_html = (
                    f'<form method="post" action="/reise/{rcode}/vma/{vid}/tatsaechliche-zeit" '
                    f'style="display:inline-flex;align-items:center;gap:6px;margin-left:10px">'
                    f'<span style="font-size:11px;color:var(--muted)">{label}:</span>'
                    f'<input type="time" name="tatsaechliche_uhrzeit" value="{tatsaechliche_zeit}" '
                    f'onchange="this.form.submit()" title="{hinweis}" '
                    f'style="font-size:11px;padding:2px 6px;border:1px solid var(--border);border-radius:4px">'
                    f'</form>')

            satz_hinweis_t = get(vt,"satz_hinweis",15) or ""
            satz_hinweis_html = ""
            if satz_hinweis_t:
                satz_hinweis_html = (
                    f'<div style="font-size:11px;color:#1d4ed8;margin-top:3px">🔔 {satz_hinweis_t} '
                    f'<form method="post" action="/reise/{rcode}/vma/{vid}/hinweis-ok" style="display:inline">'
                    f'<button type="submit" style="font-size:11px;color:#1d4ed8;background:none;border:1px solid #bfdbfe;'
                    f'border-radius:6px;padding:1px 8px;cursor:pointer;margin-left:6px">OK</button></form></div>')

            kein_land_html = ""
            if quelle_t == "Standard":
                standard_tage.append(vd)
                kein_land_html = ('<div style="font-size:11px;color:#b45309;margin-top:2px">ⓘ Land nicht erkannt '
                                  '(kein Flug, Hotel oder Land-Eintrag für diesen Tag) – es gilt Deutschland</div>')

            reset_html = ""
            if quelle_t == "manuell":
                bestaetigung = ("Diesen Tag von \u2018manuell\u2019 zur\u00fcck auf \u2018automatisch\u2019 setzen? "
                                 "Land/S\u00e4tze werden beim n\u00e4chsten VMA neu berechnen wieder automatisch ermittelt.")
                reset_html = (
                    f'<form method="post" action="/reise/{rcode}/vma/{vid}/zuruecksetzen" style="display:inline"'
                    f' onsubmit="return confirm(\'{bestaetigung}\')">'
                    f'<button type="submit" style="font-size:11px;color:#94a3b8;background:none;border:none;'
                    f'text-decoration:underline;cursor:pointer;padding:4px 6px" '
                    f'title="Manuell-Markierung entfernen, damit die automatische Erkennung wieder greift">↺ Auto</button>'
                    f'</form>')

            tage_blocks += f"""<div style="border-bottom:1px solid var(--border)">
              <div style="padding:12px 16px;background:var(--bg)">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
                  <div>
                    <div style="font-weight:700;color:var(--text);font-size:14px">{datum_txt} {halb_badge}</div>
                    <div style="font-size:11px;color:var(--muted);margin-top:2px">🌍 {lname_t} ({lcode_t})</div>
                    {kein_land_html}
                    {satz_hinweis_html}
                  </div>
                  <div style="text-align:right">
                    <div style="font-size:16px;font-weight:700;color:var(--green)">{netto:.2f} EUR VMA netto</div>
                    {trenn_badge}
                  </div>
                  <a href="/reise/{rcode}/termin/neu?datum={vd.isoformat() if vd else ''}"
                     style="font-size:11px;color:#2563eb;text-decoration:none;border:0.5px solid #bfdbfe;
                            border-radius:6px;padding:4px 10px">+ Termin</a>
                  {reset_html}
                </div>
                <form method="post" action="/reise/{rcode}/vma/{vid}/speichern" style="margin-top:8px;display:inline-block">
                  <input type="hidden" name="land_code" value="{lcode_t}">
                  <input type="hidden" name="ist_halber_satz" value="{'1' if ist_halb else ''}">
                  {cb(rcode, vid, "fruehstueck", frueh, "Frühstück", 20)}
                  {cb(rcode, vid, "mittagessen", mittag, "Mittag", 40)}
                  {cb(rcode, vid, "abendessen", abend, "Abend", 40)}
                </form>
                <details style="margin-top:6px;font-size:11px">
                  <summary style="cursor:pointer;color:#94a3b8">Satz manuell überschreiben (falls die Automatik hier mal daneben liegt)</summary>
                  <form method="post" action="/reise/{rcode}/vma/{vid}/speichern" style="margin-top:6px;display:flex;gap:6px;align-items:center;flex-wrap:wrap">
                    <input type="hidden" name="land_code" value="{lcode_t}">
                    <input type="hidden" name="ist_halber_satz" value="{'1' if ist_halb else ''}">
                    <input type="hidden" name="fruehstueck" value="{'1' if frueh else ''}">
                    <input type="hidden" name="mittagessen" value="{'1' if mittag else ''}">
                    <input type="hidden" name="abendessen" value="{'1' if abend else ''}">
                    <label>Voller Satz: <input type="number" step="0.01" name="voll_manuell" placeholder="z.B. 28.00" style="width:80px"></label>
                    <label>Halber Satz: <input type="number" step="0.01" name="halb_manuell" placeholder="z.B. 14.00" style="width:80px"></label>
                    <button type="submit" class="btn btn-secondary btn-sm">Übernehmen</button>
                  </form>
                </details>
                {trenn_select(rcode, vid, trennung) if vd and vd.weekday() in (5,6) else ""}
                {tatsaechliche_zeit_html}
              </div>
              {tagesablauf_html(vd, rcode)}
            </div>"""

        if standard_tage:
            tage_blocks = (f'<div style="padding:10px 16px;font-size:12px;color:#92400e;background:#fffbeb;'
                           f'border-bottom:1px solid var(--border)">ⓘ Für {len(standard_tage)} Tag(e) wurde kein Land erkannt, '
                           f'es gilt der Deutschland-Satz. Die VMA-Berechnung braucht <b>keine Termine</b>: Sie nutzt Abreise und '
                           f'Rückkehr, Flug- und Hotelbelege und die Länder-Einträge. Bei einer Auslandsreise bitte den '
                           f'Hotel- oder Flugbeleg hochladen oder oben <b>🌍 + Land</b> eintragen; die Tage werden '
                           f'dann automatisch neu berechnet.</div>') + tage_blocks

        # Belege mit Datum außerhalb des berechneten Reisezeitraums – sonst verschwinden sie
        tage_im_bereich = {get(vt,"datum",1) if not isinstance(get(vt,"datum",1), str)
                            else date.fromisoformat(get(vt,"datum",1)[:10]) for vt in vma_tage_rows}
        ausserhalb_tage = sorted(d for d in events_by_date.keys() if d not in tage_im_bereich)
        if ausserhalb_tage:
            tage_blocks += ('<div style="padding:10px 16px;font-size:12px;color:#b45309;'
                             'font-style:italic;background:#fffbeb">⚠ Belege außerhalb des '
                             'Reisezeitraums (Datum passt nicht zur Reisedauer)</div>')
            for d in ausserhalb_tage:
                wt = wochentage[d.weekday()]
                datum_txt = f"{wt} {d.day:02d}.{d.month:02d}.{d.year}"
                tage_blocks += f"""<div style="border-bottom:1px solid var(--border)">
                  <div style="padding:10px 16px;background:var(--bg)">
                    <div style="font-weight:700;color:var(--text);font-size:14px">{datum_txt}</div>
                  </div>
                  {tagesablauf_html(d, rcode)}
                </div>"""

        content = f"""
        {_aenderungs_banner_fuer_reise(rcode)}
        {_ueberschneidungs_banner_fuer_reise(rcode)}
        <div style="display:flex;align-items:flex-start;gap:16px;margin-bottom:20px;flex-wrap:wrap">
          <div style="flex:1">
            <div style="font-family:monospace;font-size:13px;color:var(--muted);margin-bottom:4px">{rcode}</div>
            <h1 class="page-title" style="margin:0">{titel}</h1>
            <div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
              {status_html}
              <span style="color:var(--muted);font-size:13px">
                📅 {fmt_date(ab)} – {fmt_date(zu)}
              </span>
              <span style="color:var(--muted);font-size:13px">👤 {ma_html}</span>
            </div>
            {f'<div style="margin-top:8px;font-size:13px;color:var(--muted)">{notiz}</div>' if notiz else ''}
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <a href="/reise/{rcode}/abschluss" class="btn btn-primary">🧾 Abschluss</a>
            <a href="/reise/{rcode}/habel" class="btn btn-secondary">📤 An Habel</a>
            <a href="/reise/{rcode}/vorschuss" class="btn btn-secondary">💵 Vorschuss</a>
            <a href="/reise/{rcode}/reiseplan" class="btn btn-secondary">🗺 Reiseplan</a>
            <a href="/reise/{rcode}/kalender" class="btn btn-secondary">📅 Outlook</a>
            <a href="/reise/{rcode}/vma-generieren" class="btn btn-secondary">🔄 VMA neu berechnen</a>
            <a href="/reise/{rcode}/vma-debug" class="btn btn-secondary">🔍 VMA-Debug</a>
            <a href="/reise/{rcode}/bearbeiten" class="btn btn-secondary">✏ Bearbeiten</a>
            <a href="/reise/{rcode}/land/neu" class="btn btn-secondary">🌍 + Land</a>
          </div>
        </div>

        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px">
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:22px;font-weight:600;color:var(--green)">{(vma_tage_summe + trennung_summe):.2f} €</div>
            <div style="font-size:12px;color:var(--muted)">VMA + Trennungspauschale</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:22px;font-weight:600">{beleg_kosten_gesamt:.2f} €</div>
            <div style="font-size:12px;color:var(--muted)">Kosten aus Belegen</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:22px;font-weight:600">{beleg_anzahl_gesamt}</div>
            <div style="font-size:12px;color:var(--muted)">Belege</div>
          </div></div>
        </div>

        {'<div style="font-size:12px;color:var(--muted);margin-bottom:16px">🌍 ' + laender_kompakt + '</div>' if laender_kompakt else ''}

        <div class="card">
          <div class="card-header">
            <span class="card-title">📅 Tagesverlauf & VMA</span>
          </div>
          {('<div style="padding:12px 16px;background:#fef3c7;color:#92400e;font-size:12px;border-bottom:1px solid var(--border)">⚠ Für diese Reise wurde die VMA noch nicht berechnet – solange fehlen die Tages-Köpfe und Belege werden fälschlich als "außerhalb des Reisezeitraums" angezeigt. <a href="/reise/' + rcode + '/vma-generieren" style="color:#92400e;font-weight:600">🔄 Jetzt VMA berechnen</a></div>' if not vma_tage_rows else '') + (tage_blocks if tage_blocks else '<div class="card-body"><div class="empty-state"><b>Noch keine VMA-Tage berechnet</b><p>Die Tage werden normalerweise automatisch angelegt. Termine sind dafür nicht nötig – bitte einmal berechnen.</p><a href="/reise/' + rcode + '/vma-generieren" class="btn btn-primary" style="margin-top:12px">🔄 VMA berechnen</a></div></div>')}
          {'<div style="padding:12px 16px;display:flex;justify-content:flex-end;gap:24px;background:var(--bg)"><span style="font-size:13px;color:var(--muted)">VMA gesamt: <b style="color:var(--green)">' + f"{vma_tage_summe:.2f}" + ' EUR</b></span>' + (f'<span style="font-size:13px;color:var(--muted)">Trennungspauschale: <b style="color:#7c3aed">{trennung_summe:.2f} EUR</b></span>' if trennung_summe else '') + f'<span style="font-size:13px;font-weight:700">Gesamt: {vma_tage_summe + trennung_summe:.2f} EUR</span></div>' if vma_tage_rows else ''}
        </div>

        {habel_einzel_karte_html(rcode)}

        {zugaenge_html}

        <div style="margin-top:12px">
          <a href="/reisen" class="btn btn-secondary">← Zurück</a>
        </div>"""
        return HTMLResponse(shell(f"Reise {rcode}", content, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))

@app.get("/reise/{code}/bearbeiten", response_class=HTMLResponse)
def reise_bearbeiten_form(code: str):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT code,titel,abreise,rueckkehr,notiz FROM reisen WHERE code={P}",
                    (code.upper(),))
        r = cur.fetchone()
        if not r:
            return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Nicht gefunden.</div>'))
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        rcode = get(r,"code",0); titel = get(r,"titel",1)
        ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3); notiz = get(r,"notiz",4)

        cur.execute("SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = 1"
                    " ORDER BY klarname")
        all_ma = cur.fetchall()
        cur.execute(f"SELECT kuerzel FROM reise_mitarbeiter WHERE reise_code={P}", (rcode,))
        assigned = {get(x,"kuerzel",0) for x in cur.fetchall()}
        cur.close(); db.close()

        ma_opts = "".join(
            f'<option value="{get(m,"kuerzel",0)}"'
            f'{" selected" if get(m,"kuerzel",0) in assigned else ""}>'
            f'{get(m,"kuerzel",0)} – {get(m,"klarname",1)}</option>'
            for m in all_ma)

        ab_s = str(ab)[:10] if ab else ""; zu_s = str(zu)[:10] if zu else ""
        content = f"""
        <h1 class="page-title">Reise {rcode} bearbeiten</h1>
        <div class="card" style="max-width:600px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Titel <span class="required">*</span></label>
                  <input type="text" name="titel" value="{titel}" required>
                </div>
                <div class="form-group">
                  <label>Abreise <span class="required">*</span></label>
                  <input type="date" name="abreise" value="{ab_s}" required>
                </div>
                <div class="form-group">
                  <label>Rückkehr <span class="required">*</span></label>
                  <input type="date" name="rueckkehr" value="{zu_s}" required>
                </div>
                <div class="form-group full">
                  <label>Mitarbeiter</label>
                  <select name="mitarbeiter" multiple size="4">{ma_opts}</select>
                  <div class="form-hint">Strg+Klick für Mehrfachauswahl</div>
                </div>
                <div class="form-group full">
                  <label>Notiz</label>
                  <textarea name="notiz" rows="2">{notiz or ''}</textarea>
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Reise {rcode} bearbeiten", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/bearbeiten")
async def reise_bearbeiten(code: str, request: Request):
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    abreise = (form.get("abreise") or "").strip()
    rueckkehr = (form.get("rueckkehr") or "").strip()
    notiz = (form.get("notiz") or "").strip()
    mitarbeiter = form.getlist("mitarbeiter")
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(
            f"UPDATE reisen SET titel={P},abreise={P},rueckkehr={P},notiz={P} WHERE code={P}",
            (titel, abreise, rueckkehr, notiz or None, rcode))
        cur.execute(f"DELETE FROM reise_mitarbeiter WHERE reise_code={P}", (rcode,))
        for ma in mitarbeiter:
            cur.execute(f"INSERT INTO reise_mitarbeiter (reise_code,kuerzel) VALUES ({P},{P})",
                        (rcode, ma))
        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(rcode)     # neue Reisedaten: Tage anpassen
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Land hinzufügen ────────────────────────────────────────────────────────────
@app.get("/reise/{code}/land/neu", response_class=HTMLResponse)
def land_neu_form(code: str):
    rcode = code.upper()
    land_opts = "".join(
        f'<option value="{lc}">{name} ({lc})</option>'
        for lc, name in LAENDER_LISTE)
    content = f"""
    <h1 class="page-title">Land hinzufügen – {rcode}</h1>
    <div class="card" style="max-width:500px">
      <div class="card-body">
        <form method="post" action="/reise/{rcode}/land/neu">
          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Land <span class="required">*</span></label>
              <select name="land_code" id="land_code" required onchange="ladeVMA(this.value)">
                {land_opts}
              </select>
            </div>
            <div class="form-group full">
              <label>Ort / Region</label>
              <select name="ort" id="ort_select" onchange="zeigeSatz()">
                <option value="">Standard (ganzes Land)</option>
              </select>
              <div id="vma-info" class="form-hint" style="color:var(--green)"></div>
            </div>
            <div class="form-group">
              <label>Von (Datum) <span class="required">*</span></label>
              <input type="date" name="datum_von" required>
            </div>
            <div class="form-group">
              <label>Bis (Datum) <span class="required">*</span></label>
              <input type="date" name="datum_bis" required>
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Hinzufügen</button>
            <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>
    <script>
    let aktuelleInfo = null;
    async function ladeVMA(code) {{
        const res = await fetch('/vma-saetze/info/' + code);
        const data = await res.json();
        aktuelleInfo = data;
        const sel = document.getElementById('ort_select');
        sel.innerHTML = '<option value="">Standard (ganzes Land)</option>';
        (data.staedte || []).forEach(s => {{
            const opt = document.createElement('option');
            opt.value = s.ort;
            opt.textContent = s.ort + ' (' + s.voll.toFixed(2) + ' EUR / ' + s.halb.toFixed(2) + ' EUR)';
            sel.appendChild(opt);
        }});
        zeigeSatz();
    }}
    function zeigeSatz() {{
        const el = document.getElementById('vma-info');
        const ort = document.getElementById('ort_select').value;
        if (!aktuelleInfo) {{ el.textContent = ''; return; }}
        if (ort) {{
            const s = (aktuelleInfo.staedte || []).find(x => x.ort === ort);
            if (s) el.textContent = s.name + ': ' + s.voll.toFixed(2) + ' EUR/Tag · ' + s.halb.toFixed(2) + ' EUR halber Satz';
        }} else if (aktuelleInfo.standard) {{
            const s = aktuelleInfo.standard;
            el.textContent = s.land_name + ' (' + s.quelle + '): ' + s.voll.toFixed(2) + ' EUR/Tag · ' + s.halb.toFixed(2) + ' EUR halber Satz';
        }}
    }}
    ladeVMA(document.getElementById('land_code').value);
    </script>"""
    return HTMLResponse(shell(f"Land – {rcode}", content, "reisen"))

@app.post("/reise/{code}/land/neu")
async def land_neu(code: str, request: Request):
    rcode = code.upper()
    form = await request.form()
    land_code = (form.get("land_code") or "").strip().upper()
    ort = (form.get("ort") or "").strip() or None
    datum_von = (form.get("datum_von") or "").strip()
    datum_bis = (form.get("datum_bis") or "").strip()
    if not all([land_code, datum_von, datum_bis]):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Alle Felder sind Pflicht.</div>'
            f'<a href="/reise/{rcode}/land/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        info = vma_fuer_land_erweitert(cur, land_code, ort)
        land_name = info["land_name"]
        vvoll, vhalb = info["voll"], info["halb"]
        cur.execute(
            f"INSERT INTO reise_laender (reise_code,datum_von,datum_bis,land_code,land_name,vma_voll,vma_halb,ort) "
            f"VALUES ({P},{P},{P},{P},{P},{P},{P},{P})",
            (rcode, datum_von, datum_bis, land_code, land_name, vvoll, vhalb, ort))
        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(rcode)
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/land/{lid}/bearbeiten", response_class=HTMLResponse)
def land_bearbeiten_form(code: str, lid: int):
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(
            f"SELECT id,datum_von,datum_bis,land_code,vma_voll,vma_halb FROM reise_laender WHERE id={P}",
            (lid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r: return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Nicht gefunden.</div>'))
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        dvon = str(get(r,"datum_von",1))[:10]; dbis = str(get(r,"datum_bis",2))[:10]
        lcode = get(r,"land_code",3)
        vvoll = get(r,"vma_voll",4) or 0; vhalb = get(r,"vma_halb",5) or 0

        land_opts = "".join(
            f'<option value="{lc}"{" selected" if lc==lcode else ""}>{name} ({lc})</option>'
            for lc, name in LAENDER_LISTE)

        content = f"""
        <h1 class="page-title">Land bearbeiten – {rcode}</h1>
        <div class="card" style="max-width:500px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/land/{lid}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Land</label>
                  <select name="land_code" onchange="showVMA(this.value)">{land_opts}</select>
                </div>
                <div class="form-group">
                  <label>Von</label>
                  <input type="date" name="datum_von" value="{dvon}" required>
                </div>
                <div class="form-group">
                  <label>Bis</label>
                  <input type="date" name="datum_bis" value="{dbis}" required>
                </div>
                <div class="form-group">
                  <label>VMA Voll (EUR/Tag)</label>
                  <input type="number" step="0.01" name="vma_voll" value="{vvoll}">
                </div>
                <div class="form-group">
                  <label>VMA Halb (EUR/Tag)</label>
                  <input type="number" step="0.01" name="vma_halb" value="{vhalb}">
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}/land/{lid}/loeschen"
                   onclick="return confirm('Land löschen?')"
                   class="btn btn-danger">Löschen</a>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>
        <script>
        const VMA = {json.dumps(VMA_SAETZE)};
        function showVMA(code) {{
            const info = VMA[code];
            if (info) {{
                document.querySelector('input[name="vma_voll"]').value = info.voll;
                document.querySelector('input[name="vma_halb"]').value = info.halb;
            }}
        }}
        </script>"""
        return HTMLResponse(shell(f"Land bearbeiten", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/land/{lid}/bearbeiten")
async def land_bearbeiten(code: str, lid: int, request: Request):
    rcode = code.upper()
    form = await request.form()
    lcode = (form.get("land_code") or "").strip().upper()
    dvon = (form.get("datum_von") or "").strip()
    dbis = (form.get("datum_bis") or "").strip()
    vvoll = float(form.get("vma_voll") or 0)
    vhalb = float(form.get("vma_halb") or 0)
    lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"UPDATE reise_laender SET land_code={P},land_name={P},datum_von={P},"
            f"datum_bis={P},vma_voll={P},vma_halb={P} WHERE id={P}",
            (lcode, lname, dvon, dbis, vvoll, vhalb, lid))
        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(rcode)
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/land/{lid}/loeschen")
def land_loeschen(code: str, lid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"DELETE FROM reise_laender WHERE id={P}", (lid,))
        db.commit(); cur.close(); db.close()
        vma_auto_aktualisieren(rcode)
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Manuelle Termine ────────────────────────────────────────────────────────────
TERMIN_ICON_MAP = {
    "termin": "🤝", "fahrt": "🚕", "mietwagen": "🚗",
    "hotel": "🏨", "kundenbesuch": "🏢", "sonstiges": "📌",
}
TERMIN_TYPEN = [
    ("termin", "🤝 Termin"), ("kundenbesuch", "🏢 Kundenbesuch"), ("fahrt", "🚕 Fahrt/Taxi"),
    ("mietwagen", "🚗 Mietwagen"), ("hotel", "🏨 Hotel"), ("sonstiges", "📌 Sonstiges"),
]

@app.get("/reise/{code}/termin/neu", response_class=HTMLResponse)
def termin_neu_form(code: str, datum: str = "", zurueck: str = ""):
    rcode = code.upper()
    typ_opts = "".join(f'<option value="{v}">{l}</option>' for v, l in TERMIN_TYPEN)
    ziel = f"/reise/{rcode}/reiseplan" if zurueck == "reiseplan" else f"/reise/{rcode}"

    # Ort/Adresse-Feld: Mit Google Places Autocomplete (Firmenname -> korrekte
    # Adresse live vorgeschlagen), falls ein API-Key hinterlegt ist – sonst
    # ganz normales Freitextfeld als Rückfall, nichts bricht dadurch.
    if GOOGLE_PLACES_API_KEY:
        ort_feld = f"""
            <div id="ort_autocomplete_container"></div>
            <input type="text" id="ort_fallback" placeholder="z.B. Firmenname oder Musterstraße 1, 12345 Musterstadt"
                   style="display:none">
            <input type="hidden" name="ort" id="ort_hidden">
            <input type="hidden" name="lat" id="google_lat">
            <input type="hidden" name="lon" id="google_lon">
            <input type="hidden" name="land_code" id="google_land">
            <script>
              (g=>{{var h,a,k,p="The Google Maps JavaScript API",c="google",l="importLibrary",q="__ib__",m=document,b=window;b=b[c]||(b[c]={{}});var d=b.maps||(b.maps={{}}),r=new Set,e=new URLSearchParams,u=()=>h||(h=new Promise(async(f,n)=>{{await (a=m.createElement("script"));e.set("libraries",[...r]+"");for(k in g)e.set(k.replace(/[A-Z]/g,t=>"_"+t[0].toLowerCase()),g[k]);e.set("callback",c+".maps."+q);a.src="https://maps."+c+"apis.com/maps/api/js?"+e;d[q]=f;a.onerror=()=>h=n(Error(p+" could not load."));a.nonce=m.querySelector("script[nonce]")?.nonce||"";m.head.append(a)}}));d[l]?console.warn(p+" only loads once. Ignoring:",g):d[l]=(f,...n)=>r.add(f)&&u().then(()=>d[l](f,...n))}})({{key:"{GOOGLE_PLACES_API_KEY}",v:"weekly"}});

              (async () => {{
                try {{
                  const {{ PlaceAutocompleteElement }} = await google.maps.importLibrary("places");
                  const pac = new PlaceAutocompleteElement({{}});
                  document.getElementById('ort_autocomplete_container').appendChild(pac);
                  pac.addEventListener('gmp-select', async (ev) => {{
                    const place = ev.placePrediction.toPlace();
                    await place.fetchFields({{fields: ['formattedAddress','location','addressComponents']}});
                    document.getElementById('ort_hidden').value = place.formattedAddress || '';
                    if (place.location) {{
                      document.getElementById('google_lat').value = place.location.lat();
                      document.getElementById('google_lon').value = place.location.lng();
                    }}
                    let land = '';
                    (place.addressComponents || []).forEach(comp => {{
                      if (comp.types.includes('country')) land = comp.shortText;
                    }});
                    document.getElementById('google_land').value = land;
                  }});
                }} catch (e) {{
                  console.warn('Google Places Autocomplete nicht verfügbar, Freitext-Eingabe:', e);
                  const fb = document.getElementById('ort_fallback');
                  fb.style.display = 'block';
                  fb.addEventListener('input', function() {{
                    document.getElementById('ort_hidden').value = this.value;
                  }});
                }}
              }})();
            </script>"""
    else:
        ort_feld = '<input type="text" name="ort" placeholder="z.B. Musterstraße 1, 12345 Musterstadt">'

    content = f"""
    <h1 class="page-title">Termin hinzufügen – {rcode}</h1>
    <div class="card" style="max-width:500px">
      <div class="card-body">
        <form method="post" action="/reise/{rcode}/termin/neu?zurueck={zurueck}">
          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Titel <span class="required">*</span></label>
              <input type="text" name="titel" required placeholder="z.B. Kundenbesuch bei Müller GmbH">
            </div>
            <div class="form-group">
              <label>Typ</label>
              <select name="typ">{typ_opts}</select>
            </div>
            <div class="form-group">
              <label>Datum <span class="required">*</span></label>
              <input type="date" name="datum" value="{datum}" required>
            </div>
            <div class="form-group">
              <label>Uhrzeit von</label>
              <input type="time" name="uhrzeit_von">
            </div>
            <div class="form-group">
              <label>Uhrzeit bis</label>
              <input type="time" name="uhrzeit_bis">
            </div>
            <div class="form-group full">
              <label>Ort / Adresse</label>
              {ort_feld}
            </div>
            <div class="form-group">
              <label>Ansprechpartner</label>
              <input type="text" name="ansprechpartner" placeholder="z.B. Frau Schmidt">
            </div>
            <div class="form-group">
              <label>Telefon</label>
              <input type="tel" name="telefon" placeholder="z.B. 0171 1234567">
            </div>
            <div class="form-group full">
              <label>Notiz</label>
              <input type="text" name="notiz" placeholder="optional">
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Hinzufügen</button>
            <a href="{ziel}" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell(f"Termin – {rcode}", content, "reisen"))

@app.post("/reise/{code}/termin/neu")
async def termin_neu(code: str, request: Request, zurueck: str = ""):
    rcode = code.upper()
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    typ = (form.get("typ") or "termin").strip()
    datum = (form.get("datum") or "").strip()
    von = (form.get("uhrzeit_von") or "").strip() or None
    bis = (form.get("uhrzeit_bis") or "").strip() or None
    ort = (form.get("ort") or "").strip() or None
    ansprechpartner = (form.get("ansprechpartner") or "").strip() or None
    telefon = (form.get("telefon") or "").strip() or None
    notiz = (form.get("notiz") or "").strip() or None
    # Bei Nutzung des Google-Places-Autocompletes liefert das Formular die
    # Koordinaten direkt mit (per JS-Hiddenfeld) – dann lohnt sich keine
    # zusätzliche Nominatim-Abfrage mehr, Google hat schon das Genauere.
    google_lat = (form.get("lat") or "").strip()
    google_lon = (form.get("lon") or "").strip()
    google_land = (form.get("land_code") or "").strip()
    ziel = f"/reise/{rcode}/reiseplan" if zurueck == "reiseplan" else f"/reise/{rcode}"
    if not titel or not datum:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Titel und Datum sind Pflicht.</div>'
            f'<a href="/reise/{rcode}/termin/neu?zurueck={zurueck}" class="btn btn-secondary">Zurück</a>'))
    try:
        if google_lat and google_lon:
            lat, lon, land_code = float(google_lat), float(google_lon), (google_land.upper() or None)
        else:
            # Rückfall: kein Google-Treffer (JS nicht geladen, Freitext
            # manuell eingegeben) – Adresse per Nominatim geokodieren.
            # Schlägt auch das fehl, wird der Termin trotzdem ganz normal
            # ohne Koordinaten gespeichert.
            geo = adresse_geokodieren(ort) if ort else None
            lat, lon, land_code = geo if geo else (None, None, None)

        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"INSERT INTO termine (reise_code,datum,uhrzeit_von,uhrzeit_bis,titel,typ,ort,"
            f"ansprechpartner,telefon,notiz,lat,lon,land_code) "
            f"VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})",
            (rcode, datum, von, bis, titel, typ, ort, ansprechpartner, telefon, notiz,
             lat, lon, land_code))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(ziel, status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/termin/{tid}/bearbeiten", response_class=HTMLResponse)
def termin_bearbeiten_form(code: str, tid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT id,datum,uhrzeit_von,uhrzeit_bis,titel,typ,notiz,ort,ansprechpartner,telefon "
                    f"FROM termine WHERE id={P}", (tid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Termin nicht gefunden.</div>'))
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        datum_v = str(g("datum",1))[:10]; von_v = g("uhrzeit_von",2) or ""; bis_v = g("uhrzeit_bis",3) or ""
        titel_v = g("titel",4); typ_v = g("typ",5) or "termin"; notiz_v = g("notiz",6) or ""
        ort_v = g("ort",7) or ""; ansprechpartner_v = g("ansprechpartner",8) or ""; telefon_v = g("telefon",9) or ""
        typ_opts = "".join(
            f'<option value="{v}"{" selected" if v==typ_v else ""}>{l}</option>' for v, l in TERMIN_TYPEN)
        content = f"""
        <h1 class="page-title">Termin bearbeiten – {rcode}</h1>
        <div class="card" style="max-width:500px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/termin/{tid}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Titel <span class="required">*</span></label>
                  <input type="text" name="titel" value="{titel_v}" required>
                </div>
                <div class="form-group">
                  <label>Typ</label>
                  <select name="typ">{typ_opts}</select>
                </div>
                <div class="form-group">
                  <label>Datum <span class="required">*</span></label>
                  <input type="date" name="datum" value="{datum_v}" required>
                </div>
                <div class="form-group">
                  <label>Uhrzeit von</label>
                  <input type="time" name="uhrzeit_von" value="{von_v}">
                </div>
                <div class="form-group">
                  <label>Uhrzeit bis</label>
                  <input type="time" name="uhrzeit_bis" value="{bis_v}">
                </div>
                <div class="form-group full">
                  <label>Ort / Adresse</label>
                  <input type="text" name="ort" value="{ort_v}">
                </div>
                <div class="form-group">
                  <label>Ansprechpartner</label>
                  <input type="text" name="ansprechpartner" value="{ansprechpartner_v}">
                </div>
                <div class="form-group">
                  <label>Telefon</label>
                  <input type="tel" name="telefon" value="{telefon_v}">
                </div>
                <div class="form-group full">
                  <label>Notiz</label>
                  <input type="text" name="notiz" value="{notiz_v}">
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Termin – {rcode}", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/termin/{tid}/bearbeiten")
async def termin_bearbeiten(code: str, tid: int, request: Request):
    rcode = code.upper()
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    typ = (form.get("typ") or "termin").strip()
    datum = (form.get("datum") or "").strip()
    von = (form.get("uhrzeit_von") or "").strip() or None
    bis = (form.get("uhrzeit_bis") or "").strip() or None
    ort = (form.get("ort") or "").strip() or None
    ansprechpartner = (form.get("ansprechpartner") or "").strip() or None
    telefon = (form.get("telefon") or "").strip() or None
    notiz = (form.get("notiz") or "").strip() or None
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"UPDATE termine SET titel={P},typ={P},datum={P},uhrzeit_von={P},uhrzeit_bis={P},"
            f"ort={P},ansprechpartner={P},telefon={P},notiz={P} WHERE id={P}",
            (titel, typ, datum, von, bis, ort, ansprechpartner, telefon, notiz, tid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/termin/{tid}/loeschen")
def termin_loeschen(code: str, tid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"DELETE FROM termine WHERE id={P}", (tid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── VMA-Tabelle Übersicht ──────────────────────────────────────────────────────
@app.post("/vma-saetze/import")
def vma_saetze_import():
    result = importiere_aktuelle_saetze()
    if result.get("fehler"):
        msg = f"fehler={result['fehler']}"
    else:
        msg = f"ok=1&laender={result['laender']}&staedte={result['staedte']}&ab={result.get('gueltig_ab','')}"
    return RedirectResponse(f"/vma?{msg}", status_code=303)

@app.get("/vma-saetze/info/{land_code}")
def vma_saetze_info(land_code: str):
    """JSON: Standard-Satz + Städte-Sonderfälle für ein Land (für das Land-Formular)."""
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        standard = vma_fuer_land_erweitert(cur, land_code, None)
        cur.execute(f"SELECT ort, land_name, vma_voll, vma_halb FROM vma_saetze "
                    f"WHERE land_code={P} AND ort IS NOT NULL ORDER BY ort", (land_code.upper(),))
        rows = cur.fetchall()
        cur.close(); db.close()
        staedte = [{"ort": (r[0] if isinstance(r, tuple) else r["ort"]),
                    "name": (r[1] if isinstance(r, tuple) else r["land_name"]),
                    "voll": float(r[2] if isinstance(r, tuple) else r["vma_voll"]),
                    "halb": float(r[3] if isinstance(r, tuple) else r["vma_halb"])} for r in rows]
        return JSONResponse({"standard": standard, "staedte": staedte})
    except Exception as e:
        return JSONResponse({"standard": None, "staedte": [], "fehler": str(e)})

@app.get("/vma", response_class=HTMLResponse)
def vma_uebersicht(ok: str = "", fehler: str = "", laender: str = "", staedte: str = "", ab: str = ""):
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT land_code, ort, land_name, vma_voll, vma_halb, gueltig_ab, aktualisiert
                       FROM vma_saetze ORDER BY land_name, ort""")
        rows = cur.fetchall()
        cur.close(); db.close()
    except Exception:
        rows = []

    def g(r, key, idx):
        return r[key] if hasattr(r, "keys") else r[idx]

    importiert_am = None
    laender_map: dict[str, dict] = {}
    for r in rows:
        lc = g(r,"land_code",0); ort = g(r,"ort",1); ln = g(r,"land_name",2)
        voll = float(g(r,"vma_voll",3)); halb = float(g(r,"vma_halb",4))
        importiert_am = g(r,"aktualisiert",6) or importiert_am
        eintrag = laender_map.setdefault(lc, {"name": None, "voll": None, "halb": None, "staedte": []})
        if ort:
            eintrag["staedte"].append({"name": ln, "voll": voll, "halb": halb})
        else:
            eintrag["name"] = ln; eintrag["voll"] = voll; eintrag["halb"] = halb

    zeilen = ""
    if laender_map:
        # Importierte Sätze aus der DB anzeigen
        for lc, info in sorted(laender_map.items(), key=lambda x: x[1]["name"] or x[0]):
            if info["voll"] is None:
                continue
            zeilen += f"""<tr>
                <td class="td-mono">{lc}</td>
                <td>🌍 {info["name"]}</td>
                <td style="text-align:right;font-weight:600">{info["voll"]:.2f} EUR</td>
                <td style="text-align:right">{info["halb"]:.2f} EUR</td>
            </tr>"""
            for st in sorted(info["staedte"], key=lambda s: s["name"]):
                zeilen += f"""<tr style="background:#fafbfe">
                    <td class="td-mono" style="color:var(--muted)">↳</td>
                    <td style="padding-left:24px">📍 {st["name"]}</td>
                    <td style="text-align:right;font-weight:600">{st["voll"]:.2f} EUR</td>
                    <td style="text-align:right">{st["halb"]:.2f} EUR</td>
                </tr>"""
    else:
        # Fallback: fest hinterlegte Sätze im Code
        for code, info in sorted(VMA_SAETZE.items(), key=lambda x: x[1]["name"]):
            zeilen += f"""<tr>
                <td class="td-mono">{code}</td>
                <td>🌍 {info["name"]}</td>
                <td style="text-align:right;font-weight:600">{info["voll"]:.2f} EUR</td>
                <td style="text-align:right">{info["halb"]:.2f} EUR</td>
            </tr>"""

    banner = ""
    if ok:
        banner = (f'<div class="alert alert-ok" style="margin-bottom:16px">'
                  f'✓ Import erfolgreich: {laender} Länder, {staedte} Städte-Sonderfälle '
                  f'(gültig ab {ab}).</div>')
    elif fehler:
        banner = f'<div class="alert alert-err" style="margin-bottom:16px">Import fehlgeschlagen: {fehler}</div>'
    elif not laender_map:
        banner = ('<div class="alert alert-warn" style="margin-bottom:20px">'
                   'Es wurden noch keine aktuellen Sätze importiert – unten stehen die im Code '
                   'hinterlegten Werte (Stand 2024/2026, ohne Städte-Sonderfälle).</div>')

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h1 class="page-title" style="margin:0">VMA-Tagessätze</h1>
      <form method="post" action="/vma-saetze/import">
        <button type="submit" class="btn btn-primary">🔄 Aktuelle Sätze importieren</button>
      </form>
    </div>
    {banner}
    <div class="card">
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>ISO</th><th>Land / Ort</th>
            <th style="text-align:right">Voller Satz/Tag</th>
            <th style="text-align:right">Halber Satz/Tag</th>
          </tr></thead>
          <tbody>{zeilen}</tbody>
        </table>
      </div>
    </div>
    <div class="alert alert-ok" style="margin-top:16px">
      <b>Regel:</b> Erster und letzter Reisetag → halber Satz. Volle Tage dazwischen → voller Satz.
      Bei Aufenthalt in mehreren Ländern gilt der Satz des Landes, in dem der Reisende
      um 24:00 Uhr Ortszeit war. Städte-Sonderfälle (z.B. Los Angeles) werden beim
      "Land hinzufügen" in der Reise automatisch zur Auswahl angeboten, sobald importiert.
      Quelle: <a href="https://github.com/david-loe/pauschbetrag-api" target="_blank" style="color:var(--muted)">pauschbetrag-api</a> (offizielle BMF-Daten).
    </div>"""
    return HTMLResponse(shell("VMA-Sätze", content, "vma"))


# ── Reisenden-Portal (Selbstbedienung, ohne Login, per Token) ─────────────────
def portal_shell(titel: str, content: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{titel}</title><style>{CSS}</style></head>
<body>
<main style="max-width:640px;margin:24px auto;padding:0 12px">
  <div style="text-align:center;margin-bottom:20px">
    <img src="/static/logo3.png" alt="Herrhammer" style="height:36px">
  </div>
  {content}
</main>
</body></html>"""


@app.get("/portal/{token}", response_class=HTMLResponse)
def portal_ansicht(token: str):
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig",
            '<div class="card"><div class="card-body">'
            '<p>Dieser Link ist ungültig oder abgelaufen. Bitte wende dich an dein Büro.</p>'
            '</div></div>'), status_code=404)

    tage_sicherstellen(info["reise_code"], info["kuerzel"])
    tage = tage_laden(info["reise_code"], info["kuerzel"])

    wochentage = ["Mo","Di","Mi","Do","Fr","Sa","So"]
    g = lambda r, k, i: r[k] if hasattr(r, "keys") else r[i]

    zeilen = ""
    for t in tage:
        tid = g(t,"id",0); vd = g(t,"datum",1)
        if isinstance(vd, str): vd = date.fromisoformat(vd[:10])
        lname = g(t,"land_name",3) or "Deutschland"; lcode = g(t,"land_code",2) or "DE"
        ist_halb = bool(g(t,"ist_halber_satz",6))
        frueh = bool(g(t,"fruehstueck",7)); mittag = bool(g(t,"mittagessen",8)); abend = bool(g(t,"abendessen",9))
        reise_beginn = g(t,"reise_beginn",11) or ""; reise_ende = g(t,"reise_ende",12) or ""
        arbeit_beginn = g(t,"arbeit_beginn",13) or ""; arbeit_ende = g(t,"arbeit_ende",14) or ""
        notiz = g(t,"notiz",15) or ""

        wt = wochentage[vd.weekday()]
        halb_txt = ' <span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 7px;border-radius:10px">½ Satz</span>' if ist_halb else ""

        def cb(name, checked, label):
            ch = "checked" if checked else ""
            return (f'<label style="display:inline-flex;align-items:center;gap:4px;font-size:13px;'
                    f'margin-right:14px;cursor:pointer"><input type="checkbox" name="{name}" value="1" '
                    f'{ch} style="width:auto"> {label}</label>')

        zeilen += f"""<div class="card" style="margin-bottom:12px">
          <div class="card-body">
            <div style="margin-bottom:10px">
              <b>{wt} {vd.day:02d}.{vd.month:02d}.{vd.year}</b>{halb_txt}
              <div style="font-size:12px;color:var(--muted)">🌍 {lname} ({lcode})</div>
            </div>
            <form method="post" action="/portal/{token}/tag/{tid}">
              <div style="margin-bottom:10px">{cb("fruehstueck",frueh,"🍳 Frühstück")}{cb("mittagessen",mittag,"🍽 Mittagessen")}{cb("abendessen",abend,"🌙 Abendessen")}</div>
              <div class="form-grid form-grid-2">
                <div class="form-group"><label style="font-size:12px">Reisebeginn (Uhrzeit)</label>
                  <input type="time" name="reise_beginn" value="{reise_beginn}"></div>
                <div class="form-group"><label style="font-size:12px">Reiseende (Uhrzeit)</label>
                  <input type="time" name="reise_ende" value="{reise_ende}"></div>
                <div class="form-group"><label style="font-size:12px">Arbeitsbeginn</label>
                  <input type="time" name="arbeit_beginn" value="{arbeit_beginn}"></div>
                <div class="form-group"><label style="font-size:12px">Arbeitsende</label>
                  <input type="time" name="arbeit_ende" value="{arbeit_ende}"></div>
                <div class="form-group full"><label style="font-size:12px">Notiz (optional)</label>
                  <input type="text" name="notiz" value="{notiz}" placeholder="z.B. Verspätung, Besonderheiten"></div>
              </div>
              <button type="submit" class="btn btn-primary" style="width:100%;margin-top:6px">Speichern</button>
            </form>
          </div>
        </div>"""

    cur_km = get_db(); ccur = cur_km.cursor(); Pk = ph()
    ccur.execute(f"""SELECT id, datum, kennzeichen, km FROM kilometer_fahrten
        WHERE reise_code={Pk} AND kuerzel={Pk} ORDER BY datum""",
        (info["reise_code"], info["kuerzel"]))
    km_rows = ccur.fetchall(); ccur.close(); cur_km.close()
    km_zeilen = "".join(
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'padding:6px 0;border-bottom:1px solid var(--border);font-size:13px">'
        f'<span>{g(k,"kennzeichen",2)} · {g(k,"km",3):.0f} km</span>'
        f'<a href="/portal/{token}/kilometer/{g(k,"id",0)}/loeschen" '
        f'style="color:#ef4444;text-decoration:none;font-size:12px" '
        f'onclick="return confirm(\'Eintrag löschen?\')">🗑</a></div>'
        for k in km_rows)
    km_html = f"""<div class="card" style="margin-bottom:16px">
      <div class="card-header"><span class="card-title">🚗 Gefahrene Kilometer (Firmen-/Privat-Pkw)</span></div>
      <div class="card-body">
        <p style="font-size:12px;color:var(--muted);margin:0 0 10px 0">
          Nur eintragen, wenn du mit einem Firmen- oder deinem privaten Pkw gefahren bist
          (bei Mietwagen steht das schon auf der Mietwagenrechnung, hier nicht nötig).</p>
        {km_zeilen}
        <form method="post" action="/portal/{token}/kilometer/neu" style="margin-top:10px;display:flex;gap:6px;flex-wrap:wrap;align-items:end">
          <div class="form-group" style="margin:0"><label style="font-size:12px">Kennzeichen</label>
            <input type="text" name="kennzeichen" placeholder="z.B. WÜ-AB 123" required style="width:120px"></div>
          <div class="form-group" style="margin:0"><label style="font-size:12px">Kilometer</label>
            <input type="number" step="1" name="km" placeholder="z.B. 85" required style="width:90px"></div>
          <button type="submit" class="btn btn-secondary">+ Hinzufügen</button>
        </form>
      </div>
    </div>"""

    bewirtung_html = _bw_portal_karte(token, info)
    vorschuss_html = _vs_portal_karte(token, info)
    ab_txt = fmt_date(info["abreise"]); zu_txt = fmt_date(info["rueckkehr"])
    content = f"""
    <div class="card" style="margin-bottom:16px">
      <div class="card-body">
        <h1 class="page-title" style="margin:0 0 4px 0">Hallo {info['klarname']} 👋</h1>
        <p style="font-size:13px;color:var(--muted);margin:0">
          Reise <b>{info['reise_code']}</b> – {info['titel']}<br>
          📅 {ab_txt} – {zu_txt}
        </p>
        <p style="font-size:13px;margin-top:10px">
          Bitte trage für jeden Reisetag ein, welche Mahlzeiten gestellt wurden und
          deine tatsächlichen Reise-/Arbeitszeiten ein. Du kannst die Angaben jederzeit
          über diesen Link wieder ändern.</p>
        <a href="/portal/{token}/beleg-hochladen" class="btn btn-primary"
           style="width:100%;display:block;text-align:center;margin-top:12px">📎 Beleg hochladen</a>
        <a href="/portal/{token}/anleitung" class="btn btn-secondary"
           style="width:100%;display:block;text-align:center;margin-top:8px">📖 Anleitung</a>
      </div>
    </div>
    {km_html}
    {bewirtung_html}
    {vorschuss_html}
    {zeilen}
    """
    return HTMLResponse(portal_shell(f"Reise {info['reise_code']} – {info['klarname']}", content))


@app.post("/portal/{token}/tag/{tag_id}")
async def portal_tag_speichern(token: str, tag_id: int, request: Request):
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    form = await request.form()
    try:
        tag_speichern(
            tag_id,
            bool(form.get("fruehstueck")), bool(form.get("mittagessen")), bool(form.get("abendessen")),
            (form.get("reise_beginn") or "").strip(), (form.get("reise_ende") or "").strip(),
            (form.get("arbeit_beginn") or "").strip(), (form.get("arbeit_ende") or "").strip(),
            (form.get("notiz") or "").strip(),
        )
    except Exception as e:
        # Ein Reisender soll bei einem Server-Fehler nie einen nackten
        # 502/500 sehen, sondern eine verständliche Meldung – ein Absturz
        # hier darf außerdem nicht dazu führen, dass die Eingabe kommentarlos
        # verloren geht, ohne dass jemand davon erfährt.
        return HTMLResponse(portal_shell("Fehler beim Speichern",
            '<p>Deine Eingabe konnte leider nicht gespeichert werden. '
            'Bitte versuch es gleich nochmal – klappt es weiterhin nicht, '
            'bitte kurz beim Organisator melden.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{e}</p>'
            f'<a href="/portal/{token}" class="btn btn-secondary">Zurück</a>'), status_code=500)
    return RedirectResponse(f"/portal/{token}", status_code=303)


@app.post("/portal/{token}/kilometer/neu")
async def portal_kilometer_hinzufuegen(token: str, request: Request):
    """Trägt eine Fahrt mit Firmen-/Privat-Pkw ein (Kennzeichen, km) – kein
    Datum nötig, das ist für diese Angabe nicht relevant. Nur bei Mietwagen
    nicht nötig, da steht die Info schon auf der Mietwagenrechnung."""
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    form = await request.form()
    kennzeichen = (form.get("kennzeichen") or "").strip()
    km = (form.get("km") or "").strip()
    if not (kennzeichen and km):
        return RedirectResponse(f"/portal/{token}", status_code=303)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        # datum-Spalte ist NOT NULL, wird dem Reisenden aber nicht mehr
        # angezeigt/abgefragt -> heutiges Datum als unsichtbarer Platzhalter.
        cur.execute(f"""INSERT INTO kilometer_fahrten (reise_code, kuerzel, datum, kennzeichen, km)
                        VALUES ({P},{P},{P},{P},{P})""",
                    (info["reise_code"], info["kuerzel"], date.today().isoformat(), kennzeichen, float(km)))
        db.commit(); cur.close(); db.close()
    except Exception:
        pass  # kein harter Fehler bei einem Zusatzfeld – Reisender soll einfach weitermachen können
    return RedirectResponse(f"/portal/{token}", status_code=303)


@app.get("/portal/{token}/kilometer/{kid}/loeschen")
def portal_kilometer_loeschen(token: str, kid: int):
    """Löscht einen Kilometer-Eintrag – nur, wenn er tatsächlich zu DIESEM
    Token (Reise+Person) gehört, damit über einen fremden Link nicht die
    Einträge einer anderen Person gelöscht werden können."""
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""DELETE FROM kilometer_fahrten
                        WHERE id={P} AND reise_code={P} AND kuerzel={P}""",
                    (kid, info["reise_code"], info["kuerzel"]))
        db.commit(); cur.close(); db.close()
    except Exception:
        pass
    return RedirectResponse(f"/portal/{token}", status_code=303)


@app.get("/portal/{token}/beleg-hochladen", response_class=HTMLResponse)
def portal_beleg_form(token: str):
    """Formular, über das der Reisende selbst Belege zu seiner Reise
    hochladen kann – landet automatisch als ungeprüfter Beleg im
    Organisator-Dashboard, muss nicht mehr per Mail weitergeleitet werden."""
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    content = f"""
    <div class="card">
      <div class="card-body">
        <h1 class="page-title" style="margin:0 0 4px 0">📎 Beleg hochladen</h1>
        <p style="font-size:13px;color:var(--muted);margin:0 0 16px 0">
          Für Reise <b>{info['reise_code']}</b> – {info['titel']}. Der Beleg wird automatisch
          ausgewertet und erscheint danach beim Organisator zur Prüfung.</p>
        <form method="post" action="/portal/{token}/beleg-hochladen" enctype="multipart/form-data">
          <input type="file" name="datei" accept=".pdf,.jpg,.jpeg,.png,.tif,.tiff,.heic,.webp" required
                 style="display:block;margin-bottom:12px">
          <button type="submit" class="btn btn-primary" style="width:100%">Hochladen</button>
        </form>
        <a href="/portal/{token}" class="btn btn-secondary" style="width:100%;margin-top:10px;text-align:center;display:block">← Zurück</a>
      </div>
    </div>"""
    return HTMLResponse(portal_shell("Beleg hochladen", content))


@app.post("/portal/{token}/beleg-hochladen")
async def portal_beleg_upload(token: str, datei: UploadFile = File(...)):
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    try:
        datei_bytes = await datei.read()
        ct = datei.content_type or "application/octet-stream"
        # WICHTIG: reise_code kommt aus dem Token, NICHT aus einem Formularfeld
        # -> der Reisende kann so ausschließlich Belege zu SEINER EIGENEN
        # Reise hochladen, keine Möglichkeit, versehentlich oder absichtlich
        # eine andere reise_code einzuschleusen.
        result = await beleg_verarbeiten(datei_bytes, datei.filename or "upload", info["reise_code"], ct)
        if result.get("duplikat"):
            meldung = "Dieser Beleg wurde bereits hochgeladen (Duplikat erkannt), kein neuer Eintrag nötig."
        else:
            meldung = "Danke! Der Beleg wurde hochgeladen und wartet jetzt auf die Prüfung durch den Organisator."
        content = f"""<div class="card"><div class="card-body">
          <div class="alert alert-ok" style="margin-bottom:16px">{meldung}</div>
          <a href="/portal/{token}/beleg-hochladen" class="btn btn-secondary" style="width:100%;display:block;text-align:center;margin-bottom:8px">Weiteren Beleg hochladen</a>
          <a href="/portal/{token}" class="btn btn-primary" style="width:100%;display:block;text-align:center">← Zurück zur Übersicht</a>
        </div></div>"""
        return HTMLResponse(portal_shell("Beleg hochgeladen", content))
    except Exception as e:
        return HTMLResponse(portal_shell("Fehler beim Hochladen",
            '<div class="card"><div class="card-body">'
            '<p>Der Beleg konnte leider nicht verarbeitet werden. Bitte versuch es nochmal '
            'oder melde dich beim Organisator.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{e}</p>'
            f'<a href="/portal/{token}/beleg-hochladen" class="btn btn-secondary">Zurück</a>'
            '</div></div>'), status_code=500)


@app.get("/cron/portal-mails")
def cron_portal_mails_route(key: str = ""):
    if not CRON_SECRET or key != CRON_SECRET:
        return JSONResponse({"fehler": "Ungültiger oder fehlender Schlüssel"}, status_code=403)
    result = cron_portal_mails()
    return JSONResponse(result)


@app.get("/cron/mails")
async def cron_mails_route(key: str = ""):
    """Holt neue Belege aus dem Reisekosten-Postfach (gedacht für einen Aufruf alle 5 Minuten).
    Gleiche Verarbeitung wie der Knopf „Mails abrufen“. Bei Fehlern (z. B. IMAP nicht erreichbar)
    antwortet die Adresse mit HTTP 500, damit der Cron-Dienst es meldet."""
    if not CRON_SECRET or key != CRON_SECRET:
        return JSONResponse({"fehler": "Ungültiger oder fehlender Schlüssel"}, status_code=403)
    try:
        result = await _mail_abruf_ausfuehren()
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)
    if result is None:
        return JSONResponse({"status": "läuft noch", "hinweis": "Der vorige Abruf ist noch nicht fertig."})
    if "fehler" in result and "importiert" not in result:
        return JSONResponse(result, status_code=500)
    return JSONResponse(result)


@app.get("/cron/flug-alerts")
def cron_flug_alerts_route(key: str = ""):
    """
    Für einen externen Cron-Pinger gedacht, der diese URL regelmäßig aufruft
    (idealerweise minütlich – die Funktion selbst entscheidet anhand der
    Konfiguration, welche Segmente gerade wirklich geprüft werden müssen).
    """
    if not CRON_SECRET or key != CRON_SECRET:
        return JSONResponse({"fehler": "Ungültiger oder fehlender Schlüssel"}, status_code=403)
    result = cron_flug_alerts()
    try:   # Outlook-Einladungen im selben Takt abgleichen (keine zusätzliche Cron-Einrichtung nötig)
        result["kalender"] = kal.kalender_abgleich()
    except Exception as e:
        result["kalender"] = {"fehler": str(e)}
    return JSONResponse(result)


@app.get("/cron/kalender")
def cron_kalender_route(key: str = ""):
    """Outlook-Einladungen abgleichen (läuft ohnehin im Flug-Alert-Cron mit; hier auch einzeln aufrufbar)."""
    if not CRON_SECRET or key != CRON_SECRET:
        return JSONResponse({"fehler": "Ungültiger oder fehlender Schlüssel"}, status_code=403)
    try:
        return JSONResponse(kal.kalender_abgleich())
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


def _kalender_seite(rcode: str, ergebnis: dict | None = None) -> str:
    from html import escape as e
    ue = kal.kalender_uebersicht(rcode)
    if not ue:
        return shell("Fehler", '<div class="alert alert-err">Reise nicht gefunden.</div>', "reisen")
    reise = ue["reise"]
    tn = ""
    for t in ue["teilnehmer"]:
        if t["haken"]:
            st = '<span class="badge badge-green">✔ aktiv</span>'
        elif not t["email"]:
            st = '<span class="badge badge-gray">keine E-Mail im Profil</span>'
        else:
            st = '<span class="badge badge-gray">nicht aktiviert</span>'
        tn += ('<tr><td>%s</td><td style="font-size:12px;color:var(--muted)">%s</td><td>%s</td>'
               '<td><a href="/mitarbeiter/%s/bearbeiten" style="font-size:12px">Profil</a></td></tr>'
               % (e(t["klarname"]), e(t["email"] or "–"), st, e(t["kuerzel"])))
    zl = ""
    for z in ue["zeilen"]:
        fehler = ('<div style="color:#b91c1c;font-size:11px">⚠ %s</div>' % e(z["fehler"])) if z["fehler"] else ""
        zl += ('<tr><td>%s</td><td>%s<div style="font-size:11px;color:var(--muted)">%s</div></td>'
               '<td>%s%s</td><td style="font-size:12px;color:var(--muted)">%s</td></tr>'
               % (e(z["name"]), e(z["titel"]), e(z["start"]), e(z["status"]), fehler, e(z["gesendet_am"] or "–")))
    banner = ""
    if ergebnis is not None:
        teile = ["Neu gesendet: %d" % ergebnis["gesendet"], "Aktualisiert: %d" % ergebnis["aktualisiert"],
                 "Abgesagt: %d" % ergebnis["abgesagt"]]
        klasse = "alert-ok" if not ergebnis["fehler"] else "alert-err"
        fehl = "".join("<div>⚠ %s</div>" % e(f) for f in ergebnis["fehler"])
        banner = '<div class="alert %s" style="margin-bottom:16px">%s%s</div>' % (klasse, " · ".join(teile), fehl)
    probleme = "".join("<div>• %s</div>" % e(p) for p in ue["probleme"])
    probleme = ('<div class="alert alert-warn" style="margin-bottom:16px"><b>Nicht berücksichtigt:</b>%s</div>' % probleme) if probleme else ""
    content = (
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">'
        '<div><h1 class="page-title" style="margin:0">📅 Outlook-Einladungen %s</h1>'
        '<div style="color:var(--muted);font-size:13px">%s</div></div>'
        '<a href="/reise/%s/reiseplan" class="btn btn-secondary">← Reiseplan</a></div>'
        '%s%s'
        '<div class="card" style="margin-bottom:16px"><div class="card-header"><span class="card-title">Teilnehmer</span></div>'
        '<div class="table-wrap"><table><thead><tr><th>Name</th><th>E-Mail</th><th>Kalender-Einladungen</th><th></th></tr></thead>'
        '<tbody>%s</tbody></table></div></div>'
        '<div class="card"><div class="card-header"><span class="card-title">Termine</span>'
        '<form method="post" action="/reise/%s/kalender/senden" style="margin:0">'
        '<button type="submit" class="btn btn-primary btn-sm">Jetzt senden / aktualisieren</button></form></div>'
        '<div class="table-wrap"><table><thead><tr><th>Reisender</th><th>Termin</th><th>Status</th><th>Zuletzt gesendet</th></tr></thead>'
        '<tbody>%s</tbody></table></div></div>'
        '<p style="font-size:12px;color:var(--muted);margin-top:12px">Neue und geänderte Termine gehen automatisch '
        'ca. 2–4 Minuten nach der letzten Änderung raus; „Jetzt senden“ erledigt es sofort. Entfallene Flüge/Züge/Termine '
        'werden in Outlook abgesagt. Eingeladen wird an E-Mail-Adresse 1 des Profils.</p>'
        % (e(reise["code"]), e(reise["titel"]), e(reise["code"]), banner, probleme, tn, e(reise["code"]),
           zl or '<tr><td colspan="4"><div class="empty-state">Keine Termine – oder bei niemandem ist die Einladung aktiviert.</div></td></tr>'))
    return shell("Outlook-Einladungen %s" % rcode, content, "reisen")


@app.get("/reise/{code}/kalender", response_class=HTMLResponse)
def reise_kalender_anzeigen(code: str, request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    return HTMLResponse(_kalender_seite(code.upper()))


@app.post("/reise/{code}/kalender/senden", response_class=HTMLResponse)
def reise_kalender_senden(code: str, request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    try:
        erg = kal.kalender_abgleich(code.upper(), sofort=True)
    except Exception as ex:
        return HTMLResponse(shell("Fehler", '<div class="alert alert-err">%s</div>' % ex, "reisen"))
    return HTMLResponse(_kalender_seite(code.upper(), erg))


def _mail_diagnose_seite(ergebnis: dict | None = None) -> str:
    from html import escape as e
    inhalt = ""
    if ergebnis:
        icon = {"ok": "✔", "fehler": "✖", "warn": "⚠", "info": "ℹ"}
        farbe = {"ok": "#166534", "fehler": "#b91c1c", "warn": "#92400e", "info": "var(--muted)"}
        zeilen = "".join('<tr><td style="color:%s;width:28px;font-weight:700">%s</td><td>%s</td></tr>'
                         % (farbe[st], icon[st], e(tx)) for st, tx in ergebnis["zeilen"])
        fazit = "".join("<div>%s</div>" % e(t) for t in ergebnis["fazit"])
        inhalt = ('<div class="card" style="margin-bottom:16px"><div class="table-wrap"><table><tbody>%s</tbody></table></div></div>'
                  '%s' % (zeilen, ('<div class="alert alert-info"><b>Ergebnis:</b> %s</div>' % fazit) if fazit else ""))
    content = (
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">'
        '<h1 class="page-title" style="margin:0">🔧 Mail-Diagnose</h1>'
        '<a href="/einstellungen" class="btn btn-secondary">← Einstellungen</a></div>'
        '<p style="font-size:13px;color:var(--muted)">Prüft den Mail-Postausgang Schritt für Schritt. Es wird keine Mail gesendet, '
        'die Prüfung dauert bis zu einer Minute.</p>'
        '<form method="post" action="/einstellungen/mail-diagnose" style="margin-bottom:16px">'
        '<button type="submit" class="btn btn-primary">Diagnose starten</button></form>%s' % inhalt)
    return shell("Mail-Diagnose", content, "einstellungen")


@app.get("/einstellungen/mail-diagnose", response_class=HTMLResponse)
def mail_diagnose_anzeigen(request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    return HTMLResponse(_mail_diagnose_seite())


@app.post("/einstellungen/mail-diagnose", response_class=HTMLResponse)
def mail_diagnose_starten(request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren.</div>'), status_code=403)
    try:
        erg = smtp_diagnose()
    except Exception as ex:
        erg = {"zeilen": [("fehler", "Diagnose abgebrochen: %s" % ex)], "fazit": []}
    return HTMLResponse(_mail_diagnose_seite(erg))


# ── Dokumentation (Handbuch) ──────────────────────────────────────────────────
# Inhalt und Prüfungen stehen in mod_doku.py (einzige Quelle). Organisatoren sehen alle Teile,
# andere Angemeldete und das Portal nur Teil A (Reisende).
def _doku_kontext() -> dict:
    try:
        from zoneinfo import ZoneInfo
        jetzt = datetime.now(ZoneInfo("Europe/Berlin"))
    except Exception:
        jetzt = datetime.now()
    return mod_doku.kontext_bauen(APP_VERSION, VERSIONEN, jetzt.strftime("%d.%m.%Y %H:%M"))


def _doku_pdf_antwort(teile: str):
    from fastapi.responses import Response
    t = mod_doku.teile_normalisieren(teile)
    daten = mod_doku.pdf_erzeugen(t, _doku_kontext(), live=(t != "A"))
    name = ("Reisekosten-Anleitung-Reisende" if t == "A" else "Reisekosten-Handbuch") + "_v%s.pdf" % APP_VERSION
    return Response(content=daten, media_type="application/pdf",
                    headers={"Content-Disposition": 'attachment; filename="%s"' % name})


@app.get("/einstellungen/dokumentation", response_class=HTMLResponse)
def dokumentation_seite(request: Request):
    org = ist_organisator(request)
    teile = "ABC" if org else "A"
    hinweis = ""
    if org:
        if mod_doku.DOKU_GEPRUEFT_FUER != APP_VERSION:
            hinweis += ('<div class="alert alert-warn" style="margin-bottom:12px">Das Handbuch wurde zuletzt für Version %s geprüft, '
                        'die App hat Version %s. Es kann Änderungen enthalten, die noch nicht beschrieben sind.</div>'
                        % (mod_doku.DOKU_GEPRUEFT_FUER, APP_VERSION))
        fehlt = mod_doku.fehlende_pflicht_einstellungen()
        if fehlt:
            hinweis += ('<div class="alert alert-warn" style="margin-bottom:12px">Fehlende Pflicht-Einstellungen: <b>%s</b> '
                        '(siehe Kapitel C2).</div>' % ", ".join(fehlt))
        if "SESSION_SECRET" in fehlt:
            hinweis += ('<div class="alert alert-warn" style="margin-bottom:12px"><b>SESSION_SECRET:</b> Die Anmeldung ist durch einen '
                        'abgeleiteten Ersatzschlüssel geschützt. Bitte trotzdem in Render unter Environment eintragen '
                        '(lange Zufallsfolge, mindestens 32 Zeichen). Danach müssen sich alle einmal neu anmelden.</div>')
    knoepfe = ('<a href="/einstellungen/dokumentation/pdf" class="btn btn-primary">📄 PDF herunterladen</a>')
    if org:
        knoepfe += (' <a href="/einstellungen/dokumentation/pdf?teil=A" class="btn btn-secondary">'
                    '📄 Nur Reisenden-Anleitung (PDF)</a>'
                    ' <a href="/einstellungen" class="btn btn-secondary">← Einstellungen</a>')
    content = ('<div class="nodruck" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px">'
               '<h1 class="page-title" style="margin:0">📖 Dokumentation</h1>%s</div>%s%s'
               % (knoepfe, hinweis, mod_doku.html_inhalt(teile, _doku_kontext(), live=org)))
    return HTMLResponse(shell("Dokumentation", content, "einstellungen"))


@app.get("/einstellungen/dokumentation/pdf")
def dokumentation_pdf(request: Request, teil: str = ""):
    teile = mod_doku.teile_normalisieren(teil or "ABC") if ist_organisator(request) else "A"
    return _doku_pdf_antwort(teile)


@app.get("/portal/{token}/anleitung", response_class=HTMLResponse)
def portal_anleitung(token: str):
    info = zugang_aus_token(token)
    if not info:
        return HTMLResponse(portal_shell("Link ungültig",
            '<div class="card"><div class="card-body">'
            '<p>Dieser Link ist ungültig oder abgelaufen. Bitte wende dich an dein Büro.</p>'
            '</div></div>'), status_code=404)
    content = ('<div class="nodruck" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">'
               '<a href="/portal/%s" class="btn btn-secondary">← Zurück</a>'
               '<a href="/portal/%s/anleitung/pdf" class="btn btn-primary">📄 PDF</a></div>%s'
               % (token, token, mod_doku.html_inhalt("A", _doku_kontext(), live=False)))
    return HTMLResponse(portal_shell("Anleitung", content))


@app.get("/portal/{token}/anleitung/pdf")
def portal_anleitung_pdf(token: str):
    if not zugang_aus_token(token):
        return HTMLResponse(portal_shell("Link ungültig", '<p>Ungültiger Link.</p>'), status_code=404)
    return _doku_pdf_antwort("A")


@app.get("/cron/backup")
def cron_backup_route(key: str = ""):
    """
    Für einen externen Cron-Pinger gedacht, der diese URL EINMAL TÄGLICH
    aufruft (z.B. nachts). Sichert alle Datenbank-Tabellen komprimiert zu S3 –
    notwendig, weil der kostenlose Render-Tarif keine eigenen automatischen
    Backups anbietet. Behält automatisch nur die letzten 30 Sicherungen.
    """
    if not CRON_SECRET or key != CRON_SECRET:
        return JSONResponse({"fehler": "Ungültiger oder fehlender Schlüssel"}, status_code=403)
    from mod_backup import backup_erstellen
    try:
        result = backup_erstellen()
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


# ── Bewirtungsbeleg (Eigenbeleg mit elektronischer Bestätigung) ───────────────
# Reisende füllen den Beleg über ihren Portal-Link aus (Anlass, Teilnehmer, Betrag),
# ordnen ihn immer einem Originalbeleg zu (auch ein selbst erstellter Eigenbeleg ist als
# Original möglich) und bestätigen die Richtigkeit elektronisch. Danach ist er gesperrt; Änderungen
# nur als neue Version. Fachlogik steht in mod_bewirtung.py.

_BW_WAEHRUNGEN = list(vs.WAEHRUNGEN)      # eine gemeinsame Liste (mod_vorschuss.py)

_BW_CSS = """<style>
.bw-tn-row{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:6px}
.bw-box{border:1px solid var(--border);border-radius:8px;padding:10px;margin-top:8px;background:#fafbfc}
.bw-opt{display:block;padding:8px 0;font-size:14px;font-weight:400;cursor:pointer;line-height:1.4}
.bw-opt input{width:auto;margin-right:8px}
.bw-sig{border:1px dashed #94a3b8;border-radius:8px;background:#fff;width:100%;touch-action:none;display:block}
.bw-dl{display:grid;grid-template-columns:150px 1fr;gap:4px 12px;font-size:13px;margin:0}
.bw-dl dt{color:var(--muted)}
.bw-dl dd{margin:0}
</style>"""

_BW_FORM_JS = """<script>
(function(){
  var form=document.getElementById('bwform'); if(!form) return;
  function umschalten(){
    var r=form.querySelector('input[name=quelle]:checked'); var q=r?r.value:'';
    ['beleg','upload'].forEach(function(k){
      var el=document.getElementById('bwq_'+k); if(el) el.style.display=(k===q)?'block':'none';
    });
  }
  function wert(id){var f=document.getElementById(id); return f?(f.value||''):'';}
  function datum_de(v){var p=v.split('-'); return p.length===3?p[2]+'.'+p[1]+'.'+p[0]:'';}
  function vorschau(){
    var el=document.getElementById('bw_vorschau'); if(!el) return;
    var t=[datum_de(wert('bw_datum')), wert('bw_ort'), wert('bw_betrag')?(wert('bw_betrag')+' '+wert('bw_waehrung')):''].filter(function(x){return x;});
    el.textContent=t.length?t.join(' · '):'Wird aus dem Beleg übernommen.';
  }
  ['bw_datum','bw_ort','bw_betrag','bw_waehrung'].forEach(function(id){
    var f=document.getElementById(id); if(f){f.addEventListener('input',vorschau); f.addEventListener('change',vorschau);}
  });
  form.querySelectorAll('input[name=quelle]').forEach(function(r){r.addEventListener('change',umschalten);});
  umschalten(); vorschau();
  var sel=document.getElementById('bw_hauptbeleg');
  if(sel){ sel.addEventListener('change',function(){
    var o=sel.options[sel.selectedIndex]; if(!o||!o.value) return;
    function fill(id,v){var f=document.getElementById(id); if(f && v) f.value=v;}
    fill('bw_datum',o.dataset.datum); fill('bw_ort',o.dataset.ort); fill('bw_betrag',o.dataset.betrag);
    var w=document.getElementById('bw_waehrung'); var cur=o.dataset.waehrung;
    if(w && cur){
      var da=false; for(var i=0;i<w.options.length;i++){ if(w.options[i].value===cur){da=true;break;} }
      if(!da){ var op=document.createElement('option'); op.value=cur; op.textContent=cur; w.insertBefore(op,w.firstChild); }
      w.value=cur;
    }
    vorschau();
  }); }
  var add=document.getElementById('bw_tn_add');
  if(add){ add.addEventListener('click',function(){
    var box=document.getElementById('bw_tn'); var n=box.children.length+1;
    var row=document.createElement('div'); row.className='bw-tn-row';
    row.innerHTML='<input type="text" name="tn_name" placeholder="Name des Gastes" maxlength="120">'
      +'<input type="text" name="tn_firma" placeholder="Firma (optional)" maxlength="120">';
    box.appendChild(row);
  }); }
})();
</script>"""

_BW_SIG_JS = """<script>
(function(){
  var c=document.getElementById('bw_sig'); if(!c) return;
  var ctx=c.getContext('2d'); var zeichnet=false, hat=false;
  ctx.lineWidth=2.4; ctx.lineCap='round'; ctx.lineJoin='round'; ctx.strokeStyle='#0f172a';
  function pos(ev){var r=c.getBoundingClientRect();
    return {x:(ev.clientX-r.left)*(c.width/r.width), y:(ev.clientY-r.top)*(c.height/r.height)};}
  c.addEventListener('pointerdown',function(ev){
    ev.preventDefault(); zeichnet=true; var p=pos(ev); ctx.beginPath(); ctx.moveTo(p.x,p.y);
    try{c.setPointerCapture(ev.pointerId);}catch(e){}
  });
  c.addEventListener('pointermove',function(ev){
    if(!zeichnet) return; ev.preventDefault(); var p=pos(ev); ctx.lineTo(p.x,p.y); ctx.stroke(); hat=true;
  });
  function ende(){zeichnet=false;}
  c.addEventListener('pointerup',ende); c.addEventListener('pointercancel',ende); c.addEventListener('pointerleave',ende);
  document.getElementById('bw_sig_clear').addEventListener('click',function(){
    ctx.clearRect(0,0,c.width,c.height); hat=false; document.getElementById('bw_sig_data').value='';
  });
  document.getElementById('bw_best_form').addEventListener('submit',function(){
    if(hat){document.getElementById('bw_sig_data').value=c.toDataURL('image/png');}
  });
})();
</script>"""


def _bw_e(x) -> str:
    from html import escape
    return escape("" if x is None else str(x), quote=True)


def _bw_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def _bw_betrag_txt(x, w="EUR") -> str:
    return bw.betrag_text(x, w)


def _bw_status_badge(d: dict) -> str:
    if d["status"] == "bestaetigt":
        farbe, text = "#dcfce7:#166534", "✓ bestätigt"
    elif d["status"] == "ersetzt":
        farbe, text = "#e2e8f0:#475569", "ersetzt"
    else:
        farbe, text = "#fef3c7:#92400e", "Entwurf"
    bg, fg = farbe.split(":")
    return (f'<span style="font-size:11px;background:{bg};color:{fg};padding:2px 8px;'
            f'border-radius:10px;white-space:nowrap">{text}</span>')


def _bw_ungueltig():
    return HTMLResponse(portal_shell("Link ungültig",
        '<div class="card"><div class="card-body"><p>Dieser Link ist ungültig oder abgelaufen. '
        'Bitte wende dich an dein Büro.</p></div></div>'), status_code=404)


def _bw_nicht_gefunden(token: str):
    return HTMLResponse(portal_shell("Nicht gefunden",
        '<div class="card"><div class="card-body"><p>Dieser Bewirtungsbeleg wurde nicht gefunden.</p>'
        f'<a href="/portal/{token}" class="btn btn-secondary">← Zurück</a></div></div>'), status_code=404)


def _bw_eigene(db, info: dict, bid: int):
    """Bewirtung laden – nur wenn sie zu Reise UND Person des Tokens gehört."""
    d = bw.laden(db, bid)
    if not d or d["reise_code"] != info["reise_code"] or d["kuerzel"] != info["kuerzel"]:
        return None
    return d


def _bw_portal_karte(token: str, info: dict) -> str:
    """Karte auf der Portal-Startseite: eigene Bewirtungsbelege + Button zum Anlegen."""
    zeilen = ""
    try:
        db = get_db()
        try:
            liste = bw.liste_fuer(db, info["reise_code"], info["kuerzel"])
        finally:
            db.close()
    except Exception:
        return ""          # Tabelle fehlt noch (vor /init) -> Karte einfach nicht anzeigen
    for d in liste:
        ziel = (f"/portal/{token}/bewirtung/{d['id']}/bearbeiten" if d["status"] == "entwurf"
                else f"/portal/{token}/bewirtung/{d['id']}")
        v = f" · Version {d['version']}" if d["version"] > 1 else ""
        zeilen += (f'<a href="{ziel}" style="display:flex;justify-content:space-between;align-items:center;'
                   f'gap:8px;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;'
                   f'color:inherit;text-decoration:none"><span><b>{_bw_e(d["nummer"])}</b>{v}<br>'
                   f'<span style="color:var(--muted);font-size:12px">{bw.datum_de(d["datum"])} · '
                   f'{_bw_e(d["ort"] or "–")}</span></span>{_bw_status_badge(d)}</a>')
    return f"""<div class="card" style="margin-bottom:16px">
      <div class="card-header"><span class="card-title">🍽 Bewirtungsbelege</span></div>
      <div class="card-body">
        <p style="font-size:12px;color:var(--muted);margin:0 0 10px 0">
          Wenn du Gäste (z. B. Kunden) eingeladen hast: Anlass und Teilnehmer hier eintragen und
          bestätigen. Die Rechnung bzw. der Kassenbon bleibt der Originalbeleg. Eigene Mahlzeiten
          auf der Reise gehören <b>nicht</b> hierher.</p>
        {zeilen}
        <a href="/portal/{token}/bewirtung/neu" class="btn btn-secondary"
           style="width:100%;display:block;text-align:center;margin-top:10px">+ Bewirtungsbeleg erstellen</a>
      </div>
    </div>"""


def _bw_form_seite(token: str, info: dict, d: dict, fehler: list, bid: int | None,
                   quelle: str, belege: list, warnung: str = "") -> str:
    """Formular (neu/bearbeiten). `d` enthält die Werte, die angezeigt werden sollen."""
    opts = '<option value="">– Beleg wählen –</option>'
    for b in belege:
        sel = " selected" if str(b["id"]) == str(d.get("hauptbeleg_id") or "") else ""
        label = (f'#{b["id"]} · {b["anbieter"] or "–"} · {bw.datum_de(b["datum"])} · '
                 f'{_bw_betrag_txt(b["betrag"], b["waehrung"])}')
        betrag_attr = "" if b["betrag"] is None else f'{b["betrag"]:.2f}'.replace(".", ",")
        opts += (f'<option value="{b["id"]}"{sel} data-datum="{_bw_e(b["datum"] or "")}" '
                 f'data-ort="{_bw_e(b["anbieter"] or "")}" data-betrag="{betrag_attr}" '
                 f'data-waehrung="{_bw_e(b["waehrung"])}">{_bw_e(label)}</option>')

    waehr = list(_BW_WAEHRUNGEN)
    if d.get("waehrung") and d["waehrung"] not in waehr:
        waehr.insert(0, d["waehrung"])
    waehr_opts = "".join(f'<option value="{w}"{" selected" if w == (d.get("waehrung") or "EUR") else ""}>{w}</option>'
                         for w in waehr)

    tn = list(d.get("teilnehmer") or [])
    if not tn:
        tn = [{"name": info["klarname"], "firma": ""}]
    while len(tn) < 2:
        tn.append({"name": "", "firma": ""})
    tn_html = "".join(
        f'<div class="bw-tn-row"><input type="text" name="tn_name" value="{_bw_e(p.get("name"))}" '
        f'placeholder="{"Dein Name" if i == 1 else "Name des Gastes"}" maxlength="120">'
        f'<input type="text" name="tn_firma" value="{_bw_e(p.get("firma"))}" '
        f'placeholder="Firma (optional)" maxlength="120"></div>'
        for i, p in enumerate(tn, start=1))
    # Tag/Ort/Betrag kommen aus dem Originalbeleg und bleiben eingeklappt; nur bei Fehlern offen
    daten_offen = any(("Datum" in f or "Ort" in f or "Betrag" in f) for f in fehler)
    daten_offen_attr = " open" if daten_offen else ""

    fehler_html = ""
    if fehler:
        fehler_html = ('<div class="alert alert-err" style="margin-bottom:14px"><b>Bitte prüfen:</b><ul '
                       'style="margin:6px 0 0 18px;padding:0">'
                       + "".join(f"<li>{_bw_e(f)}</li>" for f in fehler) + "</ul></div>")
    warn_html = f'<div class="alert alert-warn" style="margin-bottom:14px">{warnung}</div>' if warnung else ""

    def chk(k):
        return " checked" if quelle == k else ""

    betrag_wert = "" if d.get("betrag") is None else f'{float(d["betrag"]):.2f}'.replace(".", ",")
    teil_wert = "" if d.get("teilbetrag") is None else f'{float(d["teilbetrag"]):.2f}'.replace(".", ",")
    titel = "Bewirtungsbeleg bearbeiten" if bid else "Neuer Bewirtungsbeleg"
    version_hinweis = (f' <span style="font-size:12px;color:var(--muted)">(Korrektur, Version {d["version"]})</span>'
                       if d.get("version", 1) > 1 else "")
    nummer_txt = f' {_bw_e(d["nummer"])}' if d.get("nummer") else ""
    bid_feld = f'<input type="hidden" name="bid" value="{bid}">' if bid else ""
    loeschen = ""
    if bid:
        loeschen = (f'<form method="post" action="/portal/{token}/bewirtung/{bid}/loeschen" '
                    f'onsubmit="return confirm(\'Entwurf wirklich löschen?\')" style="margin-top:8px">'
                    f'<button type="submit" class="btn btn-secondary" style="width:100%;color:#b91c1c">'
                    f'🗑 Entwurf löschen</button></form>')

    return f"""{_BW_CSS}
    <div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 4px 0">🍽 {titel}{nummer_txt}{version_hinweis}</h1>
      <p style="font-size:13px;color:var(--muted);margin:0 0 14px 0">
        Reise <b>{_bw_e(info["reise_code"])}</b> – {_bw_e(info["titel"])}</p>
      {fehler_html}{warn_html}
      <form method="post" action="/portal/{token}/bewirtung/speichern" enctype="multipart/form-data" id="bwform">
        {bid_feld}
        <h3 style="font-size:14px;margin:0 0 4px 0">1. Originalbeleg der Bewirtung</h3>
        <p style="font-size:12px;color:var(--muted);margin:0 0 6px 0">Jede Bewirtung braucht einen Originalbeleg
          (Rechnung, Kassenbon oder selbst erstellter Eigenbeleg). Betrag, Trinkgeld und Wechselkurs stehen dort.</p>
        <label class="bw-opt"><input type="radio" name="quelle" value="beleg"{chk("beleg")}>
          Zu einem Beleg, der schon hochgeladen ist</label>
        <div id="bwq_beleg" class="bw-box">
          <select name="hauptbeleg_id" id="bw_hauptbeleg" style="width:100%">{opts}</select>
        </div>
        <label class="bw-opt"><input type="radio" name="quelle" value="upload"{chk("upload")}>
          Beleg (Rechnung, Kassenbon oder Eigenbeleg) jetzt hochladen</label>
        <div id="bwq_upload" class="bw-box">
          <input type="file" name="datei" accept=".pdf,.jpg,.jpeg,.png,.tif,.tiff,.heic,.webp">
          <p style="font-size:12px;color:var(--muted);margin:6px 0 0 0">Das Hochladen kann einen Moment dauern.</p>
        </div>

        <div style="margin-top:10px;padding:10px;background:#f1f5f9;border-radius:8px;font-size:13px">
          <b>Aus dem Beleg:</b> <span id="bw_vorschau">Wird aus dem Beleg übernommen.</span>
          <details id="bw_details"{daten_offen_attr} style="margin-top:6px">
            <summary style="cursor:pointer;font-size:12px;color:var(--muted)">Tag, Ort oder Betrag ändern</summary>
            <div class="form-grid form-grid-2" style="margin-top:8px">
              <div class="form-group"><label style="font-size:12px">Tag der Bewirtung</label>
                <input type="date" name="datum" id="bw_datum" value="{_bw_e(d.get("datum"))}"></div>
              <div class="form-group"><label style="font-size:12px">Ort / Restaurant</label>
                <input type="text" name="ort" id="bw_ort" value="{_bw_e(d.get("ort"))}" maxlength="160"
                       placeholder="z. B. Restaurant Adler, Würzburg"></div>
              <div class="form-group"><label style="font-size:12px">Betrag</label>
                <input type="text" inputmode="decimal" name="betrag" id="bw_betrag" value="{betrag_wert}"
                       placeholder="z. B. 86,40"></div>
              <div class="form-group"><label style="font-size:12px">Währung</label>
                <select name="waehrung" id="bw_waehrung">{waehr_opts}</select></div>
            </div>
          </details>
        </div>
        <div class="form-group" style="margin-top:10px">
          <label style="font-size:12px">Davon Bewirtung – nur ausfüllen, wenn die Rechnung mehr als die Bewirtung enthält
            (z. B. Hotelrechnung mit Übernachtung)</label>
          <input type="text" inputmode="decimal" name="teilbetrag" id="bw_teilbetrag" value="{teil_wert}"
                 placeholder="z. B. 70,00 (leer = ganze Rechnung ist Bewirtung)" style="max-width:320px">
        </div>
        <p style="font-size:12px;color:var(--muted);margin:6px 0 0 0">Bei Rechnungen ab 250 € muss auf der Rechnung der Name
          des Bewirtenden (unsere Firma) stehen. Bitte vor dem Bezahlen prüfen lassen.</p>

        <h3 style="font-size:14px;margin:18px 0 8px 0">2. Anlass der Bewirtung</h3>
        <input type="text" name="anlass" list="bw_anlaesse" value="{_bw_e(d.get("anlass"))}" maxlength="300"
               style="width:100%" placeholder="z. B. Projektbesprechung mit Firma Muster" autocomplete="off">
        <datalist id="bw_anlaesse">
          <option value="Projektbesprechung mit Kunde">
          <option value="Vertragsverhandlung mit Kunde">
          <option value="Kundenpflege / Geschäftsessen">
          <option value="Besprechung mit Lieferant">
          <option value="Teamessen mit Gästen">
        </datalist>

        <h3 style="font-size:14px;margin:18px 0 4px 0">3. Teilnehmer</h3>
        <p style="font-size:12px;color:var(--muted);margin:0 0 8px 0">
          Du bist schon eingetragen. Trage mindestens einen Gast ein, die Firma ist optional.</p>
        <div id="bw_tn">{tn_html}</div>
        <button type="button" id="bw_tn_add" class="btn btn-secondary btn-sm">+ Teilnehmer</button>

        <button type="submit" name="aktion" value="weiter" class="btn btn-primary"
                style="width:100%;margin-top:18px">Weiter zur Bestätigung →</button>
        <button type="submit" name="aktion" value="entwurf" class="btn btn-secondary"
                style="width:100%;margin-top:8px">Als Entwurf speichern</button>
      </form>
      {loeschen}
      <a href="/portal/{token}" class="btn btn-secondary"
         style="width:100%;margin-top:8px;text-align:center;display:block">← Zurück zur Übersicht</a>
    </div></div>
    {_BW_FORM_JS}"""


def _bw_form_aus_request(form, info: dict) -> dict:
    """Formularwerte in ein Bewirtungs-Dict übersetzen."""
    quelle = (form.get("quelle") or "beleg")
    if quelle not in ("beleg", "upload"):
        quelle = "beleg"          # alte Seite mit der früheren Option 'Eigenbeleg' -> Originalbeleg wählen
    hb = None
    try:
        hb = int(form.get("hauptbeleg_id") or 0) or None
    except ValueError:
        hb = None
    datum = (form.get("datum") or "").strip()
    try:
        datum = date.fromisoformat(datum).isoformat() if datum else None
    except ValueError:
        datum = None
    waehrung = (form.get("waehrung") or "EUR").strip().upper()[:3] or "EUR"
    return {
        "reise_code": info["reise_code"], "kuerzel": info["kuerzel"],
        "quelle": quelle, "hauptbeleg_id": hb if quelle in ("beleg", "upload") else None,
        "eigenbeleg": False, "eigenbeleg_begruendung": "",
        "datum": datum, "ort": (form.get("ort") or "").strip(),
        "anlass": (form.get("anlass") or "").strip(),
        "teilnehmer": bw.teilnehmer_aus_formular(form.getlist("tn_name"), form.getlist("tn_firma")),
        "betrag": bw.zahl_parsen(form.get("betrag")), "trinkgeld": None,
        "waehrung": waehrung,
        "teilbetrag": bw.zahl_parsen(form.get("teilbetrag")),
        # Text eingegeben, aber keine gültige Zahl (z. B. "abc" oder "-5") -> wird bei „Weiter“ als Fehler gemeldet
        "teilbetrag_ungueltig": bool((form.get("teilbetrag") or "").strip()) and bw.zahl_parsen(form.get("teilbetrag")) is None,
    }


@app.get("/portal/{token}/bewirtung/neu", response_class=HTMLResponse)
def portal_bewirtung_neu(token: str, beleg: int = 0):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        belege = bw.belege_zur_auswahl(db, info["reise_code"], info["klarname"], None, beleg or None)
    finally:
        db.close()
    d = {"hauptbeleg_id": beleg or None, "teilnehmer": [], "waehrung": "EUR", "version": 1}
    for b in belege:
        if b["id"] == beleg:
            d.update({"datum": b["datum"], "ort": b["anbieter"] or "", "betrag": b["betrag"],
                      "waehrung": b["waehrung"]})
    quelle = "beleg" if belege else "upload"
    return HTMLResponse(portal_shell("Bewirtungsbeleg",
                        _bw_form_seite(token, info, d, [], None, quelle, belege)))


@app.get("/portal/{token}/bewirtung/{bid}/bearbeiten", response_class=HTMLResponse)
def portal_bewirtung_bearbeiten(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] != "entwurf":
            return RedirectResponse(f"/portal/{token}/bewirtung/{bid}", status_code=303)
        belege = bw.belege_zur_auswahl(db, info["reise_code"], info["klarname"], d["nummer"], d["hauptbeleg_id"])
    finally:
        db.close()
    quelle = "beleg" if (d["hauptbeleg_id"] or belege) else "upload"
    return HTMLResponse(portal_shell(f"Bewirtungsbeleg {d['nummer']}",
                        _bw_form_seite(token, info, d, [], bid, quelle, belege)))


@app.post("/portal/{token}/bewirtung/speichern")
async def portal_bewirtung_speichern(token: str, request: Request):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    form = await request.form()
    try:
        bid = int(form.get("bid") or 0) or None
    except ValueError:
        bid = None
    aktion = form.get("aktion") or "weiter"
    d = _bw_form_aus_request(form, info)
    warnung = ""
    db = get_db()
    try:
        bestehend = None
        if bid:
            bestehend = _bw_eigene(db, info, bid)
            if not bestehend:
                return _bw_nicht_gefunden(token)
            if bestehend["status"] != "entwurf":
                return RedirectResponse(f"/portal/{token}/bewirtung/{bid}", status_code=303)
            d.update({"nummer": bestehend["nummer"], "version": bestehend["version"]})
        d.setdefault("version", 1)

        # Nur diese beiden Belege dürfen die Auswahlregeln (eigener Beleg, frei) umgehen:
        # der gerade hochgeladene und der bereits im eigenen Entwurf gespeicherte.
        ausnahme_id = (bestehend or {}).get("hauptbeleg_id")

        # Neuer Beleg im selben Schritt hochgeladen -> normale Beleg-Pipeline (reise_code aus dem Token!)
        datei = form.get("datei")
        if d["quelle"] == "upload" and datei is not None and getattr(datei, "filename", ""):
            try:
                inhalt = await datei.read()
                res = await beleg_verarbeiten(inhalt, datei.filename or "upload", info["reise_code"],
                                              datei.content_type or "application/octet-stream")
                neu_id = res.get("beleg_id")
                kurz = bw.beleg_kurz(db, neu_id) if neu_id else None
                if not kurz or kurz["reise_code"] != info["reise_code"]:
                    warnung = ("Der Beleg wurde schon bei einer anderen Reise erfasst und kann hier nicht "
                               "verwendet werden. Bitte beim Organisator melden.")
                else:
                    d["hauptbeleg_id"] = neu_id
                    ausnahme_id = neu_id
                    d["quelle"] = "beleg"
                    if not d["datum"] and kurz["datum"]:
                        d["datum"] = kurz["datum"]
                    if not d["ort"] and kurz["anbieter"]:
                        d["ort"] = kurz["anbieter"]
                    if d["betrag"] is None and kurz["betrag"] is not None:
                        d["betrag"] = kurz["betrag"]
                    d["waehrung"] = kurz["waehrung"] or d["waehrung"]
            except Exception as e:
                warnung = ("Der Beleg konnte nicht hochgeladen werden. Bitte noch einmal versuchen "
                           f"oder den Beleg über „Beleg hochladen“ senden. ({_bw_e(e)})")
        elif d["quelle"] == "upload" and not d["hauptbeleg_id"]:
            d["quelle"] = "beleg"

        # Gewählter Beleg muss zur Reise und zur Person passen und frei sein
        belege = bw.belege_zur_auswahl(db, info["reise_code"], info["klarname"],
                                       (bestehend or {}).get("nummer"), ausnahme_id)
        if d["hauptbeleg_id"] and d["hauptbeleg_id"] not in {b["id"] for b in belege}:
            d["hauptbeleg_id"] = None
            warnung = warnung or "Der gewählte Beleg ist nicht verfügbar. Bitte einen anderen wählen."
        if d["hauptbeleg_id"]:
            for b in belege:
                if b["id"] == d["hauptbeleg_id"]:
                    # Währung kommt immer aus dem Beleg; leere Felder werden aus dem Beleg ergänzt
                    if b["waehrung"]:
                        d["waehrung"] = b["waehrung"]
                    if not d["datum"] and b["datum"]:
                        d["datum"] = b["datum"]
                    if not d["ort"] and b["anbieter"]:
                        d["ort"] = b["anbieter"]
                    if d["betrag"] is None and b["betrag"] is not None:
                        d["betrag"] = b["betrag"]

        if aktion == "weiter":
            fehler = bw.validieren(d)
            if fehler or warnung:
                return HTMLResponse(portal_shell("Bewirtungsbeleg",
                    _bw_form_seite(token, info, d, fehler, bid, d["quelle"], belege, warnung)))
        new_id = bw.entwurf_speichern(db, d, bid)
        db.commit()
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        return HTMLResponse(portal_shell("Fehler beim Speichern",
            '<div class="card"><div class="card-body"><p>Der Bewirtungsbeleg konnte nicht gespeichert werden. '
            'Bitte versuch es gleich noch einmal oder melde dich beim Organisator.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{_bw_e(e)}</p>'
            f'<a href="/portal/{token}" class="btn btn-secondary">← Zurück</a></div></div>'), status_code=500)
    finally:
        db.close()
    if aktion == "weiter":
        return RedirectResponse(f"/portal/{token}/bewirtung/{new_id}/pruefen", status_code=303)
    return RedirectResponse(f"/portal/{token}", status_code=303)


def _bw_detail_html(d: dict, klarname: str, haupt: dict | None, integritaet: bool | None,
                    pdf_url: str, zeige_technik: bool = True) -> str:
    """Gemeinsame Anzeige (Portal und Organisator) für einen Bewirtungsbeleg."""
    tn = "".join(f'<li>{_bw_e(p.get("name"))}'
                 + (f' <span style="color:var(--muted)">({_bw_e(p.get("firma"))})</span>' if p.get("firma") else "")
                 + "</li>" for p in d["teilnehmer"])
    if d["eigenbeleg"]:
        beleg_txt = (f'<span style="color:#b45309"><b>Eigenbeleg ohne Originalbeleg (Ausnahme)</b></span><br>'
                     f'Begründung: {_bw_e(d["eigenbeleg_begruendung"])}')
    elif haupt:
        beleg_txt = (f'Beleg <b>#{haupt["id"]}</b> – {_bw_e(haupt["anbieter"] or "–")}, '
                     f'{bw.datum_de(haupt["datum"])}, {_bw_betrag_txt(haupt["betrag"], haupt["waehrung"])}')
    else:
        beleg_txt = "–"
    best = ""
    if d["status"] in ("bestaetigt", "ersetzt"):
        integ = ""
        if integritaet is True:
            integ = '<span style="color:#166534">✓ Prüfsumme stimmt – unverändert seit der Bestätigung</span>'
        elif integritaet is False:
            integ = '<span style="color:#b91c1c"><b>✖ Prüfsumme weicht ab – Daten wurden verändert!</b></span>'
        technik = ""
        if zeige_technik:
            technik = (f'<dt>IP-Adresse</dt><dd>{_bw_e(d["bestaetigt_ip"] or "–")}</dd>'
                       f'<dt>Prüfsumme</dt><dd style="font-family:monospace;font-size:11px;'
                       f'word-break:break-all">{_bw_e(d["pruefsumme"])}<br>{integ}</dd>')
        sig = ""
        if d.get("signatur_png"):
            sig = (f'<dt>Unterschrift</dt><dd><img alt="Unterschrift" style="max-width:240px;'
                   f'border:1px solid var(--border);border-radius:6px;background:#fff" '
                   f'src="data:image/png;base64,{_bw_e(d["signatur_png"])}"></dd>')
        best = (f'<h3 style="font-size:14px;margin:16px 0 8px 0">Bestätigung</h3><dl class="bw-dl">'
                f'<dt>Bestätigt von</dt><dd>{_bw_e(d["bestaetigt_name"])}</dd>'
                f'<dt>Zeitpunkt</dt><dd>{_bw_e(bw.lokal_text(d["bestaetigt_am"]))}</dd>{technik}{sig}</dl>')
    pdf_btn = (f'<a href="{pdf_url}" target="_blank" class="btn btn-secondary" '
               f'style="width:100%;display:block;text-align:center;margin-top:12px">📄 PDF ansehen</a>'
               if d.get("s3_pdf") else "")
    return f"""{_BW_CSS}
      <dl class="bw-dl">
        <dt>Nummer</dt><dd><b>{_bw_e(d["nummer"])}</b> · Version {d["version"]} {_bw_status_badge(d)}</dd>
        <dt>Bewirtender</dt><dd>{_bw_e(klarname)}</dd>
        <dt>Reise</dt><dd>{_bw_e(d["reise_code"])}</dd>
        <dt>Tag</dt><dd>{bw.datum_de(d["datum"])}</dd>
        <dt>Ort / Restaurant</dt><dd>{_bw_e(d["ort"])}</dd>
        <dt>Anlass</dt><dd>{_bw_e(d["anlass"])}</dd>
        <dt>Betrag</dt><dd><b>{_bw_betrag_txt(d["betrag"], d["waehrung"])}</b></dd>
        {('<dt>Davon Bewirtung</dt><dd><b>' + _bw_betrag_txt(d["teilbetrag"], d["waehrung"]) + '</b> '
          '<span style="color:var(--muted);font-size:12px">(der Rest der Rechnung ist keine Bewirtung)</span></dd>')
         if d.get("teilbetrag") is not None else ""}
        <dt>Zugehöriger Beleg</dt><dd>{beleg_txt}</dd>
        <dt>Teilnehmer</dt><dd><ol style="margin:0;padding-left:18px">{tn}</ol></dd>
      </dl>{best}{pdf_btn}"""


@app.get("/portal/{token}/bewirtung/{bid}/pruefen", response_class=HTMLResponse)
def portal_bewirtung_pruefen(token: str, bid: int, fehler: str = ""):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] != "entwurf":
            return RedirectResponse(f"/portal/{token}/bewirtung/{bid}", status_code=303)
        haupt = bw.beleg_kurz(db, d["hauptbeleg_id"]) if d["hauptbeleg_id"] else None
    finally:
        db.close()
    return HTMLResponse(portal_shell("Bewirtungsbeleg bestätigen",
                                     _bw_bestaetigen_seite(token, info, d, haupt, fehler)))


def _bw_bestaetigen_seite(token: str, info: dict, d: dict, haupt: dict | None, fehler: str = "",
                          name_wert: str = "") -> str:
    probleme = bw.validieren(d)
    fehler_html = (f'<div class="alert alert-err" style="margin-bottom:14px">{_bw_e(fehler)}</div>' if fehler else "")
    if probleme:
        form_html = ('<div class="alert alert-err"><b>Es fehlen noch Angaben:</b><ul style="margin:6px 0 0 18px">'
                     + "".join(f"<li>{_bw_e(p)}</li>" for p in probleme) + "</ul></div>"
                     f'<a href="/portal/{token}/bewirtung/{d["id"]}/bearbeiten" class="btn btn-primary" '
                     f'style="width:100%;display:block;text-align:center">Angaben ergänzen</a>')
    else:
        form_html = f"""
      <form method="post" action="/portal/{token}/bewirtung/{d["id"]}/bestaetigen" id="bw_best_form">
        <h3 style="font-size:14px;margin:18px 0 8px 0">Bestätigung</h3>
        <label class="bw-opt"><input type="checkbox" name="haken" value="1">
          Ich bestätige, dass die Angaben vollständig und richtig sind und die Bewirtung
          geschäftlich veranlasst war.</label>
        <div class="form-group" style="margin-top:8px"><label style="font-size:12px">
          Vollständiger Name als Bestätigung <span class="required">*</span></label>
          <input type="text" name="name_eingabe" value="{_bw_e(name_wert or info["klarname"])}" autocomplete="off"
                 placeholder="{_bw_e(info["klarname"])}"></div>
        <div class="form-group"><label style="font-size:12px">Unterschrift (optional, mit Finger oder Maus)</label>
          <canvas id="bw_sig" class="bw-sig" width="600" height="200"></canvas>
          <button type="button" id="bw_sig_clear" class="btn btn-secondary btn-sm"
                  style="margin:6px 0 4px 0;width:auto">Unterschrift löschen</button>
          <input type="hidden" name="signatur" id="bw_sig_data"></div>
        <p style="font-size:12px;color:var(--muted);margin:12px 0">
          Mit „Verbindlich bestätigen“ wird der Beleg gesperrt. Zeitpunkt, Name und eine Prüfsumme werden
          gespeichert. Spätere Änderungen sind nur als neue Version möglich; die alte Version bleibt erhalten.</p>
        <button type="submit" class="btn btn-success" style="width:100%">✓ Verbindlich bestätigen</button>
        <a href="/portal/{token}/bewirtung/{d["id"]}/bearbeiten" class="btn btn-secondary"
           style="width:100%;display:block;text-align:center;margin-top:8px">← Angaben ändern</a>
      </form>{_BW_SIG_JS}"""
    return f"""<div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 12px 0">🍽 Bewirtungsbeleg prüfen und bestätigen</h1>
      {fehler_html}{_bw_hinweise(d)}
      {_bw_detail_html(d, info["klarname"], haupt, None, "", False)}
      {form_html}
    </div></div>"""


@app.post("/portal/{token}/bewirtung/{bid}/bestaetigen")
async def portal_bewirtung_bestaetigen(token: str, bid: int, request: Request):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    form = await request.form()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] != "entwurf":
            return RedirectResponse(f"/portal/{token}/bewirtung/{bid}", status_code=303)
        name_eingabe = (form.get("name_eingabe") or "").strip()
        try:
            bw.bestaetigen(db, bid, info["klarname"], name_eingabe, bool(form.get("haken")),
                           form.get("signatur") or None, _bw_client_ip(request),
                           request.headers.get("user-agent", ""))
            db.commit()
        except ValueError as e:
            db.rollback()
            haupt = bw.beleg_kurz(db, d["hauptbeleg_id"]) if d["hauptbeleg_id"] else None
            return HTMLResponse(portal_shell("Bewirtungsbeleg bestätigen",
                _bw_bestaetigen_seite(token, info, d, haupt, str(e), name_eingabe)), status_code=400)
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        return HTMLResponse(portal_shell("Fehler",
            '<div class="card"><div class="card-body"><p>Die Bestätigung konnte nicht gespeichert werden. '
            'Es wurde nichts verändert. Bitte versuch es gleich noch einmal oder melde dich beim '
            'Organisator.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{_bw_e(e)}</p>'
            f'<a href="/portal/{token}/bewirtung/{bid}/pruefen" class="btn btn-secondary">← Zurück</a>'
            '</div></div>'), status_code=500)
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}/bewirtung/{bid}?ok=1", status_code=303)


@app.get("/portal/{token}/bewirtung/{bid}", response_class=HTMLResponse)
def portal_bewirtung_ansicht(token: str, bid: int, ok: int = 0):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] == "entwurf":
            return RedirectResponse(f"/portal/{token}/bewirtung/{bid}/bearbeiten", status_code=303)
        haupt = bw.beleg_kurz(db, d["hauptbeleg_id"]) if d["hauptbeleg_id"] else None
        alle = bw.versionen(db, d["nummer"])
    finally:
        db.close()
    banner = ""
    if ok:
        banner = ('<div class="alert alert-ok" style="margin-bottom:14px">✓ Bestätigt und gespeichert. '
                  'Der Beleg liegt jetzt beim Organisator zur Prüfung.</div>')
    versionen_html = ""
    if len(alle) > 1:
        versionen_html = '<h3 style="font-size:14px;margin:16px 0 6px 0">Versionen</h3>'
        for v in alle:
            ziel = f"/portal/{token}/bewirtung/{v['id']}"
            versionen_html += (f'<div style="font-size:13px;padding:4px 0"><a href="{ziel}">Version {v["version"]}</a> · '
                               f'{_bw_status_badge(v)} · {_bw_e(bw.lokal_text(v["bestaetigt_am"]))}</div>')
    korrektur = ""
    if d["status"] == "bestaetigt":
        korrektur = (f'<form method="post" action="/portal/{token}/bewirtung/{bid}/korrektur" '
                     f'onsubmit="return confirm(\'Eine Korrektur erzeugt eine neue Version, die erneut '
                     f'bestätigt werden muss. Fortfahren?\')" style="margin-top:10px">'
                     f'<button type="submit" class="btn btn-secondary" style="width:100%">'
                     f'✏ Korrigieren (neue Version)</button></form>')
    elif d["status"] == "ersetzt":
        banner += ('<div class="alert alert-warn" style="margin-bottom:14px">Diese Version wurde durch eine '
                   'neuere ersetzt und bleibt nur zur Nachvollziehbarkeit erhalten.</div>')
    content = f"""<div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 12px 0">🍽 Bewirtungsbeleg</h1>
      {banner}
      {_bw_detail_html(d, info["klarname"], haupt, bw.integritaet_ok(d), f"/portal/{token}/bewirtung/{bid}/pdf", False)}
      {korrektur}{versionen_html}
      <a href="/portal/{token}" class="btn btn-primary"
         style="width:100%;display:block;text-align:center;margin-top:12px">← Zurück zur Übersicht</a>
    </div></div>"""
    return HTMLResponse(portal_shell(f"Bewirtungsbeleg {d['nummer']}", content))


@app.post("/portal/{token}/bewirtung/{bid}/korrektur")
def portal_bewirtung_korrektur(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        try:
            neu_id = bw.korrektur_starten(db, bid)
            db.commit()
        except ValueError:
            db.rollback()
            # Es gibt schon einen offenen Korrektur-Entwurf -> dorthin weiterleiten
            cur = db.cursor()
            cur.execute(f"SELECT id FROM bewirtungen WHERE ersetzt_id={ph()} AND status='entwurf'", (bid,))
            r = cur.fetchone(); cur.close()
            neu_id = (r[0] if isinstance(r, tuple) else r["id"]) if r else None
            if not neu_id:
                return RedirectResponse(f"/portal/{token}/bewirtung/{bid}", status_code=303)
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}/bewirtung/{neu_id}/bearbeiten", status_code=303)


@app.post("/portal/{token}/bewirtung/{bid}/loeschen")
def portal_bewirtung_loeschen(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
        if d and d["status"] == "entwurf":
            bw.entwurf_loeschen(db, bid)
            db.commit()
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}", status_code=303)


@app.get("/portal/{token}/bewirtung/{bid}/pdf")
def portal_bewirtung_pdf(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _bw_eigene(db, info, bid)
    finally:
        db.close()
    if not d or not d.get("s3_pdf"):
        return _bw_nicht_gefunden(token)
    return _bw_pdf_antwort(d)


def _bw_pdf_antwort(d: dict):
    from fastapi.responses import Response
    try:
        inhalt = bw.pdf_laden(d)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">PDF nicht verfügbar: {_bw_e(e)}</div>'),
                            status_code=500)
    return Response(content=inhalt, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="Bewirtungsbeleg_{d["nummer"]}_v{d["version"]}.pdf"'})


# ── Bewirtungsbeleg: Organisator-Ansicht ──────────────────────────────────────
def _bewirtung_karte(bid: int) -> str:
    """Karte auf der Beleg-Detailseite: zeigt die Bewirtung, wenn der Beleg ihr Nebenbeleg
    (Eigenbeleg) oder ihr Rechnungsbeleg ist. Fehler (z. B. Tabelle fehlt) -> keine Karte."""
    try:
        db = get_db()
        try:
            liste = bw.zu_beleg(db, bid)
        finally:
            db.close()
    except Exception:
        return ""
    if not liste:
        return ""
    zeilen = ""
    for d in liste:
        rolle = "Eigenbeleg dieser Bewirtung" if d["beleg_id"] == bid else "Rechnungsbeleg dieser Bewirtung"
        zeilen += (f'<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:13px">'
                   f'<a href="/bewirtung/{d["id"]}" style="color:#2563eb"><b>{_bw_e(d["nummer"])}</b></a> '
                   f'· Version {d["version"]} {_bw_status_badge(d)}<br>'
                   f'<span style="color:var(--muted);font-size:12px">{rolle} · {bw.datum_de(d["datum"])} · '
                   f'{_bw_e(d["ort"] or "–")} · {len(d["teilnehmer"])} Teilnehmer</span></div>')
    return f"""<div class="card">
        <div class="card-header"><span class="card-title">🍽 Bewirtungsbeleg</span></div>
        <div class="card-body">{zeilen}</div>
      </div>"""


def _bewirtung_sperre(bid: int) -> str:
    """Leer, wenn der Beleg frei ist. Sonst eine Meldung: Belege einer Bewirtung (Eigenbeleg oder
    Rechnungsbeleg) werden nicht gelöscht/verschoben, damit der Nachweis vollständig bleibt."""
    try:
        db = get_db()
        try:
            liste = bw.zu_beleg(db, bid)
        finally:
            db.close()
    except Exception:
        return ""
    if not liste:
        return ""
    nummern = ", ".join(sorted({d["nummer"] for d in liste}))
    return (f"Dieser Beleg gehört zum Bewirtungsnachweis {nummern} und kann deshalb nicht gelöscht oder "
            f"einer anderen Reise zugeordnet werden. Zuerst die Bewirtung klären (Entwurf löschen bzw. "
            f"Korrektur über den Reisenden).")


def _bewirtung_vermerk_zusatz(bid: int) -> str:
    """Querverweis für das Prüf-Deckblatt: Rechnungsbeleg -> Bewirtungsbeleg und umgekehrt.
    Nur bestätigte Bewirtungsbelege. Fehler (z. B. Tabelle fehlt) -> kein Zusatz."""
    try:
        db = get_db()
        try:
            liste = [d for d in bw.zu_beleg(db, bid) if d["status"] == "bestaetigt"]
        finally:
            db.close()
    except Exception:
        return ""
    teile = []
    for d in liste:
        stand = f"Version {d['version']}, elektronisch bestätigt am {bw.lokal_text(d['bestaetigt_am'])}"
        if d["beleg_id"] == bid:
            if d["hauptbeleg_id"]:
                teile.append(f"Eigenbeleg {d['nummer']} ({stand}) zur Rechnung/Quittung Beleg #{d['hauptbeleg_id']}")
            else:
                teile.append(f"Eigenbeleg {d['nummer']} ({stand}), ohne Originalbeleg")
        else:
            teil_txt = (f", davon Bewirtung {d['teilbetrag']:.2f} {d['waehrung'] or 'EUR'}"
                        if d.get("teilbetrag") is not None else "")
            teile.append(f"Dazu gehört der Bewirtungsbeleg {d['nummer']} ({stand}){teil_txt}")
    return "; ".join(teile)


def _bw_hinweise(d: dict) -> str:
    """Weiche Hinweise (sperren nichts): 250-€-Regel und zeitnahe Erstellung."""
    out = ""
    try:
        tage = (date.today() - date.fromisoformat(str(d.get("datum"))[:10])).days
    except Exception:
        tage = None
    if tage is not None and tage > 14:
        out += (f'<div class="alert alert-warn" style="margin-bottom:12px;font-size:13px">Die Bewirtung liegt {tage} Tage zurück. '
                'Ein Bewirtungsbeleg soll zeitnah erstellt werden. Bitte nur bestätigen, wenn die Angaben sicher stimmen.</div>')
    betrag = d.get("betrag")
    if betrag is not None and (d.get("waehrung") or "EUR") == "EUR" and float(betrag) >= 250:
        out += ('<div class="alert alert-warn" style="margin-bottom:12px;font-size:13px"><b>Rechnung ab 250 €:</b> '
                'Auf der Rechnung muss der Name des Bewirtenden (unsere Firma) stehen, sonst ist der Abzug gefährdet. '
                'Bitte die Rechnung prüfen.</div>')
    return out


@app.get("/bewirtung/{bid}", response_class=HTMLResponse)
def bewirtung_organisator(request: Request, bid: int):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren haben Zugriff.</div>'), status_code=403)
    db = get_db()
    try:
        d = bw.laden(db, bid)
        if not d:
            return HTMLResponse(shell("Nicht gefunden",
                '<div class="alert alert-err">Bewirtungsbeleg nicht gefunden.</div>'), status_code=404)
        haupt = bw.beleg_kurz(db, d["hauptbeleg_id"]) if d["hauptbeleg_id"] else None
        alle = bw.versionen(db, d["nummer"])
        cur = db.cursor()
        cur.execute(f"SELECT klarname FROM mitarbeiter WHERE kuerzel={ph()}", (d["kuerzel"],))
        r = cur.fetchone(); cur.close()
        klarname = (r[0] if isinstance(r, tuple) else r["klarname"]) if r else d["kuerzel"]
    finally:
        db.close()
    links = f'<a href="/reise/{_bw_e(d["reise_code"])}" class="btn btn-secondary">🧳 Reise {_bw_e(d["reise_code"])}</a> '
    if d["beleg_id"]:
        links += f'<a href="/beleg/{d["beleg_id"]}" class="btn btn-secondary">📎 Eigenbeleg #{d["beleg_id"]}</a> '
    if haupt:
        links += f'<a href="/beleg/{haupt["id"]}" class="btn btn-secondary">🧾 Rechnungsbeleg #{haupt["id"]}</a>'
    warn = ""
    if d["eigenbeleg"]:
        warn = ('<div class="alert alert-warn" style="margin-bottom:14px">⚠ <b>Eigenbeleg ohne Originalbeleg.</b> '
                'Bei Bewirtung in einer Gaststätte ist die Rechnung Pflicht – bitte die Begründung prüfen.</div>')
    warn += _bw_hinweise(d)
    vers = ""
    if len(alle) > 1:
        vers = '<h3 style="font-size:14px;margin:16px 0 6px 0">Versionen</h3>'
        for v in alle:
            vers += (f'<div style="font-size:13px;padding:3px 0"><a href="/bewirtung/{v["id"]}">Version {v["version"]}</a> · '
                     f'{_bw_status_badge(v)} · {_bw_e(bw.lokal_text(v["bestaetigt_am"]))} · '
                     f'{"✓ unverändert" if bw.integritaet_ok(v) else ("–" if v["status"] == "entwurf" else "✖ Prüfsumme weicht ab")}</div>')
    content = f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <a href="/belege" class="btn btn-secondary">← Belege</a>
      <h1 class="page-title" style="margin:0">🍽 Bewirtungsbeleg {_bw_e(d["nummer"])}</h1>
    </div>
    <div style="margin-bottom:14px">{links}</div>
    <div class="card" style="max-width:760px"><div class="card-body">
      {warn}
      {_bw_detail_html(d, klarname, haupt, bw.integritaet_ok(d) if d["status"] != "entwurf" else None,
                       f"/bewirtung/{bid}/pdf", True)}
      {vers}
    </div></div>"""
    return HTMLResponse(shell(f"Bewirtung {d['nummer']}", content, "belege"))


@app.get("/bewirtung/{bid}/pdf")
def bewirtung_organisator_pdf(request: Request, bid: int):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren haben Zugriff.</div>'), status_code=403)
    db = get_db()
    try:
        d = bw.laden(db, bid)
    finally:
        db.close()
    if not d or not d.get("s3_pdf"):
        return HTMLResponse(shell("Nicht gefunden",
            '<div class="alert alert-err">Kein PDF vorhanden.</div>'), status_code=404)
    return _bw_pdf_antwort(d)


# ── Reisevorschuss / Bargeld-Konto ────────────────────────────────────────────
# Auszahlung und Rückgabe erfasst der Organisator, der Mitarbeiter bestätigt im Portal.
# Selbst beschafftes Bargeld (Geldautomat, Wechselstube) trägt der Mitarbeiter im Portal ein.
# Kurse werden von Hand eingetragen (1 EUR = x Fremdwährung). Fachlogik: mod_vorschuss.py.

_VS_FORM_JS = """<script>
(function(){
  var f=document.getElementById('vsform'); if(!f) return;
  var w=f.elements['waehrung'], b=f.elements['betrag'], k=f.elements['kurs'], e=f.elements['eur_betrag'];
  function num(x){x=(x||'').replace(/\\s/g,''); if(x.indexOf(',')>-1&&x.indexOf('.')>-1){x=x.replace(/\\./g,'').replace(',','.');}else{x=x.replace(',','.');} var v=parseFloat(x); return isNaN(v)?null:v;}
  function fmt(v,n){return v.toFixed(n).replace('.',',');}
  function istEur(){return (w.value||'').toUpperCase()==='EUR';}
  function umschalten(){var box=document.getElementById('vs_kursbox'); if(box) box.style.display=istEur()?'none':'block';
    var lab=document.getElementById('vs_kurslabel'); if(lab) lab.textContent=(w.value||'…').toUpperCase();}
  function ausKurs(){if(istEur()) return; var bv=num(b.value), kv=num(k.value); if(bv&&kv){e.value=fmt(bv/kv,2);}}
  function ausEur(){if(istEur()) return; var bv=num(b.value), ev=num(e.value); if(bv&&ev){k.value=fmt(bv/ev,6);}}
  w.addEventListener('input',function(){umschalten();ausKurs();});
  b.addEventListener('input',function(){ausKurs();});
  k.addEventListener('input',ausKurs); e.addEventListener('input',ausEur);
  umschalten();
})();
</script>"""


def _vs_zahl(x, n: int = 2) -> str:
    return "" if x is None else f"{float(x):.{n}f}".replace(".", ",")


def _vs_kurs4(k) -> str:
    return "–" if not k else f"{float(k):.4f}".replace(".", ",")


def _vs_eur_signed(v) -> str:
    return "–" if v is None else f"{float(v):+.2f} EUR".replace(".", ",")


def _vs_form_felder(d: dict, eigen: bool) -> str:
    datalist = '<datalist id="vs_wl">' + "".join(f'<option value="{w}">' for w in vs.WAEHRUNGEN) + "</datalist>"
    quellen_opts = "".join(
        f'<option value="{q}"{" selected" if q == d.get("quelle_txt") else ""}>{q}</option>' for q in vs.QUELLEN)
    quelle_html = ""
    if eigen:
        quelle_html = (f'<div class="form-group"><label style="font-size:12px">Beschafft über</label>'
                       f'<select name="quelle_txt"><option value="">– wählen –</option>{quellen_opts}</select></div>')
    eur_label = ("Euro-Betrag laut Kontoauszug (belastet, inkl. Gebühren)" if eigen else "Gegenwert in EUR")
    hinweis = ("Der belastete Euro-Betrag steht auf dem Kontoauszug oder der Kreditkartenabrechnung. "
               "Kurs oder Euro-Betrag genügt, das andere rechnet die Seite aus." if eigen else
               "Kurs von Hand eintragen, wie auf dem Wechselbeleg oder der Bank-Abrechnung. "
               "Kurs oder Euro-Gegenwert genügt, das andere rechnet die Seite aus.")
    return f"""{datalist}
        <div class="form-grid form-grid-2">
          <div class="form-group"><label style="font-size:12px">Datum</label>
            <input type="date" name="datum" value="{_bw_e(d.get("datum"))}"></div>
          {quelle_html}
          <div class="form-group"><label style="font-size:12px">Währung (z. B. USD)</label>
            <input type="text" name="waehrung" list="vs_wl" maxlength="3" value="{_bw_e(d.get("waehrung") or "EUR")}"
                   style="text-transform:uppercase" autocomplete="off"></div>
          <div class="form-group"><label style="font-size:12px">Betrag in dieser Währung</label>
            <input type="text" inputmode="decimal" name="betrag" value="{_vs_zahl(d.get("betrag"))}" placeholder="z. B. 500,00"></div>
        </div>
        <div id="vs_kursbox">
          <div class="form-grid form-grid-2">
            <div class="form-group"><label style="font-size:12px">Kurs: 1 EUR = … <span id="vs_kurslabel">USD</span></label>
              <input type="text" inputmode="decimal" name="kurs" value="{_vs_zahl(d.get("kurs"), 6) if d.get("waehrung") not in (None, "EUR") else ""}" placeholder="z. B. 1,0850"></div>
            <div class="form-group"><label style="font-size:12px">{eur_label}</label>
              <input type="text" inputmode="decimal" name="eur_betrag" value="{_vs_zahl(d.get("eur_betrag")) if d.get("waehrung") not in (None, "EUR") else ""}" placeholder="z. B. 460,83"></div>
          </div>
          <p style="font-size:12px;color:var(--muted);margin:0 0 8px 0">{hinweis}</p>
        </div>
        <div class="form-group"><label style="font-size:12px">Notiz (optional)</label>
          <input type="text" name="notiz" maxlength="200" value="{_bw_e(d.get("notiz"))}"></div>"""


def _vs_form_aus_request(form, art: str) -> dict:
    """Formularwerte in ein Buchungs-Dict übersetzen (Kurs/Euro-Betrag werden ausgerechnet)."""
    datum = (form.get("datum") or "").strip()
    try:
        datum = date.fromisoformat(datum).isoformat() if datum else None
    except ValueError:
        datum = None
    waehrung = (form.get("waehrung") or "").strip().upper()[:3]
    betrag = bw.zahl_parsen(form.get("betrag"))
    d = {"art": art, "datum": datum, "waehrung": waehrung, "betrag": betrag,
         "quelle_txt": (form.get("quelle_txt") or "").strip() if art == "eigenbeschaffung" else "",
         "notiz": (form.get("notiz") or "").strip()}
    kurs, eur, fehler = vs.kurs_berechnen(waehrung, betrag, form.get("kurs"), form.get("eur_betrag"))
    d["kurs"], d["eur_betrag"] = kurs, eur
    d["_kursfehler"] = fehler
    return d


def _vs_fehlerbox(fehler: list) -> str:
    if not fehler:
        return ""
    return ('<div class="alert alert-err" style="margin-bottom:14px"><b>Bitte prüfen:</b><ul '
            'style="margin:6px 0 0 18px;padding:0">' + "".join(f"<li>{_bw_e(f)}</li>" for f in fehler) + "</ul></div>")


def _vs_fehler_sammeln(d: dict) -> list:
    fehler = vs.validieren(d)
    kf = d.get("_kursfehler")
    if kf:
        # Die Standardmeldungen "Betrag/Kurs fehlt" durch die genauere Kursmeldung ersetzen
        fehler = [f for f in fehler if "Kurs oder den Euro-Gegenwert" not in f and "Betrag größer 0" not in f] + [kf]
    return fehler


def _vs_status_badge(d: dict) -> str:
    if d["status"] == "bestaetigt":
        bg, fg, text = "#dcfce7", "#166534", "✓ bestätigt"
    elif d["status"] == "ersetzt":
        bg, fg, text = "#e2e8f0", "#475569", "ersetzt"
    else:
        bg, fg, text = "#fef3c7", "#92400e", "wartet auf Bestätigung"
    return (f'<span style="font-size:11px;background:{bg};color:{fg};padding:2px 8px;border-radius:10px;'
            f'white-space:nowrap">{text}</span>')


def _vs_detail_html(d: dict, klarname: str, integritaet: bool | None, pdf_url: str, technik: bool) -> str:
    aussteller = ""
    if d.get("aussteller_name"):
        label = "Ausgezahlt durch" if d["art"] == "auszahlung" else "Entgegengenommen durch"
        aussteller = (f'<dt>{label}</dt><dd>{_bw_e(d["aussteller_name"])} am '
                      f'{_bw_e(bw.lokal_text(d["aussteller_am"]))}</dd>')
    quelle = f'<dt>Beschafft über</dt><dd>{_bw_e(d["quelle_txt"])}</dd>' if d.get("quelle_txt") else ""
    kurs = (f'<dt>Kurs</dt><dd>{_bw_e(vs.kurs_text(d))}</dd>' if d["waehrung"] != "EUR" else "")
    notiz = f'<dt>Notiz</dt><dd>{_bw_e(d["notiz"])}</dd>' if d.get("notiz") else ""
    best = ""
    if d["status"] in ("bestaetigt", "ersetzt"):
        integ = ""
        if integritaet is True:
            integ = '<span style="color:#166534">✓ Prüfsumme stimmt – unverändert seit der Bestätigung</span>'
        elif integritaet is False:
            integ = '<span style="color:#b91c1c"><b>✖ Prüfsumme weicht ab – Daten wurden verändert!</b></span>'
        tech = ""
        if technik:
            tech = (f'<dt>IP-Adresse</dt><dd>{_bw_e(d["bestaetigt_ip"] or "–")}</dd>'
                    f'<dt>Prüfsumme</dt><dd style="font-family:monospace;font-size:11px;word-break:break-all">'
                    f'{_bw_e(d["pruefsumme"])}<br>{integ}</dd>')
        sig = ""
        if d.get("signatur_png"):
            sig = (f'<dt>Unterschrift</dt><dd><img alt="Unterschrift" style="max-width:240px;border:1px solid '
                   f'var(--border);border-radius:6px;background:#fff" '
                   f'src="data:image/png;base64,{_bw_e(d["signatur_png"])}"></dd>')
        best = (f'<h3 style="font-size:14px;margin:16px 0 8px 0">Bestätigung</h3><dl class="bw-dl">'
                f'<dt>Bestätigt von</dt><dd>{_bw_e(d["bestaetigt_name"])}</dd>'
                f'<dt>Zeitpunkt</dt><dd>{_bw_e(bw.lokal_text(d["bestaetigt_am"]))}</dd>{tech}{sig}</dl>')
    pdf_btn = (f'<a href="{pdf_url}" target="_blank" class="btn btn-secondary" style="width:100%;display:block;'
               f'text-align:center;margin-top:12px">📄 PDF ansehen</a>' if d.get("s3_pdf") else "")
    return f"""{_BW_CSS}
      <dl class="bw-dl">
        <dt>Nummer</dt><dd><b>{_bw_e(d["nummer"])}</b> · Version {d["version"]} {_vs_status_badge(d)}</dd>
        <dt>Art</dt><dd>{_bw_e(vs.ARTEN[d["art"]])}</dd>
        <dt>Mitarbeiter</dt><dd>{_bw_e(klarname)}</dd>
        <dt>Reise</dt><dd>{_bw_e(d["reise_code"])}</dd>
        <dt>Datum</dt><dd>{bw.datum_de(d["datum"])}</dd>{quelle}
        <dt>Betrag</dt><dd><b>{_bw_e(vs.betrag_text(d["betrag"], d["waehrung"]))}</b></dd>{kurs}
        <dt>Gegenwert EUR</dt><dd>{_bw_e(vs.betrag_text(d["eur_betrag"], "EUR"))}</dd>{aussteller}{notiz}
      </dl>{best}{pdf_btn}"""


def _vs_pdf_antwort(d: dict):
    from fastapi.responses import Response
    try:
        inhalt = vs.pdf_laden(d)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">PDF nicht verfügbar: {_bw_e(e)}</div>'),
                            status_code=500)
    return Response(content=inhalt, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="{vs.BELEGARTEN[d["art"]]}_{d["nummer"]}_v{d["version"]}.pdf"'})


def _vs_name(db, kuerzel: str) -> str:
    cur = db.cursor()
    cur.execute(f"SELECT klarname FROM mitarbeiter WHERE kuerzel={ph()}", (kuerzel,))
    r = cur.fetchone(); cur.close()
    return (r[0] if isinstance(r, tuple) else r["klarname"]) if r else kuerzel


# ── Portal ────────────────────────────────────────────────────────────────────
def _vs_zusammenfassung_html(person: dict) -> str:
    """Kurze Übersicht je Währung für den Mitarbeiter."""
    zeilen = ""
    for c, x in sorted(person["waehrungen"].items()):
        if not (x["V"] or x["T"] or x["R"] or x["A"]):
            continue
        teile = []
        if x["V"]:
            teile.append(f"erhalten {vs.betrag_text(x['V'], c)}")
        if x["T"]:
            teile.append(f"selbst beschafft {vs.betrag_text(x['T'], c)}")
        if x["R"]:
            teile.append(f"zurückgegeben {vs.betrag_text(x['R'], c)}")
        if x["A"]:
            teile.append(f"bar ausgegeben (Belege) {vs.betrag_text(x['A'], c)}")
        hinweis = ""
        if person["offen"]:
            hinweis = '<br><span style="color:var(--muted)">Stand vorläufig, bis alle Buchungen bestätigt sind.</span>'
        elif x["rueckgabe_offen"] > 0.005:
            hinweis = (f'<br><b style="color:#b45309">Noch bei dir (Firmengeld): '
                       f'{vs.betrag_text(x["rueckgabe_offen"], c)} – bitte zurückgeben.</b>')
        elif x["eigenanteil"] > 0.005:
            hinweis = (f'<br><b style="color:#166534">Du hast {vs.betrag_text(x["eigenanteil"], c)} aus eigenem Geld '
                       f'bezahlt – wird dir erstattet.</b>')
        zeilen += (f'<div style="font-size:13px;padding:6px 0;border-bottom:1px solid var(--border)">'
                   f'<b>{c}:</b> {_bw_e(" · ".join(teile))}{hinweis}</div>')
    return zeilen


def _vs_portal_karte(token: str, info: dict) -> str:
    try:
        db = get_db()
        try:
            liste = vs.liste_reise(db, info["reise_code"], info["kuerzel"])
            erg = vs.abrechnung(db, info["reise_code"])
        finally:
            db.close()
    except Exception:
        return ""          # Tabelle fehlt noch (vor /init)
    offene = [b for b in liste if b["status"] == "offen"]
    zeilen = ""
    for b in liste:
        if b["status"] == "offen":
            ziel = f"/portal/{token}/vorschuss/{b['id']}/bestaetigen"
        else:
            ziel = f"/portal/{token}/vorschuss/{b['id']}"
        v = f" · Version {b['version']}" if b["version"] > 1 else ""
        zeilen += (f'<a href="{ziel}" style="display:flex;justify-content:space-between;align-items:center;gap:8px;'
                   f'padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;color:inherit;'
                   f'text-decoration:none"><span><b>{_bw_e(b["nummer"])}</b>{v}<br>'
                   f'<span style="color:var(--muted);font-size:12px">{_bw_e(vs.ARTEN[b["art"]])} · {bw.datum_de(b["datum"])} · '
                   f'{_bw_e(vs.betrag_text(b["betrag"], b["waehrung"]))}</span></span>{_vs_status_badge(b)}</a>')
    warn = ""
    if offene:
        warn = (f'<div class="alert alert-warn" style="margin-bottom:10px;font-size:13px">'
                f'{len(offene)} Buchung(en) warten auf deine Bestätigung. Bitte antippen und bestätigen.</div>')
    person = erg["personen"].get(info["kuerzel"])
    zus = _vs_zusammenfassung_html(person) if person else ""
    return f"""<div class="card" style="margin-bottom:16px">
      <div class="card-header"><span class="card-title">💵 Reisevorschuss / Bargeld</span></div>
      <div class="card-body">
        <p style="font-size:12px;color:var(--muted);margin:0 0 10px 0">
          Hier siehst du Vorschuss und Rückgabe und bestätigst sie. Hast du im Reiseland selbst Geld abgehoben
          oder gewechselt, trage es unten ein.</p>
        {warn}{zeilen}{zus}
        <a href="/portal/{token}/vorschuss/neu" class="btn btn-secondary"
           style="width:100%;display:block;text-align:center;margin-top:10px">+ Geldautomat / Wechselstube eintragen</a>
      </div>
    </div>"""


def _vs_eigene(db, info: dict, bid: int):
    d = vs.laden(db, bid)
    if not d or d["reise_code"] != info["reise_code"] or d["kuerzel"] != info["kuerzel"]:
        return None
    return d


def _vs_portal_form_seite(token: str, info: dict, d: dict, fehler: list, bid: int | None) -> str:
    version_hinweis = (f' <span style="font-size:12px;color:var(--muted)">(Korrektur, Version {d["version"]})</span>'
                       if d.get("version", 1) > 1 else "")
    loeschen = ""
    if bid:
        loeschen = (f'<form method="post" action="/portal/{token}/vorschuss/{bid}/loeschen" '
                    f'onsubmit="return confirm(\'Eintrag wirklich löschen?\')" style="margin-top:8px">'
                    f'<button type="submit" class="btn btn-secondary" style="width:100%;color:#b91c1c">'
                    f'🗑 Eintrag löschen</button></form>')
    bid_feld = f'<input type="hidden" name="bid" value="{bid}">' if bid else ""
    return f"""{_BW_CSS}
    <div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 4px 0">💵 Bargeld selbst beschafft{version_hinweis}</h1>
      <p style="font-size:13px;color:var(--muted);margin:0 0 14px 0">Reise <b>{_bw_e(info["reise_code"])}</b> – {_bw_e(info["titel"])}.
        Für Geld, das du im Reiseland selbst abgehoben oder gewechselt hast. Den Kontoauszug oder Wechselbeleg
        lädst du zusätzlich über „Beleg hochladen“ hoch.</p>
      {_vs_fehlerbox(fehler)}
      <form method="post" action="/portal/{token}/vorschuss/speichern" id="vsform">
        {bid_feld}
        {_vs_form_felder(d, True)}
        <button type="submit" class="btn btn-primary" style="width:100%;margin-top:6px">Weiter zur Bestätigung →</button>
      </form>
      {loeschen}
      <a href="/portal/{token}" class="btn btn-secondary" style="width:100%;margin-top:8px;text-align:center;display:block">← Zurück zur Übersicht</a>
    </div></div>
    {_VS_FORM_JS}"""


@app.get("/portal/{token}/vorschuss/neu", response_class=HTMLResponse)
def portal_vorschuss_neu(token: str):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    d = {"art": "eigenbeschaffung", "waehrung": "", "version": 1}
    return HTMLResponse(portal_shell("Bargeld selbst beschafft", _vs_portal_form_seite(token, info, d, [], None)))


@app.get("/portal/{token}/vorschuss/{bid}/bearbeiten", response_class=HTMLResponse)
def portal_vorschuss_bearbeiten(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
    finally:
        db.close()
    if not d or d["art"] != "eigenbeschaffung":
        return _bw_nicht_gefunden(token)
    if d["status"] != "offen":
        return RedirectResponse(f"/portal/{token}/vorschuss/{bid}", status_code=303)
    return HTMLResponse(portal_shell("Bargeld selbst beschafft", _vs_portal_form_seite(token, info, d, [], bid)))


@app.post("/portal/{token}/vorschuss/speichern")
async def portal_vorschuss_speichern(token: str, request: Request):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    form = await request.form()
    try:
        bid = int(form.get("bid") or 0) or None
    except ValueError:
        bid = None
    d = _vs_form_aus_request(form, "eigenbeschaffung")
    d.update({"reise_code": info["reise_code"], "kuerzel": info["kuerzel"], "version": 1})
    db = get_db()
    try:
        if bid:
            alt = _vs_eigene(db, info, bid)
            if not alt or alt["art"] != "eigenbeschaffung":
                return _bw_nicht_gefunden(token)
            if alt["status"] != "offen":
                return RedirectResponse(f"/portal/{token}/vorschuss/{bid}", status_code=303)
            d["version"] = alt["version"]
        fehler = _vs_fehler_sammeln(d)
        if fehler:
            return HTMLResponse(portal_shell("Bargeld selbst beschafft", _vs_portal_form_seite(token, info, d, fehler, bid)))
        neu = vs.speichern(db, d, bid)
        db.commit()
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        return HTMLResponse(portal_shell("Fehler beim Speichern",
            '<div class="card"><div class="card-body"><p>Der Eintrag konnte nicht gespeichert werden. Bitte versuch es '
            'gleich noch einmal oder melde dich beim Organisator.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{_bw_e(e)}</p>'
            f'<a href="/portal/{token}" class="btn btn-secondary">← Zurück</a></div></div>'), status_code=500)
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}/vorschuss/{neu}/bestaetigen", status_code=303)


def _vs_bestaetigen_seite(token: str, info: dict, d: dict, fehler: str = "", name_wert: str = "") -> str:
    fehler_html = f'<div class="alert alert-err" style="margin-bottom:14px">{_bw_e(fehler)}</div>' if fehler else ""
    text = {"auszahlung": "Ich bestätige, den Betrag als Reisevorschuss für diese Reise erhalten zu haben. "
                          "Er wird mit den tatsächlichen Reisekosten abgerechnet, nicht verbrauchtes Bargeld gebe ich zurück.",
            "rueckgabe": "Ich bestätige, den Betrag in bar zurückgegeben zu haben.",
            "eigenbeschaffung": "Ich bestätige, das Bargeld wie angegeben selbst beschafft zu haben und dass die "
                                "Angaben vollständig und richtig sind."}[d["art"]]
    bearbeiten = ""
    if d["art"] == "eigenbeschaffung":
        bearbeiten = (f'<a href="/portal/{token}/vorschuss/{d["id"]}/bearbeiten" class="btn btn-secondary" '
                      f'style="width:100%;display:block;text-align:center;margin-top:8px">← Angaben ändern</a>')
    else:
        bearbeiten = ('<p style="font-size:12px;color:var(--muted)">Stimmt etwas nicht? Dann nicht bestätigen, '
                      'sondern beim Organisator melden.</p>')
    return f"""<div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 12px 0">💵 {_bw_e(vs.ARTEN[d["art"]])} bestätigen</h1>
      {fehler_html}
      {_vs_detail_html(d, info["klarname"], None, "", False)}
      <form method="post" action="/portal/{token}/vorschuss/{d["id"]}/bestaetigen" id="bw_best_form">
        <h3 style="font-size:14px;margin:18px 0 8px 0">Bestätigung</h3>
        <label class="bw-opt"><input type="checkbox" name="haken" value="1"> {_bw_e(text)}</label>
        <div class="form-group" style="margin-top:8px"><label style="font-size:12px">
          Vollständiger Name als Bestätigung <span class="required">*</span></label>
          <input type="text" name="name_eingabe" value="{_bw_e(name_wert or info["klarname"])}" autocomplete="off"
                 placeholder="{_bw_e(info["klarname"])}"></div>
        <div class="form-group"><label style="font-size:12px">Unterschrift (optional, mit Finger oder Maus)</label>
          <canvas id="bw_sig" class="bw-sig" width="600" height="200"></canvas>
          <button type="button" id="bw_sig_clear" class="btn btn-secondary btn-sm" style="margin:6px 0 4px 0;width:auto">Unterschrift löschen</button>
          <input type="hidden" name="signatur" id="bw_sig_data"></div>
        <p style="font-size:12px;color:var(--muted);margin:12px 0">
          Mit „Verbindlich bestätigen“ wird die Buchung gesperrt. Zeitpunkt, Name und eine Prüfsumme werden
          gespeichert. Spätere Änderungen sind nur als neue Version möglich.</p>
        <button type="submit" class="btn btn-success" style="width:100%">✓ Verbindlich bestätigen</button>
        {bearbeiten}
        <a href="/portal/{token}" class="btn btn-secondary" style="width:100%;display:block;text-align:center;margin-top:8px">Später bestätigen</a>
      </form>{_BW_SIG_JS}
    </div></div>"""


@app.get("/portal/{token}/vorschuss/{bid}/bestaetigen", response_class=HTMLResponse)
def portal_vorschuss_bestaetigen_seite(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
    finally:
        db.close()
    if not d:
        return _bw_nicht_gefunden(token)
    if d["status"] != "offen":
        return RedirectResponse(f"/portal/{token}/vorschuss/{bid}", status_code=303)
    return HTMLResponse(portal_shell("Bestätigen", _vs_bestaetigen_seite(token, info, d)))


@app.post("/portal/{token}/vorschuss/{bid}/bestaetigen")
async def portal_vorschuss_bestaetigen(token: str, bid: int, request: Request):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    form = await request.form()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] != "offen":
            return RedirectResponse(f"/portal/{token}/vorschuss/{bid}", status_code=303)
        name_eingabe = (form.get("name_eingabe") or "").strip()
        try:
            vs.bestaetigen(db, bid, info["klarname"], name_eingabe, bool(form.get("haken")),
                           form.get("signatur") or None, _bw_client_ip(request), request.headers.get("user-agent", ""))
            db.commit()
        except ValueError as e:
            db.rollback()
            return HTMLResponse(portal_shell("Bestätigen", _vs_bestaetigen_seite(token, info, d, str(e), name_eingabe)),
                                status_code=400)
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        return HTMLResponse(portal_shell("Fehler",
            '<div class="card"><div class="card-body"><p>Die Bestätigung konnte nicht gespeichert werden. Es wurde nichts '
            'verändert. Bitte versuch es gleich noch einmal oder melde dich beim Organisator.</p>'
            f'<p style="font-size:11px;color:#94a3b8">{_bw_e(e)}</p>'
            f'<a href="/portal/{token}/vorschuss/{bid}/bestaetigen" class="btn btn-secondary">← Zurück</a></div></div>'),
            status_code=500)
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}/vorschuss/{bid}?ok=1", status_code=303)


@app.get("/portal/{token}/vorschuss/{bid}", response_class=HTMLResponse)
def portal_vorschuss_ansicht(token: str, bid: int, ok: int = 0):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
        if not d:
            return _bw_nicht_gefunden(token)
        if d["status"] == "offen":
            return RedirectResponse(f"/portal/{token}/vorschuss/{bid}/bestaetigen", status_code=303)
        alle = vs.versionen(db, d["nummer"])
    finally:
        db.close()
    banner = ""
    if ok:
        banner = ('<div class="alert alert-ok" style="margin-bottom:14px">✓ Bestätigt und gespeichert. '
                  'Die Buchung liegt jetzt beim Organisator.</div>')
    if d["status"] == "ersetzt":
        banner += ('<div class="alert alert-warn" style="margin-bottom:14px">Diese Version wurde durch eine neuere ersetzt '
                   'und bleibt nur zur Nachvollziehbarkeit erhalten.</div>')
    versionen_html = ""
    if len(alle) > 1:
        versionen_html = '<h3 style="font-size:14px;margin:16px 0 6px 0">Versionen</h3>' + "".join(
            f'<div style="font-size:13px;padding:4px 0"><a href="/portal/{token}/vorschuss/{v["id"]}">Version {v["version"]}</a> · '
            f'{_vs_status_badge(v)}</div>' for v in alle)
    korrektur = ""
    if d["status"] == "bestaetigt" and d["art"] == "eigenbeschaffung":
        korrektur = (f'<form method="post" action="/portal/{token}/vorschuss/{bid}/korrektur" '
                     f'onsubmit="return confirm(\'Eine Korrektur erzeugt eine neue Version, die erneut bestätigt '
                     f'werden muss. Fortfahren?\')" style="margin-top:10px"><button type="submit" '
                     f'class="btn btn-secondary" style="width:100%">✏ Korrigieren (neue Version)</button></form>')
    elif d["status"] == "bestaetigt":
        korrektur = ('<p style="font-size:12px;color:var(--muted);margin-top:10px">Korrekturen an Auszahlung und '
                     'Rückgabe macht der Organisator. Danach bestätigst du die neue Version erneut.</p>')
    content = f"""<div class="card"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 12px 0">💵 {_bw_e(vs.ARTEN[d["art"]])}</h1>
      {banner}
      {_vs_detail_html(d, info["klarname"], vs.integritaet_ok(d), f"/portal/{token}/vorschuss/{bid}/pdf", False)}
      {korrektur}{versionen_html}
      <a href="/portal/{token}" class="btn btn-primary" style="width:100%;display:block;text-align:center;margin-top:12px">← Zurück zur Übersicht</a>
    </div></div>"""
    return HTMLResponse(portal_shell(f"{d['nummer']}", content))


@app.post("/portal/{token}/vorschuss/{bid}/korrektur")
def portal_vorschuss_korrektur(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
        if not d or d["art"] != "eigenbeschaffung":
            return _bw_nicht_gefunden(token)
        try:
            neu = vs.korrektur_starten(db, bid)
            db.commit()
        except ValueError:
            db.rollback()
            cur = db.cursor()
            cur.execute(f"SELECT id FROM bargeld_buchungen WHERE ersetzt_id={ph()} AND status='offen'", (bid,))
            r = cur.fetchone(); cur.close()
            neu = (r[0] if isinstance(r, tuple) else r["id"]) if r else None
            if not neu:
                return RedirectResponse(f"/portal/{token}/vorschuss/{bid}", status_code=303)
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}/vorschuss/{neu}/bearbeiten", status_code=303)


@app.post("/portal/{token}/vorschuss/{bid}/loeschen")
def portal_vorschuss_loeschen(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
        if d and d["status"] == "offen" and d["art"] == "eigenbeschaffung":
            vs.loeschen(db, bid)
            db.commit()
    finally:
        db.close()
    return RedirectResponse(f"/portal/{token}", status_code=303)


@app.get("/portal/{token}/vorschuss/{bid}/pdf")
def portal_vorschuss_pdf(token: str, bid: int):
    info = zugang_aus_token(token)
    if not info:
        return _bw_ungueltig()
    db = get_db()
    try:
        d = _vs_eigene(db, info, bid)
    finally:
        db.close()
    if not d or not d.get("s3_pdf"):
        return _bw_nicht_gefunden(token)
    return _vs_pdf_antwort(d)


# ── Organisator ───────────────────────────────────────────────────────────────
def _vs_kein_zugriff():
    return HTMLResponse(shell("Kein Zugriff", '<div class="alert alert-err">Nur Organisatoren haben Zugriff.</div>'),
                        status_code=403)


def _vs_abrechnung_html(code: str, erg: dict) -> str:
    """Abrechnung je Mitarbeiter und Währung."""
    out = ""
    for k, p in erg["personen"].items():
        zeilen = ""
        details = ""
        for c, x in sorted(p["waehrungen"].items()):
            def t(v):
                return vs.betrag_text(v, c) if v else "–"
            rest = ("–" if x["rueckgabe_offen"] <= 0.005 else
                    f'<b style="color:#b45309">{_bw_e(vs.betrag_text(x["rueckgabe_offen"], c))}</b>'
                    + ("" if x["rueckgabe_offen_eur"] is None else f'<br><span style="color:var(--muted);font-size:11px">≈ {x["rueckgabe_offen_eur"]:.2f} EUR</span>'))
            eigen = ("–" if x["eigenanteil"] <= 0.005 else
                     f'<b style="color:#166534">{_bw_e(vs.betrag_text(x["eigenanteil"], c))}</b>'
                     + ("" if x["eigenanteil_eur"] is None else f'<br><span style="color:var(--muted);font-size:11px">≈ {x["eigenanteil_eur"]:.2f} EUR</span>'))
            kurs = "–" if c == "EUR" else _vs_kurs4(x["kurs_V"])
            kerg = _vs_eur_signed(x["kursergebnis"])
            zeilen += (f'<tr><td><b>{c}</b></td><td style="text-align:right">{_bw_e(t(x["V"]))}</td>'
                       f'<td style="text-align:right">{_bw_e(t(x["T"]))}</td><td style="text-align:right">{_bw_e(t(x["R"]))}</td>'
                       f'<td style="text-align:right">{_bw_e(t(x["A"]))}<br><span style="color:var(--muted);font-size:11px">{len(x["belege"])} Beleg(e)</span></td>'
                       f'<td style="text-align:right">{rest}</td><td style="text-align:right">{eigen}</td>'
                       f'<td style="text-align:right">{kurs}</td><td style="text-align:right">{kerg}</td></tr>')
            for wtxt in x["warnungen"]:
                details += f'<div class="alert alert-warn" style="font-size:12px;margin:6px 0">⚠ {c}: {_bw_e(wtxt)}</div>'
            if c != "EUR" and x["bewertung_eur_je_fw"] and x["belege"]:
                aend = [{"id": b["id"], "anbieter": b["anbieter"], "betrag": b["betrag"], "alt": b["eur_final"],
                         "neu": round(b["betrag"] * x["bewertung_eur_je_fw"], 2),
                         "frei": b["eur_final"] is None or (b["kurs_quelle"] or "").startswith("Bargeld-Konto")} for b in x["belege"]]
                zl = ""
                for a in aend:
                    alt_txt = "–" if a["alt"] is None else f'{a["alt"]:.2f}'
                    zl += (f'<tr><td>#{a["id"]}</td><td>{_bw_e(a["anbieter"] or "–")}</td>'
                           f'<td style="text-align:right">{_bw_e(vs.betrag_text(a["betrag"], c))}</td>'
                           f'<td style="text-align:right">{alt_txt}</td>'
                           f'<td style="text-align:right"><b>{a["neu"]:.2f}</b></td>'
                           f'<td>{"" if a["frei"] else "bleibt (manuell gesetzt)"}</td></tr>')
                kurs_je = f'{x["bewertung_eur_je_fw"]:.6f}'.replace(".", ",")
                details += f"""<details style="margin:8px 0"><summary style="cursor:pointer;font-size:13px">
                  Bar-Belege in {c} bewerten: 1 {c} = {kurs_je} EUR ({len(aend)} Beleg(e))</summary>
                  <div class="table-wrap"><table><thead><tr><th>Beleg</th><th>Anbieter</th><th style="text-align:right">Betrag</th>
                  <th style="text-align:right">EUR final alt</th><th style="text-align:right">EUR final neu</th><th></th></tr></thead>
                  <tbody>{zl}</tbody></table></div>
                  <form method="post" action="/reise/{_bw_e(code)}/vorschuss/bewerten" style="margin-top:8px">
                    <input type="hidden" name="kuerzel" value="{_bw_e(k)}"><input type="hidden" name="waehrung" value="{c}">
                    <button type="submit" class="btn btn-primary btn-sm">Bewertung in die Belege übernehmen</button>
                  </form></details>"""
        if not zeilen:
            continue
        vorlaeufig = ""
        if p["offen"]:
            vorlaeufig = ('<div class="alert alert-warn" style="font-size:12px;margin:0 0 8px 0">Vorläufig: Es gibt unbestätigte '
                          'Buchungen. „Rückgabe offen“ und „Erstattung“ sind erst nach der Bestätigung verlässlich.</div>')
        out += f"""<div class="card" style="margin-bottom:16px">
          <div class="card-header"><span class="card-title">📊 Abrechnung {_bw_e(p["name"])}</span>
            {f'<span style="font-size:12px;color:#b45309">{p["offen"]} Buchung(en) noch nicht bestätigt (zählen noch nicht)</span>' if p["offen"] else ""}</div>
          <div class="table-wrap"><table>
            <thead><tr><th>Währung</th><th style="text-align:right">Ausgezahlt</th><th style="text-align:right">Selbst beschafft</th>
              <th style="text-align:right">Zurückgegeben</th><th style="text-align:right">Bar-Belege</th>
              <th style="text-align:right">Rückgabe offen</th><th style="text-align:right">Erstattung (Eigenanteil)</th>
              <th style="text-align:right">Kurs Auszahlung (1 EUR =)</th><th style="text-align:right">Kursergebnis</th></tr></thead>
            <tbody>{zeilen}</tbody></table></div>
          <div class="card-body">{vorlaeufig}{details}</div></div>"""
    if erg["unzugeordnet"]:
        liste = "".join(f'<li><a href="/beleg/{b["id"]}">#{b["id"]}</a> {_bw_e(b["anbieter"] or "–")} · '
                        f'{_bw_e(vs.betrag_text(b["betrag"], b["waehrung"]))} · Reisender: {_bw_e(b["reisender"] or "–")}</li>'
                        for b in erg["unzugeordnet"])
        out += ('<div class="alert alert-warn" style="margin-bottom:16px"><b>Bar-Belege ohne eindeutigen Mitarbeiter</b> '
                '(bitte am Beleg über „Daten nachtragen“ den Reisenden eintragen):'
                f'<ul style="margin:6px 0 0 18px">{liste}</ul></div>')
    return out


@app.get("/reise/{code}/vorschuss", response_class=HTMLResponse)
def vorschuss_uebersicht(request: Request, code: str, ok: str = ""):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    db = get_db()
    try:
        cur = db.cursor()
        cur.execute(f"SELECT titel FROM reisen WHERE code={ph()}", (rcode,))
        r = cur.fetchone(); cur.close()
        if not r:
            return HTMLResponse(shell("Nicht gefunden", '<div class="alert alert-err">Reise nicht gefunden.</div>'), status_code=404)
        titel = r[0] if isinstance(r, tuple) else r["titel"]
        teilnehmer = reisende_der_reise(rcode)
        buchungen = vs.liste_reise(db, rcode)
        erg = vs.abrechnung(db, rcode)
    finally:
        db.close()
    personen = {}
    for m in teilnehmer:
        personen[m[0] if isinstance(m, tuple) else m["kuerzel"]] = m[1] if isinstance(m, tuple) else m["klarname"]
    for b in buchungen:
        personen.setdefault(b["kuerzel"], erg["personen"].get(b["kuerzel"], {}).get("name", b["kuerzel"]))
    banner = ""
    if ok == "gespeichert":
        banner = ('<div class="alert alert-ok" style="margin-bottom:14px">Gespeichert. Die Buchung wartet jetzt auf die '
                  'Bestätigung des Mitarbeiters (im Portal-Link).</div>')
    elif ok == "bewertet":
        banner = '<div class="alert alert-ok" style="margin-bottom:14px">Die Bar-Belege wurden bewertet.</div>'
    karten = ""
    for k, name in personen.items():
        zeilen = ""
        for b in [x for x in buchungen if x["kuerzel"] == k]:
            akt = f'<a href="/vorschuss/{b["id"]}" class="btn btn-secondary btn-sm">Ansehen</a> '
            if b["status"] == "offen" and b["art"] in ("auszahlung", "rueckgabe"):
                akt += (f'<a href="/reise/{rcode}/vorschuss/{b["id"]}/bearbeiten" class="btn btn-secondary btn-sm">Bearbeiten</a> ')
            if b["status"] == "offen":
                akt += (f'<form method="post" action="/reise/{rcode}/vorschuss/{b["id"]}/loeschen" style="display:inline" '
                        f'onsubmit="return confirm(\'Buchung wirklich löschen?\')"><button type="submit" '
                        f'class="btn btn-secondary btn-sm" style="color:#b91c1c">Löschen</button></form>')
            if b["status"] == "bestaetigt" and b["art"] in ("auszahlung", "rueckgabe"):
                akt += (f'<form method="post" action="/reise/{rcode}/vorschuss/{b["id"]}/korrektur" style="display:inline" '
                        f'onsubmit="return confirm(\'Korrektur erzeugt eine neue Version, die der Mitarbeiter erneut bestätigen muss. Fortfahren?\')">'
                        f'<button type="submit" class="btn btn-secondary btn-sm">Korrigieren</button></form>')
            zeilen += (f'<tr><td><b>{_bw_e(b["nummer"])}</b>{" v" + str(b["version"]) if b["version"] > 1 else ""}</td>'
                       f'<td>{_bw_e(vs.ARTEN[b["art"]])}</td><td>{bw.datum_de(b["datum"])}</td>'
                       f'<td style="text-align:right">{_bw_e(vs.betrag_text(b["betrag"], b["waehrung"]))}</td>'
                       f'<td style="text-align:right">{"–" if b["waehrung"] == "EUR" else _vs_kurs4(b["kurs"])}</td>'
                       f'<td style="text-align:right">{_bw_e(vs.betrag_text(b["eur_betrag"], "EUR"))}</td>'
                       f'<td>{_vs_status_badge(b)}</td><td>{akt}</td></tr>')
        tab = (f'<div class="table-wrap"><table><thead><tr><th>Nr.</th><th>Art</th><th>Datum</th>'
               f'<th style="text-align:right">Betrag</th><th style="text-align:right">Kurs (1 EUR =)</th>'
               f'<th style="text-align:right">EUR</th><th>Status</th><th></th></tr></thead><tbody>{zeilen}</tbody></table></div>'
               if zeilen else '<div class="empty-state" style="padding:14px">Noch keine Buchungen.</div>')
        karten += f"""<div class="card" style="margin-bottom:16px">
          <div class="card-header"><span class="card-title">👤 {_bw_e(name)} <span style="color:var(--muted);font-weight:400">({_bw_e(k)})</span></span>
            <span><a href="/reise/{rcode}/vorschuss/neu?kuerzel={_bw_e(k)}&art=auszahlung" class="btn btn-primary btn-sm">+ Auszahlung</a>
            <a href="/reise/{rcode}/vorschuss/neu?kuerzel={_bw_e(k)}&art=rueckgabe" class="btn btn-secondary btn-sm">+ Rückgabe</a></span></div>
          {tab}</div>"""
    content = f"""{_BW_CSS}
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <a href="/reise/{rcode}" class="btn btn-secondary">← Reise {rcode}</a>
      <h1 class="page-title" style="margin:0">💵 Reisevorschuss {rcode} – {_bw_e(titel)}</h1>
    </div>
    <p style="font-size:13px;color:var(--muted);margin:0 0 14px 0">Auszahlung und Rückgabe erfassen Sie hier mit Kurs von Hand,
      der Mitarbeiter bestätigt im Portal. Selbst beschafftes Bargeld trägt der Mitarbeiter im Portal ein.
      In die Abrechnung zählen nur bestätigte Buchungen.</p>
    {banner}{karten or '<div class="alert alert-warn">Dieser Reise sind keine Mitarbeiter zugeordnet.</div>'}
    {_vs_abrechnung_html(rcode, erg)}"""
    return HTMLResponse(shell(f"Vorschuss {rcode}", content, "reisen"))


def _vs_org_form_seite(code: str, d: dict, fehler: list, bid: int | None, name: str, aussteller: str = "") -> str:
    art = d["art"]
    titel = {"auszahlung": "Vorschuss auszahlen", "rueckgabe": "Rückgabe Bargeld erfassen"}[art]
    bid_feld = f'<input type="hidden" name="bid" value="{bid}">' if bid else ""
    version_hinweis = (f' <span style="font-size:12px;color:var(--muted)">(Korrektur, Version {d["version"]})</span>'
                       if d.get("version", 1) > 1 else "")
    return f"""{_BW_CSS}
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <a href="/reise/{code}/vorschuss" class="btn btn-secondary">← Vorschuss {code}</a>
      <h1 class="page-title" style="margin:0">💵 {titel}{version_hinweis}</h1>
    </div>
    <div class="card" style="max-width:640px"><div class="card-body">
      <p style="font-size:13px;margin:0 0 12px 0">Mitarbeiter: <b>{_bw_e(name)}</b> · Reise <b>{_bw_e(code)}</b></p>
      {_vs_fehlerbox(fehler)}
      <form method="post" action="/reise/{code}/vorschuss/speichern" id="vsform">
        {bid_feld}<input type="hidden" name="kuerzel" value="{_bw_e(d["kuerzel"])}"><input type="hidden" name="art" value="{art}">
        {_vs_form_felder(d, False)}
        <p style="font-size:12px;color:var(--muted)">Mit dem Speichern erfassen Sie die Buchung unter Ihrem Login
          ({_bw_e(aussteller)}). Danach bestätigt der Mitarbeiter im Portal.</p>
        <button type="submit" class="btn btn-primary">Speichern</button>
        <a href="/reise/{code}/vorschuss" class="btn btn-secondary">Abbrechen</a>
      </form>
    </div></div>
    {_VS_FORM_JS}"""


def _vs_pruefe_reise_und_person(db, code: str, kuerzel: str):
    cur = db.cursor()
    cur.execute(f"SELECT COUNT(*) FROM reisen WHERE code={ph()}", (code,))
    reise_da = (cur.fetchone() or [0])[0]
    cur.close()
    return bool(reise_da) and bool(kuerzel)


@app.get("/reise/{code}/vorschuss/neu", response_class=HTMLResponse)
def vorschuss_neu(request: Request, code: str, kuerzel: str = "", art: str = "auszahlung"):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    if art not in ("auszahlung", "rueckgabe"):
        art = "auszahlung"
    db = get_db()
    try:
        if not _vs_pruefe_reise_und_person(db, rcode, kuerzel):
            return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
        name = _vs_name(db, kuerzel)
    finally:
        db.close()
    d = {"art": art, "kuerzel": kuerzel, "waehrung": "", "datum": date.today().isoformat(), "version": 1}
    return HTMLResponse(shell("Vorschuss", _vs_org_form_seite(rcode, d, [], None, name, request.session.get("klarname") or request.session.get("kuerzel") or ""), "reisen"))


@app.get("/reise/{code}/vorschuss/{bid}/bearbeiten", response_class=HTMLResponse)
def vorschuss_bearbeiten(request: Request, code: str, bid: int):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    db = get_db()
    try:
        d = vs.laden(db, bid)
        name = _vs_name(db, d["kuerzel"]) if d else ""
    finally:
        db.close()
    if not d or d["reise_code"] != rcode or d["art"] not in ("auszahlung", "rueckgabe"):
        return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
    if d["status"] != "offen":
        return RedirectResponse(f"/vorschuss/{bid}", status_code=303)
    return HTMLResponse(shell("Vorschuss", _vs_org_form_seite(rcode, d, [], bid, name, request.session.get("klarname") or request.session.get("kuerzel") or ""), "reisen"))


@app.post("/reise/{code}/vorschuss/speichern")
async def vorschuss_speichern(request: Request, code: str):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    form = await request.form()
    art = form.get("art") or ""
    if art not in ("auszahlung", "rueckgabe"):
        return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
    try:
        bid = int(form.get("bid") or 0) or None
    except ValueError:
        bid = None
    kuerzel = (form.get("kuerzel") or "").strip()
    d = _vs_form_aus_request(form, art)
    d.update({"reise_code": rcode, "kuerzel": kuerzel, "version": 1,
              "aussteller_kuerzel": request.session.get("kuerzel"),
              "aussteller_name": request.session.get("klarname") or request.session.get("kuerzel"),
              "aussteller_am": bw.jetzt_utc_iso()})
    db = get_db()
    try:
        if not _vs_pruefe_reise_und_person(db, rcode, kuerzel):
            return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
        name = _vs_name(db, kuerzel)
        if bid:
            alt = vs.laden(db, bid)
            if not alt or alt["reise_code"] != rcode or alt["kuerzel"] != kuerzel or alt["status"] != "offen" \
                    or alt["art"] not in ("auszahlung", "rueckgabe"):
                return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
            d["version"] = alt["version"]
        fehler = _vs_fehler_sammeln(d)
        if fehler:
            return HTMLResponse(shell("Vorschuss", _vs_org_form_seite(rcode, d, fehler, bid, name, request.session.get("klarname") or request.session.get("kuerzel") or ""), "reisen"))
        vs.speichern(db, d, bid)
        db.commit()
    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">Speichern fehlgeschlagen: {_bw_e(e)}</div>'
                                  f'<a href="/reise/{rcode}/vorschuss" class="btn btn-secondary">← Zurück</a>'), status_code=500)
    finally:
        db.close()
    return RedirectResponse(f"/reise/{rcode}/vorschuss?ok=gespeichert", status_code=303)


@app.post("/reise/{code}/vorschuss/{bid}/loeschen")
def vorschuss_loeschen(request: Request, code: str, bid: int):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    db = get_db()
    try:
        d = vs.laden(db, bid)
        if d and d["reise_code"] == rcode and d["status"] == "offen":
            vs.loeschen(db, bid)
            db.commit()
    finally:
        db.close()
    return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)


@app.post("/reise/{code}/vorschuss/{bid}/korrektur")
def vorschuss_korrektur(request: Request, code: str, bid: int):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    db = get_db()
    try:
        d = vs.laden(db, bid)
        if not d or d["reise_code"] != rcode or d["art"] not in ("auszahlung", "rueckgabe"):
            return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
        try:
            neu = vs.korrektur_starten(db, bid)
            db.commit()
        except ValueError:
            db.rollback()
            cur = db.cursor()
            cur.execute(f"SELECT id FROM bargeld_buchungen WHERE ersetzt_id={ph()} AND status='offen'", (bid,))
            r = cur.fetchone(); cur.close()
            neu = (r[0] if isinstance(r, tuple) else r["id"]) if r else None
            if not neu:
                return RedirectResponse(f"/reise/{rcode}/vorschuss", status_code=303)
    finally:
        db.close()
    return RedirectResponse(f"/reise/{rcode}/vorschuss/{neu}/bearbeiten", status_code=303)


@app.post("/reise/{code}/vorschuss/bewerten")
async def vorschuss_bewerten(request: Request, code: str):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    rcode = code.upper()
    form = await request.form()
    db = get_db()
    try:
        vs.belege_bewerten(db, rcode, (form.get("kuerzel") or "").strip(),
                           (form.get("waehrung") or "").strip().upper(), anwenden=True)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(f"/reise/{rcode}/vorschuss?ok=bewertet", status_code=303)


@app.get("/vorschuss/{bid}", response_class=HTMLResponse)
def vorschuss_detail(request: Request, bid: int):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    db = get_db()
    try:
        d = vs.laden(db, bid)
        if not d:
            return HTMLResponse(shell("Nicht gefunden", '<div class="alert alert-err">Buchung nicht gefunden.</div>'), status_code=404)
        name = _vs_name(db, d["kuerzel"])
        alle = vs.versionen(db, d["nummer"])
    finally:
        db.close()
    links = f'<a href="/reise/{_bw_e(d["reise_code"])}/vorschuss" class="btn btn-secondary">← Vorschuss {_bw_e(d["reise_code"])}</a> '
    if d["beleg_id"]:
        links += f'<a href="/beleg/{d["beleg_id"]}" class="btn btn-secondary">📎 Beleg #{d["beleg_id"]}</a>'
    vers = ""
    if len(alle) > 1:
        vers = '<h3 style="font-size:14px;margin:16px 0 6px 0">Versionen</h3>' + "".join(
            f'<div style="font-size:13px;padding:3px 0"><a href="/vorschuss/{v["id"]}">Version {v["version"]}</a> · '
            f'{_vs_status_badge(v)} · {_bw_e(bw.lokal_text(v["bestaetigt_am"]))} · '
            f'{"✓ unverändert" if vs.integritaet_ok(v) else ("–" if v["status"] == "offen" else "✖ Prüfsumme weicht ab")}</div>'
            for v in alle)
    content = f"""<div style="margin-bottom:14px">{links}</div>
    <div class="card" style="max-width:760px"><div class="card-body">
      <h1 class="page-title" style="margin:0 0 12px 0">💵 {_bw_e(d["nummer"])}</h1>
      {_vs_detail_html(d, name, vs.integritaet_ok(d) if d["status"] != "offen" else None, f"/vorschuss/{bid}/pdf", True)}
      {vers}
    </div></div>"""
    return HTMLResponse(shell(f"Vorschuss {d['nummer']}", content, "reisen"))


@app.get("/vorschuss/{bid}/pdf")
def vorschuss_pdf(request: Request, bid: int):
    if not ist_organisator(request):
        return _vs_kein_zugriff()
    db = get_db()
    try:
        d = vs.laden(db, bid)
    finally:
        db.close()
    if not d or not d.get("s3_pdf"):
        return HTMLResponse(shell("Nicht gefunden", '<div class="alert alert-err">Kein PDF vorhanden.</div>'), status_code=404)
    return _vs_pdf_antwort(d)


# ── Einbindung in Beleg-Detailseite, Abschluss und ToDo ───────────────────────
def _vorschuss_karte(bid: int) -> str:
    try:
        db = get_db()
        try:
            liste = vs.zu_beleg(db, bid)
        finally:
            db.close()
    except Exception:
        return ""
    if not liste:
        return ""
    zeilen = "".join(
        f'<div style="padding:8px 0;font-size:13px"><a href="/vorschuss/{d["id"]}" style="color:#2563eb"><b>{_bw_e(d["nummer"])}</b></a> '
        f'· Version {d["version"]} {_vs_status_badge(d)}<br><span style="color:var(--muted);font-size:12px">'
        f'{_bw_e(vs.ARTEN[d["art"]])} · {bw.datum_de(d["datum"])} · {_bw_e(vs.betrag_text(d["betrag"], d["waehrung"]))}</span></div>'
        for d in liste)
    return (f'<div class="card"><div class="card-header"><span class="card-title">💵 Bargeld-Konto</span></div>'
            f'<div class="card-body">{zeilen}</div></div>')


def _vorschuss_sperre(bid: int) -> str:
    try:
        db = get_db()
        try:
            liste = vs.zu_beleg(db, bid)
        finally:
            db.close()
    except Exception:
        return ""
    if not liste:
        return ""
    return (f"Dieser Beleg gehört zur Bargeld-Buchung {', '.join(sorted({d['nummer'] for d in liste}))} und kann nicht "
            f"gelöscht oder einer anderen Reise zugeordnet werden. Zuerst die Buchung klären (Korrektur über die "
            f"Vorschuss-Seite der Reise).")


def _vorschuss_abschluss_html(rcode: str) -> str:
    try:
        db = get_db()
        try:
            buchungen = vs.liste_reise(db, rcode)
            erg = vs.abrechnung(db, rcode) if buchungen else None
        finally:
            db.close()
    except Exception:
        return ""
    if not buchungen:
        return ""
    zeilen = ""
    for k, p in erg["personen"].items():
        for c, x in sorted(p["waehrungen"].items()):
            if not (x["V"] or x["T"] or x["R"] or x["A"]):
                continue
            rest = []
            if x["rueckgabe_offen"] > 0.005:
                rest.append(f'Rückgabe offen {vs.betrag_text(x["rueckgabe_offen"], c)}')
            if x["eigenanteil"] > 0.005:
                rest.append(f'Erstattung {vs.betrag_text(x["eigenanteil"], c)}')
            if p["offen"]:
                rest.append("vorläufig, Buchung unbestätigt")
            zeilen += (f'<tr><td>{_bw_e(p["name"])}</td><td>{c}</td><td style="text-align:right">{_bw_e(vs.betrag_text(x["V"], c))}</td>'
                       f'<td style="text-align:right">{_bw_e(vs.betrag_text(x["T"], c))}</td>'
                       f'<td style="text-align:right">{_bw_e(vs.betrag_text(x["R"], c))}</td>'
                       f'<td style="text-align:right">{_bw_e(vs.betrag_text(x["A"], c))}</td>'
                       f'<td>{_bw_e(" · ".join(rest) or "ausgeglichen")}</td>'
                       f'<td style="text-align:right">{_vs_eur_signed(x["kursergebnis"])}</td></tr>')
    punkte = vs.offene_punkte_aus(erg)
    warn = ""
    if punkte:
        warn = ('<div style="padding:8px 16px;background:#fef2f2;color:#991b1b;font-size:12px;font-weight:600">⚠ '
                + _bw_e(" · ".join(punkte)) + "</div>")
    return f"""<div class="card" style="margin-bottom:16px">
      <div class="card-header"><span class="card-title">💵 Reisevorschuss / Bargeld</span>
        <a href="/reise/{_bw_e(rcode)}/vorschuss" class="btn btn-secondary btn-sm">Details</a></div>{warn}
      <div class="table-wrap"><table><thead><tr><th>Mitarbeiter</th><th>Währung</th><th style="text-align:right">Ausgezahlt</th>
        <th style="text-align:right">Selbst beschafft</th><th style="text-align:right">Zurückgegeben</th>
        <th style="text-align:right">Bar-Belege</th><th>Stand</th><th style="text-align:right">Kursergebnis</th></tr></thead>
        <tbody>{zeilen}</tbody></table></div></div>"""


def _vorschuss_abschluss_pdf_daten(rcode: str) -> list:
    """Tabellenzeilen für den Abschluss-PDF (erste Zeile = Kopf), leer wenn kein Bargeld-Konto."""
    try:
        db = get_db()
        try:
            buchungen = vs.liste_reise(db, rcode)
            erg = vs.abrechnung(db, rcode) if buchungen else None
        finally:
            db.close()
    except Exception:
        return []
    if not buchungen:
        return []
    daten = [["Mitarbeiter", "Währ.", "Ausgezahlt", "Selbst besch.", "Zurück", "Bar-Belege", "Stand"]]
    for k, p in erg["personen"].items():
        for c, x in sorted(p["waehrungen"].items()):
            if not (x["V"] or x["T"] or x["R"] or x["A"]):
                continue
            rest = []
            if x["rueckgabe_offen"] > 0.005:
                rest.append(f'Rückgabe offen {x["rueckgabe_offen"]:.2f}')
            if x["eigenanteil"] > 0.005:
                rest.append(f'Erstattung {x["eigenanteil"]:.2f}')
            if p["offen"]:
                rest.append("vorläufig")
            daten.append([p["name"], c, f'{x["V"]:.2f}', f'{x["T"]:.2f}', f'{x["R"]:.2f}', f'{x["A"]:.2f}',
                          " · ".join(rest) or "ausgeglichen"])
    return daten if len(daten) > 1 else []


def _vorschuss_todo_laden() -> list:
    """Reisen nach Rückkehr mit offenem Bargeld-Konto (Rückgabe offen, Buchung unbestätigt, Beleg ohne Zuordnung)."""
    ergebnis = []
    try:
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute(f"""SELECT DISTINCT b.reise_code, r.titel, r.rueckkehr FROM bargeld_buchungen b
                            LEFT JOIN reisen r ON r.code = b.reise_code WHERE b.status != 'ersetzt'""")
            reisen = cur.fetchall(); cur.close()
            heute = date.today()
            for r in reisen:
                code = r[0] if isinstance(r, tuple) else r["reise_code"]
                titel = r[1] if isinstance(r, tuple) else r["titel"]
                rk = r[2] if isinstance(r, tuple) else r["rueckkehr"]
                try:
                    rk_d = rk if isinstance(rk, date) else date.fromisoformat(str(rk)[:10])
                except Exception:
                    rk_d = None
                punkte = vs.offene_punkte(db, code)
                unbestaetigt = [p for p in punkte if "nicht bestätigt" in p]
                nach_reise = [p for p in punkte if "nicht bestätigt" not in p] if (rk_d and rk_d < heute) else []
                if unbestaetigt or nach_reise:
                    ergebnis.append({"reise_code": code, "titel": titel or "", "punkte": unbestaetigt + nach_reise})
        finally:
            db.close()
    except Exception:
        return []
    return ergebnis


# ── Kreditkarten-Abrechnung (lokal, ohne KI) ──────────────────────────────────
# Organisator lädt die Monatsabrechnung (PDF) hoch. Die Datei wird nur im
# Arbeitsspeicher gelesen und NICHT gespeichert. Es wird keine KI und kein
# externer Dienst benutzt. Gespeichert wird nur, was der Organisator in der
# Vorschau bestätigt: endgültiger Euro-Betrag, echte Auslandsgebühr,
# Abrechnungsdatum am jeweiligen Beleg.

def _kk_mitarbeiter_laden() -> list:
    db = get_db(); cur = db.cursor()
    cur.execute("SELECT kuerzel, klarname, kreditkarten_typ FROM mitarbeiter WHERE aktiv=" +
                ("TRUE" if is_postgres() else "1") + " ORDER BY klarname")
    rows = cur.fetchall()
    cur.close(); db.close()
    out = []
    for r in rows:
        out.append({"kuerzel": r[0] if isinstance(r, tuple) else r["kuerzel"],
                    "klarname": r[1] if isinstance(r, tuple) else r["klarname"],
                    "typ": (r[2] if isinstance(r, tuple) else r["kreditkarten_typ"]) or "privat"})
    return out


def _kk_abgerechnet_hinweis(bid) -> str:
    """Zeigt am Beleg, ob er mit einer Kreditkartenabrechnung abgeglichen wurde.
    Eigene Verbindung und Fehler werden abgefangen: fehlt die Spalte noch
    (vor /init), bleibt die Belegseite trotzdem benutzbar."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT kk_abrechnung_datum FROM belege WHERE id={ph()}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        d = (r[0] if isinstance(r, tuple) else r["kk_abrechnung_datum"]) if r else None
        if d:
            return (f'<div style="font-size:12px;color:#15803d;margin-top:4px">'
                    f'✔ Mit Kreditkartenabrechnung vom {fmt_date(d)} abgeglichen</div>')
    except Exception:
        pass
    return ""


def _kk_kein_zugriff():
    return HTMLResponse(shell("Kein Zugriff",
        '<div class="alert alert-err">Nur Organisatoren haben Zugriff.</div>'), status_code=403)


def _kk_upload_seite(meldung: str = "") -> str:
    from html import escape as e
    opts = '<option value="">automatisch erkennen (Karteninhaber)</option>'
    for m in _kk_mitarbeiter_laden():
        opts += f'<option value="{e(m["kuerzel"])}">{e(m["kuerzel"])} – {e(m["klarname"])}</option>'
    return f"""
    <h1 class="page-title">💳 Kreditkarten-Abrechnung</h1>
    <p style="color:var(--muted);font-size:13px;margin-bottom:16px">
      Monatsabrechnung der Firmenkarte hochladen. Die App trägt bei Fremdwährungsbelegen den
      endgültigen Euro-Betrag und die echte Auslandsgebühr ein. Alles andere auf der Abrechnung
      wird ignoriert.</p>
    {meldung}
    <div class="card" style="max-width:640px"><div class="card-body">
      <form method="post" action="/einstellungen/kreditkarten/pruefen" enctype="multipart/form-data">
        <div class="form-group">
          <label>Abrechnung (PDF) <span class="required">*</span></label>
          <input type="file" name="datei" required accept=".pdf"
                 style="width:100%;padding:8px;border:1px solid var(--border);
                        border-radius:var(--radius-s);background:white">
        </div>
        <div class="form-group">
          <label>Karteninhaber</label>
          <select name="mitarbeiter" style="width:100%">{opts}</select>
        </div>
        <button type="submit" class="btn btn-primary">Abrechnung prüfen</button>
        <a href="/einstellungen/kreditkarten/zuordnungen" class="btn btn-secondary">📋 Alle Zuordnungen</a>
        <a href="/einstellungen" class="btn btn-secondary">Abbrechen</a>
      </form>
      <p style="font-size:12px;color:var(--muted);margin-top:14px">
        🔒 Das PDF wird nur gelesen und nicht gespeichert. Keine KI, kein externer Dienst.
        Name, Kartennummer und fremde Buchungen werden nicht abgelegt. Es passiert nichts,
        bevor Sie die Vorschau bestätigen.</p>
    </div></div>"""


@app.get("/einstellungen/kreditkarten", response_class=HTMLResponse)
def kk_upload_formular(request: Request):
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    return HTMLResponse(shell("Kreditkarten-Abrechnung", _kk_upload_seite(), "einstellungen"))


def _kk_betrag(x) -> str:
    return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@app.post("/einstellungen/kreditkarten/pruefen", response_class=HTMLResponse)
async def kk_pruefen(request: Request, datei: UploadFile = File(...), mitarbeiter: str = Form("")):
    from html import escape as e
    if not ist_organisator(request):
        return _kk_kein_zugriff()

    def fehler(text):
        return HTMLResponse(shell("Kreditkarten-Abrechnung", _kk_upload_seite(
            f'<div class="alert alert-err" style="max-width:640px">{e(text)}</div>'), "einstellungen"))

    pdf_bytes = await datei.read()
    if not pdf_bytes or len(pdf_bytes) > 10 * 1024 * 1024:
        return fehler("Datei leer oder größer als 10 MB.")
    parsed = abrechnung_parsen(pdf_bytes)
    del pdf_bytes
    if parsed.get("fehler"):
        return fehler(parsed["fehler"])

    ma_liste = _kk_mitarbeiter_laden()
    if mitarbeiter:
        ma = next((m for m in ma_liste if m["kuerzel"] == mitarbeiter), None)
    else:
        ma = mitarbeiter_finden(parsed["karteninhaber"], ma_liste)
    if not ma:
        return fehler("Der Karteninhaber konnte keinem Mitarbeiter eindeutig zugeordnet werden. "
                      "Bitte oben den Karteninhaber auswählen und erneut hochladen.")

    try:
        db = get_db()
        belege = kk_belege_laden(db, parsed["zeitraum"])
        db.close()
    except Exception as ex:
        return fehler(f"Belege konnten nicht geladen werden: {ex}. "
                      "Falls die Meldung eine fehlende Spalte nennt: einmal /init aufrufen.")

    erg = abrechnung_zuordnen(parsed["positionen"], belege, parsed["karteninhaber"], ma["kuerzel"])
    label_typ = "Privatkreditkarte" if ma["typ"] == "privat" else "Firmenkreditkarte"
    karten_label = f"{label_typ}-{ma['kuerzel']}"
    abr_datum = parsed["abrechnungsdatum"] or date.today()

    def beleg_zelle(b):
        name = b.get("hotel_name") or b.get("anbieter") or b.get("tanken_tankstelle") or b.get("belegart") or "Beleg"
        return (f'<a href="/beleg/{b["id"]}" target="_blank">#{b["id"]} {e(str(name))}</a>'
                f'<div style="font-size:11px;color:var(--muted)">Reise {e(str(b.get("reise_code") or "–"))}</div>')

    def status_zelle(t):
        grund = e(", ".join(t["gruende"]))
        if t["sicher"]:
            return f'<span style="color:#15803d">✔ sicher</span><div style="font-size:11px;color:var(--muted)">{grund}</div>'
        return (f'<span style="color:#b45309">⚠ bitte prüfen</span>'
                f'<div style="font-size:11px;color:var(--muted)">{grund}</div>')

    zeilen_fremd, zeilen_eur = "", ""
    for t in erg["treffer"]:
        p, b = t["pos"], t["beleg"]
        fremd = p["waehrung"] != "EUR"
        wert = f'{b["id"]}|{1 if fremd else 0}|{p["betrag_eur"]:.2f}|{p["entgelt_eur"]:.2f}'
        haken = "checked" if t["sicher"] else ""
        if fremd:
            zeilen_fremd += f"""<tr>
              <td><input type="checkbox" name="auswahl" value="{wert}" {haken} style="width:auto"></td>
              <td>{beleg_zelle(b)}</td>
              <td>{e(p["text"])}<div style="font-size:11px;color:var(--muted)">{fmt_date(p["datum_beleg"])}</div></td>
              <td style="text-align:right">{_kk_betrag(p["betrag_fremd"])} {e(p["waehrung"])}</td>
              <td style="text-align:right"><b>{_kk_betrag(p["betrag_eur"])} €</b></td>
              <td style="text-align:right">{_kk_betrag(p["entgelt_eur"])} €</td>
              <td>{status_zelle(t)}</td></tr>"""
        else:
            zeilen_eur += f"""<tr>
              <td><input type="checkbox" name="auswahl" value="{wert}" {haken} style="width:auto"></td>
              <td>{beleg_zelle(b)}</td>
              <td>{e(p["text"])}<div style="font-size:11px;color:var(--muted)">{fmt_date(p["datum_beleg"])}</div></td>
              <td style="text-align:right">{_kk_betrag(p["betrag_eur"])} €</td>
              <td>{status_zelle(t)}</td></tr>"""

    def vorschlag_zelle(p, vorschlaege):
        if not vorschlaege:
            return '<span style="font-size:12px;color:var(--muted)">kein ähnlicher Beleg gefunden</span>'
        opts = '<option value="">– nicht zuordnen –</option>'
        for v in vorschlaege:
            b = v["beleg"]
            name = str(b.get("hotel_name") or b.get("anbieter") or b.get("tanken_tankstelle")
                       or b.get("belegart") or "Beleg")
            if v["betrag_gleich"]:
                betrag_txt = "Betrag gleich"
            elif v["diff"] is not None:
                betrag_txt = "Abweichung " + f"{v['diff']:+.2f}".replace(".", ",")
            else:
                betrag_txt = "Betrag nicht vergleichbar"
            tage_txt = f"{v['tage']} Tage Abstand" if v["tage"] is not None else "ohne Datum"
            waehr = b.get("waehrung") or "EUR"
            label = (f"#{b['id']} {name} · {_kk_betrag(b['betrag_brutto'])} {waehr} · "
                     f"{fmt_date(b.get('belegdatum'))} · Reise {b.get('reise_code')} · "
                     f"{betrag_txt}, {tage_txt}")
            opts += f'<option value="{auswahl_wert(p, b)}">{e(label)}</option>'
        return f'<select name="manuell" style="max-width:460px;font-size:12px">{opts}</select>'

    offen_paare = list(zip(erg["offen"], erg["vorschlaege"]))
    offen_fremd = [(p, v) for p, v in offen_paare if p["waehrung"] != "EUR"]
    offen_aehnlich = [(p, v) for p, v in offen_paare if p["waehrung"] == "EUR" and v]
    offen_rest = [p for p, v in offen_paare if p["waehrung"] == "EUR" and not v]
    zeilen_offen_fremd = "".join(
        f'<tr><td>{fmt_date(p["datum_beleg"])}</td><td>{e(p["text"])}</td>'
        f'<td style="text-align:right">{_kk_betrag(p["betrag_fremd"])} {e(p["waehrung"])}</td>'
        f'<td style="text-align:right">{_kk_betrag(p["betrag_eur"] + p["entgelt_eur"])} €</td>'
        f'<td>{vorschlag_zelle(p, v)}</td></tr>'
        for p, v in offen_fremd)
    zeilen_offen_aehnlich = "".join(
        f'<tr><td>{fmt_date(p["datum_beleg"])}</td><td>{e(p["text"])}</td>'
        f'<td style="text-align:right">{_kk_betrag(p["betrag_eur"])} €</td>'
        f'<td>{vorschlag_zelle(p, v)}</td></tr>'
        for p, v in offen_aehnlich)
    zeilen_offen_rest = "".join(
        f'<tr><td>{fmt_date(p["datum_beleg"])}</td><td>{e(p["text"])}</td>'
        f'<td style="text-align:right">{_kk_betrag(p["betrag_eur"])} €</td></tr>' for p in offen_rest)

    if parsed["summe_ok"] is True:
        kontrolle = (f'<span style="color:#15803d">✔ Kontrolle: Summe der gelesenen Positionen '
                     f'({_kk_betrag(parsed["summe_positionen"])} €) = Neuer Saldo</span>')
    elif parsed["summe_ok"] is False:
        kontrolle = (f'<span style="color:#b91c1c">⚠ Summe der gelesenen Positionen '
                     f'({_kk_betrag(parsed["summe_positionen"])} €) weicht vom Neuen Saldo '
                     f'({_kk_betrag(parsed["saldo_neu"])} €) ab, {parsed["nicht_gelesen"]} Position(en) '
                     f'nicht lesbar. Bitte nichts übernehmen, sondern die Abrechnung prüfen.</span>')
    else:
        kontrolle = '<span style="color:var(--muted)">Keine Kontrollsumme gefunden.</span>'

    zr = parsed["zeitraum"]
    zeitraum_txt = f"{fmt_date(zr[0])} – {fmt_date(zr[1])}" if zr else "–"
    anzahl_treffer = len(erg["treffer"])

    tab_fremd = (f"""<div class="card" style="margin-bottom:16px"><div class="card-body">
        <h3 style="margin-bottom:8px">Fremdwährung: Euro-Betrag und Gebühr werden eingetragen</h3>
        <div class="table-wrap"><table>
        <tr><th></th><th>Beleg</th><th>Abrechnung</th><th style="text-align:right">Betrag</th>
            <th style="text-align:right">Euro endgültig</th><th style="text-align:right">Gebühr</th><th>Status</th></tr>
        {zeilen_fremd}</table></div></div></div>""") if zeilen_fremd else ""
    tab_eur = (f"""<div class="card" style="margin-bottom:16px"><div class="card-body">
        <h3 style="margin-bottom:8px">Euro-Belege: Zahlung mit Karte wird bestätigt</h3>
        <div class="table-wrap"><table>
        <tr><th></th><th>Beleg</th><th>Abrechnung</th><th style="text-align:right">Betrag</th><th>Status</th></tr>
        {zeilen_eur}</table></div></div></div>""") if zeilen_eur else ""
    tab_offen_fremd = (f"""<div class="card" style="margin-bottom:16px"><div class="card-body">
        <h3 style="margin-bottom:8px">Fremdwährung ohne exakt passenden Beleg</h3>
        <p style="font-size:12px;color:var(--muted)">Entweder nicht reisebezogen oder der Beleg fehlt noch.
        Bei ähnlichem Beleg können Sie ihn wählen: Euro-Betrag und Gebühr der Abrechnung werden dann
        am Beleg eingetragen.</p>
        <div class="table-wrap"><table>
        <tr><th>Datum</th><th>Abrechnung</th><th style="text-align:right">Betrag</th>
            <th style="text-align:right">Euro inkl. Gebühr</th><th>Ähnliche Belege zuordnen</th></tr>
        {zeilen_offen_fremd}</table></div></div></div>""") if zeilen_offen_fremd else ""
    tab_offen_aehnlich = (f"""<div class="card" style="margin-bottom:16px"><div class="card-body">
        <h3 style="margin-bottom:8px">Euro-Positionen mit möglichem Beleg</h3>
        <p style="font-size:12px;color:var(--muted)">Kein sicherer Treffer, aber ein Beleg mit ähnlichem
        Betrag oder Datum. Weicht der Betrag ab (z. B. Trinkgeld), wird der Betrag der Abrechnung als
        endgültiger Betrag eingetragen.</p>
        <div class="table-wrap"><table>
        <tr><th>Datum</th><th>Abrechnung</th><th style="text-align:right">Betrag</th>
            <th>Ähnliche Belege zuordnen</th></tr>
        {zeilen_offen_aehnlich}</table></div></div></div>""") if zeilen_offen_aehnlich else ""
    tab_offen_rest = (f"""<details style="margin-bottom:16px"><summary style="cursor:pointer;font-size:13px">
        {len(offen_rest)} weitere Euro-Positionen ohne Beleg (werden ignoriert und nicht gespeichert)</summary>
        <div class="table-wrap"><table>
        <tr><th>Datum</th><th>Abrechnung</th><th style="text-align:right">Betrag</th></tr>
        {zeilen_offen_rest}</table></div></details>""") if zeilen_offen_rest else ""

    keine = ""
    if not anzahl_treffer:
        keine = '<div class="alert alert-warn">Es wurde kein Beleg dieser Abrechnung sicher zugeordnet.</div>'

    uebernehmen = ""
    if anzahl_treffer or offen_fremd or offen_aehnlich:
        uebernehmen = '<button type="submit" class="btn btn-primary">Ausgewählte übernehmen</button> '

    content = f"""
    <h1 class="page-title">💳 Abrechnung prüfen</h1>
    <div class="card" style="margin-bottom:16px"><div class="card-body" style="font-size:13px">
      <b>{e(ma["kuerzel"])} – {e(ma["klarname"])}</b> · Karte ••••{e(parsed["karte_ende"] or "")} ·
      Abrechnung vom {fmt_date(abr_datum)} (Zeitraum {zeitraum_txt})<br>
      {len(parsed["positionen"])} Positionen gelesen, {anzahl_treffer} Belegen zugeordnet.<br>{kontrolle}
    </div></div>
    {keine}
    <form method="post" action="/einstellungen/kreditkarten/uebernehmen">
      <input type="hidden" name="datum" value="{abr_datum.isoformat()}">
      <input type="hidden" name="kuerzel" value="{e(ma["kuerzel"])}">
      {tab_fremd}{tab_eur}{tab_offen_fremd}{tab_offen_aehnlich}
      <div style="margin-bottom:20px">{uebernehmen}
        <a href="/einstellungen/kreditkarten" class="btn btn-secondary">Abbrechen</a></div>
    </form>
    {tab_offen_rest}"""
    return HTMLResponse(shell("Abrechnung prüfen", content, "einstellungen"))


@app.post("/einstellungen/kreditkarten/uebernehmen", response_class=HTMLResponse)
async def kk_uebernehmen(request: Request):
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    form = await request.form()
    try:
        abr_datum = date.fromisoformat((form.get("datum") or "").strip())
        kuerzel = (form.get("kuerzel") or "").strip()
        ma = next((m for m in _kk_mitarbeiter_laden() if m["kuerzel"] == kuerzel), None)
        if not ma:
            raise ValueError("Mitarbeiter unbekannt")
        eintraege, gesehen = [], set()
        werte = list(form.getlist("auswahl")) + [v for v in form.getlist("manuell") if v]
        for v in werte:
            bid, setzen, eur, fee = v.split("|")
            if setzen not in ("0", "1"):
                raise ValueError("Ungültige Auswahl")
            if int(bid) in gesehen:
                raise ValueError(f"Beleg #{int(bid)} wurde mehrfach gewählt. "
                                 "Bitte jeden Beleg nur einer Position zuordnen.")
            gesehen.add(int(bid))
            eintraege.append({"beleg_id": int(bid), "betrag_setzen": setzen == "1",
                              "betrag_eur": round(float(eur), 2), "entgelt_eur": round(float(fee), 2)})
        label_typ = "Privatkreditkarte" if ma["typ"] == "privat" else "Firmenkreditkarte"
        db = get_db()
        n = kk_treffer_speichern(db, eintraege, abr_datum, f"{label_typ}-{kuerzel}")
        db.commit(); db.close()
    except Exception as ex:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err"><b>Fehler:</b> {ex}</div>'),
                            status_code=400)
    content = f"""
    <h1 class="page-title">💳 Abrechnung übernommen</h1>
    <div class="alert alert-ok">✔ {n} Beleg(e) aktualisiert (Abrechnung vom {fmt_date(abr_datum)}).</div>
    <a href="/einstellungen/kreditkarten" class="btn btn-primary">Weitere Abrechnung</a>
    <a href="/einstellungen/kreditkarten/zuordnungen" class="btn btn-secondary">Alle Zuordnungen</a>
    <a href="/einstellungen" class="btn btn-secondary">Einstellungen</a>"""
    return HTMLResponse(shell("Abrechnung übernommen", content, "einstellungen"))


@app.get("/einstellungen/kreditkarten/zuordnungen", response_class=HTMLResponse)
def kk_zuordnungen(request: Request):
    """Übersicht aller Belege, die mit einer Kreditkartenabrechnung abgeglichen wurden."""
    from html import escape as e
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    try:
        db = get_db()
        rows = kk_zuordnungen_laden(db)
        db.close()
    except Exception as ex:
        return HTMLResponse(shell("Zuordnungen", f'<div class="alert alert-err"><b>Fehler:</b> {e(str(ex))}. '
                                  'Falls eine Spalte fehlt: einmal /init aufrufen.</div>', "einstellungen"))
    gruppen = {}
    for r in rows:
        gruppen.setdefault(str(r["kk_abrechnung_datum"])[:10], []).append(r)
    html = ""
    for datum_s, liste in gruppen.items():
        summe = 0.0
        zeilen = ""
        for r in liste:
            final = r.get("betrag_eur_final")
            gebuehr = r.get("kreditkarte_auslandsentgelt_eur")
            summe += float(final if final is not None else (r["betrag_brutto"] if (r["waehrung"] or "EUR") == "EUR" else 0)) \
                + float(gebuehr or 0)
            name = str(r.get("hotel_name") or r.get("anbieter") or r.get("belegart") or "Beleg")
            waehr = r.get("waehrung") or "EUR"
            final_txt = (_kk_betrag(final) + " €") if final is not None else "–"
            gebuehr_txt = (_kk_betrag(gebuehr) + " €") if gebuehr is not None else "–"
            zeilen += f"""<tr>
              <td><a href="/beleg/{r["id"]}" target="_blank">#{r["id"]} {e(name)}</a></td>
              <td>{e(str(r.get("reise_code") or "–"))}</td>
              <td style="text-align:right">{_kk_betrag(r["betrag_brutto"])} {e(waehr)}</td>
              <td style="text-align:right">{final_txt}</td>
              <td style="text-align:right">{gebuehr_txt}</td>
              <td>{e(str(r.get("kreditkarte_karte") or "–"))}</td>
              <td><form method="post" action="/einstellungen/kreditkarten/loesen" style="margin:0"
                        onsubmit="return confirm('Zuordnung lösen? Endgültiger Euro-Betrag und Gebühr werden zurückgesetzt (Gebühr wird wieder berechnet).')">
                  <input type="hidden" name="beleg_id" value="{r["id"]}">
                  <button type="submit" class="btn btn-secondary btn-sm">lösen</button></form></td></tr>"""
        html += f"""<div class="card" style="margin-bottom:16px"><div class="card-body">
          <h3 style="margin-bottom:8px">Abrechnung vom {fmt_date(datum_s)} · {len(liste)} Beleg(e) · {_kk_betrag(summe)} €</h3>
          <div class="table-wrap"><table>
          <tr><th>Beleg</th><th>Reise</th><th style="text-align:right">Betrag Beleg</th>
              <th style="text-align:right">Euro endgültig</th><th style="text-align:right">Gebühr</th>
              <th>Karte</th><th></th></tr>{zeilen}</table></div></div></div>"""
    if not html:
        html = '<div class="alert alert-warn">Noch keine Zuordnungen vorhanden.</div>'
    content = f"""
    <h1 class="page-title">📋 Zuordnungen Kreditkarten-Abrechnung</h1>
    <p style="color:var(--muted);font-size:13px;margin-bottom:16px">
      Alle Belege, die mit einer Abrechnung abgeglichen wurden. Die Abrechnungsposition selbst
      wird aus Datenschutzgründen nicht gespeichert.</p>
    <p><a href="/einstellungen/kreditkarten" class="btn btn-primary">Neue Abrechnung hochladen</a></p>
    {html}"""
    return HTMLResponse(shell("Zuordnungen", content, "einstellungen"))


@app.post("/einstellungen/kreditkarten/loesen")
async def kk_zuordnung_loesen(request: Request):
    """Löst die Zuordnung eines Belegs zur Abrechnung wieder: endgültiger Betrag und
    Gebühr aus der Abrechnung werden zurückgesetzt, die Gebühr danach wieder berechnet."""
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    form = await request.form()
    try:
        bid = int((form.get("beleg_id") or "").strip())
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"""UPDATE belege SET kk_abrechnung_datum=NULL, betrag_eur_final=NULL,
                        kreditkarte_auslandsentgelt_eur=NULL WHERE id={P}""", (bid,))
        auslandsentgelt_neu_berechnen(bid, db)
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err"><b>Fehler:</b> {ex}</div>'),
                            status_code=400)
    return RedirectResponse("/einstellungen/kreditkarten/zuordnungen", status_code=303)


# ── Kreditkarten-Belege aus dem Ausland: Liste zum Abgleich mit der VISA-Abrechnung (FIWE / Herr Kolbert) ────
# Jede Rechnung und Quittung, die mit Kreditkarte im Ausland bezahlt wurde (a. Fremdwährung, b. EUR im Ausland),
# wird gegen die Kartenabrechnung abgeglichen. Bis dahin bleibt der Betrag in Originalwährung führend, der Euro-Wert
# daneben ist nur geschätzt (≈). „Abgeglichen“ heißt: belege.kk_abrechnung_datum ist gesetzt (wie beim PDF-Abgleich).
_KK_AUSLAND_WHERE = ("b.zahlungsart = 'Kreditkarte' AND b.belegart IN ('Rechnung','Quittung') "
                     "AND b.betrag_brutto IS NOT NULL "
                     "AND (COALESCE(b.waehrung,'EUR') <> 'EUR' "
                     "OR (COALESCE(b.land_beleg,'') <> '' AND UPPER(b.land_beleg) <> 'DE'))")
_KK_AUSLAND_SPALTEN = ["id", "reise_code", "belegart", "anbieter", "belegdatum", "betrag_brutto", "waehrung",
                       "land_beleg", "betrag_eur", "betrag_eur_final", "kreditkarte_karte",
                       "kreditkarte_auslandsentgelt_eur", "kk_abrechnung_datum", "reisender", "titel"]


def kk_ausland_laden(status: str = "offen", reise: str = "") -> list:
    """Belege für die Ausland-Liste als dicts. status: offen | erledigt | alle."""
    P = ph()
    bedingung = _KK_AUSLAND_WHERE
    params = []
    if status == "offen":
        bedingung += " AND b.kk_abrechnung_datum IS NULL"
    elif status == "erledigt":
        bedingung += " AND b.kk_abrechnung_datum IS NOT NULL"
    if reise:
        bedingung += f" AND b.reise_code = {P}"
        params.append(reise)
    spalten = ", ".join(("r.titel" if k == "titel" else f"b.{k}") for k in _KK_AUSLAND_SPALTEN)
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT {spalten} FROM belege b LEFT JOIN reisen r ON r.code = b.reise_code "
                    f"WHERE {bedingung} ORDER BY b.belegdatum NULLS LAST, b.id", tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); db.close()
    return [{k: (r[k] if hasattr(r, "keys") else r[i]) for i, k in enumerate(_KK_AUSLAND_SPALTEN)} for r in rows]


def kk_ausland_offen_anzahl() -> int:
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM belege b WHERE {_KK_AUSLAND_WHERE} AND b.kk_abrechnung_datum IS NULL")
        return int(cur.fetchone()[0])
    finally:
        cur.close(); db.close()


def _kk_ausland_euro(b: dict) -> tuple:
    """(Text, Art) für den Euro-Wert: ('= 11,50 €', 'final') | ('≈ 6,00 €', 'schaetzung') | ('Kurs fehlt', 'fehlt')
    | ('', 'eur') für Beträge, die schon in EUR lauten."""
    if (b.get("waehrung") or "EUR") == "EUR":
        if b.get("betrag_eur_final") is not None:
            return "= " + _kk_betrag(b["betrag_eur_final"]) + " €", "final"
        return "", "eur"
    if b.get("betrag_eur_final") is not None:
        return "= " + _kk_betrag(b["betrag_eur_final"]) + " €", "final"
    if b.get("betrag_eur") is not None:
        return "≈ " + _kk_betrag(b["betrag_eur"]) + " €", "schaetzung"
    return "Kurs fehlt", "fehlt"


def _kk_ausland_filter(request: Request) -> tuple:
    status = (request.query_params.get("status") or "offen").strip()
    if status not in ("offen", "erledigt", "alle"):
        status = "offen"
    reise = (request.query_params.get("reise") or "").strip().upper()
    return status, reise


@app.get("/einstellungen/kreditkarten/ausland", response_class=HTMLResponse)
def kk_ausland_seite(request: Request):
    from html import escape as e
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    status, reise = _kk_ausland_filter(request)
    try:
        liste = kk_ausland_laden(status, reise)
        alle = kk_ausland_laden("alle", "")
    except Exception as ex:
        return HTMLResponse(shell("Kreditkarten-Belege Ausland", f'<div class="alert alert-err"><b>Fehler:</b> {e(str(ex))}. '
                                  'Falls eine Spalte fehlt: einmal /init aufrufen.</div>', "einstellungen"))
    n_offen = sum(1 for b in alle if not b["kk_abrechnung_datum"])
    n_fertig = len(alle) - n_offen
    reisen = sorted({b["reise_code"] for b in alle if b["reise_code"]})
    reise_opts = '<option value="">alle Reisen</option>' + "".join(
        f'<option value="{e(c)}"{" selected" if c == reise else ""}>{e(c)}</option>' for c in reisen)
    status_opts = "".join(f'<option value="{k}"{" selected" if k == status else ""}>{t}</option>'
                          for k, t in (("offen", "Offen (noch abzugleichen)"), ("erledigt", "Abgeglichen"), ("alle", "Alle")))
    zeilen = ""
    for b in liste:
        bid = b["id"]
        waehr = b.get("waehrung") or "EUR"
        euro_txt, euro_art = _kk_ausland_euro(b)
        farbe = {"final": "#15803d", "schaetzung": "#b45309", "fehlt": "#b91c1c", "eur": "var(--muted)"}[euro_art]
        land = (b.get("land_beleg") or "").upper() or "–"
        original = f'<b>{_kk_betrag(b["betrag_brutto"])} {e(waehr)}</b>'
        gebuehr = b.get("kreditkarte_auslandsentgelt_eur")
        gebuehr_txt = (_kk_betrag(gebuehr) + " €") if gebuehr is not None else "–"
        if b["kk_abrechnung_datum"]:
            aktion = f'<span style="color:#15803d;font-size:12px">✔ abgeglichen am {fmt_date(b["kk_abrechnung_datum"])}</span>'
        else:
            aktion = (f'<input type="text" inputmode="decimal" name="eur_{bid}" placeholder="Euro lt. VISA" '
                      f'style="width:96px" title="Endgültiger Euro-Betrag laut Kartenabrechnung">'
                      f' <input type="text" inputmode="decimal" name="fee_{bid}" placeholder="Gebühr" '
                      f'style="width:72px" title="Auslandsgebühr laut Abrechnung. Leer = bleibt unverändert">'
                      f' <label style="font-size:12px;white-space:nowrap"><input type="checkbox" name="ok_{bid}" value="1" '
                      f'style="width:auto"> abgeglichen</label>')
        zeilen += (f'<tr><td style="white-space:nowrap">{fmt_date(b["belegdatum"]) if b["belegdatum"] else "–"}</td>'
                   f'<td><a href="/reise/{e(str(b["reise_code"] or ""))}">{e(str(b["reise_code"] or "–"))}</a></td>'
                   f'<td><a href="/beleg/{bid}" target="_blank">#{bid}</a> {e(str(b["anbieter"] or "–"))}'
                   f'<div style="font-size:11px;color:var(--muted)">{e(b["belegart"])} · {e(str(b.get("reisender") or "–"))}</div></td>'
                   f'<td>{e(land)}</td><td style="font-size:12px">{e(str(b.get("kreditkarte_karte") or "–"))}</td>'
                   f'<td style="text-align:right;white-space:nowrap">{original}</td>'
                   f'<td style="text-align:right;white-space:nowrap;color:{farbe}">{e(euro_txt) or "–"}</td>'
                   f'<td style="text-align:right;white-space:nowrap">{gebuehr_txt}</td>'
                   f'<td style="white-space:nowrap">{aktion}</td></tr>')
    if not zeilen:
        zeilen = ('<tr><td colspan="9"><div class="empty-state">Keine Belege in dieser Auswahl.</div></td></tr>')
    meldung = ""
    if request.query_params.get("gespeichert"):
        meldung = (f'<div class="alert alert-ok" style="margin-bottom:12px">✔ {e(request.query_params.get("gespeichert"))} '
                   'Beleg(e) gespeichert.</div>')
    csv_link = f'/einstellungen/kreditkarten/ausland/csv?status={e(status)}&reise={e(reise)}'
    speichern = ""
    if status != "erledigt" and any(not b["kk_abrechnung_datum"] for b in liste):
        speichern = ('<div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-top:12px">'
                     '<label style="font-size:13px">Datum der Kartenabrechnung '
                     f'<input type="date" name="datum" value="{date.today().isoformat()}" style="width:auto"></label>'
                     '<button type="submit" class="btn btn-success">💾 Eingaben speichern</button>'
                     '<span style="font-size:12px;color:var(--muted)">Gespeichert werden nur Zeilen mit Eingabe oder Haken.</span></div>')
    content = f"""
    <h1 class="page-title">💳 Kreditkarten-Belege Ausland</h1>
    <p style="color:var(--muted);font-size:13px;margin-bottom:12px">
      Alle Rechnungen und Quittungen, die mit Kreditkarte im Ausland bezahlt wurden (Fremdwährung oder EUR im Ausland).
      Zum Abgleich mit der VISA-Abrechnung: Euro-Betrag und Gebühr eintragen, oder bei EUR-Beträgen nur „abgeglichen“ ankreuzen.
      Bis dahin bleibt der Betrag in Originalwährung führend, der Euro-Wert daneben ist nur geschätzt (≈).</p>
    {meldung}
    <div class="card" style="margin-bottom:12px"><div class="card-body">
      <form method="get" style="display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin:0">
        <label style="font-size:13px">Status<br><select name="status">{status_opts}</select></label>
        <label style="font-size:13px">Reise<br><select name="reise">{reise_opts}</select></label>
        <button type="submit" class="btn btn-secondary">Filtern</button>
        <a href="{csv_link}" class="btn btn-secondary">⬇ CSV (Excel)</a>
        <span style="font-size:13px;color:var(--muted)">{n_offen} offen · {n_fertig} abgeglichen</span>
      </form></div></div>
    <form method="post" action="/einstellungen/kreditkarten/ausland/speichern">
      <input type="hidden" name="status" value="{e(status)}"><input type="hidden" name="reise" value="{e(reise)}">
      <div class="card"><div class="table-wrap"><table>
        <thead><tr><th>Datum</th><th>Reise</th><th>Beleg</th><th>Land</th><th>Karte</th>
          <th style="text-align:right">Betrag (Original)</th><th style="text-align:right">Euro</th>
          <th style="text-align:right">Gebühr</th><th>Abgleich mit VISA</th></tr></thead>
        <tbody>{zeilen}</tbody></table></div></div>
      {speichern}
    </form>
    <p style="margin-top:12px"><a href="/einstellungen/kreditkarten" class="btn btn-secondary">PDF-Abrechnung hochladen</a>
       <a href="/einstellungen/kreditkarten/zuordnungen" class="btn btn-secondary">Alle Zuordnungen</a></p>"""
    return HTMLResponse(shell("Kreditkarten-Belege Ausland", content, "einstellungen"))


def _csv_zelle(wert) -> str:
    """Eine CSV-Zelle (Semikolon, UTF-8): Anführungszeichen verdoppeln, Formelzeichen am Anfang entschärfen."""
    t = "" if wert is None else str(wert)
    if t[:1] in ("=", "+", "-", "@"):
        t = "'" + t
    return '"' + t.replace('"', '""') + '"'


def _csv_zahl(x) -> str:
    return "" if x is None else f"{float(x):.2f}".replace(".", ",")


@app.get("/einstellungen/kreditkarten/ausland/csv")
def kk_ausland_csv(request: Request):
    from fastapi.responses import Response
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    status, reise = _kk_ausland_filter(request)
    liste = kk_ausland_laden(status, reise)
    kopf = ["Beleg-Nr", "Reise", "Reisetitel", "Belegdatum", "Reisender", "Karte", "Belegart", "Anbieter", "Land",
            "Betrag Original", "Währung", "Euro geschätzt", "Euro lt. Abrechnung", "Auslandsgebühr",
            "Abrechnung vom", "Status"]
    zeilen = [";".join(_csv_zelle(k) for k in kopf)]
    for b in liste:
        zeilen.append(";".join([
            _csv_zelle(b["id"]), _csv_zelle(b["reise_code"]), _csv_zelle(b["titel"]),
            _csv_zelle(fmt_date(b["belegdatum"]) if b["belegdatum"] else ""),
            _csv_zelle(b.get("reisender")), _csv_zelle(b.get("kreditkarte_karte")), _csv_zelle(b["belegart"]),
            _csv_zelle(b["anbieter"]), _csv_zelle((b.get("land_beleg") or "").upper()),
            _csv_zahl(b["betrag_brutto"]), _csv_zelle(b.get("waehrung") or "EUR"),
            _csv_zahl(b.get("betrag_eur")), _csv_zahl(b.get("betrag_eur_final")),
            _csv_zahl(b.get("kreditkarte_auslandsentgelt_eur")),
            _csv_zelle(fmt_date(b["kk_abrechnung_datum"]) if b["kk_abrechnung_datum"] else ""),
            _csv_zelle("abgeglichen" if b["kk_abrechnung_datum"] else "offen")]))
    daten = ("﻿" + "\r\n".join(zeilen) + "\r\n").encode("utf-8")
    name = f"Kreditkarten-Ausland_{status}_{date.today().strftime('%Y%m%d')}.csv"
    return Response(content=daten, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


def _kk_zahl_de(text: str):
    """„12,40“ / „1.234,56“ / „12.40“ -> float. Leer -> None. Ungültig oder negativ -> ValueError."""
    t = (text or "").strip().replace(" ", "")
    if not t:
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    wert = float(t)
    if wert != wert or wert < 0:
        raise ValueError("Betrag ungültig")
    return round(wert, 2)


@app.post("/einstellungen/kreditkarten/ausland/speichern", response_class=HTMLResponse)
async def kk_ausland_speichern(request: Request):
    """Trägt Euro-Betrag/Gebühr aus der Kartenabrechnung nach und/oder markiert Belege als abgeglichen.
    Verarbeitet werden nur Zeilen mit Eingabe oder Haken; geprüft wird alles, bevor etwas gespeichert wird."""
    from html import escape as e
    if not ist_organisator(request):
        return _kk_kein_zugriff()
    form = await request.form()
    status = (form.get("status") or "offen"); reise = (form.get("reise") or "")
    zurueck = f"/einstellungen/kreditkarten/ausland?status={status}&reise={reise}"
    fehler, auftraege = [], []
    try:
        abr_datum = date.fromisoformat((form.get("datum") or "").strip())
    except ValueError:
        abr_datum = None
        fehler.append("Bitte das Datum der Kartenabrechnung eintragen.")
    ids = set()
    for k in form.keys():
        m = re.fullmatch(r"(eur|fee|ok)_(\d+)", k)
        if m:
            ids.add(int(m.group(2)))
    offen = {b["id"]: b for b in kk_ausland_laden("offen", "")}
    for bid in sorted(ids):
        eur_s, fee_s, ok = form.get(f"eur_{bid}") or "", form.get(f"fee_{bid}") or "", form.get(f"ok_{bid}")
        if not (eur_s.strip() or fee_s.strip() or ok):
            continue
        b = offen.get(bid)
        if not b:
            continue                      # nicht (mehr) offen oder kein Ausland-Kreditkartenbeleg: ignorieren
        try:
            eur, fee = _kk_zahl_de(eur_s), _kk_zahl_de(fee_s)
        except ValueError:
            fehler.append(f"Beleg #{bid}: Betrag oder Gebühr ist keine gültige Zahl.")
            continue
        if eur is not None and eur <= 0 and (b.get("waehrung") or "EUR") != "EUR":
            fehler.append(f"Beleg #{bid}: Der Euro-Betrag muss größer als 0 sein.")
            continue
        auftraege.append((bid, eur, fee))
    if not fehler and not auftraege:
        fehler.append("Es wurde nichts eingetragen oder angekreuzt.")
    if fehler:
        return HTMLResponse(shell("Kreditkarten-Belege Ausland",
            '<div class="alert alert-err"><b>Nicht gespeichert:</b>' + "".join(f"<div>• {e(x)}</div>" for x in fehler)
            + f'</div><a href="{e(zurueck)}" class="btn btn-primary">← Zurück zur Liste</a>', "einstellungen"), status_code=400)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        for bid, eur, fee in auftraege:
            sets, params = [f"kk_abrechnung_datum={P}"], [abr_datum.isoformat()]
            if eur is not None:
                sets.append(f"betrag_eur_final={P}"); params.append(eur)
            if fee is not None:
                sets.append(f"kreditkarte_auslandsentgelt_eur={P}"); params.append(fee)
            cur.execute(f"UPDATE belege SET {', '.join(sets)} WHERE id={P}", tuple(params + [bid]))
        db.commit()
    except Exception:
        db.rollback(); raise
    finally:
        cur.close(); db.close()
    return RedirectResponse(f"{zurueck}&gespeichert={len(auftraege)}", status_code=303)


def _version_zeit_text(zeit: str) -> str:
    """'2026-10-12 08:20' -> '12.10.2026, 08:20 Uhr'"""
    try:
        d, t = zeit.split(" ")
        j, m, tg = d.split("-")
        return f"{tg}.{m}.{j}, {t} Uhr"
    except Exception:
        return zeit


@app.get("/einstellungen/versionen", response_class=HTMLResponse)
def versionen_seite(request: Request):
    """Änderungsverlauf: je Version Datum/Uhrzeit und 2-3 Zeilen, was sich zur Vorgängerversion geändert hat."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren haben Zugriff auf die Einstellungen.</div>'), status_code=403)
    karten = ""
    for i, v in enumerate(VERSIONEN):
        punkte = "".join(f"<li>{_bw_e(t)}</li>" for t in v["aenderungen"])
        aktuell = ' <span style="font-size:11px;background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px">aktuell</span>' if i == 0 else ""
        karten += f"""<div class="card" style="margin-bottom:12px">
          <div class="card-header"><span class="card-title">Version {_bw_e(v["version"])}{aktuell}</span>
            <span style="font-size:12px;color:var(--muted)">{_bw_e(_version_zeit_text(v["zeit"]))}</span></div>
          <div class="card-body"><ul style="margin:0 0 0 18px;padding:0;font-size:13px;line-height:1.6">{punkte}</ul></div>
        </div>"""
    content = f"""
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <a href="/einstellungen" class="btn btn-secondary">← Einstellungen</a>
      <h1 class="page-title" style="margin:0">🆕 Versionen</h1>
    </div>
    <p style="color:var(--muted);font-size:13px;margin-bottom:16px">Was sich in jeder Version gegenüber der Vorgängerversion geändert hat
      (neueste oben, Ortszeit der Auslieferung).</p>
    {karten}"""
    return HTMLResponse(shell("Versionen", content))


@app.get("/einstellungen", response_class=HTMLResponse)
def einstellungen_uebersicht(request: Request):
    """
    Zentrale Übersicht aller Sonder-/Einstellungsseiten, die über die Zeit
    entstanden sind (Backups, Flug-Alerts, offene Aufgaben, Posteingang,
    VMA-Sätze) – bisher jeweils nur über einzelne Buttons an verschiedenen
    Stellen erreichbar, hier an einem Ort gebündelt.
    """
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren haben Zugriff auf die Einstellungen.</div>'), status_code=403)

    neu = VERSIONEN[0]
    versions_text = (f'<b>Version {_bw_e(neu["version"])}</b> · {_bw_e(_version_zeit_text(neu["zeit"]))}<br>'
                     + "<br>".join("• " + _bw_e(t) for t in neu["aenderungen"]))
    karten = [
        ("🆕", "Versionen", versions_text, "/einstellungen/versionen"),
        ("📋", "Offene Aufgaben", "Belege ohne Prüfung/finalen Betrag, unversendete Belege, "
         "nicht abgerechnete Reisen – alles an einem Ort.", "/todo"),
        ("✈", "Flug-/Bahn-Alerts", "Prüfplan, Testlauf, Diagnose der Verspätungs-Überwachung.",
         "/einstellungen/alerts"),
        ("💾", "Backups", "Tägliche Datenbank-Sicherung, manuell anstoßen, Liste vorhandener Backups.",
         "/einstellungen/backups"),
        ("📬", "Posteingang", "Mails/Belege, die noch keiner Reise zugeordnet werden konnten.",
         "/unzugeordnet"),
        ("💰", "VMA-Sätze", "Amtliche Verpflegungsmehraufwand-Sätze einsehen und importieren.",
         "/vma"),
        ("🗺", "Karte", "Wo sich zugeordnete Mitarbeiter bei laufenden Reisen aktuell befinden.",
         "/maps"),
        ("💳", "Kreditkarten-Abrechnung", "Monatsabrechnung hochladen: endgültige Euro-Beträge und "
         "echte Auslandsgebühren werden den Belegen zugeordnet (ohne KI).", "/einstellungen/kreditkarten"),
        ("🌍", "Kreditkarten-Belege Ausland", "Liste aller im Ausland mit Kreditkarte bezahlten Rechnungen und Quittungen "
         "zum Abgleich mit der VISA-Abrechnung (Euro-Betrag und Gebühr nachtragen, CSV-Export).",
         "/einstellungen/kreditkarten/ausland"),
        ("🔧", "Mail-Diagnose", "Prüft den Mail-Postausgang Schritt für Schritt (es wird keine Mail gesendet).",
         "/einstellungen/mail-diagnose"),
        ("📖", "Dokumentation", "Handbuch für Reisende und Organisatoren, alle Einstellungen und Funktionen – "
         "als Seite und PDF.", "/einstellungen/dokumentation"),
    ]
    karten_html = "".join(f"""
        <a href="{url}" style="text-decoration:none;color:inherit">
          <div class="card" style="height:100%">
            <div class="card-body">
              <div style="font-size:28px;margin-bottom:8px">{icon}</div>
              <div style="font-weight:700;font-size:15px;margin-bottom:4px">{titel}</div>
              <div style="font-size:13px;color:var(--muted)">{beschreibung}</div>
            </div>
          </div>
        </a>""" for icon, titel, beschreibung, url in karten)

    content = f"""
    <h1 class="page-title">⚙ Einstellungen</h1>
    <p style="color:var(--muted);font-size:13px;margin-bottom:20px">
      Übersicht aller Sonderseiten – Version {APP_VERSION}, {"PostgreSQL" if is_postgres() else "SQLite"}.</p>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px">
      {karten_html}
    </div>"""
    return HTMLResponse(shell("Einstellungen", content))


@app.get("/einstellungen/backups", response_class=HTMLResponse)
def backups_uebersicht(request: Request):
    """Zeigt vorhandene Backups und erlaubt, sofort eines manuell anzustoßen."""
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen Backups einsehen.</div>'), status_code=403)
    from mod_backup import backups_auflisten
    try:
        backups = backups_auflisten()
    except Exception as e:
        backups = []
        fehler_hinweis = f'<div class="alert alert-err">Backup-Liste konnte nicht geladen werden: {e}</div>'
    else:
        fehler_hinweis = ""

    zeilen = "".join(
        f'<tr><td>{b["datum"].strftime("%d.%m.%Y %H:%M")} UTC</td>'
        f'<td style="font-family:monospace;font-size:12px">{b["key"]}</td>'
        f'<td style="text-align:right">{b["groesse_kb"]:.1f} KB</td></tr>'
        for b in backups)

    content = f"""
    <h1 class="page-title">💾 Datenbank-Backups</h1>
    <p style="color:var(--muted);font-size:13px;margin-bottom:16px">
      Läuft automatisch einmal täglich (sofern der Cron-Job eingerichtet ist),
      behält die letzten {30} Sicherungen. Gespeichert im selben S3-Speicher
      wie die Beleg-PDFs, komprimiert als JSON.</p>
    {fehler_hinweis}
    <div class="card" style="margin-bottom:16px">
      <div class="card-body">
        <form method="post" action="/einstellungen/backups/jetzt">
          <button type="submit" class="btn btn-primary">💾 Jetzt sofort sichern</button>
        </form>
      </div>
    </div>
    <div class="card">
      <div class="card-header"><span class="card-title">Vorhandene Backups ({len(backups)})</span></div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Erstellt</th><th>Datei</th><th style="text-align:right">Größe</th></tr></thead>
          <tbody>{zeilen or '<tr><td colspan="3" class="empty-state">Noch keine Backups vorhanden</td></tr>'}</tbody>
        </table>
      </div>
    </div>
    <p style="font-size:12px;color:var(--muted);margin-top:12px">
      Externen Cron-Dienst (z.B. cron-job.org) täglich auf
      <code>/cron/backup?key=DEIN_CRON_SECRET</code> einrichten, damit das automatisch läuft.</p>
    """
    return HTMLResponse(shell("Backups", content))


@app.post("/einstellungen/backups/jetzt")
def backup_jetzt(request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen Backups anstoßen.</div>'), status_code=403)
    from mod_backup import backup_erstellen
    try:
        backup_erstellen()
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">Backup fehlgeschlagen: {e}</div>'))
    return RedirectResponse("/einstellungen/backups", status_code=303)


@app.get("/einstellungen/alerts", response_class=HTMLResponse)
def alert_einstellungen_form(request: Request, testergebnis: str = ""):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen diese Einstellungen ändern.</div>'), status_code=403)
    k = konfiguration_laden()
    testergebnis_html = ""
    if testergebnis:
        try:
            r = json.loads(testergebnis)
            fehler_html = ""
            if r.get("fehler"):
                fehler_html = "<ul style='margin:6px 0 0 18px;font-size:12px'>" + "".join(
                    f"<li>{e}</li>" for e in r["fehler"]) + "</ul>"
            diagnose_html = ""
            diag = r.get("diagnose")
            if diag:
                zeilen = ""
                for b in diag.get("belege_details", []):
                    verworfen_html = ("<ul style='margin:4px 0 0 18px'>" + "".join(
                        f"<li>{v}</li>" for v in b["verworfen"]) + "</ul>") if b["verworfen"] else ""
                    zeilen += f"""<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:12px">
                        <a href="/beleg/{b['beleg_id']}" style="font-weight:600">Beleg #{b['beleg_id']}</a>
                        – KI-JSON vorhanden: {'ja' if b['hat_ki_json'] else 'nein'},
                        Segmente im Beleg: {b['segmente_roh']}, davon im Zeitfenster übernommen: {b['segmente_uebernommen']}
                        {verworfen_html}
                    </div>"""
                if not zeilen:
                    zeilen = ('<div style="font-size:12px;color:var(--muted)">Kein einziger Flug-/Bahn-Beleg '
                               'im Datumsfenster (heute -2 bis +3 Tage) gefunden – prüfe, ob der Beleg als '
                               '"Flug" oder "Bahn" (Transportart) erkannt wurde und ein Belegdatum hat.</div>')

                verarbeitung_zeilen = ""
                for v in diag.get("verarbeitung", []):
                    api_html = ""
                    if v.get("api_antwort"):
                        api_html = (f'<pre style="font-size:11px;background:var(--bg);padding:8px;'
                                     f'border-radius:6px;margin-top:6px;white-space:pre-wrap">'
                                     f'{json.dumps(v["api_antwort"], ensure_ascii=False, indent=2)[:1500]}</pre>')
                    checkpoints_txt = ", ".join(v.get("checkpoints") or []) or "–"
                    verarbeitung_zeilen += f"""<div style="padding:8px 0;border-bottom:1px solid var(--border);font-size:12px">
                        <b>Beleg #{v['beleg_id']} · Segment {v['segment_index']} ({v['transport_nummer']})</b>
                        – {v['stunden_bis_abreise']}h bis Abreise<br>
                        <span style="color:var(--muted)">Checkpoints: {checkpoints_txt}<br>
                        Quelle: {v.get('quelle','–')}
                        {' · API-Key gesetzt: ' + ('ja' if v.get('api_key_gesetzt') else 'NEIN') if 'api_key_gesetzt' in v else ''}
                        {' · letzter Check: ' + str(v.get('letzter_check_am')) if v.get('letzter_check_am') else ' · noch nie geprüft'}</span><br>
                        <b>→ {v.get('ergebnis','?')}</b>
                        {api_html}
                    </div>"""
                if verarbeitung_zeilen:
                    diagnose_html_verarbeitung = f"""<div class="card" style="margin-bottom:16px">
                      <div class="card-header"><span class="card-title">⚙ Verarbeitung der Segmente im Fenster</span></div>
                      <div class="card-body">{verarbeitung_zeilen}</div>
                    </div>"""
                else:
                    diagnose_html_verarbeitung = ""

                diagnose_html = f"""<div class="card" style="margin-bottom:16px">
                  <div class="card-header"><span class="card-title">🔍 Diagnose (aktuelle Zeit lokal: {diag.get('jetzt_lokal','?')})</span></div>
                  <div class="card-body">{zeilen}</div>
                </div>{diagnose_html_verarbeitung}"""
            testergebnis_html = f"""<div class="alert {'alert-warn' if r.get('fehler') else 'alert-ok'}" style="margin-bottom:16px">
              <b>Testlauf abgeschlossen:</b> {r.get('segmente_im_fenster',0)} Segmente im 24h-Fenster gefunden,
              {r.get('geprueft',0)} davon bei der API abgefragt, {r.get('alerts_gesendet',0)} Alert(s) verschickt.
              {fehler_html}
            </div>{diagnose_html}"""
        except Exception:
            testergebnis_html = f'<div class="alert alert-err">Testlauf-Ergebnis konnte nicht gelesen werden.</div>'
    content = f"""
    <h1 class="page-title">✈ Flug-/Bahn-Alerts – Einstellungen</h1>
    {testergebnis_html}
    <div class="alert alert-warn" style="margin-bottom:16px">
      Ein externer Cron-Dienst (z.B. cron-job.org) muss regelmäßig
      <code>/cron/flug-alerts?key=DEIN_CRON_SECRET</code> aufrufen – am besten minütlich,
      damit die feinste Stufe (15 Min. vor Abflug) tatsächlich greifen kann. Wie oft dabei
      wirklich bei der externen Flug-/Bahn-API nachgefragt wird, steuert NICHT ein
      laufendes Intervall, sondern feste Checkpoints (siehe unten).
    </div>
    <div class="card" style="max-width:560px">
      <div class="card-header"><span class="card-title">📅 Fester Prüfplan (nicht editierbar)</span></div>
      <div class="card-body">
        <table style="width:100%;font-size:13px">
          <tr><td style="padding:4px 0"><b>Vor der geplanten Abreise</b></td>
              <td style="text-align:right">4h · 3h · 2h · 1h · 30 Min · 15 Min</td></tr>
          <tr><td style="padding:4px 0">Bei erkannter Verspätung zusätzlich</td>
              <td style="text-align:right">15 Min vor der neuen erwarteten Abreise</td></tr>
          <tr><td style="padding:4px 0">Während des Fluges/der Zugfahrt</td>
              <td style="text-align:right">kein Check</td></tr>
          <tr><td style="padding:4px 0">Vor der erwarteten Landung/Ankunft</td>
              <td style="text-align:right">30 Min vorher</td></tr>
          <tr><td style="padding:4px 0">Nach der Ankunft</td>
              <td style="text-align:right">kein Check mehr</td></tr>
        </table>
        <hr style="border:none;border-top:1px solid var(--border);margin:12px 0">
        <p style="font-size:12px;color:var(--muted);margin:0">
          Bei einer Verspätung ab <b>{k['verspaetung_alarm_ab_min']} Minuten</b> (oder Stornierung/
          Umleitung) geht sofort eine Mail an alle zugeordneten Reisenden dieser Reise
          <b>und</b> alle Organisatoren – unabhängig vom nächsten Checkpoint.</p>
      </div>
    </div>
    <div class="card" style="max-width:560px;margin-top:16px">
      <div class="card-body">
        <p style="font-size:12px;color:var(--muted);margin-bottom:10px">
          Führt den Prüflauf einmal sofort aus (wie es der Cron sonst regelmäßig tun würde) –
          zum Testen, ohne auf den externen Cron-Dienst zu warten.</p>
        <form method="post" action="/einstellungen/alerts/testlauf">
          <button type="submit" class="btn btn-secondary" style="width:100%">🧪 Jetzt testweise abrufen</button>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell("Alert-Einstellungen", content))


@app.post("/einstellungen/alerts/testlauf")
def alert_testlauf(request: Request):
    if not ist_organisator(request):
        return HTMLResponse(shell("Kein Zugriff",
            '<div class="alert alert-err">Nur Organisatoren dürfen diese Einstellungen ändern.</div>'), status_code=403)
    result = cron_flug_alerts(debug=True)
    import urllib.parse
    return RedirectResponse(
        f"/einstellungen/alerts?testergebnis={urllib.parse.quote(json.dumps(result))}",
        status_code=303)



def _basis_aus_anfrage(request: Request) -> str:
    """Adresse der App aus der aktuellen Anfrage (Ersatz, falls weder PORTAL_BASE_URL noch RENDER_EXTERNAL_URL gesetzt sind)."""
    try:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
        return f"{proto}://{host}"
    except Exception:
        return ""


@app.post("/reise/{code}/zugang/{kuerzel}/senden")
def reise_zugang_senden(code: str, kuerzel: str, request: Request):
    rcode = code.upper(); kuerzel = kuerzel.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT titel, abreise, rueckkehr FROM reisen WHERE code={P}", (rcode,))
        r = cur.fetchone()
        cur.execute(f"SELECT klarname, email FROM mitarbeiter WHERE kuerzel={P}", (kuerzel,))
        m = cur.fetchone()
        cur.close(); db.close()
        if not r or not m:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Reise oder Mitarbeiter nicht gefunden.</div>'))
        g = lambda row, i: row[i]
        titel, abreise, rueckkehr = g(r,0), g(r,1), g(r,2)
        klarname, email = g(m,0), g(m,1)
        result = portal_mail_senden(rcode, kuerzel, klarname, email, titel, abreise, rueckkehr,
                                    _basis_aus_anfrage(request))
        if result.get("fehler"):
            return HTMLResponse(shell("Fehler",
                f'<div class="alert alert-err">Versand fehlgeschlagen: {result["fehler"]}</div>'
                f'<a href="/reise/{rcode}" class="btn btn-secondary">Zurück</a>'))
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))
