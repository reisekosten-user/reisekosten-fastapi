"""
mod_laender.py – Länder-Auswahl mit ISO-Code und deutschem Klarnamen (v3.40)

Für Dropdowns, in denen ein Land gewählt wird (z. B. „Beleg-Daten nachtragen“).
Gespeichert wird immer der ISO-3166-Code (z. B. DE), angezeigt wird „DE – Deutschland“.
Die Liste ist fest im Programm (keine Internetverbindung, keine Zusatzpakete nötig).
"""
from __future__ import annotations
from html import escape

# Länder, die auf Reisen am häufigsten vorkommen (inkl. aller Länder mit eigenem Verpflegungssatz)
HAEUFIGE = ["DE", "AT", "CH", "FR", "IT", "ES", "NL", "BE", "GB", "PL", "CZ", "SK", "HU", "RO", "BG", "HR", "SI", "RS",
            "SE", "NO", "DK", "FI", "PT", "GR", "TR", "UA", "RU", "US", "CA", "MX", "BR", "AR", "CL", "CO", "CR", "PA",
            "DO", "CN", "JP", "KR", "HK", "SG", "MY", "TH", "ID", "PH", "VN", "IN", "AE", "QA", "IL", "EG", "MA", "NG",
            "KE", "ZA", "AU", "NZ"]

# Alle Länder (ISO 3166-1 alpha-2), alphabetisch nach deutschem Namen
LAENDER_ALLE = [
    ("AF", "Afghanistan"), ("EG", "Ägypten"), ("AL", "Albanien"),
    ("DZ", "Algerien"), ("AS", "Amerikanisch-Samoa"), ("VI", "Amerikanische Jungferninseln"),
    ("UM", "Amerikanische Überseeinseln"), ("AD", "Andorra"), ("AO", "Angola"),
    ("AI", "Anguilla"), ("AQ", "Antarktis"), ("AG", "Antigua und Barbuda"),
    ("GQ", "Äquatorialguinea"), ("AR", "Argentinien"), ("AM", "Armenien"),
    ("AW", "Aruba"), ("AZ", "Aserbaidschan"), ("ET", "Äthiopien"),
    ("AU", "Australien"), ("BS", "Bahamas"), ("BH", "Bahrain"),
    ("BD", "Bangladesch"), ("BB", "Barbados"), ("BY", "Belarus"),
    ("BE", "Belgien"), ("BZ", "Belize"), ("BJ", "Benin"),
    ("BM", "Bermuda"), ("BT", "Bhutan"), ("BO", "Bolivien"),
    ("BA", "Bosnien und Herzegowina"), ("BW", "Botsuana"), ("BV", "Bouvetinsel"),
    ("BR", "Brasilien"), ("VG", "Britische Jungferninseln"), ("IO", "Britisches Territorium im Indischen Ozean"),
    ("BN", "Brunei Darussalam"), ("BG", "Bulgarien"), ("BF", "Burkina Faso"),
    ("BI", "Burundi"), ("CV", "Cabo Verde"), ("CL", "Chile"),
    ("CN", "China"), ("CK", "Cookinseln"), ("CR", "Costa Rica"),
    ("CI", "Côte d’Ivoire"), ("CW", "Curaçao"), ("DK", "Dänemark"),
    ("DE", "Deutschland"), ("DM", "Dominica"), ("DO", "Dominikanische Republik"),
    ("DJ", "Dschibuti"), ("EC", "Ecuador"), ("SV", "El Salvador"),
    ("ER", "Eritrea"), ("EE", "Estland"), ("SZ", "Eswatini"),
    ("FK", "Falklandinseln"), ("FO", "Färöer"), ("FJ", "Fidschi"),
    ("FI", "Finnland"), ("FR", "Frankreich"), ("GF", "Französisch-Guayana"),
    ("PF", "Französisch-Polynesien"), ("TF", "Französische Süd- und Antarktisgebiete"), ("GA", "Gabun"),
    ("GM", "Gambia"), ("GE", "Georgien"), ("GH", "Ghana"),
    ("GI", "Gibraltar"), ("GD", "Grenada"), ("GR", "Griechenland"),
    ("GL", "Grönland"), ("GB", "Großbritannien"), ("GP", "Guadeloupe"),
    ("GU", "Guam"), ("GT", "Guatemala"), ("GG", "Guernsey"),
    ("GN", "Guinea"), ("GW", "Guinea-Bissau"), ("GY", "Guyana"),
    ("HT", "Haiti"), ("HM", "Heard und McDonaldinseln"), ("HN", "Honduras"),
    ("HK", "Hongkong"), ("IN", "Indien"), ("ID", "Indonesien"),
    ("IQ", "Irak"), ("IR", "Iran"), ("IE", "Irland"),
    ("IS", "Island"), ("IM", "Isle of Man"), ("IL", "Israel"),
    ("IT", "Italien"), ("JM", "Jamaika"), ("JP", "Japan"),
    ("YE", "Jemen"), ("JE", "Jersey"), ("JO", "Jordanien"),
    ("KY", "Kaimaninseln"), ("KH", "Kambodscha"), ("CM", "Kamerun"),
    ("CA", "Kanada"), ("BQ", "Karibische Niederlande"), ("KZ", "Kasachstan"),
    ("QA", "Katar"), ("KE", "Kenia"), ("KG", "Kirgisistan"),
    ("KI", "Kiribati"), ("CC", "Kokosinseln"), ("CO", "Kolumbien"),
    ("KM", "Komoren"), ("CG", "Kongo-Brazzaville"), ("CD", "Kongo-Kinshasa"),
    ("XK", "Kosovo"), ("HR", "Kroatien"), ("CU", "Kuba"),
    ("KW", "Kuwait"), ("LA", "Laos"), ("LS", "Lesotho"),
    ("LV", "Lettland"), ("LB", "Libanon"), ("LR", "Liberia"),
    ("LY", "Libyen"), ("LI", "Liechtenstein"), ("LT", "Litauen"),
    ("LU", "Luxemburg"), ("MO", "Macau"), ("MG", "Madagaskar"),
    ("MW", "Malawi"), ("MY", "Malaysia"), ("MV", "Malediven"),
    ("ML", "Mali"), ("MT", "Malta"), ("MA", "Marokko"),
    ("MH", "Marshallinseln"), ("MQ", "Martinique"), ("MR", "Mauretanien"),
    ("MU", "Mauritius"), ("YT", "Mayotte"), ("MX", "Mexiko"),
    ("FM", "Mikronesien"), ("MC", "Monaco"), ("MN", "Mongolei"),
    ("ME", "Montenegro"), ("MS", "Montserrat"), ("MZ", "Mosambik"),
    ("MM", "Myanmar"), ("NA", "Namibia"), ("NR", "Nauru"),
    ("NP", "Nepal"), ("NC", "Neukaledonien"), ("NZ", "Neuseeland"),
    ("NI", "Nicaragua"), ("NL", "Niederlande"), ("NE", "Niger"),
    ("NG", "Nigeria"), ("NU", "Niue"), ("KP", "Nordkorea"),
    ("MP", "Nördliche Marianen"), ("MK", "Nordmazedonien"), ("NF", "Norfolkinsel"),
    ("NO", "Norwegen"), ("OM", "Oman"), ("AT", "Österreich"),
    ("PK", "Pakistan"), ("PS", "Palästina"), ("PW", "Palau"),
    ("PA", "Panama"), ("PG", "Papua-Neuguinea"), ("PY", "Paraguay"),
    ("PE", "Peru"), ("PH", "Philippinen"), ("PN", "Pitcairninseln"),
    ("PL", "Polen"), ("PT", "Portugal"), ("PR", "Puerto Rico"),
    ("MD", "Republik Moldau"), ("RE", "Réunion"), ("RW", "Ruanda"),
    ("RO", "Rumänien"), ("RU", "Russland"), ("SB", "Salomonen"),
    ("ZM", "Sambia"), ("WS", "Samoa"), ("SM", "San Marino"),
    ("SA", "Saudi-Arabien"), ("SE", "Schweden"), ("CH", "Schweiz"),
    ("SN", "Senegal"), ("RS", "Serbien"), ("SC", "Seychellen"),
    ("SL", "Sierra Leone"), ("ZW", "Simbabwe"), ("SG", "Singapur"),
    ("SX", "Sint Maarten"), ("SK", "Slowakei"), ("SI", "Slowenien"),
    ("SO", "Somalia"), ("ES", "Spanien"), ("SJ", "Spitzbergen und Jan Mayen"),
    ("LK", "Sri Lanka"), ("BL", "St. Barthélemy"), ("SH", "St. Helena"),
    ("KN", "St. Kitts und Nevis"), ("LC", "St. Lucia"), ("MF", "St. Martin"),
    ("PM", "St. Pierre und Miquelon"), ("VC", "St. Vincent und die Grenadinen"), ("ZA", "Südafrika"),
    ("SD", "Sudan"), ("GS", "Südgeorgien und die Südlichen Sandwichinseln"), ("KR", "Südkorea"),
    ("SS", "Südsudan"), ("SR", "Suriname"), ("SY", "Syrien"),
    ("ST", "São Tomé und Príncipe"), ("TJ", "Tadschikistan"), ("TW", "Taiwan"),
    ("TZ", "Tansania"), ("TH", "Thailand"), ("TL", "Timor-Leste"),
    ("TG", "Togo"), ("TK", "Tokelau"), ("TO", "Tonga"),
    ("TT", "Trinidad und Tobago"), ("TD", "Tschad"), ("CZ", "Tschechien"),
    ("TN", "Tunesien"), ("TR", "Türkei"), ("TM", "Turkmenistan"),
    ("TC", "Turks- und Caicosinseln"), ("TV", "Tuvalu"), ("UG", "Uganda"),
    ("UA", "Ukraine"), ("HU", "Ungarn"), ("UY", "Uruguay"),
    ("US", "USA"), ("UZ", "Usbekistan"), ("VU", "Vanuatu"),
    ("VA", "Vatikanstadt"), ("VE", "Venezuela"), ("AE", "Vereinigte Arabische Emirate"),
    ("VN", "Vietnam"), ("WF", "Wallis und Futuna"), ("CX", "Weihnachtsinsel"),
    ("EH", "Westsahara"), ("CF", "Zentralafrikanische Republik"), ("CY", "Zypern"),
    ("AX", "Ålandinseln"),
]

LAND_NAMEN = dict(LAENDER_ALLE)


def land_name(code) -> str:
    """Klarname zum ISO-Code; unbekannt -> leer."""
    return LAND_NAMEN.get((code or "").strip().upper(), "")


def land_optionen_html(gewaehlt=None, leer_text: str = "– kein Land –") -> str:
    """<option>-Liste: leere Auswahl, dann häufige Reiseländer, dann alle Länder A–Z.
    Ein gespeicherter Wert, der nicht in der Liste steht, bleibt als eigene Option erhalten (kein stiller Datenverlust)."""
    wahl = (gewaehlt or "").strip().upper()

    def opt(code, text):
        sel = " selected" if code == wahl else ""
        return f'<option value="{escape(code)}"{sel}>{escape(text)}</option>'

    teile = [f'<option value=""{"" if wahl else " selected"}>{escape(leer_text)}</option>']
    if wahl and wahl not in LAND_NAMEN:
        teile.append(opt(wahl, f"{wahl} – (unbekannter Wert, bitte prüfen)"))
    teile.append('<optgroup label="Häufige Reiseländer">')
    teile.extend(opt(c, f"{c} – {LAND_NAMEN[c]}") for c in HAEUFIGE if c in LAND_NAMEN)
    teile.append("</optgroup>")
    teile.append('<optgroup label="Alle Länder A–Z">')
    teile.extend(opt(c, f"{c} – {n}") for c, n in LAENDER_ALLE)
    teile.append("</optgroup>")
    return "".join(teile)
