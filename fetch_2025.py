"""Hent 2025-regnskap for alle orgnr i orgnr.txt."""
import csv
import time
import requests
from pathlib import Path

ORGNR_FILE   = Path("orgnr.txt")
OUTPUT_FILE  = Path("regnskap_2025.csv")
REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
ENHETER_URL  = "https://data.brreg.no/enhetsregisteret/api/enheter"
DELAY = 0.15

FIELDNAMES = [
    "organisasjonsnummer", "navn", "antall_ansatte", "regnskapsaar",
    "sum_driftsinntekter", "sum_driftskostnad", "driftsresultat",
    "sum_finansinntekter", "sum_finanskostnad", "nettofinans",
    "ordinaert_resultat_foer_skatt", "aarsresultat", "totalresultat",
    "sum_anleggsmidler", "sum_omloepsmidler", "sum_eiendeler",
    "innskutt_egenkapital", "sum_egenkapital", "sum_gjeld",
    "sum_egenkapital_gjeld", "sum_langsiktig_gjeld", "sum_kortsiktig_gjeld",
]


def extract_row(r: dict) -> dict:
    v  = r.get("resultatregnskapResultat", {})
    ei = r.get("eiendeler", {})
    ek = r.get("egenkapitalGjeld", {})
    gj = ek.get("gjeldOversikt", {})

    def g(d, *keys):
        for k in keys:
            if not isinstance(d, dict):
                return ""
            d = d.get(k, {})
        return d if not isinstance(d, dict) else ""

    return {
        "organisasjonsnummer":   r.get("virksomhet", {}).get("organisasjonsnummer", ""),
        "navn":                  "",  # fylles inn fra enhetsregisteret
        "antall_ansatte":        "",  # fylles inn fra enhetsregisteret
        "regnskapsaar":          r.get("regnskapsperiode", {}).get("fraDato", "")[:4],
        "sum_driftsinntekter":   g(v, "driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
        "sum_driftskostnad":     g(v, "driftsresultat", "driftskostnad", "sumDriftskostnad"),
        "driftsresultat":        g(v, "driftsresultat", "driftsresultat"),
        "sum_finansinntekter":   g(v, "finansresultat", "finansinntekt", "sumFinansinntekter"),
        "sum_finanskostnad":     g(v, "finansresultat", "finanskostnad", "sumFinanskostnad"),
        "nettofinans":           g(v, "finansresultat", "nettoFinans"),
        "ordinaert_resultat_foer_skatt": g(v, "ordinaertResultatFoerSkattekostnad"),
        "aarsresultat":          g(v, "aarsresultat"),
        "totalresultat":         g(v, "totalresultat"),
        "sum_anleggsmidler":     g(ei, "anleggsmidler", "sumAnleggsmidler"),
        "sum_omloepsmidler":     g(ei, "omloepsmidler", "sumOmloepsmidler"),
        "sum_eiendeler":         g(ei, "sumEiendeler"),
        "innskutt_egenkapital":  g(ek, "egenkapital", "innskuttEgenkapital", "sumInnskuttEgenkaptial"),
        "sum_egenkapital":       g(ek, "egenkapital", "sumEgenkapital"),
        "sum_gjeld":             g(gj, "sumGjeld"),
        "sum_egenkapital_gjeld": g(ek, "sumEgenkapitalGjeld"),
        "sum_langsiktig_gjeld":  g(gj, "langsiktigGjeld", "sumLangsiktigGjeld"),
        "sum_kortsiktig_gjeld":  g(gj, "kortsiktigGjeld", "sumKortsiktigGjeld"),
    }


def fetch_enhet(orgnr: str, session) -> dict:
    try:
        resp = session.get(f"{ENHETER_URL}/{orgnr}", timeout=30)
        if resp.status_code == 200:
            d = resp.json()
            return {"navn": d.get("navn", ""), "antall_ansatte": d.get("antallAnsatte", "")}
    except Exception:
        pass
    return {"navn": "", "antall_ansatte": ""}


def main():
    orgnrs = [l.strip() for l in ORGNR_FILE.read_text().splitlines() if l.strip()]
    print(f"{len(orgnrs)} orgnr å hente")

    session = requests.Session()
    rows = []
    ingen_regnskap = 0
    feil = 0

    for i, orgnr in enumerate(orgnrs, 1):
        try:
            resp = session.get(f"{REGNSKAP_URL}/{orgnr}", timeout=30)
            if resp.status_code == 404:
                ingen_regnskap += 1
            else:
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict):
                    data = [data]
                for r in data:
                    aar = str(r.get("regnskapsperiode", {}).get("fraDato", ""))[:4]
                    if aar == "2025":
                        rows.append(extract_row(r))
        except Exception as e:
            feil += 1
            print(f"  FEIL {orgnr}: {e}")

        if i % 100 == 0:
            print(f"  {i}/{len(orgnrs)} — {len(rows)} 2025-regnskap funnet")

        time.sleep(DELAY)

    # Hent navn og antall_ansatte kun for selskaper med 2025-regnskap
    print(f"Henter enhetsdata for {len(rows)} selskaper...")
    for row in rows:
        enhet = fetch_enhet(row["organisasjonsnummer"], session)
        row.update(enhet)
        time.sleep(DELAY)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nFerdig: {len(rows)} regnskap → {OUTPUT_FILE}")
    print(f"Ingen regnskap: {ingen_regnskap}, feil: {feil}")


if __name__ == "__main__":
    main()
