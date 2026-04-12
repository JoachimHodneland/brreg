"""Hent 2025-regnskap for alle orgnr i orgnr.txt."""
import csv
import time
import requests
from pathlib import Path

ORGNR_FILE = Path("orgnr.txt")
OUTPUT_FILE = Path("regnskap_2025.csv")
REGNSKAP_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DELAY = 0.15

FIELDNAMES = [
    "organisasjonsnummer", "navn", "regnskapsaar",
    "sum_driftsinntekter", "sum_driftskostnad", "driftsresultat",
    "sum_finansinntekter", "sum_finanskostnad",
    "ordinaert_resultat_foer_skatt", "aarsresultat",
    "sum_eiendeler", "sum_anleggsmidler", "sum_omloepsmidler",
    "sum_egenkapital", "sum_gjeld", "sum_kortsiktig_gjeld",
    "sum_langsiktig_gjeld",
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
        "organisasjonsnummer": r.get("virksomhet", {}).get("organisasjonsnummer", ""),
        "navn":                r.get("virksomhet", {}).get("navn", ""),
        "regnskapsaar":        r.get("regnskapsperiode", {}).get("fraDato", "")[:4],
        "sum_driftsinntekter": g(v, "driftsresultat", "driftsinntekter", "sumDriftsinntekter"),
        "sum_driftskostnad":   g(v, "driftsresultat", "driftskostnad", "sumDriftskostnad"),
        "driftsresultat":      g(v, "driftsresultat", "driftsresultat"),
        "sum_finansinntekter": g(v, "finansresultat", "finansinntekt", "sumFinansinntekter"),
        "sum_finanskostnad":   g(v, "finansresultat", "finanskostnad", "sumFinanskostnad"),
        "ordinaert_resultat_foer_skatt": g(v, "ordinaertResultatFoerSkattekostnad"),
        "aarsresultat":        g(v, "aarsresultat"),
        "sum_eiendeler":       g(ei, "sumEiendeler"),
        "sum_anleggsmidler":   g(ei, "anleggsmidler", "sumAnleggsmidler"),
        "sum_omloepsmidler":   g(ei, "omloepsmidler", "sumOmloepsmidler"),
        "sum_egenkapital":     g(ek, "egenkapital", "sumEgenkapital"),
        "sum_gjeld":           g(gj, "sumGjeld"),
        "sum_kortsiktig_gjeld": g(gj, "kortsiktigGjeld", "sumKortsiktigGjeld"),
        "sum_langsiktig_gjeld": g(gj, "langsiktigGjeld", "sumLangsiktigGjeld"),
    }


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

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nFerdig: {len(rows)} regnskap → {OUTPUT_FILE}")
    print(f"Ingen regnskap: {ingen_regnskap}, feil: {feil}")


if __name__ == "__main__":
    main()
