"""One-time import of historical backup CSVs (2023.csv, 2024.csv) into regnskap_*.csv files."""
import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from pipeline.storage import RegnskapsStore

OUTPUT = Path("data/output")

# Column mapping: backup column → regnskap schema column
COL_MAP = {
    "OrgNr": "organisasjonsnummer",
    "Navn": "navn",
    "NACE_kode": "naeringskode",
    "AntallAnsatte": "antall_ansatte",
    "aar": "regnskapsaar",
    "driftsinntekter": "sum_driftsinntekter",
    "driftskostnad": "sum_driftskostnad",
    "driftsresultat": "driftsresultat",
    "finansinntekter": "sum_finansinntekter",
    "finanskostnader": "sum_finanskostnad",
    "resultatFørSkatt": "ordinaert_resultat_foer_skatt",
    "aarsresultat": "aarsresultat",
    "anleggsmidler": "sum_anleggsmidler",
    "omlopsmidler": "sum_omloepsmidler",
    "sumEiendeler": "sum_eiendeler",
    "egenkapital": "sum_egenkapital",
    "sumGjeld": "sum_gjeld",
    "langsiktigGjeld": "sum_langsiktig_gjeld",
    "kortsiktigGjeld": "sum_kortsiktig_gjeld",
}

# The full set of fieldnames expected by regnskap_*.csv (from regnskap_to_row in extract.py)
FIELDNAMES = [
    "organisasjonsnummer", "navn", "naeringskode", "organisasjonsform",
    "kommune", "postnummer", "poststed", "antall_ansatte",
    "overordnet_enhet", "er_i_konsern", "regnskapsaar", "fra_dato",
    "til_dato", "regnskapstype", "oppstillingsplan",
    "sum_driftsinntekter", "sum_driftskostnad", "driftsresultat",
    "sum_finansinntekter", "sum_finanskostnad",
    "ordinaert_resultat_foer_skatt", "aarsresultat",
    "sum_eiendeler", "sum_anleggsmidler", "sum_omloepsmidler",
    "sum_egenkapital", "sum_gjeld", "sum_kortsiktig_gjeld",
    "sum_langsiktig_gjeld", "pdf_aar_tilgjengelig", "hentet_dato",
]


def fix_mojibake(raw_bytes: bytes) -> str:
    """Fix double-encoded UTF-8 (mojibake): decode latin-1 → re-encode → decode utf-8."""
    try:
        return raw_bytes.decode("utf-8").encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return raw_bytes.decode("utf-8-sig")


def read_backup(path: str) -> list[dict]:
    """Read a backup CSV, fix encoding, map columns, skip % columns."""
    raw = Path(path).read_bytes()
    # Strip BOM if present
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    text = fix_mojibake(raw)

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for src in reader:
        row = {fn: "" for fn in FIELDNAMES}
        for src_col, dst_col in COL_MAP.items():
            val = src.get(src_col, "")
            # The mojibake fix should handle "FørSkatt" → "FørSkatt"
            # but we also need to check the mangled version
            if not val and src_col == "resultatFørSkatt":
                val = src.get("resultatFÃ¸rSkatt", "")
            row[dst_col] = val
        row["hentet_dato"] = "backup"
        rows.append(row)
    return rows


def main():
    store = RegnskapsStore(OUTPUT)
    store._fieldnames = FIELDNAMES
    store.load()

    for backup_file in ["2023.csv", "2024.csv"]:
        p = Path(backup_file)
        if not p.exists():
            print(f"SKIP: {backup_file} not found")
            continue

        rows = read_backup(backup_file)
        print(f"Read {len(rows)} rows from {backup_file}")

        # Only upsert rows that don't already exist (backup is lower priority than API data)
        new_rows = []
        for row in rows:
            orgnr = row["organisasjonsnummer"]
            year = row["regnskapsaar"]
            bucket = store._buckets.get(year, {})
            if (orgnr, year) not in bucket:
                new_rows.append(row)

        new, updated = store.upsert(new_rows)
        print(f"  → {new} new, {updated} updated (skipped {len(rows) - len(new_rows)} already in API data)")

    store.save()
    print("Done!")


if __name__ == "__main__":
    main()
