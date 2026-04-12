"""CSV-lagring med upsert-per-nøkkel og per-år regnskap-filer."""
import csv
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)


class CsvStore:
    """Enkel CSV-fil med upsert keyet på én eller flere kolonner."""

    def __init__(self, path: Path, key_cols: list):
        self.path      = path
        self.key_cols  = key_cols
        self._data: dict = {}        # key_tuple → row_dict
        self._fieldnames: list = []
        self._dirty    = False

    def _key(self, row: dict) -> tuple:
        return tuple(row.get(c, "") for c in self.key_cols)

    def load(self) -> "CsvStore":
        if self.path.exists():
            with open(self.path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                self._fieldnames = list(reader.fieldnames or [])
                for row in reader:
                    self._data[self._key(row)] = row
            log.info("Leste %d rader fra %s", len(self._data), self.path.name)
        return self

    def upsert(self, rows: list) -> tuple:
        """Upsert rader. Returnerer (nye, oppdaterte)."""
        if not rows:
            return 0, 0
        if not self._fieldnames:
            self._fieldnames = list(rows[0].keys())
        new = updated = 0
        for row in rows:
            k = self._key(row)
            if k in self._data:
                updated += 1
            else:
                new += 1
            self._data[k] = row
        self._dirty = True
        return new, updated

    def overwrite(self, rows: list) -> None:
        """Erstatt alt innhold (brukes for roller/eierskap som skrives fersk hver gang)."""
        self._data = {}
        self._fieldnames = []
        self.upsert(rows)

    def save(self) -> None:
        if not self._data or not self._dirty:
            return
        rows = list(self._data.values())
        if not self._fieldnames:
            self._fieldnames = list(rows[0].keys())
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self._fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp, self.path)
        self._dirty = False
        log.info("Skrev %d rader til %s", len(rows), self.path.name)

    def __len__(self) -> int:
        return len(self._data)

    @property
    def rows(self) -> list:
        return list(self._data.values())


class RegnskapsStore:
    """Multi-fil store: én CSV per regnskapsaar. Upsert, aldri slett."""

    def __init__(self, output_dir: Path):
        self.output_dir  = output_dir
        self._buckets: dict = {}    # year_str → {(orgnr, year): row}
        self._dirty: set  = set()   # years som trenger skriving
        self._fieldnames: list = []

    def load(self) -> "RegnskapsStore":
        for path in sorted(self.output_dir.glob("regnskap_*.csv")):
            year   = path.stem.split("_", 1)[1]
            bucket: dict = {}
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    self._fieldnames = list(reader.fieldnames)
                for row in reader:
                    orgnr = row.get("organisasjonsnummer", "")
                    aar   = row.get("regnskapsaar", "")
                    if orgnr and aar:
                        bucket[(orgnr, aar)] = row
            self._buckets[year] = bucket
            log.info("Leste %d rader fra %s", len(bucket), path.name)
        return self

    def upsert(self, rows: list) -> tuple:
        if not rows:
            return 0, 0
        if not self._fieldnames:
            self._fieldnames = list(rows[0].keys())
        new = updated = 0
        for row in rows:
            year  = row.get("regnskapsaar", "")
            orgnr = row.get("organisasjonsnummer", "")
            if not year or not orgnr:
                continue
            bucket = self._buckets.setdefault(year, {})
            key    = (orgnr, year)
            if key in bucket:
                updated += 1
            else:
                new += 1
            bucket[key] = row
            self._dirty.add(year)
        return new, updated

    def save(self) -> None:
        for year in sorted(self._dirty):
            bucket = self._buckets[year]
            path   = self.output_dir / f"regnskap_{year}.csv"
            tmp    = path.with_suffix(".tmp")
            rows   = list(bucket.values())
            with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=self._fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            os.replace(tmp, path)
            log.info("Skrev %d rader til %s", len(rows), path.name)
        self._dirty.clear()

    def __len__(self) -> int:
        return sum(len(b) for b in self._buckets.values())
