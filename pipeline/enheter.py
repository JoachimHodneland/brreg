"""Enhet-innsamling fra bulk-fil."""
import logging
from pathlib import Path

from . import bulk as bulk_mod
from . import extract

log = logging.getLogger(__name__)


def collect_from_bulk(bulk_path: Path, naeringskoder: list) -> tuple:
    """
    Stream bulk-filen og returner (orgnr_set, [enhet_row]).
    Ingen API-kall — alt hentes fra den nedlastede bulk-filen.
    """
    rows: list = []
    orgnrs: set = set()
    for e in bulk_mod.stream_enheter(bulk_path, set(naeringskoder)):
        row = extract.enhet_to_row(e)
        rows.append(row)
        orgnrs.add(row["organisasjonsnummer"])
    log.info("Hentet %d enheter fra bulk", len(rows))
    return orgnrs, rows
