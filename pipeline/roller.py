"""Roller-innsamling fra bulk-fil."""
import logging
from pathlib import Path

from . import bulk as bulk_mod
from . import extract

log = logging.getLogger(__name__)


def collect_from_bulk(bulk_path: Path, target_orgnrs: set) -> tuple:
    """
    Stream roller-bulk og returner (alle_roller_rader, eierskap_rader).
    Eierskap-radene er filtrert til eierrolle-typer.
    """
    all_rows: list = []
    eierskap_rows: list = []

    for orgnr, rollegrupper in bulk_mod.stream_roller(bulk_path, target_orgnrs):
        rows = extract.roller_to_rows(orgnr, rollegrupper)
        all_rows.extend(rows)
        eierskap_rows.extend(r for r in rows if extract.is_eierskap(r))

    log.info(
        "Roller: %d rader totalt, %d eierskap-rader",
        len(all_rows), len(eierskap_rows),
    )
    return all_rows, eierskap_rows
