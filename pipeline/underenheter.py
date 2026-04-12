"""Underenhet-innsamling: bulk + foreldredrevet søk."""
import logging
from pathlib import Path

from . import bulk as bulk_mod
from . import extract
from .http import api_get
import config

log = logging.getLogger(__name__)


def collect_from_bulk(bulk_path: Path, naeringskoder: list) -> tuple:
    """Stream underenheter fra bulk-fil filtrert på næringskode."""
    rows: list = []
    orgnrs: set = set()
    for e in bulk_mod.stream_underenheter(bulk_path, set(naeringskoder)):
        row = extract.underenhet_to_row(e)
        rows.append(row)
        orgnrs.add(row["organisasjonsnummer"])
    log.info("Hentet %d underenheter fra bulk", len(rows))
    return orgnrs, rows


def collect_for_parents(
    parent_orgnrs: set,
    session,
    existing_orgnrs: set,
) -> list:
    """
    Hent underenheter for kjente foreldreselskaper via API.
    Fanger filialer med annen næringskode enn det vi søker på.
    """
    rows: list = []
    total = len(parent_orgnrs)

    for i, orgnr in enumerate(sorted(parent_orgnrs), 1):
        page = 0
        while True:
            resp = api_get(
                session,
                config.UNDERENHETER_URL,
                params={"overordnetEnhet": orgnr, "size": config.PAGE_SIZE, "page": page},
                delay=config.REQUEST_DELAY,
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            batch = data.get("_embedded", {}).get("underenheter", [])
            for e in batch:
                sub_orgnr = e.get("organisasjonsnummer", "")
                if sub_orgnr and sub_orgnr not in existing_orgnrs:
                    rows.append(extract.underenhet_to_row(e))
                    existing_orgnrs.add(sub_orgnr)
            meta = data.get("page", {})
            if page + 1 >= meta.get("totalPages", 1):
                break
            page += 1

        if i % 50 == 0 or i == total:
            log.info("Underenheter (foreldre): %d / %d", i, total)

    log.info("Hentet %d ekstra underenheter via foreldresøk", len(rows))
    return rows
