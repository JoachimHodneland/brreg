"""Årsregnskap: individuelle kall per org.nr."""
import logging

from .http import api_get
from . import extract
import config

log = logging.getLogger(__name__)


def fetch_regnskap(orgnr: str, session) -> list:
    """Hent alle tilgjengelige årsregnskap for ett org.nr (returnerer liste)."""
    resp = api_get(session, f"{config.REGNSKAP_URL}/{orgnr}", delay=config.REQUEST_DELAY)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def fetch_pdf_years(orgnr: str, session) -> str:
    """Hent liste over år hvor PDF-kopi av regnskap er tilgjengelig."""
    resp = api_get(
        session,
        f"{config.PDF_AAR_URL}/{orgnr}/aar",
        delay=config.REQUEST_DELAY,
    )
    if resp.status_code != 200:
        return ""
    data = resp.json()
    if isinstance(data, list):
        return "|".join(sorted(str(y) for y in data))
    return ""


def process_orgnr(orgnr: str, enhet_row: dict, session) -> list:
    """
    Hent regnskap + PDF-år for ett selskap.
    Returnerer liste av regnskap-rader (vanligvis 1, kan være 0).
    """
    regnskaper = fetch_regnskap(orgnr, session)
    pdf_years  = fetch_pdf_years(orgnr, session)

    rows = []
    for r in regnskaper:
        row = extract.regnskap_to_row(r, enhet_row)
        row["pdf_aar_tilgjengelig"] = pdf_years
        rows.append(row)
    return rows
