"""Inkrementell oppdatering via oppdateringer-endepunktet."""
import logging

from .http import api_get
import config

log = logging.getLogger(__name__)


def fetch_changed_orgnrs(since_date: str, session) -> list:
    """
    Returner alle org.nr som er endret siden angitt dato (ISO-format: YYYY-MM-DD).
    Paginerer automatisk gjennom alle sider.
    """
    orgnrs: list = []
    page = 0
    while True:
        resp = api_get(
            session,
            config.OPPDATERINGER_URL,
            params={"dato": since_date, "size": config.PAGE_SIZE, "page": page},
            delay=config.REQUEST_DELAY,
        )
        if resp.status_code != 200:
            log.warning("oppdateringer returnerte HTTP %d", resp.status_code)
            break
        data   = resp.json()
        batch  = data.get("_embedded", {}).get("oppdaterteEnheter", [])
        orgnrs.extend(e.get("organisasjonsnummer", "") for e in batch)
        meta   = data.get("page", {})
        total_pages = meta.get("totalPages", 1)
        log.debug("oppdateringer side %d/%d", page + 1, total_pages)
        if page + 1 >= total_pages:
            break
        page += 1

    result = [o for o in orgnrs if o]
    log.info("Fant %d endrede enheter siden %s", len(result), since_date)
    return result
