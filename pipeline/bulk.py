"""Bulk-nedlasting og streaming-filter for enheter, underenheter og roller."""
import gzip
import json
import logging
import time
from pathlib import Path

import ijson
import requests

log = logging.getLogger(__name__)


# ── Metadata-cache ────────────────────────────────────────────────────────────

def _load_meta(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _save_meta(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2))


# ── Nedlasting ────────────────────────────────────────────────────────────────

def download_bulk(
    url: str,
    dest: Path,
    meta_key: str,
    meta_path: Path,
    ttl_days: int = 7,
    force: bool = False,
) -> Path:
    """Last ned bulk-fil hvis utdatert eller mangler. Returnerer sti til filen."""
    meta  = _load_meta(meta_path)
    entry = meta.get(meta_key, {})
    etag  = entry.get("etag", "")

    if not force and dest.exists() and entry.get("downloaded_at"):
        age_days = (time.time() - entry["downloaded_at"]) / 86400
        if age_days < ttl_days:
            log.info("Bruker cachet %-35s (%.1f dager gammel)", dest.name, age_days)
            return dest

    headers = {"If-None-Match": etag} if etag and dest.exists() else {}

    log.info("Laster ned %s ...", url)
    with requests.get(url, headers=headers, stream=True, timeout=300) as resp:
        if resp.status_code == 304:
            log.info("Ikke endret (304) — bruker cachet %s", dest.name)
            entry["downloaded_at"] = time.time()
            meta[meta_key] = entry
            _save_meta(meta_path, meta)
            return dest
        resp.raise_for_status()
        size = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                f.write(chunk)
                size += len(chunk)

    meta[meta_key] = {
        "downloaded_at": time.time(),
        "etag":          resp.headers.get("etag", ""),
        "size_bytes":    size,
        "url":           url,
    }
    _save_meta(meta_path, meta)
    log.info("Lastet ned %.1f MB → %s", size / 1e6, dest.name)
    return dest


# ── Streaming-filter ──────────────────────────────────────────────────────────

def stream_enheter(path: Path, naeringskoder: set):
    """Yield enheter fra bulk-fil filtrert på næringskode1."""
    log.info("Streamer %s ...", path.name)
    count = total = 0
    with gzip.open(path, "rb") as f:
        for e in ijson.items(f, "item"):
            total += 1
            kode = (e.get("naeringskode1") or {}).get("kode", "")
            if kode in naeringskoder:
                count += 1
                yield e
    log.info("Filtrert %d / %d enheter (næringskode match)", count, total)


def stream_underenheter(path: Path, naeringskoder: set):
    """Yield underenheter fra bulk-fil filtrert på næringskode1."""
    log.info("Streamer %s ...", path.name)
    count = total = 0
    with gzip.open(path, "rb") as f:
        for e in ijson.items(f, "item"):
            total += 1
            kode = (e.get("naeringskode1") or {}).get("kode", "")
            if kode in naeringskoder:
                count += 1
                yield e
    log.info("Filtrert %d / %d underenheter", count, total)


def stream_roller(path: Path, target_orgnrs: set):
    """Yield (orgnr, rollegrupper) for selskaper i target_orgnrs."""
    log.info("Streamer %s ...", path.name)
    count = total = 0
    with gzip.open(path, "rb") as f:
        for item in ijson.items(f, "item"):
            total += 1
            orgnr = item.get("organisasjonsnummer", "")
            if orgnr in target_orgnrs:
                count += 1
                yield orgnr, item.get("rollegrupper", [])
    log.info("Filtrert roller for %d / %d selskaper", count, total)
