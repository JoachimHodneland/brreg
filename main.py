#!/usr/bin/env python3
"""
Henter all offentlig tilgjengelig data om norske bilforhandlere
fra Brønnøysundregistrene.

Faser:
  1. Bulk-nedlasting   – enheter, underenheter, roller (1-3 kall)
  2. Filtrer enheter   – stream bulk, behold næringskodene vi vil ha
  3. Underenheter      – bulk + foreldredrevet søk for filialer
  4. Roller            – konsernstruktur og eierskap fra bulk
  5. Årsregnskap       – individuelle kall per selskap (~12 000 kall)
  6. Ferdigstill       – avledede felt, skriv run_state.json, statistikk

Bruk:
  python main.py                        # auto: full første gang, inkrementell senere
  python main.py --full                 # tving full kjøring
  python main.py --skip-regnskap        # bare enhet/roller/underenheter
  python main.py --orgnr 943733988      # test på ett selskap
  python main.py --dry-run              # vis plan uten API-kall
  python main.py --stats                # vis statistikk fra eksisterende filer
"""

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import config
from pipeline.http import build_session
from pipeline.storage import CsvStore, RegnskapsStore
from pipeline import bulk as bulk_mod
from pipeline import enheter as enheter_mod
from pipeline import underenheter as underenheter_mod
from pipeline import roller as roller_mod
from pipeline import regnskap as regnskap_mod
from pipeline import incremental

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Hent data om norske bilforhandlere fra Brreg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    g = p.add_argument_group("kjøringsmodus")
    g.add_argument("--full",        action="store_true", help="Tving full kjøring")
    g.add_argument("--incremental", action="store_true", help="Tving inkrementell kjøring")
    g.add_argument("--restart",     action="store_true", help="Slett checkpoint, start på nytt")

    g = p.add_argument_group("fase-kontroll")
    g.add_argument("--skip-bulk",          action="store_true", help="Bruk cachet bulk-filer")
    g.add_argument("--force-bulk",         action="store_true", help="Last ned bulk på nytt")
    g.add_argument("--skip-underenheter",  action="store_true")
    g.add_argument("--skip-roller",        action="store_true")
    g.add_argument("--skip-regnskap",      action="store_true")

    g = p.add_argument_group("omfang")
    g.add_argument("--naeringskoder", default=None,
                   help="Kommaseparert, f.eks. 47.810,46.710")
    g.add_argument("--orgnr",         default=None,
                   help="Test med spesifikke org.nr (kommaseparert)")

    g = p.add_argument_group("ytelse")
    g.add_argument("--delay",         type=float, default=config.REQUEST_DELAY,
                   help=f"Sekunder mellom kall (standard: {config.REQUEST_DELAY})")
    g.add_argument("--workers",       type=int,   default=1,
                   help="Parallelle regnskap-kall, maks 3")
    g.add_argument("--bulk-ttl-days", type=int,   default=config.BULK_TTL_DAYS,
                   help=f"Dager før re-nedlasting av bulk (standard: {config.BULK_TTL_DAYS})")

    g = p.add_argument_group("diagnostikk")
    g.add_argument("--dry-run",   action="store_true", help="Vis plan, ingen API-kall")
    g.add_argument("--stats",     action="store_true", help="Vis filstatistikk og avslutt")
    g.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    return p.parse_args()


# ── Checkpoint ────────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if config.CHECKPOINT_PATH.exists():
        return json.loads(config.CHECKPOINT_PATH.read_text())
    return {}


def save_checkpoint(state: dict) -> None:
    config.CHECKPOINT_PATH.write_text(json.dumps(state, indent=2, default=str))


def load_queue() -> list:
    if config.REGNSKAP_QUEUE.exists():
        lines = config.REGNSKAP_QUEUE.read_text().splitlines()
        return [l.strip() for l in lines if l.strip()]
    return []


def save_queue(items: list) -> None:
    config.REGNSKAP_QUEUE.write_text("\n".join(items))


def clear_queue() -> None:
    if config.REGNSKAP_QUEUE.exists():
        config.REGNSKAP_QUEUE.unlink()


# ── Statistikk ────────────────────────────────────────────────────────────────

def print_stats() -> None:
    log.info("── Filstatistikk ──")
    for path in sorted(config.OUTPUT_DIR.glob("*.csv")):
        try:
            n = sum(1 for _ in open(path, encoding="utf-8-sig")) - 1
            log.info("  %-45s %6d rader", path.name, n)
        except Exception:
            pass


# ── Faser ─────────────────────────────────────────────────────────────────────

def phase_bulk(args) -> None:
    kw = dict(
        meta_path=config.BULK_META_PATH,
        ttl_days=args.bulk_ttl_days,
        force=args.force_bulk,
    )
    bulk_mod.download_bulk(config.BULK_ENHETER_URL,  config.BULK_ENHETER_PATH,  "enheter",  **kw)
    bulk_mod.download_bulk(config.BULK_ROLLER_URL,   config.BULK_ROLLER_PATH,   "roller",   **kw)
    try:
        bulk_mod.download_bulk(
            config.BULK_UNDERENHETER_URL,
            config.BULK_UNDERENHETER_PATH,
            "underenheter", **kw,
        )
    except Exception as exc:
        log.warning("Underenheter bulk ikke tilgjengelig (%s) — bruker API-søk", exc)


def phase_underenheter(target_orgnrs: set, enheter_store: CsvStore, session) -> list:
    all_rows: list = []
    existing: set  = set()

    # Pass 1: fra bulk (om tilgjengelig)
    if config.BULK_UNDERENHETER_PATH.exists():
        _, rows = underenheter_mod.collect_from_bulk(
            config.BULK_UNDERENHETER_PATH, config.NAERINGSKODER,
        )
        all_rows.extend(rows)
        existing.update(r["organisasjonsnummer"] for r in rows)
        log.info("Pass 1 (bulk): %d underenheter", len(rows))
    else:
        # Fallback: paginer /underenheter?naeringskode=...
        from pipeline.http import api_get
        for kode in config.NAERINGSKODER:
            page = 0
            while True:
                resp = api_get(session, config.UNDERENHETER_URL,
                               params={"naeringskode": kode, "size": config.PAGE_SIZE, "page": page},
                               delay=config.REQUEST_DELAY)
                if resp.status_code != 200:
                    break
                data  = resp.json()
                batch = data.get("_embedded", {}).get("underenheter", [])
                for e in batch:
                    from pipeline import extract
                    row = extract.underenhet_to_row(e)
                    orgnr = row["organisasjonsnummer"]
                    if orgnr not in existing:
                        all_rows.append(row)
                        existing.add(orgnr)
                meta = data.get("page", {})
                if page + 1 >= meta.get("totalPages", 1):
                    break
                page += 1
        log.info("Pass 1 (API-søk): %d underenheter", len(all_rows))

    # Pass 2: foreldreselskaper med er_i_konsern=True
    parent_orgnrs = {
        row["organisasjonsnummer"]
        for row in enheter_store.rows
        if str(row.get("er_i_konsern", "")).lower() == "true"
        and row["organisasjonsnummer"] in target_orgnrs
    }
    extra = underenheter_mod.collect_for_parents(parent_orgnrs, session, existing)
    all_rows.extend(extra)

    return all_rows


def phase_regnskap(
    orgnr_list: list,
    enheter_lookup: dict,
    regnskap_store: RegnskapsStore,
    session,
    workers: int = 1,
    delay: float = 0.15,
) -> None:
    # Bruk eksisterende kø hvis avbrutt kjøring
    remaining = load_queue() or orgnr_list[:]
    total     = len(remaining)
    ingen = feil = 0
    start = time.monotonic()

    if workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        workers = min(workers, 3)

        def _process(orgnr):
            return orgnr, regnskap_mod.process_orgnr(orgnr, enheter_lookup.get(orgnr, {}), session)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process, o): o for o in remaining}
            done = 0
            for fut in as_completed(futures):
                try:
                    orgnr, rows = fut.result()
                    if rows:
                        regnskap_store.upsert(rows)
                    else:
                        ingen += 1
                except Exception as exc:
                    log.warning("Feil: %s", exc)
                    feil += 1
                done += 1
                if done % 50 == 0:
                    regnskap_store.save()
                if done % 200 == 0 or done == total:
                    _log_eta(done, total, start, ingen, feil)
    else:
        for i, orgnr in enumerate(remaining, 1):
            try:
                rows = regnskap_mod.process_orgnr(orgnr, enheter_lookup.get(orgnr, {}), session)
                if rows:
                    regnskap_store.upsert(rows)
                else:
                    ingen += 1
            except Exception as exc:
                log.warning("Feil for %s: %s", orgnr, exc)
                feil += 1

            if i % 50 == 0:
                regnskap_store.save()
                save_queue(remaining[i:])    # oppdater kø for resume

            if i % 200 == 0 or i == total:
                _log_eta(i, total, start, ingen, feil)

    regnskap_store.save()
    clear_queue()
    log.info("Regnskap ferdig — %d ingen regnskap, %d feil", ingen, feil)


def _log_eta(done: int, total: int, start: float, ingen: int, feil: int) -> None:
    elapsed = time.monotonic() - start
    rate    = done / elapsed if elapsed > 0 else 1
    eta_min = int((total - done) / rate / 60)
    log.info("Regnskap %d/%d  |  ETA ~%d min  |  %d ingen  |  %d feil",
             done, total, eta_min, ingen, feil)


# ── Hoved ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Opprett mapper
    for d in [config.RAW_DIR, config.CHECKPOINT_DIR, config.OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    if args.stats:
        print_stats()
        return

    if args.dry_run:
        log.info("DRY RUN — ingen API-kall")
        log.info("Næringskoder : %s", config.NAERINGSKODER)
        log.info("Bulk TTL     : %d dager", args.bulk_ttl_days)
        log.info("Delay        : %.2f s", args.delay)
        log.info("Workers      : %d", args.workers)
        log.info("Output       : %s", config.OUTPUT_DIR)
        return

    # Checkpoint og kjøringsmodus
    if args.restart and config.CHECKPOINT_PATH.exists():
        config.CHECKPOINT_PATH.unlink()
        log.info("Checkpoint slettet")

    checkpoint     = {} if args.restart else load_checkpoint()
    naeringskoder  = args.naeringskoder.split(",") if args.naeringskoder else config.NAERINGSKODER
    run_id         = str(date.today())
    incremental_mode = (
        not args.full
        and bool(checkpoint.get("completed_at"))
        and not args.incremental is False
    ) or args.incremental

    log.info("=== Brreg bilforhandler-pipeline [%s] ===", run_id)
    log.info("Næringskoder : %s", naeringskoder)
    log.info("Modus        : %s", "INKREMENTELL" if incremental_mode else "FULL")

    session = build_session()

    # Overrid delay globalt
    import pipeline.http as _http
    _http._last_call = 0.0

    # ── Fase 1: Bulk-nedlasting ───────────────────────────────────────────────
    log.info("── Fase 1: Bulk-nedlasting ──")
    if not args.skip_bulk:
        phase_bulk(args)
    else:
        log.info("Hopper over (--skip-bulk)")

    # ── Fase 2: Filtrer enheter ───────────────────────────────────────────────
    log.info("── Fase 2: Filtrer enheter ──")
    enheter_store = CsvStore(config.OUTPUT_DIR / "enheter.csv", ["organisasjonsnummer"])
    enheter_store.load()

    orgnrs, enhet_rows = enheter_mod.collect_from_bulk(config.BULK_ENHETER_PATH, naeringskoder)

    # Inkrementell: begrens til endrede org.nr
    if incremental_mode and checkpoint.get("completed_at"):
        changed = set(incremental.fetch_changed_orgnrs(checkpoint["completed_at"], session))
        orgnrs     = orgnrs & changed
        enhet_rows = [r for r in enhet_rows if r["organisasjonsnummer"] in orgnrs]
        log.info("Inkrementell: %d endrede enheter å prosessere", len(orgnrs))

    # Testmodus: begrens til spesifikke org.nr
    if args.orgnr:
        test_set   = set(args.orgnr.split(","))
        orgnrs     = orgnrs & test_set if orgnrs & test_set else test_set
        enhet_rows = [r for r in enhet_rows if r["organisasjonsnummer"] in orgnrs]
        log.info("Test-modus: %d org.nr", len(orgnrs))

    enheter_store.upsert(enhet_rows)
    enheter_store.save()
    enheter_lookup = {r["organisasjonsnummer"]: r for r in enhet_rows}

    # ── Fase 3: Underenheter ──────────────────────────────────────────────────
    if not args.skip_underenheter:
        log.info("── Fase 3: Underenheter ──")
        ue_store = CsvStore(config.OUTPUT_DIR / "underenheter.csv", ["organisasjonsnummer"])
        ue_store.load()
        ue_rows = phase_underenheter(orgnrs, enheter_store, session)
        ue_store.upsert(ue_rows)
        ue_store.save()
    else:
        log.info("── Fase 3: Hopper over underenheter ──")
        ue_store = None

    # ── Fase 4: Roller ────────────────────────────────────────────────────────
    if not args.skip_roller:
        log.info("── Fase 4: Roller og eierskap ──")
        roller_store   = CsvStore(config.OUTPUT_DIR / "roller.csv",
                                  ["orgnr_selskap", "rolle_type_kode", "fodselsdato",
                                   "etternavn", "enhet_orgnr", "rekkefolge"])
        eierskap_store = CsvStore(config.OUTPUT_DIR / "eierskap.csv",
                                  ["orgnr_selskap", "rolle_type_kode", "fodselsdato",
                                   "etternavn", "enhet_orgnr"])
        roller_rows, eierskap_rows = roller_mod.collect_from_bulk(config.BULK_ROLLER_PATH, orgnrs)
        roller_store.overwrite(roller_rows)
        eierskap_store.overwrite(eierskap_rows)
        roller_store.save()
        eierskap_store.save()
    else:
        log.info("── Fase 4: Hopper over roller ──")

    # ── Fase 5: Årsregnskap ───────────────────────────────────────────────────
    if not args.skip_regnskap:
        log.info("── Fase 5: Årsregnskap (%d selskaper) ──", len(orgnrs))
        regnskap_store = RegnskapsStore(config.OUTPUT_DIR)
        regnskap_store.load()
        phase_regnskap(
            sorted(orgnrs), enheter_lookup, regnskap_store,
            session, args.workers, args.delay,
        )
    else:
        log.info("── Fase 5: Hopper over regnskap ──")

    # ── Fase 6: Ferdigstill ───────────────────────────────────────────────────
    log.info("── Fase 6: Ferdigstiller ──")

    if ue_store and len(ue_store) > 0:
        ue_counts: dict = {}
        for row in ue_store.rows:
            parent = row.get("overordnet_enhet", "")
            if parent:
                ue_counts[parent] = ue_counts.get(parent, 0) + 1
        updated = 0
        for key, row in enheter_store._data.items():
            orgnr = key[0]
            count = ue_counts.get(orgnr, 0)
            if count:
                row["antall_underenheter"] = count
                updated += 1
        if updated:
            enheter_store._dirty = True
            enheter_store.save()
            log.info("Oppdatert antall_underenheter for %d selskaper", updated)

    save_checkpoint({
        "run_id":          run_id,
        "completed_at":    str(date.today()),
        "naeringskoder":   naeringskoder,
        "total_enheter":   len(enheter_store),
        "run_mode":        "incremental" if incremental_mode else "full",
    })

    log.info("=== Pipeline ferdig ===")
    print_stats()


if __name__ == "__main__":
    main()
