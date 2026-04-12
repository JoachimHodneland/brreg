from pathlib import Path

# ── Næringskoder ──────────────────────────────────────────────────────────────
NAERINGSKODER = ["47.810", "46.710"]   # detaljhandel + engroshandel motorvogner

# ── API-endepunkter ───────────────────────────────────────────────────────────
BASE        = "https://data.brreg.no"
ENH_BASE    = f"{BASE}/enhetsregisteret/api"
REGNSK_BASE = f"{BASE}/regnskapsregisteret/regnskap"

ENHETER_URL       = f"{ENH_BASE}/enheter"
UNDERENHETER_URL  = f"{ENH_BASE}/underenheter"
OPPDATERINGER_URL = f"{ENH_BASE}/oppdateringer/enheter"

BULK_ENHETER_URL      = f"{ENHETER_URL}/lastned"
BULK_UNDERENHETER_URL = f"{UNDERENHETER_URL}/lastned"
BULK_ROLLER_URL       = f"{ENH_BASE}/roller/totalbestand"

REGNSKAP_URL = REGNSK_BASE
PDF_AAR_URL  = f"{REGNSK_BASE}/aarsregnskap/kopi"

# ── Ytelse ────────────────────────────────────────────────────────────────────
REQUEST_DELAY = 0.15   # sekunder mellom individuelle API-kall
BULK_TTL_DAYS = 7      # dager før bulk-filer lastes ned på nytt
PAGE_SIZE     = 1000

# ── Mapper ────────────────────────────────────────────────────────────────────
DATA_DIR       = Path("data")
RAW_DIR        = DATA_DIR / "raw"
CHECKPOINT_DIR = DATA_DIR / "checkpoint"
OUTPUT_DIR     = DATA_DIR / "output"

BULK_ENHETER_PATH      = RAW_DIR / "enheter_bulk.json.gz"
BULK_UNDERENHETER_PATH = RAW_DIR / "underenheter_bulk.json.gz"
BULK_ROLLER_PATH       = RAW_DIR / "roller_bulk.json.gz"
BULK_META_PATH         = RAW_DIR / "bulk_meta.json"

CHECKPOINT_PATH = CHECKPOINT_DIR / "run_state.json"
REGNSKAP_QUEUE  = CHECKPOINT_DIR / "regnskap_queue.txt"
