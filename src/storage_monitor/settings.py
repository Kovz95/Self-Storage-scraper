from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = DATA_DIR / "reports"
DB_PATH = DATA_DIR / "storage_monitor.duckdb"


@dataclass(slots=True)
class RunPaths:
    run_date: str
    run_id: str

    @property
    def raw_dir(self) -> Path:
        return RAW_DIR / self.run_date / self.run_id

    @property
    def processed_dir(self) -> Path:
        return PROCESSED_DIR / self.run_date / self.run_id

    @property
    def reports_dir(self) -> Path:
        return REPORTS_DIR / self.run_date / self.run_id


def ensure_base_directories() -> None:
    for path in (DATA_DIR, RAW_DIR, PROCESSED_DIR, REPORTS_DIR):
        path.mkdir(parents=True, exist_ok=True)
