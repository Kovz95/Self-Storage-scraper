from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_dataframe(df: pd.DataFrame, csv_path: Path, parquet_path: Path | None = None) -> None:
    ensure_dir(csv_path.parent)
    df.to_csv(csv_path, index=False)
    if parquet_path is not None:
        ensure_dir(parquet_path.parent)
        df.to_parquet(parquet_path, index=False)


def write_json(path: Path, payload: object) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def models_to_frame(records: Iterable[object]) -> pd.DataFrame:
    rows = []
    for record in records:
        if hasattr(record, "model_dump"):
            rows.append(record.model_dump())
        else:
            rows.append(record)
    return pd.DataFrame(rows)
