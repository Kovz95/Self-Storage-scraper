from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from storage_monitor.adapters import CubeSmartAdapter, ExtraSpaceAdapter, PublicStorageAdapter
from storage_monitor.models import CrawlRunRecord, FacilityRecord
from storage_monitor.normalization import normalize_unit_snapshot
from storage_monitor.reporting import build_latest_snapshot_summary, compute_weekly_deltas, write_summary_report
from storage_monitor.sampling import select_sampled_facilities
from storage_monitor.settings import DB_PATH, RunPaths, ensure_base_directories
from storage_monitor.storage import StorageRepository
from storage_monitor.utils.files import ensure_dir, models_to_frame, write_dataframe, write_json


@dataclass(slots=True)
class CrawlConfig:
    crawl_mode: str
    sample_size_per_company: int | None = None
    max_markets: int | None = None
    dry_run: bool = False


@dataclass(slots=True)
class CrawlArtifacts:
    run_id: str
    run_date: str
    run_paths: RunPaths
    facilities_df: pd.DataFrame
    raw_df: pd.DataFrame
    normalized_df: pd.DataFrame
    deltas_df: pd.DataFrame
    summary_df: pd.DataFrame
    coverage_df: pd.DataFrame
    quality_notes: list[str]


def build_adapters():
    return [ExtraSpaceAdapter(), PublicStorageAdapter(), CubeSmartAdapter()]


def run_crawl(config: CrawlConfig) -> CrawlArtifacts:
    ensure_base_directories()
    repository = StorageRepository(DB_PATH)
    repository.ensure_tables()

    requested_at = datetime.now(UTC)
    run_id = uuid.uuid4().hex[:12]
    run_date = requested_at.date().isoformat()
    run_paths = RunPaths(run_date=run_date, run_id=run_id)
    for directory in (run_paths.raw_dir, run_paths.processed_dir, run_paths.reports_dir):
        ensure_dir(directory)

    run_record = CrawlRunRecord(
        run_id=run_id,
        crawl_mode=config.crawl_mode,
        sample_size_per_company=config.sample_size_per_company,
        dry_run=config.dry_run,
        requested_at=requested_at,
        started_at=requested_at,
        status="running",
        notes="Started crawl run",
    )
    repository.replace_crawl_run(models_to_frame([run_record]))

    facilities: list[FacilityRecord] = []
    quality_notes: list[str] = []
    adapters = build_adapters()
    for adapter in adapters:
        try:
            discovered = adapter.discover_facilities(observed_at=requested_at)
            facilities.extend(discovered)
            if config.crawl_mode == "full_universe":
                repository.mark_missing_inactive(adapter.company, [facility.facility_id for facility in discovered])
        except Exception as exc:
            quality_notes.append(f"{adapter.company}: facility discovery failed with {exc}.")

    if config.crawl_mode == "sampled_universe":
        selected_facilities = select_sampled_facilities(
            facilities,
            sample_size_per_company=config.sample_size_per_company or 5,
            max_markets=config.max_markets,
        )
    else:
        selected_facilities = facilities

    raw_records = []
    normalized_records = []
    scrape_results: list[FacilityRecord] = []
    failures: list[dict] = []
    scrape_date = requested_at.date()
    adapter_by_company = {adapter.company: adapter for adapter in adapters}

    for facility in selected_facilities:
        adapter = adapter_by_company[facility.company]
        try:
            updated_facility, raw_units = adapter.scrape_facility(facility, scrape_date=scrape_date, run_id=run_id)
            scrape_results.append(updated_facility)
            raw_records.extend(raw_units)
            normalized_records.extend(normalize_unit_snapshot(raw_unit, updated_facility) for raw_unit in raw_units)
        except Exception as exc:
            failures.append(
                {
                    "company": facility.company,
                    "facility_id": facility.facility_id,
                    "source_url": facility.source_url,
                    "error": str(exc),
                }
            )
            quality_notes.append(f"{facility.company} {facility.facility_id}: scrape failed with {exc}.")

    discovered_facility_map = {
        (facility.company, facility.facility_id): facility
        for facility in facilities
    }
    for facility in scrape_results:
        discovered_facility_map[(facility.company, facility.facility_id)] = facility
    facilities_df = models_to_frame(discovered_facility_map.values())
    if not facilities_df.empty:
        facilities_df["raw_metadata"] = facilities_df["raw_metadata"].apply(lambda value: json.dumps(value or {}))
        repository.upsert_facilities(facilities_df)

    raw_df = models_to_frame(raw_records)
    normalized_df = models_to_frame(normalized_records)
    if not raw_df.empty:
        raw_df["raw_metadata"] = raw_df["raw_metadata"].apply(lambda value: json.dumps(value or {}))
        repository.append_dataframe("raw_unit_snapshots", raw_df)
    if not normalized_df.empty:
        repository.append_dataframe("normalized_unit_snapshots", normalized_df)

    previous_run_id = ""
    previous_normalized = pd.DataFrame()
    prior_runs = repository.completed_runs(config.crawl_mode, exclude_run_id=run_id)
    for _, prior_run in prior_runs.iterrows():
        candidate_run_id = str(prior_run["run_id"])
        candidate_normalized = repository.fetch_dataframe(
            "SELECT * FROM normalized_unit_snapshots WHERE run_id = ?",
            [candidate_run_id],
        )
        if candidate_normalized.empty:
            continue
        candidate_dates = set(candidate_normalized["scrape_date"].astype(str).tolist())
        if scrape_date.isoformat() in candidate_dates:
            continue
        previous_run_id = candidate_run_id
        previous_normalized = candidate_normalized
        break
    deltas_df = compute_weekly_deltas(normalized_df, previous_normalized, run_id=run_id, previous_run_id=previous_run_id)
    if not deltas_df.empty:
        repository.append_dataframe("weekly_deltas", deltas_df)

    summary_df = build_latest_snapshot_summary(normalized_df)
    coverage_rows = []
    for company in [adapter.company for adapter in adapters]:
        coverage_rows.append(
            {
                "company": company,
                "facilities_discovered": sum(1 for facility in facilities if facility.company == company),
                "facilities_scraped_successfully": sum(1 for facility in scrape_results if facility.company == company),
                "total_unit_rows_scraped": int((normalized_df["company"] == company).sum()) if not normalized_df.empty else 0,
                "scrape_failures": sum(1 for failure in failures if failure["company"] == company),
            }
        )
    coverage_df = pd.DataFrame(coverage_rows)

    write_dataframe(
        facilities_df,
        run_paths.reports_dir / "facility_universe.csv",
        run_paths.processed_dir / "facility_universe.parquet",
    )
    write_dataframe(
        summary_df,
        run_paths.reports_dir / "latest_snapshot_summary.csv",
        run_paths.processed_dir / "latest_snapshot_summary.parquet",
    )
    write_dataframe(
        deltas_df,
        run_paths.reports_dir / "weekly_deltas.csv",
        run_paths.processed_dir / "weekly_deltas.parquet",
    )
    write_dataframe(
        raw_df,
        run_paths.raw_dir / "raw_unit_snapshots.csv",
        run_paths.raw_dir / "raw_unit_snapshots.parquet",
    )
    write_dataframe(
        normalized_df,
        run_paths.processed_dir / "normalized_unit_snapshots.csv",
        run_paths.processed_dir / "normalized_unit_snapshots.parquet",
    )
    if not coverage_df.empty:
        write_dataframe(coverage_df, run_paths.reports_dir / "coverage_summary.csv")
    if failures:
        write_dataframe(pd.DataFrame(failures), run_paths.reports_dir / "scrape_failures.csv")
    write_json(
        run_paths.reports_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "crawl_mode": config.crawl_mode,
            "sample_size_per_company": config.sample_size_per_company,
            "dry_run": config.dry_run,
            "quality_notes": quality_notes,
        },
    )
    write_summary_report(
        run_paths.reports_dir / "summary_report.md",
        coverage_by_company=coverage_df,
        normalized_df=normalized_df,
        deltas_df=deltas_df,
        facilities_df=facilities_df,
        data_quality_notes=quality_notes,
    )

    final_run_record = run_record.model_copy(
        update={
            "completed_at": datetime.now(UTC),
            "status": "completed",
            "facilities_discovered": len(facilities),
            "facilities_selected": len(selected_facilities),
            "facilities_scraped_successfully": len(scrape_results),
            "facilities_failed": len(failures),
            "units_scraped": len(raw_records),
            "notes": "Completed crawl run",
            "error_summary": None if not failures else f"{len(failures)} facility scrapes failed",
        }
    )
    repository.replace_crawl_run(models_to_frame([final_run_record]))

    return CrawlArtifacts(
        run_id=run_id,
        run_date=run_date,
        run_paths=run_paths,
        facilities_df=facilities_df,
        raw_df=raw_df,
        normalized_df=normalized_df,
        deltas_df=deltas_df,
        summary_df=summary_df,
        coverage_df=coverage_df,
        quality_notes=quality_notes,
    )


def generate_report_for_run(run_id: str) -> Path:
    repository = StorageRepository(DB_PATH)
    normalized_df = repository.fetch_dataframe("SELECT * FROM normalized_unit_snapshots WHERE run_id = ?", [run_id])
    deltas_df = repository.fetch_dataframe("SELECT * FROM weekly_deltas WHERE run_id = ?", [run_id])
    facilities_df = repository.fetch_dataframe("SELECT * FROM facilities")
    coverage_df = (
        normalized_df.groupby("company").size().reset_index(name="total_unit_rows_scraped")
        if not normalized_df.empty
        else pd.DataFrame()
    )
    run_date = normalized_df["scrape_date"].astype(str).iloc[0] if not normalized_df.empty else date.today().isoformat()
    run_paths = RunPaths(run_date=run_date, run_id=run_id)
    ensure_dir(run_paths.reports_dir)
    report_path = run_paths.reports_dir / "summary_report.md"
    write_summary_report(report_path, coverage_df, normalized_df, deltas_df, facilities_df, [])
    return report_path


def generate_report_between_dates(current_run_id: str, previous_run_id: str) -> Path:
    repository = StorageRepository(DB_PATH)
    normalized_df = repository.fetch_dataframe("SELECT * FROM normalized_unit_snapshots WHERE run_id = ?", [current_run_id])
    previous_df = repository.fetch_dataframe("SELECT * FROM normalized_unit_snapshots WHERE run_id = ?", [previous_run_id])
    deltas_df = compute_weekly_deltas(normalized_df, previous_df, current_run_id, previous_run_id)
    run_date = normalized_df["scrape_date"].astype(str).iloc[0] if not normalized_df.empty else date.today().isoformat()
    run_paths = RunPaths(run_date=run_date, run_id=current_run_id)
    ensure_dir(run_paths.reports_dir)
    comparison_path = run_paths.reports_dir / f"summary_report_vs_{previous_run_id}.md"
    facilities_df = repository.fetch_dataframe("SELECT * FROM facilities")
    coverage_df = (
        normalized_df.groupby("company").size().reset_index(name="total_unit_rows_scraped")
        if not normalized_df.empty
        else pd.DataFrame()
    )
    write_summary_report(comparison_path, coverage_df, normalized_df, deltas_df, facilities_df, [])
    return comparison_path
