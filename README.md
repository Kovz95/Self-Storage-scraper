# Self-Storage Pricing Monitor

Weekly equity-research monitor for Extra Space Storage, Public Storage, and CubeSmart.

The project discovers public facilities, scrapes publicly visible unit-level pricing, normalizes operator-specific fields into a common schema, stores weekly history in DuckDB and Parquet, and generates analyst-facing summaries and deltas.

## Setup

The project targets Python 3.12.

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

If you plan to run the scraper against live pages, install Playwright browsers once:

```bash
playwright install chromium
```

## Assumptions

- The system uses only public, unauthenticated pages.
- Facility discovery comes from operator sitemaps and public facility pages.
- `MSA` coverage is proxied by `city, state` unless an external crosswalk is added later.
- Extra Space unit extraction prefers embedded `__NEXT_DATA__`, but still uses the visible unit cards as the public row universe.
- Public Storage pricing is currently sourced from visible HTML price attributes and unit data attributes.
- CubeSmart is implemented with sitemap discovery and HTML parsing, but access can still be environment-dependent if their perimeter tooling challenges the caller IP.
- Some selectors are site-specific and may need tuning as operator page markup changes.
- The project preserves raw operator fields even when normalization is imperfect.

## Commands

The package exposes a `storage-monitor` CLI.

```bash
python -m pip install -e .[dev]
playwright install chromium
storage-monitor dry-run --sample-size-per-company 5 --max-markets 2
storage-monitor weekly-full
storage-monitor report-latest --crawl-mode full_universe
storage-monitor report-between --current-run-id <current_run_id> --previous-run-id <previous_run_id>
pytest
```

If you prefer running through Python directly:

```bash
python -m storage_monitor.cli dry-run --sample-size-per-company 5 --max-markets 2
```

`dry-run` runs a sampled crawl across 2 overlap markets and 5 facilities per operator by default. `weekly-full` runs the full universe crawl. `report-latest` rebuilds the report for the most recent completed run. `report-between` compares two selected runs by ID.

Typical end-to-end local sequence:

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
playwright install chromium
pytest
storage-monitor dry-run --sample-size-per-company 5 --max-markets 2
storage-monitor weekly-full
```

## Data Layout

The pipeline writes dated outputs into:

- `data/raw/YYYY-MM-DD/<run_id>/`
- `data/processed/YYYY-MM-DD/<run_id>/`
- `data/reports/YYYY-MM-DD/<run_id>/`

Typical files include:

- `facility_universe.csv`
- `latest_snapshot_summary.csv`
- `weekly_deltas.csv`
- `summary_report.md`

DuckDB persistence is stored at `data/storage_monitor.duckdb`.

Parquet snapshots are written alongside the CSV outputs in the dated `data/raw` and `data/processed` folders.

## Output Tables

The DuckDB database maintains:

- `facilities`
- `raw_unit_snapshots`
- `normalized_unit_snapshots`
- `weekly_deltas`
- `crawl_runs`

The code appends history and does not overwrite prior snapshots.

## Offline Validation

The repo includes fixture-based adapter tests under `tests/fixtures/` so the parsing and normalization path can be validated without hitting live sites on every change.

Run:

```bash
pytest
```

This covers:

- adapter parsing from local HTML fixtures
- size normalization
- price parsing
- promo parsing
- feature flag parsing
- deterministic `unit_key` generation
- week-over-week delta calculations

## Scheduler

A GitHub Actions workflow is included at `.github/workflows/weekly-storage-monitor.yml`.

It is scheduled for Sundays using UTC cron, with a guard step that only runs the crawl when the current time is Sunday 6:00 AM in `America/New_York`. This avoids DST drift while keeping the intended local run time stable.

Exact workflow behavior:

- scheduled cron entries fire at `10:00 UTC` and `11:00 UTC` on Sunday
- the guard step checks `America/New_York`
- only the run that lands on local Sunday `6:00 AM` continues
- the workflow executes `storage-monitor weekly-full`
- completed runs upload `data/reports/`, `data/processed/`, `data/raw/`, and `data/storage_monitor.duckdb` as a GitHub Actions artifact retained for 30 days

If you prefer local scheduling, use a weekly cron job that invokes:

```bash
storage-monitor weekly-full
```

at Sunday 6:00 AM `America/New_York`.

On Windows Task Scheduler, use:

```powershell
powershell -Command "cd 'C:\path\to\self storage scraper'; .\.venv\Scripts\Activate.ps1; storage-monitor weekly-full"
```

GitHub Actions does not write directly to your PC. If you use the hosted workflow, download the uploaded artifact from the run page to retrieve the reports and database locally.

## Validation

Most recent hardening validation:

- `pytest` passed locally
- `storage-monitor dry-run --sample-size-per-company 1 --max-markets 1` completed successfully
- live sampled outputs were written under dated `data/reports/`

## Known Limitations

- `MSA` remains a city-state proxy until an external geography crosswalk is added.
- Peer comparison only appears when the sampled or scraped markets include overlapping normalized cohorts across operators.
- Public Storage visible online price can be materially below its in-store/list price; the monitor keeps both and uses the visible online/web-comparable price as `best_visible_price_monthly`.
- CubeSmart access may still vary by environment if their anti-bot perimeter blocks the caller before public content is served.
