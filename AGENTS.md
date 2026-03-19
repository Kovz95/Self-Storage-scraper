# Project objective

Build a production-grade weekly self-storage pricing monitor for:
- Extra Space Storage
- Public Storage
- CubeSmart

The goal is equity research, not a one-off scrape. The system must discover facilities, scrape public unit-level pricing, normalize the data across operators, store weekly history, and produce week-over-week delta reports.

# Working style

- First inspect the repo and write a short execution plan in `PLAN.md`.
- Then implement the plan without waiting for confirmation.
- Make reasonable assumptions when needed and document them in `README.md`.
- Prefer robust extraction from embedded JSON, structured data, or network responses where available.
- Use Playwright only when needed for rendered pages.
- Keep site-specific logic isolated behind adapters.
- Keep parsing, normalization, storage, and reporting separate.
- Use small, reviewable edits.
- Run tests after major changes.
- If a scraper path is brittle, leave a clear TODO and fall back to the best stable alternative.

# Constraints

- Use only public, unauthenticated pages.
- Respect robots.txt, rate limits, and site terms.
- Do not bypass CAPTCHAs, anti-bot systems, or access controls.
- Build polite retry and throttling behavior.
- Never overwrite historical snapshots.
- Preserve raw operator-specific fields even when normalizing.

# Tech stack

- Python 3.12
- Playwright
- pandas
- DuckDB
- Parquet
- pydantic or dataclasses
- pytest
- structured logging

# Expected project structure

- `src/storage_monitor/`
  - `adapters/`
  - `models/`
  - `normalization/`
  - `storage/`
  - `reporting/`
  - `cli.py`
- `tests/`
- `data/`
- `README.md`
- `requirements.txt` or `pyproject.toml`

# Required outputs

Generate:
- `facility_universe.csv`
- `latest_snapshot_summary.csv`
- `weekly_deltas.csv`
- `summary_report.md`

Store dated outputs under:
- `data/raw/YYYY-MM-DD/`
- `data/processed/YYYY-MM-DD/`
- `data/reports/YYYY-MM-DD/`

# Definition of done

The project is done when:
1. A dry-run works on a small sample.
2. A full-run mode exists.
3. Historical snapshots append correctly.
4. Week-over-week deltas are computed.
5. Tests cover normalization and delta logic.
6. README explains setup, commands, and scheduler usage.
