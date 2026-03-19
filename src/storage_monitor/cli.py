from __future__ import annotations

import argparse

from storage_monitor.logging_utils import configure_logging
from storage_monitor.pipeline import CrawlConfig, generate_report_between_dates, generate_report_for_run, run_crawl
from storage_monitor.settings import DB_PATH
from storage_monitor.storage import StorageRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Weekly self-storage pricing monitor")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    dry_run = subparsers.add_parser("dry-run", help="Sampled dry run: 2 overlap markets, 5 facilities per operator")
    dry_run.add_argument("--sample-size-per-company", type=int, default=5)
    dry_run.add_argument("--max-markets", type=int, default=2)

    subparsers.add_parser("weekly-full", help="Full-universe weekly crawl")

    report_latest = subparsers.add_parser("report-latest", help="Rebuild the report for the latest completed run")
    report_latest.add_argument("--crawl-mode", default="full_universe")

    report_dates = subparsers.add_parser("report-between", help="Build a comparison report for two run IDs")
    report_dates.add_argument("--current-run-id", required=True)
    report_dates.add_argument("--previous-run-id", required=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    if args.command == "dry-run":
        artifacts = run_crawl(
            CrawlConfig(
                crawl_mode="sampled_universe",
                sample_size_per_company=args.sample_size_per_company,
                max_markets=args.max_markets,
                dry_run=True,
            )
        )
        print(artifacts.run_paths.reports_dir)
        return

    if args.command == "weekly-full":
        artifacts = run_crawl(CrawlConfig(crawl_mode="full_universe", dry_run=False))
        print(artifacts.run_paths.reports_dir)
        return

    if args.command == "report-latest":
        repository = StorageRepository(DB_PATH)
        latest = repository.latest_completed_run(args.crawl_mode)
        if latest.empty:
            raise SystemExit(f"No completed runs found for crawl mode '{args.crawl_mode}'.")
        report_path = generate_report_for_run(str(latest.iloc[0]["run_id"]))
        print(report_path)
        return

    if args.command == "report-between":
        report_path = generate_report_between_dates(args.current_run_id, args.previous_run_id)
        print(report_path)
        return


if __name__ == "__main__":
    main()
