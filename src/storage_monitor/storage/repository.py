from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pandas as pd


class StorageRepository:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    @contextmanager
    def connect(self):
        try:
            import duckdb
        except ImportError as exc:
            raise RuntimeError("duckdb is required. Install project dependencies before running the monitor.") from exc
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.database_path))
        try:
            yield con
        finally:
            con.close()

    def ensure_tables(self) -> None:
        with self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS facilities (
                    company VARCHAR,
                    facility_id VARCHAR,
                    facility_name VARCHAR,
                    address VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    zip VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    source_url VARCHAR,
                    first_seen_at TIMESTAMP,
                    last_seen_at TIMESTAMP,
                    active_flag BOOLEAN,
                    market_key VARCHAR,
                    msa VARCHAR,
                    raw_metadata JSON
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_runs (
                    run_id VARCHAR,
                    crawl_mode VARCHAR,
                    sample_size_per_company BIGINT,
                    dry_run BOOLEAN,
                    requested_at TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    status VARCHAR,
                    facilities_discovered BIGINT,
                    facilities_selected BIGINT,
                    facilities_scraped_successfully BIGINT,
                    facilities_failed BIGINT,
                    units_scraped BIGINT,
                    notes VARCHAR,
                    error_summary VARCHAR
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_unit_snapshots AS
                SELECT * FROM (SELECT
                    ''::VARCHAR AS run_id,
                    ''::VARCHAR AS company,
                    ''::VARCHAR AS facility_id,
                    ''::VARCHAR AS source_url,
                    DATE '1970-01-01' AS scrape_date,
                    TIMESTAMP '1970-01-01 00:00:00' AS scrape_timestamp,
                    ''::VARCHAR AS raw_unit_label,
                    ''::VARCHAR AS raw_size_text,
                    ''::VARCHAR AS raw_feature_text,
                    ''::VARCHAR AS raw_price_text,
                    ''::VARCHAR AS raw_promo_text,
                    ''::VARCHAR AS raw_availability_text,
                    ''::VARCHAR AS raw_admin_fee_text,
                    ''::VARCHAR AS operator_unit_id,
                    '{}'::JSON AS raw_metadata
                ) WHERE FALSE;
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS normalized_unit_snapshots AS
                SELECT * FROM (SELECT
                    ''::VARCHAR AS run_id,
                    ''::VARCHAR AS company,
                    ''::VARCHAR AS facility_id,
                    ''::VARCHAR AS address,
                    ''::VARCHAR AS city,
                    ''::VARCHAR AS state,
                    ''::VARCHAR AS zip,
                    NULL::DOUBLE AS width_ft,
                    NULL::DOUBLE AS length_ft,
                    NULL::DOUBLE AS square_feet,
                    ''::VARCHAR AS normalized_size_bucket,
                    ''::VARCHAR AS normalized_unit_type,
                    FALSE::BOOLEAN AS climate_controlled,
                    ''::VARCHAR AS indoor_outdoor,
                    FALSE::BOOLEAN AS drive_up,
                    FALSE::BOOLEAN AS elevator_access,
                    FALSE::BOOLEAN AS first_floor,
                    FALSE::BOOLEAN AS upper_floor,
                    FALSE::BOOLEAN AS vehicle_parking,
                    ''::VARCHAR AS parking_type,
                    NULL::DOUBLE AS web_rate_monthly,
                    NULL::DOUBLE AS in_store_rate_monthly,
                    NULL::DOUBLE AS standard_rate_monthly,
                    NULL::DOUBLE AS online_only_rate_monthly,
                    NULL::DOUBLE AS admin_fee,
                    ''::VARCHAR AS promo_text,
                    ''::VARCHAR AS promo_type,
                    FALSE::BOOLEAN AS promo_flag,
                    ''::VARCHAR AS availability_text,
                    FALSE::BOOLEAN AS limited_availability_flag,
                    ''::VARCHAR AS units_left_text,
                    NULL::DOUBLE AS best_visible_price_monthly,
                    NULL::DOUBLE AS price_per_sqft_month,
                    ''::VARCHAR AS source_url,
                    TIMESTAMP '1970-01-01 00:00:00' AS scrape_timestamp,
                    DATE '1970-01-01' AS scrape_date,
                    ''::VARCHAR AS unit_key,
                    ''::VARCHAR AS important_descriptors,
                    ''::VARCHAR AS market_key,
                    ''::VARCHAR AS msa,
                    ''::VARCHAR AS raw_size_text,
                    ''::VARCHAR AS raw_feature_text,
                    ''::VARCHAR AS raw_price_text,
                    ''::VARCHAR AS raw_promo_text,
                    ''::VARCHAR AS raw_availability_text
                ) WHERE FALSE;
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS weekly_deltas AS
                SELECT * FROM (SELECT
                    ''::VARCHAR AS run_id,
                    ''::VARCHAR AS previous_run_id,
                    ''::VARCHAR AS company,
                    ''::VARCHAR AS facility_id,
                    ''::VARCHAR AS unit_key,
                    DATE '1970-01-01' AS current_scrape_date,
                    DATE '1970-01-01' AS previous_scrape_date,
                    ''::VARCHAR AS normalized_size_bucket,
                    ''::VARCHAR AS normalized_unit_type,
                    ''::VARCHAR AS market_key,
                    ''::VARCHAR AS msa,
                    NULL::DOUBLE AS current_price,
                    NULL::DOUBLE AS previous_price,
                    NULL::DOUBLE AS absolute_price_change,
                    NULL::DOUBLE AS percent_price_change,
                    NULL::DOUBLE AS current_price_per_sqft_month,
                    NULL::DOUBLE AS previous_price_per_sqft_month,
                    NULL::DOUBLE AS price_per_sqft_change,
                    FALSE::BOOLEAN AS newly_available,
                    FALSE::BOOLEAN AS no_longer_available,
                    FALSE::BOOLEAN AS promo_changed,
                    FALSE::BOOLEAN AS admin_fee_changed,
                    FALSE::BOOLEAN AS feature_changed,
                    ''::VARCHAR AS current_promo_text,
                    ''::VARCHAR AS previous_promo_text,
                    NULL::DOUBLE AS current_admin_fee,
                    NULL::DOUBLE AS previous_admin_fee
                ) WHERE FALSE;
                """
            )

    def append_dataframe(self, table_name: str, dataframe: pd.DataFrame) -> None:
        if dataframe.empty:
            return
        with self.connect() as con:
            con.register("tmp_df", dataframe)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")

    def replace_crawl_run(self, dataframe: pd.DataFrame) -> None:
        if dataframe.empty:
            return
        with self.connect() as con:
            con.execute("DELETE FROM crawl_runs WHERE run_id = ?", [str(dataframe.iloc[0]["run_id"])])
            con.register("tmp_run_df", dataframe)
            con.execute("INSERT INTO crawl_runs SELECT * FROM tmp_run_df")

    def upsert_facilities(self, facilities_df: pd.DataFrame) -> None:
        if facilities_df.empty:
            return
        with self.connect() as con:
            con.register("tmp_facilities", facilities_df)
            con.execute(
                """
                MERGE INTO facilities AS target
                USING tmp_facilities AS source
                ON target.company = source.company AND target.facility_id = source.facility_id
                WHEN MATCHED THEN UPDATE SET
                    facility_name = COALESCE(source.facility_name, target.facility_name),
                    address = COALESCE(source.address, target.address),
                    city = COALESCE(source.city, target.city),
                    state = COALESCE(source.state, target.state),
                    zip = COALESCE(source.zip, target.zip),
                    latitude = COALESCE(source.latitude, target.latitude),
                    longitude = COALESCE(source.longitude, target.longitude),
                    source_url = source.source_url,
                    last_seen_at = source.last_seen_at,
                    active_flag = source.active_flag,
                    market_key = COALESCE(source.market_key, target.market_key),
                    msa = COALESCE(source.msa, target.msa),
                    raw_metadata = source.raw_metadata
                WHEN NOT MATCHED THEN INSERT (
                    company, facility_id, facility_name, address, city, state, zip,
                    latitude, longitude, source_url, first_seen_at, last_seen_at,
                    active_flag, market_key, msa, raw_metadata
                ) VALUES (
                    source.company, source.facility_id, source.facility_name, source.address, source.city, source.state, source.zip,
                    source.latitude, source.longitude, source.source_url, source.first_seen_at, source.last_seen_at,
                    source.active_flag, source.market_key, source.msa, source.raw_metadata
                );
                """
            )

    def mark_missing_inactive(self, company: str, active_facility_ids: list[str]) -> None:
        with self.connect() as con:
            if active_facility_ids:
                ids = ",".join(f"'{facility_id}'" for facility_id in active_facility_ids)
                con.execute(
                    f"""
                    UPDATE facilities
                    SET active_flag = FALSE
                    WHERE company = ? AND facility_id NOT IN ({ids})
                    """,
                    [company],
                )
            else:
                con.execute("UPDATE facilities SET active_flag = FALSE WHERE company = ?", [company])

    def fetch_dataframe(self, query: str, parameters: list | None = None) -> pd.DataFrame:
        with self.connect() as con:
            return con.execute(query, parameters or []).df()

    def latest_completed_run(self, crawl_mode: str, exclude_run_id: str | None = None) -> pd.DataFrame:
        query = """
            SELECT *
            FROM crawl_runs
            WHERE status = 'completed'
              AND crawl_mode = ?
        """
        params: list = [crawl_mode]
        if exclude_run_id is not None:
            query += " AND run_id <> ?"
            params.append(exclude_run_id)
        query += " ORDER BY completed_at DESC LIMIT 1"
        return self.fetch_dataframe(query, params)

    def completed_runs(self, crawl_mode: str, exclude_run_id: str | None = None) -> pd.DataFrame:
        query = """
            SELECT *
            FROM crawl_runs
            WHERE status = 'completed'
              AND crawl_mode = ?
        """
        params: list = [crawl_mode]
        if exclude_run_id is not None:
            query += " AND run_id <> ?"
            params.append(exclude_run_id)
        query += " ORDER BY completed_at DESC"
        return self.fetch_dataframe(query, params)
