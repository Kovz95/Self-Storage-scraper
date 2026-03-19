from __future__ import annotations

from pathlib import Path

import pandas as pd


def compute_weekly_deltas(current_df: pd.DataFrame, previous_df: pd.DataFrame, run_id: str, previous_run_id: str) -> pd.DataFrame:
    if current_df.empty and previous_df.empty:
        return pd.DataFrame()
    if not previous_run_id or previous_df.empty or "unit_key" not in previous_df.columns:
        return pd.DataFrame(
            columns=[
                "run_id",
                "previous_run_id",
                "company",
                "facility_id",
                "unit_key",
                "current_scrape_date",
                "previous_scrape_date",
                "normalized_size_bucket",
                "normalized_unit_type",
                "market_key",
                "msa",
                "current_price",
                "previous_price",
                "absolute_price_change",
                "percent_price_change",
                "current_price_per_sqft_month",
                "previous_price_per_sqft_month",
                "price_per_sqft_change",
                "newly_available",
                "no_longer_available",
                "promo_changed",
                "admin_fee_changed",
                "feature_changed",
                "current_promo_text",
                "previous_promo_text",
                "current_admin_fee",
                "previous_admin_fee",
            ]
        )
    current = current_df.add_prefix("current_")
    previous = previous_df.add_prefix("previous_")
    merged = current.merge(previous, left_on="current_unit_key", right_on="previous_unit_key", how="outer")
    matched = merged["current_unit_key"].notna() & merged["previous_unit_key"].notna()
    merged["run_id"] = run_id
    merged["previous_run_id"] = previous_run_id
    merged["company"] = merged["current_company"].fillna(merged["previous_company"])
    merged["facility_id"] = merged["current_facility_id"].fillna(merged["previous_facility_id"])
    merged["unit_key"] = merged["current_unit_key"].fillna(merged["previous_unit_key"])
    merged["current_price"] = merged["current_best_visible_price_monthly"]
    merged["previous_price"] = merged["previous_best_visible_price_monthly"]
    merged["absolute_price_change"] = ((merged["current_price"] - merged["previous_price"]).where(matched)).round(4)
    merged["percent_price_change"] = (
        (merged["absolute_price_change"] / merged["previous_price"].where(merged["previous_price"] > 0))
        .where(matched)
        .round(6)
    )
    merged["current_price_per_sqft_month"] = merged["current_price_per_sqft_month"]
    merged["previous_price_per_sqft_month"] = merged["previous_price_per_sqft_month"]
    merged["price_per_sqft_change"] = (
        merged["current_price_per_sqft_month"] - merged["previous_price_per_sqft_month"]
    ).where(matched).round(6)
    merged["newly_available"] = merged["previous_unit_key"].isna() & merged["current_unit_key"].notna()
    merged["no_longer_available"] = merged["current_unit_key"].isna() & merged["previous_unit_key"].notna()
    merged["promo_changed"] = matched & (merged["current_promo_text"].fillna("") != merged["previous_promo_text"].fillna(""))
    merged["admin_fee_changed"] = merged.apply(
        lambda row: (
            False if not (pd.notna(row.get("current_unit_key")) and pd.notna(row.get("previous_unit_key"))) else (
                False
                if pd.isna(row["current_admin_fee"]) and pd.isna(row["previous_admin_fee"])
                else row["current_admin_fee"] != row["previous_admin_fee"]
            )
        ),
        axis=1,
    )
    feature_fields = [
        "normalized_unit_type",
        "climate_controlled",
        "indoor_outdoor",
        "drive_up",
        "elevator_access",
        "first_floor",
        "upper_floor",
        "vehicle_parking",
        "parking_type",
    ]
    merged["feature_changed"] = False
    for field in feature_fields:
        merged["feature_changed"] = merged["feature_changed"] | (
            matched
            & (
                merged[f"current_{field}"].fillna("").astype(str) != merged[f"previous_{field}"].fillna("").astype(str)
            )
        )
    result = merged[
        [
            "run_id",
            "previous_run_id",
            "company",
            "facility_id",
            "unit_key",
            "current_scrape_date",
            "previous_scrape_date",
            "current_normalized_size_bucket",
            "current_normalized_unit_type",
            "current_market_key",
            "current_msa",
            "current_price",
            "previous_price",
            "absolute_price_change",
            "percent_price_change",
            "current_price_per_sqft_month",
            "previous_price_per_sqft_month",
            "price_per_sqft_change",
            "newly_available",
            "no_longer_available",
            "promo_changed",
            "admin_fee_changed",
            "feature_changed",
            "current_promo_text",
            "previous_promo_text",
            "current_admin_fee",
            "previous_admin_fee",
        ]
    ].rename(
        columns={
            "current_normalized_size_bucket": "normalized_size_bucket",
            "current_normalized_unit_type": "normalized_unit_type",
            "current_market_key": "market_key",
            "current_msa": "msa",
        }
    )
    result = result.sort_values(["company", "facility_id", "unit_key"], na_position="last").reset_index(drop=True)
    return result


def _metric_row(section: str, metric: str, dimension_value: str, company: str | None, bucket: str | None, state: str | None, value: float) -> dict:
    return {
        "section": section,
        "metric": metric,
        "dimension_value": dimension_value,
        "company": company,
        "normalized_size_bucket": bucket,
        "state": state,
        "value": value,
    }


def build_latest_snapshot_summary(normalized_df: pd.DataFrame) -> pd.DataFrame:
    if normalized_df.empty:
        return pd.DataFrame(columns=["section", "metric", "dimension_value", "company", "normalized_size_bucket", "state", "value"])
    rows: list[dict] = []
    priced = normalized_df[normalized_df["best_visible_price_monthly"].notna()].copy()
    for company, group in priced.groupby("company"):
        rows.append(_metric_row("company", "median_best_visible_price_monthly", company, company, None, None, group["best_visible_price_monthly"].median()))
        rows.append(_metric_row("company", "average_best_visible_price_monthly", company, company, None, None, group["best_visible_price_monthly"].mean()))
        rows.append(_metric_row("company", "median_price_per_sqft_month", company, company, None, None, group["price_per_sqft_month"].median()))
        rows.append(_metric_row("company", "promo_incidence", company, company, None, None, group["promo_flag"].mean()))
    for bucket, group in priced.groupby("normalized_size_bucket"):
        rows.append(_metric_row("size_bucket", "median_best_visible_price_monthly", str(bucket), None, str(bucket), None, group["best_visible_price_monthly"].median()))
    for (company, bucket), group in priced.groupby(["company", "normalized_size_bucket"]):
        rows.append(_metric_row("company_size_bucket", "median_best_visible_price_monthly", f"{company}|{bucket}", company, str(bucket), None, group["best_visible_price_monthly"].median()))
    for state, group in priced.groupby("state"):
        rows.append(_metric_row("state", "median_best_visible_price_monthly", str(state), None, None, str(state), group["best_visible_price_monthly"].median()))
    for msa, group in priced.groupby("msa"):
        if msa:
            rows.append(_metric_row("msa", "median_best_visible_price_monthly", str(msa), None, None, None, group["best_visible_price_monthly"].median()))
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame, max_rows: int = 20) -> str:
    if frame.empty:
        return "_No rows._"
    sample = frame.head(max_rows).fillna("")
    headers = list(sample.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in sample.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in headers) + " |")
    return "\n".join(lines)


def _subset_or_empty(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    existing = [column for column in columns if column in frame.columns]
    return frame[existing] if existing else pd.DataFrame(columns=columns)


def _format_currency(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"${value:,.2f}"


def _format_percent(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100:.1f}%"


def _format_number(value: float | int | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.{digits}f}"


def build_peer_comparison(normalized_df: pd.DataFrame) -> pd.DataFrame:
    if normalized_df.empty:
        return pd.DataFrame()
    grouped = (
        normalized_df.dropna(subset=["best_visible_price_monthly"])
        .groupby(["market_key", "normalized_unit_type", "company"], dropna=False)["best_visible_price_monthly"]
        .median()
        .reset_index(name="median_price")
    )
    rows: list[dict] = []
    for (market_key, cohort), group in grouped.groupby(["market_key", "normalized_unit_type"], dropna=False):
        if group["company"].nunique() < 2:
            continue
        for _, row in group.iterrows():
            peers = group[group["company"] != row["company"]]
            peer_median = peers["median_price"].mean()
            rows.append(
                {
                    "market_key": market_key,
                    "normalized_unit_type": cohort,
                    "company": row["company"],
                    "company_median_price": row["median_price"],
                    "peer_average_price": peer_median,
                    "premium_discount_pct": (row["median_price"] - peer_median) / peer_median if peer_median else None,
                }
            )
    return pd.DataFrame(rows)


def _prepare_priced_frame(normalized_df: pd.DataFrame) -> pd.DataFrame:
    if normalized_df.empty:
        return pd.DataFrame()
    return normalized_df[normalized_df["best_visible_price_monthly"].notna()].copy()


def _prepare_changed_deltas(deltas_df: pd.DataFrame) -> pd.DataFrame:
    if deltas_df.empty:
        return deltas_df.copy()
    return deltas_df[
        deltas_df["newly_available"]
        | deltas_df["no_longer_available"]
        | deltas_df["promo_changed"]
        | deltas_df["admin_fee_changed"]
        | deltas_df["feature_changed"]
        | deltas_df["absolute_price_change"].fillna(0).ne(0)
    ].copy()


def write_summary_report(
    report_path: Path,
    coverage_by_company: pd.DataFrame,
    normalized_df: pd.DataFrame,
    deltas_df: pd.DataFrame,
    facilities_df: pd.DataFrame,
    data_quality_notes: list[str],
) -> None:
    priced = _prepare_priced_frame(normalized_df)
    changed = _prepare_changed_deltas(deltas_df)
    run_date = (
        str(pd.to_datetime(normalized_df["scrape_date"]).max().date())
        if not normalized_df.empty and "scrape_date" in normalized_df.columns
        else "n/a"
    )

    company_summary = (
        priced.groupby("company")
        .agg(
            median_best_visible_price_monthly=("best_visible_price_monthly", "median"),
            average_best_visible_price_monthly=("best_visible_price_monthly", "mean"),
            median_price_per_sqft_month=("price_per_sqft_month", "median"),
            promo_incidence=("promo_flag", "mean"),
            units=("unit_key", "count"),
        )
        .reset_index()
        if not priced.empty
        else pd.DataFrame()
    )
    bucket_summary = (
        priced.groupby("normalized_size_bucket")
        .agg(
            median_best_visible_price_monthly=("best_visible_price_monthly", "median"),
            median_price_per_sqft_month=("price_per_sqft_month", "median"),
            units=("unit_key", "count"),
        )
        .reset_index()
        if not priced.empty
        else pd.DataFrame()
    )
    company_bucket_summary = (
        priced.groupby(["company", "normalized_size_bucket"])
        .agg(
            median_best_visible_price_monthly=("best_visible_price_monthly", "median"),
            median_price_per_sqft_month=("price_per_sqft_month", "median"),
            units=("unit_key", "count"),
        )
        .reset_index()
        if not priced.empty
        else pd.DataFrame()
    )
    geography_summary = (
        priced.groupby("state")
        .agg(
            median_best_visible_price_monthly=("best_visible_price_monthly", "median"),
            median_price_per_sqft_month=("price_per_sqft_month", "median"),
            units=("unit_key", "count"),
        )
        .reset_index()
        .sort_values("units", ascending=False)
        if not priced.empty
        else pd.DataFrame()
    )
    msa_summary = (
        priced.groupby("msa")
        .agg(
            median_best_visible_price_monthly=("best_visible_price_monthly", "median"),
            units=("unit_key", "count"),
        )
        .reset_index()
        .sort_values("units", ascending=False)
        if not priced.empty
        else pd.DataFrame()
    )
    delta_summary = (
        changed.groupby("company")
        .agg(
            units_changed=("unit_key", "count"),
            price_increases=("absolute_price_change", lambda s: (s.fillna(0) > 0).sum()),
            price_decreases=("absolute_price_change", lambda s: (s.fillna(0) < 0).sum()),
            newly_available=("newly_available", "sum"),
            no_longer_available=("no_longer_available", "sum"),
            promo_changes=("promo_changed", "sum"),
            admin_fee_changes=("admin_fee_changed", "sum"),
            feature_changes=("feature_changed", "sum"),
            median_absolute_change=("absolute_price_change", "median"),
            median_percent_change=("percent_price_change", "median"),
        )
        .reset_index()
        if not changed.empty
        else pd.DataFrame()
    )
    geography_changes = (
        changed.groupby("market_key")
        .agg(
            units_changed=("unit_key", "count"),
            median_absolute_change=("absolute_price_change", "median"),
            median_percent_change=("percent_price_change", "median"),
        )
        .reset_index()
        .sort_values(["median_absolute_change", "units_changed"], ascending=[False, False])
        if not changed.empty
        else pd.DataFrame()
    )
    top_positive = changed[changed["absolute_price_change"].fillna(0) > 0].sort_values("absolute_price_change", ascending=False).head(10)
    top_negative = changed[changed["absolute_price_change"].fillna(0) < 0].sort_values("absolute_price_change", ascending=True).head(10)
    peer_comparison = build_peer_comparison(priced)
    peer_comparison = (
        peer_comparison.sort_values("premium_discount_pct", ascending=False)
        if not peer_comparison.empty
        else peer_comparison
    )

    takeaways: list[str] = []
    if not company_summary.empty:
        priciest = company_summary.sort_values("median_best_visible_price_monthly", ascending=False).iloc[0]
        most_promotional = company_summary.sort_values("promo_incidence", ascending=False).iloc[0]
        takeaways.append(
            f"{priciest['company']} screens as the highest-priced operator in this cut with median visible web price {_format_currency(priciest['median_best_visible_price_monthly'])}."
        )
        takeaways.append(
            f"{most_promotional['company']} shows the highest promo incidence at {_format_percent(most_promotional['promo_incidence'])} of visible units."
        )
    if not changed.empty:
        movers = changed[changed["absolute_price_change"].notna()]
        if not movers.empty:
            takeaways.append(
                f"Units with observable price moves skew {'up' if movers['absolute_price_change'].median() > 0 else 'down' if movers['absolute_price_change'].median() < 0 else 'flat'} with median absolute move {_format_currency(movers['absolute_price_change'].median())}."
            )
    else:
        takeaways.append("No prior weekly snapshot was available for a strict week-over-week price comparison in this run.")
    if not peer_comparison.empty:
        spread = peer_comparison.iloc[0]
        takeaways.append(
            f"Within matched local cohorts, {spread['company']} shows the largest relative premium at {_format_percent(spread['premium_discount_pct'])} versus peers in {spread['market_key']}."
        )

    coverage_lines = []
    for _, row in coverage_by_company.fillna(0).iterrows():
        coverage_lines.append(
            f"- `{row['company']}`: discovered {int(row['facilities_discovered'])} facilities, scraped {int(row['facilities_scraped_successfully'])}, captured {int(row['total_unit_rows_scraped'])} unit rows, failures {int(row['scrape_failures'])}."
        )

    company_display = company_summary.copy()
    if not company_display.empty:
        company_display["median_best_visible_price_monthly"] = company_display["median_best_visible_price_monthly"].map(_format_currency)
        company_display["average_best_visible_price_monthly"] = company_display["average_best_visible_price_monthly"].map(_format_currency)
        company_display["median_price_per_sqft_month"] = company_display["median_price_per_sqft_month"].map(_format_number)
        company_display["promo_incidence"] = company_display["promo_incidence"].map(_format_percent)

    bucket_display = bucket_summary.copy()
    if not bucket_display.empty:
        bucket_display["median_best_visible_price_monthly"] = bucket_display["median_best_visible_price_monthly"].map(_format_currency)
        bucket_display["median_price_per_sqft_month"] = bucket_display["median_price_per_sqft_month"].map(_format_number)

    company_bucket_display = company_bucket_summary.copy()
    if not company_bucket_display.empty:
        company_bucket_display["median_best_visible_price_monthly"] = company_bucket_display["median_best_visible_price_monthly"].map(_format_currency)
        company_bucket_display["median_price_per_sqft_month"] = company_bucket_display["median_price_per_sqft_month"].map(_format_number)

    geography_display = geography_summary.copy()
    if not geography_display.empty:
        geography_display["median_best_visible_price_monthly"] = geography_display["median_best_visible_price_monthly"].map(_format_currency)
        geography_display["median_price_per_sqft_month"] = geography_display["median_price_per_sqft_month"].map(_format_number)

    msa_display = msa_summary.copy()
    if not msa_display.empty:
        msa_display["median_best_visible_price_monthly"] = msa_display["median_best_visible_price_monthly"].map(_format_currency)

    delta_display = delta_summary.copy()
    if not delta_display.empty:
        delta_display["median_absolute_change"] = delta_display["median_absolute_change"].map(_format_currency)
        delta_display["median_percent_change"] = delta_display["median_percent_change"].map(_format_percent)

    geography_changes_display = geography_changes.copy()
    if not geography_changes_display.empty:
        geography_changes_display["median_absolute_change"] = geography_changes_display["median_absolute_change"].map(_format_currency)
        geography_changes_display["median_percent_change"] = geography_changes_display["median_percent_change"].map(_format_percent)

    top_positive_display = _subset_or_empty(top_positive, ["company", "facility_id", "unit_key", "absolute_price_change", "percent_price_change", "promo_changed"])
    if not top_positive_display.empty:
        top_positive_display["absolute_price_change"] = top_positive_display["absolute_price_change"].map(_format_currency)
        top_positive_display["percent_price_change"] = top_positive_display["percent_price_change"].map(_format_percent)

    top_negative_display = _subset_or_empty(top_negative, ["company", "facility_id", "unit_key", "absolute_price_change", "percent_price_change", "promo_changed"])
    if not top_negative_display.empty:
        top_negative_display["absolute_price_change"] = top_negative_display["absolute_price_change"].map(_format_currency)
        top_negative_display["percent_price_change"] = top_negative_display["percent_price_change"].map(_format_percent)

    peer_display = peer_comparison.copy()
    if not peer_display.empty:
        peer_display["company_median_price"] = peer_display["company_median_price"].map(_format_currency)
        peer_display["peer_average_price"] = peer_display["peer_average_price"].map(_format_currency)
        peer_display["premium_discount_pct"] = peer_display["premium_discount_pct"].map(_format_percent)

    body = [
        f"# Self-Storage Weekly Pricing Monitor ({run_date})",
        "",
        "## Executive Takeaways",
    ]
    body.extend(f"- {line}" for line in takeaways)
    body.extend(
        [
            "",
            "## Coverage",
            "Current run coverage by operator:",
        ]
    )
    body.extend(coverage_lines if coverage_lines else ["- No facilities were captured in this run."])
    body.extend(
        [
            "",
            "## Current Pricing Screen",
            "Operator-level pricing and promo incidence:",
            _markdown_table(company_display),
            "",
            "Size-bucket summary:",
            _markdown_table(bucket_display),
            "",
            "Operator by size bucket:",
            _markdown_table(company_bucket_display),
            "",
            "State summary:",
            _markdown_table(geography_display.head(15)),
            "",
            "MSA summary:",
            _markdown_table(msa_display.head(15)),
            "",
            "## Week-over-Week Change Monitor",
        ]
    )
    if changed.empty:
        body.append("No strict week-over-week comparison set was available, or no units showed a material change versus the prior eligible snapshot.")
    else:
        body.extend(
            [
                "Operator-level change summary:",
                _markdown_table(delta_display),
                "",
                "Markets with the biggest median moves:",
                _markdown_table(geography_changes_display.head(15)),
                "",
                "Top positive movers:",
                _markdown_table(top_positive_display),
                "",
                "Top negative movers:",
                _markdown_table(top_negative_display),
            ]
        )
    body.extend(
        [
            "",
            "## Peer Comparison",
            "Matched on local geography and normalized cohort where overlap exists:",
            _markdown_table(peer_display.head(20)),
            "",
            "## Data Quality Notes",
        ]
    )
    if data_quality_notes:
        body.extend(f"- {note}" for note in data_quality_notes)
    else:
        body.append("- No additional data quality caveats were recorded in this run.")
    body.extend(
        [
            "- MSA analysis uses a city-state proxy unless an external market crosswalk is added.",
            "- Public Storage pricing is currently taken from visible HTML price attributes; Extra Space uses embedded JSON where available.",
        ]
    )
    report_path.write_text("\n".join(body), encoding="utf-8")
