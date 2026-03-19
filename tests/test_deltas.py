from __future__ import annotations

import pandas as pd

from storage_monitor.reporting.summary import compute_weekly_deltas


def test_compute_weekly_deltas_price_change_and_feature_flags() -> None:
    previous_df = pd.DataFrame(
        [
            {
                "run_id": "prev",
                "company": "extra_space",
                "facility_id": "1275",
                "unit_key": "unit-1",
                "scrape_date": "2026-03-11",
                "normalized_size_bucket": "medium",
                "normalized_unit_type": "medium_climate",
                "market_key": "Phoenix, AZ",
                "msa": "Phoenix, AZ",
                "best_visible_price_monthly": 100.0,
                "price_per_sqft_month": 1.0,
                "promo_text": "10% off",
                "admin_fee": 25.0,
                "climate_controlled": True,
                "indoor_outdoor": "indoor",
                "drive_up": False,
                "elevator_access": False,
                "first_floor": True,
                "upper_floor": False,
                "vehicle_parking": False,
                "parking_type": None,
            }
        ]
    )
    current_df = pd.DataFrame(
        [
            {
                "run_id": "curr",
                "company": "extra_space",
                "facility_id": "1275",
                "unit_key": "unit-1",
                "scrape_date": "2026-03-18",
                "normalized_size_bucket": "medium",
                "normalized_unit_type": "medium_climate",
                "market_key": "Phoenix, AZ",
                "msa": "Phoenix, AZ",
                "best_visible_price_monthly": 110.0,
                "price_per_sqft_month": 1.1,
                "promo_text": "5% off",
                "admin_fee": 29.0,
                "climate_controlled": True,
                "indoor_outdoor": "indoor",
                "drive_up": False,
                "elevator_access": False,
                "first_floor": False,
                "upper_floor": False,
                "vehicle_parking": False,
                "parking_type": None,
            }
        ]
    )

    deltas = compute_weekly_deltas(current_df, previous_df, run_id="curr", previous_run_id="prev")
    row = deltas.iloc[0]
    assert row["absolute_price_change"] == 10.0
    assert round(row["percent_price_change"], 4) == 0.1
    assert row["price_per_sqft_change"] == 0.1
    assert bool(row["promo_changed"]) is True
    assert bool(row["admin_fee_changed"]) is True
    assert bool(row["feature_changed"]) is True


def test_compute_weekly_deltas_newly_available_and_removed() -> None:
    previous_df = pd.DataFrame(
        [
            {
                "run_id": "prev",
                "company": "cube_smart",
                "facility_id": "4243",
                "unit_key": "removed-unit",
                "scrape_date": "2026-03-11",
                "normalized_size_bucket": "small",
                "normalized_unit_type": "small_climate",
                "market_key": "Auburn, AL",
                "msa": "Auburn, AL",
                "best_visible_price_monthly": 47.7,
                "price_per_sqft_month": 1.908,
                "promo_text": "10% Off",
                "admin_fee": None,
                "climate_controlled": True,
                "indoor_outdoor": "indoor",
                "drive_up": False,
                "elevator_access": True,
                "first_floor": False,
                "upper_floor": True,
                "vehicle_parking": False,
                "parking_type": None,
            }
        ]
    )
    current_df = pd.DataFrame(
        [
            {
                "run_id": "curr",
                "company": "cube_smart",
                "facility_id": "4243",
                "unit_key": "new-unit",
                "scrape_date": "2026-03-18",
                "normalized_size_bucket": "small",
                "normalized_unit_type": "small_climate",
                "market_key": "Auburn, AL",
                "msa": "Auburn, AL",
                "best_visible_price_monthly": 53.0,
                "price_per_sqft_month": 2.12,
                "promo_text": "10% Off",
                "admin_fee": None,
                "climate_controlled": True,
                "indoor_outdoor": "indoor",
                "drive_up": False,
                "elevator_access": True,
                "first_floor": False,
                "upper_floor": True,
                "vehicle_parking": False,
                "parking_type": None,
            }
        ]
    )

    deltas = compute_weekly_deltas(current_df, previous_df, run_id="curr", previous_run_id="prev")
    assert set(deltas["unit_key"]) == {"new-unit", "removed-unit"}
    newly_available = deltas.loc[deltas["unit_key"] == "new-unit"].iloc[0]
    removed = deltas.loc[deltas["unit_key"] == "removed-unit"].iloc[0]
    assert bool(newly_available["newly_available"]) is True
    assert bool(removed["no_longer_available"]) is True
