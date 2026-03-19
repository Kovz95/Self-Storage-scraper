from __future__ import annotations

from datetime import UTC, date, datetime

from storage_monitor.models import FacilityRecord, RawUnitSnapshot
from storage_monitor.normalization.parsing import (
    create_unit_key,
    normalize_unit_snapshot,
    parse_feature_flags,
    parse_price,
    parse_promo,
    parse_size_text,
)


def test_parse_size_text_standard_dimensions() -> None:
    parsed = parse_size_text("10' x 10'")
    assert parsed.width_ft == 10
    assert parsed.length_ft == 10
    assert parsed.square_feet == 100
    assert parsed.normalized_size_bucket == "medium"


def test_parse_size_text_locker() -> None:
    parsed = parse_size_text("Locker", "Locker", "Climate Controlled")
    assert parsed.width_ft is None
    assert parsed.length_ft is None
    assert parsed.normalized_size_bucket == "locker"


def test_parse_feature_flags_vehicle_drive_up_outdoor() -> None:
    flags = parse_feature_flags("Outside Unit; Drive-up access; Boat or Vehicle; Enclosed")
    assert flags["drive_up"] is True
    assert flags["vehicle_parking"] is True
    assert flags["indoor_outdoor"] == "outdoor"
    assert flags["parking_type"] == "enclosed"


def test_parse_price() -> None:
    assert parse_price("web:$104.00") == 104.0
    assert parse_price("$1,299/mo") == 1299.0
    assert parse_price(None) is None


def test_parse_promo() -> None:
    assert parse_promo("$1 first month rent") == (True, "first_month_1")
    assert parse_promo("10% off") == (True, "discount_10pct")
    assert parse_promo(None) == (False, None)


def test_create_unit_key_is_deterministic() -> None:
    payload = {
        "company": "extra_space",
        "facility_id": "1275",
        "width_ft": 10.0,
        "length_ft": 10.0,
        "climate_controlled": True,
        "indoor_outdoor": "indoor",
        "drive_up": False,
        "elevator_access": False,
        "first_floor": True,
        "upper_floor": False,
        "vehicle_parking": False,
        "parking_type": None,
        "normalized_size_bucket": "medium",
        "normalized_unit_type": "medium_climate",
        "important_descriptors": "climate,first_floor,indoor",
    }
    assert create_unit_key(payload) == create_unit_key(payload)


def test_normalize_unit_snapshot_builds_expected_fields() -> None:
    facility = FacilityRecord(
        company="public_storage",
        facility_id="1235",
        facility_name="Phoenix",
        address="123 Main St",
        city="Phoenix",
        state="AZ",
        zip="85001",
        latitude=None,
        longitude=None,
        source_url="https://example.com",
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
        active_flag=True,
    )
    raw = RawUnitSnapshot(
        run_id="run1",
        company="public_storage",
        facility_id="1235",
        source_url="https://example.com",
        scrape_date=date(2026, 3, 18),
        scrape_timestamp=datetime.now(UTC),
        raw_unit_label="Small 5'x5'",
        raw_size_text="5'x5'",
        raw_feature_text="Climate controlled; Ground Floor; Indoor",
        raw_price_text="online_only:$10 | list:$12",
        raw_promo_text="$1 first month rent",
        raw_availability_text="Available",
        raw_admin_fee_text="All new rentals subject to one-time $29 admin fee.",
    )
    normalized = normalize_unit_snapshot(raw, facility)
    assert normalized.square_feet == 25
    assert normalized.normalized_size_bucket == "small"
    assert normalized.normalized_unit_type == "small_climate"
    assert normalized.online_only_rate_monthly == 10.0
    assert normalized.in_store_rate_monthly == 12.0
    assert normalized.best_visible_price_monthly == 10.0
    assert normalized.admin_fee == 29.0
    assert normalized.promo_flag is True
