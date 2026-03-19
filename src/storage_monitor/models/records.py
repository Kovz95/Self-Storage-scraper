from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class FacilityRecord(BaseModel):
    company: str
    facility_id: str
    facility_name: str | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    source_url: str
    first_seen_at: datetime
    last_seen_at: datetime
    active_flag: bool = True
    market_key: str | None = None
    msa: str | None = None
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


class RawUnitSnapshot(BaseModel):
    run_id: str
    company: str
    facility_id: str
    source_url: str
    scrape_date: date
    scrape_timestamp: datetime
    raw_unit_label: str | None = None
    raw_size_text: str | None = None
    raw_feature_text: str | None = None
    raw_price_text: str | None = None
    raw_promo_text: str | None = None
    raw_availability_text: str | None = None
    raw_admin_fee_text: str | None = None
    operator_unit_id: str | None = None
    raw_metadata: dict[str, Any] = Field(default_factory=dict)


class NormalizedUnitSnapshot(BaseModel):
    run_id: str
    company: str
    facility_id: str
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    width_ft: float | None = None
    length_ft: float | None = None
    square_feet: float | None = None
    normalized_size_bucket: str | None = None
    normalized_unit_type: str | None = None
    climate_controlled: bool = False
    indoor_outdoor: str | None = None
    drive_up: bool = False
    elevator_access: bool = False
    first_floor: bool = False
    upper_floor: bool = False
    vehicle_parking: bool = False
    parking_type: str | None = None
    web_rate_monthly: float | None = None
    in_store_rate_monthly: float | None = None
    standard_rate_monthly: float | None = None
    online_only_rate_monthly: float | None = None
    admin_fee: float | None = None
    promo_text: str | None = None
    promo_type: str | None = None
    promo_flag: bool = False
    availability_text: str | None = None
    limited_availability_flag: bool = False
    units_left_text: str | None = None
    best_visible_price_monthly: float | None = None
    price_per_sqft_month: float | None = None
    source_url: str
    scrape_timestamp: datetime
    scrape_date: date
    unit_key: str
    important_descriptors: str | None = None
    market_key: str | None = None
    msa: str | None = None
    raw_size_text: str | None = None
    raw_feature_text: str | None = None
    raw_price_text: str | None = None
    raw_promo_text: str | None = None
    raw_availability_text: str | None = None


class CrawlRunRecord(BaseModel):
    run_id: str
    crawl_mode: str
    sample_size_per_company: int | None = None
    dry_run: bool = False
    requested_at: datetime
    started_at: datetime
    completed_at: datetime | None = None
    status: str
    facilities_discovered: int = 0
    facilities_selected: int = 0
    facilities_scraped_successfully: int = 0
    facilities_failed: int = 0
    units_scraped: int = 0
    notes: str | None = None
    error_summary: str | None = None


class WeeklyDeltaRecord(BaseModel):
    run_id: str
    previous_run_id: str
    company: str
    facility_id: str
    unit_key: str
    current_scrape_date: date
    previous_scrape_date: date
    normalized_size_bucket: str | None = None
    normalized_unit_type: str | None = None
    market_key: str | None = None
    msa: str | None = None
    current_price: float | None = None
    previous_price: float | None = None
    absolute_price_change: float | None = None
    percent_price_change: float | None = None
    current_price_per_sqft_month: float | None = None
    previous_price_per_sqft_month: float | None = None
    price_per_sqft_change: float | None = None
    newly_available: bool = False
    no_longer_available: bool = False
    promo_changed: bool = False
    admin_fee_changed: bool = False
    feature_changed: bool = False
    current_promo_text: str | None = None
    previous_promo_text: str | None = None
    current_admin_fee: float | None = None
    previous_admin_fee: float | None = None
