from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass

from storage_monitor.models import FacilityRecord, NormalizedUnitSnapshot, RawUnitSnapshot


PRICE_REGEX = re.compile(r"\$?\s*(-?\d+(?:,\d{3})*(?:\.\d{1,2})?)")
SIZE_REGEXES = [
    re.compile(r"(?P<width>\d+(?:\.\d+)?)\s*(?:ft|feet|')?\s*(?:x|×|by)\s*(?P<length>\d+(?:\.\d+)?)", re.I),
    re.compile(r"(?P<width>\d+(?:\.\d+)?)\s*'\s*x\s*(?P<length>\d+(?:\.\d+)?)\s*'", re.I),
]


@dataclass(slots=True)
class SizeParseResult:
    width_ft: float | None
    length_ft: float | None
    square_feet: float | None
    normalized_size_bucket: str | None


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def parse_price(value: str | None) -> float | None:
    value = clean_text(value)
    if not value:
        return None
    match = PRICE_REGEX.search(value.replace(",", ""))
    if not match:
        return None
    return float(match.group(1))


def parse_price_variants(raw_price_text: str | None) -> dict[str, float | None]:
    variants = {
        "web_rate_monthly": None,
        "in_store_rate_monthly": None,
        "standard_rate_monthly": None,
        "online_only_rate_monthly": None,
    }
    if not raw_price_text:
        return variants
    text = clean_text(raw_price_text) or ""
    for segment in text.split("|"):
        segment = clean_text(segment) or ""
        lower = segment.lower()
        amount = parse_price(segment)
        if "online only" in lower or "online_only" in lower:
            variants["online_only_rate_monthly"] = amount
        elif "web" in lower:
            variants["web_rate_monthly"] = amount
        elif "in store" in lower or "list" in lower:
            variants["in_store_rate_monthly"] = amount
            variants["standard_rate_monthly"] = amount
        elif "standard" in lower:
            variants["standard_rate_monthly"] = amount
        elif amount is not None and variants["web_rate_monthly"] is None:
            variants["web_rate_monthly"] = amount
    return variants


def parse_admin_fee(value: str | None) -> float | None:
    return parse_price(value)


def parse_size_text(raw_text: str | None, raw_unit_label: str | None = None, feature_text: str | None = None) -> SizeParseResult:
    candidates = [clean_text(raw_text), clean_text(raw_unit_label), clean_text(feature_text)]
    combined_text = " ".join(filter(None, candidates)).lower()
    for candidate in candidates:
        if not candidate:
            continue
        for pattern in SIZE_REGEXES:
            match = pattern.search(candidate)
            if match:
                width = float(match.group("width"))
                length = float(match.group("length"))
                square_feet = width * length
                return SizeParseResult(width, length, square_feet, bucket_size(square_feet, combined_text))
    if "locker" in combined_text:
        return SizeParseResult(None, None, None, "locker")
    if any(token in combined_text for token in ("parking", "vehicle", "rv", "boat")):
        return SizeParseResult(None, None, None, "vehicle")
    return SizeParseResult(None, None, None, None)


def bucket_size(square_feet: float | None, text: str | None = None) -> str | None:
    lowered = (text or "").lower()
    if "locker" in lowered:
        return "locker"
    if any(token in lowered for token in ("vehicle", "parking", "rv", "boat")):
        return "vehicle"
    if square_feet is None:
        return None
    if square_feet <= 25:
        return "small"
    if square_feet <= 50:
        return "small"
    if square_feet <= 150:
        return "medium"
    return "large"


def parse_feature_flags(feature_text: str | None, raw_unit_label: str | None = None) -> dict[str, object]:
    text = " ".join(filter(None, [clean_text(feature_text), clean_text(raw_unit_label)])).lower()
    normalized = text.replace("-", " ")
    flags = {
        "climate_controlled": any(token in normalized for token in ("climate", "temperature controlled", "air cooled")),
        "indoor_outdoor": None,
        "drive_up": any(token in normalized for token in ("drive up", "drive up access", "driveup")),
        "elevator_access": "elevator" in normalized or "lift access" in normalized,
        "first_floor": any(token in normalized for token in ("1st floor", "ground floor", "first floor", "ground level")),
        "upper_floor": any(token in normalized for token in ("upper floor", "2nd floor", "second floor", "3rd floor", "third floor", "upper level")),
        "vehicle_parking": any(token in normalized for token in ("vehicle", "rv", "boat", "parking")),
        "parking_type": None,
    }
    if any(token in normalized for token in ("indoor", "inside unit", "inside")):
        flags["indoor_outdoor"] = "indoor"
    elif any(token in normalized for token in ("outdoor", "outside unit", "outside")):
        flags["indoor_outdoor"] = "outdoor"
    if "enclosed" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "enclosed"
    elif "covered" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "covered"
    elif "uncovered" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "uncovered"
    elif "indoor" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "indoor"
    elif "outdoor" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "outdoor"
    elif "parking" in normalized and flags["vehicle_parking"]:
        flags["parking_type"] = "parking"
    return flags


def parse_promo(value: str | None) -> tuple[bool, str | None]:
    text = (clean_text(value) or "").lower()
    if not text:
        return False, None
    if "$1" in text and "month" in text:
        return True, "first_month_1"
    if "first month free" in text or "1st month free" in text:
        return True, "first_month_free"
    if "2nd month free" in text or "second month free" in text:
        return True, "second_month_free"
    percent_match = re.search(r"(\d{1,2})%\s+off", text)
    if percent_match:
        return True, f"discount_{percent_match.group(1)}pct"
    if "left" in text or "limited" in text:
        return True, "limited_inventory"
    return True, "other"


def parse_availability(value: str | None) -> tuple[str | None, bool, str | None]:
    text = clean_text(value)
    if not text:
        return None, False, None
    lower = text.lower()
    units_left_match = re.search(r"(\d+\s+left)", lower)
    return (
        text,
        any(token in lower for token in ("left", "limited", "few remaining", "act fast")),
        units_left_match.group(1) if units_left_match else None,
    )


def best_visible_price(variants: dict[str, float | None]) -> float | None:
    for key in ("online_only_rate_monthly", "web_rate_monthly", "standard_rate_monthly", "in_store_rate_monthly"):
        if variants.get(key) is not None:
            return variants[key]
    return None


def normalized_unit_type(
    size_bucket: str | None,
    climate_controlled: bool,
    drive_up: bool,
    vehicle_parking: bool,
    indoor_outdoor: str | None,
) -> str | None:
    if vehicle_parking:
        return "vehicle_parking"
    if not size_bucket:
        return None
    if drive_up:
        return f"{size_bucket}_drive_up"
    if climate_controlled:
        return f"{size_bucket}_climate"
    return f"{size_bucket}_non_climate"


def create_unit_key(payload: dict[str, object]) -> str:
    ordered = [
        str(payload.get("company") or ""),
        str(payload.get("facility_id") or ""),
        str(payload.get("width_ft") or ""),
        str(payload.get("length_ft") or ""),
        str(payload.get("climate_controlled") or False),
        str(payload.get("indoor_outdoor") or ""),
        str(payload.get("drive_up") or False),
        str(payload.get("elevator_access") or False),
        str(payload.get("first_floor") or False),
        str(payload.get("upper_floor") or False),
        str(payload.get("vehicle_parking") or False),
        str(payload.get("parking_type") or ""),
        str(payload.get("normalized_size_bucket") or ""),
        str(payload.get("normalized_unit_type") or ""),
        str(payload.get("important_descriptors") or ""),
    ]
    digest = hashlib.sha1("|".join(ordered).encode("utf-8")).hexdigest()
    return digest


def derive_market_key(city: str | None, state: str | None) -> str | None:
    if not city or not state:
        return None
    return f"{city}, {state}"


def derive_proxy_msa(city: str | None, state: str | None) -> str | None:
    return derive_market_key(city, state)


def normalize_unit_snapshot(raw: RawUnitSnapshot, facility: FacilityRecord) -> NormalizedUnitSnapshot:
    size = parse_size_text(raw.raw_size_text, raw.raw_unit_label, raw.raw_feature_text)
    flags = parse_feature_flags(raw.raw_feature_text, raw.raw_unit_label)
    promo_flag, promo_type = parse_promo(raw.raw_promo_text)
    availability_text, limited_availability_flag, units_left_text = parse_availability(
        raw.raw_availability_text or raw.raw_promo_text
    )
    variants = parse_price_variants(raw.raw_price_text)
    best_price = best_visible_price(variants)
    ppsf = None
    if best_price is not None and size.square_feet and not math.isclose(size.square_feet, 0.0):
        ppsf = round(best_price / size.square_feet, 4)
    bucket = size.normalized_size_bucket
    unit_type = normalized_unit_type(
        bucket,
        bool(flags["climate_controlled"]),
        bool(flags["drive_up"]),
        bool(flags["vehicle_parking"]),
        flags["indoor_outdoor"],
    )
    important_descriptors = ",".join(
        sorted(
            descriptor
            for descriptor in [
                "climate" if flags["climate_controlled"] else "",
                "drive_up" if flags["drive_up"] else "",
                "elevator" if flags["elevator_access"] else "",
                "first_floor" if flags["first_floor"] else "",
                "upper_floor" if flags["upper_floor"] else "",
                "vehicle" if flags["vehicle_parking"] else "",
                flags["indoor_outdoor"] or "",
                flags["parking_type"] or "",
            ]
            if descriptor
        )
    )
    key_payload = {
        "company": raw.company,
        "facility_id": raw.facility_id,
        "width_ft": size.width_ft,
        "length_ft": size.length_ft,
        "climate_controlled": flags["climate_controlled"],
        "indoor_outdoor": flags["indoor_outdoor"],
        "drive_up": flags["drive_up"],
        "elevator_access": flags["elevator_access"],
        "first_floor": flags["first_floor"],
        "upper_floor": flags["upper_floor"],
        "vehicle_parking": flags["vehicle_parking"],
        "parking_type": flags["parking_type"],
        "normalized_size_bucket": bucket,
        "normalized_unit_type": unit_type,
        "important_descriptors": important_descriptors,
    }
    return NormalizedUnitSnapshot(
        run_id=raw.run_id,
        company=raw.company,
        facility_id=raw.facility_id,
        address=facility.address,
        city=facility.city,
        state=facility.state,
        zip=facility.zip,
        width_ft=size.width_ft,
        length_ft=size.length_ft,
        square_feet=size.square_feet,
        normalized_size_bucket=bucket,
        normalized_unit_type=unit_type,
        climate_controlled=bool(flags["climate_controlled"]),
        indoor_outdoor=flags["indoor_outdoor"],
        drive_up=bool(flags["drive_up"]),
        elevator_access=bool(flags["elevator_access"]),
        first_floor=bool(flags["first_floor"]),
        upper_floor=bool(flags["upper_floor"]),
        vehicle_parking=bool(flags["vehicle_parking"]),
        parking_type=flags["parking_type"],
        web_rate_monthly=variants["web_rate_monthly"],
        in_store_rate_monthly=variants["in_store_rate_monthly"],
        standard_rate_monthly=variants["standard_rate_monthly"],
        online_only_rate_monthly=variants["online_only_rate_monthly"],
        admin_fee=parse_admin_fee(raw.raw_admin_fee_text),
        promo_text=clean_text(raw.raw_promo_text),
        promo_type=promo_type,
        promo_flag=promo_flag,
        availability_text=availability_text,
        limited_availability_flag=limited_availability_flag,
        units_left_text=units_left_text,
        best_visible_price_monthly=best_price,
        price_per_sqft_month=ppsf,
        source_url=raw.source_url,
        scrape_timestamp=raw.scrape_timestamp,
        scrape_date=raw.scrape_date,
        unit_key=create_unit_key(key_payload),
        important_descriptors=important_descriptors or None,
        market_key=derive_market_key(facility.city, facility.state),
        msa=derive_proxy_msa(facility.city, facility.state),
        raw_size_text=raw.raw_size_text,
        raw_feature_text=raw.raw_feature_text,
        raw_price_text=raw.raw_price_text,
        raw_promo_text=raw.raw_promo_text,
        raw_availability_text=raw.raw_availability_text,
    )
