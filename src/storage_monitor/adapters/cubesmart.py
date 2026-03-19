from __future__ import annotations

import re
from datetime import date, datetime

from storage_monitor.adapters.base import BaseOperatorAdapter
from storage_monitor.models import FacilityRecord, RawUnitSnapshot


class CubeSmartAdapter(BaseOperatorAdapter):
    company = "cube_smart"
    sitemap_url = "https://www.cubesmart.com/sitemap-facility.xml"

    def discover_facilities(self, observed_at: datetime) -> list[FacilityRecord]:
        facilities: list[FacilityRecord] = []
        for url in self.parse_sitemap_urls(self.sitemap_url):
            match = re.search(
                r"https://www\.cubesmart\.com/(?P<state>[a-z-]+)-self-storage/(?P<city>[a-z-]+)-self-storage/(?P<facility_id>\d+)\.html",
                url,
                re.I,
            )
            if not match:
                continue
            city = match.group("city").replace("-", " ").title()
            state = self._state_code_from_slug(match.group("state"))
            facility_id = match.group("facility_id")
            record = self.facility_record(facility_id=facility_id, source_url=url, observed_at=observed_at)
            record.city = city
            record.state = state
            record.market_key = f"{city}, {state}" if city and state else None
            record.msa = record.market_key
            facilities.append(record)
        return facilities

    def scrape_facility(self, facility: FacilityRecord, scrape_date: date, run_id: str) -> tuple[FacilityRecord, list[RawUnitSnapshot]]:
        html = self.fetch_html(facility.source_url)
        soup = self.soup(html)
        cards = soup.select(".csUnitFacilityListing")
        if not cards:
            html = self.fetch_html(facility.source_url, render=True)
            soup = self.soup(html)
            cards = soup.select(".csUnitFacilityListing")
        facility = self.parse_facility_from_jsonld(facility, self.find_storage_json_ld(soup))
        lat, lon = self.parse_generic_lat_lon(html)
        facility = facility.model_copy(update={"latitude": facility.latitude or lat, "longitude": facility.longitude or lon})
        units: list[RawUnitSnapshot] = []
        for index, card in enumerate(cards, start=1):
            visible_label = card.select_one(".csUnitColumn01 p [aria-hidden='true']")
            unit_name = visible_label or card.select_one(".csUnitColumn01 p")
            raw_unit_label = self.text_or_none(unit_name.get_text(" ", strip=True) if unit_name else None)
            raw_feature_text = self.join_text(
                self._dedupe(
                    [li.get_text(" ", strip=True) for li in card.select(".csDisplayFeatures li")]
                    + self._encoded_features(card.get("data-encodedfeatures"))
                ),
                separator="; ",
            )
            promo_price = card.select_one(".ptDiscountPriceSpan")
            in_store_price = card.select_one(".ptOriginalPriceSpan")
            raw_price_text = self.join_text(
                [
                    f"online_only:{promo_price.get_text(' ', strip=True)}" if promo_price else self._label_numeric_price("online_only", card.get("data-unitprice")),
                    f"in_store:{in_store_price.get_text(' ', strip=True)}" if in_store_price else self._label_numeric_price("in_store", card.get("data-price")),
                ]
            )
            promo_text = self.text_or_none(
                card.select_one(".promotions-text") and card.select_one(".promotions-text").get_text(" ", strip=True)
            )
            units.append(
                RawUnitSnapshot(
                    run_id=run_id,
                    company=self.company,
                    facility_id=facility.facility_id,
                    source_url=facility.source_url,
                    scrape_date=scrape_date,
                    scrape_timestamp=self.now(),
                    raw_unit_label=raw_unit_label,
                    raw_size_text=raw_unit_label,
                    raw_feature_text=raw_feature_text,
                    raw_price_text=raw_price_text,
                    raw_promo_text=promo_text,
                    raw_availability_text="Available",
                    operator_unit_id=f"{facility.facility_id}_{index}",
                    raw_metadata={
                        "source": "dom",
                        "selector": ".csUnitFacilityListing",
                        "data_price": card.get("data-price"),
                        "data_unitprice": card.get("data-unitprice"),
                        "encoded_features": card.get("data-encodedfeatures"),
                    },
                )
            )
        return facility, units

    @staticmethod
    def _state_code_from_slug(value: str | None) -> str | None:
        mapping = {
            "alabama": "AL",
            "arizona": "AZ",
            "california": "CA",
            "colorado": "CO",
            "connecticut": "CT",
            "florida": "FL",
            "georgia": "GA",
            "illinois": "IL",
            "indiana": "IN",
            "kentucky": "KY",
            "louisiana": "LA",
            "maryland": "MD",
            "massachusetts": "MA",
            "michigan": "MI",
            "missouri": "MO",
            "nevada": "NV",
            "new-jersey": "NJ",
            "new-york": "NY",
            "north-carolina": "NC",
            "ohio": "OH",
            "oklahoma": "OK",
            "oregon": "OR",
            "pennsylvania": "PA",
            "south-carolina": "SC",
            "tennessee": "TN",
            "texas": "TX",
            "utah": "UT",
            "virginia": "VA",
            "washington": "WA",
        }
        return mapping.get(value or "")

    @staticmethod
    def _encoded_features(value: str | None) -> list[str]:
        mapping = {
            "C": "Climate Controlled",
            "D": "Drive Up Access",
            "E": "Elevator Access",
            "N": "Indoor",
            "O": "Outdoor",
            "P": "Parking",
        }
        if not value:
            return []
        return [feature for token, feature in mapping.items() if token in value]

    @staticmethod
    def _dedupe(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                ordered.append(value)
        return ordered

    @staticmethod
    def _label_numeric_price(label: str, value: object) -> str | None:
        if value in (None, ""):
            return None
        try:
            amount = float(str(value).replace("$", "").strip())
        except (TypeError, ValueError):
            return None
        if amount.is_integer():
            return f"{label}:${int(amount)}"
        return f"{label}:${amount:.2f}"
