from __future__ import annotations
import re
from datetime import date, datetime
from urllib.parse import urlparse

from storage_monitor.adapters.base import BaseOperatorAdapter
from storage_monitor.models import FacilityRecord, RawUnitSnapshot


class ExtraSpaceAdapter(BaseOperatorAdapter):
    company = "extra_space"
    sitemap_url = "https://www.extraspace.com/facility-sitemap.xml"

    def discover_facilities(self, observed_at: datetime) -> list[FacilityRecord]:
        facilities: list[FacilityRecord] = []
        for url in self.parse_sitemap_urls(self.sitemap_url):
            path = urlparse(url).path.strip("/").split("/")
            facility_id = path[-1]
            state = path[-3].replace("_", " ").title() if len(path) >= 4 else None
            city = path[-2].replace("_", " ").title() if len(path) >= 3 else None
            record = self.facility_record(facility_id=facility_id, source_url=url, observed_at=observed_at)
            record.state = self._state_code_from_name(state)
            record.city = city
            record.market_key = f"{city}, {record.state}" if city and record.state else None
            record.msa = record.market_key
            facilities.append(record)
        return facilities

    def scrape_facility(self, facility: FacilityRecord, scrape_date: date, run_id: str) -> tuple[FacilityRecord, list[RawUnitSnapshot]]:
        html = self.fetch_html(facility.source_url)
        soup = self.soup(html)
        facility = self.parse_facility_from_jsonld(facility, self.find_storage_json_ld(soup))
        lat, lon = self.parse_generic_lat_lon(html)
        facility = facility.model_copy(update={"latitude": facility.latitude or lat, "longitude": facility.longitude or lon})
        cards = soup.select(".unit-class-card-box[data-qa='unit-class-card']")
        lookup = self._next_data_lookup(soup)
        if not cards:
            fallback_units = self._parse_units_from_next_data(soup, facility, scrape_date, run_id)
            if fallback_units:
                return facility, fallback_units
            html = self.fetch_html(facility.source_url, render=True)
            soup = self.soup(html)
            cards = soup.select(".unit-class-card-box[data-qa='unit-class-card']")
            lookup = self._next_data_lookup(soup)
        return facility, self._parse_units_from_cards(cards, facility, scrape_date, run_id, lookup)

    def _parse_units_from_cards(
        self,
        cards,
        facility: FacilityRecord,
        scrape_date: date,
        run_id: str,
        lookup: dict[str, dict] | None = None,
    ) -> list[RawUnitSnapshot]:
        units: list[RawUnitSnapshot] = []
        for card in cards:
            operator_unit_id = card.get("id")
            shirt = card.select_one("[data-qa='unit-size'] .shirt-size")
            dims = card.select_one("[data-qa='unit-size'] .width-depth")
            raw_size_text = self.join_text(
                [
                    shirt.get_text(" ", strip=True) if shirt else None,
                    dims.get_text(" ", strip=True) if dims else None,
                ],
                separator=" ",
            )
            if not raw_size_text:
                continue
            next_unit = (lookup or {}).get(operator_unit_id or "")
            raw_feature_text = self.join_text(
                [li.get_text(" ", strip=True) for li in card.select("[data-qa='features'] li")]
                or self._next_unit_features(next_unit),
                separator="; ",
            )
            availability_text = self.text_or_none(
                card.select_one(".promo-container .highlighted-promo") and card.select_one(".promo-container .highlighted-promo").get_text(" ", strip=True)
            )
            raw_price_text = self.join_text(
                [
                    self._label_price("web", card.select_one("[data-qa='web-price']")),
                    self._label_price("in_store", card.select_one("[data-qa='in-store-price']")),
                ]
            ) or self._next_unit_price_text(next_unit)
            raw_promo_text = self._next_unit_promo_text(next_unit)
            raw_availability_text = availability_text or self._availability_text(next_unit.get("availability", {}) if next_unit else {})
            units.append(
                RawUnitSnapshot(
                    run_id=run_id,
                    company=self.company,
                    facility_id=facility.facility_id,
                    source_url=facility.source_url,
                    scrape_date=scrape_date,
                    scrape_timestamp=self.now(),
                    raw_unit_label=raw_size_text,
                    raw_size_text=raw_size_text,
                    raw_feature_text=raw_feature_text,
                    raw_price_text=raw_price_text,
                    raw_promo_text=raw_promo_text,
                    raw_availability_text=raw_availability_text,
                    operator_unit_id=operator_unit_id,
                    raw_metadata={
                        "source": "dom+__NEXT_DATA__" if next_unit else "dom",
                        "selector": ".unit-class-card-box",
                        "legacy_coding": next_unit.get("legacyCoding") if next_unit else None,
                    },
                )
            )
        return units

    def _parse_units_from_next_data(
        self,
        soup,
        facility: FacilityRecord,
        scrape_date: date,
        run_id: str,
    ) -> list[RawUnitSnapshot]:
        lookup = self._next_data_lookup(soup)
        units: list[RawUnitSnapshot] = []
        for unit in lookup.values():
            dimensions = unit.get("dimensions", {}) if isinstance(unit.get("dimensions"), dict) else {}
            raw_size_text = self.join_text(
                ["Locker" if unit.get("attribute", {}).get("locker") else dimensions.get("size"), dimensions.get("display")],
                separator=" ",
            )
            if not raw_size_text:
                continue
            units.append(
                RawUnitSnapshot(
                    run_id=run_id,
                    company=self.company,
                    facility_id=facility.facility_id,
                    source_url=facility.source_url,
                    scrape_date=scrape_date,
                    scrape_timestamp=self.now(),
                    raw_unit_label=raw_size_text,
                    raw_size_text=raw_size_text,
                    raw_feature_text=self.join_text(self._next_unit_features(unit), separator="; "),
                    raw_price_text=self._next_unit_price_text(unit),
                    raw_promo_text=self._next_unit_promo_text(unit),
                    raw_availability_text=self._availability_text(unit.get("availability", {})),
                    operator_unit_id=str(unit.get("uid") or unit.get("salesforceId") or unit.get("breezeId") or ""),
                    raw_metadata={"source": "__NEXT_DATA__", "legacy_coding": unit.get("legacyCoding")},
                )
            )
        return units

    @staticmethod
    def _state_code_from_name(value: str | None) -> str | None:
        mapping = {
            "Alabama": "AL",
            "Arizona": "AZ",
            "California": "CA",
            "Colorado": "CO",
            "Connecticut": "CT",
            "Florida": "FL",
            "Georgia": "GA",
            "Illinois": "IL",
            "Indiana": "IN",
            "Kansas": "KS",
            "Kentucky": "KY",
            "Louisiana": "LA",
            "Maryland": "MD",
            "Massachusetts": "MA",
            "Michigan": "MI",
            "Minnesota": "MN",
            "Missouri": "MO",
            "Nevada": "NV",
            "New Jersey": "NJ",
            "New York": "NY",
            "North Carolina": "NC",
            "Ohio": "OH",
            "Oklahoma": "OK",
            "Oregon": "OR",
            "Pennsylvania": "PA",
            "South Carolina": "SC",
            "Tennessee": "TN",
            "Texas": "TX",
            "Utah": "UT",
            "Virginia": "VA",
            "Washington": "WA",
        }
        return mapping.get(value or "")

    @staticmethod
    def _label_price(label: str, node: object) -> str | None:
        if node is None:
            return None
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True))  # type: ignore[union-attr]
        amount_match = re.search(r"\$\s*\d+(?:\.\d{1,2})?", text)
        return f"{label}:{amount_match.group(0)}" if amount_match else None

    @staticmethod
    def _label_numeric_price(label: str, value: object) -> str | None:
        if value in (None, ""):
            return None
        try:
            amount = float(value)
        except (TypeError, ValueError):
            return None
        if amount.is_integer():
            return f"{label}:${int(amount)}"
        return f"{label}:${amount:.2f}"

    def _next_data_lookup(self, soup) -> dict[str, dict]:
        payload = self.extract_next_data(soup)
        unit_classes = (
            payload.get("props", {})
            .get("pageProps", {})
            .get("pageData", {})
            .get("data", {})
            .get("unitClasses", {})
            .get("data", {})
            .get("unitClasses", [])
        )
        if not isinstance(unit_classes, list):
            return {}
        lookup: dict[str, dict] = {}
        for unit in unit_classes:
            if not isinstance(unit, dict) or unit.get("attribute", {}).get("virtual"):
                continue
            unit_id = str(unit.get("uid") or unit.get("salesforceId") or unit.get("breezeId") or "")
            if unit_id:
                lookup[unit_id] = unit
        return lookup

    def _next_unit_features(self, unit: dict | None) -> list[str]:
        if not unit:
            return []
        features = unit.get("features", [])
        if not isinstance(features, list):
            return []
        return [feature.get("display") for feature in features if isinstance(feature, dict) and feature.get("display")]

    def _next_unit_price_text(self, unit: dict | None) -> str | None:
        if not unit:
            return None
        rates = unit.get("rates", {}) if isinstance(unit.get("rates"), dict) else {}
        return self.join_text(
            [
                self._label_numeric_price("web", rates.get("tier1") or rates.get("web")),
                self._label_numeric_price("in_store", rates.get("walkIn") or rates.get("street")),
                self._label_numeric_price("standard", rates.get("web")),
            ]
        )

    def _next_unit_promo_text(self, unit: dict | None) -> str | None:
        if not unit:
            return None
        promotions = unit.get("promotions", [])
        if not isinstance(promotions, list):
            return None
        return self.join_text(
            [
                promotion.get("discount", {}).get("description")
                for promotion in promotions
                if isinstance(promotion, dict)
            ],
            separator="; ",
        )

    @staticmethod
    def _availability_text(availability: dict) -> str | None:
        available = availability.get("available")
        if available == 1:
            return "1 left"
        if isinstance(available, int) and available > 1 and available <= 5:
            return f"{available} left"
        if availability.get("showLimited"):
            return "limited availability"
        if availability.get("showFirstCome"):
            return "limited availability"
        if availability.get("showAvailable"):
            return "available"
        return None
