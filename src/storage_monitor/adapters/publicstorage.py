from __future__ import annotations

import re
from datetime import date, datetime

from storage_monitor.adapters.base import BaseOperatorAdapter
from storage_monitor.models import FacilityRecord, RawUnitSnapshot


class PublicStorageAdapter(BaseOperatorAdapter):
    company = "public_storage"
    sitemap_url = "https://www.publicstorage.com/sitemap_0-product.xml"

    def discover_facilities(self, observed_at: datetime) -> list[FacilityRecord]:
        facilities: list[FacilityRecord] = []
        for url in self.parse_sitemap_urls(self.sitemap_url):
            match = re.search(r"/self-storage-(?P<state>[a-z]{2})-(?P<city>[^/]+)/(?P<facility_id>\d+)\.html", url, re.I)
            if not match:
                continue
            city = match.group("city").replace("-", " ").title()
            state = match.group("state").upper()
            facility_id = match.group("facility_id")
            record = self.facility_record(facility_id=facility_id, source_url=url, observed_at=observed_at)
            record.city = city
            record.state = state
            record.market_key = f"{city}, {state}"
            record.msa = record.market_key
            facilities.append(record)
        return facilities

    def scrape_facility(self, facility: FacilityRecord, scrape_date: date, run_id: str) -> tuple[FacilityRecord, list[RawUnitSnapshot]]:
        html = self.fetch_html(facility.source_url)
        soup = self.soup(html)
        cards = soup.select(".unit-list-item[data-unitid]")
        if not cards:
            html = self.fetch_html(facility.source_url, render=True)
            soup = self.soup(html)
            cards = soup.select(".unit-list-item[data-unitid]")
        facility = self.parse_facility_from_jsonld(facility, self.find_storage_json_ld(soup))
        lat, lon = self.parse_generic_lat_lon(html)
        facility = facility.model_copy(update={"latitude": facility.latitude or lat, "longitude": facility.longitude or lon})
        top_admin = soup.select_one(".admin-fee-banner-reserve-top")
        bottom_admin = soup.select_one(".admin-fee-banner-reserve-below")
        admin_fee_text = self.text_or_none(top_admin.get_text(" ", strip=True) if top_admin else None) or self.text_or_none(
            bottom_admin.get_text(" ", strip=True) if bottom_admin else None
        )
        units: list[RawUnitSnapshot] = []
        for card in cards:
            unit_id = card.get("data-unitid") or card.get("id")
            unit_name = card.select_one(".unit-name")
            raw_unit_label = self.text_or_none(unit_name.get_text(" ", strip=True) if unit_name else None)
            size_node = card.select_one(".unit-grid-item.unit-size .unit-size") or card.select_one(".size")
            raw_size_text = self.text_or_none(size_node.get_text(" ", strip=True) if size_node else None)
            feature_values = [node.get_text(" ", strip=True) for node in card.select(".unit-property-value")]
            class_features = self._class_features(card.get("class", []))
            raw_feature_text = self.join_text(feature_values + class_features, separator="; ")
            unit_price = card.select_one(".unit-price.label")
            online_only = None
            in_store = None
            if unit_price is not None:
                if unit_price.get("data-min-price"):
                    online_only = f"online_only:${unit_price.get('data-min-price')}"
                if unit_price.get("data-list-price"):
                    in_store = f"list:${unit_price.get('data-list-price')}"
            raw_price_text = self.join_text([online_only, in_store])
            promo_image = card.select_one(".promotion-content img")
            promo_detail = card.select_one(".promo-detail")
            raw_promo_text = self.join_text(
                [
                    promo_image.get("alt") if promo_image else None,
                    promo_detail.get_text(" ", strip=True) if promo_detail else None,
                ],
                separator="; ",
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
                    raw_size_text=raw_size_text,
                    raw_feature_text=raw_feature_text,
                    raw_price_text=raw_price_text,
                    raw_promo_text=raw_promo_text,
                    raw_availability_text="Available",
                    raw_admin_fee_text=admin_fee_text,
                    operator_unit_id=unit_id,
                    raw_metadata={
                        "source": "dom",
                        "selector": ".unit-list-item[data-unitid]",
                        "data_storeid": unit_price.get("data-storeid") if unit_price else None,
                        "data_unit_tier": unit_price.get("data-unit-tier") if unit_price else None,
                    },
                )
            )
        return facility, units

    @staticmethod
    def _class_features(class_tokens: list[str]) -> list[str]:
        mapping = {
            "ClimateControl": "Climate Controlled",
            "IsDriveUpAccess": "Drive Up Access",
            "IsVehicleUnit": "Vehicle Storage",
            "IsSmall": "Small",
            "IsMedium": "Medium",
            "IsLarge": "Large",
        }
        return [mapping[token] for token in class_tokens if token in mapping]
