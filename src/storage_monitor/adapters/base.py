from __future__ import annotations

import json
import logging
import re
import urllib.request
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
from requests import HTTPError

from storage_monitor.models import FacilityRecord, RawUnitSnapshot
from storage_monitor.utils.http import DEFAULT_HEADERS
from storage_monitor.utils.browser import render_page_html
from storage_monitor.utils.http import HttpClient


LOGGER = logging.getLogger(__name__)
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class BaseOperatorAdapter(ABC):
    company: str
    sitemap_url: str

    def __init__(self, http_client: HttpClient | None = None) -> None:
        self.http = http_client or HttpClient()

    @abstractmethod
    def discover_facilities(self, observed_at: datetime) -> list[FacilityRecord]:
        raise NotImplementedError

    @abstractmethod
    def scrape_facility(self, facility: FacilityRecord, scrape_date: date, run_id: str) -> tuple[FacilityRecord, list[RawUnitSnapshot]]:
        raise NotImplementedError

    def fetch_html(self, url: str, render: bool = False) -> str:
        if render:
            return render_page_html(url)
        try:
            response = self.http.get(url)
            return response.text
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                return self._fetch_via_urllib(url)
            raise

    def parse_sitemap_urls(self, sitemap_url: str) -> list[str]:
        try:
            response = self.http.get(sitemap_url)
            payload = response.text
        except HTTPError as exc:
            if exc.response is None or exc.response.status_code != 403:
                raise
            payload = self._fetch_via_urllib(sitemap_url)
        root = ET.fromstring(payload)
        return [node.text.strip() for node in root.findall(".//sm:loc", SITEMAP_NS) if node.text]

    def facility_record(self, facility_id: str, source_url: str, observed_at: datetime) -> FacilityRecord:
        return FacilityRecord(
            company=self.company,
            facility_id=facility_id,
            source_url=source_url,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
            active_flag=True,
        )

    def extract_json_ld(self, soup: BeautifulSoup) -> list[dict]:
        payloads: list[dict] = []
        for script in soup.select('script[type="application/ld+json"]'):
            if not script.string:
                continue
            try:
                parsed = json.loads(script.string)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, list):
                payloads.extend(item for item in parsed if isinstance(item, dict))
            elif isinstance(parsed, dict):
                payloads.append(parsed)
        return payloads

    def extract_next_data(self, soup: BeautifulSoup) -> dict:
        node = soup.select_one("#__NEXT_DATA__")
        if node is None or not node.string:
            return {}
        try:
            payload = json.loads(node.string)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def find_storage_json_ld(self, soup: BeautifulSoup) -> dict:
        for payload in self.extract_json_ld(soup):
            payload_type = str(payload.get("@type", "")).lower()
            if any(token in payload_type for token in ("selfstorage", "localbusiness", "storage")):
                return payload
        return {}

    def parse_facility_from_jsonld(self, facility: FacilityRecord, payload: dict) -> FacilityRecord:
        address = payload.get("address", {}) if isinstance(payload.get("address"), dict) else {}
        geo = payload.get("geo", {}) if isinstance(payload.get("geo"), dict) else {}
        city = address.get("addressLocality") or facility.city
        state = address.get("addressRegion") or facility.state
        updated = facility.model_copy(
            update={
                "facility_name": payload.get("name") or facility.facility_name,
                "address": address.get("streetAddress") or facility.address,
                "city": city,
                "state": state,
                "zip": address.get("postalCode") or facility.zip,
                "latitude": self._coerce_float(geo.get("latitude")) or facility.latitude,
                "longitude": self._coerce_float(geo.get("longitude")) or facility.longitude,
                "market_key": f"{city}, {state}" if city and state else facility.market_key,
                "msa": f"{city}, {state}" if city and state else facility.msa,
            }
        )
        return updated

    def parse_generic_lat_lon(self, html: str) -> tuple[float | None, float | None]:
        lat_match = re.search(r'"latitude"\s*:\s*"?(?P<lat>-?\d+\.\d+)', html)
        lon_match = re.search(r'"longitude"\s*:\s*"?(?P<lon>-?\d+\.\d+)', html)
        lat = self._coerce_float(lat_match.group("lat")) if lat_match else None
        lon = self._coerce_float(lon_match.group("lon")) if lon_match else None
        return lat, lon

    def is_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            parser.read()
        except Exception:
            LOGGER.warning("robots_fetch_failed", extra={"extra_data": {"company": self.company, "robots_url": robots_url}})
            return True
        return parser.can_fetch("*", url)

    @staticmethod
    def now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def soup(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def text_or_none(value: str | None) -> str | None:
        if value is None:
            return None
        return re.sub(r"\s+", " ", value).strip() or None

    @staticmethod
    def join_text(parts: list[str | None], separator: str = " | ") -> str | None:
        cleaned = [BaseOperatorAdapter.text_or_none(part) for part in parts]
        joined = separator.join(part for part in cleaned if part)
        return joined or None

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _fetch_via_urllib(url: str) -> str:
        request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.read().decode("utf-8", errors="ignore")
