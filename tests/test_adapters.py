from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from storage_monitor.adapters import CubeSmartAdapter, ExtraSpaceAdapter, PublicStorageAdapter
from storage_monitor.models import FacilityRecord


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _facility(company: str, facility_id: str, url: str) -> FacilityRecord:
    return FacilityRecord(
        company=company,
        facility_id=facility_id,
        source_url=url,
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
        active_flag=True,
    )


def test_extraspace_adapter_parses_embedded_json(monkeypatch) -> None:
    html = (FIXTURES / "extraspace_facility.html").read_text(encoding="utf-8")
    adapter = ExtraSpaceAdapter()
    monkeypatch.setattr(adapter, "fetch_html", lambda url, render=False: html)
    facility, units = adapter.scrape_facility(
        _facility("extra_space", "1937", "https://www.extraspace.com/storage/facilities/us/alabama/birmingham/1937/"),
        scrape_date=date(2026, 3, 18),
        run_id="test-run",
    )
    assert facility.city == "Birmingham"
    assert len(units) == 1
    assert units[0].operator_unit_id == "2555_1937"
    assert "web:$54" in (units[0].raw_price_text or "")
    assert "First Month Free" in (units[0].raw_promo_text or "")


def test_public_storage_adapter_parses_unit_attributes(monkeypatch) -> None:
    html = (FIXTURES / "publicstorage_facility.html").read_text(encoding="utf-8")
    adapter = PublicStorageAdapter()
    monkeypatch.setattr(adapter, "fetch_html", lambda url, render=False: html)
    facility, units = adapter.scrape_facility(
        _facility("public_storage", "297", "https://www.publicstorage.com/self-storage-al-mobile/297.html"),
        scrape_date=date(2026, 3, 18),
        run_id="test-run",
    )
    assert facility.city == "Mobile"
    assert len(units) == 1
    assert units[0].raw_admin_fee_text is not None
    assert "online_only:$21.0" in (units[0].raw_price_text or "")
    assert "Drive up access" in (units[0].raw_feature_text or "")


def test_cubesmart_adapter_parses_unit_cards(monkeypatch) -> None:
    html = (FIXTURES / "cubesmart_facility.html").read_text(encoding="utf-8")
    adapter = CubeSmartAdapter()
    monkeypatch.setattr(adapter, "fetch_html", lambda url, render=False: html)
    facility, units = adapter.scrape_facility(
        _facility("cube_smart", "4243", "https://www.cubesmart.com/alabama-self-storage/auburn-self-storage/4243.html"),
        scrape_date=date(2026, 3, 18),
        run_id="test-run",
    )
    assert facility.city == "Auburn"
    assert len(units) == 1
    assert "online_only:$47.70" in (units[0].raw_price_text or "")
    assert "10% Off" in (units[0].raw_promo_text or "")
