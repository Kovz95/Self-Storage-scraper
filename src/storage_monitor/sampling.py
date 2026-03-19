from __future__ import annotations

from collections import defaultdict

from storage_monitor.models import FacilityRecord


def select_sampled_facilities(
    facilities: list[FacilityRecord],
    sample_size_per_company: int,
    max_markets: int | None = None,
) -> list[FacilityRecord]:
    if sample_size_per_company <= 0:
        return []
    by_market: dict[str, list[FacilityRecord]] = defaultdict(list)
    for facility in facilities:
        if facility.market_key:
            by_market[facility.market_key].append(facility)
    overlap_markets = sorted(
        (
            market
            for market, market_facilities in by_market.items()
            if len({facility.company for facility in market_facilities}) >= 2
        ),
        key=lambda market: (-len(by_market[market]), market),
    )
    if max_markets is not None:
        overlap_markets = overlap_markets[:max_markets]

    selected: list[FacilityRecord] = []
    for company in sorted({facility.company for facility in facilities}):
        company_facilities = [facility for facility in facilities if facility.company == company]
        prioritized = [
            facility
            for facility in company_facilities
            if facility.market_key in overlap_markets
        ]
        remainder = [
            facility
            for facility in company_facilities
            if facility.market_key not in overlap_markets
        ]
        by_state: dict[str, list[FacilityRecord]] = defaultdict(list)
        for facility in prioritized + remainder:
            by_state[facility.state or "NA"].append(facility)
        state_keys = sorted(by_state)
        company_pick: list[FacilityRecord] = []
        state_index = 0
        while len(company_pick) < sample_size_per_company and state_keys:
            state = state_keys[state_index % len(state_keys)]
            bucket = by_state[state]
            if bucket:
                company_pick.append(bucket.pop(0))
            state_index += 1
            if not any(by_state.values()):
                break
        selected.extend(company_pick[:sample_size_per_company])
    return selected
