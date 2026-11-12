"""Last-modified dates in tool results."""

import pytest

from datahub_core_mcp_tools.freshness import date_from_millis, with_last_modified_dates

# 2026-05-20T13:45:00Z: the time of day must not reach the agent.
MAY_20_MS = 1779284700000


def test_date_from_millis_is_the_utc_calendar_date():
    assert date_from_millis(MAY_20_MS) == "2026-05-20"
    # 23:30 on May 20 in UTC-5 is already May 21 in UTC.
    assert date_from_millis(1779337800000) == "2026-05-21"


@pytest.mark.parametrize("value", [0, -1, None, True, "1779284700000", 1e30])
def test_unknown_or_malformed_times_have_no_date(value):
    # GMS reports an unknown dataset modification time as 0.
    assert date_from_millis(value) is None


def test_stamps_become_dates_and_unknown_ones_go():
    response: dict = {
        "searchResults": [
            {"entity": {"properties": {"lastModified": {"time": MAY_20_MS}}}},
            {"entity": {"properties": {"name": "t", "lastModified": {"time": 0}}}},
            {"entity": {"info": {"lastModified": {"time": MAY_20_MS, "actor": "x"}}}},
        ]
    }

    with_last_modified_dates(response)

    first, second, third = (r["entity"] for r in response["searchResults"])
    assert first["properties"]["lastModified"] == "2026-05-20"
    assert second["properties"] == {"name": "t"}
    assert third["info"]["lastModified"] == "2026-05-20"
    # Applying it twice changes nothing.
    assert with_last_modified_dates(response) == response


def test_switched_off_no_output_carries_a_date(monkeypatch):
    monkeypatch.setenv("LAST_MODIFIED_DATES_ENABLED", "false")
    response = {
        "entity": {
            "properties": {"name": "orders", "lastModified": {"time": MAY_20_MS}},
            "info": {"title": "Runbook", "lastModified": {"time": MAY_20_MS}},
        }
    }

    with_last_modified_dates(response)

    assert date_from_millis(MAY_20_MS) is None
    assert response == {
        "entity": {"properties": {"name": "orders"}, "info": {"title": "Runbook"}}
    }
