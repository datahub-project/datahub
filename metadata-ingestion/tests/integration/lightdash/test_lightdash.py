"""Integration test for the Lightdash ingestion source.

Uses a frozen-time, fully-mocked HTTP transport so the test is deterministic
and runs offline. Compares output against a golden file generated via
``pytest tests/integration/lightdash/test_lightdash.py --update-golden-files``.

Mocks at ``LightdashClient._session.get``: the source class calls into the
client and the client calls ``self._session.get(...)``. Stubbing at that level
gives us deterministic URL → fixture-file routing without having to fake
``requests.Session``-level retry/adapter machinery.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any
from unittest.mock import patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2025-05-01 00:00:00"

test_resources_dir = pathlib.Path(__file__).parent

PROJECT_UUID = "11111111-1111-1111-1111-000000000001"

# Map of Lightdash API path -> fixture filename under ``setup/``. The source
# only issues GETs against these endpoints (verified by reading
# LightdashClient).
PATH_TO_FIXTURE: dict[str, str] = {
    "/api/v1/org": "org.json",
    "/api/v1/org/projects": "org_projects.json",
    f"/api/v1/projects/{PROJECT_UUID}": f"project_{PROJECT_UUID}.json",
    f"/api/v1/projects/{PROJECT_UUID}/spaces": f"spaces_{PROJECT_UUID}.json",
    f"/api/v1/projects/{PROJECT_UUID}/charts": f"charts_{PROJECT_UUID}.json",
    f"/api/v1/projects/{PROJECT_UUID}/dashboards": (f"dashboards_{PROJECT_UUID}.json"),
    f"/api/v1/projects/{PROJECT_UUID}/explores/orders_enriched": (
        "explore_orders_enriched.json"
    ),
    "/api/v1/saved/33333333-3333-3333-3333-000000000001": (
        "saved_33333333-3333-3333-3333-000000000001.json"
    ),
    "/api/v1/saved/33333333-3333-3333-3333-000000000002": (
        "saved_33333333-3333-3333-3333-000000000002.json"
    ),
    "/api/v1/saved/33333333-3333-3333-3333-000000000003": (
        "saved_33333333-3333-3333-3333-000000000003.json"
    ),
    "/api/v1/saved/33333333-3333-3333-3333-000000000004": (
        "saved_33333333-3333-3333-3333-000000000004.json"
    ),
    "/api/v1/dashboards/44444444-4444-4444-4444-000000000001": (
        "dashboard_44444444-4444-4444-4444-000000000001.json"
    ),
    "/api/v1/dashboards/44444444-4444-4444-4444-000000000002": (
        "dashboard_44444444-4444-4444-4444-000000000002.json"
    ),
}


class _MockResponse:
    """Minimal stand-in for ``requests.Response`` that ``LightdashClient`` uses.

    Only the attributes the client actually touches are implemented.
    """

    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            from requests.exceptions import HTTPError

            raise HTTPError(f"Mock returned {self.status_code}")


def _mocked_get(self: Any, url: str, **kwargs: Any) -> _MockResponse:
    """Resolve ``url`` to the matching fixture file or raise.

    ``self`` is the ``requests.Session`` the client built; we ignore it.
    """

    base_marker = "://"
    if base_marker in url:
        url = url.split("://", 1)[1].split("/", 1)[1]
        url = "/" + url

    if url not in PATH_TO_FIXTURE:
        raise AssertionError(
            f"Unexpected Lightdash API path requested by source: {url!r}. "
            f"Add a fixture to PATH_TO_FIXTURE or assert that the source "
            f"should not call it."
        )

    fixture = test_resources_dir / "setup" / PATH_TO_FIXTURE[url]
    with open(fixture) as f:
        payload = json.load(f)
    return _MockResponse(payload)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_lightdash_ingest_success(pytestconfig, tmp_path):
    """End-to-end ingestion against canned API responses.

    Asserts the pipeline produces a metadata stream identical to the golden
    file, which captures one organisation, one project (clickhouse-backed),
    two spaces, four charts (covering all five Lightdash field kinds —
    dimension, metric, additionalMetric, customDimensionBin,
    customSqlDimension, plus one tableCalculation we expect to be reported
    as skipped), two dashboards, and full chart→column lineage to the
    upstream ClickHouse table.
    """

    with patch(
        "datahub.ingestion.source.lightdash.client.requests.Session.get",
        new=_mocked_get,
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "lightdash-integration-test",
                "source": {
                    "type": "lightdash",
                    "config": {
                        "connection": {
                            "base_url": "https://lightdash.example.com",
                            "personal_access_token": "test-pat",
                        },
                        "include_organization_container": False,
                        "extract_lineage": True,
                        "extract_owners": True,
                        "stateful_ingestion": {"enabled": False},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/lightdash_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/lightdash_mces.json",
            golden_path=test_resources_dir / "lightdash_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )
