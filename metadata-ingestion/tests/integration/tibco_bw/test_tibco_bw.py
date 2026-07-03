from pathlib import Path

import pytest
import time_machine
from requests_mock import Mocker

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"

_IGNORE_PATHS = [
    r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
    r"root\[\d+\]\['systemMetadata'\]\['runId'\]",
]


def _mock_bwagent(requests_mock: Mocker) -> None:
    base = "http://bw:8079"
    requests_mock.get(f"{base}/bw/v1/domains", json=[{"name": "MyDomain"}])
    requests_mock.get(
        f"{base}/bw/v1/domains/MyDomain/appspaces",
        json=[{"name": "MyAppSpace", "description": "primary appspace"}],
    )
    requests_mock.get(
        f"{base}/bw/v1/domains/MyDomain/appspaces/MyAppSpace/appnodes",
        json=[{"name": "node1", "status": "Running"}],
    )
    requests_mock.get(
        f"{base}/bw/v1/domains/MyDomain/appspaces/MyAppSpace/applications",
        json=[
            {"name": "orders", "version": "1.0", "state": "Running", "appType": "bwce"},
            {
                "name": "billing",
                "version": "2.1",
                "state": "Stopped",
                "appType": "bwce",
            },
        ],
    )


def _mock_tci(requests_mock: Mocker) -> None:
    base = "https://api.cloud.tibco.com"
    requests_mock.get(
        f"{base}/v1/userinfo",
        json={
            "subscriptions": [
                {"subscriptionId": "sub-1", "name": "Prod", "orgDisplayName": "Acme"}
            ]
        },
    )
    requests_mock.get(
        f"{base}/v1/subscriptions/sub-1/apps",
        json=[
            {"name": "sync", "type": "flogo", "status": "STARTED", "version": "3"},
            {"name": "report", "type": "bwce", "status": "STOPPED"},
        ],
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_tibco_bw_on_prem_ingest(
    pytestconfig: pytest.Config, tmp_path: Path, requests_mock: Mocker
) -> None:
    _mock_bwagent(requests_mock)
    test_resources_dir = pytestconfig.rootpath / "tests/integration/tibco_bw"
    output = tmp_path / "tibco_bw_on_prem_mces.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "tibco-bw",
                "config": {
                    "deployment": "on_prem",
                    "base_url": "http://bw:8079",
                    "username": "user",
                    "password": "secret",
                },
            },
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "tibco_bw_on_prem_mces_golden.json",
        ignore_paths=_IGNORE_PATHS,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_tibco_bw_cloud_ingest(
    pytestconfig: pytest.Config, tmp_path: Path, requests_mock: Mocker
) -> None:
    _mock_tci(requests_mock)
    test_resources_dir = pytestconfig.rootpath / "tests/integration/tibco_bw"
    output = tmp_path / "tibco_bw_cloud_mces.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "tibco-bw",
                "config": {"deployment": "cloud", "token": "abc"},
            },
            "sink": {"type": "file", "config": {"filename": str(output)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output,
        golden_path=test_resources_dir / "tibco_bw_cloud_mces_golden.json",
        ignore_paths=_IGNORE_PATHS,
    )
