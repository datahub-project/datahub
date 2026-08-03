import pathlib
from typing import Dict
from unittest.mock import patch

import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sap_mdg.odata_client import SapMdgODataClient
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"

_RESOURCES_DIR = pathlib.Path(__file__).parent


def _fixture_by_service() -> Dict[str, bytes]:
    return {
        "/sap/opu/odata/sap/ZMDG_DEMO_SRV": (
            _RESOURCES_DIR / "setup" / "ZMDG_DEMO_SRV.xml"
        ).read_bytes()
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_mdg_ingest(pytestconfig, tmp_path):
    fixtures = _fixture_by_service()
    output_path = tmp_path / "sap_mdg_mces.json"

    def _mock_fetch_metadata(self: SapMdgODataClient, service: str) -> bytes:
        return fixtures[service]

    with patch.object(SapMdgODataClient, "fetch_metadata", _mock_fetch_metadata):
        pipeline = Pipeline.create(
            {
                "run_id": "sap-mdg-test",
                "source": {
                    "type": "sap-mdg",
                    "config": {
                        "base_url": "https://sap-gw.example.com:44300",
                        "services": ["/sap/opu/odata/sap/ZMDG_DEMO_SRV"],
                        "username": "user",
                        "password": "pass",
                        "sap_client": "100",
                        "stateful_ingestion": {"enabled": False},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(output_path)},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=_RESOURCES_DIR / "sap_mdg_mces_golden.json",
    )
