import json
import pathlib
from typing import Any, Dict, Optional
from unittest import mock

import pytest
import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.salesforce_data360.client import SalesforceData360Client
from datahub.ingestion.source.salesforce_data360.source import (
    SalesforceData360Source,
)
from datahub.testing import mce_helpers

FROZEN_TIME = "2026-10-26 10:00:00"
INSTANCE = "mydomain.my.salesforce.com"
DATA_API = f"https://{INSTANCE}/services/data/v64.0/ssot/data-model-objects"

test_resources_dir = pathlib.Path(__file__).parent


def _read_response(file_name: str) -> Any:
    with open(test_resources_dir / "mock_files" / file_name) as file:
        return json.load(file)


class MockResponse:
    def __init__(self, json_data: Any, status_code: int = 200) -> None:
        self.json_data = json_data
        self.status_code = status_code

    def json(self) -> Any:
        return self.json_data


def side_effect_call_salesforce(
    method: str, url: str, params: Optional[Dict[str, Any]] = None
) -> MockResponse:
    if url == f"https://{INSTANCE}/services/data/":
        return MockResponse(_read_response("versions_response.json"))
    if url == DATA_API:
        assert params == {"limit": 100, "offset": 0}
        return MockResponse(_read_response("dmo_list_response.json"))
    if url == f"{DATA_API}/ssot__ContactPointEmail__dlm/relationships":
        return MockResponse(
            _read_response("contact_point_email_relationships_response.json")
        )
    if url == f"{DATA_API}/Loyalty_Member__dlm/relationships":
        return MockResponse(
            _read_response("loyalty_member_relationships_page1_response.json")
        )
    if url == (
        f"{DATA_API}/Loyalty_Member__dlm/relationships"
        "?dataspace=default&limit=500&offset=1"
    ):
        assert params is None
        return MockResponse(
            _read_response("loyalty_member_relationships_page2_response.json")
        )
    if url == f"{DATA_API}/ssot__Individual__dlm/relationships":
        return MockResponse(None, 204)
    raise AssertionError(f"Unexpected request: {url}")


def _mock_salesforce(mock_sdk: mock.Mock) -> mock.Mock:
    mock_sf = mock.Mock()
    mock_sf.sf_instance = INSTANCE
    mock_sf._call_salesforce = mock.Mock(side_effect=side_effect_call_salesforce)
    mock_sdk.return_value = mock_sf
    return mock_sf


def _source_config(**overrides: Any) -> Dict[str, Any]:
    return {
        "auth": "DIRECT_ACCESS_TOKEN",
        "instance_url": f"https://{INSTANCE}/",
        "access_token": "access_token",
        **overrides,
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_salesforce_data360_ingest(
    pytestconfig: pytest.Config, tmp_path: pathlib.Path
) -> None:
    with mock.patch("datahub.ingestion.source.salesforce.Salesforce") as mock_sdk:
        _mock_salesforce(mock_sdk)
        pipeline = Pipeline.create(
            {
                "run_id": "salesforce-data360-test",
                "source": {"type": "salesforce-data360", "config": _source_config()},
                "sink": {
                    "type": "file",
                    "config": {"filename": f"{tmp_path}/salesforce_data360_mces.json"},
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/salesforce_data360_mces.json",
        golden_path=test_resources_dir / "salesforce_data360_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )

    source = pipeline.source
    assert isinstance(source, SalesforceData360Source)
    assert source.report.dmos_scanned == 4
    assert source.report.dmos_ingested == 3
    assert list(source.report.filtered) == ["ssot__AbnExperiment__dlm"]
    assert source.report.foreign_keys_emitted == 3
    assert source.report.inactive_relationships_skipped == 1
    assert list(source.report.unknown_field_types) == [
        "Loyalty_Member__dlm.Card_Image__c: Blob"
    ]


@mock.patch("datahub.ingestion.source.salesforce.Salesforce")
def test_include_disabled_dmos_and_pattern(mock_sdk: mock.Mock) -> None:
    _mock_salesforce(mock_sdk)
    source = SalesforceData360Source.create(
        _source_config(
            include_disabled_dmos=True,
            dmo_pattern={"deny": ["^ssot__ContactPointEmail__dlm$"]},
        ),
        PipelineContext(run_id="salesforce-data360-test"),
    )
    with mock.patch.object(
        SalesforceData360Client, "list_relationships", return_value=[]
    ):
        list(source.get_workunits())
    assert source.report.dmos_ingested == 3
    assert list(source.report.filtered) == ["ssot__ContactPointEmail__dlm"]


@mock.patch("datahub.ingestion.source.salesforce.Salesforce")
def test_relationship_failure_keeps_dmo(mock_sdk: mock.Mock) -> None:
    _mock_salesforce(mock_sdk)
    source = SalesforceData360Source.create(
        _source_config(), PipelineContext(run_id="salesforce-data360-test")
    )
    with mock.patch.object(
        SalesforceData360Client,
        "list_relationships",
        side_effect=RuntimeError("relationships unavailable"),
    ):
        list(source.get_workunits())
    assert source.report.dmos_ingested == 3
    assert source.report.foreign_keys_emitted == 0
    assert len(source.report.warnings) == 1


def test_list_dmos_pages_by_offset_and_stops_on_repeated_page() -> None:
    page = {"dataModelObject": [{"name": f"DMO_{i}__dlm"} for i in range(100)]}
    sf = mock.Mock(sf_instance=INSTANCE, sf_version="64.0")
    sf._call_salesforce = mock.Mock(return_value=MockResponse(page))

    dmos = list(SalesforceData360Client(sf).list_dmos())

    assert len(dmos) == 100
    offsets = [c.kwargs["params"]["offset"] for c in sf._call_salesforce.mock_calls]
    assert offsets == [0, 100]
