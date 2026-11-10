import json
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Set
from unittest.mock import Mock, patch

import pytest
import requests_mock
from databricks.sdk.core import ApiClient, Config

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.unity.genie_client import GenieSpaceClient
from datahub.ingestion.source.unity.source import UnityCatalogSource
from datahub.metadata.schema_classes import (
    AIAgentInfoClass,
    StatusClass,
    UpstreamLineageClass,
)

pytestmark = pytest.mark.integration_batch_1

HOST = "https://workspace.example.com"
SPACES_URL = f"{HOST}/api/2.0/genie/spaces"


def test_sdk_transport_recovers_from_throttled_listing() -> None:
    api = ApiClient(
        Config(host=HOST, token="fake-token", auth_type="pat", retry_timeout_seconds=10)
    )
    with requests_mock.Mocker() as http:
        listing = http.get(
            SPACES_URL,
            [
                {
                    "status_code": 429,
                    "headers": {"Retry-After": "0"},
                    "json": {"error_code": "TOO_MANY_REQUESTS", "message": "Throttled"},
                },
                {
                    "status_code": 200,
                    "json": {"spaces": [{"space_id": "a", "title": "Assistant"}]},
                },
            ],
        )
        assert [space.space_id for space in GenieSpaceClient(api).list_spaces()] == [
            "a"
        ]
        assert listing.call_count == 2


def run_pipeline(tmp_path: Path, run: str) -> Pipeline:
    pipeline = Pipeline.create(
        {
            "run_id": run,
            "pipeline_name": "genie-mock-pipeline",
            "source": {
                "type": "unity-catalog",
                "config": {
                    "workspace_url": HOST,
                    "token": "fake-token",
                    "include_hive_metastore": False,
                    "include_ownership": False,
                    "include_usage_statistics": False,
                    "include_genie_spaces": True,
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": True,
                        "fail_safe_threshold": 100,
                        "state_provider": {
                            "type": "file",
                            "config": {"filename": str(tmp_path / "checkpoint.json")},
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(tmp_path / f"{run}.json")},
            },
        }
    )
    pipeline.run()
    return pipeline


def emitted_proposals(tmp_path: Path, run: str) -> List[MetadataChangeProposalWrapper]:
    return [
        MetadataChangeProposalWrapper.from_obj_require_wrapper(obj)
        for obj in json.loads((tmp_path / f"{run}.json").read_text())
    ]


def soft_deleted_urns(tmp_path: Path, run: str) -> Set[str]:
    return {
        p.entityUrn
        for p in emitted_proposals(tmp_path, run)
        if p.entityUrn and isinstance(p.aspect, StatusClass) and p.aspect.removed
    }


@contextmanager
def mocked_workspace() -> Iterator[requests_mock.Mocker]:
    workspace = Mock()
    workspace.api_client = ApiClient(
        Config(host=HOST, token="fake-token", auth_type="pat")
    )
    with (
        requests_mock.Mocker() as http,
        patch(
            "datahub.ingestion.source.unity.source.create_workspace_client",
            return_value=workspace,
        ),
        patch.object(UnityCatalogSource, "process_catalogs", return_value=()),
        patch.object(UnityCatalogSource, "get_view_lineage", return_value=()),
    ):
        yield http


def mock_space(http: requests_mock.Mocker, summary: Dict[str, str]) -> None:
    http.get(
        f"{SPACES_URL}/{summary['space_id']}",
        json={
            **summary,
            "create_time": "2020-01-01T00:00:00Z",
            "serialized_space": '{"version":2,"data_sources":{"tables":[{"identifier":"catalog.schema.orders"}]}}',
        },
    )


@pytest.mark.parametrize(
    "failure_mode", ["permission", "later_page", "malformed_export"]
)
def test_http_pipeline_preserves_checkpoint_on_partial_scan(
    tmp_path: Path, failure_mode: str
) -> None:
    with mocked_workspace() as http:
        summaries = [{"space_id": name, "title": name} for name in ["a", "b"]]
        http.get(SPACES_URL, json={"spaces": summaries})
        for summary in summaries:
            mock_space(http, summary)
        first = run_pipeline(tmp_path, "initial")
        first.raise_from_status()
        proposals = emitted_proposals(tmp_path, "initial")
        agents = {
            proposal.aspect.name: proposal.entityUrn
            for proposal in proposals
            if isinstance(proposal.aspect, AIAgentInfoClass)
        }
        assert set(agents) == {"a", "b"}
        lineages = [
            proposal.aspect
            for proposal in proposals
            if isinstance(proposal.aspect, UpstreamLineageClass)
        ]
        assert len(lineages) == 2
        assert all(len(lineage.upstreams) == 1 for lineage in lineages)
        exports = [
            request
            for request in http.request_history
            if request.path in ["/api/2.0/genie/spaces/a", "/api/2.0/genie/spaces/b"]
        ]
        assert all(
            request.qs["include_serialized_space"] == ["true"] for request in exports
        )

        if failure_mode == "permission":
            http.get(
                f"{SPACES_URL}/b",
                status_code=403,
                json={"error_code": "PERMISSION_DENIED", "message": "Denied"},
            )
        elif failure_mode == "later_page":
            http.get(
                SPACES_URL, json={"spaces": [summaries[0]], "next_page_token": "next"}
            )
            http.get(
                f"{SPACES_URL}?page_token=next",
                complete_qs=True,
                status_code=403,
                json={"error_code": "PERMISSION_DENIED", "message": "Denied"},
            )
        else:
            http.get(f"{SPACES_URL}/b", json={**summaries[1], "serialized_space": "{"})
        partial = run_pipeline(tmp_path, "partial")
        partial.raise_from_status()
        report = partial.source.get_report()
        assert not report.failures
        assert soft_deleted_urns(tmp_path, "partial") == set()
        expected_warning = {
            "permission": "Genie agent export requires CAN EDIT",
            "later_page": "Failed to list Genie agents",
            "malformed_export": "Invalid Genie agent export",
        }[failure_mode]
        assert expected_warning in [warning.message for warning in report.warnings]

        # b stays checkpointed across the partial run, so dropping it from the
        # next listing soft-deletes it.
        http.get(SPACES_URL, json={"spaces": [summaries[0]]})
        recovered = run_pipeline(tmp_path, "recovered")
        recovered.raise_from_status()
        assert soft_deleted_urns(tmp_path, "recovered") == {agents["b"]}


def test_never_ingested_space_is_not_soft_deleted(tmp_path: Path) -> None:
    with mocked_workspace() as http:
        a, b = ({"space_id": name, "title": name} for name in ["a", "b"])
        mock_space(http, a)
        http.get(SPACES_URL, json={"spaces": [a]})
        run_pipeline(tmp_path, "initial").raise_from_status()

        http.get(SPACES_URL, json={"spaces": [a, b]})
        http.get(
            f"{SPACES_URL}/b",
            status_code=403,
            json={"error_code": "PERMISSION_DENIED", "message": "Denied"},
        )
        warned = run_pipeline(tmp_path, "warned")
        warned.raise_from_status()
        assert "Genie agent export requires CAN EDIT" in [
            warning.message for warning in warned.source.get_report().warnings
        ]

        http.get(SPACES_URL, json={"spaces": [a]})
        run_pipeline(tmp_path, "dropped").raise_from_status()
        assert soft_deleted_urns(tmp_path, "dropped") == set()
