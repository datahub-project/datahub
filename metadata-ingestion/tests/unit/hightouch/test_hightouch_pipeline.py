import json
import pathlib
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock
import time_machine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.hightouch.config import HightouchSourceConfig
from datahub.ingestion.source.hightouch.hightouch import HightouchSource
from datahub.ingestion.source.hightouch.hightouch_model import HightouchModelEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2024-01-01 12:00:00"
BASE_URL = "https://api.hightouch.com/api/v1"
API_KEY = "test_api_key"
TEST_DIR = pathlib.Path(__file__).parent
GMS_SERVER = "http://localhost:8080"


def _paged(items: List[Dict[str, Any]], *, with_has_more: bool) -> Any:
    """Serve a list endpoint honouring limit/offset. Only /syncs and
    /syncs/{id}/runs carry hasMore in Hightouch's API; the rest return just data."""

    def callback(request: Any, context: Any) -> Dict[str, Any]:
        offset = int(request.qs.get("offset", ["0"])[0])
        limit = int(request.qs.get("limit", [str(len(items) or 1)])[0])
        body: Dict[str, Any] = {"data": items[offset : offset + limit]}
        if with_has_more:
            body["hasMore"] = offset + limit < len(items)
        return body

    return callback


def load_responses() -> Dict[str, Any]:
    return json.loads((TEST_DIR / "hightouch_api_responses.json").read_text())


def register_hightouch_api(
    mocker: requests_mock.Mocker, responses: Optional[Dict[str, Any]] = None
) -> None:
    responses = responses if responses is not None else load_responses()

    for endpoint in ("sources", "models", "destinations"):
        mocker.get(
            f"{BASE_URL}/{endpoint}",
            json=_paged(responses[endpoint], with_has_more=False),
        )
        for entity in responses[endpoint]:
            mocker.get(f"{BASE_URL}/{endpoint}/{entity['id']}", json=entity)

    mocker.get(
        f"{BASE_URL}/events/contracts",
        json=_paged(responses["contracts"], with_has_more=False),
    )
    mocker.get(
        f"{BASE_URL}/syncs",
        json=_paged(responses["syncs"], with_has_more=True),
    )
    for sync_id, runs in responses["sync_runs"].items():
        mocker.get(
            f"{BASE_URL}/syncs/{sync_id}/runs",
            json=_paged(runs, with_has_more=True),
        )


def _run_pipeline(tmp_path: pathlib.Path, output: str, config: Dict[str, Any]) -> str:
    output_path = str(tmp_path / output)
    pipeline = Pipeline.create(
        {
            "run_id": "hightouch-test",
            "source": {
                "type": "hightouch",
                "config": {
                    "api_config": {"api_key": API_KEY, "base_url": BASE_URL},
                    "env": "PROD",
                    **config,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    report = pipeline.source.get_report()
    assert not report.warnings, list(report.warnings)
    assert not report.failures, list(report.failures)
    return output_path


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hightouch_source_basic(pytestconfig, tmp_path, requests_mock):
    register_hightouch_api(requests_mock)

    output_path = _run_pipeline(
        tmp_path,
        "hightouch_mces.json",
        {
            "emit_models_as_datasets": True,
            "include_sync_runs": True,
            "max_sync_runs_per_sync": 5,
            "parse_model_sql": True,
            "include_contracts": True,
            "include_sibling_relationships": False,
            # Hightouch ids are numbers in the API; the recipe keys them as strings.
            "destination_to_platform_instance": {
                "301": {"platform_instance": "prod-salesforce"}
            },
        },
    )

    # The pipeline may also probe a DataHub server configured in ~/.datahubenv.
    history = [r for r in requests_mock.request_history if r.url.startswith(BASE_URL)]
    assert all(r.headers["Authorization"] == f"Bearer {API_KEY}" for r in history)
    run_requests = [r for r in history if r.path.endswith("/runs")]
    assert len(run_requests) == 3
    assert all(r.qs["limit"] == ["5"] for r in run_requests)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=TEST_DIR / "hightouch_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hightouch_source_with_patterns(pytestconfig, tmp_path, requests_mock):
    register_hightouch_api(requests_mock)

    output_path = _run_pipeline(
        tmp_path,
        "hightouch_filtered_mces.json",
        {
            "emit_models_as_datasets": True,
            "include_sync_runs": False,
            "include_contracts": False,
            "include_sibling_relationships": False,
            "sync_patterns": {
                "allow": ["product-events-.*"],
                "deny": [".*-contacts$"],
            },
            "model_patterns": {"deny": ["Active Customers"]},
        },
    )

    assert not any(
        r.path.endswith("/runs") or r.path.endswith("/events/contracts")
        for r in requests_mock.request_history
        if r.url.startswith(BASE_URL)
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=TEST_DIR / "hightouch_filtered_mces_golden.json",
    )


def _stateful_config(sink: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "run_id": "hightouch-stateful",
        "pipeline_name": "hightouch-stateful",
        "source": {
            "type": "hightouch",
            "config": {
                "api_config": {"api_key": API_KEY, "base_url": BASE_URL},
                "env": "PROD",
                "include_sibling_relationships": False,
                "include_contracts": True,
                "stateful_ingestion": {
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": sink,
    }


def _soft_deleted_urns(output_path: str) -> Set[str]:
    return {
        record["entityUrn"]
        for record in json.loads(pathlib.Path(output_path).read_text())
        if record.get("aspectName") == "status"
        and record["aspect"]["json"].get("removed")
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hightouch_stateful_removes_deleted_syncs_and_models(
    tmp_path, requests_mock, mock_datahub_graph
):
    register_hightouch_api(requests_mock)

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_run1 = run_and_get_pipeline(_stateful_config({"type": "console"}))
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
        assert checkpoint1 and checkpoint1.state

        # Sync 403 and the model only it used are deleted in Hightouch.
        responses = load_responses()
        responses["syncs"] = [s for s in responses["syncs"] if s["id"] != 403]
        responses["models"] = [m for m in responses["models"] if m["id"] != 202]
        register_hightouch_api(requests_mock, responses)

        output_path = str(tmp_path / "hightouch_stateful.json")
        pipeline_run2 = run_and_get_pipeline(
            _stateful_config({"type": "file", "config": {"filename": output_path}})
        )
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
        assert checkpoint2 and checkpoint2.state

        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

    assert _soft_deleted_urns(output_path) == {
        "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),403)",
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,active-customers,PROD)",
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_hightouch_stateful_keeps_contracts_it_could_not_read(
    tmp_path, requests_mock, mock_datahub_graph
):
    register_hightouch_api(requests_mock)

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        run_and_get_pipeline(_stateful_config({"type": "console"}))

        # The contracts endpoint is unreachable on the next run.
        requests_mock.get(
            f"{BASE_URL}/events/contracts", exc=requests.exceptions.ConnectionError
        )
        output_path = str(tmp_path / "hightouch_stateful_failed.json")
        pipeline = Pipeline.create(
            _stateful_config({"type": "file", "config": {"filename": output_path}})
        )
        pipeline.run()

    # The failure keeps stale-entity removal from soft-deleting the contracts.
    assert _soft_deleted_urns(output_path) == set()
    report = pipeline.source.get_report()
    assert [f.title for f in report.failures] == ["Could not fetch Event Contracts"]


def _run_source(
    config: Dict[str, Any], graph: Optional[MagicMock] = None
) -> Tuple[HightouchSource, List[Any]]:
    source = HightouchSource(
        HightouchSourceConfig.model_validate(
            {"api_config": {"api_key": API_KEY, "base_url": BASE_URL}, **config}
        ),
        PipelineContext(run_id="hightouch-unit", graph=graph),
    )
    # Sibling patches arrive as MetadataChangeProposalClass, not the wrapper.
    mcps = [
        wu.metadata
        for wu in source.get_workunits()
        if isinstance(
            wu.metadata, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        )
    ]
    return source, mcps


def _aspects(mcps: List[Any], urn: str, aspect_name: str) -> List[Any]:
    return [
        m.aspect for m in mcps if m.entityUrn == urn and m.aspectName == aspect_name
    ]


PRODUCT_EVENTS_MODEL = (
    "urn:li:dataset:(urn:li:dataPlatform:hightouch,product-events,PROD)"
)
PRODUCT_EVENTS_TABLE = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.product_events,PROD)"


def _graph_with_product_events_table() -> MagicMock:
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = lambda urn: (
        SchemaMetadataClass(
            schemaName="product_events",
            platform="urn:li:dataPlatform:snowflake",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="VARCHAR",
                )
                for name in ("EVENT_ID", "USER_EMAIL", "EVENT_NAME", "EVENT_TIMESTAMP")
            ],
        )
        if urn == PRODUCT_EVENTS_TABLE
        else None
    )
    graph.exists.side_effect = lambda urn: urn == PRODUCT_EVENTS_TABLE
    return graph


def test_table_model_takes_schema_lineage_and_sibling_from_datahub(requests_mock):
    register_hightouch_api(requests_mock)
    _, mcps = _run_source(
        {"include_sync_runs": False}, _graph_with_product_events_table()
    )

    [schema] = _aspects(mcps, PRODUCT_EVENTS_MODEL, "schemaMetadata")
    assert [(f.fieldPath, f.isPartOfKey) for f in schema.fields][:2] == [
        ("EVENT_ID", True),
        ("USER_EMAIL", False),
    ]

    [lineage] = _aspects(mcps, PRODUCT_EVENTS_MODEL, "upstreamLineage")
    assert [u.dataset for u in lineage.upstreams] == [PRODUCT_EVENTS_TABLE]
    assert len(lineage.fineGrainedLineages) == 4

    # The warehouse table exists in DataHub, so it gets the sibling back-link.
    back_links = [
        m
        for m in mcps
        if m.entityUrn == PRODUCT_EVENTS_TABLE and m.aspectName == "siblings"
    ]
    assert back_links

    # Sync field mappings adopt the model schema's casing.
    job = "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),401)"
    [io] = _aspects(mcps, job, "dataJobInputOutput")
    assert io.fineGrainedLineages[0].upstreams == [
        f"urn:li:schemaField:({PRODUCT_EVENTS_MODEL},USER_EMAIL)"
    ]


def test_no_sibling_back_link_for_a_table_not_in_datahub(requests_mock):
    register_hightouch_api(requests_mock)
    graph = MagicMock()
    graph.get_schema_metadata.return_value = None
    graph.exists.return_value = False
    _, mcps = _run_source({"include_sync_runs": False}, graph)
    assert not [m for m in mcps if m.entityUrn == PRODUCT_EVENTS_TABLE]
    assert [
        m
        for m in mcps
        if m.entityUrn == PRODUCT_EVENTS_MODEL and m.aspectName == "siblings"
    ]


def test_model_shared_by_syncs_is_emitted_once_with_their_columns(requests_mock):
    register_hightouch_api(requests_mock)
    _, mcps = _run_source({"include_sync_runs": False})
    [schema] = _aspects(mcps, PRODUCT_EVENTS_MODEL, "schemaMetadata")
    assert [f.fieldPath for f in schema.fields] == [
        "event_id",
        "user_email",
        "event_name",
        "event_timestamp",
    ]


def test_syncs_read_warehouse_tables_when_models_are_not_emitted(requests_mock):
    register_hightouch_api(requests_mock)
    _, mcps = _run_source(
        {"emit_models_as_datasets": False, "include_sync_runs": False}
    )
    assert not [m for m in mcps if m.entityUrn.startswith(PRODUCT_EVENTS_MODEL[:45])]

    table_sync = "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),401)"
    [io] = _aspects(mcps, table_sync, "dataJobInputOutput")
    assert io.inputDatasets == [PRODUCT_EVENTS_TABLE]
    assert io.fineGrainedLineages

    sql_sync = "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),403)"
    [io] = _aspects(mcps, sql_sync, "dataJobInputOutput")
    assert io.inputDatasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.customers,PROD)"
    ]
    # The SQL model's output columns are not the queried table's columns.
    assert not io.fineGrainedLineages


@pytest.mark.parametrize(
    "status, expected_result",
    [
        ("success", "SUCCESS"),
        ("warning", "SUCCESS"),
        ("failed", "FAILURE"),
        ("cancelled", "SKIPPED"),
        ("interrupted", "SKIPPED"),
        ("queued", None),
        ("querying", None),
        ("processing", None),
        ("reporting", None),
    ],
)
def test_sync_run_status(requests_mock, status, expected_result):
    responses = load_responses()
    [run] = responses["sync_runs"]["402"]
    responses["sync_runs"] = {"401": [], "402": [{**run, "status": status}], "403": []}
    register_hightouch_api(requests_mock, responses)

    source, mcps = _run_source(
        {"sync_patterns": {"allow": ["^product-events-to-contacts$"]}}
    )
    results = [
        m.aspect.result.type
        for m in mcps
        if m.aspectName == "dataProcessInstanceRunEvent" and m.aspect.result
    ]
    assert results == ([expected_result] if expected_result else [])
    assert not source.report.warnings


def test_unknown_sync_run_status_is_not_a_success(requests_mock):
    responses = load_responses()
    [run] = responses["sync_runs"]["402"]
    responses["sync_runs"]["402"] = [{**run, "status": "brand_new"}]
    register_hightouch_api(requests_mock, responses)

    source, mcps = _run_source(
        {"sync_patterns": {"allow": ["^product-events-to-contacts$"]}}
    )
    results = [
        m.aspect.result.type
        for m in mcps
        if m.aspectName == "dataProcessInstanceRunEvent" and m.aspect.result
    ]
    assert results == ["SKIPPED"]
    assert source.report.unknown_sync_run_statuses == {"brand_new": 1}


def test_sync_run_fetch_error_keeps_the_sync(requests_mock):
    register_hightouch_api(requests_mock)
    requests_mock.get(f"{BASE_URL}/syncs/401/runs", status_code=400)
    source, mcps = _run_source({})
    assert _aspects(
        mcps, "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),401)", "dataJobInfo"
    )
    assert [w.title for w in source.report.warnings] == ["Failed to fetch sync runs"]
    assert not source.report.failures


def test_test_connection_reports_rejected_key(requests_mock):
    requests_mock.get(f"{BASE_URL}/sources", status_code=401)
    report = HightouchSource.test_connection(
        {"api_config": {"api_key": API_KEY, "base_url": BASE_URL}}
    )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "rejected the API key" in (report.basic_connectivity.failure_reason or "")


def test_dbt_model_gets_lineage_but_no_sibling(requests_mock):
    responses = load_responses()
    model = responses["models"][0]
    model.pop("table")
    model.update(
        queryType="dbt",
        dbt={"database": "ANALYTICS", "schema": "PUBLIC", "name": "PRODUCT_EVENTS"},
    )
    register_hightouch_api(requests_mock, responses)

    _, mcps = _run_source(
        {"include_sync_runs": False}, _graph_with_product_events_table()
    )

    [lineage] = _aspects(mcps, PRODUCT_EVENTS_MODEL, "upstreamLineage")
    assert [u.dataset for u in lineage.upstreams] == [PRODUCT_EVENTS_TABLE]
    # The dbt connector already makes its dbt node the table's primary sibling.
    assert not [m for m in mcps if m.aspectName == "siblings"]


def test_sync_reads_warehouse_tables_when_its_model_fails(requests_mock):
    register_hightouch_api(requests_mock)
    real_emit = HightouchModelEmitter.emit

    def emit(self, model, *args, **kwargs):
        if model.id == "201":
            raise ValueError("model emission failed")
        yield from real_emit(self, model, *args, **kwargs)

    with patch.object(HightouchModelEmitter, "emit", emit):
        source, mcps = _run_source({"include_sync_runs": False})

    assert [f.title for f in source.report.failures] == ["Failed to process model"]
    job = "urn:li:dataJob:(urn:li:dataFlow:(hightouch,1,PROD),401)"
    [io] = _aspects(mcps, job, "dataJobInputOutput")
    assert io.inputDatasets == [PRODUCT_EVENTS_TABLE]
