"""PowerBiAPI.fill_metadata_from_definitions (metadata_api_mode: workspace_apis)."""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PowerBIDataset,
    Report,
    ReportType,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
    FabricItem,
    FabricLroTimeoutError,
    ItemDefinition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"
MODEL_A = "05169CD2-E713-41E6-9600-1D8066D95445"
MODEL_B = "7A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D"
LAKEHOUSE_ID = "3C4D5E6F-7A8B-4C9D-8E0F-1A2B3C4D5E6F"
SQL_ENDPOINT_ID = "6F2E7D8C-9B0A-4C1D-8E2F-3A4B5C6D7E8F"

TABLE_TMDL = """table Policies

\tcolumn PolicyId
\t\tdataType: int64
\t\tsourceColumn: POLICY_ID

\tpartition Policies = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Snowflake.Databases("my-account.snowflakecomputing.com","WH")
\t\t\t\tin
\t\t\t\t    Source
"""


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    base: Dict[str, Any] = {
        "tenant_id": "fake",
        "client_id": "foo",
        "client_secret": "bar",
        "metadata_api_mode": "workspace_apis",
        "extract_lineage": False,
        "extract_column_level_lineage": False,
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.model_validate(base)


def _workspace(workspace_type: str = "Workspace") -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        name="demo-workspace",
        type=workspace_type,
        webUrl=None,
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _definition(text: str = TABLE_TMDL) -> ItemDefinition:
    return ItemDefinition(
        format="TMDL",
        parts=[DefinitionPart("definition/tables/Policies.tmdl", text)],
    )


def _api(
    *,
    datasets: Optional[List[Dict[str, Any]]] = None,
    items: Optional[List[FabricItem]] = None,
    definitions: Optional[Dict[str, Any]] = None,
    config: Optional[PowerBiDashboardSourceConfig] = None,
) -> PowerBiAPI:
    """A PowerBiAPI with mocked resolvers; `definitions` maps item id to an
    ItemDefinition or an exception to raise for that item."""
    api = PowerBiAPI.__new__(PowerBiAPI)
    api._PowerBiAPI__config = config or _config()  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()
    api.dataset_registry = {}
    api.log_http_error = MagicMock()  # type: ignore[method-assign]
    api._PowerBiAPI__workspace_users = {}  # type: ignore[attr-defined]

    regular = MagicMock()
    regular.get_datasets.return_value = (
        datasets
        if datasets is not None
        else [{"id": MODEL_A, "name": "Insurance Model"}]
    )
    regular.get_dataset_parameters.return_value = {}
    api._PowerBiAPI__regular_api_resolver = regular  # type: ignore[attr-defined]

    fabric = MagicMock()
    fabric.list_items.return_value = items if items is not None else []
    fabric.get_lakehouse_sql_endpoint_id.return_value = SQL_ENDPOINT_ID
    definitions = definitions if definitions is not None else {MODEL_A: _definition()}

    def get_item_definition(
        workspace_id: str, item_id: str, item_kind: str, definition_format: Any = None
    ) -> ItemDefinition:
        result = definitions[item_id]
        if isinstance(result, Exception):
            raise result
        return result

    fabric.get_item_definition.side_effect = get_item_definition
    api._PowerBiAPI__fabric_resolver = fabric  # type: ignore[attr-defined]
    return api


def test_workspace_type_filter_excludes_workspace_without_api_calls() -> None:
    api = _api()
    workspace = _workspace(workspace_type="PersonalGroup")

    excluded = api.fill_metadata_from_definitions([workspace])

    assert excluded == {WORKSPACE_ID}
    assert workspace.datasets == {}
    api._PowerBiAPI__fabric_resolver.list_items.assert_not_called()  # type: ignore[attr-defined]


def test_datasets_and_tables_come_from_definitions() -> None:
    api = _api()
    workspace = _workspace()

    assert api.fill_metadata_from_definitions([workspace]) == set()

    dataset = workspace.datasets[MODEL_A]
    assert [t.name for t in dataset.tables] == ["Policies"]
    assert dataset.tables[0].expression is not None
    assert dataset.tables[0].expression.startswith("let")
    assert dataset.tables[0].columns is not None
    assert dataset.tables[0].columns[0].dataType == "Int64"
    assert api.dataset_registry[MODEL_A] is dataset
    assert api.reporter.fabric_definitions_fetched == 1
    assert api.reporter.tmdl_tables_parsed == 1


def test_one_forbidden_model_is_a_failure_but_does_not_abort_the_workspace() -> None:
    api = _api(
        datasets=[
            {"id": MODEL_A, "name": "Insurance Model"},
            {"id": MODEL_B, "name": "Protected Model"},
        ],
        definitions={MODEL_A: _definition(), MODEL_B: _http_error(403)},
    )
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert set(workspace.datasets) == {MODEL_A, MODEL_B}
    assert workspace.datasets[MODEL_B].tables == []
    assert api.reporter.fabric_definitions_failed == 1
    assert api.reporter.fabric_definitions_fetched == 1
    assert [f.title for f in api.reporter.failures] == ["Fabric API Permission Denied"]
    # Not every definition failed, so the aggregate failure is not raised.
    assert not any(
        f.title == "No Item Definitions Fetched" for f in api.reporter.failures
    )


def test_lro_timeout_on_a_model_is_a_failure_and_all_failures_escalate() -> None:
    # A model without its definition has no tables this run; under stale-entity
    # removal a green run would soft-delete them, so even a timeout is a failure.
    api = _api(definitions={MODEL_A: FabricLroTimeoutError("too slow")})
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])
    api.report_if_no_definitions_fetched()

    assert api.reporter.fabric_lro_timeouts == 1
    titles = [f.title for f in api.reporter.failures]
    assert "Semantic Model Definition Not Fetched" in titles
    assert "No Item Definitions Fetched" in titles


def test_all_definitions_failed_is_judged_across_batches() -> None:
    # With scan_batch_size=1 the first batch can fail entirely while later
    # ones succeed; the escalation must look at the whole run.
    api = _api(
        datasets=[
            {"id": MODEL_A, "name": "Insurance Model"},
            {"id": MODEL_B, "name": "Other Model"},
        ],
        definitions={MODEL_A: _http_error(500), MODEL_B: _definition()},
    )

    api.fill_metadata_from_definitions([_workspace()])
    api.report_if_no_definitions_fetched()

    assert api.reporter.fabric_definitions_failed == 1
    assert api.reporter.fabric_definitions_fetched == 1
    assert not any(
        f.title == "No Item Definitions Fetched" for f in api.reporter.failures
    )


def test_unparsed_table_part_is_a_failure_but_other_parts_only_warn() -> None:
    # A table part that did not parse is a table missing from the run (a
    # soft-delete under stale-entity removal); a broken relationships part
    # only loses enrichment.
    good = _definition()
    good.failed_parts = ["definition/relationships.tmdl"]
    bad = _definition()
    bad.parts.append(
        DefinitionPart("definition/tables/Broken.tmdl", "annotation Only = 1\n")
    )
    api = _api(
        datasets=[
            {"id": MODEL_A, "name": "Insurance Model"},
            {"id": MODEL_B, "name": "Partly Broken Model"},
        ],
        definitions={MODEL_A: good, MODEL_B: bad},
    )
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert api.reporter.tmdl_parts_failed == 2
    assert [w.title for w in api.reporter.warnings] == ["TMDL Part Not Parsed"]
    assert [f.title for f in api.reporter.failures] == ["TMDL Table Not Parsed"]
    # The model keeps the tables that did parse.
    assert [t.name for t in workspace.datasets[MODEL_B].tables] == ["Policies"]


def test_fabric_item_listing_failure_keeps_datasets() -> None:
    api = _api()
    api._PowerBiAPI__fabric_resolver.list_items.side_effect = _http_error(500)  # type: ignore[attr-defined]
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert workspace.fabric_artifacts == {}
    assert MODEL_A in workspace.datasets
    assert any(w.title == "Fabric Items Not Listed" for w in api.reporter.warnings)


def test_lakehouse_endpoint_registered_and_failure_degrades_gracefully() -> None:
    items = [
        FabricItem(LAKEHOUSE_ID, "Claims_Lakehouse", "Lakehouse", WORKSPACE_ID),
        FabricItem(SQL_ENDPOINT_ID, "Claims_Lakehouse", "SQLEndpoint", WORKSPACE_ID),
    ]
    api = _api(items=items)
    workspace = _workspace()
    api.fill_metadata_from_definitions([workspace])

    endpoint = workspace.fabric_artifacts[SQL_ENDPOINT_ID]
    assert endpoint.artifact_type == "SQLAnalyticsEndpoint"
    assert endpoint.physical_item_ids == [LAKEHOUSE_ID]
    assert workspace.fabric_artifacts[LAKEHOUSE_ID].artifact_type == "Lakehouse"

    api = _api(items=items)
    api._PowerBiAPI__fabric_resolver.get_lakehouse_sql_endpoint_id.side_effect = (  # type: ignore[attr-defined]
        _http_error(500)
    )
    workspace = _workspace()
    api.fill_metadata_from_definitions([workspace])

    assert workspace.fabric_artifacts[SQL_ENDPOINT_ID].physical_item_ids is None
    assert any(
        w.title == "Lakehouse SQL Endpoint Not Resolved" for w in api.reporter.warnings
    )


@pytest.mark.parametrize(
    "status,title",
    [
        pytest.param(403, "Workspace Datasets Permission Denied", id="forbidden"),
        pytest.param(500, "Datasets Not Listed", id="server_error"),
    ],
)
def test_dataset_listing_error_is_a_failure(status: int, title: str) -> None:
    # Either way the workspace is emitted without tables, which stale-entity
    # removal would otherwise treat as deletions.
    api = _api()
    api._PowerBiAPI__regular_api_resolver.get_datasets.side_effect = _http_error(  # type: ignore[attr-defined]
        status
    )
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert workspace.datasets == {}
    assert [f.title for f in api.reporter.failures] == [title]


def test_dataset_parameters_failure_is_reported_and_model_still_built() -> None:
    api = _api()
    api._PowerBiAPI__regular_api_resolver.get_dataset_parameters.side_effect = (  # type: ignore[attr-defined]
        _http_error(500)
    )
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert MODEL_A in workspace.datasets
    assert api.reporter.dataset_parameters_failed == 1
    assert any(
        w.title == "Dataset Parameters Not Fetched" for w in api.reporter.warnings
    )


def test_unexpected_error_in_one_model_is_isolated() -> None:
    # An error outside the definition fetch (here: profiling) must not take
    # the rest of the workspace down, but the skipped model is a failure.
    api = _api(
        datasets=[
            {"id": MODEL_A, "name": "Insurance Model"},
            {"id": MODEL_B, "name": "Odd Model"},
        ],
        definitions={MODEL_A: _definition(), MODEL_B: _definition()},
        config=_config(profiling={"enabled": True}),
    )
    regular = api._PowerBiAPI__regular_api_resolver  # type: ignore[attr-defined]

    def profile_dataset(dataset: Any, *args: Any, **kwargs: Any) -> None:
        if dataset.id == MODEL_B:
            raise RuntimeError("boom")

    regular.profile_dataset.side_effect = profile_dataset
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert MODEL_A in workspace.datasets
    assert MODEL_B not in workspace.datasets
    assert api.reporter.semantic_models_not_processed == 1
    assert [f.title for f in api.reporter.failures] == ["Semantic Model Not Processed"]


# --- paginated report RDL binding ------------------------------------------


def _paginated_report() -> Report:
    return Report(
        id="rdl-1",
        name="Paginated",
        type=ReportType.PaginatedReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=None,
        dataset=None,
        pages=[],
        users=[],
        tags=[],
    )


def _rdl_definition(dataset_id: str) -> ItemDefinition:
    rdl = (
        "<Report><ConnectString>Data Source=pbiazure://api.powerbi.com;"
        f"Initial Catalog=sobe_wowvirtualserver-{dataset_id};</ConnectString></Report>"
    )
    return ItemDefinition(format=None, parts=[DefinitionPart("report.rdl", rdl)])


def _registered_dataset() -> PowerBIDataset:
    return PowerBIDataset(
        id=MODEL_A,
        name="Insurance Model",
        description="",
        webUrl=None,
        workspace_id=WORKSPACE_ID,
        workspace_name="demo-workspace",
        parameters={},
        tables=[],
        tags=[],
    )


def test_paginated_report_bound_through_rdl_definition() -> None:
    api = _api(definitions={"rdl-1": _rdl_definition(MODEL_A)})
    api.dataset_registry[MODEL_A] = _registered_dataset()
    report = _paginated_report()

    api._resolve_paginated_report_lineage(report, _workspace())

    assert report.dataset_id == MODEL_A
    assert report.dataset is not None
    api._PowerBiAPI__regular_api_resolver.get_report_datasources.assert_not_called()  # type: ignore[attr-defined]


def test_paginated_report_rdl_binding_to_unknown_dataset_is_reported() -> None:
    api = _api(definitions={"rdl-1": _rdl_definition(MODEL_B)})
    report = _paginated_report()

    api._resolve_paginated_report_lineage(report, _workspace())

    assert report.dataset_id == MODEL_B
    assert report.dataset is None
    assert any(
        i.title == "Missing Lineage For Paginated Report" for i in api.reporter.infos
    )


@pytest.mark.parametrize(
    "status,title",
    [
        pytest.param(500, "Report Definition Not Fetched", id="server_error"),
        pytest.param(403, "Report Definition Permission Denied", id="forbidden"),
    ],
)
def test_paginated_report_definition_failure_is_a_warning_with_fallback(
    status: int, title: str
) -> None:
    # A report definition only enriches existing entities, so even a 403
    # (an encrypted sensitivity label, say) must not fail the run.
    api = _api(definitions={"rdl-1": _http_error(status)})
    api._get_resolver = MagicMock()  # type: ignore[method-assign]
    api._get_resolver.return_value.get_report_datasources.return_value = []
    report = _paginated_report()

    api._resolve_paginated_report_lineage(report, _workspace())

    assert api.reporter.fabric_definitions_failed == 1
    assert not api.reporter.failures
    assert [w.title for w in api.reporter.warnings] == [title]
    api._get_resolver.return_value.get_report_datasources.assert_called_once()


# --- workspace users as ownership -------------------------------------------


def test_workspace_users_are_cached_and_failure_yields_no_owners() -> None:
    api = _api(config=_config(extract_ownership=True))
    regular = api._PowerBiAPI__regular_api_resolver  # type: ignore[attr-defined]
    regular.get_workspace_users.return_value = [MagicMock()]

    assert len(api.get_report_users(WORKSPACE_ID, "r1")) == 1
    assert len(api.get_report_users(WORKSPACE_ID, "r2")) == 1
    regular.get_workspace_users.assert_called_once_with(WORKSPACE_ID)

    api = _api(config=_config(extract_ownership=True))
    api._PowerBiAPI__regular_api_resolver.get_workspace_users.side_effect = (  # type: ignore[attr-defined]
        _http_error(403)
    )
    assert api.get_report_users(WORKSPACE_ID, "r1") == []
    assert any(w.title == "Workspace Users Not Fetched" for w in api.reporter.warnings)
