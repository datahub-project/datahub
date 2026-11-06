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
    FieldModelProperties,
    Page,
    PowerBIDataset,
    RefreshPolicy,
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
SECOND_WORKSPACE_ID = "2B7C4D1E-9F3A-4E6B-8C2D-5A1F0E9B7C3D"
MODEL_A = "05169CD2-E713-41E6-9600-1D8066D95445"
MODEL_B = "7A1B2C3D-4E5F-4A6B-8C7D-9E0F1A2B3C4D"
LAKEHOUSE_ID = "3C4D5E6F-7A8B-4C9D-8E0F-1A2B3C4D5E6F"
SQL_ENDPOINT_ID = "6F2E7D8C-9B0A-4C1D-8E2F-3A4B5C6D7E8F"

TABLE_TMDL = """table Policies

\tcolumn PolicyId
\t\tdataType: int64
\t\tsourceColumn: POLICY_ID

\tcolumn PremiumBand = IF(Policies[PolicyId] > 10, "High", "Low")
\t\tdataType: string
\t\ttype: calculated

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
    columns = dataset.tables[0].columns
    assert columns is not None
    assert columns[0].dataType == "Int64"
    assert columns[0].source_column == "POLICY_ID"
    assert columns[0].columnType == "Data"
    assert columns[0].references == []
    assert columns[1].columnType == "Calculated"
    assert columns[1].source_column is None
    assert [(r.table, r.field) for r in columns[1].references] == [
        ("Policies", "PolicyId")
    ]
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
    # ones succeed; the escalation must look at the whole run, so each batch
    # is filled separately against the same reporter and the check runs once.
    api = _api(definitions={MODEL_A: _http_error(500), MODEL_B: _definition()})
    first = _workspace()
    second = _workspace()
    second.id = SECOND_WORKSPACE_ID
    datasets_by_workspace = {
        first.id: [{"id": MODEL_A, "name": "Insurance Model"}],
        second.id: [{"id": MODEL_B, "name": "Other Model"}],
    }
    regular = api._PowerBiAPI__regular_api_resolver  # type: ignore[attr-defined]
    regular.get_datasets.side_effect = lambda workspace: datasets_by_workspace[
        workspace.id
    ]

    api.fill_metadata_from_definitions([first])
    # The first batch alone has only failures; filling it must not escalate.
    assert api.reporter.fabric_definitions_fetched == 0
    assert api.reporter.fabric_definitions_failed == 1
    api.fill_metadata_from_definitions([second])
    api.report_if_no_definitions_fetched()

    assert first.datasets[MODEL_A].tables == []
    assert [t.name for t in second.datasets[MODEL_B].tables] == ["Policies"]
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


PROPERTIES_TABLE_TMDL = """table Policies

\tcolumn PolicyId
\t\tdataType: int64
\t\tisKey
\t\tsourceColumn: POLICY_ID

\tcolumn State
\t\tdataType: string
\t\tsourceColumn: STATE

\tmeasure Premium = SUM('Policies'[Amount])
\t\tformatString: $#,##0

\trefreshPolicy
\t\tpolicyType: basic
\t\trollingWindowGranularity: year
\t\trollingWindowPeriods: 2

\tpartition Policies = m
\t\tmode: directQuery
\t\tsource = Sql.Database("server", "db")
"""

ROLE_TMDL = """role 'Regional Managers'
\tmodelPermission: read

\ttablePermission Policies = 'Policies'[State] = "CA"

\ttablePermission Missing = TRUE()

\tmember 'regional-managers@example.com'
\t\tidentityProvider: AzureAD
"""


def _properties_definition() -> ItemDefinition:
    return ItemDefinition(
        format="TMDL",
        parts=[
            DefinitionPart("definition/tables/Policies.tmdl", PROPERTIES_TABLE_TMDL),
            DefinitionPart("definition/roles/Regional Managers.tmdl", ROLE_TMDL),
            DefinitionPart("definition/roles/Broken.tmdl", "annotation Only = 1\n"),
        ],
    )


def test_model_properties_attach_rls_filters_and_count() -> None:
    api = _api(
        definitions={MODEL_A: _properties_definition()},
        config=_config(extract_model_properties=True),
    )
    api._PowerBiAPI__regular_api_resolver.get_dataset_parameters.return_value = {  # type: ignore[attr-defined]
        "Environment": "prod",
        "Api Token": "abc",
    }
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    table = workspace.datasets[MODEL_A].tables[0]
    assert table.storage_mode == "DirectQuery"
    assert table.partition_count == 1
    assert table.refresh_policy == RefreshPolicy(
        rolling_window_granularity="year", rolling_window_periods="2"
    )
    # Only the filter on a table of the model is attached; members never are.
    assert [(f.role, f.filter) for f in table.rls_filters] == [
        ("Regional Managers", "'Policies'[State] = \"CA\"")
    ]
    columns = {c.name: c.model_properties for c in table.columns or []}
    assert columns["PolicyId"] == FieldModelProperties(is_key=True)
    assert columns["State"] == FieldModelProperties()
    measures = {m.name: m.model_properties for m in table.measures or []}
    assert measures["Premium"] == FieldModelProperties(format_string="$#,##0")

    assert api.reporter.rls_roles_parsed == 1
    assert api.reporter.tmdl_roles_failed == 1
    # The `Missing` table filter is reported, not silently dropped.
    assert api.reporter.rls_filters_unresolved == 1
    assert api.reporter.dataset_parameters_emitted == 2
    # PolicyId (isKey) and Premium (formatString); State has no properties.
    assert api.reporter.model_fields_with_properties == 2
    assert sorted(w.title or "" for w in api.reporter.warnings) == [
        "RLS Filter Table Not Found",
        "TMDL Role Not Parsed",
    ]
    # A broken role part is enrichment loss, never a failed table part.
    assert api.reporter.tmdl_parts_failed == 0
    assert api.reporter.failures == []


def test_model_properties_off_leaves_roles_and_counters_untouched() -> None:
    api = _api(definitions={MODEL_A: _properties_definition()})
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    table = workspace.datasets[MODEL_A].tables[0]
    assert table.rls_filters == []
    assert api.reporter.rls_roles_parsed == 0
    assert api.reporter.tmdl_roles_failed == 0
    assert api.reporter.rls_filters_unresolved == 0
    assert api.reporter.model_fields_with_properties == 0
    assert api.reporter.warnings == []


def test_direct_lake_mode_on_a_non_entity_partition_is_not_direct_lake() -> None:
    # Only an `entity` partition is DirectLake; the storage mode of any other
    # partition kind is recorded as written so lineage routing is unchanged.
    text = PROPERTIES_TABLE_TMDL.replace("mode: directQuery", "mode: directLake")
    api = _api(definitions={MODEL_A: _definition(text)})
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    table = workspace.datasets[MODEL_A].tables[0]
    assert table.storage_mode is None
    assert table.expression is not None

    unknown = PROPERTIES_TABLE_TMDL.replace("mode: directQuery", "mode: future")
    api = _api(definitions={MODEL_A: _definition(unknown)})
    workspace = _workspace()
    api.fill_metadata_from_definitions([workspace])
    assert workspace.datasets[MODEL_A].tables[0].storage_mode == "future"


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


def test_paginated_report_without_a_dataset_binding_falls_back_to_datasources() -> None:
    external = ItemDefinition(
        format=None,
        parts=[
            DefinitionPart(
                "report.rdl",
                "<Report><ConnectString>Data Source=sqlhost;Initial Catalog=sales;"
                "</ConnectString></Report>",
            )
        ],
    )
    api = _api(definitions={"rdl-1": external})
    api._get_resolver = MagicMock()  # type: ignore[method-assign]
    api._get_resolver.return_value.get_report_datasources.return_value = []
    report = _paginated_report()

    api._resolve_paginated_report_lineage(report, _workspace())

    assert report.dataset_id is None
    api._get_resolver.return_value.get_report_datasources.assert_called_once()


def test_report_definitions_do_not_decide_the_all_models_failed_check() -> None:
    # A fetched report definition must not hide that every model failed.
    api = _api(
        definitions={MODEL_A: _http_error(500), "rdl-1": _rdl_definition(MODEL_A)}
    )
    api.fill_metadata_from_definitions([_workspace()])
    api._resolve_paginated_report_lineage(_paginated_report(), _workspace())
    api.report_if_no_definitions_fetched()

    assert api.reporter.fabric_definitions_fetched == 1
    assert any(f.title == "No Item Definitions Fetched" for f in api.reporter.failures)


def test_failed_report_definition_alone_does_not_escalate() -> None:
    api = _api(definitions={"rdl-1": _http_error(500)})
    api._get_resolver = MagicMock()  # type: ignore[method-assign]
    api._get_resolver.return_value.get_report_datasources.return_value = []

    api._resolve_paginated_report_lineage(_paginated_report(), _workspace())
    api.report_if_no_definitions_fetched()

    assert api.reporter.fabric_definitions_failed == 1
    assert not api.reporter.failures


def test_incomplete_table_part_is_a_warning_and_keeps_the_table() -> None:
    broken = _definition(TABLE_TMDL + "\tcolumn Broken Name\n\t\tdataType: string\n")
    api = _api(definitions={MODEL_A: broken})
    workspace = _workspace()

    api.fill_metadata_from_definitions([workspace])

    assert [t.name for t in workspace.datasets[MODEL_A].tables] == ["Policies"]
    assert api.reporter.tmdl_parts_incomplete == 1
    assert not api.reporter.failures
    assert [w.title for w in api.reporter.warnings] == ["TMDL Object Not Parsed"]


# --- report field usage -----------------------------------------------------


def test_report_definition_the_parser_cannot_handle_is_a_warning() -> None:
    # Valid JSON of an unexpected nested shape (a section that is not an
    # object) must not escape and take the workspace's reports down with it.
    report = Report(
        id="report-1",
        name="Claims Overview",
        type=ReportType.PowerBIReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=MODEL_A,
        dataset=None,
        pages=[Page(id="p1", displayName="Overview", name="p1", order=0)],
        users=[],
        tags=[],
    )
    malformed = ItemDefinition(
        format=None,
        parts=[DefinitionPart("report.json", '{"sections": [1]}')],
    )
    api = _api(definitions={report.id: malformed})

    api._fill_report_field_usage(_workspace(), report)

    assert api.reporter.report_definitions_failed == 1
    assert api.reporter.report_definitions_parsed == 0
    assert not api.reporter.failures
    assert [w.title for w in api.reporter.warnings] == ["Report Definition Not Parsed"]
    assert report.pages[0].field_references == []


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
