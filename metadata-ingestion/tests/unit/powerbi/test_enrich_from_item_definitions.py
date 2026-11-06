"""PowerBiAPI with ``enrich_from_item_definitions`` (admin_scan mode): the
TMDL definition is overlaid onto the tables the admin scan built."""

import json
from typing import Any, Dict, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper, PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Page,
    PowerBIDataset,
    Report,
    ReportType,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
    ItemDefinition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import (
    PowerBiAPI,
    registry_key,
)
from datahub.metadata.schema_classes import InputFieldsClass

WORKSPACE_ID = "64ED5CAD-7C10-4684-8180-826122881108"
MODEL_ID = "05169CD2-E713-41E6-9600-1D8066D95445"

TABLE_TMDL = """/// Policies from the warehouse.
table Policies

\tcolumn PolicyId
\t\tdataType: int64
\t\tisKey
\t\tsummarizeBy: none
\t\tsourceColumn: POLICY_ID

\t/// Two-letter state code.
\tcolumn State
\t\tdataType: string
\t\tsourceColumn: STATE_CODE

\t/// Premium band.
\tcolumn PremiumBand = IF('Policies'[Premium] > 10, "High", "Low")
\t\tdataType: string
\t\ttype: calculated

\tmeasure 'Total Premium' = SUM('Policies'[Premium])
\t\tformatString: $#,##0

\tpartition Policies = m
\t\tmode: import
\t\tsource = Sql.Database("server", "db")
"""

RENAMED_TABLE_TMDL = """table Policies

\tcolumn PolicyId
\t\tdataType: int64
\t\tsourceColumn: POLICY_ID

\tcolumn StateCode
\t\tdataType: string
\t\tsourceColumn: STATE_CODE

\tmeasure 'Total Premium' = SUM('Policies'[Premium])

\tmeasure 'Average Premium' = AVERAGE('Policies'[Premium])

\tpartition Policies = m
\t\tmode: import
\t\tsource = Sql.Database("server", "db")
"""

PAGE_NAME = "a1b2c3d4e5f6a7b8c9d0"
REPORT_ID = "584cf13a-1485-41c2-a514-b1bb66fff163"
VISUAL_JSON = """{
  "name": "1111aaaa2222bbbb3333",
  "visual": {
    "visualType": "card",
    "query": {
      "queryState": {
        "Values": {
          "projections": [
            {
              "field": {
                "Measure": {
                  "Expression": {"SourceRef": {"Entity": "Policies"}},
                  "Property": "Total Premium"
                }
              },
              "queryRef": "Policies.Total Premium"
            },
            {
              "field": {
                "Column": {
                  "Expression": {"SourceRef": {"Entity": "Policies"}},
                  "Property": "State"
                }
              },
              "queryRef": "Policies.State"
            }
          ]
        }
      }
    }
  }
}"""

DEFINITION_ONLY_TABLE_TMDL = """table Calendar

\tcolumn Date
\t\tdataType: dateTime

\tpartition Calendar = calculated
\t\tsource = CALENDAR(DATE(2020, 1, 1), DATE(2030, 12, 31))
"""

RELATIONSHIPS_TMDL = """relationship r1
\tfromColumn: Claims.PolicyId
\ttoColumn: Policies.PolicyId
"""

ROLE_TMDL = """role Managers
\tmodelPermission: read

\ttablePermission Policies = 'Policies'[State] = "CA"

\tmember 'managers@example.com'
\t\tidentityProvider: AzureAD
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
        "enrich_from_item_definitions": True,
        "extract_lineage": False,
        "extract_column_level_lineage": False,
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.model_validate(base)


def _workspace() -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        name="demo-workspace",
        type="Workspace",
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


def _scan_result() -> Dict[str, Any]:
    """One workspace whose scanned model has two tables; only `Policies`
    is also in the definition."""
    return {
        "workspaces": [
            {
                Constant.ID: WORKSPACE_ID,
                Constant.NAME: "demo-workspace",
                Constant.STATE: Constant.ACTIVE,
                Constant.TYPE: "Workspace",
                "datasets": [
                    {
                        "id": MODEL_ID,
                        "name": "Insurance Model",
                        "tables": [
                            {
                                "name": "Policies",
                                "source": [{"expression": "let Source = 1 in Source"}],
                                "columns": [
                                    {
                                        "name": "PolicyId",
                                        "dataType": "Int64",
                                        "isHidden": False,
                                    },
                                    {
                                        "name": "State",
                                        "dataType": "String",
                                        "isHidden": False,
                                        "description": "from scan",
                                    },
                                    {
                                        "name": "Premium",
                                        "dataType": "Double",
                                        "isHidden": False,
                                    },
                                    {
                                        "name": "PremiumBand",
                                        "dataType": "String",
                                        "isHidden": False,
                                        "columnType": "Calculated",
                                        "expression": "IF(1 > 0, 1, 0)",
                                    },
                                ],
                                "measures": [
                                    {
                                        "name": "Total Premium",
                                        "expression": "SUM('Policies'[Premium])",
                                        "isHidden": False,
                                    }
                                ],
                            },
                            {"name": "Claims", "columns": [], "measures": []},
                        ],
                    }
                ],
                "reports": [],
                "dashboards": [],
            }
        ]
    }


def _definition(parts: Optional[List[DefinitionPart]] = None) -> ItemDefinition:
    return ItemDefinition(
        format="TMDL",
        parts=parts
        if parts is not None
        else [
            DefinitionPart("definition/tables/Policies.tmdl", TABLE_TMDL),
            DefinitionPart(
                "definition/tables/Calendar.tmdl", DEFINITION_ONLY_TABLE_TMDL
            ),
            DefinitionPart("definition/relationships.tmdl", RELATIONSHIPS_TMDL),
            DefinitionPart("definition/roles/Managers.tmdl", ROLE_TMDL),
        ],
    )


def _api(
    *,
    definition: Any = None,
    config: Optional[PowerBiDashboardSourceConfig] = None,
    fabric_resolver: bool = True,
) -> PowerBiAPI:
    """A PowerBiAPI over a mocked admin scan; `definition` is the
    ItemDefinition to return or the exception to raise for the model."""
    api = PowerBiAPI.__new__(PowerBiAPI)
    api._PowerBiAPI__config = config or _config()  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()
    api._PowerBiAPI__reporter = api.reporter  # type: ignore[attr-defined]
    api.dataset_registry = {}
    api.datasets_by_workspace_and_name = {}
    api.ambiguous_remote_model_keys = set()
    api.log_http_error = MagicMock()  # type: ignore[method-assign]
    api._PowerBiAPI__workspace_users = {}  # type: ignore[attr-defined]
    api._get_scan_result = MagicMock(return_value=_scan_result())  # type: ignore[method-assign]

    regular = MagicMock()
    regular.get_dataset_parameters.return_value = {}
    api._PowerBiAPI__regular_api_resolver = regular  # type: ignore[attr-defined]
    api._PowerBiAPI__admin_api_resolver = MagicMock()  # type: ignore[attr-defined]

    fabric: Optional[MagicMock] = None
    if fabric_resolver:
        fabric = MagicMock()
        result = definition if definition is not None else _definition()
        if isinstance(result, Exception):
            fabric.get_item_definition.side_effect = result
        else:
            fabric.get_item_definition.return_value = result
    api._PowerBiAPI__fabric_resolver = fabric  # type: ignore[attr-defined]
    return api


def _fabric(api: PowerBiAPI) -> MagicMock:
    return api._PowerBiAPI__fabric_resolver  # type: ignore[attr-defined]


def _model(workspace: Workspace) -> PowerBIDataset:
    return workspace.datasets[MODEL_ID]


def test_definition_is_overlaid_onto_scan_tables() -> None:
    api = _api(
        config=_config(extract_model_relationships=True, extract_model_properties=True)
    )
    workspace = _workspace()

    assert api.fill_metadata_from_scan_result([workspace]) == set()

    dataset = _model(workspace)
    # The scan decides the table set: Claims stays, Calendar is not added.
    assert [t.name for t in dataset.tables] == ["Policies", "Claims"]
    policies = dataset.tables[0]
    assert policies.description == "Policies from the warehouse."
    assert policies.expression == "let Source = 1 in Source"
    assert policies.storage_mode == "Import"
    assert policies.partition_count == 1
    columns = {c.name: c for c in policies.columns or []}
    assert columns["PolicyId"].source_column == "POLICY_ID"
    assert columns["PolicyId"].model_properties is not None
    assert columns["PolicyId"].model_properties.is_key
    assert columns["PolicyId"].dataType == "Int64"
    assert columns["State"].source_column == "STATE_CODE"
    # A description the scan already carries wins; an empty one is filled.
    assert columns["State"].description == "from scan"
    assert columns["PremiumBand"].description == "Premium band."
    assert columns["Premium"].source_column is None
    assert columns["PremiumBand"].columnType == "Calculated"
    assert [(r.table, r.field) for r in columns["PremiumBand"].references] == [
        ("Policies", "Premium")
    ]
    measure = (policies.measures or [])[0]
    assert measure.model_properties is not None
    assert measure.model_properties.format_string == "$#,##0"
    assert [(r.table, r.field) for r in measure.references] == [("Policies", "Premium")]
    assert [(r.role, r.filter) for r in policies.rls_filters] == [
        ("Managers", "'Policies'[State] = \"CA\"")
    ]
    assert [r.from_table for r in dataset.relationships] == ["Claims"]
    assert api.dataset_registry[registry_key(MODEL_ID)] is dataset

    report = api.reporter
    assert report.fabric_definitions_fetched == 1
    assert report.scan_datasets_enriched == 1
    assert report.scan_tables_not_in_definition == 1
    assert report.definition_tables_not_in_scan == 1
    assert report.relationships_parsed == 1
    assert report.rls_roles_parsed == 1
    # `Premium` is scanned but has no TMDL twin.
    assert report.scan_fields_not_in_definition == 1
    assert report.definition_fields_not_in_scan == 0
    assert sorted(w.title or "" for w in report.warnings) == [
        Constant.DEFINITION_TABLE_NOT_IN_SCAN,
        Constant.SCAN_FIELD_NOT_IN_DEFINITION,
        Constant.SCAN_TABLE_NOT_IN_DEFINITION,
    ]
    assert report.failures == []
    _fabric(api).get_item_definition.assert_called_once_with(
        workspace_id=WORKSPACE_ID,
        item_id=MODEL_ID,
        item_kind="semanticModels",
        definition_format="TMDL",
    )


def test_enrichment_flags_off_leave_relationships_and_rls_alone() -> None:
    api = _api()
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    dataset = _model(workspace)
    assert dataset.relationships == []
    assert dataset.tables[0].rls_filters == []
    # The ungated overlay (sourceColumn, DAX references) still applies.
    assert (dataset.tables[0].columns or [])[0].source_column == "POLICY_ID"
    assert api.reporter.relationships_parsed == 0
    assert api.reporter.rls_roles_parsed == 0


@pytest.mark.parametrize("status", [403, 500], ids=["forbidden", "server_error"])
def test_definition_failure_is_a_warning_and_keeps_scan_tables(status: int) -> None:
    api = _api(definition=_http_error(status))
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])
    api.report_if_no_definitions_fetched()

    dataset = _model(workspace)
    assert [t.name for t in dataset.tables] == ["Policies", "Claims"]
    assert (dataset.tables[0].columns or [])[0].source_column is None
    assert api.reporter.fabric_definitions_failed == 1
    assert api.reporter.scan_datasets_not_enriched == 1
    assert api.reporter.scan_datasets_enriched == 0
    assert api.reporter.failures == []
    assert [w.title for w in api.reporter.warnings] == [
        Constant.ITEM_DEFINITION_NOT_FETCHED,
        "No Item Definitions Fetched",
    ]


def test_unparsed_table_part_is_a_warning_not_a_failure() -> None:
    api = _api(
        definition=_definition(
            [
                DefinitionPart("definition/tables/Policies.tmdl", TABLE_TMDL),
                DefinitionPart("definition/tables/Broken.tmdl", "annotation X = 1\n"),
            ]
        )
    )
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    assert api.reporter.tmdl_parts_failed == 1
    assert api.reporter.failures == []
    assert "TMDL Table Not Parsed" in [w.title for w in api.reporter.warnings]
    assert (_model(workspace).tables[0].columns or [])[0].source_column == "POLICY_ID"


def test_unexpected_overlay_error_is_isolated_per_model() -> None:
    api = _api()
    api._overlay_tables = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    assert MODEL_ID in workspace.datasets
    assert api.reporter.scan_datasets_not_enriched == 1
    assert api.reporter.failures == []
    assert [w.title for w in api.reporter.warnings] == [
        Constant.SEMANTIC_MODEL_NOT_ENRICHED
    ]


def test_flag_off_never_touches_the_fabric_api() -> None:
    api = _api(config=_config(enrich_from_item_definitions=False))
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    _fabric(api).get_item_definition.assert_not_called()
    assert (_model(workspace).tables[0].columns or [])[0].source_column is None
    assert api.reporter.warnings == []


def test_missing_fabric_token_is_a_warning_and_skips_enrichment() -> None:
    api = _api(fabric_resolver=False)
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    assert [t.name for t in _model(workspace).tables] == ["Policies", "Claims"]
    assert api.reporter.scan_datasets_enriched == 0
    assert api.reporter.scan_datasets_not_enriched == 1
    assert api.reporter.failures == []


def _mock_msal_cca(*args: Any, **kwargs: Any) -> Any:
    client = MagicMock()
    client.acquire_token_for_client.return_value = {"access_token": "dummy"}
    return client


def test_constructor_reports_fabric_token_failure_instead_of_raising() -> None:
    with (
        mock.patch("msal.ConfidentialClientApplication", side_effect=_mock_msal_cca),
        mock.patch(
            "datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api."
            "FabricDefinitionResolver.get_access_token",
            side_effect=RuntimeError("no fabric token"),
        ),
    ):
        reporter = PowerBiDashboardSourceReport()
        api = PowerBiAPI(config=_config(), reporter=reporter)

    assert api._PowerBiAPI__fabric_resolver is None  # type: ignore[attr-defined]
    assert [w.title for w in reporter.warnings] == [Constant.FABRIC_TOKEN_NOT_ACQUIRED]
    assert reporter.failures == []
    api.close()


def test_test_connection_flags_missing_fabric_token_as_incapable() -> None:
    config_dict = {
        "tenant_id": "fake",
        "client_id": "foo",
        "client_secret": "bar",
        "enrich_from_item_definitions": True,
    }
    with (
        mock.patch("msal.ConfidentialClientApplication", side_effect=_mock_msal_cca),
        mock.patch(
            "datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api."
            "FabricDefinitionResolver.get_access_token",
            side_effect=RuntimeError("no fabric token"),
        ),
    ):
        report = PowerBiDashboardSource.test_connection(config_dict)
        plain = PowerBiDashboardSource.test_connection(
            {**config_dict, "enrich_from_item_definitions": False}
        )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    fine_lineage = report.capability_report[SourceCapability.LINEAGE_FINE]
    assert not fine_lineage.capable
    assert fine_lineage.failure_reason is not None
    assert "Fabric" in fine_lineage.failure_reason
    assert plain.capability_report is None


# --- field-level mismatches within a matched table ---------------------------


def _renamed_api() -> PowerBiAPI:
    return _api(
        definition=_definition(
            [DefinitionPart("definition/tables/Policies.tmdl", RENAMED_TABLE_TMDL)]
        )
    )


def test_scan_column_renamed_in_definition_is_kept_and_reported() -> None:
    api = _renamed_api()
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    policies = _model(workspace).tables[0]
    columns = {c.name: c for c in policies.columns or []}
    # The scan's column set is untouched: `State` stays, `StateCode` is not
    # added, and only the matched column was enriched.
    assert list(columns) == ["PolicyId", "State", "Premium", "PremiumBand"]
    assert columns["PolicyId"].source_column == "POLICY_ID"
    assert columns["State"].source_column is None
    assert api.reporter.scan_fields_not_in_definition == 3
    warning = next(
        w
        for w in api.reporter.warnings
        if w.title == Constant.SCAN_FIELD_NOT_IN_DEFINITION
    )
    assert any(
        "table=Policies, fields=['Premium', 'PremiumBand', 'State']" in context
        for context in warning.context
    )
    assert api.reporter.failures == []


def test_definition_measure_absent_from_scan_is_reported_and_not_added() -> None:
    api = _renamed_api()
    workspace = _workspace()

    api.fill_metadata_from_scan_result([workspace])

    policies = _model(workspace).tables[0]
    assert [m.name for m in policies.measures or []] == ["Total Premium"]
    # `StateCode` (column) and `Average Premium` (measure) exist only in TMDL.
    assert api.reporter.definition_fields_not_in_scan == 2
    warning = next(
        w
        for w in api.reporter.warnings
        if w.title == Constant.DEFINITION_FIELD_NOT_IN_SCAN
    )
    assert any(
        "table=Policies, fields=['Average Premium', 'StateCode']" in context
        for context in warning.context
    )


# --- report definitions in admin_scan mode -----------------------------------


def _pbir_definition() -> ItemDefinition:
    return ItemDefinition(
        format="PBIR",
        parts=[
            DefinitionPart(
                "definition/pages/pages.json",
                json.dumps({"pageOrder": [PAGE_NAME], "activePageName": PAGE_NAME}),
            ),
            DefinitionPart(
                f"definition/pages/{PAGE_NAME}/page.json",
                json.dumps({"name": PAGE_NAME, "displayName": "Overview"}),
            ),
            DefinitionPart(
                f"definition/pages/{PAGE_NAME}/visuals/1111aaaa2222bbbb3333/visual.json",
                VISUAL_JSON,
            ),
        ],
    )


def _report() -> Report:
    return Report(
        id=REPORT_ID,
        name="Loss Report",
        type=ReportType.PowerBIReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=MODEL_ID,
        dataset=None,
        pages=[Page(id=PAGE_NAME, displayName="Overview", name=PAGE_NAME, order=0)],
        users=[],
        tags=[],
    )


def _field_usage_config() -> PowerBiDashboardSourceConfig:
    return _config(extract_report_field_usage=True, extract_reports=True)


def test_report_definition_fills_field_usage_that_resolves_against_scan_columns() -> (
    None
):
    config = _field_usage_config()
    api = _api(config=config)
    workspace = _workspace()
    api.fill_metadata_from_scan_result([workspace])
    _fabric(api).get_item_definition.return_value = _pbir_definition()
    report = _report()
    report.dataset = api.dataset_registry[registry_key(MODEL_ID)]

    api._fill_report_field_usage(workspace, report)

    page = report.pages[0]
    assert [(r.table, r.field) for r in page.field_references] == [
        ("Policies", "Total Premium"),
        ("Policies", "State"),
    ]
    assert page.visual_count == 1
    assert page.visual_types == ["card"]
    assert api.reporter.report_definitions_parsed == 1
    assert api.reporter.failures == []

    mapper = Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=api.reporter,
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )
    mcp = mapper._page_input_fields_mcp(page, report, "urn:li:chart:(powerbi,page)")
    assert mcp is not None
    assert isinstance(mcp.aspect, InputFieldsClass)
    assert [f.schemaFieldUrn.rsplit(",", 1)[1] for f in mcp.aspect.fields] == [
        "Total Premium)",
        "State)",
    ]
    assert api.reporter.report_field_references_resolved == 2
    assert api.reporter.report_field_references_unresolved == 0


def test_report_field_usage_without_fabric_token_fetches_nothing() -> None:
    api = _api(config=_field_usage_config(), fabric_resolver=False)
    report = _report()

    api._fill_report_field_usage(_workspace(), report)

    assert report.pages[0].field_references == []
    assert api.reporter.fabric_definitions_failed == 0
    assert api.reporter.warnings == []
