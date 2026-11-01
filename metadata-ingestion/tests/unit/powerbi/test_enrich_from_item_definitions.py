"""PowerBiAPI with ``enrich_from_item_definitions`` (admin_scan mode): the
TMDL definition is overlaid onto the tables the admin scan built."""

from typing import Any, Dict, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PowerBIDataset,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
    ItemDefinition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

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

\tcolumn PremiumBand = IF('Policies'[Premium] > 10, "High", "Low")
\t\tdataType: string
\t\ttype: calculated

\tmeasure 'Total Premium' = SUM('Policies'[Premium])
\t\tformatString: $#,##0

\tpartition Policies = m
\t\tmode: import
\t\tsource = Sql.Database("server", "db")
"""

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
    assert columns["State"].description == "Two-letter state code."
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
    assert api.dataset_registry[MODEL_ID] is dataset

    report = api.reporter
    assert report.fabric_definitions_fetched == 1
    assert report.scan_datasets_enriched == 1
    assert report.scan_tables_not_in_definition == 1
    assert report.definition_tables_not_in_scan == 1
    assert report.relationships_parsed == 1
    assert report.rls_roles_parsed == 1
    assert sorted(w.title or "" for w in report.warnings) == [
        Constant.DEFINITION_TABLE_NOT_IN_SCAN,
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
