"""Unit tests for resolving DirectLake ``sourceColumn`` from the semantic model's
TMDL definition (``extract_directlake_source_columns_from_definition``)."""

import base64
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest import mock

import pytest
import requests

from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_resolver
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FIELD_TYPE_MAPPING,
    Column,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    SemanticModelDefinitionError,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import (
    TmdlParseError,
    decode_definition_part,
    iter_table_parts,
    parse_tmdl_name,
    parse_tmdl_table,
)

WORKSPACE_ID = "11111111-2222-4333-8444-555555555555"
DATASET_ID = "66666666-7777-4888-9999-000000000000"
FABRIC = "https://api.fabric.microsoft.com/v1"
DEFINITION_URL = (
    f"{FABRIC}/workspaces/{WORKSPACE_ID}/semanticModels/{DATASET_ID}/getDefinition"
)
OPERATION_URL = f"{FABRIC}/operations/abcd-1234"

CUSTOMERS_TMDL = """/// Customer dimension
table Customers
\tlineageTag: 1f7c0e0a

\tcolumn CustomerID
\t\tdataType: int64
\t\tlineageTag: 5a1b
\t\tsummarizeBy: none
\t\tsourceColumn: CustomerID

\t\tannotation SummarizationSetBy = Automatic

\tcolumn 'Customer Name'
\t\tdataType: string
\t\tsourceColumn: CustomerName

\tcolumn 'Name Upper' = UPPER(Customers[Customer Name])
\t\tdataType: string

\tmeasure 'Customer Count' = COUNTROWS(Customers)
\t\tformatString: 0

\tpartition Customers = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: customers
\t\t\tschemaName: dbo
\t\t\texpressionSource: 'DirectLake - SalesLakehouse'
"""


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _parts(tables: Dict[str, str]) -> List[dict]:
    parts = [
        {
            "path": "definition/model.tmdl",
            "payload": _b64("model Model\n\tculture: en-US\n"),
            "payloadType": "InlineBase64",
        },
        {
            "path": "definition/expressions.tmdl",
            "payload": _b64("expression 'DirectLake - SalesLakehouse' =\n"),
            "payloadType": "InlineBase64",
        },
    ]
    for name, text in tables.items():
        parts.append(
            {
                "path": f"definition/tables/{name}.tmdl",
                "payload": _b64(text),
                "payloadType": "InlineBase64",
            }
        )
    return parts


class TestTmdlParser:
    def test_quoted_and_renamed_columns(self) -> None:
        table, columns = parse_tmdl_table(CUSTOMERS_TMDL)
        assert table == "Customers"
        assert columns == {"CustomerID": "CustomerID", "Customer Name": "CustomerName"}

    def test_calculated_columns_and_measures_ignored(self) -> None:
        _, columns = parse_tmdl_table(CUSTOMERS_TMDL)
        assert "Name Upper" not in columns
        assert "Customer Count" not in columns

    def test_quoted_names_with_apostrophes(self) -> None:
        text = (
            "table 'Customer''s Orders'\n"
            "\tcolumn 'Owner''s Name'\n"
            "\t\tsourceColumn: owner_name\n"
            "\tcolumn 'Order.Total = Net'\n"
            '\t\tsourceColumn: "order total"\n'
        )
        table, columns = parse_tmdl_table(text)
        assert table == "Customer's Orders"
        assert columns == {
            "Owner's Name": "owner_name",
            "Order.Total = Net": "order total",
        }

    def test_space_indentation_and_bom(self) -> None:
        text = (
            "\ufefftable Sales\n"
            "    column order_id\n"
            "        dataType: int64\n"
            "        sourceColumn: OrderID\n"
            "    column 'Order Amount'\n"
            "        sourceColumn: order_amount\n"
        )
        assert parse_tmdl_table(text) == (
            "Sales",
            {"order_id": "OrderID", "Order Amount": "order_amount"},
        )

    def test_fenced_multiline_expression_skipped(self) -> None:
        text = (
            "table Sales\n"
            "\tcolumn Calc = ```\n"
            "\t\t\tVAR x = 1\n"
            "\t\t\tcolumn Fake\n"
            "\t\t\tRETURN x\n"
            "\t\t\t```\n"
            "\t\tsourceColumn: not_a_binding\n"
            "\tcolumn Real\n"
            "\t\tsourceColumn: real_col\n"
        )
        assert parse_tmdl_table(text) == ("Sales", {"Real": "real_col"})

    def test_multiline_measure_expression_not_read_as_column(self) -> None:
        text = (
            "table Sales\n"
            "\tmeasure Total =\n"
            "\t\t\tcolumn Fake\n"
            "\t\tformatString: 0\n"
            "\tcolumn Real\n"
            "\t\tsourceColumn: real_col\n"
        )
        assert parse_tmdl_table(text) == ("Sales", {"Real": "real_col"})

    def test_non_data_column_types_ignored(self) -> None:
        text = (
            "table 'Calc Table'\n"
            "\tcolumn Region\n"
            "\t\ttype: calculatedTableColumn\n"
            "\t\tsourceColumn: [Region]\n"
            "\tcolumn 'RowNumber-2662979B'\n"
            "\t\ttype: rowNumber\n"
            "\t\tsourceColumn: x\n"
        )
        assert parse_tmdl_table(text) == ("Calc Table", {})

    def test_hierarchy_level_column_property_not_a_binding(self) -> None:
        text = (
            "table Dates\n"
            "\tcolumn Year\n"
            "\t\tsourceColumn: year\n"
            "\thierarchy Calendar\n"
            "\t\tlevel Year\n"
            "\t\t\tcolumn: Year\n"
        )
        assert parse_tmdl_table(text) == ("Dates", {"Year": "year"})

    def test_column_without_source_column_omitted(self) -> None:
        text = "table T\n\tcolumn a\n\t\tdataType: string\n"
        assert parse_tmdl_table(text) == ("T", {})

    def test_no_table_declaration(self) -> None:
        assert parse_tmdl_table("model Model\n\tculture: en-US\n") == (None, {})

    def test_unterminated_quoted_name_raises(self) -> None:
        with pytest.raises(TmdlParseError):
            parse_tmdl_name("'Customer Name")

    def test_decode_part(self) -> None:
        part = {"payload": _b64("table T\n"), "payloadType": "InlineBase64"}
        assert decode_definition_part(part) == "table T\n"
        with pytest.raises(TmdlParseError):
            decode_definition_part({"payload": "!!!", "payloadType": "InlineBase64"})
        with pytest.raises(TmdlParseError):
            decode_definition_part({"payload": "", "payloadType": "Other"})

    def test_only_table_parts_selected(self) -> None:
        paths = [p["path"] for p in iter_table_parts(_parts({"A": "table A\n"}))]
        assert paths == ["definition/tables/A.tmdl"]

    def test_malformed_parts_skipped(self) -> None:
        parts: List[Any] = [
            "not-a-dict",
            None,
            {"path": 42},
            {"payload": _b64("table A\n")},
            *_parts({"A": "table A\n"}),
        ]
        paths = [p["path"] for p in iter_table_parts(parts)]
        assert paths == ["definition/tables/A.tmdl"]

    def test_non_string_payload_raises_parse_error(self) -> None:
        with pytest.raises(TmdlParseError):
            decode_definition_part({"payload": 123, "payloadType": "InlineBase64"})

    def test_keywords_case_insensitive(self) -> None:
        text = (
            "Table Sales\n"
            "\tColumn 'Order Amount'\n"
            "\t\tSourceColumn: order_amount\n"
            "\tCOLUMN Region\n"
            "\t\tTYPE: calculatedTableColumn\n"
            "\t\tsourcecolumn: [Region]\n"
        )
        assert parse_tmdl_table(text) == ("Sales", {"Order Amount": "order_amount"})

    def test_single_line_fence_does_not_swallow_rest_of_file(self) -> None:
        text = (
            "table Sales\n"
            "\tmeasure Total = ```SUM(Sales[Amount])```\n"
            "\t\tformatString: 0\n"
            "\tcolumn Real\n"
            "\t\tsourceColumn: real_col\n"
        )
        assert parse_tmdl_table(text) == ("Sales", {"Real": "real_col"})


def _mock_msal_cca(*args: Any, **kwargs: Any) -> Any:
    client = mock.MagicMock()
    client.acquire_token_for_client.return_value = {"access_token": "dummy"}
    return client


@pytest.fixture(autouse=True)
def _patch_msal_and_sleep() -> Iterator[mock.MagicMock]:
    with (
        mock.patch("msal.ConfidentialClientApplication", side_effect=_mock_msal_cca),
        mock.patch.object(data_resolver, "sleep") as sleep,
    ):
        yield sleep


@pytest.fixture
def resolver() -> AdminAPIResolver:
    return AdminAPIResolver(
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
        metadata_api_timeout=30,
    )


class TestGetSemanticModelDefinition:
    def test_immediate_200(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        parts = _parts({"Customers": CUSTOMERS_TMDL})
        requests_mock.post(DEFINITION_URL, json={"definition": {"parts": parts}})

        assert (
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )
            == parts
        )
        request = requests_mock.request_history[0]
        assert request.qs == {"format": ["tmdl"]}
        # Fabric scope token, not the Power BI one
        resolver._msal_client.acquire_token_for_client.assert_any_call(
            scopes=["https://api.fabric.microsoft.com/.default"]
        )

    def test_202_poll_then_result(
        self,
        resolver: AdminAPIResolver,
        requests_mock: Any,
        _patch_msal_and_sleep: mock.MagicMock,
    ) -> None:
        parts = _parts({"Customers": CUSTOMERS_TMDL})
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": OPERATION_URL, "Retry-After": "2"},
        )
        requests_mock.get(
            OPERATION_URL,
            [
                {"json": {"status": "Running"}, "headers": {"Retry-After": "3"}},
                {
                    "json": {"status": "Succeeded"},
                    "headers": {"Location": f"{OPERATION_URL}/result"},
                },
            ],
        )
        requests_mock.get(
            f"{OPERATION_URL}/result", json={"definition": {"parts": parts}}
        )

        assert (
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )
            == parts
        )
        assert [c.args[0] for c in _patch_msal_and_sleep.call_args_list] == [2, 3]

    def test_202_without_location_uses_operation_id(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"x-ms-operation-id": "abcd-1234", "Retry-After": "1"},
        )
        requests_mock.get(OPERATION_URL, json={"status": "Succeeded"})
        requests_mock.get(f"{OPERATION_URL}/result", json={"definition": {"parts": []}})
        assert (
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )
            == []
        )

    def test_non_fabric_location_not_followed(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={
                "Location": "https://attacker.example.com/operations/abcd-1234",
                "x-ms-operation-id": "abcd-1234",
            },
        )
        requests_mock.get(OPERATION_URL, json={"status": "Succeeded"})
        requests_mock.get(f"{OPERATION_URL}/result", json={"definition": {"parts": []}})
        resolver.get_semantic_model_definition(
            WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
        )
        assert all(
            r.hostname == "api.fabric.microsoft.com"
            for r in requests_mock.request_history
        )

    def test_operation_failed(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(
            DEFINITION_URL, status_code=202, headers={"Location": OPERATION_URL}
        )
        requests_mock.get(
            OPERATION_URL,
            json={
                "status": "Failed",
                "error": {"errorCode": "OperationFailed", "message": "boom"},
            },
        )
        with pytest.raises(SemanticModelDefinitionError, match="OperationFailed boom"):
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )

    def test_operation_timeout(
        self,
        resolver: AdminAPIResolver,
        requests_mock: Any,
        _patch_msal_and_sleep: mock.MagicMock,
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": OPERATION_URL, "Retry-After": "5"},
        )
        requests_mock.get(
            OPERATION_URL, json={"status": "Running"}, headers={"Retry-After": "5"}
        )
        with pytest.raises(SemanticModelDefinitionError, match="within 12 seconds"):
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=12
            )
        # Polled at 5s and 10s, then once more at the 12s budget (the server's
        # Retry-After is capped at the remaining time).
        assert len([r for r in requests_mock.request_history if r.method == "GET"]) == 3
        assert [c.args[0] for c in _patch_msal_and_sleep.call_args_list] == [5, 5, 2]

    def test_timeout_budget_counts_wall_clock_time(
        self,
        resolver: AdminAPIResolver,
        requests_mock: Any,
        _patch_msal_and_sleep: mock.MagicMock,
    ) -> None:
        """Slow poll requests count against the budget, not only the sleeps."""
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": OPERATION_URL, "Retry-After": "1"},
        )
        requests_mock.get(
            OPERATION_URL, json={"status": "Running"}, headers={"Retry-After": "1"}
        )
        # Each monotonic() read advances 20s, as if every poll request were slow.
        clock = iter(range(0, 10_000, 20))
        with (
            mock.patch.object(data_resolver, "monotonic", lambda: next(clock)),
            pytest.raises(SemanticModelDefinitionError, match="within 60 seconds"),
        ):
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )
        assert len([r for r in requests_mock.request_history if r.method == "GET"]) < 5

    def test_long_retry_after_honoured(
        self,
        resolver: AdminAPIResolver,
        requests_mock: Any,
        _patch_msal_and_sleep: mock.MagicMock,
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"Location": OPERATION_URL, "Retry-After": "30"},
        )
        requests_mock.get(OPERATION_URL, json={"status": "Succeeded"})
        requests_mock.get(f"{OPERATION_URL}/result", json={"definition": {"parts": []}})
        resolver.get_semantic_model_definition(
            WORKSPACE_ID, DATASET_ID, max_wait_seconds=120
        )
        assert [c.args[0] for c in _patch_msal_and_sleep.call_args_list] == [30]

    def test_operation_id_is_url_quoted(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=202,
            headers={"x-ms-operation-id": "../../admin/x?y=1", "Retry-After": "1"},
        )
        quoted = f"{FABRIC}/operations/..%2F..%2Fadmin%2Fx%3Fy%3D1"
        requests_mock.get(quoted, json={"status": "Succeeded"})
        requests_mock.get(f"{quoted}/result", json={"definition": {"parts": []}})
        resolver.get_semantic_model_definition(
            WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
        )
        assert all(
            r.hostname == "api.fabric.microsoft.com"
            and r.path.startswith("/v1/operations/")
            for r in requests_mock.request_history[1:]
        )

    def test_forbidden_raises_http_error(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(
            DEFINITION_URL,
            status_code=403,
            json={"errorCode": "InsufficientPrivileges"},
        )
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )
        assert data_resolver.is_permission_error(exc_info.value)

    def test_unexpected_body(
        self, resolver: AdminAPIResolver, requests_mock: Any
    ) -> None:
        requests_mock.post(DEFINITION_URL, json={"unexpected": True})
        with pytest.raises(SemanticModelDefinitionError):
            resolver.get_semantic_model_definition(
                WORKSPACE_ID, DATASET_ID, max_wait_seconds=60
            )


def _column(name: str, source_column: Optional[str] = None) -> Column:
    return Column(
        name=name,
        dataType="String",
        isHidden=False,
        datahubDataType=FIELD_TYPE_MAPPING["String"],
        columnType="Data",
        sourceColumn=source_column,
    )


def _workspace_and_dataset(
    storage_mode: str = Constant.DIRECT_LAKE,
) -> Tuple[Workspace, PowerBIDataset]:
    workspace = Workspace(
        id=WORKSPACE_ID,
        name="Sales Analytics",
        type="Workspace",
        dashboards={},
        reports={},
        datasets={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )
    dataset = PowerBIDataset(
        id=DATASET_ID,
        name="Sales Model",
        description="",
        webUrl=None,
        workspace_id=WORKSPACE_ID,
        workspace_name=workspace.name,
        parameters={},
        tables=[],
        tags=[],
    )
    dataset.tables.append(
        Table(
            name="Customers",
            full_name="Sales_Model.Customers",
            columns=[
                _column("CustomerID"),
                _column("Customer Name"),
                _column("Region", source_column="region_from_scan"),
            ],
            dataset=dataset,
            storage_mode=storage_mode,
        )
    )
    return workspace, dataset


def _make_api(**overrides: Any) -> PowerBiAPI:
    config = PowerBiDashboardSourceConfig(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        extract_directlake_source_columns_from_definition=True,
        **overrides,
    )
    return PowerBiAPI(config=config, reporter=PowerBiDashboardSourceReport())


class TestApplyDirectLakeDefinition:
    def test_fills_missing_source_columns_and_caches(self, requests_mock: Any) -> None:
        api = _make_api()
        requests_mock.post(
            DEFINITION_URL,
            json={"definition": {"parts": _parts({"Customers": CUSTOMERS_TMDL})}},
        )
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)
        api._apply_directlake_definition(workspace, dataset)

        columns = {c.name: c.sourceColumn for c in dataset.tables[0].columns or []}
        assert columns == {
            "CustomerID": "CustomerID",
            "Customer Name": "CustomerName",
            # The scan's own binding is kept.
            "Region": "region_from_scan",
        }
        assert requests_mock.call_count == 1
        assert api.reporter.directlake_definitions_fetched == 1
        assert api.reporter.directlake_source_columns_from_definition == 2
        assert api.reporter.directlake_definition_failures == 0

    def test_non_directlake_model_not_requested(self, requests_mock: Any) -> None:
        api = _make_api()
        workspace, dataset = _workspace_and_dataset(storage_mode="Import")
        api._apply_directlake_definition(workspace, dataset)
        assert requests_mock.call_count == 0

    def test_forbidden_warns_and_falls_back(self, requests_mock: Any) -> None:
        api = _make_api()
        requests_mock.post(DEFINITION_URL, status_code=403)
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)

        assert dataset.tables[0].columns is not None
        assert dataset.tables[0].columns[1].sourceColumn is None
        assert api.reporter.directlake_definition_failures == 1
        titles = [w.title for w in api.reporter.warnings]
        assert "DirectLake semantic model definition access denied" in titles

    def test_operation_failure_warns(self, requests_mock: Any) -> None:
        api = _make_api()
        requests_mock.post(
            DEFINITION_URL, status_code=202, headers={"Location": OPERATION_URL}
        )
        requests_mock.get(OPERATION_URL, json={"status": "Failed"})
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)

        assert api.reporter.directlake_definition_failures == 1
        titles = [w.title for w in api.reporter.warnings]
        assert "DirectLake semantic model definition unavailable" in titles

    def test_request_timeout_warns(self, requests_mock: Any) -> None:
        api = _make_api()
        requests_mock.post(DEFINITION_URL, exc=requests.exceptions.ReadTimeout)
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)

        assert api.reporter.directlake_definition_failures == 1

    def test_unparseable_part_warns_other_parts_used(self, requests_mock: Any) -> None:
        api = _make_api()
        parts = _parts({"Customers": CUSTOMERS_TMDL})
        parts.append(
            {
                "path": "definition/tables/Broken.tmdl",
                "payload": _b64("table 'Broken\n"),
                "payloadType": "InlineBase64",
            }
        )
        requests_mock.post(DEFINITION_URL, json={"definition": {"parts": parts}})
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)

        assert api.reporter.directlake_definition_parse_failures == 1
        assert api.reporter.directlake_source_columns_from_definition == 2

    def test_token_failure_warns_once_and_stops_requesting(
        self, requests_mock: Any
    ) -> None:
        api = _make_api()
        resolver = api._get_resolver()
        resolver._msal_client.acquire_token_for_client.return_value = {}
        workspace, dataset = _workspace_and_dataset()
        _, other = _workspace_and_dataset()
        other.id = "77777777-7777-4777-8777-777777777777"

        api._apply_directlake_definition(workspace, dataset)
        api._apply_directlake_definition(workspace, other)

        assert requests_mock.call_count == 0
        assert api.reporter.directlake_definition_failures == 1
        titles = [w.title for w in api.reporter.warnings]
        assert titles.count("DirectLake semantic model definitions unavailable") == 1

    def test_unexpected_error_isolated_to_model(self, requests_mock: Any) -> None:
        """A bug or odd payload must not propagate into scan-result parsing,
        where it would drop the scan metadata of the whole workspace batch."""
        api = _make_api()
        requests_mock.post(
            DEFINITION_URL,
            json={"definition": {"parts": _parts({"Customers": CUSTOMERS_TMDL})}},
        )
        workspace, dataset = _workspace_and_dataset()
        with mock.patch(
            "datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api.parse_tmdl_table",
            side_effect=RuntimeError("boom"),
        ):
            api._apply_directlake_definition(workspace, dataset)

        assert dataset.tables[0].columns is not None
        assert dataset.tables[0].columns[1].sourceColumn is None
        assert api.reporter.directlake_definition_failures == 1
        titles = [w.title for w in api.reporter.warnings]
        assert "DirectLake semantic model definition processing failed" in titles

    def test_table_missing_from_definition_reported(self, requests_mock: Any) -> None:
        api = _make_api()
        requests_mock.post(
            DEFINITION_URL,
            json={"definition": {"parts": _parts({"Other": "table Other\n"})}},
        )
        workspace, dataset = _workspace_and_dataset()

        api._apply_directlake_definition(workspace, dataset)

        assert api.reporter.directlake_definition_tables_missing == 1
        titles = [w.title for w in api.reporter.warnings]
        assert "DirectLake tables missing from semantic model definition" in titles


class TestConfig:
    def test_default_off(self) -> None:
        config = PowerBiDashboardSourceConfig(
            tenant_id="t", client_id="c", client_secret="s"
        )
        assert config.extract_directlake_source_columns_from_definition is False

    def test_requires_column_level_lineage(self) -> None:
        with pytest.raises(ValueError, match="requires extract_column_level_lineage"):
            PowerBiDashboardSourceConfig(
                tenant_id="t",
                client_id="c",
                client_secret="s",
                extract_column_level_lineage=False,
                extract_directlake_source_columns_from_definition=True,
            )

    def test_rejects_government_environment(self) -> None:
        with pytest.raises(ValueError, match="COMMERCIAL"):
            PowerBiDashboardSourceConfig(
                tenant_id="t",
                client_id="c",
                client_secret="s",
                environment="GOVERNMENT",
                extract_directlake_source_columns_from_definition=True,
            )
