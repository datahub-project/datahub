"""Tests for the standalone `GoogleSheetsConnectorHandler`.

These exercise the handler without standing up a `FivetranSource`,
proving the workaround is encapsulated cleanly enough that deleting
the file (when DataHub gets native Google Sheets support) won't
require touching the source's tests.
"""

import datetime
from typing import Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.google_sheets_handler import (
    GoogleSheetsConnectorHandler,
)
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionConfig,
    FivetranConnectionDetails,
)
from datahub.metadata.schema_classes import BrowsePathsV2Class
from datahub.sdk.dataset import Dataset


def _make_conn_details(
    sheet_id: str = "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo",
    named_range: str = "Test_Range",
    connector_id: str = "test_connector",
) -> FivetranConnectionDetails:
    return FivetranConnectionDetails(
        id=connector_id,
        group_id="test_group",
        service="google_sheets",
        created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        succeeded_at=datetime.datetime(2025, 1, 1, 1, 0, 0),
        paused=False,
        sync_frequency=360,
        config=FivetranConnectionConfig(
            auth_type="ServiceAccount",
            sheet_id=sheet_id,
            named_range=named_range,
        ),
    )


def _make_handler(
    api_client: Optional[MagicMock] = None,
) -> GoogleSheetsConnectorHandler:
    cfg = FivetranSourceConfig.model_validate(
        {"api_config": {"api_key": "k", "api_secret": "s"}}
    )
    return GoogleSheetsConnectorHandler(
        api_client_provider=lambda: api_client,
        config=cfg,
        report=FivetranSourceReport(),
    )


class TestAppliesTo:
    def test_matches_google_sheets(self):
        assert (
            GoogleSheetsConnectorHandler.applies_to(
                Constant.GOOGLE_SHEETS_CONNECTOR_TYPE
            )
            is True
        )

    def test_rejects_other_types(self):
        # Sample of non-GSheets connector types — the handler must not
        # claim them, otherwise the source's main lineage path would
        # never run for these.
        for non_gsheets in ("postgres", "snowflake", "salesforce"):
            assert GoogleSheetsConnectorHandler.applies_to(non_gsheets) is False


class TestSheetIdExtraction:
    def test_full_url(self):
        handler = _make_handler()
        details = _make_conn_details(
            sheet_id="https://docs.google.com/spreadsheets/d/abc123/edit?gid=0#gid=0"
        )
        assert handler._get_sheet_id_from_url(details) == "abc123"

    def test_plain_id(self):
        handler = _make_handler()
        details = _make_conn_details(sheet_id="abc123")
        assert handler._get_sheet_id_from_url(details) == "abc123"

    def test_invalid_url_returns_none(self):
        handler = _make_handler()
        details = _make_conn_details(
            sheet_id="https://docs.google.com/invalid/path/format"
        )
        assert handler._get_sheet_id_from_url(details) is None


class TestNamedRangeId:
    def test_combines_sheet_and_range(self):
        handler = _make_handler()
        details = _make_conn_details(sheet_id="abc123", named_range="My_Range")
        assert handler._get_named_range_dataset_id(details) == "abc123.My_Range"

    def test_returns_none_when_sheet_id_invalid(self):
        handler = _make_handler()
        details = _make_conn_details(sheet_id="https://docs.google.com/invalid/format")
        assert handler._get_named_range_dataset_id(details) is None


class TestBuildInputDatasetUrn:
    def test_returns_urn_for_valid_connector(self):
        api_client = MagicMock()
        api_client.get_connection_details_by_id.return_value = _make_conn_details(
            sheet_id="abc123", named_range="Range1"
        )
        handler = _make_handler(api_client=api_client)
        connector = Connector(
            connector_id="c1",
            connector_name="GSheets c1",
            connector_type=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            paused=False,
            sync_frequency=360,
            destination_id="d1",
            user_id="",
            lineage=[],
            jobs=[],
        )

        urn = handler.build_input_dataset_urn(connector, env="PROD")
        assert urn is not None
        assert "google_sheets" in str(urn)
        assert "abc123.Range1" in str(urn)

    def test_returns_none_when_api_client_missing(self):
        # Callers should treat None as "skip this lineage edge."
        handler = _make_handler(api_client=None)
        connector = Connector(
            connector_id="c1",
            connector_name="GSheets c1",
            connector_type=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            paused=False,
            sync_frequency=360,
            destination_id="d1",
            user_id="",
            lineage=[],
            jobs=[],
        )
        assert handler.build_input_dataset_urn(connector, env="PROD") is None


class TestApiClientLazyResolution:
    def test_provider_called_each_time(self):
        # The provider callable is what enables tests to swap api_client
        # post-construction; pin that the handler reads it lazily.
        mock_client = MagicMock()
        mock_client.get_connection_details_by_id.return_value = _make_conn_details(
            sheet_id="abc", named_range="r"
        )
        clients: list[Optional[MagicMock]] = [None, mock_client]
        index = [0]

        def provider():
            return clients[index[0]]

        cfg = FivetranSourceConfig.model_validate(
            {"api_config": {"api_key": "k", "api_secret": "s"}}
        )
        handler = GoogleSheetsConnectorHandler(
            api_client_provider=provider,
            config=cfg,
            report=FivetranSourceReport(),
        )

        # First call: api_client is None → returns None.
        assert handler._get_connection_details("c1") is None

        # Swap to a real client and try again — handler picks up the new value.
        index[0] = 1
        details = handler._get_connection_details("c1")
        assert details is not None


def _browse_path_ids(dataset: Dataset) -> list[str]:
    browse = dataset._get_aspect(BrowsePathsV2Class)
    assert browse is not None
    return [entry.id for entry in browse.path]


def _make_connector(
    connector_id: str = "c1",
    connector_name: str = "Weekly Metrics",
    connector_type: str = Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
) -> Connector:
    return Connector(
        connector_id=connector_id,
        connector_name=connector_name,
        connector_type=connector_type,
        paused=False,
        sync_frequency=360,
        destination_id="d1",
        user_id="",
        lineage=[],
        jobs=[],
    )


class TestEmitWorkunitsHumanReadableNames:
    def test_uses_connector_name_for_sheet_and_named_range_for_range(self):
        sheet_id = "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo"
        named_range_id = "Weekly_Metrics_Range"
        api_client = MagicMock()
        api_client.get_connection_details_by_id.return_value = _make_conn_details(
            sheet_id=sheet_id, named_range=named_range_id
        )
        handler = _make_handler(api_client=api_client)
        connector = _make_connector()

        entities = list(handler.emit_workunits(connector))
        assert len(entities) == 2
        sheet, named_range = entities
        assert isinstance(sheet, Dataset)
        assert isinstance(named_range, Dataset)

        # URNs stay ID-based so lineage does not churn when the connection
        # is renamed in Fivetran.
        assert sheet_id in str(sheet.urn)
        assert f"{sheet_id}.{named_range_id}" in str(named_range.urn)

        assert sheet.display_name == "Weekly Metrics"
        assert named_range.display_name == named_range_id
        assert sheet.display_name != sheet_id

        assert _browse_path_ids(sheet) == ["Weekly Metrics"]
        assert _browse_path_ids(named_range) == ["Weekly Metrics"]

        assert sheet.custom_properties["sheet_id"] == sheet_id
        assert sheet.custom_properties["connector_name"] == "Weekly Metrics"
        assert named_range.custom_properties["named_range"] == named_range_id
        assert named_range.custom_properties["connector_name"] == "Weekly Metrics"
        assert named_range.subtype == DatasetSubTypes.GOOGLE_SHEETS_NAMED_RANGE
        assert sheet.subtype == DatasetSubTypes.GOOGLE_SHEETS

    def test_falls_back_to_connector_id_when_name_blank(self):
        api_client = MagicMock()
        api_client.get_connection_details_by_id.return_value = _make_conn_details(
            sheet_id="abc123", named_range="Range1"
        )
        handler = _make_handler(api_client=api_client)
        connector = _make_connector(connector_name="   ")

        sheet, named_range = list(handler.emit_workunits(connector))
        assert isinstance(sheet, Dataset)
        assert isinstance(named_range, Dataset)
        assert sheet.display_name == "c1"
        assert named_range.display_name == "Range1"
        assert _browse_path_ids(sheet) == ["c1"]
        assert _browse_path_ids(named_range) == ["c1"]

    def test_shared_spreadsheet_uses_min_connection_name_regardless_of_order(self):
        sheet_id = "shared_sheet_abc"
        api_client = MagicMock()

        def _details(connection_id: str) -> FivetranConnectionDetails:
            named_range = "Budget" if connection_id == "c_budget" else "Actuals"
            return _make_conn_details(
                sheet_id=sheet_id,
                named_range=named_range,
                connector_id=connection_id,
            )

        api_client.get_connection_details_by_id.side_effect = _details
        budget = _make_connector(connector_id="c_budget", connector_name="sales.budget")
        actuals = _make_connector(
            connector_id="c_actuals", connector_name="sales.actuals"
        )

        for order in ((budget, actuals), (actuals, budget)):
            handler = _make_handler(api_client=api_client)
            handler.remember_connections(order)
            first_sheet, first_range = list(handler.emit_workunits(order[0]))
            second_sheet, second_range = list(handler.emit_workunits(order[1]))
            assert isinstance(first_sheet, Dataset)
            assert isinstance(second_sheet, Dataset)
            assert isinstance(first_range, Dataset)
            assert isinstance(second_range, Dataset)
            assert first_sheet.display_name == "sales.actuals"
            assert second_sheet.display_name == "sales.actuals"
            assert _browse_path_ids(first_sheet) == ["sales.actuals"]
            assert _browse_path_ids(second_sheet) == ["sales.actuals"]
            assert first_sheet.custom_properties["connector_name"] == "sales.actuals"
            assert first_sheet.custom_properties["connector_id"] == "c_actuals"
            assert {first_range.display_name, second_range.display_name} == {
                "Budget",
                "Actuals",
            }
            range_by_name = {
                entity.display_name: entity
                for entity in (first_range, second_range)
                if isinstance(entity, Dataset)
            }
            assert _browse_path_ids(range_by_name["Budget"]) == ["sales.budget"]
            assert _browse_path_ids(range_by_name["Actuals"]) == ["sales.actuals"]
            assert range_by_name["Budget"].custom_properties["connector_name"] == (
                "sales.budget"
            )
            assert range_by_name["Actuals"].custom_properties["connector_name"] == (
                "sales.actuals"
            )
