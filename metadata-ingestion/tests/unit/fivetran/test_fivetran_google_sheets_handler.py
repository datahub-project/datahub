"""Tests for the standalone `GoogleSheetsConnectorHandler`.

These exercise the handler without standing up a `FivetranSource`,
proving the workaround is encapsulated cleanly enough that deleting
the file (when DataHub gets native Google Sheets support) won't
require touching the source's tests.
"""

import datetime
from typing import Optional
from unittest.mock import MagicMock

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


def _make_conn_details(
    sheet_id: str = "1A82PdLAE7NXLLb5JcLPKeIpKUMytXQba5Z-Ei-mbXLo",
    named_range: str = "Test_Range",
) -> FivetranConnectionDetails:
    return FivetranConnectionDetails(
        id="test_connector",
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
