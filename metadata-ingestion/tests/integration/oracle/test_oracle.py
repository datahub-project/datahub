from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from tests.integration.oracle.common import OracleTestCaseBase  # type: ignore

FROZEN_TIME = "2022-02-03 07:00:00"


class OracleIntegrationTestCase(OracleTestCaseBase):
    def apply_mock_data(self, mock_create_engine, mock_inspect, mock_event):
        mock_event.listen.return_value = None

        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.side_effect = self.get_mock_data

        inspector_magic_mock = MagicMock()
        inspector_magic_mock.bind = connection_magic_mock
        inspector_magic_mock.engine.url.database = self.get_database_name()
        inspector_magic_mock.dialect.normalize_name.side_effect = lambda x: x
        inspector_magic_mock.dialect.denormalize_name.side_effect = lambda x: x
        inspector_magic_mock.dialect.server_version_info = (
            self.get_server_version_info()
        )
        inspector_magic_mock.dialect.type_compiler.process = lambda x: "NUMBER"

        mock_inspect.return_value = inspector_magic_mock
        mock_create_engine.connect.return_value = connection_magic_mock

    @mock.patch("datahub.ingestion.source.sql.sql_common.create_engine")
    @mock.patch("datahub.ingestion.source.sql.sql_common.inspect")
    @mock.patch("datahub.ingestion.source.sql.oracle.event")
    def apply(self, mock_create_engine, mock_inspect, mock_event):
        self.apply_mock_data(mock_create_engine, mock_inspect, mock_event)
        super().apply()


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_oracle_source_integration_with_out_database(pytestconfig, tmp_path, mock_time):
    oracle_source_integration_test = OracleIntegrationTestCase(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
        golden_file_name="golden_test_ingest_with_out_database.json",
        output_file_name="oracle_mce_output_with_out_database.json",
        add_database_name_to_urn=False,
    )
    oracle_source_integration_test.apply()


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_oracle_source_integration_with_database(pytestconfig, tmp_path, mock_time):
    oracle_source_integration_test = OracleIntegrationTestCase(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
        golden_file_name="golden_test_ingest_with_database.json",
        output_file_name="oracle_mce_output_with_database.json",
        add_database_name_to_urn=True,
    )
    oracle_source_integration_test.apply()
