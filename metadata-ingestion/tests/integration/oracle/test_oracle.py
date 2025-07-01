from typing import Any
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from sqlalchemy import exc

from datahub.ingestion.api.source import StructuredLogLevel
from datahub.ingestion.source.sql.oracle import OracleInspectorObjectWrapper
from tests.integration.oracle.common import (  # type: ignore[import-untyped]
    OracleSourceMockDataBase,
    OracleTestCaseBase,
)

FROZEN_TIME = "2022-02-03 07:00:00"


class OracleErrorHandlingMockData(OracleSourceMockDataBase):
    def get_data(self, *args: Any, **kwargs: Any) -> Any:
        if isinstance(args[0], str) and "sys_context" in args[0]:
            raise exc.DatabaseError("statement", [], "Mock DB Error")
        return super().get_data(*args, **kwargs)


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


class TestOracleSourceErrorHandling(OracleIntegrationTestCase):
    def __init__(self, pytestconfig, tmp_path):
        super().__init__(
            pytestconfig=pytestconfig,
            tmp_path=tmp_path,
            golden_file_name="golden_test_error_handling.json",
            output_file_name="oracle_mce_output_error_handling.json",
            add_database_name_to_urn=False,
        )
        self.default_mock_data = OracleErrorHandlingMockData()

    def test_get_db_name_error_handling(self):
        inspector = MagicMock()
        inspector.bind.execute.side_effect = exc.DatabaseError(
            "statement", [], "Mock DB Error"
        )
        inspector_wrapper = OracleInspectorObjectWrapper(inspector)

        db_name = inspector_wrapper.get_db_name()

        assert db_name == ""
        assert len(inspector_wrapper.report.failures) == 1
        error = inspector_wrapper.report.failures[0]
        assert error.impact.name == StructuredLogLevel.ERROR.name
        assert error.message == "database_fetch_error"

    def test_get_pk_constraint_error_handling(self):
        inspector = MagicMock()
        inspector.dialect.normalize_name.side_effect = lambda x: x
        inspector.dialect.denormalize_name.side_effect = lambda x: x
        inspector_wrapper = OracleInspectorObjectWrapper(inspector)

        with patch.object(
            inspector_wrapper, "_get_constraint_data"
        ) as mock_get_constraint:
            mock_get_constraint.side_effect = Exception("Mock constraint error")

            result = inspector_wrapper.get_pk_constraint("test_table", "test_schema")

            assert result == {"constrained_columns": [], "name": None}
            assert len(inspector_wrapper.report.failures) == 1
            error = inspector_wrapper.report.failures[0]
            assert error.impact.name == StructuredLogLevel.ERROR.name
            assert "Error processing primary key constraints" in error.message

    def test_get_foreign_keys_missing_table_warning(self):
        inspector = MagicMock()
        inspector.dialect.normalize_name.side_effect = lambda x: x
        inspector.dialect.denormalize_name.side_effect = lambda x: x
        inspector_wrapper = OracleInspectorObjectWrapper(inspector)

        mock_data = [
            (
                "FK1",
                "R",
                "local_col",
                None,
                "remote_col",
                "remote_owner",
                1,
                1,
                None,
                "NO ACTION",
            )
        ]

        with patch.object(
            inspector_wrapper, "_get_constraint_data"
        ) as mock_get_constraint:
            mock_get_constraint.return_value = mock_data

            inspector_wrapper.get_foreign_keys("test_table", "test_schema")

            assert len(inspector_wrapper.report.warnings) == 1
            warning = inspector_wrapper.report.warnings[0]
            assert warning.message == "Unable to query table_name from dba_cons_columns"

    def test_get_table_comment_with_cast(self):
        inspector = MagicMock()
        inspector.dialect.normalize_name.side_effect = lambda x: x
        inspector.dialect.denormalize_name.side_effect = lambda x: x
        inspector_wrapper = OracleInspectorObjectWrapper(inspector)

        mock_comment = "Test table comment"
        inspector.bind.execute.return_value.scalar.return_value = mock_comment

        result = inspector_wrapper.get_table_comment("test_table", "test_schema")

        assert result == {"text": mock_comment}
        execute_args = inspector.bind.execute.call_args[0]
        sql_text = str(execute_args[0])
        assert "CAST(:table_name AS VARCHAR(128))" in sql_text
        assert "CAST(:schema_name AS VARCHAR(128))" in sql_text


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_oracle_source_integration_with_out_database(pytestconfig, tmp_path):
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
def test_oracle_source_integration_with_database(pytestconfig, tmp_path):
    oracle_source_integration_test = OracleIntegrationTestCase(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
        golden_file_name="golden_test_ingest_with_database.json",
        output_file_name="oracle_mce_output_with_database.json",
        add_database_name_to_urn=True,
    )
    oracle_source_integration_test.apply()


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_oracle_source_error_handling(pytestconfig, tmp_path):
    test_case = TestOracleSourceErrorHandling(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
    )
    test_case.apply()
