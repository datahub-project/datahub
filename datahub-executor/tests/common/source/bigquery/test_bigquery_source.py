import datetime
from unittest.mock import Mock, call, patch

import pytest
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

from datahub_executor.common.connection.bigquery.bigquery_connection import (
    BigQueryConnection,
)
from datahub_executor.common.exceptions import (
    CustomSQLErrorException,
    InvalidParametersException,
    InvalidSourceTypeException,
)
from datahub_executor.common.source.bigquery.bigquery import BigQuerySource
from datahub_executor.common.source.bigquery.types import DEFAULT_OPERATION_TYPES_FILTER
from datahub_executor.common.source.types import DatabaseParams
from datahub_executor.common.types import (
    DatasetFilterType,
    EntityEventType,
    FreshnessFieldKind,
)

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_db.public.test_table,PROD)"
)
TEST_START = 1687643700064
TEST_END = 1687644000064
JAN_1_DATE = datetime.date(2023, 1, 1)
JAN_1_DATETIME = datetime.datetime(2023, 1, 1, 0, 0).replace(
    tzinfo=datetime.timezone.utc
)
JAN_1_TIMESTAMP = 1672531200000

TEST_INFORMATION_SCHEMA_UPDATE_QUERY = f"""
            SELECT last_modified_time
            FROM test_db.public.__TABLES__
            WHERE table_id="test_table"
                AND last_modified_time >= {TEST_START}
                AND last_modified_time <= {TEST_END}
            LIMIT 5
        ;"""
TEST_FIELD_UPDATE_QUERY = f"""
                SELECT timestamp as last_altered_date
                FROM test_db.public.test_table
                WHERE timestamp >= (TIMESTAMP_MILLIS(CAST({TEST_START} AS INT64)))
                AND timestamp <= (TIMESTAMP_MILLIS(CAST({TEST_END} AS INT64)))
                AND foo = 'bar'
                ORDER BY timestamp DESC
                LIMIT 5
            ;"""
TEST_HIGHWATERMARK_VALUE_QUERY = f"""
        SELECT timestamp
        FROM test_db.public.test_table
        WHERE timestamp >= TIMESTAMP('{TEST_START}')
        AND foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_VALUE_NO_PREV_QUERY = """
        SELECT timestamp
        FROM test_db.public.test_table
        
        WHERE foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_COUNT_QUERY = f"""
        SELECT COUNT(*)
        FROM test_db.public.test_table
        WHERE timestamp = TIMESTAMP('{TEST_END}')
        AND foo = 'bar'
    """
TEST_GET_ROW_COUNT_QUERY = """
            SELECT row_count
            FROM test_db.`public`.__TABLES__
            WHERE table_id='test_table';"""
TEST_NUM_ROWS_VIA_COUNT_QUERY = """
        SELECT COUNT(*)
        FROM test_db.public.test_table
        
    """
TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY = """
        SELECT COUNT(*)
        FROM test_db.public.test_table
        WHERE foo = 'bar'
    """
TEST_CUSTOM_SQL_STATEMENT = "SELECT SUM(num_items) FROM test_db.public.test_table;"


class MockLogginEntry:
    timestamp: datetime.datetime

    def __init__(self, timestamp: datetime.datetime):
        self.timestamp = timestamp


class TestBigQuerySource:
    def setup_method(self) -> None:
        self.bigquery_connection_mock = Mock(spec=BigQueryConnection)
        self.bigquery_connection_mock.config = Mock(spec=BigQueryV2Config)
        self.bigquery_source = BigQuerySource(self.bigquery_connection_mock)

    @patch.object(BigQuerySource, "_build_audit_log_results")
    @patch.object(BigQuerySource, "_extract_audit_logs_for_table")
    def test_get_entity_events_audit_log(
        self, extract_audit_logs_mock: Mock, build_mock: Mock
    ) -> None:
        self.bigquery_connection_mock.config.make_gcp_logging_client.return_value = None
        self.bigquery_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {
                "operation_types": None,
                "user_name": None,
            },
        )
        extract_audit_logs_mock.assert_called_once_with(
            None,
            "test_db",
            "public",
            "test_table",
            TEST_START,
            TEST_END,
            {"operation_types": None, "user_name": None},
        )
        build_mock.assert_called_once()

    @patch.object(BigQuerySource, "_build_information_schema_results")
    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_entity_events_information_schema_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.bigquery_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.INFORMATION_SCHEMA_UPDATE,
            [TEST_START, TEST_END],
            {},
        )
        execute_query_mock.assert_called_once_with(
            TEST_INFORMATION_SCHEMA_UPDATE_QUERY,
        )
        build_mock.assert_called_once()

    @patch.object(BigQuerySource, "_build_field_update_results")
    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_entity_events_field_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.bigquery_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.FIELD_UPDATE,
            [TEST_START, TEST_END],
            {
                "path": "timestamp",
                "type": "TIMESTAMP",
                "native_type": "TIMESTAMP",
                "kind": FreshnessFieldKind.LAST_MODIFIED,
                "filter": {"type": DatasetFilterType.SQL, "sql": "WHERE foo = 'bar';"},
            },
        )
        execute_query_mock.assert_called_once_with(TEST_FIELD_UPDATE_QUERY)
        build_mock.assert_called_once()

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_current_high_watermark_for_column(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[TEST_END]]
        (
            field_value,
            row_count,
        ) = self.bigquery_source.get_current_high_watermark_for_column(
            TEST_ENTITY_URN,
            EntityEventType.FIELD_UPDATE,
            [TEST_START, TEST_END],
            {
                "path": "timestamp",
                "type": "TIMESTAMP",
                "native_type": "TIMESTAMP",
                "kind": FreshnessFieldKind.HIGH_WATERMARK,
                "filter": {"type": DatasetFilterType.SQL, "sql": "WHERE foo = 'bar';"},
            },
            str(TEST_START),
        )
        execute_query_mock.assert_has_calls(
            [call(TEST_HIGHWATERMARK_VALUE_QUERY), call(TEST_HIGHWATERMARK_COUNT_QUERY)]
        )
        assert row_count == TEST_END
        assert field_value == str(TEST_END)

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_current_high_watermark_for_column_no_previous_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[TEST_END]]
        (
            field_value,
            row_count,
        ) = self.bigquery_source.get_current_high_watermark_for_column(
            TEST_ENTITY_URN,
            EntityEventType.FIELD_UPDATE,
            [TEST_START, TEST_END],
            {
                "path": "timestamp",
                "type": "TIMESTAMP",
                "native_type": "TIMESTAMP",
                "kind": FreshnessFieldKind.HIGH_WATERMARK,
                "filter": {"type": DatasetFilterType.SQL, "sql": "WHERE foo = 'bar';"},
            },
            None,
        )
        execute_query_mock.assert_has_calls(
            [
                call(TEST_HIGHWATERMARK_VALUE_NO_PREV_QUERY),
                call(TEST_HIGHWATERMARK_COUNT_QUERY),
            ]
        )
        assert row_count == TEST_END
        assert field_value == str(TEST_END)

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_current_high_watermark_for_column_no_previous_state(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = []
        (
            field_value,
            row_count,
        ) = self.bigquery_source.get_current_high_watermark_for_column(
            TEST_ENTITY_URN,
            EntityEventType.FIELD_UPDATE,
            [TEST_START, TEST_END],
            {
                "path": "timestamp",
                "type": "TIMESTAMP",
                "native_type": "TIMESTAMP",
                "kind": FreshnessFieldKind.HIGH_WATERMARK,
                "filter": {"type": DatasetFilterType.SQL, "sql": "WHERE foo = 'bar';"},
            },
            None,
        )
        execute_query_mock.assert_has_calls(
            [call(TEST_HIGHWATERMARK_VALUE_NO_PREV_QUERY)]
        )
        assert row_count == 0
        assert field_value == ""

    def test_execute_fetchall_query(self) -> None:
        query = "SELECT * FROM TABLE;"
        self.bigquery_source._execute_fetchall_query(query)
        self.bigquery_connection_mock.get_client().query.assert_called_once_with(query)

    def test_get_entity_events_field_update_bad_column_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.bigquery_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {
                    "path": "timestamp",
                    "type": "TIMESTAMP",
                    "native_type": "TIMESTAMP____NOTSUPPORTED",
                },
            )

    def test_get_entity_events_field_update_missing_inputs(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.bigquery_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_entity_events_unsupported_entity_type(self) -> None:
        with pytest.raises(InvalidSourceTypeException):
            self.bigquery_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_operation_types_filter(self) -> None:
        operational_type_filter = self.bigquery_source._get_operation_types_filter(
            {"operation_types": ["INSERT", "UPDATE"]}
        )
        assert operational_type_filter == '"INSERT" OR "UPDATE"'

    def test_get_operation_types_filter_empty(self) -> None:
        operational_type_filter = self.bigquery_source._get_operation_types_filter(
            {"operation_types": []}
        )
        assert operational_type_filter == DEFAULT_OPERATION_TYPES_FILTER

    def test_get_operation_types_filter_none(self) -> None:
        operational_type_filter = self.bigquery_source._get_operation_types_filter({})
        assert operational_type_filter == DEFAULT_OPERATION_TYPES_FILTER

    def test_get_user_name_filter(self) -> None:
        name_filter = self.bigquery_source._get_user_name_filter(
            {"user_name": "TestUser"}
        )
        assert name_filter == "testuser"

    def test_get_user_name_filter_empty(self) -> None:
        name_filter = self.bigquery_source._get_user_name_filter({"user_name": ""})
        assert name_filter == ""

    def test_get_user_name_filter_none(self) -> None:
        name_filter = self.bigquery_source._get_user_name_filter({})
        assert name_filter is None

    def test_build_audit_log_results(self) -> None:
        results = self.bigquery_source._build_audit_log_results(
            [["", MockLogginEntry(timestamp=JAN_1_DATETIME)]]
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.AUDIT_LOG_OPERATION

    def test_build_information_schema_results(self) -> None:
        results = self.bigquery_source._build_information_schema_results(
            [[JAN_1_TIMESTAMP]]
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE

    def test_get_current_high_watermark_for_column_invalid_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.bigquery_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {
                    "path": "timestamp",
                    "type": "STRING",
                    "native_type": "STRING",
                    "kind": FreshnessFieldKind.HIGH_WATERMARK,
                },
                None,
            )

    def test_get_current_high_watermark_for_column_missing_inputs(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.bigquery_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {},
                None,
            )

    def test_get_current_high_watermark_for_column_unsupported_entity_type(
        self,
    ) -> None:
        with pytest.raises(InvalidSourceTypeException):
            self.bigquery_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {},
                None,
            )

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_num_rows_via_stats_table(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["10"]]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.bigquery_source._get_num_rows_via_stats_table(db_params)
        execute_query_mock.assert_called_once_with(
            TEST_GET_ROW_COUNT_QUERY,
        )
        assert result == 10

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_num_rows_via_stats_table_no_rows(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = []
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.bigquery_source._get_num_rows_via_stats_table(db_params)
        execute_query_mock.assert_called_once_with(
            TEST_GET_ROW_COUNT_QUERY,
        )
        assert result is None

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_num_rows_via_count(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["10"]]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.bigquery_source._get_num_rows_via_count(db_params, "")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_QUERY,
        )
        assert result == 10

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_num_rows_via_count_with_filter(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["10"]]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.bigquery_source._get_num_rows_via_count(db_params, "foo = 'bar'")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY,
        )
        assert result == 10

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_single_value_failure_multiple_values(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[10, 11]]

        with pytest.raises(CustomSQLErrorException):
            self.bigquery_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_single_value_failure_invalid_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [["not a float"]]

        with pytest.raises(CustomSQLErrorException):
            self.bigquery_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(BigQuerySource, "_execute_fetchall_query")
    def test_get_single_value_success(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["100"]]

        value = self.bigquery_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)
        assert value == 100.0
