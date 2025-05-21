import datetime
from unittest.mock import Mock, call, patch

import pytest
from datahub.utilities.time import ts_millis_to_datetime

from datahub_executor.common.assertion.types import AssertionDatabaseParams
from datahub_executor.common.connection.databricks.databricks_connection import (
    DatabricksConnection,
)
from datahub_executor.common.constants import DATABRICKS_PLATFORM_URN
from datahub_executor.common.exceptions import (
    CustomSQLErrorException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
)
from datahub_executor.common.source.databricks.databricks import DatabricksSource
from datahub_executor.common.source.types import DatabaseParams
from datahub_executor.common.types import (
    DatasetFilterType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    EntityEventType,
    FreshnessFieldKind,
)

TEST_ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:databricks,hive_metastore.default.base_table,PROD)"
UNITY_CATALOG_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,main.default.base_table,PROD)"
)
TEST_QUALIFIED_NAME = "hive_metastore.default.base_table"
TEST_TABLE_NAME = "base_table"
TEST_START = 1687643700064
TEST_END = 1687644000064
JAN_1_DATE = datetime.date(2023, 1, 1)
JAN_1_DATETIME = datetime.datetime(2023, 1, 1, 0, 0).replace(
    tzinfo=datetime.timezone.utc
)
JAN_1_TIMESTAMP = 1672531200000

TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER = f"""
            WITH table_audit_log AS 
                (DESCRIBE HISTORY `hive_metastore`.`default`.`base_table`)
            SELECT timestamp FROM table_audit_log 
            WHERE 
                TO_TIMESTAMP(FROM_UNIXTIME({TEST_START}/1000)) <= timestamp 
                AND timestamp <= TO_TIMESTAMP(FROM_UNIXTIME({TEST_END}/1000))
                
                
                LIMIT 5;
        """

TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER = f"""
            WITH table_audit_log AS 
                (DESCRIBE HISTORY `hive_metastore`.`default`.`base_table`)
            SELECT timestamp FROM table_audit_log 
            WHERE 
                TO_TIMESTAMP(FROM_UNIXTIME({TEST_START}/1000)) <= timestamp 
                AND timestamp <= TO_TIMESTAMP(FROM_UNIXTIME({TEST_END}/1000))
                
                AND userName = "TestUserName"
                LIMIT 5;
        """

TEST_AUDIT_LOG_QUERY_OPERATIONAL_TYPE_FILTER = f"""
            WITH table_audit_log AS 
                (DESCRIBE HISTORY `hive_metastore`.`default`.`base_table`)
            SELECT timestamp FROM table_audit_log 
            WHERE 
                TO_TIMESTAMP(FROM_UNIXTIME({TEST_START}/1000)) <= timestamp 
                AND timestamp <= TO_TIMESTAMP(FROM_UNIXTIME({TEST_END}/1000))
                AND operation IN ("MERGE","UPDATE")
                
                LIMIT 5;
        """
TEST_DESCRIBE_DETAIL_UPDATE_QUERY = (
    "DESCRIBE DETAIL `hive_metastore`.`default`.`base_table`;"
)

TEST_FILE_METADATA_UPDATE_QUERY = f"""
            SELECT MAX(_metadata.file_modification_time)
            FROM `hive_metastore`.`default`.`base_table`
            WHERE _metadata.file_modification_time >= (TO_TIMESTAMP(FROM_UNIXTIME({TEST_START}/1000)))
            AND _metadata.file_modification_time <= (TO_TIMESTAMP(FROM_UNIXTIME({TEST_END}/1000)));
        """

TEST_FIELD_UPDATE_QUERY = f"""
                SELECT timestamp as last_altered_date
                FROM `hive_metastore`.`default`.`base_table`
                WHERE timestamp >= (TO_TIMESTAMP(FROM_UNIXTIME({TEST_START}/1000)))
                AND timestamp <= (TO_TIMESTAMP(FROM_UNIXTIME({TEST_END}/1000)))
                AND foo = 'bar'
                ORDER BY timestamp DESC
                LIMIT 5
                ;
            """
TEST_HIGHWATERMARK_VALUE_QUERY = f"""
        SELECT timestamp
        FROM `hive_metastore`.`default`.`base_table`
        WHERE timestamp >= TO_TIMESTAMP('{TEST_START}')
        AND foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_VALUE_NO_PREV_QUERY = """
        SELECT timestamp
        FROM `hive_metastore`.`default`.`base_table`
        
        WHERE foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_COUNT_QUERY = f"""
        SELECT COUNT(*)
        FROM `hive_metastore`.`default`.`base_table`
        WHERE timestamp = TO_TIMESTAMP('{TEST_END}')
        AND foo = 'bar'
    """
TEST_NUM_ROWS_VIA_COUNT_QUERY = """
        SELECT COUNT(*)
        FROM `hive_metastore`.`default`.`base_table`
        
    """
TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY = """
        SELECT COUNT(*)
        FROM `hive_metastore`.`default`.`base_table`
        WHERE foo = 'bar'
    """
TEST_CUSTOM_SQL_STATEMENT = (
    "SELECT SUM(num_items) FROM `hive_metastore`.`default`.`base_table`;"
)


class TestDatabricksSource:
    def setup_method(self) -> None:
        self.databricks_connection_mock = Mock(spec=DatabricksConnection)
        self.databricks_source = DatabricksSource(self.databricks_connection_mock)

    @patch.object(DatabricksSource, "_build_audit_log_results")
    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_no_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": None, "user_name": None},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(DatabricksSource, "_build_audit_log_results")
    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": None, "user_name": "TestUserName"},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(DatabricksSource, "_build_audit_log_results")
    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_operational_type_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": ["MERGE", "UPDATE"], "user_name": None},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_OPERATIONAL_TYPE_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_entity_events_via_audit_log_fails_for_nondelta(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.side_effect = SourceQueryFailedException(
            "DESCRIBE HISTORY is only supported for Delta tables",
            query="DESCRIBE HISTORY xxx",
        )
        with pytest.raises(InvalidSourceTypeException):
            self.databricks_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.AUDIT_LOG_OPERATION,
                [TEST_START, TEST_END],
                {"operation_types": None, "user_name": None},
            )

    @patch.object(DatabricksSource, "_build_describe_detail_results")
    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_entity_events_describe_detail_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.INFORMATION_SCHEMA_UPDATE,
            [TEST_START, TEST_END],
            {},
        )
        execute_query_mock.assert_called_once_with(
            TEST_DESCRIBE_DETAIL_UPDATE_QUERY,
        )
        build_mock.assert_called_once()

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_entity_events_via_describe_detail_fails_for_nondelta(
        self, execute_query_mock: Mock
    ) -> None:
        with pytest.raises(InvalidSourceTypeException):
            execute_query_mock.return_value = ["", "", "", "", "", "", None]
            self.databricks_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.INFORMATION_SCHEMA_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    @patch.object(DatabricksSource, "_build_file_last_updated_results")
    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_entity_events_file_metadata_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.FILE_METADATA_UPDATE,
            [TEST_START, TEST_END],
            {},
        )
        execute_query_mock.assert_called_once_with(
            TEST_FILE_METADATA_UPDATE_QUERY,
        )
        build_mock.assert_called_once()

    @patch.object(DatabricksSource, "_build_field_update_results")
    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_entity_events_field_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.databricks_source.get_entity_events(
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

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.databricks_source.get_current_high_watermark_for_column(
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

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.databricks_source.get_current_high_watermark_for_column(
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

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_state(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = []
        (
            field_value,
            row_count,
        ) = self.databricks_source.get_current_high_watermark_for_column(
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
        self.databricks_source._execute_fetchall_query(query)
        self.databricks_connection_mock.get_client().cursor().execute.assert_has_calls(
            [call(query)]
        )

    def test_execute_fetchone_query(self) -> None:
        query = "SELECT * FROM TABLE;"
        self.databricks_source._execute_fetchone_query(query)
        self.databricks_connection_mock.get_client().cursor().execute.assert_has_calls(
            [call(query)]
        )

    def test_get_entity_events_field_update_bad_column_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.databricks_source.get_entity_events(
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
            self.databricks_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_entity_events_unsupported_entity_type(self) -> None:
        with pytest.raises(InvalidSourceTypeException):
            self.databricks_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_current_high_watermark_for_column_invalid_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.databricks_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {
                    "path": "timestamp",
                    "type": "STRING",
                    "native_type": "STRING",
                    "kind": FreshnessFieldKind.HIGH_WATERMARK,
                    "database": AssertionDatabaseParams(
                        qualified_name=TEST_QUALIFIED_NAME, table_name=TEST_TABLE_NAME
                    ),
                },
                None,
            )

    def test_get_current_high_watermark_for_column_missing_inputs(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.databricks_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {
                    "database": AssertionDatabaseParams(
                        qualified_name=TEST_QUALIFIED_NAME, table_name=TEST_TABLE_NAME
                    )
                },
                None,
            )

    def test_get_current_high_watermark_for_column_unsupported_entity_type(
        self,
    ) -> None:
        with pytest.raises(InvalidSourceTypeException):
            self.databricks_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {
                    "database": AssertionDatabaseParams(
                        qualified_name=TEST_QUALIFIED_NAME, table_name=TEST_TABLE_NAME
                    )
                },
                None,
            )

    def test_get_operation_types_filter(self) -> None:
        operational_type_filter = self.databricks_source._get_operation_types_filter(
            ["MERGE", "UPDATE"]
        )
        assert operational_type_filter == 'operation IN ("MERGE","UPDATE")'

    def test_get_operation_types_filter_empty(self) -> None:
        operational_type_filter = self.databricks_source._get_operation_types_filter([])
        assert operational_type_filter is None

    def test_get_operation_types_filter_none(self) -> None:
        operational_type_filter = self.databricks_source._get_operation_types_filter(
            None
        )
        assert operational_type_filter is None

    def test_get_user_name_filter(self) -> None:
        name_filter = self.databricks_source._get_user_name_filter("TestUser")
        assert name_filter == 'userName = "TestUser"'

    def test_get_user_name_filter_empty(self) -> None:
        name_filter = self.databricks_source._get_user_name_filter("")
        assert name_filter is None

    def test_get_user_name_filter_none(self) -> None:
        name_filter = self.databricks_source._get_user_name_filter(None)
        assert name_filter is None

    def test_get_operational_params(self) -> None:
        operational_params = self.databricks_source._get_operation_params(
            TEST_ENTITY_URN, [TEST_START, TEST_END], {}
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "hive_metastore"
        assert operational_params.schema == "default"
        assert operational_params.table == "base_table"

    def test_get_operational_params_platform_case(self) -> None:
        urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,hive_metastore.default.base_table.extra,PROD)"
        operational_params = self.databricks_source._get_operation_params(
            urn, [TEST_START, TEST_END], {}
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "hive_metastore"
        assert operational_params.schema == "default"
        assert operational_params.table == "base_table"

    def test_get_operational_params_qualified_name(self) -> None:
        operational_params = self.databricks_source._get_operation_params(
            TEST_ENTITY_URN,
            [TEST_START, TEST_END],
            {
                "database": AssertionDatabaseParams(
                    qualified_name="hive_metastore.default.camelCasedTableName",
                    table_name=None,
                )
            },
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "hive_metastore"
        assert operational_params.schema == "default"
        assert operational_params.table == "camelCasedTableName"

    def test_get_operational_params_table_name(self) -> None:
        operational_params = self.databricks_source._get_operation_params(
            TEST_ENTITY_URN,
            [TEST_START, TEST_END],
            {
                "database": AssertionDatabaseParams(
                    qualified_name="hive_metastore.default.camelCasedTableName",
                    table_name="TitleCasedTableName",
                )
            },
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "hive_metastore"
        assert operational_params.schema == "default"
        assert operational_params.table == "TitleCasedTableName"

    def test_build_audit_log_results(self) -> None:
        results = list(
            self.databricks_source._build_audit_log_results(
                [[ts_millis_to_datetime(JAN_1_TIMESTAMP)]]
            )
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.AUDIT_LOG_OPERATION

    def test_build_describe_detail_results(self) -> None:
        operational_params = self.databricks_source._get_operation_params(
            TEST_ENTITY_URN,
            [TEST_START, TEST_END],
            {
                "database": AssertionDatabaseParams(
                    qualified_name="hive_metastore.default.camelCasedTableName",
                    table_name="TitleCasedTableName",
                )
            },
        )
        results = list(
            self.databricks_source._build_describe_detail_results(
                ["", "", "", "", "", "", ts_millis_to_datetime(TEST_END)],
                operation_params=operational_params,
            )
        )
        assert len(results) == 1
        assert results[0].event_time == TEST_END
        assert results[0].event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE

    def test_build_file_last_updated_results(self) -> None:
        results = list(
            self.databricks_source._build_file_last_updated_results(
                [ts_millis_to_datetime(TEST_END)],
            )
        )
        assert len(results) == 1
        assert results[0].event_time == TEST_END
        assert results[0].event_type == EntityEventType.FILE_METADATA_UPDATE

    def test_build_field_update_results_date(self) -> None:
        results = self.databricks_source._build_field_update_results([JAN_1_DATE])
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.FIELD_UPDATE

    def test_build_field_update_results_datetime(self) -> None:
        results = self.databricks_source._build_field_update_results([JAN_1_DATETIME])
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.FIELD_UPDATE

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_num_rows_via_count(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [10]
        db_params = DatabaseParams(
            dataset_part_0="hive_metastore",
            dataset_part_1="default",
            dataset_part_2="base_table",
        )
        result = self.databricks_source._get_num_rows_via_count(db_params, "")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_QUERY,
        )
        assert result == 10

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_num_rows_via_count_no_rows(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = []
        db_params = DatabaseParams(
            dataset_part_0="hive_metastore",
            dataset_part_1="default",
            dataset_part_2="base_table",
        )
        result = self.databricks_source._get_num_rows_via_count(db_params, "")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_QUERY,
        )
        assert result is None

    @patch.object(DatabricksSource, "_get_num_rows_via_count")
    def test_volume_assertion_retries(self, _get_num_rows_via_count_mock: Mock) -> None:
        _get_num_rows_via_count_mock.side_effect = [Exception, 10]
        count = self.databricks_source.get_row_count(
            TEST_ENTITY_URN,
            AssertionDatabaseParams(
                qualified_name=TEST_QUALIFIED_NAME, table_name=TEST_TABLE_NAME
            ),
            DatasetVolumeAssertionParameters(source_type=DatasetVolumeSourceType.QUERY),
            None,
        )

        assert count == 10

    @patch.object(DatabricksSource, "_get_num_rows_via_count")
    def test_volume_assertion_beyond_max_retries(
        self, _get_num_rows_via_count_mock: Mock
    ) -> None:
        _get_num_rows_via_count_mock.side_effect = [Exception, Exception, Exception, 10]
        with pytest.raises(Exception):
            self.databricks_source.get_row_count(
                TEST_ENTITY_URN,
                AssertionDatabaseParams(
                    qualified_name=TEST_QUALIFIED_NAME, table_name=TEST_TABLE_NAME
                ),
                DatasetVolumeAssertionParameters(
                    source_type=DatasetVolumeSourceType.QUERY
                ),
                None,
            )

    @patch.object(DatabricksSource, "_execute_fetchone_query")
    def test_get_num_rows_via_count_with_filter(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [10]
        db_params = DatabaseParams(
            dataset_part_0="hive_metastore",
            dataset_part_1="default",
            dataset_part_2="base_table",
        )
        result = self.databricks_source._get_num_rows_via_count(
            db_params, "foo = 'bar'"
        )
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY,
        )
        assert result == 10

    def test_source_connection_failure_bubbles_up(self) -> None:
        self.databricks_connection_mock.get_client.side_effect = (
            SourceConnectionErrorException(
                message="Unable to connect to databricks instance.",
                connection_urn=DATABRICKS_PLATFORM_URN,
            )
        )

        with pytest.raises(SourceConnectionErrorException):
            self.databricks_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

        with pytest.raises(SourceConnectionErrorException):
            self.databricks_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.INFORMATION_SCHEMA_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_single_value_failure_multiple_rows(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[10], [11]]

        with pytest.raises(CustomSQLErrorException):
            self.databricks_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_single_value_failure_multiple_values(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[10, 11]]

        with pytest.raises(CustomSQLErrorException):
            self.databricks_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_single_value_failure_invalid_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [["not a float"]]

        with pytest.raises(CustomSQLErrorException):
            self.databricks_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(DatabricksSource, "_execute_fetchall_query")
    def test_get_single_value_success(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["100"]]

        value = self.databricks_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)
        assert value == 100.0
