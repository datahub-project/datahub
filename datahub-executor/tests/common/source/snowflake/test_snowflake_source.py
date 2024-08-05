import datetime
from unittest.mock import Mock, call, patch

import pytest

from datahub_executor.common.assertion.types import AssertionDatabaseParams
from datahub_executor.common.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_executor.common.exceptions import (
    CustomSQLErrorException,
    InvalidParametersException,
    InvalidSourceTypeException,
)
from datahub_executor.common.source.snowflake.snowflake import SnowflakeSource
from datahub_executor.common.source.snowflake.types import (
    DEFAULT_OPERATION_TYPES_FILTER,
)
from datahub_executor.common.source.types import DatabaseParams
from datahub_executor.common.types import (
    DatasetFilterType,
    EntityEventType,
    FreshnessFieldKind,
)

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)
TEST_QUALIFIED_NAME = "test_db.public.test_table"
TEST_TABLE_NAME = "test_table"
TEST_START = 1687643700064
TEST_END = 1687644000064
JAN_1_DATE = datetime.date(2023, 1, 1)
JAN_1_DATETIME = datetime.datetime(2023, 1, 1, 0, 0).replace(
    tzinfo=datetime.timezone.utc
)
JAN_1_TIMESTAMP = 1672531200000

TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER = f"""
            WITH exploded_access_history AS (
            SELECT 
                access_history.query_id as query_id,
                access_history.user_name as user_name,
                access_history.query_start_time as query_start_time,
                updated_objects.value as updated_objects
            FROM 
                snowflake.account_usage.access_history access_history,
                LATERAL FLATTEN(input => access_history.objects_modified) updated_objects
            WHERE access_history.query_start_time >= to_timestamp_ltz({TEST_START}, 3)
                AND access_history.query_start_time < to_timestamp_ltz({TEST_END}, 3)
                
            )

            SELECT
                query_history.query_text AS "QUERY_TEXT",
                query_history.query_type AS "OPERATION_TYPE",
                query_history.rows_inserted AS "ROWS_INSERTED",
                query_history.rows_updated AS "ROWS_UPDATED",
                query_history.rows_deleted AS "ROWS_DELETED",
                exploded_access_history.user_name AS "USER_NAME",
                (DATE_PART('EPOCH', exploded_access_history.query_start_time) * 1000) AS "QUERY_START_MS",
                exploded_access_history.updated_objects:objectName::STRING AS "MODIFIED_OBJECT"
            FROM
                exploded_access_history as exploded_access_history
            INNER JOIN
                (SELECT * FROM snowflake.account_usage.query_history 
                WHERE query_history.start_time >= to_timestamp_ltz({TEST_START}, 3)
                    AND query_history.start_time < to_timestamp_ltz({TEST_END}, 3)
                    AND (query_history.rows_produced > 0 OR query_history.rows_inserted > 0 OR query_history.rows_updated > 0 OR query_history.rows_deleted > 0)
                    AND query_history.query_type in ({DEFAULT_OPERATION_TYPES_FILTER})) query_history
                ON exploded_access_history.query_id = query_history.query_id
            WHERE                
                REGEXP_REPLACE(LOWER(exploded_access_history.updated_objects:objectName::STRING), '\\"|\\'', '') in ('test_db.public.test_table')
            ORDER BY query_history.start_time DESC
            LIMIT 5
;"""
TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER = f"""
            WITH exploded_access_history AS (
            SELECT 
                access_history.query_id as query_id,
                access_history.user_name as user_name,
                access_history.query_start_time as query_start_time,
                updated_objects.value as updated_objects
            FROM 
                snowflake.account_usage.access_history access_history,
                LATERAL FLATTEN(input => access_history.objects_modified) updated_objects
            WHERE access_history.query_start_time >= to_timestamp_ltz({TEST_START}, 3)
                AND access_history.query_start_time < to_timestamp_ltz({TEST_END}, 3)
                AND LOWER(access_history.user_name) = 'testusername'
            )

            SELECT
                query_history.query_text AS "QUERY_TEXT",
                query_history.query_type AS "OPERATION_TYPE",
                query_history.rows_inserted AS "ROWS_INSERTED",
                query_history.rows_updated AS "ROWS_UPDATED",
                query_history.rows_deleted AS "ROWS_DELETED",
                exploded_access_history.user_name AS "USER_NAME",
                (DATE_PART('EPOCH', exploded_access_history.query_start_time) * 1000) AS "QUERY_START_MS",
                exploded_access_history.updated_objects:objectName::STRING AS "MODIFIED_OBJECT"
            FROM
                exploded_access_history as exploded_access_history
            INNER JOIN
                (SELECT * FROM snowflake.account_usage.query_history 
                WHERE query_history.start_time >= to_timestamp_ltz({TEST_START}, 3)
                    AND query_history.start_time < to_timestamp_ltz({TEST_END}, 3)
                    AND (query_history.rows_produced > 0 OR query_history.rows_inserted > 0 OR query_history.rows_updated > 0 OR query_history.rows_deleted > 0)
                    AND query_history.query_type in ({DEFAULT_OPERATION_TYPES_FILTER})) query_history
                ON exploded_access_history.query_id = query_history.query_id
            WHERE                
                REGEXP_REPLACE(LOWER(exploded_access_history.updated_objects:objectName::STRING), '\\"|\\'', '') in ('test_db.public.test_table')
            ORDER BY query_history.start_time DESC
            LIMIT 5
;"""
TEST_AUDIT_LOG_QUERY_OPERATIONAL_TYPE_FILTER = f"""
            WITH exploded_access_history AS (
            SELECT 
                access_history.query_id as query_id,
                access_history.user_name as user_name,
                access_history.query_start_time as query_start_time,
                updated_objects.value as updated_objects
            FROM 
                snowflake.account_usage.access_history access_history,
                LATERAL FLATTEN(input => access_history.objects_modified) updated_objects
            WHERE access_history.query_start_time >= to_timestamp_ltz({TEST_START}, 3)
                AND access_history.query_start_time < to_timestamp_ltz({TEST_END}, 3)
                
            )

            SELECT
                query_history.query_text AS "QUERY_TEXT",
                query_history.query_type AS "OPERATION_TYPE",
                query_history.rows_inserted AS "ROWS_INSERTED",
                query_history.rows_updated AS "ROWS_UPDATED",
                query_history.rows_deleted AS "ROWS_DELETED",
                exploded_access_history.user_name AS "USER_NAME",
                (DATE_PART('EPOCH', exploded_access_history.query_start_time) * 1000) AS "QUERY_START_MS",
                exploded_access_history.updated_objects:objectName::STRING AS "MODIFIED_OBJECT"
            FROM
                exploded_access_history as exploded_access_history
            INNER JOIN
                (SELECT * FROM snowflake.account_usage.query_history 
                WHERE query_history.start_time >= to_timestamp_ltz({TEST_START}, 3)
                    AND query_history.start_time < to_timestamp_ltz({TEST_END}, 3)
                    AND (query_history.rows_produced > 0 OR query_history.rows_inserted > 0 OR query_history.rows_updated > 0 OR query_history.rows_deleted > 0)
                    AND query_history.query_type in ('INSERT','UPDATE')) query_history
                ON exploded_access_history.query_id = query_history.query_id
            WHERE                
                REGEXP_REPLACE(LOWER(exploded_access_history.updated_objects:objectName::STRING), '\\"|\\'', '') in ('test_db.public.test_table')
            ORDER BY query_history.start_time DESC
            LIMIT 5
;"""
TEST_INFORMATION_SCHEMA_UPDATE_QUERY = f"""
            SELECT table_name, table_type, (DATE_PART('EPOCH', last_altered) * 1000) as last_altered
            FROM TEST_DB.information_schema.tables
            WHERE last_altered >= to_timestamp_ltz({TEST_START}, 3)
            AND last_altered < to_timestamp_ltz({TEST_END}, 3)
            AND table_name = 'test_table'
            AND table_schema = 'PUBLIC'
            AND table_catalog = 'TEST_DB'
            LIMIT 5;"""
TEST_FIELD_UPDATE_QUERY = f"""
                SELECT timestamp as last_altered_date
                FROM test_db.public."test_table"
                WHERE timestamp >= (TO_TIMESTAMP({TEST_START}, 3))
                AND timestamp <= (TO_TIMESTAMP({TEST_END}, 3))
                AND foo = 'bar'
                ORDER BY timestamp DESC
                LIMIT 5
                ;
            """
TEST_HIGHWATERMARK_VALUE_QUERY = f"""
        SELECT timestamp
        FROM test_db.public."test_table"
        WHERE timestamp >= TO_TIMESTAMP('{TEST_START}')
        AND foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_VALUE_NO_PREV_QUERY = """
        SELECT timestamp
        FROM test_db.public."test_table"
        
        WHERE foo = 'bar'
        ORDER by timestamp DESC
        LIMIT 1;
    """
TEST_HIGHWATERMARK_COUNT_QUERY = f"""
        SELECT COUNT(*)
        FROM test_db.public."test_table"
        WHERE timestamp = TO_TIMESTAMP('{TEST_END}')
        AND foo = 'bar'
    """
TEST_GET_ROW_COUNT_QUERY = """
            SELECT row_count
            FROM TEST_DB.information_schema.tables
            WHERE table_name = 'test_table'
            AND table_schema = 'PUBLIC'
            AND table_catalog = 'TEST_DB';"""
TEST_NUM_ROWS_VIA_COUNT_QUERY = """
        SELECT COUNT(*)
        FROM test_db.public."test_table"
        
    """
TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY = """
        SELECT COUNT(*)
        FROM test_db.public."test_table"
        WHERE foo = 'bar'
    """
TEST_CUSTOM_SQL_STATEMENT = "SELECT SUM(num_items) FROM test_db.public.test_table;"


class TestSnowflakeSource:
    def setup_method(self) -> None:
        self.snowflake_connection_mock = Mock(spec=SnowflakeConnection)
        self.snowflake_source = SnowflakeSource(self.snowflake_connection_mock)

    @patch.object(SnowflakeSource, "_build_audit_log_results")
    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_no_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.snowflake_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": None, "user_name": None},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(SnowflakeSource, "_build_audit_log_results")
    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.snowflake_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": None, "user_name": "TestUserName"},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(SnowflakeSource, "_build_audit_log_results")
    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_operational_type_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.snowflake_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": ["INSERT", "UPDATE"], "user_name": None},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_OPERATIONAL_TYPE_FILTER,
        )
        build_mock.assert_called_once()

    @patch.object(SnowflakeSource, "_build_information_schema_results")
    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_entity_events_information_schema_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.snowflake_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.INFORMATION_SCHEMA_UPDATE,
            [TEST_START, TEST_END],
            {},
        )
        execute_query_mock.assert_called_once_with(
            TEST_INFORMATION_SCHEMA_UPDATE_QUERY,
        )
        build_mock.assert_called_once()

    @patch.object(SnowflakeSource, "_build_field_update_results")
    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_entity_events_field_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.snowflake_source.get_entity_events(
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

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.snowflake_source.get_current_high_watermark_for_column(
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

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.snowflake_source.get_current_high_watermark_for_column(
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

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_state(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = []
        (
            field_value,
            row_count,
        ) = self.snowflake_source.get_current_high_watermark_for_column(
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
        self.snowflake_source._execute_fetchall_query(query)
        self.snowflake_connection_mock.get_client().cursor().execute.assert_has_calls(
            [
                call(
                    "ALTER SESSION SET TIMEZONE = 'UTC', STATEMENT_TIMEOUT_IN_SECONDS = 600;"
                ),
                call(query),
            ]
        )

    def test_execute_fetchone_query(self) -> None:
        query = "SELECT * FROM TABLE;"
        self.snowflake_connection_mock.get_client().cursor().fetchone.return_value = (
            None
        )
        self.snowflake_source._execute_fetchone_query(query)
        self.snowflake_connection_mock.get_client().cursor().execute.assert_has_calls(
            [
                call(
                    "ALTER SESSION SET TIMEZONE = 'UTC', STATEMENT_TIMEOUT_IN_SECONDS = 600;"
                ),
                call(query),
            ]
        )

    def test_get_entity_events_field_update_bad_column_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.snowflake_source.get_entity_events(
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
            self.snowflake_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_entity_events_unsupported_entity_type(self) -> None:
        with pytest.raises(InvalidSourceTypeException):
            self.snowflake_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {},
            )

    def test_get_current_high_watermark_for_column_invalid_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.snowflake_source.get_current_high_watermark_for_column(
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
            self.snowflake_source.get_current_high_watermark_for_column(
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
            self.snowflake_source.get_current_high_watermark_for_column(
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
        operational_type_filter = self.snowflake_source._get_operation_types_filter(
            {"operation_types": ["INSERT", "UPDATE"]}
        )
        assert operational_type_filter == "'INSERT','UPDATE'"

    def test_get_operation_types_filter_empty(self) -> None:
        operational_type_filter = self.snowflake_source._get_operation_types_filter(
            {"operation_types": []}
        )
        assert operational_type_filter == DEFAULT_OPERATION_TYPES_FILTER

    def test_get_operation_types_filter_none(self) -> None:
        operational_type_filter = self.snowflake_source._get_operation_types_filter({})
        assert operational_type_filter == DEFAULT_OPERATION_TYPES_FILTER

    def test_get_user_name_filter(self) -> None:
        name_filter = self.snowflake_source._get_user_name_filter(
            {"user_name": "TestUser"}
        )
        assert name_filter == "testuser"

    def test_get_user_name_filter_empty(self) -> None:
        name_filter = self.snowflake_source._get_user_name_filter({"user_name": ""})
        assert name_filter == ""

    def test_get_user_name_filter_none(self) -> None:
        name_filter = self.snowflake_source._get_user_name_filter({})
        assert name_filter is None

    def test_get_operational_params(self) -> None:
        operational_params = self.snowflake_source._get_operation_params(
            TEST_ENTITY_URN, [TEST_START, TEST_END], {}
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "test_db"
        assert operational_params.schema == "public"
        assert operational_params.table == "test_table"

    def test_get_operational_params_platform_case(self) -> None:
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table.extra,PROD)"
        operational_params = self.snowflake_source._get_operation_params(
            urn, [TEST_START, TEST_END], {}
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "test_db"
        assert operational_params.schema == "public"
        assert operational_params.table == "test_table"

    def test_get_operational_params_qualified_name(self) -> None:
        operational_params = self.snowflake_source._get_operation_params(
            TEST_ENTITY_URN,
            [TEST_START, TEST_END],
            {
                "database": AssertionDatabaseParams(
                    qualified_name="test_db.public.camelCasedTableName", table_name=None
                )
            },
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "test_db"
        assert operational_params.schema == "public"
        assert operational_params.table == "camelCasedTableName"

    def test_get_operational_params_table_name(self) -> None:
        operational_params = self.snowflake_source._get_operation_params(
            TEST_ENTITY_URN,
            [TEST_START, TEST_END],
            {
                "database": AssertionDatabaseParams(
                    qualified_name="test_db.public.camelCasedTableName",
                    table_name="TitleCasedTableName",
                )
            },
        )
        assert operational_params.start_time_millis == TEST_START
        assert operational_params.end_time_millis == TEST_END
        assert operational_params.catalog == "test_db"
        assert operational_params.schema == "public"
        assert operational_params.table == "TitleCasedTableName"

    def test_build_audit_log_results(self) -> None:
        results = self.snowflake_source._build_audit_log_results(
            [["", "", "", "", "", "", JAN_1_TIMESTAMP]]
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.AUDIT_LOG_OPERATION

    def test_build_information_schema_results(self) -> None:
        results = self.snowflake_source._build_information_schema_results(
            [["", "", JAN_1_TIMESTAMP]]
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.INFORMATION_SCHEMA_UPDATE

    def test_build_field_update_results_date(self) -> None:
        results = self.snowflake_source._build_field_update_results([JAN_1_DATE])
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.FIELD_UPDATE

    def test_build_field_update_results_datetime(self) -> None:
        results = self.snowflake_source._build_field_update_results([JAN_1_DATETIME])
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.FIELD_UPDATE

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_num_rows_via_stats_table(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [10]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2=TEST_TABLE_NAME,
        )
        result = self.snowflake_source._get_num_rows_via_stats_table(db_params)
        execute_query_mock.assert_called_once_with(
            TEST_GET_ROW_COUNT_QUERY,
        )
        assert result == 10

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_num_rows_via_count(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [10]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.snowflake_source._get_num_rows_via_count(db_params, "")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_QUERY,
        )
        assert result == 10

    @patch.object(SnowflakeSource, "_execute_fetchone_query")
    def test_get_num_rows_via_count_with_filter(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [10]
        db_params = DatabaseParams(
            dataset_part_0="test_db",
            dataset_part_1="public",
            dataset_part_2="test_table",
        )
        result = self.snowflake_source._get_num_rows_via_count(db_params, "foo = 'bar'")
        execute_query_mock.assert_called_once_with(
            TEST_NUM_ROWS_VIA_COUNT_WITH_FILTER_QUERY,
        )
        assert result == 10

    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_single_value_failure_multiple_rows(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[10], [11]]

        with pytest.raises(CustomSQLErrorException):
            self.snowflake_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_single_value_failure_multiple_values(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [[10, 11]]

        with pytest.raises(CustomSQLErrorException):
            self.snowflake_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_single_value_failure_invalid_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [["not a float"]]

        with pytest.raises(CustomSQLErrorException):
            self.snowflake_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)

    @patch.object(SnowflakeSource, "_execute_fetchall_query")
    def test_get_single_value_success(self, execute_query_mock: Mock) -> None:
        execute_query_mock.return_value = [["100"]]

        value = self.snowflake_source._execute_custom_sql(TEST_CUSTOM_SQL_STATEMENT)
        assert value == 100.0
