import datetime
from unittest.mock import Mock, call, patch

import pytest

from datahub_monitors.connection.redshift.redshift_connection import RedshiftConnection
from datahub_monitors.exceptions import (
    InvalidParametersException,
    InvalidSourceTypeException,
)
from datahub_monitors.source.redshift.redshift import RedshiftSource
from datahub_monitors.types import (
    DatasetFilterType,
    EntityEventType,
    FreshnessFieldKind,
)

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:redshift,test_db.public.test_table,PROD)"
)
TEST_START = 1687643700064
TEST_END = 1687644000064
JAN_1_DATE = datetime.date(2023, 1, 1)
JAN_1_DATETIME = datetime.datetime(2023, 1, 1, 0, 0).replace(
    tzinfo=datetime.timezone.utc
)
JAN_1_TIMESTAMP = 1672531200000

TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER = f"""
            SELECT
                sq.querytxt AS query,
                sui.usename AS username,
                si.endtime AS endtime
            FROM stl_insert si
                JOIN svv_table_info sti ON si.tbl = sti.table_id
                JOIN stl_query sq ON si.query = sq.query
                JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE si.endtime >= (TIMESTAMP 'epoch' + {TEST_START}/1000 * interval '1 second')
                AND si.endtime < (TIMESTAMP 'epoch' + {TEST_END}/1000 * interval '1 second')
                AND sq.startTime >= (TIMESTAMP 'epoch' + {TEST_START}/1000 * interval '1 second')
                AND sq.endtime < (TIMESTAMP 'epoch' + {TEST_END}/1000 * interval '1 second')
                AND sq.aborted = 0
                AND si.rows > 0
                AND sti.database = 'test_db'
                AND sti.schema = 'public'
                AND sti.table = 'test_table'
                
            ORDER BY endtime DESC;
        """
TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER = f"""
            SELECT
                sq.querytxt AS query,
                sui.usename AS username,
                si.endtime AS endtime
            FROM stl_insert si
                JOIN svv_table_info sti ON si.tbl = sti.table_id
                JOIN stl_query sq ON si.query = sq.query
                JOIN svl_user_info sui ON sq.userid = sui.usesysid
            WHERE si.endtime >= (TIMESTAMP 'epoch' + {TEST_START}/1000 * interval '1 second')
                AND si.endtime < (TIMESTAMP 'epoch' + {TEST_END}/1000 * interval '1 second')
                AND sq.startTime >= (TIMESTAMP 'epoch' + {TEST_START}/1000 * interval '1 second')
                AND sq.endtime < (TIMESTAMP 'epoch' + {TEST_END}/1000 * interval '1 second')
                AND sq.aborted = 0
                AND si.rows > 0
                AND sti.database = 'test_db'
                AND sti.schema = 'public'
                AND sti.table = 'test_table'
                AND sui.usename = 'testusername'
            ORDER BY endtime DESC;
        """
TEST_FIELD_UPDATE_QUERY = f"""
                SELECT timestamp as last_altered_date
                FROM test_db.public.test_table
                WHERE timestamp >= (TIMESTAMP 'epoch' + {TEST_START / 1000} * INTERVAL '1 second')
                AND timestamp <= (TIMESTAMP 'epoch' + {TEST_END / 1000} * INTERVAL '1 second')
                AND foo = 'bar'
                ORDER BY timestamp DESC
                ;
            """
TEST_HIGHWATERMARK_VALUE_QUERY = f"""
        SELECT timestamp
        FROM test_db.public.test_table
        WHERE timestamp >= '{TEST_START}'
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
        WHERE timestamp = '{TEST_END}'
        AND foo = 'bar'
    """


class TestRedshiftSource:
    def setup_method(self) -> None:
        self.redshift_connection_mock = Mock(spec=RedshiftConnection)
        self.redshift_source = RedshiftSource(self.redshift_connection_mock)

    @patch.object(RedshiftSource, "_build_audit_log_results")
    @patch.object(RedshiftSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_no_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.redshift_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": ["INSERT"], "user_name": None},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_NO_USER_NAME_FILTER
        )
        build_mock.assert_called_once()

    @patch.object(RedshiftSource, "_build_audit_log_results")
    @patch.object(RedshiftSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_user_name_filter(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.redshift_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": ["INSERT"], "user_name": "TestUserName"},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER
        )
        build_mock.assert_called_once()

    @patch.object(RedshiftSource, "_build_audit_log_results")
    @patch.object(RedshiftSource, "_execute_fetchall_query")
    def test_get_entity_events_audit_log_with_no_operation_types(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.redshift_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {"operation_types": None, "user_name": "TestUserName"},
        )
        execute_query_mock.assert_called_once_with(
            TEST_AUDIT_LOG_QUERY_WITH_USER_NAME_FILTER
        )
        build_mock.assert_called_once()

    @patch.object(RedshiftSource, "_execute_fetchall_query")
    def test_get_entity_events_information_schema_update(
        self, execute_query_mock: Mock
    ) -> None:
        self.redshift_source.get_entity_events(
            TEST_ENTITY_URN,
            EntityEventType.INFORMATION_SCHEMA_UPDATE,
            [TEST_START, TEST_END],
            {},
        )
        execute_query_mock.assert_not_called()

    @patch.object(RedshiftSource, "_build_field_update_results")
    @patch.object(RedshiftSource, "_execute_fetchall_query")
    def test_get_entity_events_field_update(
        self, execute_query_mock: Mock, build_mock: Mock
    ) -> None:
        self.redshift_source.get_entity_events(
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

    @patch.object(RedshiftSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.redshift_source.get_current_high_watermark_for_column(
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

    @patch.object(RedshiftSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_value(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = [TEST_END]
        (
            field_value,
            row_count,
        ) = self.redshift_source.get_current_high_watermark_for_column(
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

    @patch.object(RedshiftSource, "_execute_fetchone_query")
    def test_get_current_high_watermark_for_column_no_previous_state(
        self, execute_query_mock: Mock
    ) -> None:
        execute_query_mock.return_value = []
        (
            field_value,
            row_count,
        ) = self.redshift_source.get_current_high_watermark_for_column(
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

    def test_get_entity_events_audit_log_bad_operation_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.redshift_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.AUDIT_LOG_OPERATION,
                [TEST_START, TEST_END],
                {"operation_types": ["DELETE"], "user_name": "TestUserName"},
            )

    def test_get_entity_events_field_update_bad_column_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.redshift_source.get_entity_events(
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
            self.redshift_source.get_entity_events(
                TEST_ENTITY_URN,
                EntityEventType.FIELD_UPDATE,
                [TEST_START, TEST_END],
                {},
            )

    def test_execute_fetchall_query(self) -> None:
        query = "SELECT * FROM TABLE;"
        self.redshift_source._execute_fetchall_query(query)
        self.redshift_connection_mock.get_client().cursor().execute.assert_called_once_with(
            query
        )

    def test_execute_fetchone_query(self) -> None:
        query = "SELECT * FROM TABLE;"
        self.redshift_source._execute_fetchone_query(query)
        self.redshift_connection_mock.get_client().cursor().execute.assert_called_once_with(
            query
        )

    def test_build_audit_log_results(self) -> None:
        results = self.redshift_source._build_audit_log_results(
            [["", "", JAN_1_DATETIME]]
        )
        assert len(results) == 1
        assert results[0].event_time == JAN_1_TIMESTAMP
        assert results[0].event_type == EntityEventType.AUDIT_LOG_OPERATION

    def test_get_current_high_watermark_for_column_invalid_type(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.redshift_source.get_current_high_watermark_for_column(
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
            self.redshift_source.get_current_high_watermark_for_column(
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
            self.redshift_source.get_current_high_watermark_for_column(
                TEST_ENTITY_URN,
                EntityEventType.DATA_JOB_RUN_COMPLETED_SUCCESS,
                [TEST_START, TEST_END],
                {},
                None,
            )
