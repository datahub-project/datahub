from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from datahub.configuration.time_window_config import BucketDuration
from datahub.utilities.str_enum import StrEnum

# Databases MSSQL always excludes from enumeration, independent of
# database_pattern -- SQL Server's own system databases plus the reporting
# services pair. Single source of truth for MSSQLQuery.list_databases()
# below and the agent probe (SQLServerConfig.default_databases()).
MSSQL_SYSTEM_DATABASES = (
    "master",
    "model",
    "msdb",
    "tempdb",
    "Resource",
    "distribution",
    "reportserver",
    "reportservertempdb",
)
_SYSTEM_DATABASE_EXCLUSION = ", ".join(f"'{name}'" for name in MSSQL_SYSTEM_DATABASES)

QueryParams = Dict[str, Union[int, str, datetime]]

# BucketDuration -> T-SQL datepart, from a closed enum, so it is safe to format
# into the SQL text.
_BUCKET_DATEPART: Dict[BucketDuration, str] = {
    BucketDuration.DAY: "DAY",
    BucketDuration.HOUR: "HOUR",
}


class QueryHistorySource(StrEnum):
    """Where query history is read from."""

    # Query Store, falling back to the plan-cache DMVs. No user attribution.
    QUERY_STORE = "query_store"
    # SQL Server Audit / Azure SQL Auditing with BATCH_COMPLETED_GROUP.
    AUDIT_LOG = "audit_log"
    # An Extended Events session capturing sql_batch_completed / rpc_completed.
    EXTENDED_EVENTS = "extended_events"


# Audit action ids for BATCH_COMPLETED_GROUP: batch completed, RPC completed.
AUDIT_BATCH_COMPLETED_ACTION_IDS = ("BCM", "RCM")
XE_COMPLETED_EVENTS = ("sql_batch_completed", "rpc_completed")
# SERVERPROPERTY('EngineEdition') for Azure SQL Database, where audit and
# Extended Events catalog views are database-scoped or absent, and for Managed
# Instance. Both report an old ProductMajorVersion but support everything the
# log readers use.
AZURE_SQL_DATABASE_ENGINE_EDITION = 5
AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION = 8
# STRING_AGG and fn_xe_file_target_read_file's timestamp_utc need SQL Server 2017.
QUERY_LOG_MIN_MAJOR_VERSION = 14
# Azure SQL Auditing truncates statements to this many characters.
AUDIT_STATEMENT_MAX_CHARS = 4000
# RPCs worth keeping: driver wrappers around parameterized SQL. Other RPCs
# (sp_reset_connection, sp_unprepare, sp_cursorfetch, procedure calls, ...)
# never reference tables and would only crowd out real queries in the top-N.
RPC_SQL_WRAPPER_PROCEDURES = (
    "sp_executesql",
    "sp_prepexec",
    "sp_prepare",
    "sp_cursorprepexec",
    "sp_cursorprepare",
)
# `_` is a LIKE wildcard, so escape it.
_RPC_WRAPPER_FILTER = (
    "("
    + " OR ".join(
        f"statement LIKE '%{name.replace('_', '[_]')}%'"
        for name in RPC_SQL_WRAPPER_PROCEDURES
    )
    + ")"
)


@dataclass(frozen=True)
class QueryHistoryWindow:
    """Time window for query history; start/end are naive UTC for SQL params."""

    start_time: datetime
    end_time: datetime
    bucket_duration: BucketDuration = BucketDuration.DAY


# Catalog schemas and system table-valued functions that show up in query
# history (often from DataHub's own metadata queries, e.g. SQLAlchemy calls
# fn_listextendedproperty unqualified). They are not datasets, so queries that
# only touch them must not become Query entities or lineage.
MSSQL_SYSTEM_SCHEMAS = frozenset({"sys", "information_schema"})
MSSQL_SYSTEM_TABLE_VALUED_FUNCTIONS = frozenset(
    {
        "fn_builtin_permissions",
        "fn_dblog",
        "fn_get_audit_file",
        "fn_helpcollations",
        "fn_listextendedproperty",
        "fn_my_permissions",
        "fn_trace_gettable",
        "fn_virtualfilestats",
    }
)
_SYSTEM_DATABASES_LOWER = frozenset(name.lower() for name in MSSQL_SYSTEM_DATABASES)


def is_mssql_system_object(name: str) -> bool:
    """True for a `db.schema.object` name that refers to a SQL Server system object."""
    parts = name.lower().split(".")
    if parts[-1] in MSSQL_SYSTEM_TABLE_VALUED_FUNCTIONS:
        return True
    if len(parts) >= 2 and parts[-2] in MSSQL_SYSTEM_SCHEMAS:
        return True
    return len(parts) >= 3 and parts[-3] in _SYSTEM_DATABASES_LOWER


class MSSQLQuery:
    """SQL queries for extracting query history from MS SQL Server."""

    @staticmethod
    def _build_exclude_clause(
        exclude_patterns: Optional[List[str]], column_expr: str
    ) -> str:
        """Build SQL WHERE clause for excluding patterns."""
        if not exclude_patterns:
            return ""

        conditions = []
        for i in range(len(exclude_patterns)):
            condition = f"{column_expr} NOT LIKE :exclude_{i}"
            conditions.append(condition)

        return "AND " + " AND ".join(conditions)

    @staticmethod
    def _build_exclude_params(
        exclude_patterns: Optional[List[str]],
        base_params: QueryParams,
    ) -> QueryParams:
        """Build parameter dict with exclude patterns."""
        params = base_params.copy()
        if exclude_patterns:
            for i, pattern in enumerate(exclude_patterns):
                key = f"exclude_{i}"
                params[key] = pattern
        return params

    @staticmethod
    def _finalize_query(
        query_template: str,
        exclude_patterns: Optional[List[str]],
        limit: int,
        min_calls: int,
        window: QueryHistoryWindow,
    ) -> Tuple[TextClause, QueryParams]:
        """Finalize query by building params and wrapping in TextClause."""
        params = MSSQLQuery._build_exclude_params(
            exclude_patterns,
            {
                "limit": limit,
                "min_calls": min_calls,
                "start_time": window.start_time,
                "end_time": window.end_time,
            },
        )
        return text(query_template), params

    @staticmethod
    def check_query_store_enabled() -> TextClause:
        """Check if Query Store is enabled for the current database."""
        return text("""
            SELECT 
                CASE 
                    WHEN actual_state_desc IN ('READ_WRITE', 'READ_ONLY') THEN 1
                    ELSE 0
                END AS is_enabled
            FROM sys.database_query_store_options
        """)

    @staticmethod
    def check_dmv_permissions() -> TextClause:
        """Check if user has VIEW SERVER STATE permission for DMVs."""
        return text("""
            SELECT 
                HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS has_view_server_state
        """)

    @staticmethod
    def _validate_query_params(limit: int, min_calls: int) -> None:
        """Validate query history parameters."""
        if limit <= 0:
            raise ValueError(f"limit must be positive, got: {limit}")
        if min_calls < 0:
            raise ValueError(f"min_calls must be non-negative, got: {min_calls}")

    @staticmethod
    def _get_query_history_base(
        exclude_column_expr: str,
        query_template_builder: str,
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        """Common logic for Query Store and DMV query history extraction."""
        MSSQLQuery._validate_query_params(limit, min_calls)

        exclude_clause = MSSQLQuery._build_exclude_clause(
            exclude_patterns, exclude_column_expr
        )

        query = query_template_builder.format(
            exclude_clause=exclude_clause,
            bucket_unit=_BUCKET_DATEPART[window.bucket_duration],
        )

        return MSSQLQuery._finalize_query(
            query_template=query,
            exclude_patterns=exclude_patterns,
            limit=limit,
            min_calls=min_calls,
            window=window,
        )

    @staticmethod
    def get_query_history_from_query_store(
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        """Extract query history from Query Store (SQL Server 2016+).

        Returns one row per (query, usage bucket) with the executions from
        runtime stats intervals that start inside the window, so counts are
        per bucket rather than a lifetime total and reruns over the same
        window report the same numbers. Queries that ran in the window rank
        ahead of older ones for the TOP limit. A selected query with no
        in-window executions yields one row with NULL execution columns so
        its lineage is still kept.
        """
        query_template = """
            WITH top_queries AS (
                SELECT TOP(:limit)
                    q.query_id,
                    qt.query_sql_text,
                    SUM(rs.count_executions) AS execution_count,
                    SUM(rs.avg_duration * rs.count_executions) / 1000.0 AS total_exec_time_ms
                FROM sys.query_store_query AS q
                INNER JOIN sys.query_store_query_text AS qt
                    ON q.query_text_id = qt.query_text_id
                INNER JOIN sys.query_store_plan AS p
                    ON q.query_id = p.query_id
                INNER JOIN sys.query_store_runtime_stats AS rs
                    ON p.plan_id = rs.plan_id
                INNER JOIN sys.query_store_runtime_stats_interval AS rsi
                    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE
                    rs.count_executions >= :min_calls
                    {exclude_clause}
                    AND qt.query_sql_text IS NOT NULL
                    AND LEN(qt.query_sql_text) > 0
                GROUP BY q.query_id, qt.query_sql_text
                HAVING SUM(rs.count_executions) >= :min_calls
                ORDER BY
                    MAX(CASE
                        WHEN rsi.start_time >= :start_time AND rsi.start_time < :end_time
                        THEN 1 ELSE 0
                    END) DESC,
                    SUM(rs.avg_duration * rs.count_executions) DESC,
                    q.query_id
            )
            SELECT
                CAST(tq.query_id AS VARCHAR(50)) AS query_id,
                tq.query_sql_text AS query_text,
                tq.execution_count AS execution_count,
                tq.total_exec_time_ms AS total_exec_time_ms,
                DB_NAME() AS database_name,
                we.last_execution_time_utc AS last_execution_time_utc,
                we.window_execution_count AS window_execution_count,
                CAST(NULL AS NVARCHAR(256)) AS user_name
            FROM top_queries AS tq
            OUTER APPLY (
                SELECT
                    CAST(MAX(CONVERT(DATETIME2, rs.last_execution_time, 1)) AS DATETIME)
                        AS last_execution_time_utc,
                    SUM(rs.count_executions) AS window_execution_count
                FROM sys.query_store_plan AS p
                INNER JOIN sys.query_store_runtime_stats AS rs
                    ON p.plan_id = rs.plan_id
                INNER JOIN sys.query_store_runtime_stats_interval AS rsi
                    ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
                WHERE
                    p.query_id = tq.query_id
                    AND rsi.start_time >= :start_time
                    AND rsi.start_time < :end_time
                GROUP BY DATEADD(
                    {bucket_unit},
                    DATEDIFF({bucket_unit}, 0, CONVERT(DATETIME2, rsi.start_time, 1)),
                    0
                )
            ) AS we
            ORDER BY tq.total_exec_time_ms DESC, tq.query_id
        """

        return MSSQLQuery._get_query_history_base(
            exclude_column_expr="qt.query_sql_text",
            query_template_builder=query_template,
            window=window,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_query_history_from_dmv(
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        """Extract query history from DMVs (fallback for SQL Server 2014 or when Query Store is disabled).

        The plan cache only keeps a cumulative execution count, the plan's
        creation time and its last execution time. A query whose last
        execution falls inside the window is reported at that time with its
        full count if the plan was cached inside the window (every execution
        is then in the window), otherwise with a count of 1, so repeated runs
        don't re-add a plan's lifetime total to each new day. Queries outside
        the window get NULL execution columns.

        The database filter uses the plan's dbid attribute: sql_text.dbid is
        NULL for ad-hoc and auto-parameterized statements, which are most of
        the workload. Stats are per statement while sql_handle is per batch,
        so the id and text are scoped to the statement's offset in the batch.
        last_execution_time is server-local and shifted to UTC with the
        server's current offset.
        """
        query_template = """
            SELECT TOP(:limit)
                CONVERT(VARCHAR(130), qs.sql_handle, 1)
                    + ':' + CAST(qs.statement_start_offset AS VARCHAR(20)) AS query_id,
                stmt.statement_text AS query_text,
                qs.execution_count AS execution_count,
                qs.total_elapsed_time / 1000.0 AS total_exec_time_ms,
                DB_NAME() AS database_name,
                CASE
                    WHEN lx.in_window = 1 THEN lx.last_execution_time_utc
                END AS last_execution_time_utc,
                CASE
                    WHEN lx.in_window = 0 THEN NULL
                    WHEN lx.creation_time_utc >= :start_time THEN qs.execution_count
                    ELSE 1
                END AS window_execution_count,
                CAST(NULL AS NVARCHAR(256)) AS user_name
            FROM sys.dm_exec_query_stats AS qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
            CROSS APPLY (
                SELECT CONVERT(INT, pa.value) AS dbid
                FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
                WHERE pa.attribute = N'dbid'
            ) AS plan_db
            CROSS APPLY (
                SELECT CAST(SUBSTRING(
                    st.text,
                    qs.statement_start_offset / 2 + 1,
                    (CASE qs.statement_end_offset
                        WHEN -1 THEN DATALENGTH(st.text)
                        ELSE qs.statement_end_offset
                    END - qs.statement_start_offset) / 2 + 1
                ) AS NVARCHAR(MAX)) AS statement_text
            ) AS stmt
            CROSS APPLY (
                SELECT -DATEPART(TZOFFSET, SYSDATETIMEOFFSET()) AS utc_offset_minutes
            ) AS tz
            CROSS APPLY (
                SELECT
                    CAST(DATEADD(MINUTE, tz.utc_offset_minutes, qs.last_execution_time) AS DATETIME)
                        AS last_execution_time_utc,
                    CAST(DATEADD(MINUTE, tz.utc_offset_minutes, qs.creation_time) AS DATETIME)
                        AS creation_time_utc
            ) AS utc
            CROSS APPLY (
                SELECT
                    utc.last_execution_time_utc,
                    utc.creation_time_utc,
                    CASE
                        WHEN utc.last_execution_time_utc >= :start_time
                            AND utc.last_execution_time_utc < :end_time
                        THEN 1 ELSE 0
                    END AS in_window
            ) AS lx
            WHERE
                qs.execution_count >= :min_calls
                {exclude_clause}
                AND stmt.statement_text IS NOT NULL
                AND LEN(stmt.statement_text) > 0
                AND plan_db.dbid = DB_ID()
            ORDER BY lx.in_window DESC, qs.total_elapsed_time DESC
        """

        return MSSQLQuery._get_query_history_base(
            exclude_column_expr="stmt.statement_text",
            query_template_builder=query_template,
            window=window,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    # Shared tail of the audit and Extended Events readers. `log_rows` yields
    # (event_time, user_name, statement, is_rpc, is_truncated) for successful
    # completed statements in the current database and window. Rows are
    # grouped per (statement, user, usage bucket) and the top :limit raw
    # statements by in-window count are kept; the caller regroups them after
    # unwrapping RPC parameter values, so :limit is deliberately larger than
    # max_queries_to_extract and min_calls is applied after regrouping.
    # Statements are grouped by hash because NVARCHAR(MAX) can't be a GROUP BY
    # key. The connector's own login is excluded so DataHub's metadata queries
    # don't show up as user activity.
    _LOG_QUERY_TAIL = f"""
            filtered AS (
                SELECT *
                FROM log_rows
                WHERE
                    statement IS NOT NULL
                    AND LEN(statement) > 0
                    AND (user_name IS NULL OR user_name <> SUSER_SNAME())
                    AND (is_rpc = 0 OR {_RPC_WRAPPER_FILTER})
                    {{exclude_clause}}
            ),
            grouped AS (
                SELECT
                    HASHBYTES('SHA2_256', statement) AS statement_hash,
                    MAX(statement) AS statement,
                    user_name,
                    MAX(event_time) AS last_event_time,
                    COUNT(*) AS bucket_execution_count,
                    MAX(is_truncated) AS is_truncated
                FROM filtered
                GROUP BY
                    HASHBYTES('SHA2_256', statement),
                    user_name,
                    DATEADD({{bucket_unit}}, DATEDIFF({{bucket_unit}}, 0, event_time), 0)
            ),
            totals AS (
                SELECT
                    *,
                    SUM(bucket_execution_count) OVER (PARTITION BY statement_hash)
                        AS execution_count
                FROM grouped
            ),
            ranked AS (
                SELECT
                    *,
                    DENSE_RANK() OVER (ORDER BY execution_count DESC, statement_hash)
                        AS statement_rank
                FROM totals
            )
            SELECT
                CONVERT(VARCHAR(64), statement_hash, 2) AS query_id,
                statement AS query_text,
                execution_count,
                -- Duration is only recorded by Azure audits and XE; it is used
                -- only for Query Store ranking, so it isn't extracted here.
                0.0 AS total_exec_time_ms,
                DB_NAME() AS database_name,
                CAST(last_event_time AS DATETIME) AS last_execution_time_utc,
                bucket_execution_count AS window_execution_count,
                user_name,
                is_truncated
            FROM ranked
            WHERE statement_rank <= :limit
            ORDER BY statement_rank, last_event_time
        """.replace(
        "{_RPC_WRAPPER_FILTER}",
        "(statement LIKE '%sp[_]executesql%' OR statement LIKE '%sp[_]prepexec%' OR statement LIKE '%sp[_]prepare%' OR statement LIKE '%sp[_]cursorprepexec%' OR statement LIKE '%sp[_]cursorprepare%')",
    )

    @staticmethod
    def _get_log_query_history(
        log_rows_cte: str,
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        query, params = MSSQLQuery._get_query_history_base(
            exclude_column_expr="statement",
            query_template_builder=log_rows_cte + MSSQLQuery._LOG_QUERY_TAIL,
            window=window,
            limit=limit,
            min_calls=0,
            exclude_patterns=exclude_patterns,
        )
        params.pop("min_calls")
        params["path"] = path
        return query, params

    @staticmethod
    def get_query_history_from_audit_log(
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        """Query history with users from a SQL Server Audit / Azure SQL Auditing log.

        Requires an audit capturing BATCH_COMPLETED_GROUP. :path is a file
        pattern (SQL Server, Managed Instance) or a blob URL prefix (Azure SQL
        Database). event_time is already UTC. SQL Server splits statements over
        4000 characters across records sharing a sequence_group_id, so they are
        reassembled; Azure truncates them instead, which is flagged. The user is
        the session's original login (not an EXECUTE AS identity), falling back
        to the database user for contained users.
        """
        action_ids = ", ".join(f"'{a}'" for a in AUDIT_BATCH_COMPLETED_ACTION_IDS)
        log_rows_cte = f"""
            WITH audit_records AS (
                SELECT
                    event_time,
                    session_id,
                    sequence_group_id,
                    sequence_number,
                    action_id,
                    COALESCE(
                        NULLIF(session_server_principal_name, ''),
                        NULLIF(server_principal_name, ''),
                        NULLIF(database_principal_name, '')
                    ) AS user_name,
                    statement
                FROM sys.fn_get_audit_file(:path, DEFAULT, DEFAULT)
                WHERE
                    action_id IN ({action_ids})
                    AND succeeded = 1
                    AND database_name = DB_NAME()
                    AND event_time >= :start_time
                    AND event_time < :end_time
            ),
            log_rows AS (
                SELECT
                    MIN(event_time) AS event_time,
                    MAX(user_name) AS user_name,
                    STRING_AGG(CAST(statement AS NVARCHAR(MAX)), N'')
                        WITHIN GROUP (ORDER BY sequence_number) AS statement,
                    MAX(CASE WHEN action_id = 'RCM' THEN 1 ELSE 0 END) AS is_rpc,
                    CASE
                        WHEN COUNT(*) = 1
                            AND MAX(LEN(statement)) >= {AUDIT_STATEMENT_MAX_CHARS}
                        THEN 1 ELSE 0
                    END AS is_truncated
                FROM audit_records
                GROUP BY session_id, sequence_group_id, event_time
            ),"""
        return MSSQLQuery._get_log_query_history(
            log_rows_cte=log_rows_cte,
            path=path,
            window=window,
            limit=limit,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_query_history_from_extended_events(
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        exclude_patterns: Optional[List[str]],
        database_scoped: bool,
    ) -> Tuple[TextClause, QueryParams]:
        """Query history with users from an Extended Events event_file target.

        The session must capture sql_batch_completed and/or rpc_completed with
        the server_principal_name (or username) action. A server-scoped session
        must also collect database_name or database_id, or rows can't be tied
        to a database; Azure SQL Database sessions are database-scoped, so rows
        without them belong to the current database. :path is an .xel file
        pattern or blob URL prefix.
        """
        events = ", ".join(f"'{e}'" for e in XE_COMPLETED_EVENTS)
        database_filter = (
            "(database_name IS NULL OR database_name = DB_NAME())"
            if database_scoped
            else "(database_name = DB_NAME() OR database_id = DB_ID())"
        )
        log_rows_cte = f"""
            WITH xe_events AS (
                -- timestamp_utc only compares correctly once cast explicitly.
                SELECT
                    object_name,
                    CAST(timestamp_utc AS DATETIME2(7)) AS timestamp_utc,
                    CAST(event_data AS XML) AS event_xml
                FROM sys.fn_xe_file_target_read_file(:path, NULL, NULL, NULL)
                WHERE
                    object_name IN ({events})
                    AND CAST(timestamp_utc AS DATETIME2(7)) >= :start_time
                    AND CAST(timestamp_utc AS DATETIME2(7)) < :end_time
            ),
            xe_rows AS (
                SELECT
                    timestamp_utc AS event_time,
                    NULLIF(COALESCE(
                        event_xml.value('(event/action[@name="server_principal_name"]/value)[1]', 'nvarchar(256)'),
                        event_xml.value('(event/action[@name="username"]/value)[1]', 'nvarchar(256)')
                    ), '') AS user_name,
                    COALESCE(
                        event_xml.value('(event/data[@name="batch_text"]/value)[1]', 'nvarchar(max)'),
                        event_xml.value('(event/data[@name="statement"]/value)[1]', 'nvarchar(max)')
                    ) AS statement,
                    event_xml.value('(event/action[@name="database_name"]/value)[1]', 'nvarchar(256)')
                        AS database_name,
                    event_xml.value('(event/action[@name="database_id"]/value)[1]', 'int')
                        AS database_id,
                    event_xml.value('(event/data[@name="result"]/text)[1]', 'nvarchar(32)')
                        AS result,
                    CASE WHEN object_name = 'rpc_completed' THEN 1 ELSE 0 END AS is_rpc
                FROM xe_events
            ),
            log_rows AS (
                SELECT event_time, user_name, statement, is_rpc, 0 AS is_truncated
                FROM xe_rows
                WHERE
                    {database_filter}
                    AND (result IS NULL OR result = 'OK')
            ),"""
        return MSSQLQuery._get_log_query_history(
            log_rows_cte=log_rows_cte,
            path=path,
            window=window,
            limit=limit,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_engine_edition() -> TextClause:
        return text(
            "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT) AS engine_edition"
        )

    @staticmethod
    def discover_audit_log_files() -> TextClause:
        """Started file audits with a BATCH_COMPLETED_GROUP specification.

        SQL Server and Managed Instance only; Azure SQL Database has no
        sys.dm_server_audit_status, so the blob URL must be configured.
        """
        return text("""
            SELECT DISTINCT st.name, st.audit_file_path
            FROM sys.dm_server_audit_status AS st
            WHERE
                st.status_desc = 'STARTED'
                AND st.audit_file_path IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM sys.database_audit_specifications AS spec
                    INNER JOIN sys.database_audit_specification_details AS d
                        ON spec.database_specification_id = d.database_specification_id
                    INNER JOIN sys.server_audits AS a
                        ON spec.audit_guid = a.audit_guid
                    WHERE
                        a.audit_id = st.audit_id
                        AND spec.is_state_enabled = 1
                        AND d.audit_action_name = 'BATCH_COMPLETED_GROUP'
                    UNION ALL
                    SELECT 1
                    FROM sys.server_audit_specifications AS spec
                    INNER JOIN sys.server_audit_specification_details AS d
                        ON spec.server_specification_id = d.server_specification_id
                    INNER JOIN sys.server_audits AS a
                        ON spec.audit_guid = a.audit_guid
                    WHERE
                        a.audit_id = st.audit_id
                        AND spec.is_state_enabled = 1
                        AND d.audit_action_name = 'BATCH_COMPLETED_GROUP'
                )
            ORDER BY st.name
        """)

    @staticmethod
    def discover_extended_events_files(database_scoped: bool) -> TextClause:
        """Running Extended Events sessions with an event_file target that
        capture completed batches or RPCs with the login. Azure SQL Database
        sessions are database-scoped and use the dm_xe_database_* views."""
        prefix = "sys.dm_xe_database_" if database_scoped else "sys.dm_xe_"
        events = ", ".join(f"'{e}'" for e in XE_COMPLETED_EVENTS)
        return text(f"""
            SELECT DISTINCT
                s.name,
                CAST(t.target_data AS XML).value('(EventFileTarget/File/@name)[1]', 'nvarchar(max)')
                    AS file_name
            FROM {prefix}sessions AS s
            INNER JOIN {prefix}session_targets AS t
                ON s.address = t.event_session_address
            INNER JOIN {prefix}session_events AS e
                ON s.address = e.event_session_address
            INNER JOIN {prefix}session_event_actions AS a
                ON s.address = a.event_session_address
                AND e.event_name = a.event_name
            WHERE
                t.target_name = 'event_file'
                AND e.event_name IN ({events})
                AND a.action_name IN ('server_principal_name', 'username')
            ORDER BY s.name
        """)

    @staticmethod
    def get_mssql_version() -> TextClause:
        """Get SQL Server version number."""
        return text("""
            SELECT
                CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) AS version,
                CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS major_version
        """)

    @staticmethod
    def list_databases(conn: Connection) -> List[str]:
        """List databases visible on this connection, minus MSSQL system
        databases (see MSSQL_SYSTEM_DATABASES). Does not apply
        database_pattern -- callers (SQLServerSource.get_inspectors() and the
        agent probe) apply that themselves, so both filter on the exact same
        raw listing rather than each re-deriving it.
        """
        rows = (
            conn.execute(
                text(
                    f"SELECT name FROM master.sys.databases WHERE name NOT IN ({_SYSTEM_DATABASE_EXCLUSION})"
                )
            )
            .mappings()
            .fetchall()
        )
        return [str(row["name"]) for row in rows]
