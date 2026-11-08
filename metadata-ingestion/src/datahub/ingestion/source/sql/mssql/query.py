from dataclasses import dataclass
from datetime import datetime, timedelta
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
# Driver RPCs are kept only when they carry SQL: wrappers around parameterized
# SQL (sp_executesql, sp_prepexec, sp_prepare), and sp_execute calls that run a
# statement prepared earlier in the session. Other RPCs (sp_reset_connection,
# sp_unprepare, sp_cursorfetch, ...) carry no SQL to parse, and procedure-call
# RPCs get their lineage from the stored-procedure extractor instead; keeping
# them would only crowd out real queries in the top-N.
# Group key for sp_execute rows whose prepare isn't in the log for the session.
UNMAPPED_EXECUTE_KEY = "<unmapped sp_execute>"
# `_` is a LIKE wildcard, so it's escaped.
_EXECUTE_LIKE = (
    "statement LIKE N'%sp[_]execute [0-9]%' "
    "OR statement LIKE N'%sp[_]cursorexecute [0-9]%'"
)
_PREPARE_LIKE = "statement LIKE N'%sp[_]prep%' OR statement LIKE N'%sp[_]cursorprep%'"
# How far before start_time to look for the prepare of an in-window sp_execute.
# Bounds the second log read; executes of older prepares stay unmapped (and are
# reported) rather than rescanning the whole retained log.
QUERY_LOG_PREPARE_LOOKBACK = timedelta(hours=24)


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


# System TVFs live in `sys`, but callers usually invoke them unqualified, which
# the SQL parser resolves to the default `dbo` schema. Only those two schemas are
# treated as system for TVF names, so a user object elsewhere with such a name
# keeps its lineage.
_SYSTEM_TVF_SCHEMAS = frozenset({"sys", "dbo"})


def is_mssql_system_object(name: str) -> bool:
    """True for a `db.schema.object` name that refers to a SQL Server system object."""
    parts = name.lower().split(".")
    if parts[-1] in MSSQL_SYSTEM_TABLE_VALUED_FUNCTIONS and (
        len(parts) < 2 or parts[-2] in _SYSTEM_TVF_SCHEMAS
    ):
        return True
    if len(parts) >= 2 and parts[-2] in MSSQL_SYSTEM_SCHEMAS:
        return True
    return len(parts) >= 3 and parts[-3] in _SYSTEM_DATABASES_LOWER


class MSSQLQuery:
    """SQL queries for extracting query history from MS SQL Server."""

    # see https://stackoverflow.com/questions/5953330/how-do-i-map-the-id-in-sys-extended-properties-to-an-object-name
    # also see https://www.mssqltips.com/sqlservertip/5384/working-with-sql-server-extended-properties/
    TABLE_DESCRIPTIONS = """
            SELECT
              SCHEMA_NAME(T.SCHEMA_ID) AS schema_name,
              T.NAME AS table_name,
              EP.VALUE AS table_description
            FROM sys.tables AS T
            INNER JOIN sys.extended_properties AS EP
              ON EP.MAJOR_ID = T.[OBJECT_ID]
              AND EP.MINOR_ID = 0
              AND EP.NAME = 'MS_Description'
              AND EP.CLASS = 1
            """

    @staticmethod
    def table_description(conn: Connection, schema: str, table: str) -> Optional[str]:
        """One table's MS_Description, in the database `conn` is on.

        Bound parameters, not interpolation: the names come from a caller.
        """
        row = conn.execute(
            text(
                f"SELECT d.table_description FROM ({MSSQLQuery.TABLE_DESCRIPTIONS}) AS d "
                "WHERE d.schema_name = :schema AND d.table_name = :table"
            ),
            {"schema": schema, "table": table},
        ).fetchone()
        return None if row is None or row[0] is None else str(row[0])

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

        Returns one row per (query, usage bucket). Each runtime-stats row (one
        per plan, interval and execution type) is counted in the bucket of its
        last_execution_time, and only when that time is inside the window, so
        the returned timestamp always lies in its own bucket and reruns over
        the same window report the same numbers. Counts are exact at the
        granularity of Query Store's runtime-stats interval (60 minutes by
        default): an interval's executions all go to its last execution's
        bucket. Queries that ran in the window rank ahead of older ones for the
        TOP limit. A selected query with no in-window executions yields one
        row with NULL execution columns so its lineage is still kept.
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
                WHERE
                    qt.query_sql_text IS NOT NULL
                    {exclude_clause}
                    AND LEN(qt.query_sql_text) > 0
                GROUP BY q.query_id, qt.query_sql_text
                HAVING SUM(rs.count_executions) >= :min_calls
                ORDER BY
                    MAX(CASE
                        WHEN rs.last_execution_time >= :start_time
                            AND rs.last_execution_time < :end_time
                        THEN 1 ELSE 0
                    END) DESC,
                    SUM(rs.avg_duration * rs.count_executions) DESC,
                    q.query_id
            )
            SELECT
                CAST(tq.query_id AS VARCHAR(50)) AS query_id,
                CAST(tq.query_id AS VARCHAR(50)) AS session_key,
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
                WHERE
                    p.query_id = tq.query_id
                    AND rs.last_execution_time >= :start_time
                    AND rs.last_execution_time < :end_time
                GROUP BY DATEADD(
                    {bucket_unit},
                    DATEDIFF({bucket_unit}, 0, CONVERT(DATETIME2, rs.last_execution_time, 1)),
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
        server's current offset. Times from before a daylight-saving change
        can be off by an hour; T-SQL before SQL Server 2022 has no portable
        way to look up the historical offset, and the DMV fallback's counts
        are approximate anyway.
        """
        query_template = """
            SELECT TOP(:limit)
                CONVERT(VARCHAR(130), qs.sql_handle, 1)
                    + ':' + CAST(qs.statement_start_offset AS VARCHAR(20)) AS query_id,
                -- Statements of one batch share a session so temp tables
                -- created earlier in the batch resolve for later statements.
                CONVERT(VARCHAR(130), qs.sql_handle, 1) AS session_key,
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

    # Shared by the audit and Extended Events readers. The log is read once
    # into #dh_query_log (event_time, session_id, user_name, statement, is_rpc,
    # is_truncated, database_name) for every database, then:
    # - driver wrappers (sp_executesql, sp_prepexec, sp_prepare) are reduced to
    #   their inner SQL, so all parameter values of a query share one key;
    # - `exec sp_execute <handle>` rows take the SQL of the latest prepare of
    #   that handle in the same session (JDBC/ODBC reuse prepared statements);
    # - rows are grouped per (database, statement, user, usage bucket), and the
    #   top :limit statements per database by in-window count are kept.
    # Literal boundaries are found after replacing doubled quotes with a
    # placeholder. Statements are grouped by hash because NVARCHAR(MAX) can't
    # be a GROUP BY key. The connector's own login is excluded so DataHub's
    # metadata queries don't show up as user activity.
    _LOG_QUERY_TAIL = (
        """
            -- Materialized: the sp_execute mapping below reads it twice, and
            -- inlining these string expressions twice exceeds SQL Server's
            -- expression limits.
            SELECT
                    q.*,
                    CASE WHEN f.inner_sql IS NOT NULL
                        THEN REPLACE(f.inner_sql, NCHAR(1), N'''')
                        ELSE q.statement
                    END AS stmt_key,
                    CASE WHEN f.inner_sql IS NOT NULL THEN 1 ELSE 0 END AS is_wrapper,
                    f.inner_end,
                    LEN(e.t) AS raw_len,
                    -- sp_prepare / sp_cursorprepare only prepare; their executions
                    -- arrive as sp_execute / sp_cursorexecute.
                    CASE WHEN p.prep_pos BETWEEN 1 AND 400
                        AND PATINDEX(N'%sp[_]prepexec %', e.t) = 0
                        AND PATINDEX(N'%sp[_]cursorprepexec %', e.t) = 0
                        THEN 1 ELSE 0
                    END AS is_prepare_only,
                    h.prep_handle,
                    h.exec_handle
            INTO #dh_query_norm
                FROM #dh_query_log AS q
                CROSS APPLY (
                    SELECT LTRIM(REPLACE(q.statement, N'''''', NCHAR(1))) AS t
                ) AS e
                CROSS APPLY (
                    SELECT
                        CASE WHEN e.t LIKE N'exec%sp[_]executesql N''%'
                            AND CHARINDEX(N'sp_executesql', e.t) <= 20
                            THEN CHARINDEX(N'sp_executesql N''', e.t) + 16
                        END AS es_start,
                        CASE WHEN e.t LIKE N'declare%' OR e.t LIKE N'exec%'
                            THEN COALESCE(
                                NULLIF(PATINDEX(N'%sp[_]prepexec %', e.t), 0),
                                NULLIF(PATINDEX(N'%sp[_]prepare %', e.t), 0),
                                NULLIF(PATINDEX(N'%sp[_]cursorprepexec %', e.t), 0),
                                NULLIF(PATINDEX(N'%sp[_]cursorprepare %', e.t), 0)
                            )
                        END AS prep_pos,
                        -- Start of the handle in "exec sp_execute <handle>,..." or,
                        -- after the cursor prelude, "exec sp_cursorexecute <handle>,...".
                        CASE
                            WHEN e.t LIKE N'exec sp[_]execute [0-9]%' THEN 17
                            WHEN PATINDEX(N'%exec sp[_]cursorexecute [0-9]%', e.t) BETWEEN 1 AND 400
                                THEN PATINDEX(N'%exec sp[_]cursorexecute [0-9]%', e.t) + 22
                        END AS exec_pos
                ) AS p
                CROSS APPLY (
                    SELECT
                        CASE WHEN p.es_start IS NOT NULL
                            THEN CHARINDEX(N'''', e.t, p.es_start)
                        END AS es_end,
                        -- The prepare call follows the driver's short prelude.
                        CASE WHEN p.prep_pos BETWEEN 1 AND 400
                            THEN CHARINDEX(N'''', e.t, p.prep_pos)
                        END AS l1s
                ) AS a
                CROSS APPLY (
                    SELECT CASE WHEN a.l1s > 0 THEN CHARINDEX(N'''', e.t, a.l1s + 1) END AS l1e
                ) AS b
                CROSS APPLY (
                    SELECT
                        CASE WHEN b.l1e > 0
                            THEN SUBSTRING(e.t, a.l1s + 1, b.l1e - a.l1s - 1)
                        END AS lit1,
                        CASE WHEN b.l1e > 0 THEN CHARINDEX(N'''', e.t, b.l1e + 1) END AS l2s
                ) AS c
                CROSS APPLY (
                    SELECT CASE WHEN c.l2s > 0 THEN CHARINDEX(N'''', e.t, c.l2s + 1) END AS l2e
                ) AS d
                CROSS APPLY (
                    SELECT CASE
                        WHEN a.es_end > p.es_start
                            THEN SUBSTRING(e.t, p.es_start, a.es_end - p.es_start)
                        -- Prepare variants pass the parameter declaration
                        -- ('@P1 int', or NULL) before the statement.
                        WHEN c.lit1 LIKE N'@%' AND d.l2e > c.l2s
                            THEN SUBSTRING(e.t, c.l2s + 1, d.l2e - c.l2s - 1)
                        WHEN c.lit1 IS NOT NULL AND c.lit1 NOT LIKE N'@%'
                            THEN c.lit1
                    END AS inner_sql,
                    CASE
                        WHEN a.es_end > p.es_start THEN a.es_end
                        WHEN c.lit1 LIKE N'@%' AND d.l2e > c.l2s THEN d.l2e
                        WHEN c.lit1 IS NOT NULL AND c.lit1 NOT LIKE N'@%' THEN b.l1e
                    END AS inner_end
                ) AS f
                CROSS APPLY (
                    SELECT
                        -- "declare @p1 int set @p1=<handle> exec sp_prepexec @p1 output, ...".
                        -- Completed events render the returned handle, and the
                        -- prepared handle is the first output in every variant.
                        CASE WHEN p.prep_pos BETWEEN 1 AND 400 AND e.t LIKE N'declare%'
                            THEN LTRIM(SUBSTRING(
                                e.t,
                                CHARINDEX(N'=', e.t, NULLIF(CHARINDEX(N'set @', e.t), 0)) + 1,
                                12
                            ))
                        END AS prep_raw,
                        CASE WHEN p.exec_pos IS NOT NULL THEN SUBSTRING(e.t, p.exec_pos, 12) END
                            AS exec_raw
                ) AS hr
                CROSS APPLY (
                    SELECT
                        TRY_CAST(NULLIF(LEFT(hr.prep_raw, PATINDEX(N'%[^0-9]%', hr.prep_raw + N'x') - 1), N'') AS INT)
                            AS prep_handle,
                        TRY_CAST(NULLIF(LEFT(hr.exec_raw, PATINDEX(N'%[^0-9]%', hr.exec_raw + N'x') - 1), N'') AS INT)
                            AS exec_handle
                ) AS h;

            -- Azure truncates the raw record, but wrappers put the SQL literal
            -- before the parameter values: a literal that closes before the end
            -- of the record is complete.
            ALTER TABLE #dh_query_norm ADD stmt_truncated INT;
            UPDATE #dh_query_norm SET stmt_truncated =
                CASE WHEN is_truncated = 1 AND COALESCE(inner_end, raw_len) >= raw_len
                    THEN 1 ELSE 0
                END;

            WITH mapped AS (
                SELECT
                    n.event_time,
                    n.session_id,
                    n.user_name,
                    n.database_name,
                    n.is_rpc,
                    CASE WHEN n.exec_handle IS NULL THEN n.stmt_truncated
                        ELSE COALESCE(m.stmt_truncated, 0)
                    END AS is_truncated,
                    n.is_prepare_only,
                    n.in_window,
                    CASE
                        WHEN n.exec_handle IS NULL THEN n.stmt_key
                        ELSE COALESCE(m.stmt_key, N'"""
        + UNMAPPED_EXECUTE_KEY
        + """')
                    END AS stmt_key,
                    CASE WHEN n.exec_handle IS NULL THEN n.is_wrapper ELSE 1 END AS is_wrapper,
                    CASE WHEN n.exec_handle IS NOT NULL AND m.stmt_key IS NULL THEN 1 ELSE 0 END
                        AS is_unmapped
                FROM #dh_query_norm AS n
                OUTER APPLY (
                    SELECT TOP 1 pr.stmt_key, pr.stmt_truncated
                    FROM #dh_query_norm AS pr
                    WHERE
                        n.exec_handle IS NOT NULL
                        AND pr.prep_handle = n.exec_handle
                        AND pr.session_id = n.session_id
                        AND pr.database_name = n.database_name
                        AND pr.event_time <= n.event_time
                    ORDER BY pr.event_time DESC
                ) AS m
            ),
            filtered AS (
                SELECT *
                FROM mapped
                WHERE
                    stmt_key IS NOT NULL
                    AND LEN(stmt_key) > 0
                    AND (user_name IS NULL OR user_name <> SUSER_SNAME())
                    AND in_window = 1
                    -- Truncated RPCs are kept so they're reported, not dropped.
                    AND (is_rpc = 0 OR is_wrapper = 1 OR is_truncated = 1)
                    AND is_prepare_only = 0
                    {exclude_clause}
            ),
            -- Statements that reference a #temp table also keep their session,
            -- so the parser sees a temp table's producer and its readers in one
            -- session and resolves lineage to the real source. Everything else
            -- stays grouped across sessions, which keeps the row count low.
            sessioned AS (
                SELECT
                    *,
                    CASE WHEN stmt_key LIKE N'%#%' THEN session_id END AS temp_session_id,
                    DATEADD({bucket_unit}, DATEDIFF({bucket_unit}, 0, event_time), 0)
                        AS bucket_start
                FROM filtered
            ),
            grouped AS (
                SELECT
                    database_name,
                    HASHBYTES('SHA2_256', stmt_key) AS statement_hash,
                    MAX(stmt_key) AS statement,
                    user_name,
                    temp_session_id,
                    bucket_start,
                    MAX(event_time) AS last_event_time,
                    COUNT(*) AS bucket_execution_count,
                    MAX(is_truncated) AS is_truncated,
                    MAX(is_unmapped) AS is_unmapped,
                    SUM(is_wrapper) AS wrapped_execution_count
                FROM sessioned
                GROUP BY
                    database_name,
                    HASHBYTES('SHA2_256', stmt_key),
                    user_name,
                    temp_session_id,
                    bucket_start
            ),
            totals AS (
                SELECT
                    *,
                    SUM(bucket_execution_count)
                        OVER (PARTITION BY database_name, statement_hash) AS execution_count,
                    MAX(is_truncated)
                        OVER (PARTITION BY database_name, statement_hash) AS statement_truncated
                FROM grouped
            ),
            ranked AS (
                SELECT
                    *,
                    DENSE_RANK() OVER (
                        PARTITION BY database_name, is_unmapped, statement_truncated
                        ORDER BY execution_count DESC, statement_hash
                    ) AS statement_rank
                FROM totals
                WHERE
                    is_unmapped = 1
                    OR statement_truncated = 1
                    OR execution_count >= :min_calls
            )
            SELECT
                CONVERT(VARCHAR(64), statement_hash, 2) AS query_id,
                -- The parser session must span the whole connection, so it
                -- leaves out the usage bucket and the current database (a
                -- connection can cross buckets or run USE). Session ids are
                -- reused by later connections; the login keeps another login's
                -- connection apart. The audit's connection_id would be exact,
                -- but SQL Server 2017/2019 audits don't return it.
                CASE
                    WHEN temp_session_id IS NULL THEN CONVERT(VARCHAR(64), statement_hash, 2)
                    ELSE CONCAT(
                        N'session:', temp_session_id, N':', COALESCE(user_name, N'')
                    )
                END AS session_key,
                statement AS query_text,
                execution_count,
                -- Duration is only recorded by Azure audits and XE; it is used
                -- only for Query Store ranking, so it isn't extracted here.
                0.0 AS total_exec_time_ms,
                database_name,
                CAST(last_event_time AS DATETIME) AS last_execution_time_utc,
                bucket_execution_count AS window_execution_count,
                user_name,
                statement_truncated AS is_truncated,
                is_unmapped,
                wrapped_execution_count
            FROM ranked
            WHERE statement_rank <= :limit
            ORDER BY database_name, statement_rank, last_event_time
        """
    )

    @staticmethod
    def _get_log_query_history(
        log_rows_cte: str,
        window_predicate: str,
        before_window_predicate: str,
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
    ) -> Tuple[TextClause, QueryParams]:
        """Assemble a log reader. `log_rows_cte` defines `log_rows` with a
        `__TIME__` slot for the event-time predicate.

        Window rows are loaded first. When the window holds sp_execute /
        sp_cursorexecute calls, the prepares those sessions made in the
        QUERY_LOG_PREPARE_LOOKBACK before the window are loaded too (pooled
        connections prepare once and execute for hours), only to resolve
        handles: they aren't counted. `before_window_predicate` may use
        :prepare_start_time and should filter to prepares as cheaply as the
        log format allows.
        """
        columns = (
            "event_time, session_id, user_name, statement, is_rpc, "
            "is_truncated, database_name"
        )
        load = (
            "SET NOCOUNT ON;\nDROP TABLE IF EXISTS #dh_query_log;\n"
            + "DROP TABLE IF EXISTS #dh_query_norm;\n"
            + log_rows_cte.replace("__TIME__", window_predicate)
            + f"\nSELECT {columns}, 1 AS in_window INTO #dh_query_log FROM log_rows;\n"
            + f"IF EXISTS (SELECT 1 FROM #dh_query_log WHERE {_EXECUTE_LIKE})\nBEGIN\n;"
            + log_rows_cte.replace("__TIME__", before_window_predicate)
            + f"""
            INSERT INTO #dh_query_log
            SELECT {columns}, 0 AS in_window
            FROM log_rows
            WHERE
                ({_PREPARE_LIKE})
                AND session_id IN (
                    SELECT session_id FROM #dh_query_log
                    WHERE in_window = 1 AND ({_EXECUTE_LIKE})
                );
            END;
            """
        )
        query, params = MSSQLQuery._get_query_history_base(
            exclude_column_expr="stmt_key",
            query_template_builder=load + MSSQLQuery._LOG_QUERY_TAIL,
            window=window,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )
        params["path"] = path
        params["prepare_start_time"] = window.start_time - QUERY_LOG_PREPARE_LOOKBACK
        return query, params

    @staticmethod
    def get_query_history_from_audit_log(
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
        statements_truncated: bool,
    ) -> Tuple[TextClause, QueryParams]:
        """Query history with users from a SQL Server Audit / Azure SQL Auditing log.

        Requires an audit capturing BATCH_COMPLETED_GROUP. :path is a file
        pattern (SQL Server, Managed Instance) or a blob URL prefix (Azure SQL
        Database). Rows for every database in the log are returned, so a
        server audit is read once per run. event_time is already UTC. SQL
        Server splits statements over 4000 characters across records sharing
        a sequence_group_id, so they are reassembled; Azure truncates them
        instead, which is flagged. The user is the session's original login
        (not an EXECUTE AS identity), falling back to the database user for
        contained users.
        """
        action_ids = ", ".join(f"'{a}'" for a in AUDIT_BATCH_COMPLETED_ACTION_IDS)
        # Only Azure SQL Auditing truncates; SQL Server splits long statements
        # across records (reassembled below), so a single 4000-character record
        # there is a complete statement.
        is_truncated_expr = (
            f"CASE WHEN COUNT(*) = 1 AND MAX(LEN(statement)) >= {AUDIT_STATEMENT_MAX_CHARS} "
            "THEN 1 ELSE 0 END"
            if statements_truncated
            else "0"
        )
        log_rows_cte = f"""
            WITH audit_records AS (
                SELECT
                    event_time,
                    session_id,
                    sequence_group_id,
                    sequence_number,
                    action_id,
                    database_name,
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
                    AND database_name IS NOT NULL
                    AND __TIME__
            ),
            log_rows AS (
                SELECT
                    MIN(event_time) AS event_time,
                    session_id,
                    MAX(user_name) AS user_name,
                    STRING_AGG(CAST(statement AS NVARCHAR(MAX)), N'')
                        WITHIN GROUP (ORDER BY sequence_number) AS statement,
                    MAX(CASE WHEN action_id = 'RCM' THEN 1 ELSE 0 END) AS is_rpc,
                    {is_truncated_expr} AS is_truncated,
                    MAX(database_name) AS database_name
                FROM audit_records
                GROUP BY session_id, sequence_group_id, event_time
            )"""
        return MSSQLQuery._get_log_query_history(
            log_rows_cte=log_rows_cte,
            window_predicate="event_time >= :start_time AND event_time < :end_time",
            before_window_predicate=(
                "event_time >= :prepare_start_time AND event_time < :start_time"
            ),
            path=path,
            window=window,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_query_history_from_extended_events(
        path: str,
        window: QueryHistoryWindow,
        limit: int,
        min_calls: int,
        exclude_patterns: Optional[List[str]],
        database_scoped: bool,
    ) -> Tuple[TextClause, QueryParams]:
        """Query history with users from an Extended Events event_file target.

        The session must capture sql_batch_completed and/or rpc_completed with
        a login action (session_server_principal_name preferred, so EXECUTE AS
        is credited to the original login, then server_principal_name or
        username), and session_id to resolve sp_execute calls. A server-scoped
        session must also collect database_name or database_id, or rows can't
        be tied to a database; its rows for every database are returned, so the
        file is read and shredded once per run. Azure SQL Database sessions are
        database-scoped, so rows without a database belong to the current one.
        :path is an .xel file pattern or blob URL prefix.
        """
        events = ", ".join(f"'{e}'" for e in XE_COMPLETED_EVENTS)
        database_expr = (
            "COALESCE(x.action_database_name, DB_NAME())"
            if database_scoped
            else "COALESCE(x.action_database_name, DB_NAME(x.action_database_id))"
        )
        log_rows_cte = f"""
            WITH xe_events AS (
                SELECT
                    object_name,
                    CAST(timestamp_utc AS DATETIME2(7)) AS timestamp_utc,
                    CAST(event_data AS XML) AS event_xml
                FROM sys.fn_xe_file_target_read_file(:path, NULL, NULL, NULL)
                WHERE
                    object_name IN ({events})
                    AND __TIME__
            ),
            xe_rows AS (
                SELECT
                    timestamp_utc AS event_time,
                    event_xml.value('(event/action[@name="session_id"]/value)[1]', 'int')
                        AS session_id,
                    COALESCE(
                        NULLIF(event_xml.value('(event/action[@name="session_server_principal_name"]/value)[1]', 'nvarchar(256)'), ''),
                        NULLIF(event_xml.value('(event/action[@name="server_principal_name"]/value)[1]', 'nvarchar(256)'), ''),
                        NULLIF(event_xml.value('(event/action[@name="username"]/value)[1]', 'nvarchar(256)'), '')
                    ) AS user_name,
                    COALESCE(
                        event_xml.value('(event/data[@name="batch_text"]/value)[1]', 'nvarchar(max)'),
                        event_xml.value('(event/data[@name="statement"]/value)[1]', 'nvarchar(max)')
                    ) AS statement,
                    event_xml.value('(event/action[@name="database_name"]/value)[1]', 'nvarchar(256)')
                        AS action_database_name,
                    event_xml.value('(event/action[@name="database_id"]/value)[1]', 'int')
                        AS action_database_id,
                    event_xml.value('(event/data[@name="result"]/text)[1]', 'nvarchar(32)')
                        AS result,
                    CASE WHEN object_name = 'rpc_completed' THEN 1 ELSE 0 END AS is_rpc
                FROM xe_events
            ),
            log_rows AS (
                SELECT
                    x.event_time,
                    x.session_id,
                    x.user_name,
                    x.statement,
                    x.is_rpc,
                    0 AS is_truncated,
                    {database_expr} AS database_name
                FROM xe_rows AS x
                WHERE
                    (x.result IS NULL OR x.result = 'OK')
                    AND {database_expr} IS NOT NULL
            )"""
        # timestamp_utc only compares correctly once cast explicitly.
        timestamp = "CAST(timestamp_utc AS DATETIME2(7))"
        return MSSQLQuery._get_log_query_history(
            log_rows_cte=log_rows_cte,
            window_predicate=f"{timestamp} >= :start_time AND {timestamp} < :end_time",
            # Filter on the raw event text before it is cast to XML and shredded.
            before_window_predicate=(
                f"{timestamp} >= :prepare_start_time AND {timestamp} < :start_time "
                "AND (event_data LIKE N'%sp[_]prep%' OR event_data LIKE N'%sp[_]cursorprep%')"
            ),
            path=path,
            window=window,
            limit=limit,
            min_calls=min_calls,
            exclude_patterns=exclude_patterns,
        )

    @staticmethod
    def get_engine_edition() -> TextClause:
        return text(
            "SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT) AS engine_edition, "
            "DB_NAME() AS current_database"
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
        # A server-scoped session sees every database, so its rows are only
        # usable when it records which database each event ran in.
        database_action_filter = (
            ""
            if database_scoped
            else f"""AND EXISTS (
                    SELECT 1 FROM {prefix}session_event_actions AS da
                    WHERE da.event_session_address = s.address
                        AND da.event_name = e.event_name
                        AND da.action_name IN ('database_name', 'database_id')
                )"""
        )
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
                AND a.action_name IN (
                    'session_server_principal_name', 'server_principal_name', 'username'
                )
                {database_action_filter}
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
