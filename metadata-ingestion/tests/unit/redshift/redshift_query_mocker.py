from datetime import datetime
from unittest.mock import MagicMock


def mock_temp_table_cursor(cursor: MagicMock) -> None:
    cursor.description = [
        ["transaction_id"],
        ["session_id"],
        ["query_text"],
        ["create_command"],
        ["start_time"],
    ]

    cursor.fetchmany.side_effect = [
        [
            (
                126,
                "abc",
                "CREATE TABLE #player_price distkey(player_id) AS SELECT player_id, SUM(price) AS "
                "price_usd from player_activity group by player_id",
                "CREATE TABLE #player_price",
                datetime.now(),
            )
        ],
        [
            # Empty result to stop the while loop
        ],
    ]


def mock_stl_insert_table_cursor(cursor: MagicMock) -> None:
    cursor.description = [
        ["source_schema"],
        ["source_table"],
        ["target_schema"],
        ["target_table"],
        ["ddl"],
    ]

    cursor.fetchmany.side_effect = [
        [
            (
                "public",
                "#player_price",
                "public",
                "player_price_with_hike_v6",
                "INSERT INTO player_price_with_hike_v6 SELECT (price_usd + 0.2 * price_usd) as price, '20%' FROM "
                "#player_price",
            )
        ],
        [
            # Empty result to stop the while loop
        ],
    ]


def mock_alter_table_rename_cursor(cursor: MagicMock) -> None:
    cursor.description = [
        ["transaction_id"],
        ["session_id"],
        ["start_time"],
        ["query_text"],
    ]

    cursor.fetchmany.side_effect = [
        [
            (
                12345,  # transaction_id
                "session_123",  # session_id
                datetime.fromisoformat("2024-01-05 10:00:00+00:00"),  # start_time
                "ALTER TABLE public.old_table_name RENAME TO new_table_name;",  # query_text
            ),
            (
                12346,  # transaction_id
                "session_124",  # session_id
                datetime.fromisoformat("2024-01-06 11:00:00+00:00"),  # start_time
                "ALTER TABLE analytics.old_analytics_table RENAME TO new_analytics_table;",  # query_text
            ),
        ],
        [
            # Empty result to stop the while loop
        ],
    ]


query_vs_cursor_mocker = {
    "-- DataHub Redshift Source temp table DDL query\nwith\n-- Stage 1: Identify individual queries/transactions that contain CREATE TABLE fragments\n-- Purpose: Pre-filter to only queries with DDL activity to reduce LISTAGG workload\nqueries_with_create_table AS (\n    SELECT DISTINCT pid, xid, starttime\n    FROM SVL_STATEMENTTEXT\n    WHERE type IN ('DDL', 'QUERY')                              -- Include both DDL and QUERY (CREATE TABLE AS SELECT)\n      AND starttime >= '2024-01-01 12:00:00'                       -- 2024-01-01 12:00:00+00:00\n      AND endtime < '2024-01-10 12:00:00'                            -- 2024-01-10 12:00:00+00:00\n      AND sequence < 290\n      -- Look for any fragment containing CREATE (TEMP)? TABLE keywords for early filtering of queries\n      AND (text ILIKE '%CREATE%TABLE%'    \n        OR text ILIKE '%CREATE%TEMP%'\n        OR text ILIKE '%CREATE%TEMPORARY%')\n      -- Push down simple filters that can be applied at fragment level\n      AND text NOT ILIKE 'CREATE TEMP TABLE volt_tt_%'          -- Exclude Redshift internal temp tables\n      AND text NOT ILIKE '%https://stackoverflow.com/questions/72770890/redshift-result-size-exceeds-listagg-limit-on-svl-statementtext%'  -- Exclude our own query\n),\n-- Stage 2: Reconstruct complete SQL statements from fragments\n-- Purpose: Use LISTAGG to reassemble fragmented queries, but only for pre-identified queries\nstatement_fragments AS (\n    SELECT\n        s.starttime,\n        s.pid,\n        s.xid,\n        s.type,\n        s.userid,\n        -- Reassemble text fragments into complete SQL statements, preserving whitespace\n        LISTAGG(\n            CASE WHEN LEN(RTRIM(s.text)) = 0 THEN s.text ELSE RTRIM(s.text) END,\n            ''\n        ) WITHIN GROUP (ORDER BY s.sequence) AS query_text\n    FROM SVL_STATEMENTTEXT s\n    INNER JOIN queries_with_create_table c                      -- Only process fragments from identified queries\n      ON s.pid = c.pid AND s.xid = c.xid AND s.starttime = c.starttime\n    WHERE s.type IN ('DDL', 'QUERY')\n      AND s.sequence < 290                  -- Prevent runaway aggregation on very long statements\n    GROUP BY s.starttime, s.pid, s.xid, s.type, s.userid\n),\n-- Stage 3: Parse and normalize CREATE commands from complete statements\n-- Purpose: Extract clean CREATE TABLE commands and add row numbering for deduplication\nparsed_statements AS (\n    SELECT\n        pid AS session_id,\n        xid AS transaction_id,\n        starttime AS start_time,\n        userid,\n        type,\n        -- Complex regex to extract and normalize CREATE TABLE command\n        -- 1. Convert escaped newlines to actual newlines\n        -- 2. Extract CREATE TABLE pattern with optional TEMP/TEMPORARY\n        -- 3. Normalize whitespace to single spaces\n        REGEXP_REPLACE(\n            REGEXP_SUBSTR(\n                REGEXP_REPLACE(query_text,'\\\\\\\\n','\\\\n'),\n                '(CREATE(?:[\\\\n\\\\s\\\\t]+(?:temp|temporary))?(?:[\\\\n\\\\s\\\\t]+)table(?:[\\\\n\\\\s\\\\t]+)[^\\\\n\\\\s\\\\t()-]+)',\n                0, 1, 'ipe'\n            ),\n            '[\\\\n\\\\s\\\\t]+', ' ', 1, 'p'\n        ) as create_command,\n        query_text,\n        -- Add row number for deduplication (keep most recent query per session/statement)\n        ROW_NUMBER() OVER (PARTITION BY pid, TRIM(query_text) ORDER BY start_time DESC) AS rn\n    FROM statement_fragments\n)\n-- Stage 4: Final filtering and deduplication\n-- Purpose: Apply complex CREATE TABLE pattern matching and return only unique recent statements\nSELECT *\nFROM parsed_statements\nWHERE (create_command ILIKE 'create temp table %'      -- Match normalized CREATE TEMP TABLE\n    OR create_command ILIKE 'create temporary table %' -- Match normalized CREATE TEMPORARY TABLE  \n    OR create_command ILIKE 'create table %')          -- Match normalized CREATE TABLE\n  AND rn = 1;  -- Keep only the most recent occurrence of each unique statement\n": mock_temp_table_cursor,
    "select * from test_collapse_temp_lineage": mock_stl_insert_table_cursor,
    "\n            SELECT  transaction_id,\n                    session_id,\n                    start_time,\n                    query_text\n            FROM       sys_query_history SYS\n            WHERE      SYS.status = 'success'\n            AND        SYS.query_type = 'DDL'\n            AND        SYS.database_name = 'test'\n            AND        SYS.start_time >= '2024-01-01 12:00:00'\n            AND        SYS.end_time < '2024-01-10 12:00:00'\n            AND        SYS.query_text ILIKE '%alter table % rename to %'\n        ": mock_alter_table_rename_cursor,
}


def mock_cursor(cursor: MagicMock, query: str) -> None:
    query_vs_cursor_mocker[query](cursor=cursor)
