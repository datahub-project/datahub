import re
from datetime import datetime
from unittest.mock import MagicMock

from datahub.ingestion.source.redshift.redshift_schema import (
    REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE,
)


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
    (
        "-- DataHub Redshift Source temp table DDL query\nselect\n    *\nfrom (\n    select\n        session_id,\n        transaction_id,\n        start_time,\n        userid,\n        REGEXP_REPLACE(REGEXP_SUBSTR(REGEXP_REPLACE(query_text,'\\\\\\\\n','\\\\n'), '(CREATE(?:[\\\\n\\\\s\\\\t]+(?:temp|temporary))?(?:[\\\\n\\\\s\\\\t]+)table(?:[\\\\n\\\\s\\\\t]+)[^\\\\n\\\\s\\\\t()-]+)', 0, 1, 'ipe'),'[\\\\n\\\\s\\\\t]+',' ',1,'p') as create_command,\n        query_text,\n        row_number() over (\n            partition by session_id, TRIM(query_text)\n            order by start_time desc\n        ) rn\n    from (\n        select\n            pid as session_id,\n            xid as transaction_id,\n            starttime as start_time,\n            type,\n            query_text,\n            userid\n        from (\n            select\n                starttime,\n                pid,\n                xid,\n                type,\n                userid,\n                LISTAGG(case\n                    when LEN(RTRIM(text)) = 0 then text\n                    else RTRIM(text)\n                end,\n                '') within group (\n                    order by sequence\n                ) as query_text\n            from\n                SVL_STATEMENTTEXT\n            where\n                type in ('DDL', 'QUERY')\n                AND        starttime >= '2024-01-01 12:00:00'\n                AND        starttime < '2024-01-10 12:00:00'\n                AND sequence < 290\n            group by\n                starttime,\n                pid,\n                xid,\n                type,\n                userid\n            order by\n                starttime,\n                pid,\n                xid,\n                type,\n                userid\n                asc\n        )\n        where\n            type in ('DDL', 'QUERY')\n    )\n    where\n        (create_command ilike 'create temp table %'\n            or create_command ilike 'create temporary table %'\n            -- we want to get all the create table statements and not just temp tables if non temp table is created and dropped in the same transaction\n            or create_command ilike 'create table %')\n        -- Redshift creates temp tables with the following names: volt_tt_%. We need to filter them out.\n        and query_text not ilike 'CREATE TEMP TABLE volt_tt_%'\n        and create_command not like 'CREATE TEMP TABLE volt_tt_'\n        -- We need to filter out our query and it was not possible earlier when we did not have any comment in the query\n        and query_text not ilike '%https://stackoverflow.com/questions/72770890/redshift-result-size-exceeds-listagg-limit-on-svl-statementtext%'\n\n)\nwhere\n    rn = 1\n"
    ): mock_temp_table_cursor,
    "select * from test_collapse_temp_lineage": mock_stl_insert_table_cursor,
    "\n            SELECT  transaction_id,\n                    session_id,\n                    start_time,\n                    query_text\n            FROM       sys_query_history SYS\n            WHERE      SYS.status = 'success'\n            AND        SYS.query_type = 'DDL'\n            AND        SYS.database_name = 'test'\n            AND        SYS.start_time >= '2024-01-01 12:00:00'\n            AND        SYS.end_time < '2024-01-10 12:00:00'\n            AND        SYS.query_text ILIKE '%alter table % rename to %'\n        ": mock_alter_table_rename_cursor,
}


def mock_cursor(cursor: MagicMock, query: str) -> None:
    # Strip query tag if present (format: "-- partner: DataHub -v <version>\n")
    # This allows the mock to work with both tagged and untagged queries.
    # Use the shared template as the canonical source of the prefix, then strip
    # the entire first line (including the version + newline).
    tag_prefix = REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE.split("{version}", 1)[0]
    query_without_tag = re.sub(
        r"^" + re.escape(tag_prefix) + r"[^\n]+\n",
        "",
        query,
        count=1,
    )

    # Prefer matching the untagged query (our fixtures are stored untagged),
    # but fall back to the original string for compatibility.
    query_key = (
        query_without_tag if query_without_tag in query_vs_cursor_mocker else query
    )
    if query_key not in query_vs_cursor_mocker:
        raise KeyError(
            "Query not found in mock dictionary. "
            f"Query (first 200 chars): {query[:200]}..."
        )

    query_vs_cursor_mocker[query_key](cursor=cursor)
