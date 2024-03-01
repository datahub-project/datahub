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


query_vs_cursor_mocker = {
    (
        "-- DataHub Redshift Source temp table DDL query\n                    SELECT\n                       "
        " *\n                    FROM\n                    (\n                        SELECT\n               "
        "             session_id,\n                            transaction_id,\n                            s"
        "tart_time,\n                            query_type AS \"type\",\n                            user_id"
        " AS userid,\n                            query_text, -- truncated to 4k characters, join with SYS_QU"
        "ERY_TEXT to build full query using \"sequence\" number field, probably no reason to do so, since we "
        "are interested in the begining of the statement anyway\n                            REGEXP_REPLACE(R"
        "EGEXP_SUBSTR(REGEXP_REPLACE(query_text,'\\\\\\\\n','\\\\n'), '(CREATE(?:[\\\\n\\\\s\\\\t]+(?:temp|te"
        "mporary))?(?:[\\\\n\\\\s\\\\t]+)table(?:[\\\\n\\\\s\\\\t]+)[^\\\\n\\\\s\\\\t()-]+)', 0, 1, 'ipe'),'["
        "\\\\n\\\\s\\\\t]+',' ',1,'p') as create_command,\n                            ROW_NUMBER() OVER (\n "
        "                               PARTITION BY TRIM(query_text)\n                                ORDER "
        "BY start_time DESC\n                            ) rn\n                        FROM\n                "
        "            SYS_QUERY_HISTORY\n                        WHERE\n                            query_type"
        " IN ('DDL', 'CTAS', 'OTHER', 'COMMAND')\n                            AND start_time >= '2024-01-01 1"
        "2:00:00'\n                            AND start_time < '2024-01-10 12:00:00'\n                      "
        "  GROUP BY start_time, session_id, transaction_id, type, userid, query_text\n                       "
        " ORDER BY start_time, session_id, transaction_id, type, userid ASC\n                    )\n         "
        "           WHERE\n                        (\n                            create_command ILIKE 'creat"
        "e temp table %'\n                            OR create_command ILIKE 'create temporary table %'\n   "
        "                         -- we want to get all the create table statements and not just temp tables "
        "if non temp table is created and dropped in the same transaction\n                            OR cre"
        "ate_command ILIKE 'create table %'\n                        )\n                        -- Redshift c"
        "reates temp tables with the following names: volt_tt_%. We need to filter them out.\n               "
        "         AND query_text NOT ILIKE 'CREATE TEMP TABLE volt_tt_%'\n                        AND create_"
        "command NOT ILIKE 'CREATE TEMP TABLE volt_tt_'\n                        AND rn = 1\n                "
        "    ;\n            "
    ): mock_temp_table_cursor,
    "select * from test_collapse_temp_lineage": mock_stl_insert_table_cursor,
}


def mock_cursor(cursor: MagicMock, query: str) -> None:
    query_vs_cursor_mocker[query](cursor=cursor)
