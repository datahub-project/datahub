from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    SnowflakeOpenflowQuery,
)

HISTORY_BUILDERS = [
    SnowflakeOpenflowQuery.deployment_history,
    SnowflakeOpenflowQuery.runtime_history,
    SnowflakeOpenflowQuery.connector_history,
]


def test_history_queries_target_account_usage():
    for builder in HISTORY_BUILDERS:
        assert "SNOWFLAKE.ACCOUNT_USAGE.OPENFLOW_" in builder(None)


def test_history_queries_never_use_offset():
    # OFFSET on a growing view silently skips and duplicates rows. Pagination is
    # a manual cursor on CREATED_ON.
    for builder in HISTORY_BUILDERS:
        assert "OFFSET" not in builder(None).upper()


def test_history_queries_paginate_by_created_on():
    for builder in HISTORY_BUILDERS:
        query = builder("2026-09-03T00:00:00")
        # `>=`, asserted exactly. A strict `>` drops every row sharing the page
        # boundary's CREATED_ON, and a dropped history row is invisible: it
        # surfaces as a deleted object staying live in DataHub. `"CREATED_ON >"`
        # would be satisfied by both spellings, so it cannot pin this.
        assert "CREATED_ON >=" in query
        assert "ORDER BY CREATED_ON" in query


def test_first_page_has_no_cursor_predicate():
    assert "CREATED_ON >" not in SnowflakeOpenflowQuery.deployment_history(None)


def test_deleted_rows_are_retained_for_deletion_detection():
    # DELETED_ON must NOT be filtered in SQL: the deleted rows are what drives
    # stale-entity removal. Filtering happens in Python, after the merge.
    for builder in HISTORY_BUILDERS:
        assert "DELETED_ON IS NULL" not in builder(None).upper()


def test_show_commands_are_the_documented_grammar():
    assert SnowflakeOpenflowQuery.show_deployments() == "SHOW OPENFLOW DEPLOYMENTS"
    assert SnowflakeOpenflowQuery.show_runtimes() == "SHOW OPENFLOW RUNTIMES"
    assert SnowflakeOpenflowQuery.show_connectors() == "SHOW OPENFLOW CONNECTORS"


def test_stage_get_quotes_the_uri_verbatim():
    # The version URI must be used exactly as the connector row reports it.
    # Hardcoding a path segment such as /versions/live/ produced Snowflake
    # errno 99112 "version live is not found".
    uri = "snow://openflow_connector/MY_DB.MY_SCHEMA.pg/versions/3/"
    query = SnowflakeOpenflowQuery.get_stage_file_to_local(uri, "config.json", "/tmp/x")
    assert uri in query
    assert query.startswith("GET ")
    assert "'file:///tmp/x'" in query


def test_generated_limit_matches_the_page_size_the_pager_reads():
    # The pager decides a page is the last one by comparing the row count against
    # PAGE_SIZE, so a LIMIT that disagreed with it would silently truncate.
    assert (
        f"LIMIT {SnowflakeOpenflowQuery.PAGE_SIZE}"
        in SnowflakeOpenflowQuery.deployment_history(None)
    )


def test_describe_connector_escapes_the_string_literal():
    # IDENTIFIER() takes the quoted name as a string literal, so a single quote
    # inside the name has to be doubled or it closes the literal early.
    query = SnowflakeOpenflowQuery.describe_connector('"db"."s"."o\'brien"')
    assert query == (
        'DESCRIBE OPENFLOW CONNECTOR IDENTIFIER(\'"db"."s"."o\'\'brien"\')'
    )
