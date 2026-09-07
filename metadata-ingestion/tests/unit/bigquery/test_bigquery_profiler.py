from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryTable,
    PartitionInfo,
)
from datahub.ingestion.source.bigquery_v2.profiling.profiler import BigqueryProfiler
from datahub.ingestion.source.bigquery_v2.profiling.security import (
    build_safe_table_reference,
    mask_string_literals,
    validate_and_filter_expressions,
    validate_bigquery_identifier,
    validate_column_name,
    validate_column_names,
    validate_filter_expression,
    validate_sql_structure,
)


def test_not_generate_partition_profiler_query_if_not_partitioned_sharded_table():
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
        partition_datetime=None,
    )

    assert query == (None, None)


def test_get_batch_kwargs_includes_row_count():
    # The SQLAlchemy profiler uses this row count to skip a COUNT(*) when
    # deciding whether to sample. The GE profiler ignores it (**kwargs).
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=12345,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )

    kwargs = profiler.get_batch_kwargs(
        table=test_table, schema_name="test_dataset", db_name="test_project"
    )

    assert kwargs["row_count"] == 12345
    assert kwargs["schema"] == "test_project"
    assert kwargs["table"] == "test_dataset.test_table"


def test_generate_day_partitioned_partition_profiler_query():
    column = BigqueryColumn(
        name="date",
        field_path="date",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", fields=("date",), columns=(column,))
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `date` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert query[0] == "20200101"
    assert query[1]
    assert expected_query == query[1].strip()


# If partition time is passed in we force to use that time instead of the max partition id
def test_generate_day_partitioned_partition_profiler_query_with_set_partition_time():
    column = BigqueryColumn(
        name="date",
        field_path="date",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", fields=("date",), columns=(column,))
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `date` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert query[0] == "20200101"
    assert query[1]
    assert expected_query == query[1].strip()


def test_generate_hour_partitioned_partition_profiler_query():
    column = BigqueryColumn(
        name="partition_column",
        field_path="partition_column",
        ordinal_position=1,
        data_type="TIMESTAMP",
        is_partition_column=True,
        cluster_column_position=None,
        comment=None,
        is_nullable=False,
    )
    partition_info = PartitionInfo(type="DAY", fields=("date",), columns=(column,))
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="2020010103",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
        partition_datetime=None,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `partition_column` BETWEEN TIMESTAMP('2020-01-01 03:00:00') AND TIMESTAMP('2020-01-01 04:00:00')
""".strip()

    assert query[0] == "2020010103"
    assert query[1]
    assert expected_query == query[1].strip()


# Ingestion partitioned tables do not have partition column in the schema as it uses a psudo column _PARTITIONTIME to partition
def test_generate_ingestion_partitioned_partition_profiler_query():
    partition_info = PartitionInfo(type="DAY", fields=("date",))
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="test_table",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
        partition_info=partition_info,
        max_partition_id="20200101",
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )
    expected_query = """
SELECT
    *
FROM
    `test_project.test_dataset.test_table`
WHERE
    `_PARTITIONTIME` BETWEEN TIMESTAMP('2020-01-01 00:00:00') AND TIMESTAMP('2020-01-02 00:00:00')
""".strip()

    assert query[0] == "20200101"
    assert query[1]
    assert expected_query == query[1].strip()


def test_generate_sharded_table_profiler_query():
    profiler = BigqueryProfiler(config=BigQueryV2Config(), report=BigQueryV2Report())
    test_table = BigqueryTable(
        name="my_sharded_table",
        max_shard_id="20200101",
        comment="test_comment",
        rows_count=1,
        size_in_bytes=1,
        last_altered=datetime.now(timezone.utc),
        created=datetime.now(timezone.utc),
    )
    query = profiler.generate_partition_profiler_query(
        project="test_project",
        schema="test_dataset",
        table=test_table,
    )

    assert query[0] == "20200101"
    assert query[1] is None


@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
)
@patch("datahub.ingestion.source.bigquery_v2.profiling.profiler.create_engine")
def test_profiler_engine_uses_user_supplied_client_when_credential_set(
    mock_create_engine, mock_from_sa_info
):
    """When a credential block is provided, the profiler engine must pass the
    prebuilt bigquery.Client via connect_args and flag the URL with
    user_supplied_client=true. This is what keeps the SQLAlchemy dialect from
    falling back to google.auth.default() and reading
    GOOGLE_APPLICATION_CREDENTIALS.
    """
    # Intercept create_engine so we can inspect what was passed without
    # actually opening a BigQuery connection.
    mock_create_engine.side_effect = RuntimeError("intercepted")

    config = BigQueryV2Config.model_validate(
        {
            "project_id": "test-project",
            "credential": {
                "project_id": "test-project",
                "private_key_id": "test-private-key",
                "private_key": "random_private_key",
                "client_email": "test@acryl.io",
                "client_id": "test_client-id",
            },
        }
    )
    fake_client = MagicMock(spec=bigquery.Client)
    fake_client.project = "test-project"

    profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())
    with (
        patch.object(
            BigQueryConnectionConfig, "get_bigquery_client", return_value=fake_client
        ),
        pytest.raises(RuntimeError, match="intercepted"),
    ):
        profiler.get_profiler_instance("test-project")

    args, kwargs = mock_create_engine.call_args
    url = args[0]
    assert "user_supplied_client=true" in url
    assert kwargs["connect_args"]["client"] is fake_client


@patch(
    "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
)
@patch("datahub.ingestion.source.bigquery_v2.profiling.profiler.create_engine")
def test_profiler_engine_uses_user_supplied_client_for_wif(
    mock_create_engine, mock_build_wif
):
    """WIF builds in-memory credentials with no `credential` field set. The
    profiler must still inject the explicit bigquery.Client so the SQLAlchemy
    dialect does not fall back to GOOGLE_APPLICATION_CREDENTIALS lookup.
    """
    mock_create_engine.side_effect = RuntimeError("intercepted")
    mock_build_wif.return_value = (MagicMock(), None)

    config = BigQueryV2Config.model_validate(
        {
            "project_id": "test-project",
            "auth_type": "workload_identity_federation",
            "gcp_wif_configuration_json": {
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": {"url": "https://example.com/token"},
            },
        }
    )
    fake_client = MagicMock(spec=bigquery.Client)
    fake_client.project = "test-project"

    profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())
    with (
        patch.object(
            BigQueryConnectionConfig, "get_bigquery_client", return_value=fake_client
        ),
        pytest.raises(RuntimeError, match="intercepted"),
    ):
        profiler.get_profiler_instance("test-project")

    args, kwargs = mock_create_engine.call_args
    url = args[0]
    assert "user_supplied_client=true" in url
    assert kwargs["connect_args"]["client"] is fake_client


@patch("datahub.ingestion.source.bigquery_v2.profiling.profiler.create_engine")
def test_profiler_engine_falls_back_to_adc_when_no_credential(mock_create_engine):
    """When NO credential block is provided, the user opted into Application
    Default Credentials (Workload Identity, gcloud, GOOGLE_APPLICATION_CREDENTIALS).
    The profiler must NOT inject user_supplied_client in that case — the
    dialect's normal credential lookup has to run unchanged.
    """
    mock_create_engine.side_effect = RuntimeError("intercepted")

    config = BigQueryV2Config.model_validate({"project_id": "test-project"})
    profiler = BigqueryProfiler(config=config, report=BigQueryV2Report())

    with pytest.raises(RuntimeError, match="intercepted"):
        profiler.get_profiler_instance("test-project")

    args, kwargs = mock_create_engine.call_args
    url = args[0]
    assert "user_supplied_client" not in url
    assert kwargs["connect_args"] == {}


@pytest.mark.parametrize(
    "input_id, expected",
    [
        ("my_table", "`my_table`"),
        ("dataset_123", "`dataset_123`"),
        ("_PARTITIONTIME", "`_PARTITIONTIME`"),
    ],
)
def test_validate_bigquery_identifier_valid(input_id: str, expected: str) -> None:
    assert validate_bigquery_identifier(input_id) == expected


@pytest.mark.parametrize(
    "invalid_id",
    ["col;drop", "col'injection", "col--comment", "col/*x*/", "bad col"],
)
def test_validate_bigquery_identifier_invalid(invalid_id: str) -> None:
    with pytest.raises(ValueError):
        validate_bigquery_identifier(invalid_id)


def test_build_safe_table_reference():
    assert (
        build_safe_table_reference("my-project", "my_dataset", "my_table")
        == "`my-project`.`my_dataset`.`my_table`"
    )


@pytest.mark.parametrize(
    "dangerous_sql",
    [
        "DROP TABLE foo",
        "SELECT * FROM t; DELETE FROM t",
        "INSERT INTO t VALUES (1)",
    ],
)
def test_validate_sql_structure_dangerous(dangerous_sql: str) -> None:
    with pytest.raises(ValueError):
        validate_sql_structure(dangerous_sql)


def test_validate_sql_structure_valid():
    for query in [
        "SELECT * FROM `project.dataset.table`",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "SELECT COUNT(*) FROM `table` LIMIT 1000",
    ]:
        assert validate_sql_structure(query) is True


def test_validate_column_name():
    assert validate_column_name("valid_col") is True
    assert validate_column_name("_PARTITIONTIME") is True
    assert validate_column_name("invalid col") is False
    assert validate_column_name("col;drop") is False


def test_validate_column_names_filters_invalid():
    result = validate_column_names(["valid_col", "invalid col", "another_valid"])
    assert result == ["valid_col", "another_valid"]


def test_validate_filter_expression_valid():
    assert validate_filter_expression("`event_date` = '2023-12-25'") is True


def test_validate_and_filter_expressions():
    result = validate_and_filter_expressions(
        [
            "`valid_col` = '2023-12-25'",
            "invalid;expression",
            "`another_valid` = 123",
            "DROP TABLE malicious",
        ]
    )
    assert len(result) == 2
    assert "`valid_col` = '2023-12-25'" in result


@pytest.mark.parametrize(
    "dangerous_sql",
    [
        "SELECT 1; EXPORT DATA OPTIONS(uri='gs://x') AS SELECT * FROM t",
        "SELECT 1; LOAD DATA INTO t FROM FILES(uri=['gs://x'])",
        "SELECT 1; CALL `p.d.proc`()",
    ],
)
def test_validate_sql_structure_blocks_export_load_call(dangerous_sql: str) -> None:
    with pytest.raises(ValueError):
        validate_sql_structure(dangerous_sql)


def test_validate_filter_expression_blocks_union_distinct_and_hash_comment():
    # UNION DISTINCT SELECT must be caught like UNION ALL SELECT.
    assert (
        validate_filter_expression("`p` = 1 UNION DISTINCT SELECT secret FROM t")
        is False
    )
    # '#' line comment could comment out the rest of the interpolated predicate.
    assert validate_filter_expression("`p` = 1 # ") is False


def test_validate_filter_expression_allows_between():
    assert (
        validate_filter_expression("`event_date` BETWEEN '2023-01-01' AND '2023-01-31'")
        is True
    )


def test_validate_column_name_rejects_trailing_newline():
    # A trailing newline must not slip through (fullmatch, not match with a `$` anchor).
    assert validate_column_name("valid_col\n") is False


def test_build_safe_table_reference_allows_digit_leading_shard():
    # BigQuery date-sharded tables are digit-leading, backtick-quoted names.
    assert (
        build_safe_table_reference("my-project", "my_dataset", "20200101")
        == "`my-project`.`my_dataset`.`20200101`"
    )


def test_mask_string_literals_blanks_interior_keeps_structure():
    # Interior blanked, delimiters kept, non-literal SQL untouched.
    assert mask_string_literals("`c` = 'a; DROP'") == "`c` = 'xxxxxxx'"
    # Backslash-escaped quote does not close the literal.
    assert mask_string_literals(r"`c` = 'a\'b'") == "`c` = 'xxxx'"
    # Doubled-quote escape stays inside the literal. NB: BigQuery has no '' escape (it uses
    # backslash), so 'a''b' is really two literals there; masking it as one only ever
    # over-masks (a BigQuery syntax error either way), never hides executable SQL. Pinned
    # here so a future rewrite of this branch is a deliberate, tested change.
    assert mask_string_literals("`c` = 'a''b'") == "`c` = 'xxxx'"
    # A comment delimiter is preserved but its body is masked (kept out of denylist scans).
    assert mask_string_literals("SELECT 1 -- x") == "SELECT 1 --xx"


def test_mask_string_literals_triple_quoted_body_is_masked():
    # Triple-quoted strings are masked correctly even though nothing handles ''' explicitly
    # (the doubled-quote branch happens to cover it). Pinned so a cleanup can't silently
    # regress it and let a ';' inside a triple-quoted value escape the mask.
    assert mask_string_literals("`c` = '''a;b'''") == "`c` = 'xxxxxxx'"
    assert validate_filter_expression("`c` = '''a;b'''") is True


@pytest.mark.parametrize(
    "safe_filter",
    [
        # Comment/injection tokens that are inert *inside* a quoted partition value.
        "`uri` = 'gs://bucket/data:image/png'",
        "`path` = 'a--b/c'",
        "`note` = 'value # 1'",
        "`raw` = 'a; b'",
    ],
)
def test_validate_filter_expression_allows_tokens_inside_literal(
    safe_filter: str,
) -> None:
    # Quote-aware scanning must not reject legitimate partition strings that happen to
    # contain SQL comment / URI-scheme characters inside the quoted literal.
    assert validate_filter_expression(safe_filter) is True


def test_validate_filter_expression_still_blocks_tokens_outside_literal():
    # The same tokens outside a literal are genuine injection and must be rejected.
    assert validate_filter_expression("`c` = '2023-01-01' -- drop") is False
    assert validate_filter_expression("`c` = 1 # ") is False


def test_validate_filter_expression_blocks_parenthesized_union():
    # A parenthesized set-query must be caught too, not just `UNION SELECT` / `UNION ALL
    # SELECT` — the '(' between UNION and SELECT used to slip past the denylist.
    assert (
        validate_filter_expression("`c` = 1 UNION ALL (SELECT secret FROM t)") is False
    )
    assert validate_filter_expression("`c` = 1 UNION ((SELECT secret FROM t))") is False


def test_validate_filter_expression_blocks_stacked_statement():
    # A ';' outside a literal stacks a statement regardless of the following keyword
    # (SELECT was previously absent from the keyword list); a WHERE predicate never has one.
    assert validate_filter_expression("`c` = 1; SELECT secret FROM t") is False
    assert validate_filter_expression("`c` = 1; DROP TABLE t") is False
    # But a ';' inside a quoted partition value is inert and must still pass.
    assert validate_filter_expression("`uri` = 'gs://b/a;b'") is True


def test_validate_sql_structure_allows_uri_literal_with_scheme():
    # A `data:`/`javascript:` substring inside a quoted literal is inert.
    assert (
        validate_sql_structure(
            "SELECT * FROM `p.d.t` WHERE `uri` = 'gs://b/data:image'"
        )
        is True
    )


def test_validate_sql_structure_rejects_second_statement():
    # A single read-only statement is required; a trailing ';' alone is fine.
    assert validate_sql_structure("SELECT 1 FROM `p.d.t`;") is True
    with pytest.raises(ValueError):
        validate_sql_structure("SELECT 1 FROM `p.d.t`; SELECT 2 FROM `p.d.t2`")


def test_mask_string_literals_quote_in_comment_does_not_hide_sql():
    # A quote inside a line comment must NOT open a literal and swallow the SQL that
    # follows the comment newline — otherwise a stacked statement stays hidden from the
    # single-statement guard.
    masked = mask_string_literals("SELECT 1 -- it's fine\n; DROP TABLE t")
    assert "; DROP TABLE t" in masked
    # The comment delimiter survives but the body is masked (so a quote in it cannot open
    # a literal and the body stays out of the denylist scans).
    assert "--" in masked
    assert "it's fine" not in masked


def test_validate_sql_structure_rejects_stacked_statement_after_quoted_comment():
    # The concrete P0: a stacked DROP after a comment containing an apostrophe must still
    # be caught rather than masked away as literal content.
    with pytest.raises(ValueError):
        validate_sql_structure("SELECT 1 FROM `p.d.t` -- it's fine\n; DROP TABLE t")


def test_validate_sql_structure_allows_inline_comment_mentioning_keywords():
    # A benign comment that mentions SQL keywords is inert and must not be rejected:
    # comment bodies are no longer scanned for keyword substrings.
    assert (
        validate_sql_structure("SELECT * FROM `p.d.t` -- select everything, no union")
        is True
    )
    assert (
        validate_sql_structure("SELECT * FROM `p.d.t` /* drop/update note */") is True
    )


def test_validate_column_name_allows_flexible_names():
    # BigQuery flexible column names: a leading digit and international characters are
    # legitimate and must not be silently dropped.
    assert validate_column_name("123col") is True
    assert validate_column_name("café") is True
    # Injection / structural characters are still rejected.
    assert validate_column_name("bad col") is False
    assert validate_column_name("col`inject") is False


def test_validate_filter_expression_allows_flexible_column_ref():
    # FILTER_COLUMN_REF_RE must accept the same flexible names validate_column_name does,
    # or filters on a leading-digit / international column would be dropped after the
    # column itself validated (the consistency bug flagged in review).
    assert validate_filter_expression("`123col` = '2023-12-25'") is True
    assert validate_filter_expression("`café` = 1") is True


def test_mask_string_literals_masks_comment_body_keywords():
    # A benign comment mentioning a DDL keyword / ';' must have its body masked so it does
    # not trip the DROP-TABLE denylist or the single-statement guard (a false positive),
    # while the delimiter and surrounding SQL are preserved.
    line_masked = mask_string_literals("SELECT 1 -- DROP TABLE cleanup")
    assert line_masked.startswith("SELECT 1 --")
    assert "DROP" not in line_masked.upper()
    block_masked = mask_string_literals("SELECT 1 /* id ; DROP */ FROM t")
    assert block_masked.startswith("SELECT 1 /*")
    assert block_masked.endswith("*/ FROM t")
    assert "DROP" not in block_masked.upper() and ";" not in block_masked
    # And such queries are accepted rather than rejected.
    assert validate_sql_structure("SELECT 1 FROM `p.d.t` -- DROP TABLE cleanup") is True
    assert validate_sql_structure("SELECT 1 FROM `p.d.t` /* note ; DROP */") is True


def test_build_safe_table_reference_information_schema_case_insensitive():
    # A lower-case / space-padded INFORMATION_SCHEMA reference must reach the dotted-name
    # branch rather than falling through to the table validator and failing on the dot.
    assert (
        build_safe_table_reference(
            "my-project", "my_dataset", "information_schema.tables"
        )
        == "`my-project`.`my_dataset`.information_schema.tables"
    )
    assert (
        build_safe_table_reference(
            "my-project", "my_dataset", " INFORMATION_SCHEMA.TABLES "
        )
        == "`my-project`.`my_dataset`.INFORMATION_SCHEMA.TABLES"
    )
