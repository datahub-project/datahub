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
