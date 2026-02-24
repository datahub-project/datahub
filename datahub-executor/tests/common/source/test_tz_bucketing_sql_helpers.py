import pytest

from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.source.bigquery.time_utils import (
    build_tz_bucket_boundary_expression as build_bigquery_tz_bucket_boundary_expression,
)
from datahub_executor.common.source.databricks.time_utils import (
    build_tz_bucket_boundary_expression as build_databricks_tz_bucket_boundary_expression,
)
from datahub_executor.common.source.redshift.time_utils import (
    build_tz_bucket_boundary_expression as build_redshift_tz_bucket_boundary_expression,
)
from datahub_executor.common.source.snowflake.time_utils import (
    build_tz_bucket_boundary_expression as build_snowflake_tz_bucket_boundary_expression,
)


@pytest.mark.parametrize(
    "bucket_interval,expected_sql",
    [
        (
            "DAILY",
            "DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_ts))",
        ),
        (
            "WEEKLY",
            "DATE_TRUNC('WEEK', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_ts))",
        ),
    ],
)
def test_snowflake_build_tz_bucket_boundary_expression(
    bucket_interval: str, expected_sql: str
) -> None:
    assert (
        build_snowflake_tz_bucket_boundary_expression(
            "event_ts", bucket_interval, "America/Los_Angeles"
        )
        == expected_sql
    )


@pytest.mark.parametrize(
    "bucket_interval,expected_sql",
    [
        (
            "DAILY",
            "TIMESTAMP_TRUNC(event_ts, DAY, 'America/Los_Angeles')",
        ),
        (
            "WEEKLY",
            "TIMESTAMP_TRUNC(event_ts, WEEK(MONDAY), 'America/Los_Angeles')",
        ),
    ],
)
def test_bigquery_build_tz_bucket_boundary_expression(
    bucket_interval: str, expected_sql: str
) -> None:
    assert (
        build_bigquery_tz_bucket_boundary_expression(
            "event_ts", bucket_interval, "America/Los_Angeles"
        )
        == expected_sql
    )


@pytest.mark.parametrize(
    "bucket_interval,expected_sql",
    [
        (
            "DAILY",
            "date_trunc('DAY', from_utc_timestamp(event_ts, 'America/Los_Angeles'))",
        ),
        (
            "WEEKLY",
            "date_trunc('WEEK', from_utc_timestamp(event_ts, 'America/Los_Angeles'))",
        ),
    ],
)
def test_databricks_build_tz_bucket_boundary_expression(
    bucket_interval: str, expected_sql: str
) -> None:
    assert (
        build_databricks_tz_bucket_boundary_expression(
            "event_ts", bucket_interval, "America/Los_Angeles"
        )
        == expected_sql
    )


@pytest.mark.parametrize(
    "bucket_interval,expected_sql",
    [
        (
            "DAILY",
            "date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', event_ts))",
        ),
        (
            "WEEKLY",
            "date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', event_ts))",
        ),
    ],
)
def test_redshift_build_tz_bucket_boundary_expression(
    bucket_interval: str, expected_sql: str
) -> None:
    assert (
        build_redshift_tz_bucket_boundary_expression(
            "event_ts", bucket_interval, "America/Los_Angeles"
        )
        == expected_sql
    )


@pytest.mark.parametrize(
    "build_expression",
    [
        build_snowflake_tz_bucket_boundary_expression,
        build_bigquery_tz_bucket_boundary_expression,
        build_databricks_tz_bucket_boundary_expression,
        build_redshift_tz_bucket_boundary_expression,
    ],
)
def test_build_tz_bucket_boundary_expression_rejects_invalid_intervals(
    build_expression,
) -> None:
    with pytest.raises(InvalidParametersException):
        build_expression("event_ts", "MONTHLY", "UTC")
