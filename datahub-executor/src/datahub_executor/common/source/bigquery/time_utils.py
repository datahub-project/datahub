from datahub_executor.common.exceptions import InvalidParametersException

SUPPORTED_TZ_BUCKET_INTERVALS = {"DAILY", "WEEKLY"}


def convert_millis_to_date(millis: int) -> str:
    return f"DATE(TIMESTAMP_MILLIS(CAST({millis} AS INT64)))"


def convert_millis_to_datetime(millis: int) -> str:
    # TODO: What if dates are stored with another TZ?
    return f"DATETIME(TIMESTAMP_MILLIS(CAST({millis} AS INT64)), 'UTC')"


def convert_millis_to_timestamp(millis: int) -> str:
    return f"TIMESTAMP_MILLIS(CAST({millis} AS INT64))"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "DATE":
        return convert_millis_to_date(millis)
    elif column_type == "DATETIME":
        return convert_millis_to_datetime(millis)
    elif column_type == "TIMESTAMP":
        return convert_millis_to_timestamp(millis)
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "millis": millis},
    )


def convert_value_for_comparison(column_value: str, column_type: str) -> str:
    if column_type == "DATE":
        return f"DATE('{column_value}')"
    elif column_type == "DATETIME":
        return f"DATETIME('{column_value}')"
    elif column_type == "TIMESTAMP":
        return f"TIMESTAMP('{column_value}')"
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "column_value": column_value},
    )


def build_tz_bucket_boundary_expression(
    timestamp_column: str, bucket_interval: str, timezone_name: str
) -> str:
    normalized_interval = bucket_interval.upper()
    if normalized_interval not in SUPPORTED_TZ_BUCKET_INTERVALS:
        raise InvalidParametersException(
            message=f"Unsupported bucket interval {bucket_interval} provided!",
            parameters={
                "bucket_interval": bucket_interval,
                "supported_intervals": sorted(SUPPORTED_TZ_BUCKET_INTERVALS),
            },
        )

    if normalized_interval == "DAILY":
        return f"TIMESTAMP_TRUNC({timestamp_column}, DAY, '{timezone_name}')"
    return f"TIMESTAMP_TRUNC({timestamp_column}, WEEK(MONDAY), '{timezone_name}')"
