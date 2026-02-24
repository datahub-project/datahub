from datahub_executor.common.exceptions import InvalidParametersException

SUPPORTED_TZ_BUCKET_INTERVALS = {"DAILY", "WEEKLY"}


def convert_millis_to_timestamp(millis: int) -> str:
    seconds = millis / 1000.0
    return f"TIMESTAMP 'epoch' + {seconds} * INTERVAL '1 second'"


def convert_millis_to_timestamptz(millis: int) -> str:
    seconds = millis / 1000.0
    return f"TIMESTAMPTZ 'epoch' + {seconds} * INTERVAL '1 second'"


def convert_millis_to_date(millis: int) -> str:
    seconds = millis / 1000.0
    return f"(TIMESTAMP 'epoch' + {seconds} * INTERVAL '1 second')::DATE"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "TIMESTAMP" or column_type == "TIMESTAMP WITHOUT TIME ZONE":
        return convert_millis_to_timestamp(millis)
    elif column_type == "TIMESTAMPTZ" or column_type == "TIMESTAMP WITH TIME ZONE":
        return convert_millis_to_timestamptz(millis)
    elif column_type == "DATE":
        return convert_millis_to_date(millis)
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "millis": millis},
    )


def convert_value_for_comparison(column_value: str, column_type: str) -> str:
    if column_type == "DATE":
        return f"'{column_value}'"
    elif "TIMESTAMP" in column_type:
        return f"'{column_value}'"
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

    interval_unit = "day" if normalized_interval == "DAILY" else "week"
    return (
        f"date_trunc('{interval_unit}', "
        f"convert_timezone('UTC', '{timezone_name}', {timestamp_column}))"
    )
