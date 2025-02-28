from datahub_executor.common.exceptions import InvalidParametersException


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
