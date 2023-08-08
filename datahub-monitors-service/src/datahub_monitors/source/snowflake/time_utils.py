from datahub_monitors.exceptions import InvalidParametersException


def convert_millis_to_date(millis: int) -> str:
    return f"DATE(TO_TIMESTAMP({millis}, 3))"


def convert_millis_to_timestamp(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)"


def convert_millis_to_timestamp_tz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_TZ"


def convert_millis_to_timestamp_ltz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_LTZ"


def convert_millis_to_timestamp_ntz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_NTZ"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "DATE":
        return convert_millis_to_date(millis)
    elif column_type == "TIMESTAMP":
        return convert_millis_to_timestamp(millis)
    elif column_type == "TIMESTAMP_TZ":
        return convert_millis_to_timestamp_tz(millis)
    elif column_type == "TIMESTAMP_LTZ":
        return convert_millis_to_timestamp_ltz(millis)
    elif column_type == "TIMESTAMP_NTZ":
        return convert_millis_to_timestamp_ntz(millis)
    elif column_type == "DATETIME":
        return convert_millis_to_timestamp_ntz(millis)
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "millis": millis},
    )


def convert_value_for_comparison(column_value: str, column_type: str) -> str:
    if column_type == "DATE":
        return f"TO_DATE('{column_value}')"
    elif column_type == "DATETIME":
        return f"TO_DATETIME('{column_value}')"
    elif "TIMESTAMP" in column_type:
        return f"TO_TIMESTAMP('{column_value}')"
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "column_value": column_value},
    )
