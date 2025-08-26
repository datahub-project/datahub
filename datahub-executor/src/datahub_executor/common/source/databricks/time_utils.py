from datahub_executor.common.exceptions import InvalidParametersException


def convert_millis_to_date(millis: int) -> str:
    return f"TO_DATE(FROM_UNIXTIME({millis}/1000))"


def convert_millis_to_timestamp(millis: int) -> str:
    return f"TO_TIMESTAMP(FROM_UNIXTIME({millis}/1000))"


def convert_millis_to_timestamp_ntz(millis: int) -> str:
    return f"TO_TIMESTAMP(FROM_UNIXTIME({millis}/1000))::TIMESTAMP_NTZ"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "DATE":
        return convert_millis_to_date(millis)
    elif column_type == "TIMESTAMP":
        return convert_millis_to_timestamp(millis)
    elif column_type == "TIMESTAMP_NTZ":
        return convert_millis_to_timestamp_ntz(millis)
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "millis": millis},
    )


def convert_value_for_comparison(column_value: str, column_type: str) -> str:
    if column_type == "DATE":
        return f"TO_DATE('{column_value}')"
    elif column_type == "TIMESTAMP":
        return f"TO_TIMESTAMP('{column_value}')"
    elif column_type == "TIMESTAMP_NTZ":
        return f"TO_TIMESTAMP('{column_value}')::TIMESTAMP_NTZ"
    raise InvalidParametersException(
        message=f"Unsupported column type {column_type} provided!",
        parameters={"column_type": column_type, "column_value": column_value},
    )
