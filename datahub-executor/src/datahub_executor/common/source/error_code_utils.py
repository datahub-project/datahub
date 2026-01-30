from typing import Iterable, Optional


def extract_response_code(error: BaseException) -> Optional[int]:
    for exc in _iter_exceptions(error):
        response_code = _extract_int_attr(exc, "status_code")
        if response_code is not None:
            return response_code
        response_code = _extract_int_attr(exc, "http_status")
        if response_code is not None:
            return response_code
        response_code = _extract_int_attr(exc, "http_code")
        if response_code is not None:
            return response_code
        response_code = _extract_int_attr(exc, "code")
        if response_code is not None:
            return response_code
        response_code = _extract_int_attr(exc, "status")
        if response_code is not None:
            return response_code
    return None


def extract_sqlstate(error: BaseException) -> Optional[str]:
    for exc in _iter_exceptions(error):
        sqlstate = _extract_str_attr(exc, "sqlstate")
        if sqlstate is not None:
            return sqlstate
        sqlstate = _extract_str_attr(exc, "pgcode")
        if sqlstate is not None:
            return sqlstate
        sqlstate = _extract_str_attr(exc, "C")
        if sqlstate is not None:
            return sqlstate
    return None


def _iter_exceptions(error: BaseException) -> Iterable[BaseException]:
    seen = set()
    current: Optional[BaseException] = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _extract_int_attr(error: BaseException, attr_name: str) -> Optional[int]:
    value = getattr(error, attr_name, None)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _extract_str_attr(error: BaseException, attr_name: str) -> Optional[str]:
    value = getattr(error, attr_name, None)
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else None
    return None
