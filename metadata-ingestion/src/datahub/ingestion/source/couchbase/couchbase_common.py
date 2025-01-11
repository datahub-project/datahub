from typing import Any, List, Tuple


def flatten(field_path: List[str], data: Any, truncate: bool = True) -> Tuple[str, Any]:
    if isinstance(data, dict):
        for key, value in data.items():
            field_path.append(key)
            yield from flatten(field_path, value)
            if isinstance(value, dict) or isinstance(value, list):
                del field_path[-1]
    elif isinstance(data, list):
        for value in data:
            yield from flatten(field_path, value, False)
    else:
        yield '.'.join(field_path), data
        if len(field_path) > 0 and truncate:
            del field_path[-1]
