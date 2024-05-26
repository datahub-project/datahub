import os


def get_boolean_env_variable(key: str, default: bool = False) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    elif value.lower() in ("true", "1"):
        return True
    else:
        return False
