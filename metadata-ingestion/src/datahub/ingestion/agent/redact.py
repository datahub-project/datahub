from typing import Dict, Set

_MASK = "***"


def collect_secret_values(
    resolved_config: Dict[str, object], secret_field_names: Set[str]
) -> Set[str]:
    values: Set[str] = set()
    for name in secret_field_names:
        value = resolved_config.get(name)
        if isinstance(value, str) and value:
            values.add(value)
    return values


def redact(payload: object, secret_values: Set[str]) -> object:
    if not secret_values:
        return payload
    if isinstance(payload, str):
        redacted = payload
        for secret in secret_values:
            if secret and secret in redacted:
                redacted = redacted.replace(secret, _MASK)
        return redacted
    if isinstance(payload, dict):
        return {k: redact(v, secret_values) for k, v in payload.items()}
    if isinstance(payload, list):
        return [redact(v, secret_values) for v in payload]
    return payload
