from typing import Dict, Set, Tuple

_MASK = "***"

_SENSITIVE_KEY_HINTS: Tuple[str, ...] = (
    "password",
    "sasl",
    "secret",
    "token",
    "basic.auth.user.info",
    "ssl.key",
)


def collect_secret_values(
    resolved_config: Dict[str, object], secret_field_names: Set[str]
) -> Set[str]:
    values: Set[str] = set()
    for name in secret_field_names:
        value = resolved_config.get(name)
        if isinstance(value, str) and value:
            values.add(value)
    return values


def collect_nested_secret_values(obj: object, hints: Tuple[str, ...]) -> Set[str]:
    """Recursively collect string values whose (dict) key contains a sensitive
    hint. Defense-in-depth for secrets that live in free-form dict config fields
    (e.g. Kafka's consumer_config) and so are not typed SecretStr."""
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and v and any(h in str(k).lower() for h in hints):
                found.add(v)
            else:
                found |= collect_nested_secret_values(v, hints)
    elif isinstance(obj, list):
        for item in obj:
            found |= collect_nested_secret_values(item, hints)
    return found


def redact(payload: object, secret_values: Set[str]) -> object:
    if not secret_values:
        return payload
    if isinstance(payload, str):
        # Substring redaction is best-effort defense-in-depth: it can over-mask
        # short values and won't catch secrets that were transformed or encoded.
        redacted = payload
        for secret in secret_values:
            if secret and secret in redacted:
                redacted = redacted.replace(secret, _MASK)
        return redacted
    if isinstance(payload, dict):
        return {
            redact(k, secret_values): redact(v, secret_values)
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [redact(v, secret_values) for v in payload]
    return payload
