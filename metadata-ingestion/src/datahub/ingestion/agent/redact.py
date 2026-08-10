from typing import Dict, Set, Tuple

_MASK = "***"

# Below this length a value is matched only against a whole string, never as a
# substring. Substring masking is deliberately blunt -- it is what catches a
# password embedded in a driver error or a connection string -- but on a very
# short value it corrupts every identifier and dict key that happens to contain
# those characters ("name" -> "n***me" for a one-character secret), producing
# output an agent cannot read while masking nothing that could plausibly be a
# credential. Whole-value matches are still masked at any length, so this
# narrows the blast radius rather than dropping protection.
_MIN_SUBSTRING_SECRET_LEN = 4

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
        # Best-effort defense-in-depth: this still over-masks when a secret
        # happens to equal a real identifier (a database named the same as the
        # password reports as "***"), and it cannot catch a secret that was
        # transformed or encoded on the way out. Over-masking is the safe
        # failure, so it stays.
        redacted = payload
        for secret in secret_values:
            if not secret:
                continue
            if len(secret) < _MIN_SUBSTRING_SECRET_LEN:
                if redacted == secret:
                    redacted = _MASK
                continue
            if secret in redacted:
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
