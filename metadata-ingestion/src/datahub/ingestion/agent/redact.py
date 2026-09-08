from typing import Dict, List, Set, Tuple

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
    # Key-pair auth (Snowflake) and service-account JSON (GCP) both carry the
    # key under this name, nested one level down (`credential.private_key`), so
    # the top-level SecretStr sweep misses it even though the field is typed.
    "private_key",
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


def _maskable_forms(secret_values: Set[str]) -> List[str]:
    """Every form of every secret worth matching, longest first.

    Delegates to datahub.masking rather than restating what a secret can look
    like on the way out. Exact-substring matching on the raw value alone missed
    the case the probe most needs to cover: a driver echoing a connection
    string URL-encodes a password's special characters, so the raw value never
    appears in the error text at all. For the secret "p@ssword",
    "postgresql://u:p%40ssword@host" passed through unmasked -- and driver error
    text is precisely where credentials leak in practice.

    maskable_renderings also covers escaped forms and each substantial line of a
    multi-line value, which matters for a PEM private key echoed back one line
    at a time.
    """
    from datahub.masking.secret_registry import maskable_renderings

    forms: Set[str] = set()
    for secret in secret_values:
        if not secret or len(secret) < _MIN_SUBSTRING_SECRET_LEN:
            continue
        forms.update(
            form
            for form in maskable_renderings(secret)
            if len(form) >= _MIN_SUBSTRING_SECRET_LEN
        )
    return sorted(forms, key=len, reverse=True)


def redact(payload: object, secret_values: Set[str]) -> object:
    if not secret_values:
        return payload
    if isinstance(payload, str):
        # Best-effort defense-in-depth: this still over-masks when a secret
        # happens to equal a real identifier (a database named the same as the
        # password reports as "***"). Over-masking is the safe failure, so it
        # stays. The encoded and escaped forms ARE covered, via
        # _maskable_forms -- an earlier version of this comment claimed they
        # could not be, and the URL-encoded password it was describing was
        # leaking.
        redacted = payload
        # A short secret is compared whole and never as a substring: masking a
        # one-to-three character value inside longer text corrupts every
        # identifier containing it ("name" -> "n***me") while masking nothing
        # plausibly a credential. Its encoded forms are not considered either,
        # for the same reason.
        for secret in secret_values:
            if secret and len(secret) < _MIN_SUBSTRING_SECRET_LEN:
                if redacted == secret:
                    return _MASK
        # Longest first, across every form of every secret. Two registered
        # secrets can overlap -- a password and a connection string containing
        # it -- and replacing the shorter first destroys the match for the
        # longer, leaving its tail in the output ("***SECRETTAIL"). Set
        # iteration order is arbitrary, so without the ordering the leak is real
        # but intermittent.
        for form in _maskable_forms(secret_values):
            if form in redacted:
                redacted = redacted.replace(form, _MASK)
        return redacted
    if isinstance(payload, dict):
        return {
            redact(k, secret_values): redact(v, secret_values)
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [redact(v, secret_values) for v in payload]
    return payload
