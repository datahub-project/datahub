from typing import Any, Dict, FrozenSet, List, Sequence, Set, Tuple

from datahub.masking.secret_registry import (
    SENSITIVE_KEY_HINTS,
    plain_config_values,
)

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

# Result columns whose values name a person rather than describe shape. A
# catalog relation can be admitted for the structure it carries and still have
# one column that is identity: ACCOUNT_USAGE.ACCESS_HISTORY is the case that
# forced this -- it is how Snowflake lineage works, and whether it is empty is
# the difference between "lineage will work" and "lineage silently returns
# nothing", so refusing the whole relation costs a real capability. Masking the
# column keeps the answer and drops the identity.
#
# Matched on the whole column name, case-insensitively, not as a substring:
# over-masking is its own failure. `owner` is a role on most catalog views and
# stays readable; a column literally called `email` does not.
#
# Read by TWO layers, and it has to be, because neither covers the other:
#
#   sql_gate refuses a query that NAMES one of these columns. Masking here
#   matches the driver's output names, so the caller picks the name and
#   therefore picks whether masking applies -- `USER_NAME AS u`,
#   `LOWER(user_name)`, `ARRAY_AGG(user_name)` all came back in the clear
#   until the gate started looking at the projection.
#
#   This masker covers `SELECT *`, which names no column for the gate to
#   refuse and whose output names ARE the real ones.
WITHHELD_COLUMN_NAMES: FrozenSet[str] = frozenset(
    {
        # Who the person is.
        "user_name",
        "username",
        "user_email",
        "email",
        "email_address",
        "login_name",
        "display_name",
        # Who was granted what, and by whom. These are reachable on a DEFAULT
        # recipe and were coming back in the clear:
        # `SELECT * FROM information_schema.table_privileges` is permitted by
        # the base catalog scope on every SQL connector, and its `grantor` and
        # `grantee` are account names. Same for role_table_grants,
        # enabled_roles, applicable_roles and Snowflake's object_privileges.
        #
        # A principal here is a role name on most engines, which is not
        # obviously a person -- until you look at what roles are called in
        # practice. Treated as identity for the same reason `login_name` is.
        "grantee",
        "grantor",
        "granted_by",
        "grantee_name",
        "grantor_name",
        "granted_to",
        "role_name",
        "principal",
        "principal_name",
        "account_name",
    }
)

# Exact match, not substring, and that is load-bearing rather than lazy.
# `information_schema.tables` and `.columns` carry
# `user_defined_type_name`, `user_defined_type_catalog` and
# `user_defined_type_schema`; a substring rule on "user" would mask three
# ordinary type-metadata columns on every wildcard read of the two relations
# the probe exists to read. The cost of exact match is that this list has to
# be extended when a new identity-bearing catalog column is found -- which is
# the trade this comment exists to record, not one to fix by widening the
# match.


def mask_identity_columns(
    columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> List[List[Any]]:
    """Replace values in identity columns with the redaction marker.

    The column is kept, not dropped. An agent that cannot see USER_NAME should
    still know the view has one -- silently narrowing a result is the failure
    this interface exists to prevent, and a masked value says "withheld" where
    a missing column says nothing at all.
    """
    masked_at = [
        i
        for i, name in enumerate(columns)
        if str(name).lower() in WITHHELD_COLUMN_NAMES
    ]
    if not masked_at:
        return [list(row) for row in rows]
    out: List[List[Any]] = []
    for row in rows:
        copy = list(row)
        for i in masked_at:
            if i < len(copy) and copy[i] is not None:
                copy[i] = _MASK
        out.append(copy)
    return out


# The policy lives in datahub.masking so the executor can import it too --
# datahub.executor must not depend on this package. Aliased rather than
# re-spelled, so existing call sites and tests are unaffected.
_SENSITIVE_KEY_HINTS: Tuple[str, ...] = SENSITIVE_KEY_HINTS


def collect_secret_values(
    resolved_config: Dict[str, object], secret_field_names: Set[str]
) -> Set[str]:
    values: Set[str] = set()
    for name in secret_field_names:
        value = resolved_config.get(name)
        if isinstance(value, str) and value:
            values.add(value)
    return values


def collect_nested_secret_values(
    obj: object, hints: Tuple[str, ...], under_sensitive: bool = False
) -> Set[str]:
    """Recursively collect string values whose (dict) key contains a sensitive
    hint. Defense-in-depth for secrets that live in free-form dict config fields
    (e.g. Kafka's consumer_config) and so are not typed SecretStr.

    `under_sensitive` carries the parent's verdict down. Without it the
    decision was re-made from each child's own name, so a sensitive key
    holding a MAPPING lost everything inside it:

        token:      {access: ...}              -> not masked
        credential: {private_key: {pem: ...}}  -> not masked

    The second is the shape the private_key hint exists for, one level deeper
    than its comment assumes -- "nested one level down" holds only while the
    value is a string. Everything beneath a sensitive key is the secret, so
    the flag travels with the walk.
    """
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            sensitive = under_sensitive or any(h in str(k).lower() for h in hints)
            if isinstance(v, str):
                if v and sensitive:
                    found.add(v)
            else:
                found |= collect_nested_secret_values(v, hints, sensitive)
    elif isinstance(obj, list):
        for item in obj:
            found |= collect_nested_secret_values(item, hints, under_sensitive)
    return found


collect_plain_config_values = plain_config_values


# Suffixes that turn a credential-ish key name into something that is not the
# credential: an identifier, a location, or a reference to it. `private_key`
# is the hint that needs this -- it exists for GCP's `credential.private_key`,
# and matched `private_key_id` (public key metadata) and `private_key_path`
# (a filename) too, so a correct service-account recipe was reported as
# holding two plaintext secrets.
_NOT_THE_SECRET_SUFFIXES = ("_id", "_path", "_file", "_filename", "_url", "_uri")


def collect_nested_credential_values(
    obj: object, hints: Tuple[str, ...], under_sensitive: bool = False
) -> Set[str]:
    """Like collect_nested_secret_values, but for DETECTING rather than masking.

    The two want opposite errors. Masking everything under a `sasl`-ish key is
    right on the way out: over-masking costs some mangled output, under-masking
    leaks. Telling an author "this file holds a plaintext secret" is the other
    way round -- a false positive sends them to fix a correct recipe, and the
    only fix for `sasl.mechanism: PLAIN` is to stop setting a mandatory field.

    So a dotted key is judged on its LAST segment (`sasl.mechanism` ->
    `mechanism`, no match; `sasl.password` -> `password`, match), except for
    hints that are themselves dotted (`basic.auth.user.info`, `ssl.key`), which
    name a whole key and are matched against the whole key.
    """
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k).lower()
            leaf = key.rsplit(".", 1)[-1]
            # The suffix rule is about the LEAF's own name, so it still applies
            # under a sensitive parent: `credential.private_key_id` is an
            # identifier wherever it sits. Inheriting sensitivity is what
            # reaches `credential.private_key.pem`, which has no such suffix.
            named = any((h in key) if "." in h else (h in leaf) for h in hints)
            sensitive = (under_sensitive or named) and not leaf.endswith(
                _NOT_THE_SECRET_SUFFIXES
            )
            if isinstance(v, str):
                if v and sensitive:
                    found.add(v)
            else:
                found |= collect_nested_credential_values(
                    v, hints, under_sensitive or named
                )
    elif isinstance(obj, list):
        for item in obj:
            found |= collect_nested_credential_values(item, hints, under_sensitive)
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
        # Built incrementally, not as a comprehension: two distinct keys can
        # redact to the same string (two secrets, two config keys named after
        # them), and a comprehension keeps only the last -- silently dropping
        # a field from the report. Losing a field without saying so is the
        # failure this whole interface exists to avoid, and it was happening
        # in the function whose job is to be careful.
        out: Dict[object, object] = {}
        for key, value in payload.items():
            redacted_key = redact(key, secret_values)
            redacted_value = redact(value, secret_values)
            if redacted_key in out:
                # Suffixed rather than merged: the caller cannot tell these
                # apart anyway (that is the point of redaction), but it must
                # be able to see that there was more than one.
                suffix = 2
                while f"{redacted_key}~{suffix}" in out:
                    suffix += 1
                redacted_key = f"{redacted_key}~{suffix}"
            out[redacted_key] = redacted_value
        return out
    if isinstance(payload, list):
        return [redact(v, secret_values) for v in payload]
    return payload
