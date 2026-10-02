import re
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Sequence,
    Set,
    Tuple,
)

# Public so a reader of redacted output (filter_input) can recognise it.
MASK = "***"

# Shorter values are masked only as a whole string, never as a substring: a
# one-character secret would turn "name" into "n***me" and mask nothing.
_MIN_SUBSTRING_SECRET_LEN = 4

# Result columns whose values name a person rather than describe shape. A
# catalog relation can be worth admitting for its structure while one column is
# identity; masking the column keeps the answer. Read by two layers: sql_gate
# refuses a query naming one, and mask_identity_columns covers `SELECT *`.
#
# Matched on the whole name, case-insensitively: a substring rule on "user"
# would mask information_schema's user_defined_type_* columns. A newly found
# identity column is added here.
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
        # Who was granted what, and by whom: account or role names (roles are
        # often named after people), readable through information_schema's
        # *_privileges and role views on every SQL connector.
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


def mask_identity_columns(
    columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> List[List[Any]]:
    """Replace values in identity columns with the redaction marker. The column
    is kept, so the caller sees a value withheld rather than a column missing."""
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
                copy[i] = MASK
        out.append(copy)
    return out


# Key fragments that mark a config value as a credential.
SENSITIVE_KEY_HINTS: Tuple[str, ...] = (
    "password",
    "sasl",
    "secret",
    "token",
    "basic.auth.user.info",
    "ssl.key",
    # Key-pair auth and service-account JSON nest the key
    # (`credential.private_key`), out of a top-level SecretStr sweep's reach.
    "private_key",
    # Spellings that are not substrings of the above. Bare "key" is absent: it
    # would match partition_key, primary_key and key_path.
    "passwd",
    "api_key",
    "apikey",
    "access_key",
    # Catalog properties (`s3.access-key-id`, `adls.account-key`) match after
    # normalize_key.
    "account_key",
    # Half of an API credential pair.
    "client_id",
    # Not "credential": see _SCALAR_ONLY_KEY_HINTS.
)

# A name heuristic because it runs over the raw recipe, before any config class
# has typed a field and whether or not a field is SecretStr. ConfigModel's
# SecretStr set is a second source, not a replacement.
_SENSITIVE_KEY_HINTS = SENSITIVE_KEY_HINTS

# Hints that count only when the value is a string: a service-account
# `credential` object holds project_id beside private_key (caught by its own
# hint), and masking the project id would corrupt every line of output.
_SCALAR_ONLY_KEY_HINTS: Tuple[str, ...] = ("credential",)


def collect_secret_values(
    resolved_config: Dict[str, object], secret_field_names: Set[str]
) -> Set[str]:
    values: Set[str] = set()
    for name in secret_field_names:
        value = resolved_config.get(name)
        if isinstance(value, str) and value:
            values.add(value)
    return values


def _is_scalar_only_secret_key(key: object) -> bool:
    name = normalize_key(key)
    if name.endswith(_NOT_THE_SECRET_SUFFIXES):
        return False
    # Ends with the hint, so `credential_source` is not treated as the secret.
    return any(name.rstrip("s").endswith(h) for h in _SCALAR_ONLY_KEY_HINTS)


def collect_nested_secret_values(
    obj: object, hints: Tuple[str, ...], under_sensitive: bool = False
) -> Set[str]:
    """String values under a key holding a sensitive hint, recursively: free-form
    dict fields (a client's consumer config) are not typed SecretStr.
    `under_sensitive` carries a parent's verdict down, since everything beneath a
    sensitive key (`credential: {private_key: {pem: ...}}`) is the secret.
    """
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            sensitive = under_sensitive or any(h in normalize_key(k) for h in hints)
            if isinstance(v, str):
                if v and (sensitive or _is_scalar_only_secret_key(k)):
                    found.add(v)
            else:
                found |= collect_nested_secret_values(v, hints, sensitive)
    elif isinstance(obj, list):
        for item in obj:
            found |= collect_nested_secret_values(item, hints, under_sensitive)
    return found


# Suffixes that make a credential-ish key an identifier, location or reference
# rather than the credential (`private_key_id`, `private_key_path`).
_NOT_THE_SECRET_SUFFIXES = ("_id", "_path", "_file", "_filename", "_url", "_uri")


def collect_nested_credential_values(
    obj: object, hints: Tuple[str, ...], under_sensitive: bool = False
) -> Set[str]:
    """Like collect_nested_secret_values, but for detecting a plaintext secret,
    where a false positive sends an author to fix a correct recipe. A dotted key
    is judged on its last segment (`sasl.mechanism` no, `sasl.password` yes);
    a dotted hint (`ssl.key`) is matched against the whole key.
    """
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k).lower()
            leaf = key.rsplit(".", 1)[-1]
            # The suffix rule applies to the leaf even under a sensitive parent:
            # `credential.private_key_id` is an identifier wherever it sits.
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
    """Every form of every secret worth matching, longest first, from
    datahub.masking: URL-encoded and escaped forms (a driver echoing a
    connection string encodes the password), and each substantial line of a
    multi-line value such as a PEM key."""
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


def normalize_key(key: object) -> str:
    """One spelling for config keys, so `s3.access-key-id` meets underscore hints."""
    return re.sub(r"[-.]", "_", str(key).lower())


# Shapes that carry a secret whatever its value: an ADC or IAM-role recipe
# registers no values to match. Userinfo runs to the last `@` of the authority.
_URL_USERINFO = re.compile(r"(?<=://)[^/\s]*@")
# The optional key prefix lets `client_secret`, `auth_token` and camelCase
# `secretKey` match, while the lookbehind keeps it from starting mid-word
# (and keeps the scan linear on long inputs).
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?<![A-Za-z0-9_])([A-Za-z0-9_]*?(?:"
    r"password|passwd|pwd|secret[_-]?(?:access[_-]?)?key|secret|token"
    r"|api[_-]?key|access[_-]?key(?:[_-]?id)?|private[_-]?key|account[_-]?key"
    r"|signature|sig|credential"
    r"))([\"']?(?:\s*[=:]\s*|%3[Dd]))"
    # An ODBC braced value (`PWD={a;b}`) may hold `;`, so it is read to its
    # closing brace. `{` is excluded inside it so an unterminated brace stops
    # at the next one instead of rescanning the rest of the input.
    r"(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|\{[^{}\n]*\}|[^\s&;,]+)"
)
_BEARER = re.compile(r"(?i)\b(bearer)\s+([A-Za-z0-9._~+/=-]+)")
# The keyword alone is case-insensitive; the token must look like base64 --
# eight or more characters with a digit, an uppercase letter, `+`, `/` or `=`
# -- so prose ("basic connectivity failed") is left alone.
_BASIC = re.compile(
    r"\b((?i:basic))\s+"
    r"(?=[A-Za-z0-9+/]*[0-9A-Z+/=])([A-Za-z0-9+/]{8,}={0,2})"
)
# `Authorization: Token <value>`, the scheme Django REST APIs and others use.
_AUTHORIZATION_TOKEN = re.compile(
    r"(?i)\b(authorization[\"']?\s*[:=]\s*[\"']?token)\s+([A-Za-z0-9._~+/=-]+)"
)
_AWS_KEY_ID = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
# Tokens recognisable by their prefix: JWTs, GitHub and Slack tokens. Not ARNs
# or account ids, which callers supply and read back.
_PREFIXED_TOKEN = re.compile(
    r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}"
    r"|\bgh[pousr]_[A-Za-z0-9]{20,}"
    r"|\bxox[abprs]-[A-Za-z0-9-]{10,}"
)
# The body class stops at the first non-base64 character, so an unterminated
# or truncated key is still masked without a lazy scan to a missing END marker.
_PEM_BLOCK = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----[A-Za-z0-9+/=\s]*"
    r"(?:-----END [A-Z ]*PRIVATE KEY-----)?"
)
# pydantic's `input_value='...'` suffix, in text a source builds itself: its
# repr truncates mid-value, so a registered secret no longer matches. Bounded to
# stay linear; with no `input_type` in reach, the rest of the line goes.
_PYDANTIC_INPUT = re.compile(r"(input_value=)[^\n]{0,400}?(?=, input_type=)")
_PYDANTIC_INPUT_UNTERMINATED = re.compile(r"(input_value=)(?!\*\*\*, )[^\n]*")
# A value that is really the next word of a diagnostic ("Invalid password:
# authentication failed", "access_key: field required"). Masking it would
# swallow the explanation the caller needs. Kept short on purpose.
_DIAGNOSTIC_WORDS = frozenset(
    [
        "required",
        "missing",
        "invalid",
        "failed",
        "not",
        "none",
        "null",
        "empty",
        "expired",
        "denied",
        "incorrect",
        "authentication",
        "is",
        "was",
        "must",
        "field",
        "token",
    ]
)


def _is_diagnostic(value: str) -> bool:
    return value.strip("\"'").lower() in _DIAGNOSTIC_WORDS


def _mask_assignment(m: "re.Match[str]") -> str:
    if _is_diagnostic(m.group(3)):
        return m.group(0)
    return f"{m.group(1)}{m.group(2)}{MASK}"


def _mask_scheme(m: "re.Match[str]") -> str:
    if _is_diagnostic(m.group(2)):
        return m.group(0)
    return f"{m.group(1)} {MASK}"


def scrub_text(text: str, secret_values: Set[str]) -> str:
    """Remove registered secret values, then credential-shaped substrings.

    For free text only; structured payloads go through `redact`, so a view
    definition containing `password=` is not rewritten. Registered values go
    first: a structural pass may consume only part of a secret.
    """
    redacted = redact(text, secret_values)
    assert isinstance(redacted, str)
    out = _PYDANTIC_INPUT.sub(r"\1" + MASK, redacted)
    out = _PYDANTIC_INPUT_UNTERMINATED.sub(r"\1" + MASK, out)
    out = _PEM_BLOCK.sub(MASK, out)
    out = _URL_USERINFO.sub(MASK + "@", out)
    out = _SECRET_ASSIGNMENT.sub(_mask_assignment, out)
    out = _BEARER.sub(_mask_scheme, out)
    out = _BASIC.sub(_mask_scheme, out)
    out = _AUTHORIZATION_TOKEN.sub(_mask_scheme, out)
    out = _PREFIXED_TOKEN.sub(MASK, out)
    return _AWS_KEY_ID.sub(MASK, out)


def scrub_strings(obj: object, secret_values: Set[str]) -> object:
    """scrub_text over every string in a JSON-shaped value (driver text lives in
    arbitrary fields of a test-connection report). Keys are left as they are."""
    if isinstance(obj, str):
        return scrub_text(obj, secret_values)
    if isinstance(obj, dict):
        return {k: scrub_strings(v, secret_values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub_strings(v, secret_values) for v in obj]
    return obj


def redact(payload: object, secret_values: Set[str]) -> object:
    if not secret_values:
        return payload
    if isinstance(payload, str):
        # Over-masks a real identifier equal to a secret: the safe failure.
        redacted = payload
        # Short secrets are compared whole (_MIN_SUBSTRING_SECRET_LEN).
        for secret in secret_values:
            if secret and len(secret) < _MIN_SUBSTRING_SECRET_LEN:
                if redacted == secret:
                    return MASK
        # Longest first: replacing a password before the connection string
        # holding it would leave the string's tail in the output.
        for form in _maskable_forms(secret_values):
            if form in redacted:
                redacted = redacted.replace(form, MASK)
        return redacted
    if isinstance(payload, dict):
        # Built incrementally: two keys can redact to one string, and a
        # comprehension would silently drop one.
        out: Dict[object, object] = {}
        for key, value in payload.items():
            redacted_key = redact(key, secret_values)
            redacted_value = redact(value, secret_values)
            if redacted_key in out:
                # Suffixed, so the caller still sees there was more than one.
                suffix = 2
                while f"{redacted_key}~{suffix}" in out:
                    suffix += 1
                redacted_key = f"{redacted_key}~{suffix}"
            out[redacted_key] = redacted_value
        return out
    if isinstance(payload, list):
        return [redact(v, secret_values) for v in payload]
    return payload
