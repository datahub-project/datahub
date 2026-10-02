"""Which exception text a probe may show, and how the rest is named.

Text is trusted by type only. A framework type (TRUSTED_TYPES, which the SQL
and API gate refusals subclass) carries a message written for the caller.
Anything else raised while a provider is opened, called or closed -- reused
ingestion code, a driver, an SDK, or the provider's own plain ValueError --
is named by `foreign_label` instead: its class name and at most one short
code, because its text is where connection strings, token-endpoint bodies and
query literals come from.

Codes come from cross-library conventions only (an HTTP status, a SQLSTATE,
an errno). A provider that knows its vendor's error shape declares
`probe_error_code(exc)` on its class, and that is asked first. Attributes are
read with plain getattr under try/except: the threat is accidental leakage,
and a strict pattern on every value is what keeps a code from carrying text.

`DATAHUB_PROBE_VERBOSE_LOGS=1` appends each foreign exception's scrubbed text
to its label, for a person debugging a connector locally.
"""

import copy
import re
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Type

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)

TRUSTED_TYPES: Tuple[Type[BaseException], ...] = (
    ProbeArgumentError,
    ProbeSoftError,
    ProbeReadFailed,
    ProbeConnectionError,
    ProbeInternalError,
)

# Python defects: after run_probe_method has coerced the arguments, these mean
# the code misread something, not that the caller's input was wrong (exit 1).
DEFECT_TYPES: Tuple[Type[BaseException], ...] = (
    TypeError,
    KeyError,
    AttributeError,
    AssertionError,
    IndexError,
    NameError,
)

# What the CLI reads as "your input was wrong" (exit 2) when raised bare.
_ARGUMENT_TYPES: Tuple[Type[BaseException], ...] = (ValueError, re.error)

# The shape every provider-read code must match in full: a name and at most
# one short token, so a reader that returns a message by mistake shows nothing.
_PROVIDER_CODE = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,47}( [A-Za-z0-9_.-]{1,32})?")
_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
_MAX_ERRNO = 999_999

# How many `raise ... from` links a code is looked for in, and how many
# cause/context links the backstop walks.
_MAX_CODE_LINKS = 8
_MAX_CHAIN_LINKS = 32
# Shorter renderings carry too little to leak and would match innocently
# inside the message (a one-letter name, a row number).
_MIN_RENDERING = 6


def is_trusted(exc: BaseException) -> bool:
    """Whether the framework may show this exception's text."""
    return isinstance(exc, TRUSTED_TYPES)


def _attr(obj: object, name: str) -> object:
    # A foreign property or __getattr__ may raise with the text it holds.
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


def _cause_links(exc: BaseException) -> List[BaseException]:
    """`exc` and its `raise ... from` causes, outermost first.

    Never the context: an exception raised while handling another is not that
    other failure, and a 429 handled before an unrelated ConnectionError would
    label it as rate limiting.
    """
    links: List[BaseException] = []
    link: object = exc
    while isinstance(link, BaseException) and len(links) < _MAX_CODE_LINKS:
        if any(link is seen for seen in links):
            break
        links.append(link)
        link = _attr(link, "__cause__")
    return links


def http_status_code(value: object) -> Optional[str]:
    """`HTTP n` when `value` is an int status from 100 to 599. A bool is an
    int, and True is not a status."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    number = int(value)
    return f"HTTP {number}" if 100 <= number <= 599 else None


def sqlstate_code(value: object) -> Optional[str]:
    """`SQLSTATE x` when `value` is a SQLSTATE: five capitals or digits, at
    least one a digit (five capitals alone is a word, and words are what a
    code must not carry)."""
    match = _SQLSTATE.fullmatch(value) if isinstance(value, str) else None
    if match is None or not any(c.isdigit() for c in match.group(0)):
        return None
    return f"SQLSTATE {match.group(0)}"


def errno_code(value: object) -> Optional[str]:
    """`errno n` when `value` is a non-negative error number of at most six
    digits."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    number = int(value)
    return f"errno {number}" if 0 <= number <= _MAX_ERRNO else None


def _generic_code(exc: BaseException) -> Optional[str]:
    for status in (
        _attr(_attr(exc, "response"), "status_code"),
        _attr(exc, "status_code"),
        _attr(exc, "status"),
    ):
        code = http_status_code(status)
        if code:
            return code
    return sqlstate_code(_attr(exc, "sqlstate")) or errno_code(_attr(exc, "errno"))


def _provider_code(exc: BaseException, provider_cls: Optional[type]) -> Optional[str]:
    reader = _attr(provider_cls, "probe_error_code")
    if not callable(reader):
        return None
    try:
        code = reader(exc)
    except Exception:
        return None
    match = _PROVIDER_CODE.fullmatch(code) if isinstance(code, str) else None
    # The matched characters, not `code` itself: a str subclass may render as
    # anything.
    return match.group(0) if match else None


def _first(
    links: Iterable[BaseException], read: Callable[[BaseException], Optional[str]]
) -> Optional[str]:
    return next((code for code in map(read, links) if code), None)


def foreign_label(exc: BaseException, provider_cls: Optional[type] = None) -> str:
    """How an untrusted exception is named in place of its text: its class
    name, plus one short code when it or its cause chain carries one.

    `ProgrammingError; SQLSTATE 42P01`, `HTTPError; HTTP 403`. The code tells
    the caller what kind of failure it was (missing relation, permission,
    throttling) without the message. The provider's own reader is tried on
    every link before the generic one is. Never raises: a failure reading the
    exception leaves the class name alone.
    """
    name = type(exc).__name__
    try:
        links = _cause_links(exc)
        code = _first(links, lambda link: _provider_code(link, provider_cls))
        code = code or _first(links, _generic_code)
        label = f"{name}; {code}" if code else name
        if get_probe_verbose_logs():
            return f"{label}: {scrub_text(str(exc), set())}"
        return label
    except Exception:
        return name


def _foreign_family(exc: BaseException, message: str) -> Exception:
    if isinstance(exc, DEFECT_TYPES):
        return ProbeInternalError(message)
    if isinstance(exc, _ARGUMENT_TYPES):
        return ProbeArgumentError(message)
    return ProbeConnectionError(message)


def classify_foreign(
    exc: BaseException, context: str, provider_cls: Optional[type] = None
) -> Exception:
    """The framework exception an untrusted failure in a provider call is
    reported as, named by foreign_label.

    Withholding the text must not also move the exit code, so this keeps the
    family the bare exception would have reached the CLI as: a defect exits 1,
    the ValueError family 2, and everything else (drivers, SDKs, HTTP and
    permission errors) 3.
    """
    return _foreign_family(
        exc, f"{context} failed ({foreign_label(exc, provider_cls)})"
    )


def _foreign_in_chain(exc: BaseException) -> List[BaseException]:
    """Every untrusted exception in `exc`'s cause and context chain."""
    found: List[BaseException] = []
    seen: Set[int] = {id(exc)}
    pending = [exc]
    while pending and len(seen) <= _MAX_CHAIN_LINKS:
        current = pending.pop()
        for name in ("__cause__", "__context__"):
            link = _attr(current, name)
            if not isinstance(link, BaseException) or id(link) in seen:
                continue
            seen.add(id(link))
            if not is_trusted(link):
                found.append(link)
            pending.append(link)
    return found


def withhold_foreign_text(
    exc: BaseException, provider_cls: Optional[type] = None
) -> str:
    """`str(exc)` with the text of any untrusted exception in its chain
    replaced by `(label)`.

    A trusted type is trusted wherever it is raised, which makes
    `ProbeConnectionError(f"login failed: {exc}")` around a driver error carry
    the driver's text out. Only text that appears verbatim can be caught (the
    str or repr of the foreign exception); a message built from parts of it
    cannot, which is why the guide says never to interpolate an exception you
    did not raise. A lookup error is matched by its repr only, and a rendering
    under _MIN_RENDERING characters not at all (see there).
    """
    message = str(exc)
    labels: Dict[str, str] = {}
    for foreign in _foreign_in_chain(exc):
        label = f"({foreign_label(foreign, provider_cls)})"
        # A lookup error's str is the key it missed, which is the caller's own
        # argument quoted back to them, not the failure's text.
        renders = (repr,) if isinstance(foreign, LookupError) else (repr, str)
        for render in renders:
            try:
                rendering = render(foreign)
            except Exception:
                # A rendering that raises cannot be in the message either.
                continue
            if len(rendering) >= _MIN_RENDERING:
                labels.setdefault(rendering, label)
    if not labels:
        return message
    # One pass, longest first: a repr contains its str, and a label must not
    # be rewritten by a later, shorter match.
    pattern = re.compile(
        "|".join(re.escape(r) for r in sorted(labels, key=len, reverse=True))
    )
    return pattern.sub(lambda match: labels[match.group(0)], message)


def police_authored(
    exc: BaseException, provider_cls: Optional[type] = None
) -> Optional[BaseException]:
    """A replacement for a trusted exception whose message quotes an untrusted
    one, or None when it may be raised as it is.

    The replacement keeps the exception's type, so the exit code does not
    move. A type that cannot be rebuilt with a plain message (its constructor
    takes other arguments, or its __str__ ignores args) is replaced by the
    framework type of the same exit family instead.
    """
    message = withhold_foreign_text(exc, provider_cls)
    if message == str(exc):
        return None
    try:
        rebuilt = copy.copy(exc)
        rebuilt.args = (message,)
        if str(rebuilt) == message:
            return rebuilt
    except Exception:
        pass
    for family in (ProbeInternalError, ProbeReadFailed, ProbeConnectionError):
        if isinstance(exc, family):
            return family(message)
    return _foreign_family(exc, message)
