"""Which exception text a probe may show, and how the rest is named.

Text is trusted by type only. A framework type (TRUSTED_TYPES, which the SQL
and API gate refusals subclass) carries a message written for the caller.
Anything else raised while a provider is opened, called or closed, or while a
config hook runs -- reused ingestion code, a driver, an SDK, or the
connector's own plain ValueError -- is named by `foreign_label` instead: its
class name and at most one short
code, because its text is where connection strings, token-endpoint bodies and
query literals come from.

Codes come from cross-library conventions only (an HTTP status, a SQLSTATE,
an errno). A provider that knows its vendor's error shape declares
`probe_error_code(exc)` on its class, and that is asked first. Attributes are
read with plain getattr under try/except: the threat is accidental leakage,
and a strict pattern on every value is what keeps a code from carrying text.

`DATAHUB_PROBE_VERBOSE_LOGS=1` puts each foreign exception's scrubbed text
after its label (see name_foreign), for a person debugging a connector
locally.
"""

import binascii
import copy
import errno
import json
import re
import socket
import ssl
from typing import (
    AbstractSet,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import pydantic

from datahub.configuration.env_vars import get_probe_verbose_logs
from datahub.ingestion.agent.redact import scrub_text
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)

T = TypeVar("T")

TRUSTED_TYPES: Tuple[Type[BaseException], ...] = (
    ProbeArgumentError,
    ProbeSoftError,
    ProbeReadFailed,
    ProbeConnectionError,
    ProbeInternalError,
)

# Never a failure of the source: an interrupt is the user's and GeneratorExit
# the interpreter's, so both reach the caller unchanged, from a provider and
# from a label reader alike. Everything else raised, SystemExit included, is
# policed.
PASS_THROUGH: Tuple[Type[BaseException], ...] = (KeyboardInterrupt, GeneratorExit)

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

# Failures reading what the source sent: a ValueError by type, but the
# source's -- an SSO login page parsed as JSON, a response failing its model,
# bytes or base64 that do not decode -- so exit 3. Checked before
# _ARGUMENT_TYPES; an OSError can be a ValueError too (io.UnsupportedOperation).
_SOURCE_DATA_TYPES: Tuple[Type[BaseException], ...] = (
    OSError,
    UnicodeError,
    binascii.Error,
    json.JSONDecodeError,
    pydantic.ValidationError,
)

# What the CLI reads as "your input was wrong" (exit 2) when raised bare.
_ARGUMENT_TYPES: Tuple[Type[BaseException], ...] = (ValueError, re.error)

# The shape every provider-read code must match in full: a name with at most
# one dot (`AccessDenied`, `InvalidInstanceID.NotFound`), then at most one
# short token holding a digit (`SQLSTATE 42P01`, `errno 1146`). A reader that
# returns a message, a hostname or a phrase by mistake shows nothing.
_PROVIDER_CODE = re.compile(
    r"[A-Za-z][A-Za-z0-9_]{0,31}(?:\.[A-Za-z0-9_]{1,31})?"
    r"(?: (?=[A-Za-z_]*[0-9])[A-Za-z0-9_]{1,16})?"
)
_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
_MAX_ERRNO = 999_999

# How many `raise ... from` links a code is looked for in, and how many
# cause, context and group-child links the backstop walks.
_MAX_CODE_LINKS = 8
_MAX_CHAIN_LINKS = 32
# Shorter renderings carry too little to leak and would match innocently
# inside the message (a one-letter name, a row number).
_MIN_RENDERING = 6


def is_trusted(exc: BaseException) -> bool:
    """Whether the framework may show this exception's text."""
    return isinstance(exc, TRUSTED_TYPES)


def _attr(obj: object, name: str) -> object:
    # A foreign property or __getattr__ may raise, or exit, with the text it
    # holds.
    try:
        return getattr(obj, name, None)
    except PASS_THROUGH:
        raise
    except BaseException:
        return None


def _cause_links(exc: BaseException) -> List[BaseException]:
    """`exc` and its `raise ... from` causes, outermost first. Never the
    context: an exception raised while handling another is not that failure."""
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


def generic_error_code(exc: BaseException) -> Optional[str]:
    """The code `exc` itself carries by a cross-library convention: an HTTP
    status, else a SQLSTATE, else an errno."""
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
    except PASS_THROUGH:
        raise
    except BaseException:
        return None
    match = _PROVIDER_CODE.fullmatch(code) if isinstance(code, str) else None
    # The matched characters, not `code` itself: a str subclass may render as
    # anything.
    return match.group(0) if match else None


def _first(
    links: Iterable[BaseException], read: Callable[[BaseException], Optional[str]]
) -> Optional[str]:
    return next((code for code in map(read, links) if code), None)


# The label of an exception whose class cannot give a plain name.
_UNNAMED = "exception"


def foreign_code(
    exc: BaseException, provider_cls: Optional[type] = None
) -> Optional[str]:
    """The one short code foreign_label shows for `exc`: the provider's reader
    on every link of its cause chain, then the generic ones. Never raises."""
    try:
        links = _cause_links(exc)
        code = _first(links, lambda link: _provider_code(link, provider_cls))
        return code or _first(links, generic_error_code)
    except PASS_THROUGH:
        raise
    except BaseException:
        return None


def foreign_label(exc: BaseException, provider_cls: Optional[type] = None) -> str:
    """An untrusted exception's name in place of its text: its class, plus one
    short code from it or its cause chain (`ProgrammingError; SQLSTATE 42P01`),
    which says what kind of failure it was. The provider's reader is tried on
    every link before the generic ones. Never raises.
    """
    name = _UNNAMED
    try:
        # Read in here: a metaclass can make the class name run code. Exactly
        # str, since a subclass may render as anything.
        raw = type(exc).__name__
        name = raw if type(raw) is str else _UNNAMED
        code = foreign_code(exc, provider_cls)
        return f"{name}; {code}" if code else name
    except PASS_THROUGH:
        raise
    except BaseException:
        return name


# Why a source could not be opened, read from the stdlib network exception a
# driver keeps in its chain, and what the caller can do about each. Fixed
# text: nothing here comes from the exception but its type and errno.
NETWORK_REASON_HINTS: Dict[str, str] = {
    "HostNotResolved": (
        "the recipe's host name did not resolve; check its spelling, and the "
        "DNS or VPN this machine uses"
    ),
    "ConnectionRefused": (
        "nothing is listening at the recipe's host and port; check the port, "
        "and that the server is running"
    ),
    "Timeout": (
        "the host did not answer; a firewall, allowlist or private network "
        "may be dropping this machine's traffic, and retrying will not help "
        "until that changes"
    ),
    "HostUnreachable": (
        "there is no network route to the host from this machine; it may be "
        "on a private network"
    ),
    "TlsVerifyFailed": (
        "the server's TLS certificate failed verification; check the "
        "recipe's CA or TLS settings"
    ),
}

_UNREACHABLE_ERRNOS = frozenset({errno.EHOSTUNREACH, errno.ENETUNREACH})


def _network_reason_of(link: BaseException) -> Optional[str]:
    # Before OSError's own checks: a certificate failure is an OSError too.
    if isinstance(link, ssl.SSLCertVerificationError):
        return "TlsVerifyFailed"
    if isinstance(link, (socket.gaierror, socket.herror)):
        return "HostNotResolved"
    if isinstance(link, ConnectionRefusedError):
        return "ConnectionRefused"
    # socket.timeout is TimeoutError since Python 3.10.
    if isinstance(link, TimeoutError):
        return "Timeout"
    if isinstance(link, OSError) and _attr(link, "errno") in _UNREACHABLE_ERRNOS:
        return "HostUnreachable"
    return None


def network_reason(exc: BaseException) -> Optional[str]:
    """The NETWORK_REASON_HINTS key for the innermost stdlib network
    exception in `exc`'s chain, or None. Never raises.

    The chain is the one a traceback prints: each link's cause, else its
    context. The context counts here, unlike in foreign_code, because drivers
    such as PyMySQL raise their own error while handling the socket's without
    `from`; for a failure opening a source, the exception being handled is
    the failure. The innermost match wins, so pytds's TimeoutError raised
    from a ConnectionRefusedError reads as refused. Only types and errno are
    read, never text.
    """
    try:
        reason: Optional[str] = None
        seen: List[BaseException] = []
        link: object = exc
        while isinstance(link, BaseException) and len(seen) < _MAX_CHAIN_LINKS:
            if any(link is prior for prior in seen):
                break
            seen.append(link)
            reason = _network_reason_of(link) or reason
            cause = _attr(link, "__cause__")
            link = cause if cause is not None else _attr(link, "__context__")
        return reason
    except PASS_THROUGH:
        raise
    except BaseException:
        return None


# SQLSTATE class 42, syntax error or access rule violation: a query the
# caller wrote was wrong (a misspelled column, a missing relation, a grant it
# lacks), not the source.
_CALLER_SQLSTATE_CLASS = "SQLSTATE 42"
# The same failures from a MySQL-protocol server, whose drivers report the
# server's error number and no SQLSTATE: each is class 42 in MySQL's error
# reference. Server numbers start at 1000, above any OS errno. Errors a
# recipe's own connection settings can cause (1044, 1049: the database it
# names) are left out: those are not the query's.
_CALLER_MYSQL_ERRNOS = frozenset(
    {
        1054,  # ER_BAD_FIELD_ERROR, 42S22: unknown column
        1055,  # ER_WRONG_FIELD_WITH_GROUP, 42000
        1064,  # ER_PARSE_ERROR, 42000
        1066,  # ER_NONUNIQ_TABLE, 42000: not unique table/alias
        1142,  # ER_TABLEACCESS_DENIED_ERROR, 42000
        1143,  # ER_COLUMNACCESS_DENIED_ERROR, 42000
        1146,  # ER_NO_SUCH_TABLE, 42S02
        1149,  # ER_SYNTAX_ERROR, 42000
        1305,  # ER_SP_DOES_NOT_EXIST, 42000: unknown function
    }
)
_CALLER_CODES = frozenset(f"errno {number}" for number in _CALLER_MYSQL_ERRNOS)


def _is_callers_code(code: Optional[str]) -> bool:
    return code is not None and (
        code.startswith(_CALLER_SQLSTATE_CLASS) or code in _CALLER_CODES
    )


def is_callers_sql_error(
    exc: BaseException, provider_cls: Optional[type] = None
) -> bool:
    """Whether a failure running the caller's own SQL carries SQLSTATE class
    42, or a MySQL-protocol error number of that class, on any link of its
    cause chain: a wrapper's own code (an HTTP status) does not hide the SQL
    failure it was raised from. Never raises."""
    try:
        return any(
            _is_callers_code(_provider_code(link, provider_cls))
            or _is_callers_code(generic_error_code(link))
            for link in _cause_links(exc)
        )
    except PASS_THROUGH:
        raise
    except BaseException:
        return False


# A Python module name, dotted: what ImportError.name holds when an import
# fails. Anything else is not shown.
_MODULE_NAME = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]{0,63}(?:\.[A-Za-z_][A-Za-z0-9_]{0,63}){0,7}"
)


def missing_module(exc: BaseException) -> Optional[str]:
    """The module an ImportError in `exc`'s cause chain could not import, when
    its name is a plain module name, else None. A missing driver is the
    environment's, not the source's, and the name says which extra to
    install. Never raises."""
    try:
        for link in _cause_links(exc):
            if isinstance(link, ImportError):
                name = _attr(link, "name")
                if type(name) is str and _MODULE_NAME.fullmatch(name):
                    return name
    except PASS_THROUGH:
        raise
    except BaseException:
        return None
    return None


def verbose_detail(exc: BaseException) -> str:
    """`: <scrubbed text>` under DATAHUB_PROBE_VERBOSE_LOGS, else empty. Never
    raises."""
    if not get_probe_verbose_logs():
        return ""
    try:
        return f": {scrub_text(str(exc), set())}"
    except PASS_THROUGH:
        raise
    except BaseException:
        return ""


def name_foreign(exc: BaseException, provider_cls: Optional[type] = None) -> str:
    """`(label)`, how an untrusted exception appears in a message, with its
    withheld text after the parenthesis under the verbose switch: inside, a
    masked value would swallow the closing parenthesis."""
    return f"({foreign_label(exc, provider_cls)}){verbose_detail(exc)}"


def classify_foreign(
    exc: BaseException, context: str, provider_cls: Optional[type] = None
) -> Exception:
    """The framework exception an untrusted failure in a provider call is
    reported as: a defect 1; a failure reading what the source sent 3; the
    rest of the ValueError family 2; anything else (drivers, SDKs, HTTP) 3."""
    message = f"{context} failed {name_foreign(exc, provider_cls)}"
    if isinstance(exc, DEFECT_TYPES):
        return ProbeInternalError(message)
    if isinstance(exc, _SOURCE_DATA_TYPES):
        return ProbeConnectionError(message)
    if isinstance(exc, _ARGUMENT_TYPES):
        return ProbeArgumentError(message)
    return ProbeConnectionError(message)


def _links(exc: BaseException) -> List[object]:
    """What `exc` leads to: its cause, its context and, for an exception
    group, its children. A child's text can be quoted as readily as a
    cause's. Read by attribute, as Python 3.10 has no BaseExceptionGroup."""
    links = [_attr(exc, "__cause__"), _attr(exc, "__context__")]
    children = _attr(exc, "exceptions")
    if isinstance(children, (tuple, list)):
        links.extend(children)
    return links


def _foreign_in_chain(exc: BaseException) -> List[BaseException]:
    """Every untrusted exception in `exc`'s cause and context chain and in
    the children of any exception group on it."""
    found: List[BaseException] = []
    seen: Set[int] = {id(exc)}
    pending = [exc]
    while pending and len(seen) <= _MAX_CHAIN_LINKS:
        current = pending.pop()
        for link in _links(current):
            # Checked per link too: one group can hold any number of children.
            if len(seen) > _MAX_CHAIN_LINKS:
                break
            if not isinstance(link, BaseException) or id(link) in seen:
                continue
            seen.add(id(link))
            if not is_trusted(link):
                found.append(link)
            pending.append(link)
    return found


def _is_own_argument(exc: BaseException, own_values: AbstractSet[str]) -> bool:
    """A lookup error raised with just a value the caller passed this call
    (`KeyError(name)` for its own `name`): its str is that argument, not
    foreign text. Any other lookup error, a KeyError carrying a message
    included, is foreign like the rest. Never raises."""
    try:
        if not isinstance(exc, LookupError) or len(exc.args) != 1:
            return False
        key = exc.args[0]
        # Exactly str or int, whose renderings are the value itself: a
        # subclass or another type may render as anything.
        if type(key) not in (str, int) or str(key) not in own_values:
            return False
        return str(exc) in (repr(key), str(key))
    except PASS_THROUGH:
        raise
    except BaseException:
        return False


def label_foreign_text(
    exc: BaseException,
    provider_cls: Optional[type] = None,
    own_values: AbstractSet[str] = frozenset(),
) -> str:
    """`str(exc)` with the text of any untrusted exception in its chain
    replaced by `(label)`: the backstop for `ProbeConnectionError(f"...{exc}")`.

    Only a verbatim str or repr is caught, never text rebuilt from parts.
    `own_values` are the call's own argument values, as strings: a lookup
    error naming one of them is matched by its repr only, so a refusal
    quoting the caller's missed name keeps it. Renderings shorter than
    _MIN_RENDERING are not matched at all.
    """
    message = str(exc)
    labels: Dict[str, str] = {}
    for foreign in _foreign_in_chain(exc):
        label = name_foreign(foreign, provider_cls)
        renders = (repr,) if _is_own_argument(foreign, own_values) else (repr, str)
        for render in renders:
            try:
                rendering = render(foreign)
            except PASS_THROUGH:
                raise
            except BaseException:
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


def call_config_hook(
    config: object, name: str, hook: Callable[..., T], *args: object, **kwargs: object
) -> T:
    """`hook(*args, **kwargs)`, the config's `name` hook, held to a provider
    call's rule: a trusted exception keeps its type, minus any untrusted text
    it quotes; anything else raised is the connector's defect (exit 1), named
    by class, hook and label, never by its text."""
    try:
        return hook(*args, **kwargs)
    except PASS_THROUGH:
        raise
    except BaseException as exc:
        if is_trusted(exc):
            replacement = police_trusted(exc)
            if replacement is not None:
                raise replacement from None
            raise
        owner = config if isinstance(config, type) else type(config)
        raise ProbeInternalError(
            f"the connector is defective: {owner.__name__}.{name} failed "
            f"{name_foreign(exc)}"
        ) from None


def police_trusted(
    exc: BaseException,
    provider_cls: Optional[type] = None,
    own_values: AbstractSet[str] = frozenset(),
) -> Optional[BaseException]:
    """A replacement for a trusted exception whose message quotes an untrusted
    one, or None. It keeps the type, so the exit code does not move; a
    subclass that cannot be rebuilt with a plain message becomes the trusted
    type it derives from. `own_values` as for label_foreign_text."""
    message = label_foreign_text(exc, provider_cls, own_values)
    if message == str(exc):
        return None
    try:
        rebuilt = copy.copy(exc)
        rebuilt.args = (message,)
        if str(rebuilt) == message:
            return rebuilt
    except Exception:
        pass
    return next(t for t in TRUSTED_TYPES if isinstance(exc, t))(message)
