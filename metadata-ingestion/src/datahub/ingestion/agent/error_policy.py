import copy
import inspect
import os
import re
import types
from typing import AbstractSet, List, Optional, Set, Tuple

from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)

# Directory of the framework package, with a trailing separator so a sibling
# such as `agent_extras/` cannot match by prefix.
_FRAMEWORK_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "")

# Framework files whose frames do NOT vouch for an exception. These modules
# are trampolines: they call back into provider-supplied callables (openers,
# closers, `keep`/`key` functions, listings to iterate). When that callable is
# C code -- sqlite3.Connection.close, functools.partial over a driver's
# connect, a DB-API cursor -- no Python frame of its own is recorded, so the
# innermost frame is the helper's and the driver's text would pass as
# authored. What these modules raise on purpose is a framework type, trusted
# by type wherever it is raised.
_TRAMPOLINE_FILES = frozenset(
    os.path.join(_FRAMEWORK_DIR, name) for name in ("provider_helpers.py",)
)

# Exceptions whose text the framework vouches for: raised deliberately, by code
# that knows the message is safe to show. SqlScopeError / ApiScopeError live in
# sql_gate / api_gate under the framework package, so is_authored covers them by
# file path.
_FRAMEWORK_TYPES: Tuple[type, ...] = (
    ProbeArgumentError,
    ProbeSoftError,
    ProbeReadFailed,
    ProbeConnectionError,
    ProbeInternalError,
)

# Python defects: after run_probe_method has coerced the arguments, these mean
# the code misread something, not that the caller's input was wrong (exit 1).
DEFECT_TYPES: Tuple[type, ...] = (
    TypeError,
    KeyError,
    AttributeError,
    AssertionError,
    IndexError,
    NameError,
)

# What the CLI reads as "your input was wrong" (exit 2) when raised bare.
_ARGUMENT_TYPES: Tuple[type, ...] = (ValueError, re.error)


def is_authored(exc: BaseException, provider_files: AbstractSet[str]) -> bool:
    """Whether the framework may show this exception's text.

    True for framework types, and for an exception whose innermost raising
    Python frame is in one of the provider's own source files (see
    probe_methods.provider_source_files) or in the framework package (minus
    the trampoline modules in _TRAMPOLINE_FILES): a message the provider
    wrote on purpose. Anything raised inside
    reused ingestion code or a third-party library is foreign -- that is where
    text quoting a JDBC URL, a YAML node or a token endpoint body comes from.

    Compared by resolved file path, not dotted module name: the same file can
    import as `tests.unit.agent.x` or `unit.agent.x` depending on pytest's
    rootdir, and as an installed or editable package in production.

    Limitation: an exception raised by a C extension called directly from the
    provider's frame (int("..."), a C YAML loader) reports that frame and so
    counts as authored. Its text still goes through scrub_text.
    """
    if isinstance(exc, _FRAMEWORK_TYPES):
        return True
    tb = exc.__traceback__
    if tb is None:
        # Never raised, so there is no frame to vouch for it: fail closed.
        return False
    while tb.tb_next is not None:
        tb = tb.tb_next
    # From the code object rather than traceback.extract_tb, which reads
    # source lines through linecache for every frame.
    innermost = os.path.realpath(tb.tb_frame.f_code.co_filename)
    if innermost.startswith(_FRAMEWORK_DIR) and innermost not in _TRAMPOLINE_FILES:
        return True
    return any(innermost == os.path.realpath(f) for f in provider_files if f)


# Every code shown must match one of these in full; anything else is dropped
# without a word, since a "code" attribute is still the foreign library's
# text and may hold whatever it was handed.
_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
_ERRNO = re.compile(r"\d{1,6}")
_AWS_CODE = re.compile(r"[A-Za-z][A-Za-z0-9.]{1,63}")

# Drivers whose errors carry their code as args[0] -- pyodbc a SQLSTATE,
# PyMySQL and mysqlclient an int errno. Read from no other exception: args[0]
# of a plain ValueError or KeyError is free text or caller data.
_SQLSTATE_ARG_MODULES = ("pyodbc",)
_ERRNO_ARG_MODULES = ("pymysql", "MySQLdb")

# `.code` is an HTTP status only on these SDKs' exceptions (google-api-core,
# googleapiclient, urllib's HTTPError); elsewhere it is anything.
_HTTP_CODE_MODULES = ("google", "googleapiclient", "urllib")

# Attribute descriptors implemented in C (psycopg2's pgcode, BaseException's
# args): reading them runs no foreign Python code.
_C_DESCRIPTORS = (types.GetSetDescriptorType, types.MemberDescriptorType)

# How far down a cause/context chain to look for a code.
_MAX_CHAIN = 8


def _plain_attr(obj: object, name: str) -> object:
    """`obj.name` when reading it cannot run the foreign library's Python
    code, else None.

    A property, a __getattr__ or any other Python descriptor is code that
    may raise with the text it holds, so it is not read at all. A value
    stored on the instance or class, or a C-level slot, is.
    """
    try:
        raw = inspect.getattr_static(obj, name)
    except Exception:
        return None
    if isinstance(raw, _C_DESCRIPTORS):
        try:
            return getattr(obj, name)
        except Exception:
            return None
    if hasattr(type(raw), "__get__"):
        return None
    return raw


def _module_root(exc: BaseException) -> str:
    module = type(exc).__dict__.get("__module__")
    return module.split(".")[0] if isinstance(module, str) else ""


def _as_str(value: object) -> Optional[str]:
    # str.__str__ rather than str(): a str subclass may render anything.
    return str.__str__(value) if isinstance(value, str) else None


def _as_int(value: object) -> Optional[int]:
    # bool is an int, and True is not an error number. int.__int__ for the
    # same reason as str.__str__: http.HTTPStatus is an int subclass, and
    # another subclass may override __int__ or __format__.
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int.__int__(value)


def _sqlstate(value: object) -> Optional[str]:
    text = _as_str(value)
    if text is None or not _SQLSTATE.fullmatch(text):
        return None
    # Every SQLSTATE class has a digit in it; five capitals with none is a
    # word, and words are what this must not show.
    return f"SQLSTATE {text}" if any(c.isdigit() for c in text) else None


def _errno(value: object) -> Optional[str]:
    number = _as_int(value)
    if number is None or not _ERRNO.fullmatch(str(number)):
        return None
    return f"errno {number}"


def _http(value: object) -> Optional[str]:
    number = _as_int(value)
    return f"HTTP {number}" if number is not None and 100 <= number <= 599 else None


def _aws_code(response: object) -> Optional[str]:
    # dict.get, not response.get: a dict subclass may override it.
    if not isinstance(response, dict):
        return None
    error = dict.get(response, "Error")
    if not isinstance(error, dict):
        return None
    text = _as_str(dict.get(error, "Code"))
    return text if text is not None and _AWS_CODE.fullmatch(text) else None


def _codes_of(exc: BaseException) -> List[str]:
    """The codes this one exception carries itself, without its chain."""
    codes: List[str] = []
    sqlstate = _sqlstate(_plain_attr(exc, "pgcode")) or _sqlstate(
        _plain_attr(exc, "sqlstate")
    )
    args = _plain_attr(exc, "args")
    first = args[0] if isinstance(args, tuple) and args else None
    root = _module_root(exc)
    if sqlstate is None and root in _SQLSTATE_ARG_MODULES:
        sqlstate = _sqlstate(first)
    if sqlstate:
        codes.append(sqlstate)
    errno = _errno(_plain_attr(exc, "errno"))
    if errno is None and root in _ERRNO_ARG_MODULES:
        errno = _errno(first)
    if errno:
        codes.append(errno)
    if codes:
        return codes
    aws = _aws_code(_plain_attr(exc, "response"))
    if aws:
        return [aws]
    response = _plain_attr(exc, "response")
    http = (
        (_http(_plain_attr(response, "status_code")) if response is not None else None)
        or _http(_plain_attr(exc, "status_code"))
        or _http(_plain_attr(exc, "status"))
        or (_http(_plain_attr(exc, "code")) if root in _HTTP_CODE_MODULES else None)
    )
    return [http] if http else []


def foreign_label(exc: BaseException) -> str:
    """How a foreign exception is named in place of its text: its class name,
    plus a short machine code when it or its chain carries one.

    `ProgrammingError; SQLSTATE 42P01`, `HTTPError; HTTP 403`,
    `ClientError; AccessDenied`. A code tells the caller what kind of
    failure it was (missing relation, permission, throttling) without the
    message, which is where the URL or the query text is. Read duck-typed:
    the SDKs are optional, and none is imported here. SQLAlchemy's
    DBAPIError keeps the driver's error as `.orig`, so that is looked at
    right after the exception itself, before its cause and context.
    """
    name = type(exc).__name__
    seen: Set[int] = set()
    pending: List[BaseException] = [exc]
    while pending and len(seen) < _MAX_CHAIN:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        codes = _codes_of(current)
        if codes:
            return "; ".join([name, *codes])
        for link in (
            _plain_attr(current, "orig"),
            current.__cause__,
            current.__context__,
        ):
            if isinstance(link, BaseException):
                pending.append(link)
    return name


def classify_foreign(exc: BaseException, context: str) -> Exception:
    """The framework exception a foreign failure in a provider call is reported
    as -- by class name and code (see foreign_label), never its text.

    Withholding the text must not also move the exit code, so this keeps the
    family the bare exception would have reached the CLI as: a defect stays
    exit 1, the ValueError family stays exit 2, and everything else (drivers,
    SDKs, HTTP and permission errors) stays exit 3.
    """
    message = f"{context} failed ({foreign_label(exc)})"
    if isinstance(exc, DEFECT_TYPES):
        return ProbeInternalError(message)
    if isinstance(exc, _ARGUMENT_TYPES):
        return ProbeArgumentError(message)
    return ProbeConnectionError(message)


def _foreign_in_chain(
    exc: BaseException, provider_files: AbstractSet[str]
) -> List[BaseException]:
    """Every foreign exception in `exc`'s cause and context chain."""
    found: List[BaseException] = []
    seen: Set[int] = {id(exc)}
    pending = [exc.__cause__, exc.__context__]
    while pending:
        link = pending.pop()
        if link is None or id(link) in seen:
            continue
        seen.add(id(link))
        if not is_authored(link, provider_files):
            found.append(link)
        pending.extend([link.__cause__, link.__context__])
    return found


def withhold_foreign_text(exc: BaseException, provider_files: AbstractSet[str]) -> str:
    """`str(exc)` with the text of any foreign exception it wraps replaced by
    that exception's class name.

    Framework types are trusted wherever they are raised, and so is anything
    the provider raises in its own file -- which makes
    `ProbeConnectionError(f"login failed: {exc}")` around a driver error carry
    the driver's text out under a vouched-for type. Only text that appears
    verbatim can be caught (str or repr of the foreign exception); a message
    built from parts of it cannot, which is why the docs say never to
    interpolate an exception you did not raise.
    """
    message = str(exc)
    swaps = []
    for foreign in _foreign_in_chain(exc, provider_files):
        name = f"({foreign_label(foreign)})"
        for render in (repr, str):
            try:
                rendering = render(foreign)
            except Exception:
                # A rendering that raises cannot have reached the message
                # either, so there is nothing to substitute -- and the
                # render's own exception text is never shown.
                continue
            if rendering:
                swaps.append((rendering, name))
    # Longest first: a repr contains its str, and replacing the str first
    # would leave the repr's class-name wrapper around a placeholder.
    for rendering, name in sorted(swaps, key=lambda s: len(s[0]), reverse=True):
        message = message.replace(rendering, name)
    return message


def police_authored(
    exc: BaseException, provider_files: AbstractSet[str]
) -> Optional[BaseException]:
    """A replacement for an authored exception whose message quotes a foreign
    one, or None when it may be raised as it is.

    The replacement keeps the exception's type, so the exit code does not
    move. A type that cannot be rebuilt with a plain message (its constructor
    takes other arguments, or its __str__ ignores args) is replaced by the
    framework type of the same exit family instead.
    """
    message = withhold_foreign_text(exc, provider_files)
    if message == str(exc):
        return None
    try:
        rebuilt = copy.copy(exc)
        rebuilt.args = (message,)
        if str(rebuilt) == message:
            return rebuilt
    except Exception:
        pass
    return _same_family(exc, message)


def _same_family(exc: BaseException, message: str) -> Exception:
    if isinstance(exc, (ProbeInternalError, *DEFECT_TYPES)):
        return ProbeInternalError(message)
    if isinstance(exc, ProbeReadFailed):
        return ProbeReadFailed(message)
    if isinstance(exc, ProbeConnectionError):
        return ProbeConnectionError(message)
    if isinstance(exc, _ARGUMENT_TYPES):
        return ProbeArgumentError(message)
    return ProbeConnectionError(message)
