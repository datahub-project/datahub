import copy
import os
import re
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


def classify_foreign(exc: BaseException, context: str) -> Exception:
    """The framework exception a foreign failure in a provider call is reported
    as -- by class name only, never its text.

    Withholding the text must not also move the exit code, so this keeps the
    family the bare exception would have reached the CLI as: a defect stays
    exit 1, the ValueError family stays exit 2, and everything else (drivers,
    SDKs, HTTP and permission errors) stays exit 3.
    """
    message = f"{context} failed ({type(exc).__name__})"
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
        name = f"({type(foreign).__name__})"
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
