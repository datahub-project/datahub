import os
import re
from typing import AbstractSet, Tuple

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
    probe_methods.provider_source_files) or in the framework package: a
    message the provider wrote on purpose. Anything raised inside
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
    if innermost.startswith(_FRAMEWORK_DIR):
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
