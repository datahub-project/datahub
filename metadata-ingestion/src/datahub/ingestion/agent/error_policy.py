import os
import traceback
from typing import Tuple

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
# sql_gate / api_gate under the framework module, so is_authored covers them by
# module.
_FRAMEWORK_TYPES: Tuple[type, ...] = (
    ProbeArgumentError,
    ProbeSoftError,
    ProbeReadFailed,
    ProbeConnectionError,
    ProbeInternalError,
)

# Class names that mean "could not reach or authenticate to the source",
# matched by name across the MRO so the framework takes no dependency on
# requests, botocore, google-auth or azure-core.
_UNREACHABLE_NAMES = frozenset(
    {
        "ConnectionError",
        "ConnectTimeout",
        "ReadTimeout",
        "Timeout",
        "TimeoutError",
        "SSLError",
        "EndpointConnectionError",
        "ConnectTimeoutError",
        "NoCredentialsError",
        "CredentialRetrievalError",
        "TransportError",
        "RefreshError",
        "DefaultCredentialsError",
        "ClientAuthenticationError",
        "ServiceRequestError",
        "ServiceUnavailable",
        "OperationalError",
        "InterfaceError",
        "ConfigurationError",
    }
)


def is_authored(exc: BaseException, provider_file: str) -> bool:
    """Whether the framework may show this exception's text.

    True for framework types, and for an exception whose innermost raising
    Python frame is in the provider's own source file or in the framework
    package: a message the provider wrote on purpose. Anything raised inside
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
    frames = traceback.extract_tb(exc.__traceback__)
    if not frames:
        return True
    innermost = os.path.realpath(frames[-1].filename)
    return innermost.startswith(_FRAMEWORK_DIR) or innermost == os.path.realpath(
        provider_file
    )


def classify_foreign(exc: BaseException, context: str) -> Exception:
    """The framework exception a foreign failure is reported as -- by class
    name only, never its text."""
    names = {klass.__name__ for klass in type(exc).__mro__}
    message = f"{context} failed ({type(exc).__name__})"
    if names & _UNREACHABLE_NAMES:
        return ProbeConnectionError(message)
    return ProbeInternalError(message)
