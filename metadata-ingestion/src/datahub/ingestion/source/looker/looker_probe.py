import re
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set, TypeVar
from urllib.parse import urlparse

import looker_sdk.rtl.requests_transport as looker_requests_transport
import requests
from looker_sdk.error import SDKError
from looker_sdk.rtl.serialize import DeserializeError

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import (
    ProbeConnectionError,
    ProbeReadFailed,
    ProbeSoftError,
)
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.looker_source import (
    BASIC_INGEST_REQUIRED_PERMISSIONS,
    USAGE_INGEST_REQUIRED_PERMISSIONS,
)

_T = TypeVar("_T")

# looker_sdk raises SDKError carrying Looker's documentation link, whose path
# holds the HTTP status: .../looker/docs/r/err/4.0/404/get/dashboards/...
_DOC_URL_STATUS = re.compile(r"/r/err/[^/]+/(\d{3})(?:/|$)")
# Older SDKs and LookerAPI's own checks use "Looker Not Found (404)".
_MESSAGE_STATUS = re.compile(r"\((\d{3})\)")


def sdk_error_status(exc: SDKError) -> Optional[int]:
    """The HTTP status behind an SDKError, or None.

    Read so that nothing else of the error has to be: SDKError's str() is
    Looker's response body plus documentation links, which is not ours to
    forward.
    """
    urls = [exc.documentation_url or ""] + [
        detail.documentation_url or "" for detail in exc.errors or []
    ]
    for url in urls:
        match = _DOC_URL_STATUS.search(url)
        if match:
            return int(match.group(1))
    match = _MESSAGE_STATUS.search(exc.message or "")
    return int(match.group(1)) if match else None


@contextmanager
def _looker_call(context: str, not_found: Optional[str] = None) -> Iterator[None]:
    """Run one Looker read, translating its failure into the probe's terms.

    Never formats the original: an SDKError renders the response body and a
    requests error the URL. `from None` keeps them out of any traceback a
    caller logs too. 403/404 degrade (ProbeSoftError), except a 404 on the
    object the caller named, which is a bad argument (`not_found`).
    """
    try:
        yield
    except SDKError as exc:
        status = sdk_error_status(exc)
        if status == 404 and not_found is not None:
            raise ValueError(not_found) from None
        if status in (403, 404):
            raise ProbeSoftError(
                f"{context} returned HTTP {status}; treating it as empty."
            ) from None
        reason = f"HTTP {status}" if status is not None else "an unrecognised error"
        raise ProbeReadFailed(f"{context} failed: Looker returned {reason}") from None
    except DeserializeError:
        raise ProbeReadFailed(
            f"{context} failed: Looker's response could not be read"
        ) from None
    except requests.exceptions.RequestException as exc:
        raise ProbeConnectionError(
            f"{context} failed: Looker could not be reached ({type(exc).__name__})"
        ) from None


def _open_looker(config: LookerDashboardSourceConfig) -> LookerAPI:
    """The connector's own wrapper: in-memory credentials
    (_DataHubLookerApiSettings), its retry adapter and transport options, and
    the me() call that proves the credentials work."""
    parsed = urlparse(config.base_url)
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            "base_url carries a user name or password before the host; Looker "
            "authenticates with client_id and client_secret, so remove it from "
            "base_url"
        )
    host = parsed.hostname or "the configured base_url"
    try:
        return LookerAPI(config)
    except ConfigurationError:
        # LookerAPI raises this from me() for any SDKError, i.e. refused
        # credentials or a base_url that is not a Looker API.
        raise ProbeConnectionError(
            f"Looker at {host} refused the API credentials; check client_id, "
            f"client_secret and base_url"
        ) from None
    except (SDKError, DeserializeError, requests.exceptions.RequestException) as exc:
        raise ProbeConnectionError(
            f"could not reach Looker at {host} ({type(exc).__name__})"
        ) from None


class LookerMetadataProbe:
    """Metadata-only probe over the Looker API, for the `looker` source.

    Builds LookerAPI on the first command rather than in for_config, so a bad
    credential surfaces as that command's connection error. Never constructs
    LookerDashboardSource, whose __init__ opens the API and builds registries.
    """

    # Read back by run_probe_method after each command.
    warnings: List[str]

    def __init__(self, config: LookerDashboardSourceConfig) -> None:
        self._config = config
        self._looker: Optional[LookerAPI] = None
        self.warnings = []

    @classmethod
    def for_config(cls, config: LookerDashboardSourceConfig) -> "LookerMetadataProbe":
        return cls(config)

    def __enter__(self) -> "LookerMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        if self._looker is None:
            return
        transport = self._looker.client.transport
        if isinstance(transport, looker_requests_transport.RequestsTransport):
            transport.session.close()

    def _api(self) -> LookerAPI:
        if self._looker is None:
            self._looker = _open_looker(self._config)
        return self._looker

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _degrading(self, fetch: Callable[[], _T], empty: _T) -> _T:
        """`empty` plus a warning when the read was refused (403/404), so an
        empty result says "could not look" rather than "nothing here"."""
        try:
            return fetch()
        except ProbeSoftError as exc:
            self._warn(str(exc))
            return empty

    def _fetch(self, context: str, call: Callable[[], Sequence[_T]]) -> List[_T]:
        def run() -> List[_T]:
            with _looker_call(context):
                return list(call())

        return self._degrading(run, [])

    @probe_method()
    def permissions(self) -> Dict[str, object]:
        """Looker permissions this API credential's roles grant, and the ones
        ingestion needs that are missing: `missing_for_metadata` are the
        permissions `test_connection` requires for dashboards, charts and
        ownership; `missing_for_usage` the ones usage statistics need. Names
        only, never the API user's id or email. All three are null, with a
        warning, when the credential cannot read its own roles."""
        api = self._api()

        def fetch() -> Optional[Set[str]]:
            with _looker_call("role lookup for the API user"):
                return api.get_available_permissions()

        granted = self._degrading(fetch, None)
        if granted is None:
            return {
                "granted": None,
                "missing_for_metadata": None,
                "missing_for_usage": None,
            }
        return {
            "granted": sorted(granted),
            "missing_for_metadata": sorted(BASIC_INGEST_REQUIRED_PERMISSIONS - granted),
            "missing_for_usage": sorted(USAGE_INGEST_REQUIRED_PERMISSIONS - granted),
        }
