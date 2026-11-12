"""DataHub clients for one caller: bounded, and never replaying a request GMS may have run.

The SDK's defaults retry every method, including POST, on 5xx responses and read
timeouts, with exponential backoff. GraphQL mutations are POSTs, so a retry
could apply a write twice, and a slow GMS could hold a call for minutes. Caller
clients instead retry only what GMS never ran: refused connections and 429s.

Each client can also carry a deadline for the tool call it serves. A worker
thread can't be interrupted, so the deadline is checked before every GMS request:
past it, the next request fails instead of being sent, and a request in flight
gets no more than the time left.
"""

import functools
import time
from typing import Any, Callable, Optional

import requests
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from fastmcp.exceptions import ToolError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .._version import __version__

_RETRIES = Retry(
    total=2,
    connect=2,  # the request never reached GMS
    read=0,  # it may have: never replay
    status=2,
    status_forcelist=[429],  # GMS refused it before running it
    other=0,
    allowed_methods=None,  # any method: only safe failures are retried at all
    backoff_factor=0.5,
    respect_retry_after_header=True,
    raise_on_status=False,
)


class ToolDeadlineExceeded(ToolError):
    """The tool call ran past its time limit before its next GMS request."""


def _with_deadline(
    request: Callable[..., requests.Response],
    *,
    deadline: float,
    limit_seconds: float,
    timeout_seconds: float,
    clock: Callable[[], float],
) -> Callable[..., requests.Response]:
    @functools.wraps(request)
    def bounded(method: str, url: str, **kwargs: Any) -> requests.Response:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ToolDeadlineExceeded(
                f"The tool call ran past its {limit_seconds:.0f}s limit and stopped "
                "before its next DataHub request. Try a narrower request."
            )
        budget = min(timeout_seconds, remaining)
        return request(method, url, **{**kwargs, "timeout": (budget, budget)})

    return bounded


class CallerClientFactory:
    """Builds a DataHub client for one caller's token. Construction makes no calls."""

    def __init__(
        self,
        gms_url: str,
        *,
        timeout_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._gms_url = gms_url
        self._timeout_seconds = timeout_seconds
        self._clock = clock

    def build(
        self, token: str, *, time_limit_seconds: Optional[float] = None
    ) -> DataHubClient:
        """A client for ``token``; with a time limit, it stops sending requests
        once that many seconds have passed since now."""
        client = DataHubClient(
            config=DatahubClientConfig(
                server=self._gms_url,
                token=token,
                timeout_sec=self._timeout_seconds,
                client_mode=ClientMode.SDK,
                datahub_component=f"datahub-ai-service/{__version__}",
            )
        )
        # The SDK exposes no setting for which failures to retry; replace its adapter.
        session = client._graph._session
        adapter = HTTPAdapter(max_retries=_RETRIES)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        if time_limit_seconds is not None:
            session.request = _with_deadline(  # type: ignore[method-assign]
                session.request,
                deadline=self._clock() + time_limit_seconds,
                limit_seconds=time_limit_seconds,
                timeout_seconds=self._timeout_seconds,
                clock=self._clock,
            )
        return client
