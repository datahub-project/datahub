"""Retries for DataHub clients that never replay a request GMS may have run.

The SDK's defaults retry every method, including POST, on 5xx responses and read
timeouts, with exponential backoff. GraphQL mutations are POSTs, so a retry
could apply a write twice. Clients that serve MCP tools instead retry only what
GMS never ran: refused connections and 429s.
"""

from datahub.sdk.main_client import DataHubClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NO_REPLAY_RETRIES = Retry(
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


def use_no_replay_retries(client: DataHubClient) -> None:
    """Make ``client`` retry only requests GMS never ran (see NO_REPLAY_RETRIES)."""
    # The SDK exposes no setting for which failures to retry; replace its adapter.
    session = client._graph._session
    adapter = HTTPAdapter(max_retries=NO_REPLAY_RETRIES)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
