from typing import Dict, Optional

import requests

from datahub.ingestion.agent.api_gate import probe_api_url
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase

# Long enough for a slow listing endpoint, short enough that a hung probe fails
# rather than occupying the executor. Overridable per connector.
DEFAULT_API_TIMEOUT_SECONDS = 30


class RestApiPassthrough(ProbeProviderBase):
    """Supplies the `api` probe command to a provider whose source has a REST API.

    The gate checks the input (scoped_path_param); this base makes the call
    the same way everywhere: through the connector's own session (its rate
    limiter and auth), with a timeout, raise_for_status before decoding, and
    the one URL join the gate validated. A provider sets `api_base_url` and
    `api_allowlist` (declared on ProbeProviderBase), and sets `api_session` or
    overrides `api_fetch_json`.
    """

    api_timeout_seconds: int = DEFAULT_API_TIMEOUT_SECONDS
    api_session: Optional[requests.Session] = None

    def api_headers(self) -> Dict[str, str]:
        """Headers for a probe request — auth, usually. Empty by default."""
        return {}

    def api_fetch_json(self, url: str) -> object:
        """Perform one GET and return the decoded body. Override to route
        through the connector's own fetcher where it does more (retries,
        logging), so the probe behaves as ingestion does on the same call."""
        assert self.api_session is not None, (
            "a provider using RestApiPassthrough must set api_session, or override "
            "api_fetch_json to use its own fetcher"
        )
        response = self.api_session.get(
            url=url, headers=self.api_headers(), timeout=self.api_timeout_seconds
        )
        # Before .json(): an error page must not read as a listing.
        response.raise_for_status()
        return response.json()

    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> object:
        """Fetch one listed read endpoint from this source's API, for a question no
        typed command answers. Only GET, and only a path this connector lists --
        the framework checks `path` before this runs, so an unlisted path, a write
        verb, an absolute URL or a traversal is refused before the source is
        called. Prefer a typed command where one exists: it returns the name a
        pattern is matched against, whereas a raw record leaves you guessing which
        field that is."""
        # The same join the gate validated (probe_api_url).
        return self.api_fetch_json(probe_api_url(self.api_base_url, path))
