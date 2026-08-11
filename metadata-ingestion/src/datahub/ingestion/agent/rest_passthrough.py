from typing import Dict, Optional, Sequence

import requests

from datahub.ingestion.agent.probe_methods import probe_method

# Long enough for a slow listing endpoint, short enough that a hung probe fails
# rather than occupying the executor. Overridable per connector.
DEFAULT_API_TIMEOUT_SECONDS = 30


class RestApiPassthrough:
    """Supplies the `api` probe command to a provider whose source has a REST API.

    The framework already validates the *input* to such a command: declaring
    `scoped_path_param` makes probe_methods._enforce_gates apply api_gate's method,
    URL-shape and allowlist checks before the method body runs. What it cannot
    validate is the *call*, and that is what every connector was writing by hand --
    six lines in which four things go quietly wrong:

    - reaching for a bare `requests.get` instead of the connector's own session,
      which on Hex means escaping the rate limiter installed in HexApi.__init__
    - omitting a timeout, so a hung endpoint holds the executor
    - omitting raise_for_status, so a 403 body is handed to the agent as though it
      were a listing
    - concatenating onto the wrong base (Mode's carries a workspace segment,
      Hex's does not)

    A provider mixes this in, sets `api_base_url` and `api_allowlist`, and supplies
    either `api_session` or its own `api_fetch_json`. It gets the command, the gate,
    and one agent-facing docstring shared across connectors.

    Discovery finds the inherited command because _iter_specs walks dir(), and the
    gate reads `api_allowlist` off the instance, so the mixing class's list governs.
    """

    # Deliberately annotated without a default. An unset allowlist must read as
    # None so _enforce_gates can say the *provider* is incomplete; defaulting to
    # () would instead refuse every path with "not in this connector's allowlist",
    # blaming the caller for a list nobody wrote.
    api_allowlist: Sequence[str]

    api_base_url: str = ""
    api_timeout_seconds: int = DEFAULT_API_TIMEOUT_SECONDS
    api_session: Optional[requests.Session] = None

    def api_headers(self) -> Dict[str, str]:
        """Headers for a probe request — auth, usually. Empty by default."""
        return {}

    def api_fetch_json(self, url: str) -> object:
        """Perform one GET and return the decoded body.

        Override where the connector's own fetcher does more than requests does,
        and route through that instead: Mode's adds curl-equivalent debug logging
        and rate-limit/timeout retry accounting, and a probe that bypassed it
        would behave differently from ingestion on the same call.
        """
        assert self.api_session is not None, (
            "a provider using RestApiPassthrough must set api_session, or override "
            "api_fetch_json to use its own fetcher"
        )
        response = self.api_session.get(
            url=url, headers=self.api_headers(), timeout=self.api_timeout_seconds
        )
        # Before .json(): an error body is not metadata, and a 403 page returned as
        # though it were a listing is worse than a refusal, because it looks like an
        # answer.
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
        return self.api_fetch_json(f"{self.api_base_url}{path}")
