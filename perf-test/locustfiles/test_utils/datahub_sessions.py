import os
from urllib.parse import urlparse

from datahub.cli.cli_utils import (
    fixup_gms_url,
    get_frontend_session_login_as,
    guess_frontend_url_from_gms_url,
)


class DataHubSessions:

    def __init__(self):
        self._session_cache = {}

    def get_session(self, url):
        if url not in self._session_cache:
            self._session_cache[url] = DataHubSession(url)

        return self._session_cache[url]


class DataHubSession:
    """
    Authenticates to DataHub using credentials from environment variables.
    Creates a session with authentication cookies.
    """

    def __init__(self, url):
        self._url = fixup_gms_url(url)
        self._host = urlparse(self._url).netloc

        # Get credentials from environment variables
        username = os.environ.get('ADMIN_USERNAME')
        password = os.environ.get('ADMIN_PASSWORD')

        if not username:
            raise RuntimeError("ADMIN_USERNAME environment variable is required")
        if not password:
            raise RuntimeError("ADMIN_PASSWORD environment variable is required")

        # Derive frontend URL from the provided URL
        frontend_url = guess_frontend_url_from_gms_url(self._url)
        self._session = get_frontend_session_login_as(username, password, frontend_url)

    def get_url(self):
        return self._url

    def get_short_host(self):
        return self._host.replace(".acryl.io", "")

    def get_cookies(self):
        return self._session.cookies.get_dict()


if __name__ == "__main__":
    session_cache = DataHubSessions()
    session = session_cache.get_session("https://deploy-perf-01.acryl.io")
    print(session.get_cookies())
