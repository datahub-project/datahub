from typing import List, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.tibco_ems.config import TibcoEmsSourceConfig
from datahub.ingestion.source.tibco_ems.constants import (
    AUTH_BEARER_PREFIX,
    BRIDGES_PATH,
    CONNECT_PATH,
    CONTENT_TYPE_JSON,
    HEADER_AUTHORIZATION,
    HEADER_CONTENT_TYPE,
    HTTP_RETRY_ALLOWED_METHODS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
    HTTP_SCHEME_HTTP,
    HTTP_SCHEME_HTTPS,
    QUEUES_PATH,
    TOPICS_PATH,
)
from datahub.ingestion.source.tibco_ems.models import (
    DestinationType,
    TibcoBridge,
    TibcoDestination,
)

JsonList = List[dict]


class TibcoEmsRestClient:
    def __init__(self, config: TibcoEmsSourceConfig) -> None:
        self.config = config
        self.session = self._build_session()
        self._connected = False

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers[HEADER_CONTENT_TYPE] = CONTENT_TYPE_JSON

        retry = Retry(
            total=HTTP_RETRY_MAX_ATTEMPTS,
            backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
            status_forcelist=HTTP_RETRY_STATUS_CODES,
            allowed_methods=HTTP_RETRY_ALLOWED_METHODS,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount(HTTP_SCHEME_HTTP, adapter)
        session.mount(HTTP_SCHEME_HTTPS, adapter)

        if self.config.token is not None:
            session.headers[HEADER_AUTHORIZATION] = (
                f"{AUTH_BEARER_PREFIX}{self.config.token.get_secret_value()}"
            )
        elif self.config.username is not None and self.config.password is not None:
            session.auth = (
                self.config.username.get_secret_value(),
                self.config.password.get_secret_value(),
            )

        verify: Union[bool, str] = (
            self.config.ca_certificate_path
            if self.config.ca_certificate_path is not None
            else self.config.verify_ssl
        )
        session.verify = verify
        return session

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}/{path}"

    def connect(self) -> None:
        # Establishes the EMS server session; the returned cookie is stored on the
        # session and authorises the monitoring endpoints.
        if self._connected:
            return
        response = self.session.post(
            self._url(CONNECT_PATH), timeout=self.config.timeout
        )
        response.raise_for_status()
        self._connected = True

    def _get_list(self, path: str) -> JsonList:
        self.connect()
        response = self.session.get(self._url(path), timeout=self.config.timeout)
        response.raise_for_status()
        return _as_object_list(response.json())

    def fetch_queues(self) -> List[TibcoDestination]:
        return [
            TibcoDestination.model_validate(
                {**raw, "destination_type": DestinationType.QUEUE}
            )
            for raw in self._get_list(QUEUES_PATH)
        ]

    def fetch_topics(self) -> List[TibcoDestination]:
        return [
            TibcoDestination.model_validate(
                {**raw, "destination_type": DestinationType.TOPIC}
            )
            for raw in self._get_list(TOPICS_PATH)
        ]

    def fetch_bridges(self) -> List[TibcoBridge]:
        return [TibcoBridge.model_validate(raw) for raw in self._get_list(BRIDGES_PATH)]

    def test_connection(self) -> None:
        # Proves connectivity + auth by opening a server session.
        self.connect()

    def close(self) -> None:
        self.session.close()


def _as_object_list(payload: object) -> JsonList:
    # The REST Proxy returns either a bare array of objects or an envelope object
    # wrapping the array under a single key. We accept both without depending on
    # the exact envelope key name.
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = next(
            (value for value in payload.values() if isinstance(value, list)), []
        )
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]
