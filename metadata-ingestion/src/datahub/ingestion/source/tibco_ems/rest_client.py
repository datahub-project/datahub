from typing import Dict, List, Optional, Set, Union

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
    MAX_PAGES,
    QUERY_PARAM_CURSOR,
    QUEUES_PATH,
    RESPONSE_KEY_BRIDGES,
    RESPONSE_KEY_ERRORS,
    RESPONSE_KEY_NEXT_CURSOR,
    RESPONSE_KEY_QUEUES,
    RESPONSE_KEY_TOPICS,
    TOPICS_PATH,
)
from datahub.ingestion.source.tibco_ems.models import (
    DestinationType,
    TibcoBridge,
    TibcoDestination,
    TibcoEmsListing,
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

    def _get_list(self, path: str, record_key: str) -> TibcoEmsListing[dict]:
        # The proxy caps each response at its configured page limit and hands back
        # a "next" cursor for the remainder, so a single GET silently truncates
        # every estate larger than that limit.
        self.connect()
        records: JsonList = []
        errors: List[str] = []
        cursor: Optional[str] = None
        seen_cursors: Set[str] = set()
        for _ in range(MAX_PAGES):
            params: Dict[str, str] = (
                {QUERY_PARAM_CURSOR: cursor} if cursor is not None else {}
            )
            response = self.session.get(
                self._url(path), params=params, timeout=self.config.timeout
            )
            response.raise_for_status()
            payload = response.json()
            records.extend(_records(payload, record_key))
            errors.extend(_errors(payload))
            cursor = _next_cursor(payload)
            if cursor is None or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
        return TibcoEmsListing[dict](records=records, errors=errors)

    def fetch_queues(self) -> TibcoEmsListing[TibcoDestination]:
        return self._fetch_destinations(
            QUEUES_PATH, RESPONSE_KEY_QUEUES, DestinationType.QUEUE
        )

    def fetch_topics(self) -> TibcoEmsListing[TibcoDestination]:
        return self._fetch_destinations(
            TOPICS_PATH, RESPONSE_KEY_TOPICS, DestinationType.TOPIC
        )

    def _fetch_destinations(
        self, path: str, record_key: str, destination_type: DestinationType
    ) -> TibcoEmsListing[TibcoDestination]:
        listing = self._get_list(path, record_key)
        return TibcoEmsListing[TibcoDestination](
            records=[
                TibcoDestination.model_validate(
                    {**raw, "destination_type": destination_type}
                )
                for raw in listing.records
            ],
            errors=listing.errors,
        )

    def fetch_bridges(self) -> TibcoEmsListing[TibcoBridge]:
        listing = self._get_list(BRIDGES_PATH, RESPONSE_KEY_BRIDGES)
        return TibcoEmsListing[TibcoBridge](
            records=[TibcoBridge.model_validate(raw) for raw in listing.records],
            errors=listing.errors,
        )

    def test_connection(self) -> None:
        # Proves connectivity + auth by opening a server session.
        self.connect()

    def close(self) -> None:
        self.session.close()


def _records(payload: object, record_key: str) -> JsonList:
    # The REST Proxy returns an envelope keyed by resource name, e.g.
    # {"errors": [], "first": ..., "next": ..., "queues": [...]}. The key must be
    # named: "errors" is also an array and is serialised first, so taking the
    # envelope's first array value yields an empty result on every real response.
    # A bare array is accepted too - the cloud API omits the envelope.
    if isinstance(payload, list):
        items: List[object] = payload
    elif isinstance(payload, dict):
        raw = payload.get(record_key)
        items = raw if isinstance(raw, list) else []
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]


def _errors(payload: object) -> List[str]:
    # A call that reaches some but not all server groups still returns HTTP 200,
    # listing the unreachable ones under "errors". Ignoring them would present a
    # partial estate as complete.
    if not isinstance(payload, dict):
        return []
    raw = payload.get(RESPONSE_KEY_ERRORS)
    if not isinstance(raw, list):
        return []
    return [str(error) for error in raw if error]


def _next_cursor(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    cursor = payload.get(RESPONSE_KEY_NEXT_CURSOR)
    return cursor if isinstance(cursor, str) and cursor else None
