import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from google.auth.transport.requests import AuthorizedSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.configuration.datetimes import parse_user_datetime
from datahub.ingestion.source.common.gcp_session import build_authorized_session
from datahub.ingestion.source.sql_queries.config import (
    SECOPS_MAX_EVENTS_PER_SEARCH,
    SecOpsQuerySourceConfig,
)
from datahub.ingestion.source.sql_queries.readers.mapped import (
    MappedQueryLogReader,
    lookup_path,
)
from datahub.ingestion.source.sql_queries.report import SqlQueriesSourceReport

logger = logging.getLogger(__name__)

SECOPS_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
RETRY_STATUSES = (429, 500, 502, 503, 504)
RETRY_BACKOFF_FACTOR = 2.0
ERROR_BODY_MAX_CHARS = 2000
OLDEST = datetime.min.replace(tzinfo=timezone.utc)

Event = Dict[str, Any]


class SecOpsApiError(Exception):
    """udmSearch returned an error response."""


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime


def _rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SecOpsQueryLogReader(MappedQueryLogReader[Event]):
    """Reads query records from Google SecOps (Chronicle) UDM events.

    udmSearch caps each call at SECOPS_MAX_EVENTS_PER_SEARCH events and has no
    paging, only a `moreDataAvailable` flag, so the time range is walked in
    windows and a window that hits the cap is bisected. Windows are read oldest
    first and each window's events are sorted by timestamp, because the
    aggregator resolves session temp tables from earlier statements.
    """

    def __init__(
        self,
        config: SecOpsQuerySourceConfig,
        report: SqlQueriesSourceReport,
        *,
        time_range: TimeWindow,
    ) -> None:
        super().__init__(config, report)
        self.secops_config = config
        self.time_range = time_range

    def _session(self) -> AuthorizedSession:
        session = build_authorized_session(
            self.secops_config.credential, [SECOPS_OAUTH_SCOPE]
        )
        # A long range is hundreds of calls, so one rate-limit or transient
        # server error must not discard everything read so far. Statuses are
        # not raised by the adapter, so the last error response still reaches
        # _search with its body.
        retry = Retry(
            total=self.secops_config.max_retries,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=RETRY_STATUSES,
            allowed_methods=["GET"],
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        # endpoint may point at a plain-http private endpoint.
        session.mount("http://", adapter)
        return session

    def _search(
        self, session: AuthorizedSession, window: TimeWindow
    ) -> Tuple[List[Event], bool]:
        config = self.secops_config
        params: Dict[str, Union[str, int]] = {
            "query": config.udm_query,
            "timeRange.startTime": _rfc3339(window.start),
            "timeRange.endTime": _rfc3339(window.end),
            "limit": SECOPS_MAX_EVENTS_PER_SEARCH,
        }
        response = session.get(
            f"{config.api_base_url()}/v1/{config.instance_name()}:udmSearch",
            params=params,
            timeout=config.request_timeout_seconds,
        )
        self.report.secops_searches += 1
        if not response.ok:
            # The status line alone doesn't say whether a role is missing or
            # the instance/location is wrong; the error body does.
            raise SecOpsApiError(
                f"udmSearch returned HTTP {response.status_code} for "
                f"{_rfc3339(window.start)} - {_rfc3339(window.end)}: "
                f"{response.text[:ERROR_BODY_MAX_CHARS]}"
            )
        body = response.json()
        return body.get("events") or [], bool(body.get("moreDataAvailable"))

    def _initial_windows(self) -> Iterator[TimeWindow]:
        step = timedelta(minutes=self.secops_config.window_minutes)
        start = self.time_range.start
        while start < self.time_range.end:
            end = min(start + step, self.time_range.end)
            yield TimeWindow(start, end)
            start = end

    def _events(self, session: AuthorizedSession) -> Iterator[Event]:
        min_window = timedelta(seconds=self.secops_config.min_window_seconds)
        # Windows are half-open [start, end), like the API's time range, so
        # adjacent windows never read an event twice. pending is a stack popped
        # oldest first; a split pushes its later half underneath.
        pending = list(reversed(list(self._initial_windows())))
        while pending:
            window = pending.pop()
            events, truncated = self._search(session, window)
            if truncated and window.end - window.start > min_window:
                middle = window.start + (window.end - window.start) / 2
                pending.extend(
                    [TimeWindow(middle, window.end), TimeWindow(window.start, middle)]
                )
                continue
            if truncated:
                self.report.secops_windows_truncated += 1
                self.report.warning(
                    title="SecOps search window truncated",
                    message=f"More than {SECOPS_MAX_EVENTS_PER_SEARCH} events matched "
                    "a window that can't be split further; the rest were not read. "
                    "Narrow udm_query or lower min_window_seconds.",
                    context=f"{_rfc3339(window.start)} - {_rfc3339(window.end)}",
                )
            yield from self._sorted_by_time(events)

    def _sorted_by_time(self, events: List[Event]) -> List[Event]:
        keyed = [
            (self._event_time(event), index, event)
            for index, event in enumerate(events)
        ]
        keyed.sort(key=lambda item: (item[0], item[1]))
        return [event for _, _, event in keyed]

    def _event_time(self, event: Event) -> datetime:
        """An event's timestamp for ordering.

        Proto JSON timestamps carry 0, 3, 6 or 9 fractional digits, so they can't
        be compared as strings: `...:05Z` would sort after `...:05.500Z`. A missing
        or unparseable value sorts first and is counted, since it can break
        session temp-table resolution.
        """
        path = self.secops_config.field_mappings.timestamp
        value = self._lookup(event, path) if path else None
        if value:
            try:
                return parse_user_datetime(str(value))
            except (ValueError, OverflowError):
                pass
        self.report.secops_events_without_timestamp += 1
        return OLDEST

    def _read_records(self) -> Iterable[Event]:
        logger.info(
            f"Reading UDM events between {self.time_range.start} and "
            f"{self.time_range.end}"
        )
        session = self._session()
        try:
            yield from self._events(session)
        finally:
            session.close()
        if self.report.secops_events_without_timestamp:
            self.report.warning(
                title="SecOps events without a timestamp",
                message="Some events had no usable value in field_mappings.timestamp. "
                "They are ordered first in their window and have no query time, "
                "which can break session temp-table lineage.",
                context=f"{self.report.secops_events_without_timestamp} events",
            )

    def _lookup(self, record: Event, path: str) -> Any:
        return lookup_path(record.get("udm") or {}, path)

    def _context_of(self, record: Event) -> Optional[str]:
        return record.get("name")
