import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Union
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.langfuse.langfuse_config import LangfuseConnectionConfig

logger = logging.getLogger(__name__)

# Field groups requested from the Observations API v2. `io` and `metadata` are
# intentionally excluded from the default selection: they can carry large
# and/or sensitive LLM prompt/response payloads that a metadata catalog
# should not copy into DataHub by default.
DEFAULT_OBSERVATION_FIELDS = "core,basic,model,usage,metrics,trace_context"

# Score subject kinds that this connector version can attach a score to.
# "session" and "experiment" (dataset run) subjects have no corresponding
# DataHub entity in this version (Sessions/Dataset Runs are out of v1 scope),
# so scores attached to them are counted and dropped rather than emitted.
ATTACHABLE_SCORE_SUBJECT_KINDS = frozenset({"trace", "observation"})

# Langfuse Public API endpoints used by this connector. Centralized here per
# the "Query/Request Isolation" pattern (standards/patterns.md) so the full
# set of endpoints this connector depends on is auditable in one place.
PATH_HEALTH = "/api/public/health"
PATH_PROJECTS = "/api/public/projects"
PATH_OBSERVATIONS_V2 = "/api/public/v2/observations"
PATH_SCORES_V3 = "/api/public/v3/scores"
PATH_PROMPTS_V2 = "/api/public/v2/prompts"
PATH_PROMPT_V2 = "/api/public/v2/prompts/{name}"


T = TypeVar("T")


class LangfuseAuthenticationError(Exception):
    """Raised when the configured public_key/secret_key pair is rejected by Langfuse."""


@dataclass
class LangfuseObservation:
    """A single Langfuse Observation, as returned by the Observations API v2.

    Only the fields this connector uses are modeled explicitly; `raw` retains
    the full parsed JSON object for anything else callers may need.
    """

    id: str
    trace_id: str
    type: str
    is_root_observation: bool
    name: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    trace_name: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    release: Optional[str] = None
    level: Optional[str] = None
    status_message: Optional[str] = None
    model: Optional[str] = None
    usage_details: Dict[str, Any] = field(default_factory=dict)
    cost_details: Dict[str, Any] = field(default_factory=dict)
    total_cost: Optional[float] = None
    latency: Optional[float] = None
    time_to_first_token: Optional[float] = None
    prompt_name: Optional[str] = None
    prompt_version: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, item: Dict[str, Any]) -> "LangfuseObservation":
        return cls(
            id=item["id"],
            trace_id=item["traceId"],
            type=item.get("type", "SPAN"),
            is_root_observation=bool(item.get("isRootObservation", False)),
            name=item.get("name"),
            start_time=item.get("startTime"),
            end_time=item.get("endTime"),
            session_id=item.get("sessionId"),
            user_id=item.get("userId"),
            trace_name=item.get("traceName"),
            tags=item.get("tags") or [],
            release=item.get("release"),
            level=item.get("level"),
            status_message=item.get("statusMessage"),
            model=item.get("model"),
            usage_details=item.get("usageDetails") or {},
            cost_details=item.get("costDetails") or {},
            total_cost=item.get("totalCost"),
            latency=item.get("latency"),
            time_to_first_token=item.get("timeToFirstToken"),
            prompt_name=item.get("promptName"),
            prompt_version=item.get("promptVersion"),
            raw=item,
        )


@dataclass
class LangfuseScore:
    """A single Langfuse Score, as returned by the Scores API v3."""

    id: str
    name: str
    value: Union[float, bool, str, None]
    data_type: str
    timestamp: Optional[str]
    subject_kind: Optional[str] = None
    subject_id: Optional[str] = None
    subject_trace_id: Optional[str] = None

    @classmethod
    def from_json(cls, item: Dict[str, Any]) -> "LangfuseScore":
        subject = item.get("subject") or {}
        return cls(
            id=item["id"],
            name=item["name"],
            value=item.get("value"),
            data_type=item.get("dataType", "NUMERIC"),
            timestamp=item.get("timestamp"),
            subject_kind=subject.get("kind"),
            subject_id=subject.get("id"),
            subject_trace_id=subject.get("traceId"),
        )


@dataclass
class LangfusePromptVersion:
    """A single version of a Langfuse Prompt, as returned by the Prompts API v2."""

    name: str
    version: int
    prompt_type: str
    prompt: Union[str, List[Dict[str, Any]]]
    config: Any
    labels: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    commit_message: Optional[str] = None

    @classmethod
    def from_json(cls, item: Dict[str, Any]) -> "LangfusePromptVersion":
        return cls(
            name=item["name"],
            version=item["version"],
            prompt_type=item.get("type", "text"),
            prompt=item.get("prompt", ""),
            config=item.get("config"),
            labels=item.get("labels") or [],
            tags=item.get("tags") or [],
            commit_message=item.get("commitMessage"),
        )


class LangfuseClient:
    """Thin REST client for the Langfuse Public API (v2/v3 surface only).

    Deliberately hand-rolled with `requests` rather than the official
    `langfuse` Python SDK: the SDK's query surface targets endpoints (legacy
    `/api/public/traces`, etc.) that self-hosted Langfuse v4 deployments in
    "events_only" mode disable outright, and its version churn (v3 vs v4
    namespaces) is safer to avoid for a query-only connector.
    """

    def __init__(
        self,
        connection: LangfuseConnectionConfig,
        page_size: int = 50,
        timeout_seconds: int = 30,
    ):
        self.host = connection.host
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()
        self.session.auth = (
            connection.public_key,
            connection.secret_key.get_secret_value(),
        )
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def close(self) -> None:
        self.session.close()

    def _get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        clean_params = self._clean_params(params or {})
        response = self.session.get(
            url, params=clean_params, timeout=self.timeout_seconds
        )
        if response.status_code == 401:
            raise LangfuseAuthenticationError(
                f"Langfuse rejected the configured credentials for {url}. "
                "Verify connection.public_key and connection.secret_key."
            )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _clean_params(params: Dict[str, Any]) -> Dict[str, Any]:
        cleaned: Dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                cleaned[key] = "true" if value else "false"
            elif isinstance(value, datetime):
                cleaned[key] = value.isoformat()
            else:
                cleaned[key] = value
        return cleaned

    def get_health(self) -> Dict[str, Any]:
        """Calls GET /api/public/health. Does not require authentication."""
        return self._get(PATH_HEALTH)

    def get_project(self) -> Dict[str, Any]:
        """Returns the single project associated with the configured API key."""
        response = self._get(PATH_PROJECTS)
        projects = response.get("data") or []
        if not projects:
            raise LangfuseAuthenticationError(
                "Langfuse returned no project for the configured API key."
            )
        return projects[0]

    def _iter_cursor_paginated(
        self, path: str, params: Dict[str, Any]
    ) -> Iterator[Dict[str, Any]]:
        cursor: Optional[str] = None
        while True:
            query = dict(params)
            query["limit"] = self.page_size
            if cursor:
                query["cursor"] = cursor
            response = self._get(path, query)
            items = response.get("data") or []
            yield from items
            cursor = (response.get("meta") or {}).get("cursor")
            if not cursor:
                return

    def _iter_page_paginated(
        self, path: str, params: Dict[str, Any]
    ) -> Iterator[Dict[str, Any]]:
        page = 1
        while True:
            query = dict(params)
            query["page"] = page
            query["limit"] = self.page_size
            response = self._get(path, query)
            items = response.get("data") or []
            yield from items
            meta = response.get("meta") or {}
            total_pages = meta.get("totalPages")
            if not items or (total_pages is not None and page >= total_pages):
                return
            page += 1

    def iter_observations(
        self,
        from_start_time: datetime,
        to_start_time: datetime,
        observation_type: Optional[str] = None,
        is_root_observation: Optional[bool] = None,
        fields: str = DEFAULT_OBSERVATION_FIELDS,
    ) -> Iterator[LangfuseObservation]:
        """Iterates GET /api/public/v2/observations, cursor-paginated."""
        params: Dict[str, Any] = {
            "fromStartTime": from_start_time,
            "toStartTime": to_start_time,
            "fields": fields,
        }
        if observation_type is not None:
            params["type"] = observation_type
        if is_root_observation is not None:
            params["isRootObservation"] = is_root_observation
        for item in self._iter_cursor_paginated(PATH_OBSERVATIONS_V2, params):
            parsed = self._safe_parse(
                LangfuseObservation.from_json, item, "observation"
            )
            if parsed is not None:
                yield parsed

    def iter_scores(
        self,
        from_timestamp: datetime,
        to_timestamp: datetime,
    ) -> Iterator[LangfuseScore]:
        """Iterates GET /api/public/v3/scores, cursor-paginated, with subject info."""
        params: Dict[str, Any] = {
            "fromTimestamp": from_timestamp,
            "toTimestamp": to_timestamp,
            "fields": "subject",
        }
        for item in self._iter_cursor_paginated(PATH_SCORES_V3, params):
            parsed = self._safe_parse(LangfuseScore.from_json, item, "score")
            if parsed is not None:
                yield parsed

    def iter_prompt_names(self) -> Iterator[Dict[str, Any]]:
        """Iterates GET /api/public/v2/prompts, page-paginated.

        Each item is a `PromptMeta` object: one row per prompt *name*, with a
        `versions` field listing all existing version numbers for that name.
        The actual prompt content per version is fetched separately via
        `get_prompt_version`.
        """
        yield from self._iter_page_paginated(PATH_PROMPTS_V2, {})

    def get_prompt_version(self, name: str, version: int) -> LangfusePromptVersion:
        """Calls GET /api/public/v2/prompts/{name}?version=N&resolve=false."""
        item = self._get(
            PATH_PROMPT_V2.format(name=quote(name, safe="")),
            {"version": version, "resolve": False},
        )
        return LangfusePromptVersion.from_json(item)

    @staticmethod
    def _safe_parse(
        parse_fn: Callable[[Dict[str, Any]], T], item: Dict[str, Any], kind: str
    ) -> Optional[T]:
        """Parses one paginated record, isolating failures to that record.

        A single malformed record (e.g. missing an expected field due to a
        server-side schema change) must not abort the entire paginated fetch
        for every other trace/score in the window.
        """
        try:
            return parse_fn(item)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(
                "Skipping malformed %s record %r: %s",
                kind,
                item.get("id", "<unknown id>"),
                e,
            )
            return None
