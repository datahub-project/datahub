# Thin Collibra client for the `collibra` ingestion source. Transport layer only:
# auth, paging, GraphQL, Output Module, and parallel fan-out. Mapping, resolution,
# and emission live in mapper/urn_resolver/source.

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, TypeVar

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase, HTTPBasicAuth
from urllib3.util.retry import Retry

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.collibra import constants as c
from datahub.ingestion.source.collibra.config import (
    CollibraAuthMethod,
    CollibraSourceConfig,
)
from datahub.ingestion.source.collibra.models import ApplicationInfo, CollibraEntity
from datahub.ingestion.source.collibra.report import CollibraSourceReport

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CollibraApiError(Exception):
    pass


class _BearerAuth(AuthBase):
    def __init__(self, token: str) -> None:
        self._token = token

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        req.headers[c.AUTHORIZATION] = c.BEARER.format(self._token)
        return req


class _OAuthTokenAuth(AuthBase):
    def __init__(self, config: CollibraSourceConfig) -> None:
        self._config = config
        self._token: Optional[str] = None
        self._lock = Lock()  # single-flight refresh under parallel workers

    def invalidate(self) -> None:
        with self._lock:
            self._token = None

    def _fetch(self) -> None:
        with self._lock:
            if self._token:
                return
            client_id = self._config.client_id
            client_secret = self._config.client_secret
            assert client_id and client_secret  # enforced by config validator
            resp = requests.post(
                self._config.url + c.OAUTH_TOKEN,
                data=c.GRANT_CLIENT_CREDENTIALS,
                auth=(client_id, client_secret.get_secret_value()),
                verify=self._config.ca_cert_path or self._config.verify_ssl,
                timeout=self._config.request_timeout,
            )
            resp.raise_for_status()
            try:
                self._token = resp.json()[c.ACCESS_TOKEN_FIELD]
            except (ValueError, KeyError) as e:
                raise CollibraApiError(
                    "OAuth token response missing access_token; check client_id/client_secret"
                ) from e

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        if not self._token:
            self._fetch()
        req.headers[c.AUTHORIZATION] = c.BEARER.format(self._token)
        return req


def _session_login(session: requests.Session, config: CollibraSourceConfig) -> None:
    assert config.username and config.password
    resp = session.post(
        config.url + c.SESSION_LOGIN,
        json={
            c.USERNAME_FIELD: config.username,
            c.PASSWORD_FIELD: config.password.get_secret_value(),
        },
        timeout=config.request_timeout,
    )
    resp.raise_for_status()


def build_session(
    config: CollibraSourceConfig, report: CollibraSourceReport
) -> requests.Session:
    session = requests.Session()
    # respect_retry_after_header=True => 429 Retry-After honored; no hand-rolled backoff.
    # Retry only idempotent methods (urllib3 default excludes POST) so a transient failure
    # can't double-submit a non-idempotent POST such as an Output Module export job.
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=c.RETRY_STATUS,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=config.max_workers,
        pool_maxsize=config.max_workers,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.verify = config.ca_cert_path or config.verify_ssl
    if not config.verify_ssl:
        report.warning("verify_ssl is disabled; TLS verification is off for Collibra")

    method = config.auth_method
    if method == CollibraAuthMethod.OAUTH:
        session.auth = _OAuthTokenAuth(config)
    elif method == CollibraAuthMethod.BASIC:
        assert config.username and config.password
        session.auth = HTTPBasicAuth(
            config.username, config.password.get_secret_value()
        )
    elif method in (CollibraAuthMethod.TOKEN, CollibraAuthMethod.JWT):
        assert config.token
        session.auth = _BearerAuth(config.token.get_secret_value())
    elif method == CollibraAuthMethod.SESSION:
        _session_login(session, config)
    return session


class CollibraClient:
    def __init__(
        self,
        config: CollibraSourceConfig,
        report: Optional[CollibraSourceReport] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = config
        self.report = report if report is not None else CollibraSourceReport()
        self.session = (
            session if session is not None else build_session(config, self.report)
        )
        self._info: Optional[ApplicationInfo] = None

    # --- low level -----------------------------------------------------------
    def _request(self, method: str, path: str, **kw: Any) -> requests.Response:
        kw.setdefault("timeout", self.config.request_timeout)
        url = self.config.url + path
        resp = self.session.request(method, url, **kw)
        if resp.status_code == c.HTTP_UNAUTHORIZED and self._refresh_auth():
            resp = self.session.request(method, url, **kw)
        resp.raise_for_status()
        return resp

    def _refresh_auth(self) -> bool:
        # OAuth: drop the cached token so the next request re-fetches. SESSION: the login
        # cookie can expire mid-run, so re-login. Static creds (basic/token/jwt) can't be
        # refreshed — a 401 there is a hard credential error, not a retryable condition.
        if isinstance(self.session.auth, _OAuthTokenAuth):
            self.session.auth.invalidate()
            return True
        if self.config.auth_method == CollibraAuthMethod.SESSION:
            _session_login(self.session, self.config)
            return True
        return False

    def _json(self, resp: requests.Response) -> Dict[str, Any]:
        # A 2xx body that isn't JSON (proxy HTML error page, WAF challenge, truncated
        # response) is a transport error, not empty data — returning {} would let a caller
        # mistake a mid-pagination failure for end-of-data. Fail loudly instead.
        try:
            return resp.json()
        except ValueError as e:
            raise CollibraApiError(
                f"Expected JSON from {resp.url}, got {resp.headers.get('Content-Type')}"
            ) from e

    def _get_json(self, path: str, params: Optional[dict] = None) -> Dict[str, Any]:
        return self._json(self._request("GET", path, params=params or {}))

    # --- capability probe ----------------------------------------------------
    def info(self) -> ApplicationInfo:
        if self._info is None:
            self._info = ApplicationInfo.model_validate(
                self._get_json(c.APPLICATION_INFO)
            )
        return self._info

    def _version(self) -> Tuple[int, int]:
        match = c.VERSION_RE.search(self.info().version or "")
        if not match:
            return (0, 0)
        return (int(match.group(1)), int(match.group(2)))

    def use_graphql(self) -> bool:
        if self.config.force_paging:
            return False
        return self._version() >= c.GRAPHQL_MIN_VERSION

    def _use_cursor(self) -> bool:
        if self.config.force_offset_paging:
            return False
        return self._version() >= c.CURSOR_MIN_VERSION

    # --- paging --------------------------------------------------------------
    def _cursor_pages(
        self,
        fetch: Callable[[Optional[str]], Tuple[List[Dict[str, Any]], Optional[str]]],
        label: str,
    ) -> Iterator[Dict[str, Any]]:
        # Shared cursor loop for REST and GraphQL paging: fetch(cursor) -> (rows, next).
        # Stops at natural end and guards against a server returning a repeating cursor.
        cursor: Optional[str] = None
        seen: Set[str] = set()
        while True:
            rows, cursor = fetch(cursor)
            self.report.pages_fetched += 1
            yield from rows
            if not cursor or not rows:
                return
            if cursor in seen:
                self.report.warning("Cursor loop detected; stopping", context=label)
                return
            seen.add(cursor)

    def paginate(
        self, path: str, params: Optional[dict] = None
    ) -> Iterator[Dict[str, Any]]:
        base = {**(params or {}), c.LIMIT: self.config.page_size, c.COUNT_LIMIT: 0}

        def fetch(
            cursor: Optional[str],
        ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
            page = dict(base)
            if cursor:
                page[c.CURSOR] = cursor
            data = self._get_json(path, page)
            return data.get(c.RESULTS_FIELD, []), data.get(c.CURSOR_FIELD)

        return self._cursor_pages(fetch, path)

    def paginate_offset(
        self, path: str, params: Optional[dict] = None
    ) -> Iterator[Dict[str, Any]]:
        offset = 0
        while True:
            page = {
                **(params or {}),
                c.LIMIT: self.config.page_size,
                c.OFFSET: offset,
                c.COUNT_LIMIT: 0,
            }
            rows = self._get_json(path, page).get(c.RESULTS_FIELD, [])
            self.report.pages_fetched += 1
            yield from rows
            if len(rows) < self.config.page_size:
                return
            offset += len(rows)

    def _extract(
        self, path: str, params: Optional[dict] = None
    ) -> Iterator[Dict[str, Any]]:
        if self._use_cursor():
            return self.paginate(path, params)
        return self.paginate_offset(path, params)

    def count(self, path: str, params: Optional[dict] = None) -> Optional[int]:
        # Offset paging returns the total when counting is enabled (countLimit omitted).
        data = self._get_json(path, {**(params or {}), c.LIMIT: 1, c.OFFSET: 0})
        total = data.get(c.TOTAL_FIELD)
        return int(total) if isinstance(total, int) else None

    # --- GraphQL -------------------------------------------------------------
    def graphql(self, query: str, variables: Optional[dict] = None) -> Dict[str, Any]:
        resp = self._request(
            "POST",
            c.GRAPHQL,
            json={c.QUERY_FIELD: query, c.VARIABLES_FIELD: variables or {}},
        )
        body = self._json(resp)
        errors = body.get(c.ERRORS_FIELD)
        if errors:
            # GraphQL returns HTTP 200 even on failure — surface it instead of
            # silently treating the error payload as data.
            raise CollibraApiError(f"GraphQL errors: {errors}")
        return body.get(c.DATA_FIELD, {})

    def graphql_paginate(
        self,
        query: str,
        variables: Optional[dict],
        extract: Callable[[dict], Tuple[List[dict], Optional[str]]],
    ) -> Iterator[Dict[str, Any]]:
        # extract(data) -> (rows, next_cursor). The caller owns the query and its
        # connection shape (Knowledge Graph paging is version-specific — VERIFY).
        base_vars = dict(variables or {})

        def fetch(
            cursor: Optional[str],
        ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
            base_vars[c.CURSOR] = cursor
            return extract(self.graphql(query, base_vars))

        return self._cursor_pages(fetch, "GraphQL")

    # --- Output Module -------------------------------------------------------
    def output_export(self, view_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        job = self._json(self._request("POST", c.OUTPUT_MODULE_JOB, json=view_config))
        try:
            job_id = job[c.ID_FIELD]
        except (KeyError, TypeError) as e:
            raise CollibraApiError(
                f"Output Module submit returned no job id; got keys {list(job)}"
            ) from e
        self.report.output_jobs += 1
        status: Dict[str, Any] = {}
        state: Optional[str] = None
        for _ in range(self.config.poll_max_attempts):
            status = self._get_json(f"{c.JOBS}/{job_id}")
            state = status.get(c.JOB_STATE_FIELD)
            if state in c.JOB_TERMINAL:
                break
            time.sleep(self.config.poll_interval)
        else:
            raise CollibraApiError(
                f"Output Module job {job_id} did not finish within "
                f"{self.config.poll_max_attempts} polls (last state {state})"
            )
        if state != c.JOB_SUCCESS:
            raise CollibraApiError(f"Output Module job {job_id} ended in state {state}")
        try:
            file_id = status[c.RESULT_FIELD][c.MESSAGE_FIELD][c.ID_FIELD]
        except (KeyError, TypeError) as e:
            raise CollibraApiError(
                f"Output Module job {job_id} result missing file id; got keys {list(status)}"
            ) from e
        content = self._request("GET", f"{c.OUTPUT_MODULE_FILES}/{file_id}").content
        return self._parse_output(content)

    def _parse_output(self, content: bytes) -> List[Dict[str, Any]]:
        # Ran after a JOB_SUCCESS, so an unreadable file is a real error, not empty data.
        try:
            data = json.loads(content)
        except ValueError as e:
            raise CollibraApiError("Output Module file was not valid JSON") from e
        if isinstance(data, list):
            return data
        return data.get(c.RESULTS_FIELD, [])

    # --- typed reads ---------------------------------------------------------
    def _read(self, path: str, params: Optional[dict] = None) -> List[CollibraEntity]:
        # Validate per row so one malformed entity (e.g. missing id, or a nested shape that
        # differs from the doc-derived model) is skipped and counted, not fatal to the whole
        # partition — the shapes in models.py are explicitly unverified against a live env.
        entities: List[CollibraEntity] = []
        for row in self._extract(path, params):
            try:
                entities.append(CollibraEntity.model_validate(row))
            except ValidationError as e:
                self.report.warning(
                    "Skipping unparseable Collibra entity",
                    context=str(row.get(c.ID_FIELD)),
                    exc=e,
                )
        self.report.entities_extracted += len(entities)
        return entities

    def _read_filtered(
        self, path: str, pattern: AllowDenyPattern
    ) -> List[CollibraEntity]:
        out: List[CollibraEntity] = []
        for entity in self._read(path):
            if entity.name is not None and not pattern.allowed(entity.name):
                self.report.filtered += 1
                continue
            out.append(entity)
        return out

    def asset_types(self) -> List[CollibraEntity]:
        return self._read_filtered(c.ASSET_TYPES, self.config.asset_type_pattern)

    def attribute_types(self) -> List[CollibraEntity]:
        return self._read(c.ATTRIBUTE_TYPES)

    def relation_types(self) -> List[CollibraEntity]:
        return self._read(c.RELATION_TYPES)

    def domain_types(self) -> List[CollibraEntity]:
        return self._read(c.DOMAIN_TYPES)

    def communities(self) -> List[CollibraEntity]:
        return self._read_filtered(c.COMMUNITIES, self.config.community_pattern)

    def domains(self) -> List[CollibraEntity]:
        return self._read_filtered(c.DOMAINS, self.config.domain_pattern)

    def users(self) -> List[CollibraEntity]:
        return self._read(c.USERS)

    def user_groups(self) -> List[CollibraEntity]:
        return self._read(c.USER_GROUPS)

    def roles(self) -> List[CollibraEntity]:
        return self._read(c.ROLES)

    def responsibilities(self) -> List[CollibraEntity]:
        return self._read(c.RESPONSIBILITIES)

    def assets(self, type_id: Optional[str] = None) -> List[CollibraEntity]:
        params = {c.TYPE_ID: type_id} if type_id else None
        return self._read(c.ASSETS, params)

    def attributes(self, asset_id: Optional[str] = None) -> List[CollibraEntity]:
        params = {c.ASSET_ID: asset_id} if asset_id else None
        return self._read(c.ATTRIBUTES, params)

    def relations(self, relation_type_id: Optional[str] = None) -> List[CollibraEntity]:
        params = {c.RELATION_TYPE_ID: relation_type_id} if relation_type_id else None
        return self._read(c.RELATIONS, params)

    def count_assets(self, type_id: str) -> Optional[int]:
        return self.count(c.ASSETS, {c.TYPE_ID: type_id})

    # --- parallel ------------------------------------------------------------
    def extract_parallel(self, tasks: List[Callable[[], List[T]]]) -> List[T]:
        # Parallelism is ACROSS partitions/tasks (cursor paging is sequential within
        # one). Each task is a thunk so REST, GraphQL, and Output Module reads all
        # compose. A failing task is warned + counted (never fatal to siblings), but a
        # total wipeout escalates to a failure so "everything failed" isn't reported as
        # an empty source.
        results: List[T] = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_index = {executor.submit(task): i for i, task in enumerate(tasks)}
            for future in as_completed(future_to_index):
                try:
                    results.extend(future.result())
                except Exception as e:
                    self.report.partitions_failed += 1
                    self.report.warning(
                        "Extraction partition failed",
                        context=f"task #{future_to_index[future]}",
                        exc=e,
                    )
        if tasks and self.report.partitions_failed == len(tasks):
            self.report.failure(
                "All extraction partitions failed; source produced no entities"
            )
        return results
