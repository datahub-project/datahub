# Thin Collibra client for the `collibra` ingestion source.
# See RFC "Collibra -> DataHub Governance Migrator" (Page 4 Engineering Design,
# Page 6 Extraction). Transport layer only: auth, paging, GraphQL, Output Module,
# and parallel fan-out. Mapping/resolution/emission live in mapper/urn_resolver/source.

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable, Iterator, List, Optional, Set, Tuple

import requests
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
            self._token = resp.json()[c.ACCESS_TOKEN_FIELD]

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
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=c.RETRY_STATUS,
        respect_retry_after_header=True,
        allowed_methods=None,  # reads are idempotent; retry POSTs too
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
        invalidate = getattr(self.session.auth, "invalidate", None)
        if callable(invalidate):
            invalidate()
            return True
        return False

    def _json(self, resp: requests.Response) -> dict:
        try:
            return resp.json()
        except ValueError:
            self.report.warning(f"Non-JSON response from {resp.url}")
            return {}

    def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
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

    # --- REST paging ---------------------------------------------------------
    def paginate(self, path: str, params: Optional[dict] = None) -> Iterator[dict]:
        base = {**(params or {}), c.LIMIT: self.config.page_size, c.COUNT_LIMIT: 0}
        cursor: Optional[str] = None
        seen: Set[str] = set()
        while True:
            page = dict(base)
            if cursor:
                page[c.CURSOR] = cursor
            data = self._get_json(path, page)
            rows = data.get(c.RESULTS_FIELD, [])
            self.report.pages_fetched += 1
            yield from rows
            cursor = data.get(c.CURSOR_FIELD)
            if not cursor or not rows:
                return
            if cursor in seen:
                self.report.warning(f"Cursor loop detected paging {path}; stopping")
                return
            seen.add(cursor)

    def paginate_offset(
        self, path: str, params: Optional[dict] = None
    ) -> Iterator[dict]:
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

    def _extract(self, path: str, params: Optional[dict] = None) -> Iterator[dict]:
        if self._use_cursor():
            return self.paginate(path, params)
        return self.paginate_offset(path, params)

    def count(self, path: str, params: Optional[dict] = None) -> Optional[int]:
        # Offset paging returns the total when counting is enabled (countLimit omitted).
        data = self._get_json(path, {**(params or {}), c.LIMIT: 1, c.OFFSET: 0})
        total = data.get(c.TOTAL_FIELD)
        return int(total) if isinstance(total, int) else None

    # --- GraphQL -------------------------------------------------------------
    def graphql(self, query: str, variables: Optional[dict] = None) -> dict:
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
    ) -> Iterator[dict]:
        # extract(data) -> (rows, next_cursor). The caller owns the query and its
        # connection shape (Knowledge Graph paging is version-specific — VERIFY).
        variables = dict(variables or {})
        cursor: Optional[str] = None
        seen: Set[str] = set()
        while True:
            variables[c.CURSOR] = cursor
            rows, cursor = extract(self.graphql(query, variables))
            yield from rows
            if not cursor or not rows:
                return
            if cursor in seen:
                self.report.warning("Cursor loop detected in GraphQL paging; stopping")
                return
            seen.add(cursor)

    # --- Output Module -------------------------------------------------------
    def output_export(self, view_config: dict) -> List[dict]:
        job = self._json(self._request("POST", c.OUTPUT_MODULE_JOB, json=view_config))
        job_id = job[c.ID_FIELD]
        self.report.output_jobs += 1
        status: dict = {}
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
        file_id = status[c.RESULT_FIELD][c.MESSAGE_FIELD][c.ID_FIELD]
        content = self._request("GET", f"{c.OUTPUT_MODULE_FILES}/{file_id}").content
        return self._parse_output(content)

    def _parse_output(self, content: bytes) -> List[dict]:
        try:
            data = json.loads(content)
        except ValueError:
            self.report.warning("Output Module file was not valid JSON")
            return []
        if isinstance(data, list):
            return data
        return data.get(c.RESULTS_FIELD, [])

    # --- typed reads ---------------------------------------------------------
    def _read(self, path: str, params: Optional[dict] = None) -> List[CollibraEntity]:
        entities = [
            CollibraEntity.model_validate(row) for row in self._extract(path, params)
        ]
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
    def extract_parallel(self, tasks: List[Callable[[], List[Any]]]) -> List[Any]:
        # Parallelism is ACROSS partitions/tasks (cursor paging is sequential within
        # one). Each task is a thunk so REST, GraphQL, and Output Module reads all
        # compose. A failing task is warned + counted, never fatal.
        results: List[Any] = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [executor.submit(task) for task in tasks]
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    self.report.partitions_failed += 1
                    self.report.warning(f"Extraction partition failed: {e}")
        return results
