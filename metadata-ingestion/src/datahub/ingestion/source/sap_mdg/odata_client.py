from typing import Dict, List, Optional, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig
from datahub.ingestion.source.sap_mdg.constants import (
    AUTH_BEARER_PREFIX,
    HEADER_AUTHORIZATION,
    HTTP_RETRY_ALLOWED_METHODS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
    HTTP_SCHEME_HTTP,
    HTTP_SCHEME_HTTPS,
    MAX_PAGES,
    METADATA_DOCUMENT_PATH,
    ODATA_JSON_FORMAT,
    ODATA_JSON_FORMAT_PARAM,
    ODATA_V2_ENVELOPE,
    ODATA_V2_NEXT_LINK,
    ODATA_V2_RESULTS,
    ODATA_V4_ENVELOPE,
    ODATA_V4_NEXT_LINK,
    SAP_CLIENT_PARAM,
    SERVICE_PATH_STRIP_PATTERN,
)
from datahub.ingestion.source.sap_mdg.models import (
    DrfDistribution,
    DrfReplicationModelRow,
    DrfSystemRow,
)
from datahub.ingestion.source.sap_mdg.report import SapMdgSourceReport


class SapMdgODataClient:
    def __init__(self, config: SapMdgSourceConfig, report: SapMdgSourceReport) -> None:
        self.config = config
        self.report = report
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()

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

        if self.config.client_certificate_path is not None:
            if self.config.client_key_path is not None:
                session.cert = (
                    self.config.client_certificate_path,
                    self.config.client_key_path,
                )
            else:
                session.cert = self.config.client_certificate_path

        verify: Union[bool, str] = (
            self.config.ca_certificate_path
            if self.config.ca_certificate_path is not None
            else self.config.verify_ssl
        )
        session.verify = verify
        return session

    def _metadata_url(self, service: str) -> str:
        service_path = SERVICE_PATH_STRIP_PATTERN.sub("", service)
        return f"{self.config.base_url}/{service_path}/{METADATA_DOCUMENT_PATH}"

    def _query_params(self) -> Dict[str, str]:
        if self.config.sap_client is not None:
            return {SAP_CLIENT_PARAM: self.config.sap_client}
        return {}

    def fetch_metadata(self, service: str) -> bytes:
        url = self._metadata_url(service)
        response = self.session.get(
            url, params=self._query_params(), timeout=self.config.timeout
        )
        response.raise_for_status()
        return response.content

    def test_connection(self) -> None:
        # Fetch the first configured service's metadata to prove connectivity + auth.
        self.fetch_metadata(self.config.services[0])

    def fetch_drf_distribution(self) -> DrfDistribution:
        # Reads DRFC_APPL (model -> data model, active) and DRFC_APPL_SYS
        # (model -> business system) and pivots them into data model -> targets.
        drf = self.config.drf
        model_rows = [
            DrfReplicationModelRow.model_validate(row)
            for row in self._fetch_rows(drf.model_entity_set)
        ]
        system_rows = [
            DrfSystemRow.model_validate(row)
            for row in self._fetch_rows(drf.system_entity_set)
        ]

        data_model_by_model = {
            row.model: row.data_model
            for row in model_rows
            if row.is_active and row.data_model
        }
        distribution = DrfDistribution()
        for system_row in system_rows:
            data_model = data_model_by_model.get(system_row.model)
            if data_model is None:
                continue
            targets = distribution.targets_by_data_model.setdefault(data_model, [])
            if system_row.business_system not in targets:
                targets.append(system_row.business_system)
        return distribution

    def _fetch_rows(self, entity_set: str) -> List[Dict[str, object]]:
        assert self.config.drf.table_read_service is not None
        service_path = SERVICE_PATH_STRIP_PATTERN.sub(
            "", self.config.drf.table_read_service
        )
        url = f"{self.config.base_url}/{service_path}/{entity_set}"
        params = dict(self._query_params())
        params[ODATA_JSON_FORMAT_PARAM] = ODATA_JSON_FORMAT

        # Follow the server's paging links. A DRF customizing table that spills onto
        # a second page would otherwise be read partially, and a replication model
        # missing from page one drops every lineage edge that depends on it.
        rows: List[Dict[str, object]] = []
        seen_urls = {url}
        for _ in range(MAX_PAGES):
            response = self.session.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()
            payload = response.json()
            rows.extend(self._extract_rows(payload))
            next_link = self._next_link(payload)
            if next_link is None:
                return rows
            # The next link carries its own $skiptoken and repeats the original query
            # options, so params must not be reapplied on top of it.
            url = urljoin(response.url, next_link)
            params = {}
            if url in seen_urls:
                self.report.warning(
                    title="OData paging link repeated",
                    message="Stopped following pages; the collection may be partial.",
                    context=entity_set,
                )
                return rows
            seen_urls.add(url)

        self.report.warning(
            title="OData paging limit reached",
            message=f"Stopped after {MAX_PAGES} pages; the collection may be partial.",
            context=entity_set,
        )
        return rows

    @staticmethod
    def _extract_rows(payload: object) -> List[Dict[str, object]]:
        # OData V4 returns {"value": [...]}, V2 {"d": {"results": [...]}} or {"d": [...]}.
        if not isinstance(payload, dict):
            return []
        if ODATA_V4_ENVELOPE in payload:
            rows = payload[ODATA_V4_ENVELOPE]
        elif ODATA_V2_ENVELOPE in payload:
            inner = payload[ODATA_V2_ENVELOPE]
            rows = inner.get(ODATA_V2_RESULTS, []) if isinstance(inner, dict) else inner
        else:
            return []
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _next_link(payload: object) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        candidates = [payload.get(ODATA_V4_NEXT_LINK), payload.get(ODATA_V2_NEXT_LINK)]
        inner = payload.get(ODATA_V2_ENVELOPE)
        if isinstance(inner, dict):
            candidates.append(inner.get(ODATA_V2_NEXT_LINK))
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate
        return None

    def close(self) -> None:
        self.session.close()
