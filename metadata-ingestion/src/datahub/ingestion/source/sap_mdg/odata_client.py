from typing import Dict, Union

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
    METADATA_DOCUMENT_PATH,
    SAP_CLIENT_PARAM,
    SERVICE_PATH_STRIP_PATTERN,
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

    def close(self) -> None:
        self.session.close()
