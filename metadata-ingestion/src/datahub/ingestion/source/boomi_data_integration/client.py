import logging
from typing import Dict, Iterator, Type, TypeVar, Union

import requests
from pydantic import SecretStr, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.boomi_data_integration.config import (
    BoomiDataIntegrationSourceConfig,
)
from datahub.ingestion.source.boomi_data_integration.constants import (
    API_PATH_TEMPLATE,
    BEARER_TEMPLATE,
    CONTENT_TYPE_JSON,
    ENVIRONMENT_PATH_TEMPLATE,
    ENVIRONMENTS_PAGE_SIZE,
    ENVIRONMENTS_PATH,
    HEADER_ACCEPT,
    HEADER_AUTHORIZATION,
    HTTP_GET,
    MAX_PAGES,
    PARAM_ITEMS_PER_PAGE,
    PARAM_PAGE,
    RETRY_BACKOFF_FACTOR,
    RETRY_METHODS,
    RETRY_STATUS_CODES,
    RETRY_TOTAL,
    RIVER_PATH_TEMPLATE,
    RIVERS_PAGE_SIZE,
    RIVERS_PATH_TEMPLATE,
    RIVERS_PROBE_PAGE_SIZE,
)
from datahub.ingestion.source.boomi_data_integration.models import (
    DIModel,
    Environment,
    PageResponse,
    River,
    RiverSummary,
)
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=DIModel)

QueryParams = Dict[str, Union[str, int]]


class BoomiDataIntegrationApiError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class BoomiDataIntegrationClient:
    def __init__(
        self,
        config: BoomiDataIntegrationSourceConfig,
        report: BoomiDataIntegrationSourceReport,
    ) -> None:
        self.config = config
        self.report = report
        self.base_url = API_PATH_TEMPLATE.format(
            api_url=config.resolved_api_url, account_id=config.account_id
        )
        self._limiter = RateLimiter(max_calls=config.max_requests_per_second)
        self._session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=RETRY_TOTAL,
                backoff_factor=RETRY_BACKOFF_FACTOR,
                status_forcelist=RETRY_STATUS_CODES,
                allowed_methods=RETRY_METHODS,
                respect_retry_after_header=True,
                raise_on_status=False,
            )
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def close(self) -> None:
        self._session.close()

    def _get(
        self, path: str, token: SecretStr, params: QueryParams
    ) -> Dict[str, object]:
        # Tokens are scoped to one environment, so each call carries its own.
        headers = {
            HEADER_ACCEPT: CONTENT_TYPE_JSON,
            HEADER_AUTHORIZATION: BEARER_TEMPLATE.format(
                token=token.get_secret_value()
            ),
        }
        with self._limiter:
            response = self._session.request(
                HTTP_GET,
                f"{self.base_url}/{path}",
                headers=headers,
                params=params,
                timeout=self.config.request_timeout_seconds,
            )
        self.report.report_api_call()
        if not response.ok:
            raise BoomiDataIntegrationApiError(
                response.status_code,
                f"GET {path} returned HTTP {response.status_code}: {response.text[:300]}",
            )
        body = response.json()
        if not isinstance(body, dict):
            raise BoomiDataIntegrationApiError(
                response.status_code, f"GET {path} did not return a JSON object"
            )
        return body

    def _paginate(
        self, path: str, token: SecretStr, page_size: int, model: Type[M]
    ) -> Iterator[M]:
        for page_number in range(1, MAX_PAGES + 1):
            page = PageResponse.model_validate(
                self._get(
                    path,
                    token,
                    {PARAM_PAGE: page_number, PARAM_ITEMS_PER_PAGE: page_size},
                )
            )
            for record in page.items:
                try:
                    yield model.model_validate(record)
                except ValidationError as e:
                    self.report.invalid_records += 1
                    self.report.warning(
                        title="Invalid Boomi Data Integration API record",
                        message="Skipped a record that did not match the expected shape.",
                        context=f"path={path}, record={str(record)[:200]}",
                        exc=e,
                    )
            # next_page is a link, but page numbers are enough to walk it and
            # avoid depending on whether the link is absolute. An empty page
            # with a link is followed, since the link is what marks the end.
            if not page.next_page:
                return
        # A failure rather than a warning, so that stale entity removal does
        # not delete what lies beyond the cap.
        self.report.failure(
            title="Too many pages",
            message=(
                "The API kept returning a next page, so listing stopped at the "
                "page limit. Later records were not ingested."
            ),
            context=f"path={path}, max_pages={MAX_PAGES}",
        )

    def list_environments(self, token: SecretStr) -> Iterator[Environment]:
        return self._paginate(
            ENVIRONMENTS_PATH, token, ENVIRONMENTS_PAGE_SIZE, Environment
        )

    def get_environment(self, environment_id: str, token: SecretStr) -> Environment:
        return Environment.model_validate(
            self._get(
                ENVIRONMENT_PATH_TEMPLATE.format(environment_id=environment_id),
                token,
                {},
            )
        )

    def list_rivers(
        self, environment_id: str, token: SecretStr
    ) -> Iterator[RiverSummary]:
        return self._paginate(
            RIVERS_PATH_TEMPLATE.format(environment_id=environment_id),
            token,
            RIVERS_PAGE_SIZE,
            RiverSummary,
        )

    def check_river_access(self, environment_id: str, token: SecretStr) -> None:
        self._get(
            RIVERS_PATH_TEMPLATE.format(environment_id=environment_id),
            token,
            {PARAM_PAGE: 1, PARAM_ITEMS_PER_PAGE: RIVERS_PROBE_PAGE_SIZE},
        )

    def get_river(self, environment_id: str, river_id: str, token: SecretStr) -> River:
        return River.model_validate(
            self._get(
                RIVER_PATH_TEMPLATE.format(
                    environment_id=environment_id, river_id=river_id
                ),
                token,
                {},
            )
        )
