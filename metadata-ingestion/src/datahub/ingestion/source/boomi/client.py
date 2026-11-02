import logging
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Type, TypeVar, Union

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.constants import (
    API_PATH_TEMPLATE,
    COMPONENT_TYPE_PROCESS,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_TEXT,
    CONTENT_TYPE_XML,
    EXECUTION_TIME_FORMAT,
    FIELD_ACTIVE,
    FIELD_BRANCH_NAME,
    FIELD_COMPONENT_TYPE,
    FIELD_CURRENT_VERSION,
    FIELD_DELETED,
    FIELD_EXECUTION_TIME,
    FIELD_TYPE,
    HEADER_ACCEPT,
    HEADER_CONTENT_TYPE,
    HTTP_GET,
    HTTP_POST,
    OBJECT_COMPONENT,
    OBJECT_COMPONENT_METADATA,
    OBJECT_DEPLOYED_PACKAGE,
    OBJECT_ENVIRONMENT,
    OBJECT_EXECUTION_RECORD,
    OBJECT_FOLDER,
    OPERATOR_EQUALS,
    OPERATOR_GREATER_THAN_OR_EQUAL,
    QUERY_EXPRESSION_KEY,
    QUERY_FILTER_KEY,
    QUERY_MORE_SUFFIX,
    QUERY_SUFFIX,
    RETRY_BACKOFF_FACTOR,
    RETRY_METHODS,
    RETRY_STATUS_CODES,
    RETRY_TOTAL,
    TOKEN_USERNAME_PREFIX,
    VALUE_FALSE,
    VALUE_TRUE,
)
from datahub.ingestion.source.boomi.models import (
    BoomiModel,
    ComponentMetadata,
    DeployedPackage,
    Environment,
    ExecutionRecord,
    Folder,
    GroupingExpression,
    QueryResponse,
    SimpleExpression,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=BoomiModel)


class BoomiApiError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def _equals(property_name: str, value: str) -> SimpleExpression:
    return SimpleExpression(
        property=property_name, operator=OPERATOR_EQUALS, argument=[value]
    )


class BoomiClient:
    def __init__(self, config: BoomiSourceConfig, report: BoomiSourceReport) -> None:
        self.config = config
        self.report = report
        self.base_url = API_PATH_TEMPLATE.format(
            api_url=config.api_url, account_id=config.account_id
        )
        self._limiter = RateLimiter(max_calls=config.max_requests_per_second)
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(
            TOKEN_USERNAME_PREFIX + config.username,
            config.api_token.get_secret_value(),
        )
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

    def _request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        json_body: Optional[Dict[str, object]] = None,
        text_body: Optional[str] = None,
    ) -> requests.Response:
        url = f"{self.base_url}/{path}"
        with self._limiter:
            response = self._session.request(
                method,
                url,
                headers=headers,
                json=json_body,
                data=text_body,
                timeout=self.config.request_timeout_seconds,
            )
        self.report.report_api_call()
        if not response.ok:
            raise BoomiApiError(
                response.status_code,
                f"{method} {path} returned HTTP {response.status_code}: {response.text[:300]}",
            )
        return response

    def query(
        self,
        object_type: str,
        expression: Optional[Union[SimpleExpression, GroupingExpression]] = None,
    ) -> Iterator[Dict[str, object]]:
        headers = {
            HEADER_ACCEPT: CONTENT_TYPE_JSON,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_JSON,
        }
        body: Dict[str, object] = {}
        if expression is not None:
            body = {
                QUERY_FILTER_KEY: {
                    QUERY_EXPRESSION_KEY: expression.model_dump(by_alias=True),
                }
            }
        response = self._request(
            HTTP_POST, f"{object_type}/{QUERY_SUFFIX}", headers, json_body=body
        )
        page = QueryResponse.model_validate(response.json())
        yield from page.result
        # queryMore takes the raw token as a text/plain body, not JSON.
        more_headers = {
            HEADER_ACCEPT: CONTENT_TYPE_JSON,
            HEADER_CONTENT_TYPE: CONTENT_TYPE_TEXT,
        }
        while page.query_token:
            response = self._request(
                HTTP_POST,
                f"{object_type}/{QUERY_MORE_SUFFIX}",
                more_headers,
                text_body=page.query_token,
            )
            page = QueryResponse.model_validate(response.json())
            yield from page.result

    def _query_models(
        self,
        object_type: str,
        model: Type[M],
        expression: Optional[Union[SimpleExpression, GroupingExpression]] = None,
    ) -> Iterator[M]:
        for record in self.query(object_type, expression):
            try:
                yield model.model_validate(record)
            except ValidationError as e:
                self.report.invalid_records += 1
                self.report.warning(
                    title="Invalid Boomi API record",
                    message="Skipped a record that did not match the expected shape.",
                    context=f"object={object_type}, record={str(record)[:200]}",
                    exc=e,
                )

    def list_folders(self) -> Iterator[Folder]:
        return self._query_models(OBJECT_FOLDER, Folder)

    def list_environments(self) -> Iterator[Environment]:
        return self._query_models(OBJECT_ENVIRONMENT, Environment)

    def list_active_process_deployments(self) -> Iterator[DeployedPackage]:
        expression = GroupingExpression(
            nested_expression=[
                _equals(FIELD_COMPONENT_TYPE, COMPONENT_TYPE_PROCESS),
                _equals(FIELD_ACTIVE, VALUE_TRUE),
            ]
        )
        return self._query_models(OBJECT_DEPLOYED_PACKAGE, DeployedPackage, expression)

    def list_processes(self, branch_name: Optional[str]) -> Iterator[ComponentMetadata]:
        filters: List[Union[SimpleExpression, GroupingExpression]] = [
            _equals(FIELD_TYPE, COMPONENT_TYPE_PROCESS),
            _equals(FIELD_CURRENT_VERSION, VALUE_TRUE),
            _equals(FIELD_DELETED, VALUE_FALSE),
        ]
        if branch_name:
            filters.append(_equals(FIELD_BRANCH_NAME, branch_name))
        return self._query_models(
            OBJECT_COMPONENT_METADATA,
            ComponentMetadata,
            GroupingExpression(nested_expression=filters),
        )

    def list_executions(self, since: datetime) -> Iterator[ExecutionRecord]:
        # One window query for the whole account; filtering per process would
        # cost a query per process against the shared 10 requests/second limit.
        expression = SimpleExpression(
            property=FIELD_EXECUTION_TIME,
            operator=OPERATOR_GREATER_THAN_OR_EQUAL,
            argument=[since.astimezone(timezone.utc).strftime(EXECUTION_TIME_FORMAT)],
        )
        return self._query_models(OBJECT_EXECUTION_RECORD, ExecutionRecord, expression)

    def get_component_xml(self, component_id: str) -> str:
        response = self._request(
            HTTP_GET,
            f"{OBJECT_COMPONENT}/{component_id}",
            {HEADER_ACCEPT: CONTENT_TYPE_XML},
        )
        return response.text

    def check_connectivity(self) -> None:
        # One page of the cheapest Build-read query proves auth and privileges.
        next(iter(self.list_processes(self.config.branch_name)), None)
