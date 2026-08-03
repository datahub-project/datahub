from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.constants import (
    APPNODE_ENTRY_DELIMITER,
    AUTH_BEARER_PREFIX,
    BW_APPLICATIONS_TEMPLATE,
    BW_APPNODES_TEMPLATE,
    BW_APPSPACES_TEMPLATE,
    BW_DOMAINS_PATH,
    CONTENT_TYPE_JSON,
    DEPLOYMENT_CLOUD,
    DEPLOYMENT_ON_PREM,
    HEADER_ACCEPT,
    HEADER_AUTHORIZATION,
    HTTP_RETRY_ALLOWED_METHODS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
    HTTP_SCHEME_HTTP,
    HTTP_SCHEME_HTTPS,
    PROPERTY_APP_TYPE,
    PROPERTY_APPNODE_COUNT,
    PROPERTY_APPNODES,
    PROPERTY_APPSPACE,
    PROPERTY_DEPLOYMENT_TYPE,
    PROPERTY_DOMAIN,
    PROPERTY_ORGANIZATION,
    PROPERTY_REGION,
    PROPERTY_STATE,
    PROPERTY_STATUS,
    PROPERTY_SUBSCRIPTION,
    PROPERTY_VERSION,
    SCOPE_ID_DELIMITER,
    TCI_APPS_TEMPLATE,
    TCI_SUBSCRIPTIONS_KEY,
    TCI_USERINFO_PATH,
)
from datahub.ingestion.source.tibco_bw.models import (
    BwApplication,
    BwAppNode,
    BwAppSpace,
    BwDomain,
    TciApp,
    TciSubscription,
    TibcoApplication,
    TibcoDeployment,
    TibcoScope,
)

JsonList = List[dict]


def _build_session(config: TibcoBwSourceConfig) -> requests.Session:
    session = requests.Session()
    session.headers[HEADER_ACCEPT] = CONTENT_TYPE_JSON

    retry = Retry(
        total=HTTP_RETRY_MAX_ATTEMPTS,
        backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
        status_forcelist=HTTP_RETRY_STATUS_CODES,
        allowed_methods=HTTP_RETRY_ALLOWED_METHODS,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount(HTTP_SCHEME_HTTP, adapter)
    session.mount(HTTP_SCHEME_HTTPS, adapter)

    if config.token is not None:
        session.headers[HEADER_AUTHORIZATION] = (
            f"{AUTH_BEARER_PREFIX}{config.token.get_secret_value()}"
        )
    elif config.username is not None and config.password is not None:
        session.auth = (
            config.username.get_secret_value(),
            config.password.get_secret_value(),
        )

    verify: Union[bool, str] = (
        config.ca_certificate_path
        if config.ca_certificate_path is not None
        else config.verify_ssl
    )
    session.verify = verify
    return session


def _as_object_list(payload: object) -> JsonList:
    # Accept either a bare array of objects or an envelope object wrapping the
    # array under a single key, without depending on the exact envelope name.
    if isinstance(payload, list):
        items: List[object] = payload
    elif isinstance(payload, dict):
        items = next(
            (value for value in payload.values() if isinstance(value, list)), []
        )
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]


def _app_properties(
    version: Optional[str], state: Optional[str], app_type: Optional[str]
) -> Dict[str, str]:
    properties: Dict[str, str] = {}
    if version:
        properties[PROPERTY_VERSION] = version
    if state:
        properties[PROPERTY_STATE] = state
    if app_type:
        properties[PROPERTY_APP_TYPE] = app_type
    return properties


class TibcoClient(ABC):
    def __init__(self, config: TibcoBwSourceConfig) -> None:
        self.config = config
        self.session = _build_session(config)

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}/{path}"

    def _get_list(self, path: str) -> JsonList:
        response = self.session.get(self._url(path), timeout=self.config.timeout)
        response.raise_for_status()
        return _as_object_list(response.json())

    @abstractmethod
    def fetch_scopes(self) -> List[TibcoScope]: ...

    @abstractmethod
    def test_connection(self) -> None: ...

    def close(self) -> None:
        self.session.close()


class BwAgentClient(TibcoClient):
    def fetch_scopes(self) -> List[TibcoScope]:
        scopes: List[TibcoScope] = []
        for domain in self._domains():
            for appspace in self._appspaces(domain.name):
                scopes.append(self._build_scope(domain, appspace))
        return scopes

    def test_connection(self) -> None:
        self._get_list(BW_DOMAINS_PATH)

    def _domains(self) -> List[BwDomain]:
        return [BwDomain.model_validate(raw) for raw in self._get_list(BW_DOMAINS_PATH)]

    def _appspaces(self, domain: str) -> List[BwAppSpace]:
        path = BW_APPSPACES_TEMPLATE.format(domain=domain)
        return [BwAppSpace.model_validate(raw) for raw in self._get_list(path)]

    def _appnodes(self, domain: str, appspace: str) -> List[BwAppNode]:
        path = BW_APPNODES_TEMPLATE.format(domain=domain, appspace=appspace)
        return [BwAppNode.model_validate(raw) for raw in self._get_list(path)]

    def _applications(self, domain: str, appspace: str) -> List[BwApplication]:
        path = BW_APPLICATIONS_TEMPLATE.format(domain=domain, appspace=appspace)
        return [BwApplication.model_validate(raw) for raw in self._get_list(path)]

    def _build_scope(self, domain: BwDomain, appspace: BwAppSpace) -> TibcoScope:
        properties: Dict[str, str] = {
            PROPERTY_DEPLOYMENT_TYPE: DEPLOYMENT_ON_PREM,
            PROPERTY_DOMAIN: domain.name,
            PROPERTY_APPSPACE: appspace.name,
        }
        if appspace.status:
            properties[PROPERTY_STATUS] = appspace.status
        if self.config.include_appnodes:
            self._attach_appnode_properties(domain.name, appspace.name, properties)

        applications = [
            TibcoApplication(
                name=app.name,
                properties=_app_properties(app.version, app.state, app.app_type),
            )
            for app in self._applications(domain.name, appspace.name)
        ]
        return TibcoScope(
            id=f"{domain.name}{SCOPE_ID_DELIMITER}{appspace.name}",
            name=appspace.name,
            description=appspace.description,
            properties=properties,
            applications=applications,
        )

    def _attach_appnode_properties(
        self, domain: str, appspace: str, properties: Dict[str, str]
    ) -> None:
        nodes = self._appnodes(domain, appspace)
        properties[PROPERTY_APPNODE_COUNT] = str(len(nodes))
        if nodes:
            properties[PROPERTY_APPNODES] = APPNODE_ENTRY_DELIMITER.join(
                f"{node.name} ({node.status})" if node.status else node.name
                for node in nodes
            )


class TciClient(TibcoClient):
    def fetch_scopes(self) -> List[TibcoScope]:
        return [self._build_scope(sub) for sub in self._subscriptions()]

    def test_connection(self) -> None:
        response = self.session.get(
            self._url(TCI_USERINFO_PATH), timeout=self.config.timeout
        )
        response.raise_for_status()

    def _subscriptions(self) -> List[TciSubscription]:
        response = self.session.get(
            self._url(TCI_USERINFO_PATH), timeout=self.config.timeout
        )
        response.raise_for_status()
        payload = response.json()
        raw = (
            payload.get(TCI_SUBSCRIPTIONS_KEY, []) if isinstance(payload, dict) else []
        )
        return [
            TciSubscription.model_validate(item)
            for item in raw
            if isinstance(item, dict)
        ]

    def _apps(self, subscription: str) -> List[TciApp]:
        path = TCI_APPS_TEMPLATE.format(subscription=subscription)
        return [TciApp.model_validate(raw) for raw in self._get_list(path)]

    def _build_scope(self, subscription: TciSubscription) -> TibcoScope:
        properties: Dict[str, str] = {
            PROPERTY_DEPLOYMENT_TYPE: DEPLOYMENT_CLOUD,
            PROPERTY_SUBSCRIPTION: subscription.subscription_id,
        }
        if subscription.organization:
            properties[PROPERTY_ORGANIZATION] = subscription.organization
        if subscription.region:
            properties[PROPERTY_REGION] = subscription.region

        applications = [
            TibcoApplication(
                name=app.name,
                description=app.description,
                properties=_app_properties(app.version, app.state, app.app_type),
            )
            for app in self._apps(subscription.subscription_id)
        ]
        return TibcoScope(
            id=subscription.subscription_id,
            name=subscription.name or subscription.subscription_id,
            properties=properties,
            applications=applications,
        )


def create_client(config: TibcoBwSourceConfig) -> TibcoClient:
    if config.deployment is TibcoDeployment.ON_PREM:
        return BwAgentClient(config)
    return TciClient(config)
