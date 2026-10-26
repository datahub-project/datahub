"""Probe provider for Salesforce: the sObjects ingestion would consider.

Metadata only. The listing is ingestion's own EntityDefinition query
(customizable objects only), so an object the probe lists is one ingestion
would judge against object_pattern, and nothing else. No records, no record
counts, no field definitions (those carry the last modifier's username).
"""

from typing import Dict, List, Optional, Tuple

from simple_salesforce.exceptions import (
    SalesforceAuthenticationFailed,
    SalesforceError,
)

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, take
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
)
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.salesforce import (
    EntityDefinition,
    SalesforceApi,
    SalesforceAuthType,
    SalesforceConfig,
    SalesforceSourceReport,
    is_custom_object,
)

# The fields SalesforceApi.create_salesforce_client asserts for each auth type.
# Checked first, so a recipe missing one is the caller's to fix (exit 2)
# rather than an AssertionError read as the connector's defect.
_REQUIRED_BY_AUTH: Dict[SalesforceAuthType, Tuple[str, ...]] = {
    SalesforceAuthType.DIRECT_ACCESS_TOKEN: ("access_token", "instance_url"),
    SalesforceAuthType.USERNAME_PASSWORD: ("username", "password", "security_token"),
    SalesforceAuthType.JSON_WEB_TOKEN: ("username", "consumer_key", "private_key"),
}


def _salesforce_error_code(exc: SalesforceError) -> Optional[str]:
    if isinstance(exc, SalesforceAuthenticationFailed):
        return str(exc.code) if exc.code is not None else None
    # The REST API answers an error with a list of {"errorCode", "message"};
    # simple_salesforce's exception_handler keeps the parsed body as `content`,
    # though its annotation says bytes.
    content = exc.content
    if isinstance(content, list) and content and isinstance(content[0], dict):
        code = content[0].get("errorCode")
        return code if isinstance(code, str) else None
    return None


class SalesforceMetadataProbe(ProbeProviderBase):
    def __init__(self, config: SalesforceConfig) -> None:
        self._config = config

    @classmethod
    def for_config(cls, config: SalesforceConfig) -> "SalesforceMetadataProbe":
        return cls(config)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """Salesforce's own error code (`INVALID_LOGIN`, `INVALID_SESSION_ID`,
        `INSUFFICIENT_ACCESS`); the message is not shown."""
        if not isinstance(exc, SalesforceError):
            return None
        return _salesforce_error_code(exc)

    def _api(self) -> SalesforceApi:
        def open_api() -> SalesforceApi:
            missing = [
                name
                for name in _REQUIRED_BY_AUTH.get(self._config.auth, ())
                if getattr(self._config, name) is None
            ]
            if missing:
                raise ProbeArgumentError(
                    f"auth {self._config.auth.value} needs {', '.join(missing)} "
                    f"in the recipe"
                )
            try:
                # The connector's own client builder: the same auth, sandbox
                # domain and API version (the latest, when the recipe pins
                # none) as ingestion.
                sf = SalesforceApi.create_salesforce_client(self._config)
            except SalesforceAuthenticationFailed as e:
                if _salesforce_error_code(e) == "API_CURRENTLY_DISABLED":
                    raise ProbeConnectionError(
                        "Salesforce login failed (API_CURRENTLY_DISABLED): the "
                        "user needs the API Enabled permission"
                    ) from e
                raise
            return SalesforceApi(sf, self._config, SalesforceSourceReport())

        def close_api(api: SalesforceApi) -> None:
            api.sf.session.close()

        return self._open_once("salesforce", open_api, close=close_api)

    def _entities(self) -> List[EntityDefinition]:
        api = self._api()
        try:
            return api.list_objects()
        except SalesforceError as e:
            if _salesforce_error_code(e) == "INVALID_TYPE":
                # What ingestion reports for the same failure, keyed on the
                # code rather than the message text.
                raise ProbeConnectionError(
                    "listing sObjects failed (INVALID_TYPE): querying "
                    "EntityDefinition needs the 'View Setup and Configuration' "
                    "permission"
                ) from e
            raise

    def _records(self, custom: bool, limit: int) -> List[Dict[str, str]]:
        return take(
            (
                {"name": entity["QualifiedApiName"], "label": entity["Label"]}
                for entity in self._entities()
                if is_custom_object(entity["QualifiedApiName"]) == custom
            ),
            limit,
        )

    @probe_method(
        kind=DatasetSubTypes.SALESFORCE_STANDARD_OBJECT, row_limit_param="limit"
    )
    def objects(self, limit: int = 500) -> List[Dict[str, str]]:
        """Standard sObjects ingestion would consider (customizable ones, as
        listed in EntityDefinition), including ones object_pattern would
        exclude: a denied object is reported, not hidden. `name` is the API
        name object_pattern is matched against (`Account`), `label` the display
        label it is not. Custom objects are listed by `custom_objects`.
        Metadata only: no records are read."""
        return self._records(custom=False, limit=limit)

    @probe_method(
        kind=DatasetSubTypes.SALESFORCE_CUSTOM_OBJECT, row_limit_param="limit"
    )
    def custom_objects(self, limit: int = 500) -> List[Dict[str, str]]:
        """Custom sObjects (API names ending `__c`), judged by the same
        object_pattern as standard ones and listed the same way as
        `objects`."""
        return self._records(custom=True, limit=limit)
