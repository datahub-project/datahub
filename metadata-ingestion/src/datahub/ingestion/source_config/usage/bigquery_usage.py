import json
import logging
import os
import tempfile
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pydantic

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

logger = logging.getLogger(__name__)


class BigQueryCredential(ConfigModel):
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str = "https://accounts.google.com/o/oauth2/auth"
    token_uri: str = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url: str = "https://www.googleapis.com/oauth2/v1/certs"
    type: str = "service_account"
    client_x509_cert_url: Optional[str]

    @pydantic.root_validator()
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("client_x509_cert_url") is None:
            values[
                "client_x509_cert_url"
            ] = f'https://www.googleapis.com/robot/v1/metadata/x509/{values["client_email"]}'
        return values

    def create_credential_temp_file(self) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(self.dict(), indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name


class BigQueryUsageConfig(DatasetSourceConfigBase, BaseUsageConfig):
    projects: Optional[List[str]] = None
    project_id: Optional[str] = None  # deprecated in favor of `projects`
    extra_client_options: dict = {}
    use_v2_audit_metadata: Optional[bool] = False

    bigquery_audit_metadata_datasets: Optional[List[str]] = None
    use_exported_bigquery_audit_metadata: bool = False
    use_date_sharded_audit_log_tables: bool = False

    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    dataset_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    log_page_size: pydantic.PositiveInt = 1000

    query_log_delay: Optional[pydantic.PositiveInt] = None
    max_query_duration: timedelta = timedelta(minutes=15)
    credential: Optional[BigQueryCredential]
    _credentials_path: Optional[str] = pydantic.PrivateAttr(None)

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    @pydantic.validator("project_id")
    def note_project_id_deprecation(cls, v, values, **kwargs):
        logger.warning(
            "bigquery-usage project_id option is deprecated; use projects instead"
        )
        values["projects"] = [v]
        return None

    @pydantic.validator("platform")
    def platform_is_always_bigquery(cls, v):
        return "bigquery"

    @pydantic.validator("platform_instance")
    def bigquery_platform_instance_is_meaningless(cls, v):
        raise ConfigurationError(
            "BigQuery project-ids are globally unique. You don't need to provide a platform_instance"
        )

    @pydantic.validator("use_exported_bigquery_audit_metadata")
    def use_exported_bigquery_audit_metadata_uses_v2(cls, v, values):
        if v is True and not values["use_v2_audit_metadata"]:
            raise ConfigurationError(
                "To use exported BigQuery audit metadata, you must also use v2 audit metadata"
            )
        return v

    def get_allow_pattern_string(self) -> str:
        return "|".join(self.table_pattern.allow) if self.table_pattern else ""

    def get_deny_pattern_string(self) -> str:
        return "|".join(self.table_pattern.deny) if self.table_pattern else ""
