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
    project_id: str = pydantic.Field(description="Project id to set the credentials")
    private_key_id: str = pydantic.Field(description="Private key id")
    private_key: str = pydantic.Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'"
    )
    client_email: str = pydantic.Field(description="Client email")
    client_id: str = pydantic.Field(description="Client Id")
    auth_uri: str = pydantic.Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = pydantic.Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = pydantic.Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = pydantic.Field(
        default="service_account", description="Authentication type"
    )
    client_x509_cert_url: Optional[str] = pydantic.Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

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
    projects: Optional[List[str]] = pydantic.Field(
        default=None,
        description="List of project ids to ingest usage from. If not specified, will infer from environment.",
    )
    project_id: Optional[str] = pydantic.Field(
        default=None,
        description="Project ID to ingest usage from. If not specified, will infer from environment. Deprecated in favour of projects ",
    )
    extra_client_options: dict = pydantic.Field(
        default_factory=dict,
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )
    use_v2_audit_metadata: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to ingest logs using the v2 format. Required if use_exported_bigquery_audit_metadata is set to True.",
    )

    bigquery_audit_metadata_datasets: Optional[List[str]] = pydantic.Field(
        description="A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB.",
    )
    use_exported_bigquery_audit_metadata: bool = pydantic.Field(
        default=False,
        description="When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute usage information.",
    )

    use_date_sharded_audit_log_tables: bool = pydantic.Field(
        default=False,
        description="Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs.",
    )

    table_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for tables to include/exclude from ingestion.",
    )
    dataset_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for datasets to include/exclude from ingestion.",
    )
    log_page_size: pydantic.PositiveInt = pydantic.Field(
        default=1000,
        description="",
    )

    query_log_delay: Optional[pydantic.PositiveInt] = pydantic.Field(
        default=None,
        description="To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least query_log_delay additional events to be processed before attempting to resolve BigQuery job information from the logs. If query_log_delay is None, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage.",
    )

    max_query_duration: timedelta = pydantic.Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )

    credential: Optional[BigQueryCredential] = pydantic.Field(
        default=None,
        description="Bigquery credential. Required if GOOGLE_APPLICATION_CREDENTIALS enviroment variable is not set. See this example recipe for details",
    )
    _credentials_path: Optional[str] = pydantic.PrivateAttr(None)
    temp_table_dataset_prefix: str = pydantic.Field(
        default="_",
        description="If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming.",
    )

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
