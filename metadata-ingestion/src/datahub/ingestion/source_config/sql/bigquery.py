import logging
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pydantic

from datahub.configuration.common import ConfigurationError
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.sql.sql_common import SQLAlchemyConfig
from datahub.ingestion.source_config.bigquery import BigQueryBaseConfig
from datahub.ingestion.source_config.usage.bigquery_usage import BigQueryCredential

logger = logging.getLogger(__name__)


class BigQueryConfig(BigQueryBaseConfig, BaseTimeWindowConfig, SQLAlchemyConfig):
    scheme: str = "bigquery"
    project_id: Optional[str] = pydantic.Field(
        default=None,
        description="Project ID to ingest from. If not specified, will infer from environment.",
    )
    lineage_client_project_id: Optional[str] = pydantic.Field(
        default=None,
        description="If you want to use a different ProjectId for the lineage collection you can set it here.",
    )
    log_page_size: pydantic.PositiveInt = pydantic.Field(
        default=1000,
        description="The number of log item will be queried per page for lineage collection",
    )
    credential: Optional[BigQueryCredential] = pydantic.Field(
        description="BigQuery credential informations"
    )
    # extra_client_options, include_table_lineage and max_query_duration are relevant only when computing the lineage.
    extra_client_options: Dict[str, Any] = pydantic.Field(
        default={},
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )
    include_table_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation. Is enabled by default.",
    )
    max_query_duration: timedelta = pydantic.Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )
    bigquery_audit_metadata_datasets: Optional[List[str]] = pydantic.Field(
        default=None,
        description="A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB.",
    )
    use_exported_bigquery_audit_metadata: bool = pydantic.Field(
        default=False,
        description="When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute lineage information.",
    )
    use_date_sharded_audit_log_tables: bool = pydantic.Field(
        default=False,
        description="Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs.",
    )
    _credentials_path: Optional[str] = pydantic.PrivateAttr(None)
    use_v2_audit_metadata: Optional[bool] = pydantic.Field(
        default=False, description="Whether to ingest logs using the v2 format."
    )
    upstream_lineage_in_report: bool = pydantic.Field(
        default=False,
        description="Useful for debugging lineage information. Set to True to see the raw lineage created internally.",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def get_sql_alchemy_url(self):
        if self.project_id:
            return f"{self.scheme}://{self.project_id}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return f"{self.scheme}://"

    @pydantic.validator("platform_instance")
    def bigquery_doesnt_need_platform_instance(cls, v):
        if v is not None:
            raise ConfigurationError(
                "BigQuery project ids are globally unique. You do not need to specify a platform instance."
            )

    @pydantic.root_validator()
    def validate_that_bigquery_audit_metadata_datasets_is_correctly_configured(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        if (
            values.get("use_exported_bigquery_audit_metadata")
            and not values.get("use_v2_audit_metadata")
            and not values.get("bigquery_audit_metadata_datasets")
        ):
            raise ConfigurationError(
                "bigquery_audit_metadata_datasets must be specified if using exported audit metadata. Otherwise set use_v2_audit_metadata to True."
            )
            pass
        return values

    @pydantic.validator("platform")
    def platform_is_always_bigquery(cls, v):
        return "bigquery"
