import logging
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pydantic

from datahub.configuration.common import ConfigurationError
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.sql.sql_common import SQLAlchemyConfig
from datahub.ingestion.source_config.usage.bigquery_usage import BigQueryCredential

logger = logging.getLogger(__name__)


class BigQueryConfig(BaseTimeWindowConfig, SQLAlchemyConfig):
    scheme: str = "bigquery"
    project_id: Optional[str] = None
    lineage_client_project_id: Optional[str] = None

    log_page_size: Optional[pydantic.PositiveInt] = 1000
    credential: Optional[BigQueryCredential]
    # extra_client_options, include_table_lineage and max_query_duration are relevant only when computing the lineage.
    extra_client_options: Dict[str, Any] = {}
    include_table_lineage: Optional[bool] = True
    max_query_duration: timedelta = timedelta(minutes=15)

    credentials_path: Optional[str] = None
    bigquery_audit_metadata_datasets: Optional[List[str]] = None
    use_exported_bigquery_audit_metadata: bool = False
    use_date_sharded_audit_log_tables: bool = False
    _credentials_path: Optional[str] = pydantic.PrivateAttr(None)
    use_v2_audit_metadata: Optional[bool] = False

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

    @pydantic.validator("platform")
    def platform_is_always_bigquery(cls, v):
        return "bigquery"
