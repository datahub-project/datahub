from typing import Optional

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn,
    make_user_urn,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryFilterConfig,
    BigQueryIdentifierConfig,
)

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATE_SHARD_FORMAT = "%Y%m%d"

BQ_EXTERNAL_TABLE_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"
BQ_EXTERNAL_DATASET_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m4!1m3!3m2!1s{project}!2s{dataset}"

BQ_SYSTEM_TABLES_PATTERN = [r".*\.INFORMATION_SCHEMA\..*", r".*\.__TABLES__.*"]


class BigQueryIdentifierBuilder:
    platform = "bigquery"

    def __init__(
        self,
        identifier_config: BigQueryIdentifierConfig,
        structured_reporter: SourceReport,
    ) -> None:
        self.identifier_config = identifier_config
        if self.identifier_config.enable_legacy_sharded_table_support:
            BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = ""
        self.structured_reporter = structured_reporter

    def gen_dataset_urn(
        self, project_id: str, dataset_name: str, table: str, use_raw_name: bool = False
    ) -> str:
        datahub_dataset_name = BigqueryTableIdentifier(project_id, dataset_name, table)
        return make_dataset_urn(
            self.platform,
            (
                str(datahub_dataset_name)
                if not use_raw_name
                else datahub_dataset_name.raw_table_name()
            ),
            self.identifier_config.env,
        )

    def gen_dataset_urn_from_raw_ref(self, ref: BigQueryTableRef) -> str:
        return self.gen_dataset_urn(
            ref.table_identifier.project_id,
            ref.table_identifier.dataset,
            ref.table_identifier.table,
            use_raw_name=True,
        )

    def gen_user_urn(self, user_email: str) -> str:
        return make_user_urn(user_email.split("@")[0])

    def make_data_platform_urn(self) -> str:
        return make_data_platform_urn(self.platform)

    def make_dataplatform_instance_urn(self, project_id: str) -> Optional[str]:
        return (
            make_dataplatform_instance_urn(self.platform, project_id)
            if self.identifier_config.include_data_platform_instance
            else None
        )


class BigQueryFilter:
    def __init__(
        self, filter_config: BigQueryFilterConfig, structured_reporter: SourceReport
    ) -> None:
        self.filter_config = filter_config
        self.structured_reporter = structured_reporter

    def is_allowed(self, table_id: BigqueryTableIdentifier) -> bool:
        return AllowDenyPattern(deny=BQ_SYSTEM_TABLES_PATTERN).allowed(
            str(table_id)
        ) and (
            self.is_project_allowed(table_id.project_id)
            and is_schema_allowed(
                self.filter_config.dataset_pattern,
                table_id.dataset,
                table_id.project_id,
                self.filter_config.match_fully_qualified_names,
            )
            and self.filter_config.table_pattern.allowed(str(table_id))
        )  # TODO: use view_pattern ?

    def is_project_allowed(self, project_id: str) -> bool:
        if self.filter_config.project_ids:
            return project_id in self.filter_config.project_ids
        return self.filter_config.project_id_pattern.allowed(project_id)
