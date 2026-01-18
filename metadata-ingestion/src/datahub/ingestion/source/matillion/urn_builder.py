import logging
from typing import Optional

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_data_flow_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn,
)
from datahub.ingestion.source.matillion.config import MatillionSourceConfig
from datahub.ingestion.source.matillion.constants import MATILLION_PLATFORM
from datahub.ingestion.source.matillion.models import (
    MatillionConnection,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionProject,
)

logger = logging.getLogger(__name__)


class MatillionUrnBuilder:
    def __init__(self, config: MatillionSourceConfig):
        self.config = config

    def make_project_container_urn(self, project: MatillionProject) -> str:
        return make_container_urn(
            guid=project.id,
        )

    def make_environment_container_urn(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> str:
        return make_container_urn(
            guid=f"{project.id}.{environment.id}",
        )

    def make_pipeline_urn(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> str:
        return make_data_flow_urn(
            orchestrator=MATILLION_PLATFORM,
            flow_id=pipeline.id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance or project.name,
        )

    def make_connection_dataset_urn(
        self, connection: MatillionConnection
    ) -> Optional[str]:
        if not connection.name:
            return None

        platform = self._get_platform_for_connection(connection)
        dataset_name = connection.name

        return make_dataset_urn(
            platform=platform,
            name=dataset_name,
            env=self.config.env,
        )

    def _get_platform_for_connection(self, connection: MatillionConnection) -> str:
        connection_type = connection.connection_type or "generic"

        platform_mapping = {
            "snowflake": "snowflake",
            "bigquery": "bigquery",
            "redshift": "redshift",
            "postgres": "postgres",
            "postgresql": "postgres",
            "mysql": "mysql",
            "sqlserver": "mssql",
            "mssql": "mssql",
            "oracle": "oracle",
            "s3": "s3",
            "azure": "abs",
            "gcs": "gcs",
            "databricks": "databricks",
            "db2": "db2",
            "teradata": "teradata",
            "sap-hana": "sap-hana",
            "saphana": "sap-hana",
            "mongodb": "mongodb",
            "mongo": "mongodb",
            "cassandra": "cassandra",
            "elasticsearch": "elasticsearch",
            "elastic": "elasticsearch",
            "kafka": "kafka",
            "delta-lake": "delta-lake",
            "delta": "delta-lake",
            "deltalake": "delta-lake",
            "dremio": "dremio",
            "firebolt": "firebolt",
        }

        return platform_mapping.get(connection_type.lower(), "external")

    def make_platform_instance_urn(self) -> Optional[str]:
        if self.config.platform_instance:
            return make_dataplatform_instance_urn(
                platform=MATILLION_PLATFORM,
                instance=self.config.platform_instance,
            )
        return None
