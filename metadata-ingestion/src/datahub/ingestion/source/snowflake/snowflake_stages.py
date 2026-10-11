import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

from pydantic import Field

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    SchemaKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.azure.abs_utils import make_abs_urn
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeStage,
    SnowflakeStageType,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class StageKey(ContainerKey):
    database: str
    db_schema: str = Field(alias="schema")
    stage: str


@dataclass
class StageLookupEntry:
    stage: SnowflakeStage
    container_key: StageKey
    dataset_urn: Optional[str] = None


@dataclass
class SnowflakeStagesExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder

    # Lookup map: "DB.SCHEMA.STAGE_NAME" -> StageLookupEntry
    stage_lookup: Dict[str, StageLookupEntry] = field(default_factory=dict)

    def get_workunits(
        self,
        db_name: str,
        schema_name: str,
        schema_container_key: SchemaKey,
    ) -> Iterable[MetadataWorkUnit]:
        stages = self.data_dictionary.get_stages_for_schema(db_name, schema_name)

        for stage in stages:
            stage_identifier = f"{db_name}.{schema_name}.{stage.name}".upper()

            if not self.config.stage_pattern.allowed(stage_identifier):
                continue

            self.report.stages_scanned += 1

            stage_key = StageKey(
                database=self.identifiers.snowflake_identifier(db_name),
                schema=self.identifiers.snowflake_identifier(schema_name),
                stage=self.identifiers.snowflake_identifier(stage.name),
                platform="snowflake",
                instance=self.config.platform_instance,
                env=self.config.env,
            )

            lookup_entry = StageLookupEntry(
                stage=stage,
                container_key=stage_key,
            )

            yield from self._gen_stage_container(
                stage=stage,
                stage_key=stage_key,
                parent_key=schema_container_key,
            )

            if stage.stage_type == SnowflakeStageType.INTERNAL:
                dataset_urn = self._gen_internal_stage_dataset_urn(stage)
                lookup_entry.dataset_urn = dataset_urn
                yield from self._gen_internal_stage_dataset(
                    stage=stage,
                    dataset_urn=dataset_urn,
                    stage_key=stage_key,
                )
            else:
                lookup_entry.dataset_urn = self._resolve_external_stage_url(stage.url)

            self.stage_lookup[stage_identifier] = lookup_entry

    def _gen_stage_container(
        self,
        stage: SnowflakeStage,
        stage_key: StageKey,
        parent_key: SchemaKey,
    ) -> Iterable[MetadataWorkUnit]:
        extra_properties: Dict[str, str] = {
            "stage_type": stage.stage_type.value,
        }
        if stage.url:
            extra_properties["url"] = stage.url
        if stage.cloud:
            extra_properties["cloud"] = stage.cloud
        if stage.region:
            extra_properties["region"] = stage.region
        if stage.storage_integration:
            extra_properties["storage_integration"] = stage.storage_integration

        yield from gen_containers(
            container_key=stage_key,
            name=stage.name,
            sub_types=[DatasetContainerSubTypes.SNOWFLAKE_STAGE],
            parent_container_key=parent_key,
            description=stage.comment,
            owner_urn=make_group_urn(stage.owner) if stage.owner else None,
            extra_properties=extra_properties,
            created=int(stage.created.timestamp() * 1000) if stage.created else None,
        )

    def _gen_internal_stage_dataset_urn(self, stage: SnowflakeStage) -> str:
        dataset_name = self.identifiers.get_dataset_identifier(
            stage.name, stage.schema_name, stage.database_name
        )
        return make_dataset_urn_with_platform_instance(
            platform="snowflake",
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _gen_internal_stage_dataset(
        self,
        stage: SnowflakeStage,
        dataset_urn: str,
        stage_key: StageKey,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=stage.name,
                description="Internal stage data managed by Snowflake",
                customProperties={
                    "stage_type": "INTERNAL",
                    "stage_name": stage.name,
                    "database": stage.database_name,
                    "schema": stage.schema_name,
                },
                qualifiedName=f"{stage.database_name}.{stage.schema_name}.{stage.name}",
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(
                typeNames=[DatasetSubTypes.SNOWFLAKE_STAGE_DATA],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        yield from self._add_dataset_to_stage_container(dataset_urn, stage_key)

        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn("snowflake"),
                    instance=make_dataplatform_instance_urn(
                        "snowflake", self.config.platform_instance
                    ),
                ),
            ).as_workunit()

    def _add_dataset_to_stage_container(
        self, dataset_urn: str, stage_key: StageKey
    ) -> Iterable[MetadataWorkUnit]:
        yield from add_dataset_to_container(
            container_key=stage_key,
            dataset_urn=dataset_urn,
        )

    def _resolve_external_stage_url(self, url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith("s3://"):
            return make_s3_urn_for_lineage(url, self.config.env)
        if url.startswith("gcs://"):
            # Snowflake uses gcs:// but DataHub GCS platform expects the path without prefix
            path = url[len("gcs://") :]
            if path.endswith("/"):
                path = path[:-1]
            return make_dataset_urn_with_platform_instance(
                platform="gcs",
                name=path,
                env=self.config.env,
                platform_instance=None,
            )
        if url.startswith("azure://"):
            # Snowflake stores azure://<account>.blob.core.windows.net/<container>/<path>
            abs_url = url.replace("azure://", "https://", 1)
            return make_abs_urn(abs_url, self.config.env)
        logger.debug(f"Unsupported external stage URL scheme: {url}")
        return None

    def get_stage_lookup_entry(self, stage_fqn: str) -> Optional[StageLookupEntry]:
        return self.stage_lookup.get(stage_fqn.upper())
