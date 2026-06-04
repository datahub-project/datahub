import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, Literal, Optional, Tuple

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
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.s3_util import (
    make_s3_urn_for_lineage,
    strip_s3_prefix,
)
from datahub.ingestion.source.azure.abs_utils import make_abs_urn, strip_abs_prefix
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.data_lake_common.path_urn_resolver import (
    DataLakePathResolver,
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
from datahub.utilities.urns.error import InvalidUrnError

logger: logging.Logger = logging.getLogger(__name__)


class StageKey(ContainerKey):
    database: str
    db_schema: str = Field(alias="schema")
    stage: str


ExternalStagePlatform = Literal["s3", "gcs", "abs"]


@dataclass(frozen=True)
class ParsedStageUrl:
    platform: ExternalStagePlatform
    path: str
    fallback_urn: str

    @property
    def bucket(self) -> str:
        return self.path.split("/", 1)[0]


@dataclass(frozen=True)
class StageLookupEntry:
    stage: SnowflakeStage
    container_key: StageKey
    dataset_urns: Tuple[str, ...] = ()


@dataclass
class SnowflakeStagesExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    graph: Optional[DataHubGraph] = None

    stage_lookup: Dict[str, StageLookupEntry] = field(default_factory=dict)

    _stage_url_cache: Dict[str, Tuple[str, ...]] = field(default_factory=dict)

    _path_resolver: Optional[DataLakePathResolver] = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.graph is not None:
            self._path_resolver = DataLakePathResolver(
                graph=self.graph, env=self.config.env
            )

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

            yield from self._gen_stage_container(
                stage=stage,
                stage_key=stage_key,
                parent_key=schema_container_key,
            )

            if stage.stage_type == SnowflakeStageType.INTERNAL:
                dataset_urn = self._gen_internal_stage_dataset_urn(stage)
                dataset_urns: Tuple[str, ...] = (dataset_urn,)
                yield from self._gen_internal_stage_dataset(
                    stage=stage,
                    dataset_urn=dataset_urn,
                    stage_key=stage_key,
                )
            else:
                dataset_urns = self._resolve_external_stage_url(stage.url)

            self.stage_lookup[stage_identifier] = StageLookupEntry(
                stage=stage,
                container_key=stage_key,
                dataset_urns=dataset_urns,
            )

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

    def _resolve_external_stage_url(self, url: Optional[str]) -> Tuple[str, ...]:
        if not url:
            return ()
        if url in self._stage_url_cache:
            return self._stage_url_cache[url]

        urns = self._compute_external_stage_urns(url)
        if urns is None:
            # Transient failure — do not cache so a later lookup can retry.
            return ()
        self._stage_url_cache[url] = urns
        return urns

    def _compute_external_stage_urns(self, url: str) -> Optional[Tuple[str, ...]]:
        try:
            parsed = self._parse_external_stage_url(url)
        except (ValueError, InvalidUrnError) as e:
            # InvalidUrnError is also caught because the gcs URN constructor rejects
            # an empty path (e.g. ``gcs://``) before our downstream guard can fire.
            self.report.warning(
                title="Failed to parse external stage URL",
                message="Stage lineage will be skipped for this stage.",
                context=url,
                exc=e,
            )
            return ()

        if parsed is None:
            self.report.info(
                title="Unsupported external stage URL scheme",
                message="Stage lineage will be skipped. Only s3://, gcs://, and azure:// are supported.",
                context=url,
            )
            return ()

        if (
            self.config.resolve_external_stage_lineage_via_graph
            and self._path_resolver is not None
        ):
            return self._resolve_stage_via_graph(parsed, url)
        return (parsed.fallback_urn,)

    def _parse_external_stage_url(self, url: str) -> Optional[ParsedStageUrl]:
        if url.startswith("s3://"):
            return ParsedStageUrl(
                platform="s3",
                path=strip_s3_prefix(url).rstrip("/"),
                fallback_urn=make_s3_urn_for_lineage(url, self.config.env),
            )
        if url.startswith("gcs://"):
            path = url[len("gcs://") :].rstrip("/")
            return ParsedStageUrl(
                platform="gcs",
                path=path,
                fallback_urn=make_dataset_urn_with_platform_instance(
                    platform="gcs",
                    name=path,
                    env=self.config.env,
                    platform_instance=None,
                ),
            )
        if url.startswith("azure://"):
            # Snowflake's azure:// is the same host structure as Azure's https:// URL,
            # which is what make_abs_urn / strip_abs_prefix expect.
            abs_url = url.replace("azure://", "https://", 1)
            return ParsedStageUrl(
                platform="abs",
                path=strip_abs_prefix(abs_url).rstrip("/"),
                fallback_urn=make_abs_urn(abs_url, self.config.env),
            )
        return None

    def _resolve_stage_via_graph(
        self, parsed: ParsedStageUrl, url: str
    ) -> Optional[Tuple[str, ...]]:
        # ``raise`` (not ``assert``) so the guard stays loud under ``python -O``.
        if self._path_resolver is None:
            raise RuntimeError(
                "_resolve_stage_via_graph requires a path resolver; "
                "guard on self._path_resolver in the caller."
            )

        if not parsed.path:
            self.report.warning(
                title="External stage URL has no path component",
                message=(
                    "Cannot resolve stage lineage; URL has empty path after stripping "
                    "the scheme. This usually indicates a malformed stage definition."
                ),
                context=url,
            )
            self.report.external_stage_lineage_unresolved += 1
            return ()

        result = self._path_resolver.resolve_datasets_under_path(
            platform=parsed.platform,
            bucket=parsed.bucket,
            path=parsed.path,
            platform_instance=self.config.external_stage_platform_instance,
        )

        if result.transient_error is not None:
            self.report.warning(
                title="Failed to resolve external stage lineage via graph",
                message="Stage lineage will be skipped for this stage.",
                context=url,
                exc=result.transient_error,
            )
            self.report.external_stage_lineage_unresolved += 1
            return None

        if not result.matched_urns:
            self.report.warning(
                title="External stage lineage unresolved",
                message=(
                    "No existing dataset URNs found for this stage. Lineage will be "
                    "skipped until the underlying data lake source has been ingested; "
                    "re-run after the data lake source completes."
                ),
                context=f"platform={parsed.platform}, url={url}",
            )
            self.report.external_stage_lineage_unresolved += 1
            return ()

        self.report.external_stage_lineage_resolved += 1
        return result.matched_urns

    def get_stage_lookup_entry(self, stage_fqn: str) -> Optional[StageLookupEntry]:
        return self.stage_lookup.get(stage_fqn.upper())
