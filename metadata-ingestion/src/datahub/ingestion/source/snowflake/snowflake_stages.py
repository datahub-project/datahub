import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Literal, NamedTuple, Optional, Tuple

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
from datahub.ingestion.graph.filters import RawSearchFilterRule
from datahub.ingestion.source.aws.s3_util import (
    make_s3_urn_for_lineage,
    strip_s3_prefix,
)
from datahub.ingestion.source.azure.abs_utils import make_abs_urn, strip_abs_prefix
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


ExternalStagePlatform = Literal["s3", "gcs", "abs"]


class ParsedStageUrl(NamedTuple):
    platform: ExternalStagePlatform
    path: str
    fallback_urn: str


@dataclass
class StageLookupEntry:
    stage: SnowflakeStage
    container_key: StageKey
    dataset_urns: List[str] = field(default_factory=list)


@dataclass
class SnowflakeStagesExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    graph: Optional[DataHubGraph] = None

    # Lookup map: "DB.SCHEMA.STAGE_NAME" -> StageLookupEntry
    stage_lookup: Dict[str, StageLookupEntry] = field(default_factory=dict)

    # Tuple, not List, because cached entries are returned to callers; an immutable
    # value prevents downstream mutation from corrupting the cache.
    _stage_url_cache: Dict[str, Tuple[str, ...]] = field(default_factory=dict)

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
                lookup_entry.dataset_urns = [dataset_urn]
                yield from self._gen_internal_stage_dataset(
                    stage=stage,
                    dataset_urn=dataset_urn,
                    stage_key=stage_key,
                )
            else:
                lookup_entry.dataset_urns = self._resolve_external_stage_url(stage.url)

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

    def _resolve_external_stage_url(self, url: Optional[str]) -> List[str]:
        if not url:
            return []
        if url in self._stage_url_cache:
            return list(self._stage_url_cache[url])

        urns = self._compute_external_stage_urns(url)
        if urns is None:
            # Transient failure (e.g. graph network blip). Don't cache so a later
            # stage with the same URL can retry. Counter is incremented at the
            # failure site.
            return []
        self._stage_url_cache[url] = tuple(urns)
        return urns

    def _compute_external_stage_urns(self, url: str) -> Optional[List[str]]:
        """Returns ``None`` on transient failure (caller should not cache).
        Returns a possibly-empty list on a deterministic outcome."""
        try:
            parsed = self._parse_external_stage_url(url)
        except ValueError as e:
            self.report.warning(
                title="Failed to parse external stage URL",
                message="Stage lineage will be skipped for this stage.",
                context=url,
                exc=e,
            )
            return []

        if parsed is None:
            self.report.info(
                title="Unsupported external stage URL scheme",
                message="Stage lineage will be skipped. Only s3://, gcs://, and azure:// are supported.",
                context=url,
            )
            return []

        if (
            self.config.resolve_external_stage_lineage_via_graph
            and self.graph is not None
        ):
            return self._resolve_stage_via_graph(
                platform=parsed.platform, path=parsed.path, url=url
            )
        return [parsed.fallback_urn]

    def _parse_external_stage_url(self, url: str) -> Optional[ParsedStageUrl]:
        """Map a Snowflake external stage URL to (DataHub platform, path, fallback URN)."""
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
        self, *, platform: ExternalStagePlatform, path: str, url: str
    ) -> Optional[List[str]]:
        """Find existing dataset URNs whose path equals or sits strictly under the stage path.

        Returns ``None`` on transient graph failure (signals "do not cache").
        """
        if self.graph is None:
            raise RuntimeError(
                "_resolve_stage_via_graph called without a graph client; "
                "this is a programming error in the dispatcher."
            )

        if not path:
            self.report.warning(
                title="External stage URL has no path component",
                message=(
                    "Cannot resolve stage lineage; URL has empty path after stripping "
                    "the scheme. This usually indicates a malformed stage definition."
                ),
                context=url,
            )
            self.report.external_stage_lineage_unresolved += 1
            return []

        platform_instance = self.config.external_stage_platform_instance
        path_prefix = f"{platform_instance}.{path}" if platform_instance else path
        urn_prefix = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{path_prefix}"

        extra_filters: List[RawSearchFilterRule] = [
            {
                "field": "urn",
                "condition": "START_WITH",
                "values": [urn_prefix],
            }
        ]

        try:
            candidate_urns = list(
                self.graph.get_urns_by_filter(
                    entity_types=["dataset"],
                    platform=platform,
                    platform_instance=platform_instance,
                    env=self.config.env,
                    extraFilters=extra_filters,
                )
            )
        except Exception as e:
            self.report.warning(
                title="Failed to resolve external stage lineage via graph",
                message="Stage lineage will be skipped for this stage.",
                context=url,
                exc=e,
            )
            self.report.external_stage_lineage_unresolved += 1
            return None

        matches = [
            urn for urn in candidate_urns if _dataset_path_is_rooted_at(urn, urn_prefix)
        ]

        if not matches:
            self.report.warning(
                title="External stage lineage unresolved",
                message=(
                    "No existing dataset URNs found for this stage. Lineage will be "
                    "skipped until the underlying data lake source has been ingested; "
                    "re-run after the data lake source completes."
                ),
                context=f"platform={platform}, url={url}",
            )
            self.report.external_stage_lineage_unresolved += 1
            return []

        self.report.external_stage_lineage_resolved += 1
        return matches

    def get_stage_lookup_entry(self, stage_fqn: str) -> Optional[StageLookupEntry]:
        return self.stage_lookup.get(stage_fqn.upper())


def _dataset_path_is_rooted_at(dataset_urn: str, urn_prefix: str) -> bool:
    """Reject false positives where a START_WITH URN filter matches `foo` against `foobar`."""
    if not dataset_urn.startswith(urn_prefix):
        return False
    # Char immediately after the path is `/` (child folder) or `,` (URN env separator, exact match).
    return dataset_urn[len(urn_prefix) : len(urn_prefix) + 1] in ("/", ",")
