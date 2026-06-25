import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type, Union

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dagster.config import (
    DAGSTER_PLATFORM,
    DagsterSourceConfig,
    DagsterSourceReport,
)
from datahub.ingestion.source.dagster.dagster_api import DagsterGraphQLClient
from datahub.ingestion.source.dagster.data_classes import (
    DagsterAsset,
    DagsterJob,
    DagsterOwner,
    DagsterRepository,
    DagsterTag,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    AzkabanJobTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MySqlDDLClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn

logger = logging.getLogger(__name__)

ASSET_SUBTYPE = "Asset"
_INGESTION_ACTOR = "urn:li:corpuser:_ingestion"

# Coarse native-type -> DataHub type mapping for asset table schemas. Unknown
# types fall back to StringType.
_FIELD_TYPE_MAPPING: Dict[
    str, Type[Union[StringTypeClass, NumberTypeClass, TimeTypeClass]]
] = {
    "string": StringTypeClass,
    "text": StringTypeClass,
    "varchar": StringTypeClass,
    "int": NumberTypeClass,
    "integer": NumberTypeClass,
    "bigint": NumberTypeClass,
    "float": NumberTypeClass,
    "double": NumberTypeClass,
    "number": NumberTypeClass,
    "decimal": NumberTypeClass,
    "timestamp": TimeTypeClass,
    "datetime": TimeTypeClass,
    "date": TimeTypeClass,
}


@dataclass
class _OpIO:
    """Accumulated input/output asset dataset URNs for a single op within a job."""

    inputs: Set[str] = field(default_factory=set)
    outputs: Set[str] = field(default_factory=set)


@platform_name("Dagster")
@config_class(DagsterSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE, "Enabled by default via asset dependencies"
)
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, can be disabled via `include_ownership`",
)
@capability(
    SourceCapability.TAGS, "Enabled by default, can be disabled via `include_tags`"
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
)
class DagsterSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests jobs, ops, and Software-Defined Assets from a Dagster instance via its GraphQL API.

    This is a pull-based source: it scrapes the Dagster webserver's `/graphql`
    endpoint and emits DataFlow (jobs), DataJob (ops), and Dataset (assets)
    entities, along with their lineage, descriptions, ownership, tags, and
    documentation. It complements the push-based `datahub-dagster-plugin` and
    deliberately produces URNs matching that plugin's conventions.
    """

    config: DagsterSourceConfig
    report: DagsterSourceReport
    platform: str = DAGSTER_PLATFORM

    def __init__(self, config: DagsterSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = DagsterSourceReport()
        self.client = DagsterGraphQLClient(config)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DagsterSource":
        config = DagsterSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = DagsterSourceConfig.parse_obj(config_dict)
            DagsterGraphQLClient(config).test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> SourceReport:
        return self.report

    # --- id / urn construction (mirrors the push plugin's conventions) ---

    def _id_prefix(self, location_name: str) -> str:
        if self.config.is_cloud and self.config.deployment:
            return f"{self.config.deployment}/{location_name}"
        return location_name

    def _dataflow_urn(self, job_name: str, location_name: str) -> DataFlowUrn:
        return DataFlowUrn.create_from_ids(
            orchestrator=DAGSTER_PLATFORM,
            flow_id=f"{self._id_prefix(location_name)}/{job_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

    def _datajob_urn(
        self, job_name: str, op_name: str, location_name: str
    ) -> DataJobUrn:
        return DataJobUrn.create_from_ids(
            data_flow_urn=str(self._dataflow_urn(job_name, location_name)),
            job_id=f"{self._id_prefix(location_name)}/{op_name}",
        )

    def _asset_dataset_urn(self, asset_key: List[str]) -> str:
        return builder.make_dataset_urn(
            platform=DAGSTER_PLATFORM,
            name=".".join(asset_key),
            env=self.config.env,
        )

    # --- shared aspect builders ---

    def _platform_instance_aspect(self) -> DataPlatformInstanceClass:
        instance = (
            builder.make_dataplatform_instance_urn(
                DAGSTER_PLATFORM, self.config.platform_instance
            )
            if self.config.platform_instance
            else None
        )
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(DAGSTER_PLATFORM),
            instance=instance,
        )

    def _ownership_aspect(self, owners: List[DagsterOwner]) -> Optional[OwnershipClass]:
        if not self.config.include_ownership or not owners:
            return None
        owner_classes = []
        ownership_type = OwnershipTypeClass.__dict__.get(
            self.config.default_ownership_type, self.config.default_ownership_type
        )
        for owner in owners:
            if owner.email:
                owner_urn = builder.make_user_urn(owner.email)
            elif owner.team:
                team = (
                    owner.team[len("team:") :]
                    if owner.team.startswith("team:")
                    else owner.team
                )
                owner_urn = builder.make_group_urn(team)
            else:
                continue
            owner_classes.append(OwnerClass(owner=owner_urn, type=ownership_type))
        if not owner_classes:
            return None
        return OwnershipClass(
            owners=owner_classes,
            lastModified=AuditStampClass(time=0, actor=_INGESTION_ACTOR),
        )

    def _tags_aspect(
        self,
        tags: List[DagsterTag],
        *,
        group_name: Optional[str] = None,
        kinds: Optional[List[str]] = None,
        compute_kind: Optional[str] = None,
    ) -> Optional[GlobalTagsClass]:
        if not self.config.include_tags:
            return None
        tag_strings: Set[str] = set()
        for tag in tags:
            tag_strings.add(f"{tag.key}:{tag.value}" if tag.value else tag.key)
        for kind in kinds or []:
            tag_strings.add(kind)
        if compute_kind:
            tag_strings.add(compute_kind)
        if group_name:
            tag_strings.add(f"asset_group:{group_name}")
        if not tag_strings:
            return None
        return GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=builder.make_tag_urn(tag))
                for tag in sorted(tag_strings)
            ]
        )

    # --- emission ---

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Override get_workunits_internal (not get_workunits) so the base class
        # threads emissions through get_workunit_processors() — that is what
        # auto-generates Status / browse paths and applies stale-entity removal.
        try:
            repositories = self.client.get_repositories()
        except Exception as e:
            self.report.report_failure(
                message="Failed to fetch repositories from Dagster", exc=e
            )
            return

        for repo in repositories:
            if not self.config.repository_pattern.allowed(repo.name):
                self.report.report_repository_filtered(repo.name)
                continue
            self.report.repositories_scanned += 1
            yield from self._process_repository(repo)

    def _process_repository(
        self, repo: DagsterRepository
    ) -> Iterable[MetadataWorkUnit]:
        allowed_assets = self._allowed_assets(repo)

        if self.config.include_assets:
            for asset in allowed_assets:
                yield from self._emit_asset(asset)

        # Every job referenced by jobs metadata or by an allowed asset becomes a
        # DataFlow, so each DataJob has a parent flow with dataFlowInfo.
        jobs_by_name: Dict[str, Optional[DagsterJob]] = {
            job.name: job for job in repo.jobs
        }
        for asset in allowed_assets:
            for job_name in asset.job_names:
                jobs_by_name.setdefault(job_name, None)

        for job_name, job in jobs_by_name.items():
            if not self.config.job_pattern.allowed(job_name):
                self.report.report_job_filtered(job_name)
                continue
            self.report.jobs_scanned += 1
            yield from self._emit_dataflow(job_name, job, repo.location_name)

        yield from self._emit_datajobs(allowed_assets, jobs_by_name, repo.location_name)

    def _allowed_assets(self, repo: DagsterRepository) -> List[DagsterAsset]:
        allowed: List[DagsterAsset] = []
        for asset in repo.assets:
            dotted = ".".join(asset.key)
            if not self.config.asset_pattern.allowed(dotted):
                self.report.report_asset_filtered(dotted)
                continue
            self.report.assets_scanned += 1
            allowed.append(asset)
        return allowed

    def _emit_asset(self, asset: DagsterAsset) -> Iterable[MetadataWorkUnit]:
        urn = self._asset_dataset_urn(asset.key)

        custom_properties = dict(asset.metadata.custom_properties)
        if asset.group_name:
            custom_properties["group_name"] = asset.group_name
        if asset.job_names:
            custom_properties["job_names"] = ", ".join(asset.job_names)

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(
                name=asset.key[-1],
                description=asset.description if self.config.include_metadata else None,
                customProperties=custom_properties,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=SubTypesClass(typeNames=[ASSET_SUBTYPE])
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=self._platform_instance_aspect()
        ).as_workunit()

        ownership = self._ownership_aspect(asset.owners)
        if ownership:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=ownership
            ).as_workunit()

        tags = self._tags_aspect(
            asset.tags,
            group_name=asset.group_name,
            kinds=asset.kinds,
            compute_kind=asset.compute_kind,
        )
        if tags:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=tags
            ).as_workunit()

        if self.config.include_metadata:
            links = self._institutional_memory(asset)
            if links:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=links
                ).as_workunit()
            schema = self._schema_metadata(asset)
            if schema:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=schema
                ).as_workunit()

        if self.config.include_asset_lineage and asset.upstream_keys:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=self._asset_dataset_urn(upstream_key),
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                        for upstream_key in asset.upstream_keys
                    ]
                ),
            ).as_workunit()

    def _institutional_memory(
        self, asset: DagsterAsset
    ) -> Optional[InstitutionalMemoryClass]:
        if not asset.metadata.links:
            return None
        return InstitutionalMemoryClass(
            elements=[
                InstitutionalMemoryMetadataClass(
                    url=link.url,
                    description=link.description,
                    createStamp=AuditStampClass(time=0, actor=_INGESTION_ACTOR),
                )
                for link in asset.metadata.links
            ]
        )

    def _schema_metadata(self, asset: DagsterAsset) -> Optional[SchemaMetadataClass]:
        if not asset.metadata.columns:
            return None
        fields = [
            SchemaFieldClass(
                fieldPath=col.name,
                nativeDataType=col.native_type,
                type=SchemaFieldDataTypeClass(
                    type=_FIELD_TYPE_MAPPING.get(
                        col.native_type.lower(), StringTypeClass
                    )()
                ),
                nullable=col.nullable,
                description=col.description,
            )
            for col in asset.metadata.columns
        ]
        return SchemaMetadataClass(
            schemaName=".".join(asset.key),
            platform=builder.make_data_platform_urn(DAGSTER_PLATFORM),
            version=0,
            hash="",
            platformSchema=MySqlDDLClass(tableSchema=""),
            fields=fields,
        )

    def _emit_dataflow(
        self, job_name: str, job: Optional[DagsterJob], location_name: str
    ) -> Iterable[MetadataWorkUnit]:
        urn = str(self._dataflow_urn(job_name, location_name))
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DataFlowInfoClass(
                name=job_name,
                description=job.description
                if job and self.config.include_metadata
                else None,
                externalUrl=self._job_url(job_name),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=self._platform_instance_aspect()
        ).as_workunit()

        if job is not None:
            ownership = self._ownership_aspect(job.owners)
            if ownership:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=ownership
                ).as_workunit()
            tags = self._tags_aspect(job.tags)
            if tags:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=tags
                ).as_workunit()

    def _emit_datajobs(
        self,
        assets: List[DagsterAsset],
        jobs_by_name: Dict[str, Optional[DagsterJob]],
        location_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        # Group asset I/O by (job, op): a multi-asset op produces several assets,
        # so we accumulate rather than overwrite.
        op_io: Dict[Tuple[str, str], _OpIO] = defaultdict(_OpIO)
        for asset in assets:
            op_name = asset.op_names[0] if asset.op_names else asset.key[-1]
            asset_urn = self._asset_dataset_urn(asset.key)
            for job_name in asset.job_names:
                if not self.config.job_pattern.allowed(job_name):
                    continue
                io = op_io[(job_name, op_name)]
                io.outputs.add(asset_urn)
                for upstream_key in asset.upstream_keys:
                    io.inputs.add(self._asset_dataset_urn(upstream_key))

        for (job_name, op_name), io in op_io.items():
            urn = str(self._datajob_urn(job_name, op_name, location_name))
            flow_urn = str(self._dataflow_urn(job_name, location_name))
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DataJobInfoClass(
                    name=op_name,
                    type=AzkabanJobTypeClass.COMMAND,
                    flowUrn=flow_urn,
                    externalUrl=self._job_url(job_name),
                ),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=self._platform_instance_aspect()
            ).as_workunit()

            # dataJobInputOutput is required for every DataJob, even if empty.
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=sorted(io.inputs),
                    outputDatasets=sorted(io.outputs),
                ),
            ).as_workunit()

    def _job_url(self, job_name: str) -> Optional[str]:
        if self.config.is_cloud and self.config.deployment:
            return f"{self.config.host}/{self.config.deployment}/jobs/{job_name}"
        return f"{self.config.host}/jobs/{job_name}"
