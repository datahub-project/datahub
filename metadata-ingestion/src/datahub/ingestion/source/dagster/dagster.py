import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type, Union

import datahub.emitter.mce_builder as builder
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
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MySqlDDLClass,
    NumberTypeClass,
    OwnerClass,
    OwnershipClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.specific.dataset import DatasetPatchBuilder

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
    SourceCapability.LINEAGE_FINE,
    "Enabled when assets expose column lineage metadata; toggle via `include_column_lineage`",
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
        # Caches existence checks in enrich mode (fail-open on error).
        self._exists_cache: Dict[str, bool] = {}

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

    def _ownership_aspect(self, owners: List[DagsterOwner]) -> Optional[OwnershipClass]:
        if not self.config.include_ownership or not owners:
            return None
        # default_ownership_type is validated by config to be a valid
        # OwnershipTypeClass string value, so it can be used directly.
        ownership_type = self.config.default_ownership_type
        owner_classes = []
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
        if self.config.extraction_mode == "enrich" and self.ctx.graph is None:
            self.report.report_failure(
                message="enrich mode requires a DataHub connection",
                context="Configure a sink or `datahub_api` so the source can check "
                "which assets exist, or set `extraction_mode: full`.",
            )
            return

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
            # Isolate per-repository failures so one bad repository does not
            # abort the entire ingestion run.
            try:
                yield from self._process_repository(repo)
            except Exception as e:
                self.report.report_warning(
                    message="Failed to process repository",
                    context=repo.name,
                    exc=e,
                )

    def _process_repository(
        self, repo: DagsterRepository
    ) -> Iterable[MetadataWorkUnit]:
        allowed_assets = self._allowed_assets(repo)

        if self.config.include_assets:
            for asset in allowed_assets:
                # Per-asset isolation: a single malformed asset should not drop
                # the rest of the repository.
                try:
                    yield from self._emit_asset(asset)
                except Exception as e:
                    self.report.report_warning(
                        message="Failed to emit asset",
                        context=".".join(asset.key),
                        exc=e,
                    )

        # Enrich mode only tops up existing assets; jobs/ops are entity creation
        # and belong to the plugin (or full mode).
        if self.config.extraction_mode == "enrich":
            return

        # Jobs/ops are implementation artifacts; emit them only when explicitly
        # requested. The asset-to-asset dependency graph is emitted separately
        # (see _emit_asset), so disabling jobs does not lose lineage.
        if not self.config.include_jobs:
            return

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
            try:
                yield from self._emit_dataflow(job_name, job, repo.location_name)
            except Exception as e:
                self.report.report_warning(
                    message="Failed to emit job", context=job_name, exc=e
                )

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
        if self.config.extraction_mode == "enrich":
            yield from self._enrich_asset(asset)
            return

        custom_properties = dict(asset.metadata.custom_properties)
        if asset.group_name:
            custom_properties["group_name"] = asset.group_name
        if asset.job_names:
            custom_properties["job_names"] = ", ".join(asset.job_names)

        ownership = self._ownership_aspect(asset.owners)
        tags = self._tags_aspect(
            asset.tags,
            group_name=asset.group_name,
            kinds=asset.kinds,
            compute_kind=asset.compute_kind,
        )
        links = (
            self._institutional_memory(asset) if self.config.include_metadata else None
        )
        schema = self._schema_metadata(asset) if self.config.include_metadata else None
        upstreams = self._upstream_lineage(asset, self._asset_dataset_urn(asset.key))

        dataset = Dataset(
            platform=DAGSTER_PLATFORM,
            name=".".join(asset.key),
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            subtype=ASSET_SUBTYPE,
            description=asset.description if self.config.include_metadata else None,
            custom_properties=custom_properties,
            owners=ownership.owners if ownership else None,
            tags=tags.tags if tags else None,
            links=links.elements if links else None,
            schema=schema,
            upstreams=upstreams,
        )
        yield from dataset.as_workunits()

    def _asset_exists(self, urn: str) -> bool:
        # Cached, fail-open: on a transient graph error, keep enriching rather
        # than silently dropping the asset.
        if urn not in self._exists_cache:
            assert self.ctx.graph is not None
            try:
                self._exists_cache[urn] = self.ctx.graph.exists(urn)
            except Exception as e:
                self.report.report_warning(
                    message="Existence check failed; enriching anyway",
                    context=urn,
                    exc=e,
                )
                self._exists_cache[urn] = True
        return self._exists_cache[urn]

    def _enrich_asset(self, asset: DagsterAsset) -> Iterable[MetadataWorkUnit]:
        # Enrich an already-existing asset (created by the plugin) with PATCH
        # aspects only — never entity-defining aspects, and never overwriting.
        urn = self._asset_dataset_urn(asset.key)
        if not self._asset_exists(urn):
            self.report.assets_skipped_absent += 1
            return
        self.report.assets_enriched += 1

        patch = DatasetPatchBuilder(urn)
        if self.config.include_metadata and asset.description:
            patch.set_description(asset.description)
        ownership = self._ownership_aspect(asset.owners)
        if ownership and ownership.owners:
            # set_owners (replace) rather than per-owner add: the compound-key
            # owner patch isn't supported across GMS versions.
            patch.set_owners(ownership.owners)
        tags = self._tags_aspect(
            asset.tags,
            group_name=asset.group_name,
            kinds=asset.kinds,
            compute_kind=asset.compute_kind,
        )
        if tags:
            for tag in tags.tags:
                patch.add_tag(tag)
        upstreams = self._upstream_lineage(asset, urn)
        if upstreams:
            for upstream in upstreams.upstreams:
                patch.add_upstream_lineage(upstream)
            for fine_grained in upstreams.fineGrainedLineages or []:
                patch.add_fine_grained_upstream_lineage(fine_grained)

        for mcp in patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
            )

    def _upstream_lineage(
        self, asset: DagsterAsset, urn: str
    ) -> Optional[UpstreamLineageClass]:
        # Collect the table-level upstream datasets. Datasets referenced only by
        # column lineage are added too, so every fine-grained edge has a
        # backing coarse edge (DataHub needs the dataset-level upstream to
        # render the field-level one).
        upstream_urns: Set[str] = set()
        if self.config.include_asset_lineage:
            for upstream_key in asset.upstream_keys:
                upstream_urns.add(self._asset_dataset_urn(upstream_key))

        fine_grained: List[FineGrainedLineageClass] = []
        if self.config.include_column_lineage:
            for col_lineage in asset.metadata.column_lineage:
                upstream_fields = []
                for dep in col_lineage.upstreams:
                    dep_urn = self._asset_dataset_urn(dep.asset_key)
                    upstream_urns.add(dep_urn)
                    upstream_fields.append(
                        builder.make_schema_field_urn(dep_urn, dep.column)
                    )
                if not upstream_fields:
                    continue
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=upstream_fields,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(
                                urn, col_lineage.downstream_column
                            )
                        ],
                    )
                )

        if not upstream_urns and not fine_grained:
            return None

        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=dataset_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
                for dataset_urn in sorted(upstream_urns)
            ],
            fineGrainedLineages=fine_grained or None,
        )

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
        ownership = self._ownership_aspect(job.owners) if job else None
        tags = self._tags_aspect(job.tags) if job else None
        dataflow = DataFlow(
            platform=DAGSTER_PLATFORM,
            name=f"{self._id_prefix(location_name)}/{job_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=job_name,
            description=job.description
            if job and self.config.include_metadata
            else None,
            external_url=self._job_url(job_name),
            owners=ownership.owners if ownership else None,
            tags=tags.tags if tags else None,
        )
        yield from dataflow.as_workunits()

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
            datajob = DataJob(
                name=f"{self._id_prefix(location_name)}/{op_name}",
                flow_urn=str(self._dataflow_urn(job_name, location_name)),
                platform_instance=self.config.platform_instance,
                display_name=op_name,
                external_url=self._job_url(job_name),
                inlets=sorted(io.inputs),
                outlets=sorted(io.outputs),
            )
            yield from datajob.as_workunits()

    def _job_url(self, job_name: str) -> Optional[str]:
        if self.config.is_cloud and self.config.deployment:
            return f"{self.config.host}/{self.config.deployment}/jobs/{job_name}"
        return f"{self.config.host}/jobs/{job_name}"
