import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Union

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, DatabaseKey, SchemaKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.informix.client import (
    InformixClient,
    InformixClientProtocol,
)
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM
from datahub.ingestion.source.informix.lineage import build_view_upstream_lineage
from datahub.ingestion.source.informix.mapping import (
    build_foreign_key_constraints,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.models import InformixTable
from datahub.ingestion.source.informix.report import InformixSourceReport
from datahub.ingestion.source.sql.sql_utils import gen_domain_urn
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    SchemaFieldClass,
    SchemalessClass,
    SchemaMetadataClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.registries.domain_registry import DomainRegistry


@dataclass
class _PendingView:
    # A view emitted in pass 1, carried to pass 2 for lineage parsing.
    table: InformixTable
    urn: str
    columns: List[str]


@platform_name("Informix", id="informix")
@config_class(InformixSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
@capability(SourceCapability.DATA_PROFILING, "Row counts only, via systables.nrows")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "View lineage",
    subtype_modifier=[SourceCapabilityModifier.VIEW],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level view lineage",
    subtype_modifier=[SourceCapabilityModifier.VIEW],
)
class InformixSource(StatefulIngestionSourceBase):
    config: InformixSourceConfig
    report: InformixSourceReport

    def __init__(
        self,
        ctx: PipelineContext,
        config: InformixSourceConfig,
        client: Optional[InformixClientProtocol] = None,
    ) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.platform = PLATFORM
        self.report = InformixSourceReport()
        self._client = client
        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=list(self.config.domain), graph=ctx.graph
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "InformixSource":
        config = InformixSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def _get_client(self) -> InformixClientProtocol:
        if self._client is None:
            self._client = InformixClient(self.config)
        return self._client

    def _database_key(self) -> DatabaseKey:
        return DatabaseKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            database=self.config.database,
        )

    def _domain_urn(self, name: str) -> Optional[str]:
        if not self.domain_registry:
            return None
        return gen_domain_urn(
            name,
            domain_config=self.config.domain,
            domain_registry=self.domain_registry,
        )

    def _schema_key(self, owner: str) -> ContainerKey:
        # SchemaKey's db_schema attribute is aliased to "schema" (to avoid
        # shadowing pydantic's BaseModel.schema()), so construct by alias.
        return SchemaKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            database=self.config.database,
            schema=owner,
        )

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Container, Dataset]]:
        client = self._get_client()
        try:
            db_key = self._database_key()
            yield Container(
                db_key,
                display_name=self.config.database,
                subtype=DatasetContainerSubTypes.DATABASE,
                domain=self._domain_urn(self.config.database),
            )

            # Pass 1 populates this resolver with every emitted dataset's schema so
            # pass 2 can resolve view SQL against it, including view-on-view
            # references and views defined earlier or later than their sources.
            resolver = SchemaResolver(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            views: List[_PendingView] = []

            seen_owners = set()
            for table in client.get_tables():
                if not self.config.schema_pattern.allowed(table.owner):
                    self.report.filtered += 1
                    continue
                if table.is_view and not self.config.include_views:
                    self.report.filtered += 1
                    continue
                if not table.is_view and not self.config.include_tables:
                    self.report.filtered += 1
                    continue
                name = make_table_identifier(
                    self.config.database, table.owner, table.name
                )
                pattern = (
                    self.config.view_pattern
                    if table.is_view
                    else self.config.table_pattern
                )
                if not pattern.allowed(name):
                    self.report.filtered += 1
                    continue

                if table.owner not in seen_owners:
                    seen_owners.add(table.owner)
                    yield Container(
                        self._schema_key(table.owner),
                        display_name=table.owner,
                        subtype=DatasetContainerSubTypes.SCHEMA,
                        parent_container=db_key,
                        domain=self._domain_urn(table.owner),
                    )

                # Isolate per-table failures: one broken/inaccessible object
                # degrades to a warning, the run continues.
                try:
                    columns = client.get_columns(table)
                    fields = columns_to_schema_fields(columns, self.report)
                    if table.is_view:
                        subtype = DatasetSubTypes.VIEW
                    else:
                        subtype = DatasetSubTypes.TABLE

                    schema: Union[List[SchemaFieldClass], SchemaMetadataClass] = fields
                    if self.config.include_foreign_keys and not table.is_view:
                        fks = client.get_foreign_keys(table)
                        for fk in fks:
                            if len(fk.child_columns) > 1:
                                self.report.warning(
                                    title="Composite foreign key columns may be misaligned",
                                    message="Informix's catalog does not guarantee "
                                    "child/parent column pairing order for composite "
                                    "keys; columns are paired best-effort.",
                                    context=f"{table.owner}.{table.name} fk={fk.name}",
                                )
                        if fks:
                            child_urn = make_dataset_urn_with_platform_instance(
                                platform=self.platform,
                                name=name,
                                platform_instance=self.config.platform_instance,
                                env=self.config.env,
                            )
                            fk_constraints = build_foreign_key_constraints(
                                fks,
                                child_urn,
                                self.config.database,
                                self.config.env,
                                self.config.platform_instance,
                            )
                            schema = SchemaMetadataClass(
                                schemaName="",
                                platform=make_data_platform_urn(self.platform),
                                version=0,
                                hash="",
                                platformSchema=SchemalessClass(),
                                fields=fields,
                                foreignKeys=fk_constraints,
                            )

                    dataset = Dataset(
                        platform=self.platform,
                        name=name,
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                        subtype=subtype,
                        parent_container=self._schema_key(table.owner),
                        schema=schema,
                        display_name=table.name,
                        domain=self._domain_urn(name),
                    )
                    yield dataset
                    dataset_urn = dataset.urn.urn()
                    resolver.add_raw_schema_info(
                        dataset_urn, {f.fieldPath: f.nativeDataType for f in fields}
                    )
                    if table.is_view:
                        self.report.views_scanned += 1
                        views.append(
                            _PendingView(
                                table=table,
                                urn=dataset_urn,
                                columns=[f.fieldPath for f in fields],
                            )
                        )
                    else:
                        self.report.tables_scanned += 1
                        if self.config.include_row_counts and table.nrows is not None:
                            yield MetadataChangeProposalWrapper(
                                entityUrn=dataset_urn,
                                aspect=DatasetProfileClass(
                                    timestampMillis=int(time.time() * 1000),
                                    rowCount=table.nrows,
                                ),
                            ).as_workunit()
                            self.report.row_counts_emitted += 1
                except Exception as e:
                    self.report.warning(
                        title="Failed to ingest table",
                        message="Skipping object due to an error during extraction.",
                        context=f"{table.owner}.{table.name}",
                        exc=e,
                    )

            if self.config.include_view_lineage:
                for pending in views:
                    try:
                        sql = client.get_view_definition(pending.table)
                        if not sql:
                            continue
                        upstream_lineage = build_view_upstream_lineage(
                            pending.urn,
                            sql,
                            resolver,
                            self.config.database,
                            pending.table.owner,
                            pending.columns,
                        )
                        if upstream_lineage is not None:
                            yield MetadataChangeProposalWrapper(
                                entityUrn=pending.urn, aspect=upstream_lineage
                            ).as_workunit()
                            self.report.views_with_lineage += 1
                    except Exception as e:
                        self.report.view_lineage_failures += 1
                        self.report.warning(
                            title="Failed to parse view lineage",
                            message="Skipping view lineage due to an error during "
                            "SQL parsing.",
                            context=f"{pending.table.owner}.{pending.table.name}",
                            exc=e,
                        )
        finally:
            client.close()

    def get_report(self) -> InformixSourceReport:
        return self.report
