import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Union

from datahub.configuration.common import ConfigurationError
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
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
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
    sanitize_informix_error,
)
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM
from datahub.ingestion.source.informix.lineage import build_view_upstream_lineage
from datahub.ingestion.source.informix.mapping import (
    build_foreign_key_constraints,
    build_owners,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.models import (
    InformixForeignKey,
    InformixTable,
)
from datahub.ingestion.source.informix.report import InformixSourceReport
from datahub.ingestion.source.sql.sql_utils import gen_domain_urn
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    OwnerClass,
    SchemaFieldClass,
    SchemalessClass,
    SchemaMetadataClass,
    ViewPropertiesClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.registries.domain_registry import DomainRegistry


@dataclass
class _PendingView:
    # A view emitted in pass 1, carried to pass 2 for viewProperties + lineage.
    table: InformixTable
    urn: str
    columns: List[str]


@platform_name("Informix", id="informix")
@config_class(InformixSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.OWNERSHIP,
    "Schema/table/view owner from `systables.owner`, via the `include_ownership` "
    "config field",
)
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
class InformixSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Informix databases via the proprietary JDBC driver and system catalogs."""

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

    @staticmethod
    def test_connection(config_dict: Dict[str, object]) -> TestConnectionReport:
        test_report = TestConnectionReport()
        client: Optional[InformixClient] = None
        config: Optional[InformixSourceConfig] = None
        try:
            config = InformixSourceConfig.parse_obj_allow_extras(config_dict)
            # Constructing the client resolves the JDBC driver, starts the JVM and
            # opens the connection, so a successful get_tables() exercises every
            # step the real run depends on.
            client = InformixClient(config)
            tables = client.get_tables()
            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {
                SourceCapability.SCHEMA_METADATA: CapabilityReport(
                    capable=True,
                    mitigation_message=(
                        f"Listed {len(tables)} tables/views from the system catalog"
                    ),
                ),
                SourceCapability.LINEAGE_COARSE: CapabilityReport(
                    capable=True,
                    mitigation_message=(
                        "View lineage is available when include_view_lineage is enabled"
                    ),
                ),
            }
        except Exception as error:
            if isinstance(error, ConfigurationError):
                # Already sanitized by InformixClient (never includes the JDBC URL).
                failure_reason = str(error)
            elif config is not None:
                failure_reason = sanitize_informix_error(
                    error, config, "test connection"
                )
            else:
                failure_reason = str(error)
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=failure_reason
            )
        finally:
            if client is not None:
                client.close()
        return test_report

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

    def _owners(self, owner: str) -> Optional[List[OwnerClass]]:
        if not self.config.include_ownership:
            return None
        return build_owners(owner)

    def _schema_key(self, owner: str) -> ContainerKey:
        # SchemaKey accepts the schema name via the `schema` constructor kwarg
        # (the underlying field is `db_schema`, exposed by alias).
        return SchemaKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            database=self.config.database,
            schema=owner,
        )

    def _build_table_schema(
        self,
        client: InformixClientProtocol,
        table: InformixTable,
        name: str,
        fields: List[SchemaFieldClass],
    ) -> Union[List[SchemaFieldClass], SchemaMetadataClass]:
        if table.is_view or not self.config.include_foreign_keys:
            return fields

        usable_fks: List[InformixForeignKey] = []
        for fk in client.get_foreign_keys(table):
            if len(fk.child_columns) != len(fk.parent_columns):
                # Defense in depth: InformixClient already drops these, but fake
                # clients / model_construct can still hand mismatched lists through.
                # sourceFields/foreignFields pair positionally, so unequal lists
                # would emit a constraint joining the wrong columns.
                self.report.foreign_keys_dropped_mismatched += 1
                self.report.warning(
                    title="Skipped foreign key with mismatched column counts",
                    message="Informix's catalog returned a different number of "
                    "child and parent index columns, so the constraint cannot be "
                    "paired reliably.",
                    context=f"{table.owner}.{table.name} fk={fk.name} "
                    f"child={len(fk.child_columns)} parent={len(fk.parent_columns)}",
                )
                continue
            if len(fk.child_columns) > 1:
                self.report.warning(
                    title="Composite foreign key columns may be misaligned",
                    message="Informix's catalog does not guarantee child/parent "
                    "column pairing order for composite keys; columns are paired "
                    "best-effort.",
                    context=f"{table.owner}.{table.name} fk={fk.name}",
                )
            usable_fks.append(fk)

        if not usable_fks:
            return fields

        child_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        return SchemaMetadataClass(
            schemaName="",
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=fields,
            foreignKeys=build_foreign_key_constraints(
                fks=usable_fks,
                child_dataset_urn=child_urn,
                database=self.config.database,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
                convert_to_lowercase=self.config.convert_urns_to_lowercase,
            ),
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
                name = make_table_identifier(
                    self.config.database,
                    table.owner,
                    table.name,
                    self.config.convert_urns_to_lowercase,
                )
                if not self.config.schema_pattern.allowed(table.owner):
                    self.report.report_dropped(name)
                    continue
                if table.is_view and not self.config.include_views:
                    self.report.report_dropped(name)
                    continue
                if not table.is_view and not self.config.include_tables:
                    self.report.report_dropped(name)
                    continue
                pattern = (
                    self.config.view_pattern
                    if table.is_view
                    else self.config.table_pattern
                )
                if not pattern.allowed(name):
                    self.report.report_dropped(name)
                    continue

                self.report.objects_selected += 1
                schema_key = self._schema_key(table.owner)
                owners = self._owners(table.owner)

                if table.owner not in seen_owners:
                    seen_owners.add(table.owner)
                    yield Container(
                        schema_key,
                        display_name=table.owner,
                        subtype=DatasetContainerSubTypes.SCHEMA,
                        parent_container=db_key,
                        domain=self._domain_urn(table.owner),
                        owners=owners,
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

                    schema: Union[List[SchemaFieldClass], SchemaMetadataClass] = (
                        self._build_table_schema(client, table, name, fields)
                    )

                    dataset = Dataset(
                        platform=self.platform,
                        name=name,
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                        subtype=subtype,
                        parent_container=schema_key,
                        schema=schema,
                        display_name=table.name,
                        domain=self._domain_urn(name),
                        owners=owners,
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

            # Per-object failures above are only warnings, so a systemic problem
            # (revoked catalog privileges, a dropped connection mid-scan) would
            # otherwise finish as a successful run that emitted nothing but
            # containers. Escalate once here instead.
            ingested = self.report.tables_scanned + self.report.views_scanned
            if self.report.objects_selected and not ingested:
                self.report.failure(
                    title="No tables or views could be ingested",
                    message="Every table and view selected from the catalog failed "
                    "extraction. This usually indicates a systemic problem such as "
                    "missing privileges on the system catalogs rather than a "
                    "per-object issue; see the preceding warnings.",
                    context=f"{self.report.objects_selected} objects selected, "
                    "0 ingested",
                )

            # Pass 2: emit viewProperties for every view with stored SQL, and
            # optionally parse that SQL for upstream lineage.
            for pending in views:
                try:
                    sql = client.get_view_definition(pending.table)
                except Exception as e:
                    self.report.warning(
                        title="Failed to fetch view definition",
                        message="Skipping viewProperties/lineage due to an error "
                        "reading sysviews.viewtext.",
                        context=f"{pending.table.owner}.{pending.table.name}",
                        exc=e,
                    )
                    continue
                if not sql:
                    # sysviews.viewtext is empty when the view text is
                    # unreadable (permissions) or was never stored. Count it
                    # so it is distinguishable from "parsed, no upstreams".
                    self.report.views_without_definition += 1
                    continue
                yield MetadataChangeProposalWrapper(
                    entityUrn=pending.urn,
                    aspect=ViewPropertiesClass(
                        materialized=False,
                        viewLogic=sql,
                        viewLanguage="SQL",
                    ),
                ).as_workunit()
                if not self.config.include_view_lineage:
                    continue
                try:
                    upstream_lineage = build_view_upstream_lineage(
                        view_urn=pending.urn,
                        view_sql=sql,
                        schema_resolver=resolver,
                        database=self.config.database,
                        owner=pending.table.owner,
                        report=self.report,
                        view_columns=pending.columns,
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

            # The empty-viewtext path above only bumps a counter, so the same
            # systemic failure the pass 1 escalation catches -- privileges
            # revoked on sysviews specifically, leaving syscolumns readable --
            # would emit no diagnostic at all. Every single view coming back
            # empty is that, not a database of views with no stored SQL.
            if views and self.report.views_without_definition == len(views):
                self.report.warning(
                    title="No view definitions could be read",
                    message="Every view returned an empty sysviews.viewtext, so "
                    "no viewProperties or view lineage was emitted. This usually "
                    "indicates missing privileges on sysviews rather than views "
                    "without stored SQL.",
                    context=f"{len(views)} views, 0 definitions read",
                )
        finally:
            client.close()

    def get_report(self) -> InformixSourceReport:
        return self.report
