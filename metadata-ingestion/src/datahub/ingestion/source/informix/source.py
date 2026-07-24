import time
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
)
from datahub.ingestion.source.informix.client import (
    InformixClient,
    InformixClientProtocol,
)
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM
from datahub.ingestion.source.informix.mapping import (
    build_foreign_key_constraints,
    columns_to_schema_fields,
    make_table_identifier,
)
from datahub.ingestion.source.informix.report import InformixSourceReport
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


@platform_name("Informix", id="informix")
@config_class(InformixSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
@capability(SourceCapability.DATA_PROFILING, "Row counts only, via systables.nrows")
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
            )

            seen_owners = set()
            for table in client.get_tables():
                if not self.config.schema_pattern.allowed(table.owner):
                    self.report.filtered += 1
                    continue
                pattern = (
                    self.config.view_pattern
                    if table.is_view
                    else self.config.table_pattern
                )
                if not pattern.allowed(table.name):
                    self.report.filtered += 1
                    continue

                if table.owner not in seen_owners:
                    seen_owners.add(table.owner)
                    yield Container(
                        self._schema_key(table.owner),
                        display_name=table.owner,
                        subtype=DatasetContainerSubTypes.SCHEMA,
                        parent_container=db_key,
                    )

                # Isolate per-table failures: one broken/inaccessible object
                # degrades to a warning, the run continues.
                try:
                    columns = client.get_columns(table)
                    fields = columns_to_schema_fields(columns, self.report)
                    name = make_table_identifier(
                        self.config.database, table.owner, table.name
                    )
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
                    )
                    yield dataset
                    if table.is_view:
                        self.report.views_scanned += 1
                    else:
                        self.report.tables_scanned += 1
                        if self.config.include_row_counts and table.nrows is not None:
                            yield MetadataChangeProposalWrapper(
                                entityUrn=dataset.urn.urn(),
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
        finally:
            client.close()

    def get_report(self) -> InformixSourceReport:
        return self.report
