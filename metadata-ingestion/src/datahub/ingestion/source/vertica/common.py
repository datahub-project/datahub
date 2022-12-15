import logging
import datetime
import traceback
from datahub.configuration.common import AllowDenyPattern
from dataclasses import dataclass
from datahub.ingestion.api.common import PipelineContext
from collections import defaultdict
from pydantic.fields import Field
from sqlalchemy import sql, create_engine
import json
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.engine.reflection import Inspector
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    PlatformKey
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    SQLSourceReport,
    SQLAlchemyConfig,
    SqlWorkUnit,
    get_schema_metadata,
    _field_type_mapping,
    _known_unknown_field_types
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
    dataset_urn_to_key
)
from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
    wrap_aspect_as_workunit
)

from datahub.metadata.com.linkedin.pegasus2avro.schema import (

    ForeignKeyConstraint,
    MySqlDDL,
    NullTypeClass,

    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,

)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    ViewPropertiesClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import (
        GEProfilerRequest,
    )
MISSING_COLUMN_INFO = "missing column information"
logger: logging.Logger = logging.getLogger(__name__)

#Extended SQLSourceReport from sql_common.py to add support for projection , MLModels and Outh metadata reports .
@dataclass
class SQLSourceReportVertica(SQLSourceReport):
    Projection_scanned: int = 0
    models_scanned: int = 0
    Outh_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a models or OAuth .
        """

        if ent_type == "projection":

            self.Projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        elif ent_type == "OAuth":
            self.Outh_scanned += 1
        else:
            super().report_entity_scanned(name, ent_type)

#Extended BasicSQLAlchemyConfig to config for projections,models and outh metadata.
class SQLAlchemyConfigVertica(BasicSQLAlchemyConfig):

    projection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for projection to filter in ingestion. Specify regex to match the entire projection name in database.schema.projection format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    models_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for ml models to filter in ingestion. "
    )
    oauth_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for OAuth to filter in ingestion. "
    )

    include_projections: Optional[bool] = Field(
        default=True, description="Whether projections should be ingested."
    )
    include_models: Optional[bool] = Field(
        default=True, description="Whether Models should be ingested."
    )
    include_Outh: Optional[bool] = Field(
        default=True, description="Whether OAuth should be ingested."
    )
    include_view_lineage: Optional[bool] = Field(
        default=True, description="Whether lineages should be ingested for views"
    )
    include_projection_lineage: Optional[bool] = Field(
        default=True, description="Whether lineages should be ingested for Projection"
    )


# config flags to emit telemetry for
config_options_to_report = [
    "include_views",
    "include_tables",
    "include_projections",
    "include_models",
    "include_Outh",
    "include_view_lineage",
    "include_projection_lineage"

]

# flags to emit telemetry for
profiling_flags_to_report = [
    "turn_off_expensive_profiling_metrics",
    "profile_table_level_only",
    "include_field_null_count",
    "include_field_min_value",
    "include_field_max_value",
    "include_field_mean_value",
    "include_field_median_value",
    "include_field_stddev_value",
    "include_field_quantiles",
    "include_field_distinct_value_frequencies",
    "include_field_histogram",
    "include_field_sample_values",
    "query_combiner_enabled",
]


class SchemaKeyHelper(SchemaKey):
    numberOfProjection: Optional[str]
    udxsFunctions : Optional[str] = None
    UDXsLanguage : Optional[str] = None


class DatabaseKeyHelper(DatabaseKey):
    clusterType : Optional[str] = None
    clusterSize : Optional[str] = None
    subClusters : Optional[str] = None
    communalStoragePath : Optional[str] = None


class VerticaSQLAlchemySource(SQLAlchemySource):

    def __init__(self, config: SQLAlchemyConfig, ctx: PipelineContext, platform: str):
        # self.platform = platform
        super(VerticaSQLAlchemySource, self).__init__(config, ctx, platform)

        self.report: SQLSourceReport = SQLSourceReportVertica()

    def get_column_type(sql_report: SQLSourceReport, dataset_name: str, column_type: Any
                        ) -> SchemaFieldDataType:
        """
        Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
        """

        TypeClass: Optional[Type] = None
        for sql_type in _field_type_mapping.keys():
            if isinstance(column_type, sql_type):
                TypeClass = _field_type_mapping[sql_type]
                break
        if TypeClass is None:
            for sql_type in _known_unknown_field_types:
                if isinstance(column_type, sql_type):
                    TypeClass = NullTypeClass
                    break

        if TypeClass is None:
            sql_report.report_warning(
                dataset_name, f"unable to map type {column_type!r} to metadata schema"
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_schema_metadata(
        sql_report: SQLSourceReport,
        dataset_name: str,
        platform: str,
        foreign_keys: Optional[List[ForeignKeyConstraint]] = None,
        canonical_schema: List[SchemaField] = [],
    ) -> SchemaMetadata:

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=canonical_schema,
        )
        if foreign_keys is not None and foreign_keys != []:
            schema_metadata.foreignKeys = foreign_keys

        return schema_metadata

    def gen_schema_key(self, db_name: str, schema: str) -> PlatformKey:
        try:
            all_properties_keys = dict()
            for inspector in self.get_inspectors():

                all_properties_keys = inspector._get_properties_keys(db_name , schema, level='schema')

            return SchemaKeyHelper(
                database=db_name,
                schema=schema,
                platform=self.platform,
                instance=self.config.platform_instance,
                backcompat_instance_for_guid=self.config.env,

                numberOfProjection=all_properties_keys.get("projection_count", ""),
                udxsFunctions=all_properties_keys.get("udx_list", ""),
                UDXsLanguage=all_properties_keys.get("Udx_langauge", ""),
            )
        except Exception as e:
            print("Hey something went wrong, while gettting schema in gen schema key")

    def gen_database_key(self, database: str) -> PlatformKey:
        try:
            all_properties_keys = dict()
            for inspector in self.get_inspectors():
                all_properties_keys = inspector._get_properties_keys(database , "schema", level='database')

            return DatabaseKeyHelper(
                database=database,
                platform=self.platform,
                instance=self.config.platform_instance,
                backcompat_instance_for_guid=self.config.env,


                clusterType=all_properties_keys.get("cluster_type"),
                clusterSize=all_properties_keys.get("cluster_size"),
                subClusters=all_properties_keys.get("Subcluster"),
                communalStoragePath=all_properties_keys.get("communinal_storage_path"),
            )
        except Exception as e:
            traceback.print_exc()
            print("Hey something went wrong, while gettting Generation of database key")

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", True)

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if sql_config.profiling.enabled:
            sql_config.options.setdefault(
                "max_overflow", sql_config.profiling.max_workers
            )

        for inspector in self.get_inspectors():
            profiler = None
            profile_requests: List["GEProfilerRequest"] = []
            if sql_config.profiling.enabled:
                profiler = self.get_profiler_instance(inspector)

            db_name = self.get_db_name(inspector)
            yield from self.gen_database_containers(db_name)

            for schema in self.get_allowed_schemas(inspector, db_name):
                self.add_information_for_schema(inspector, schema)

                yield from self.gen_schema_containers(schema, db_name)

                if sql_config.include_tables:
                    yield from self.loop_tables(inspector, schema, sql_config)

                if sql_config.include_views:
                    yield from self.loop_views(inspector, schema, sql_config)

                if sql_config.include_projections:
                    yield from self.loop_projections(inspector, schema, sql_config)
                if sql_config.include_models:
                    yield from self.loop_models(inspector, schema, sql_config)

                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )
                    profile_requests += list(
                        self.loop_profiler_requests_for_projections(inspector, schema, sql_config)
                    )

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )

            Outh_schema = "Entities"
            if sql_config.include_Outh:
                yield from self.loop_Oauth(inspector, Outh_schema, sql_config)

        # Clean up stale entities.
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            table_tags = self.get_extra_tags(inspector, schema, "table")
            for table in inspector.get_table_names(schema):
                schema, table = self.standardize_schema_table_names(
                    schema=schema, entity=table
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=table, inspector=inspector
                )
                dataset_name = self.normalise_dataset_name(dataset_name)
                if dataset_name not in tables_seen:
                    tables_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="table")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_table(
                        dataset_name, inspector, schema, table, sql_config, table_tags
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{table} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{table}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str, str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self._get_columns(dataset_name, inspector, schema, table)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("table", dataset_urn)
        description, properties, location_urn = self.get_table_properties(
            inspector, schema, table
        )
        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = table
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != table:
                properties["original_table_name"] = table
        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        # if location_urn:
        #     external_upstream_table = UpstreamClass(
        #         dataset=location_urn,
        #         type=DatasetLineageTypeClass.COPY,
        #     )
        #     lineage_mcpw = MetadataChangeProposalWrapper(
        #         entityType="dataset",
        #         changeType=ChangeTypeClass.UPSERT,
        #         entityUrn=dataset_snapshot.urn,
        #         aspectName="upstreamLineage",
        #         aspect=UpstreamLineage(upstreams=[external_upstream_table]),
        #     )
        #     lineage_wu = MetadataWorkUnit(
        #         id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
        #         mcp=lineage_mcpw,
        #     )
        #     self.report.report_workunit(lineage_wu)
        #     yield lineage_wu
        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(table, schema)

        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, table)

        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
        )
        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            pk_constraints,
            foreign_keys,
            schema_fields,
        )
        dataset_snapshot.aspects.append(schema_metadata)
        db_name = self.get_db_name(inspector)

        # table_tags = self.get_extra_tags(inspector, schema, table)

        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(table)}")]
            )
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["table"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect
        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            table_tags = self.get_extra_tags(inspector, schema, "view")
            for view in inspector.get_view_names(schema):
                schema, view = self.standardize_schema_table_names(
                    schema=schema, entity=view
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )
                dataset_name = self.normalise_dataset_name(dataset_name)
                self.report.report_entity_scanned(dataset_name, ent_type="view")
                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:

                    yield from self._process_view(
                        dataset_name=dataset_name,
                        inspector=inspector,
                        schema=schema,
                        view=view,
                        sql_config=sql_config,
                        table_tags=table_tags,
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{view}", f"Ingestion error: {e}"
                    )

                if sql_config.include_view_lineage:

                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn,
                            aspects=[StatusClass(removed=False)],
                        )
                        lineage_info = self._get_upstream_lineage_info(dataset_urn, view)

                        if lineage_info is not None:
                            # Emit the lineage work unit
                            # upstream_column_props = []
                            upstream_lineage = lineage_info
                            lineage_mcpw = MetadataChangeProposalWrapper(
                                entityType="dataset",
                                changeType=ChangeTypeClass.UPSERT,
                                entityUrn=dataset_snapshot.urn,
                                aspectName="upstreamLineage",
                                aspect=upstream_lineage,
                            )

                            lineage_wu = MetadataWorkUnit(
                                id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                                mcp=lineage_mcpw,
                            )
                            self.report.report_workunit(lineage_wu)
                            yield lineage_wu

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lieange of view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(
                            f"{schema}.{view}", f"Ingestion error: {e}"
                        )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Views error: {e}")

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str, str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            columns = inspector.get_columns(view, schema)
        except KeyError:
            # For certain types of views, we are unable to fetch the list of columns.
            self.report.report_warning(
                dataset_name, "unable to get schema for this view"
            )
            schema_metadata = None
        else:
            # extra_tags = self.get_extra_tags(inspector, schema, view)
            extra_tags = dict()
            schema_fields = self.get_schema_fields(dataset_name, columns, tags=extra_tags)
            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                canonical_schema=schema_fields,
            )
        description, properties, _ = self.get_table_properties(inspector, schema, view)
        try:
            view_definition = inspector.get_view_definition(view, schema)
            if view_definition is None:
                view_definition = ""
            else:
                # Some dialects return a TextClause instead of a raw string,
                # so we need to convert them to a string.
                view_definition = str(view_definition)

        except NotImplementedError:
            view_definition = ""
        properties["view_definition"] = view_definition
        properties["is_view"] = "True"

        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        db_name = self.get_db_name(inspector)
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        # table_tags = self.get_extra_tags(inspector, schema, table)
        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(view)}")]
            )

            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        # Add view to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("view", dataset_urn)
        dataset_properties = DatasetPropertiesClass(
            name=view,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{view}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["view"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect
        if "view_definition" in properties:
            view_definition_string = properties["view_definition"]
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            view_properties_wu = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()
            self.report.report_workunit(view_properties_wu)
            yield view_properties_wu
        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def loop_projections(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        projections_seen: Set[str] = set()

        try:
            table_tags = self.get_extra_tags(inspector, schema, "projection")
            for projection in inspector.get_projection_names(schema):

                schema, projection = self.standardize_schema_table_names(
                    schema=schema, entity=projection
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=projection, inspector=inspector
                )
                dataset_name = self.normalise_dataset_name(dataset_name)
                if dataset_name not in projections_seen:
                    projections_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="projection")
                if not sql_config.projection_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_projections(
                        dataset_name, inspector, schema, projection, sql_config, table_tags
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{projection} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{projection}", f"Ingestion error: {e}"
                    )
                if sql_config.include_projection_lineage:

                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn,
                            aspects=[StatusClass(removed=False)],
                        )
                        lineage_info = self._get_upstream_lineage_info_Projection(dataset_urn, projection)

                        if lineage_info is not None:
                            # Emit the lineage work unit
                            
                            upstream_lineage = lineage_info
                            
                            lineage_mcpw = MetadataChangeProposalWrapper(
                                entityType="dataset",
                                changeType=ChangeTypeClass.UPSERT,
                                entityUrn=dataset_snapshot.urn,
                                aspectName="upstreamLineage",
                                aspect=upstream_lineage,
                            )
                          
                           
                            lineage_wu = MetadataWorkUnit(
                                id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                                mcp=lineage_mcpw,
                            )
                           
                            self.report.report_workunit(lineage_wu)
                            yield lineage_wu
                            
                            
                            

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lieange of Projection {schema}.{projection} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(
                            f"{schema}.{projection}", f"Ingestion error: {e}"
                        )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def loop_models(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        models_seen: Set[str] = set()
        try:
            # models_tags = self.get_extra_tags(inspector, schema, "table")

            for models in inspector.get_models_names(schema):

                schema, models = self.standardize_schema_table_names(
                    schema=schema, entity=models
                )
                dataset_name = self.get_identifier(
                    schema="Entities", entity=models, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)
                if dataset_name not in models_seen:
                    models_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="models")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_models(
                        dataset_name, inspector, schema, models, sql_config,)
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{models} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{models}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[dict]:
        columns = []
        try:
            columns = inspector.get_columns(table, schema)
            if len(columns) == 0:
                self.report.report_warning(MISSING_COLUMN_INFO, dataset_name)
        except Exception as e:
            self.report.report_warning(
                dataset_name,
                f"unable to get column information due to an error -> {e}",
            )
        return columns

    def _process_models(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # columns = self._get_columns(dataset_name, inspector, schema, table)
        columns = []
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("model", dataset_urn)
        description, properties, location_urn = self.get_model_properties(
            inspector, schema, table
        )
        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = table
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != table:
                properties["original_table_name"] = table
        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(table, schema)

        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, table)

        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
        )

        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            pk_constraints,
            foreign_keys,
            schema_fields,
        )

        dataset_snapshot.aspects.append(schema_metadata)
        db_name = self.get_db_name(inspector)

        # table_tags = self.get_extra_tags(inspector, schema, table)

        # tags_to_add = []
        # if table_tags:
        #     tags_to_add.extend(
        #         [make_tag_urn(f"{table_tags.get(table)}")]
        #     )
        #     yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["ML Models"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect
        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def loop_Oauth(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        oauth_seen: Set[str] = set()
        try:

            for OAuth in inspector.get_Oauth_names(schema):

                schema, OAuth = self.standardize_schema_table_names(
                    schema=schema, entity=OAuth
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=OAuth, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)
                if dataset_name not in oauth_seen:
                    oauth_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="OAuth")
                if not sql_config.oauth_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_Oauth(
                        dataset_name, inspector, schema, OAuth, sql_config,)
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest {schema}.{OAuth} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{OAuth}", f"Ingestion error: {e}"
                    )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def _process_Oauth(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        OAuth: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        # columns = self._get_columns(dataset_name, inspector, schema, table)
        columns = []
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("OAuth", dataset_urn)
        description, properties, location_urn = self.get_oauth_properties(
            inspector, schema, OAuth
        )
        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = OAuth
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != OAuth:
                properties["original_table_name"] = OAuth
        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(OAuth, schema)

        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, OAuth)

        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
        )
        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            pk_constraints,
            foreign_keys,
            schema_fields,
        )
        dataset_snapshot.aspects.append(schema_metadata)
        db_name = self.get_db_name(inspector)

        # table_tags = self.get_extra_tags(inspector, schema, table)

        tags_to_add = []
        # if table_tags:
        #     tags_to_add.extend(
        #         [make_tag_urn(f"{table_tags.get(table)}")]
        #     )
        #     yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["OAuth"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def _process_projections(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        projection: str,
        sql_config: SQLAlchemyConfig,
        table_tags: Dict[str, str] = dict()
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self._get_columns(dataset_name, inspector, schema, projection)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        # Add table to the checkpoint state
        self.stale_entity_removal_handler.add_entity_to_state("projection", dataset_urn)
        description, properties, location_urn = self.get_projection_properties(
            inspector, schema, projection
        )

        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = projection
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != projection:
                properties["original_table_name"] = projection

        dataset_properties = DatasetPropertiesClass(
            name=normalised_table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # extra_tags = self.get_extra_tags(inspector, schema, table)
        extra_tags = list()
        pk_constraints: dict = inspector.get_pk_constraint(projection, schema)

        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, projection)

        schema_fields = self.get_schema_fields(
            dataset_name, columns, pk_constraints, tags=extra_tags
        )
        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            pk_constraints,
            foreign_keys,
            schema_fields,
        )
        dataset_snapshot.aspects.append(schema_metadata)
        db_name = self.get_db_name(inspector)

        # table_tags = self.get_extra_tags(inspector, schema, table)

        tags_to_add = []
        if table_tags:
            tags_to_add.extend(
                [make_tag_urn(f"{table_tags.get(projection)}")]
            )
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = SqlWorkUnit(id=dataset_name, mce=mce)
        self.report.report_workunit(wu)
        yield wu
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        subtypes_aspect = MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["Projections"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        yield from self._get_domain_wu(
            dataset_name=dataset_name,
            entity_urn=dataset_urn,
            entity_type="dataset",
            sql_config=sql_config,
        )

    def _get_projection(
        self, dataset_name: str, inspector: Inspector, schema: str, projection: str
    ) -> List[dict]:
        columns = []
        try:
            columns = inspector.get_projection(projection, schema)

            if len(columns) == 0:
                self.report.report_warning(MISSING_COLUMN_INFO, dataset_name)
        except Exception as e:
            self.report.report_warning(
                dataset_name,
                f"unable to get column information due to an error -> {e}",
            )
        return columns

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[Dict[str, str]]:
        try:

            tags = inspector._get_extra_tags(table, schema)

            return tags
        except Exception as e:
            print("Exception : ", e)

    def get_projection_properties(
        self, inspector: Inspector, schema: str, projection: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            projection_info: dict = inspector.get_projection_comment(projection, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {properties}",
                pe,
            )
            projection_info: dict = inspector.get_projection_comment(properties, f'"{schema}"')  # type: ignore
        description = projection_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = projection_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = projection_info.get("properties", {})
        return description, properties, location

    def get_model_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_model_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {model}",
                pe,
            )
            table_info: dict = inspector.get_model_comment(model, f'"{schema}"')  # type: ignore
        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

    def get_oauth_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_oauth_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {model}",
                pe,
            )
            table_info: dict = inspector.get_oauth_comment(model, f'"{schema}"')  # type: ignore
        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[SqlWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            )
            wu = SqlWorkUnit(id=f"{dataset_urn}-dataPlatformInstance", mcp=mcp)
            self.report.report_workunit(wu)
            return wu
        else:
            return None

    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        sql_config: SQLAlchemyConfig,
        inspector: Inspector,
        profile_candidates: Optional[List[str]],
    ) -> bool:
        return (
            sql_config.table_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        ) and (
            sql_config.projection_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        ) and (
            profile_candidates is None
            or (profile_candidates is not None and dataset_name in profile_candidates)
        )

    def loop_profiler_requests_for_projections(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        if (
            sql_config.profiling.profile_if_updated_since_days is not None
            or sql_config.profiling.profile_table_size_limit is not None
            or sql_config.profiling.profile_table_row_limit is None
        ):
            try:
                threshold_time: Optional[datetime.datetime] = None
                if sql_config.profiling.profile_if_updated_since_days is not None:
                    threshold_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ) - datetime.timedelta(
                        sql_config.profiling.profile_if_updated_since_days
                    )
                profile_candidates = self.generate_profile_candidates(
                    inspector, threshold_time, schema
                )
            except NotImplementedError:
                logger.debug("Source does not support generating profile candidates.")
        for projection in inspector.get_projection_names(schema):

            schema, projection = self.standardize_schema_table_names(
                schema=schema, entity=projection
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=projection, inspector=inspector
            )

            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue
            dataset_name = self.normalise_dataset_name(dataset_name)
            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
                continue
            missing_column_info_warn = self.report.warnings.get(MISSING_COLUMN_INFO)
            if (
                missing_column_info_warn is not None
                and dataset_name in missing_column_info_warn
            ):
                continue
            (partition, custom_sql) = self.generate_partition_profiler_query(
                schema, projection, self.config.profiling.partition_datetime
            )
            if partition is None and self.is_table_partitioned(
                database=None, schema=schema, table=projection
            ):
                self.report.report_warning(
                    "profile skipped as partitioned table is empty or partition id was invalid",
                    dataset_name,
                )
                continue
            if (
                partition is not None
                and not self.config.profiling.partition_profiling_enabled
            ):
                logger.debug(
                    f"{dataset_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
                )
                continue
            self.report.report_entity_profiled(dataset_name)
            logger.debug(
                f"Preparing profiling request for {schema}, {projection}, {partition}"
            )

            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(
                    inspector=inspector,
                    schema=schema,
                    table=projection,
                    partition=partition,
                    custom_sql=custom_sql,
                ),
            )

    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        wu = wrap_aspect_as_workunit("dataset", dataset_urn, "globalTags", tags)
        self.report.report_workunit(wu)
        return wu

    def _get_upstream_lineage_info(
        self, dataset_urn: str, view
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:

        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self._populate_view_lineage(view)
        dataset_name = dataset_key.name
        lineage = self.view_lineage_map[dataset_name]

        if not (lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []

        for lineage_entry in lineage:
            # Update the view-lineage
            upstream_table_name = lineage_entry[0]
      
            upstream_table = UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)

        if upstream_tables:

            logger.debug(
                f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
       
            return UpstreamLineage(upstreams=upstream_tables)

        return None

    def _populate_view_lineage(self, view) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        get_refrence_table = sql.text(dedent("""
            select reference_table_name 
            from v_catalog.view_tables                                
            where table_name = '%(view)s'
        """ % {'view': view}))

        refrence_table = ""
        for data in engine.execute(get_refrence_table):
            # refrence_table.append(data)
            refrence_table = data['reference_table_name']

        view_upstream_lineage_query = sql.text(dedent("""
            select reference_table_name ,reference_table_schema
            from v_catalog.view_tables 
            where table_name = '%(view)s'
        """ % {'view': view}))

        view_downstream_query = sql.text(dedent("""
            select table_name ,table_schema
            from v_catalog.view_tables 
            where reference_table_name = '%(view)s'
        """ % {'view': refrence_table}))
        num_edges: int = 0

        try:
            self.view_lineage_map = defaultdict(list)
            for db_row_key in engine.execute(view_downstream_query):

                downstream = f"{db_row_key['table_schema']}.{db_row_key['table_name']}"

                for db_row_value in engine.execute(view_upstream_lineage_query):

                    upstream = f"{db_row_value['reference_table_schema']}.{db_row_value['reference_table_name']}"

                    view_upstream: str = upstream.lower()
                    view_name: str = downstream.lower()

                    self.view_lineage_map[view_name].append(
                        # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                        (view_upstream, "[]", "[]")
                    )

                    num_edges += 1

        except Exception as e:
            traceback.print_exc()
            self.warn(
                logger,
                "view_upstream_lineage",
                "Extracting the upstream & Downstream view lineage from vertica failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )

        logger.info(f"A total of {num_edges} View upstream edges found found for {view}")
        self.report.num_table_to_view_edges_scanned = num_edges

    def _get_upstream_lineage_info_Projection(
        self, dataset_urn: str, projection
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:

        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self._populate_projection_lineage(projection)
        dataset_name = dataset_key.name
        lineage = self.Projection_lineage_map[dataset_name]

        if not (lineage):
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []
       
        for lineage_entry in lineage:
            # Update the projection-lineage
            upstream_table_name = lineage_entry[0]

            upstream_table = UpstreamClass(
                dataset=make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
            
            
          

        if upstream_tables:

            logger.debug(
                f"lineage of Projection '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
            return UpstreamLineage(upstreams=upstream_tables)
        return None

    def _populate_projection_lineage(self, projection) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        view_upstream_lineage_query = sql.text(dedent("""
           select basename , schemaname
           from vs_projections 
           where name ='%(projection)s'
        """ % {'projection': projection}))

         
        num_edges: int = 0

        try:
            self.Projection_lineage_map = defaultdict(list)
            for db_row_key in engine.execute(view_upstream_lineage_query):
                basename = db_row_key['basename']
                upstream = f"{db_row_key['schemaname']}.{db_row_key['basename']}"

                view_downstream_query = sql.text(dedent("""
                    select name,schemaname 
                    from vs_projections 
                    where basename='%(basename)s'
                    """ % {'basename': basename}))
                for db_row_value in engine.execute(view_downstream_query):
                    downstream = f"{db_row_value['schemaname']}.{db_row_value['name']}"
                    projection_upstream: str = upstream.lower()
                    projection_name: str = downstream.lower()
                    
                  
                    self.Projection_lineage_map[projection_name].append(
                            # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                        (projection_upstream, "[]", "[]")
                    )
                       
                    num_edges += 1

        except Exception as e:
            traceback.print_exc()
            self.warn(
                logger,
                "Extracting the upstream & downstream Projection lineage from Vertica failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )

        logger.info(f"A total of {num_edges} Projection lineage edges found for {projection}.")
        self.report.num_table_to_view_edges_scanned = num_edges

    def close(self):
        self.prepare_for_commit()
