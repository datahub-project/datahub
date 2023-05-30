import logging
import traceback
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import pydantic
from pydantic.class_validators import validator
from sqlalchemy.engine.reflection import Inspector  # type: ignore
from sqlalchemy.exc import ProgrammingError  # type: ignore

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_owner_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConfig,
)
from datahub.ingestion.source.sql.sql_utils import get_domain_wu
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    ForeignKeyConstraintClass,
    SubTypesClass,
    UpstreamClass,
    _Aspect,
)
from datahub.utilities import config_clean

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
MISSING_COLUMN_INFO = "missing column information"
logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class VerticaSourceReport(SQLSourceReport):
    projection_scanned: int = 0
    models_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a models  .
        """

        if ent_type == "projection":
            self.projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        else:
            super().report_entity_scanned(name, ent_type)


# Extended BasicSQLAlchemyConfig to config for projections,models  metadata.
class VerticaConfig(BasicSQLAlchemyConfig):
    models_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for ml models to filter in ingestion. ",
    )
    include_projections: Optional[bool] = pydantic.Field(
        default=True, description="Whether projections should be ingested."
    )
    include_models: Optional[bool] = pydantic.Field(
        default=True, description="Whether Models should be ingested."
    )

    include_view_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="If the source supports it, include view lineage to the underlying storage location.",
    )
    include_projection_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="If the source supports it, include view lineage to the underlying storage location.",
    )

    # defaults
    scheme: str = pydantic.Field(default="vertica+vertica_python")

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, can be disabled via configuration `include_view_lineage` and `include_projection_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class VerticaSource(SQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext):
        # self.platform = platform
        super(VerticaSource, self).__init__(config, ctx, "vertica")
        self.report: SQLSourceReport = VerticaSourceReport()
        self.view_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self.projection_lineage_map: Optional[
            Dict[str, List[Tuple[str, str, str]]]
        ] = None
        self.tables: DefaultDict[str, List[str]] = DefaultDict(list)
        self.views: DefaultDict[str, List[str]] = DefaultDict(list)
        self.projection: DefaultDict[str, List[str]] = DefaultDict(list)
        self.config: VerticaConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_workunit_reporter(self.report, self.get_workunits_internal())

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
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
            yield from self.gen_database_containers(
                database=db_name,
                extra_properties=self.get_database_properties(
                    inspector=inspector, database=db_name
                ),
            )

            for schema in self.get_allowed_schemas(inspector, db_name):
                self.add_information_for_schema(inspector, schema)

                yield from self.gen_schema_containers(
                    schema=schema,
                    database=db_name,
                    extra_properties=self.get_schema_properties(
                        inspector=inspector, schema=schema, database=db_name
                    ),
                )

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

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )

    def get_database_properties(
        self, inspector: Inspector, database: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_database_properties(database)  # type: ignore
            return custom_properties

        except Exception as ex:
            self.report.report_failure(
                f"{database}", f"unable to get extra_properties : {ex}"
            )
        return None

    def get_schema_properties(
        self, inspector: Inspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_schema_properties(schema)  # type: ignore
            return custom_properties
        except Exception as ex:
            self.report.report_failure(
                f"{database}.{schema}", f"unable to get extra_properties : {ex}"
            )
        return None

    def loop_tables(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        try:
            self.tables[schema] = inspector.get_table_names(schema)

            # called get_columns function from dialect, it returns a list of all columns in all the table in the schema
            columns = inspector.get_all_columns(schema)  # type: ignore

            # called get_pk_constraint function from dialect , it returns a list of all columns which is primary key in all the table in the schema
            primary_key = inspector.get_pk_constraint(schema)

            description, properties, location_urn = self.get_table_properties(
                inspector, schema
            )  # called get_table_properties function from dialect , it returns a list description and properties of all table in the schema

            # called get_table_owner function from dialect , it returns a list of all owner of all table in the schema
            table_owner = inspector.get_table_owner(schema)  # type: ignore

            # loops on each table in the schema
            for table in self.tables[schema]:
                finalcolumns = []
                # loops through columns in the schema and creates all columns on current table
                for column in columns:
                    if column["tablename"] == table.lower():
                        finalcolumns.append(column)

                final_primary_key: dict = {}
                # loops through primary_key in the schema and saves the pk of current table
                for primary_key_column in primary_key:
                    if primary_key_column["tablename"] == table.lower():
                        final_primary_key = primary_key_column

                table_properties: Dict[str, str] = {}
                # loops through properties  in the schema and saves the properties of current table
                for data in properties:
                    if data["table_name"] == table.lower():  # type: ignore
                        table_properties["create_time"] = data["create_time"]  # type: ignore
                        table_properties["table_size"] = data["table_size"]  # type: ignore

                owner_name = None
                # loops through all owners in the schema and saved the value of current table owner
                for owner in table_owner:
                    if owner[0].lower() == table.lower():
                        owner_name = owner[1].lower()

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
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                normalised_table = table
                splits = dataset_name.split(".")

                if splits:
                    normalised_table = splits[-1]
                    if table_properties and normalised_table != table:
                        table_properties["original_table_name"] = table
                dataset_properties = DatasetPropertiesClass(
                    name=normalised_table,
                    description=description,
                    customProperties=table_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)

                pk_constraints: dict = final_primary_key
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: list = []

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn=dataset_urn, db_name=db_name, schema=schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                subtypes_aspect = MetadataWorkUnit(
                    id=f"{dataset_name}-subtypes",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
                    ),
                )
                self.report.report_workunit(subtypes_aspect)
                yield subtypes_aspect
                if self.config.domain:
                    assert self.domain_registry

                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=sql_config.domain,
                        domain_registry=self.domain_registry,
                    )

        except Exception as e:
            self.report.report_failure(f"{schema}", f"Tables error: {e}")

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: Optional[str] = None
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {table}",
                pe,
            )
            table_info: dict = inspector.get_table_comment(table, f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

    def loop_views(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        views_seen: Set[str] = set()

        try:
            self.views[schema] = inspector.get_view_names(schema)

            # called get_view_columns function from dialect , it returns a list of all columns in all the view in the schema
            columns = inspector.get_all_view_columns(schema)  # type: ignore

            description, properties, location_urn = self.get_view_properties(
                inspector, schema
            )  # called get_view_properties function from dialect , it returns a list description and properties of all view in the schema

            # called get_view_owner function from dialect , it returns a list of all owner of all view in the schema
            view_owner = inspector.get_view_owner(
                schema
            )  # type: ignore  # called get_view_owner function from dialect , it returns a list of all owner of all view in the schema

            # started a loop on each view in the schema
            for view in self.views[schema]:
                finalcolumns = []
                # loops through columns in the schema and creates all columns on current view
                for column in columns:
                    if column["tablename"] == view.lower():
                        finalcolumns.append(column)

                view_properties = {}
                # loops through properties  in the schema and saves the properties of current views
                for data in properties:
                    if data["table_name"] == view.lower():  # type: ignore
                        view_properties["create_time"] = data["create_time"]  # type: ignore

                owner_name = None
                # loops through all views in the schema and returns the owner name of current view
                for owner in view_owner:
                    if owner[0].lower() == view.lower():
                        owner_name = owner[1].lower()

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
                view_properties["view_definition"] = view_definition
                view_properties["is_view"] = "True"

                schema, view = self.standardize_schema_table_names(
                    schema=schema, entity=view
                )

                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)

                if dataset_name not in views_seen:
                    views_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )

                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                normalised_table = view

                splits = dataset_name.split(".")

                if splits:
                    normalised_table = splits[-1]
                    if view_properties and normalised_table != view:
                        view_properties["original_table_name"] = view

                dataset_properties = DatasetPropertiesClass(
                    name=normalised_table,
                    description=description,
                    customProperties=view_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)
                pk_constraints: dict = {}
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: Optional[List[ForeignKeyConstraintClass]] = None

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)

                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn=dataset_urn, db_name=db_name, schema=schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                subtypes_aspect = MetadataWorkUnit(
                    id=f"{dataset_name}-subtypes",
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
                    ),
                )
                self.report.report_workunit(subtypes_aspect)
                yield subtypes_aspect
                if self.config.domain:
                    assert self.domain_registry
                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=sql_config.domain,
                        domain_registry=self.domain_registry,
                    )

                if sql_config.include_view_lineage:  # type: ignore
                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn, aspects=[StatusClass(removed=False)]
                        )

                        lineage_info = self._get_upstream_lineage_info(
                            dataset_urn, schema, inspector
                        )

                        if lineage_info is not None:
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
                            f"Unable to get lineage of view {schema} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(f"{schema}", f"Ingestion error: {e}")

        except Exception as e:
            print(traceback.format_exc())
            self.report.report_failure(f"{schema}", f"Views error: {e}")

    def get_view_properties(
        self, inspector: Inspector, schema: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_view_comment(schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} ",
                pe,
            )
            table_info: dict = inspector.get_view_comment(f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})

        return description, properties, location

    def _get_upstream_lineage_info(
        self, dataset_urn: str, schema: str, inspector
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self.view_lineage_map = inspector._populate_view_lineage(schema)
        dataset_name = dataset_key.name
        lineage = self.view_lineage_map[dataset_name]  # type: ignore

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
                f" lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )

            return UpstreamLineage(upstreams=upstream_tables)

        return None

    def loop_projections(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        projection_seen: Set[str] = set()
        try:
            self.projection[schema] = inspector.get_projection_names(schema)

            # called get_view_columns function from dialect , it returns a list of all columns in all the view in the schema
            columns = inspector.get_all_projection_columns(schema)  # type: ignore

            # called get_pk_constraint function from dialect , it returns a list of all columns which is primary key in all the table in the schema
            # primary_key = inspector.get_pk_constraint(schema)

            description, properties, location_urn = self.get_projection_properties(
                inspector, schema
            )  # called get_view_properties function from dialect , it returns a list description and properties of all view in the schema

            # called get_view_owner function from dialect , it returns a list of all owner of all view in the schema
            projection_owner = inspector.get_projection_owner(schema)  # type: ignore

            # started a loop on each view in the schema
            for projection in self.projection[schema]:
                finalcolumns = []
                # loops through all the columns in the schema and find all the columns of current projection
                for column in columns:
                    if column["tablename"] == projection.lower():
                        finalcolumns.append(column)

                projection_properties = {}
                # loops through all the properties in current schema and find all the properties of current projection
                for projection_Comment in properties:
                    if projection_Comment["projection_name"] == projection.lower():  # type: ignore
                        if "ROS_Count" in projection_Comment:
                            projection_properties["Ros count"] = str(
                                projection_Comment["ROS_Count"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Ros count"] = "Not Available"

                        if "Projection_Type" in projection_Comment:
                            projection_properties["Projection Type"] = str(
                                projection_Comment["Projection_Type"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Projection Type"] = "Not Available"

                        if "is_segmented" in projection_Comment:
                            projection_properties["is_segmented"] = str(
                                projection_Comment["is_segmented"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["is_segmented"] = "Not Available"

                        if "Segmentation_key" in projection_Comment:
                            projection_properties["Segmentation_key"] = str(
                                projection_Comment["Segmentation_key"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Segmentation_key"] = "Not Available"

                        if "Partition_Key" in projection_Comment:
                            projection_properties["Partition_Key"] = str(
                                projection_Comment["Partition_Key"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Partition_Key"] = "Not Available"

                        if "Partition_Size" in projection_Comment:
                            projection_properties["Partition Size"] = str(
                                projection_Comment["Partition_Size"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Partition Size"] = "0"

                        if "projection_size" in projection_Comment:
                            projection_properties["Projection Size"] = str(
                                projection_Comment["projection_size"]  # type: ignore
                            )
                        else:
                            # Handle the case when the key is not present
                            projection_properties["Projection Size"] = "0 KB"

                        if "Projection_Cached" in projection_Comment:
                            projection_properties["Projection Cached"] = str(
                                projection_Comment["Projection_Cached"]  # type: ignore
                            )
                        else:
                            projection_properties["Projection Cached"] = "False"

                owner_name = None
                # loops through all owners in the schema and saved the value of current projection owner
                for owner in projection_owner:
                    if owner[0].lower() == projection.lower():
                        owner_name = owner[1].lower()

                schema, projection = self.standardize_schema_table_names(
                    schema=schema, entity=projection
                )

                dataset_name = self.get_identifier(
                    schema=schema, entity=projection, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)

                if dataset_name not in projection_seen:
                    projection_seen.add(dataset_name)
                else:
                    logger.debug(f"{dataset_name} has already been seen, skipping...")
                    continue

                self.report.report_entity_scanned(dataset_name, ent_type="projection")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                dataset_urn = make_dataset_urn_with_platform_instance(
                    self.platform,
                    dataset_name,
                    self.config.platform_instance,
                    self.config.env,
                )

                yield from add_owner_to_entity_wu(
                    entity_type="dataset",
                    entity_urn=dataset_urn,
                    owner_urn=f"urn:li:corpuser:{owner_name}",
                )
                dataset_snapshot = DatasetSnapshot(
                    urn=dataset_urn,
                    aspects=[StatusClass(removed=False)],
                )

                normalised_table = projection
                splits = dataset_name.split(".")

                if splits:
                    normalised_table = splits[-1]
                    if projection_properties and normalised_table != projection:
                        projection_properties["original_table_name"] = projection
                dataset_properties = DatasetPropertiesClass(
                    name=normalised_table,
                    description=description,
                    customProperties=projection_properties,
                )

                dataset_snapshot.aspects.append(dataset_properties)

                pk_constraints: dict = {}
                extra_tags: Optional[Dict[str, List[str]]] = None
                foreign_keys: Optional[List[ForeignKeyConstraintClass]] = None

                schema_fields = self.get_schema_fields(
                    dataset_name, finalcolumns, pk_constraints, tags=extra_tags
                )

                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    finalcolumns,
                    pk_constraints,
                    foreign_keys,
                    schema_fields,
                )
                dataset_snapshot.aspects.append(schema_metadata)
                db_name = self.get_db_name(inspector)
                yield from self.add_table_to_schema_container(
                    dataset_urn, db_name, schema
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                yield SqlWorkUnit(id=dataset_name, mce=mce)
                dpi_aspect = self.get_dataplatform_instance_aspect(
                    dataset_urn=dataset_urn
                )
                if dpi_aspect:
                    yield dpi_aspect
                yield MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="subTypes",
                    aspect=SubTypesClass(typeNames=["Projections"]),
                ).as_workunit()

                if self.config.domain:
                    assert self.domain_registry
                    yield from get_domain_wu(
                        dataset_name=dataset_name,
                        entity_urn=dataset_urn,
                        domain_config=self.config.domain,
                        domain_registry=self.domain_registry,
                    )

                if sql_config.include_projection_lineage:  # type: ignore
                    try:
                        dataset_urn = make_dataset_urn_with_platform_instance(
                            self.platform,
                            dataset_name,
                            self.config.platform_instance,
                            self.config.env,
                        )

                        dataset_snapshot = DatasetSnapshot(
                            urn=dataset_urn, aspects=[StatusClass(removed=False)]
                        )

                        lineage_info = self._get_upstream_lineage_info_projection(
                            dataset_urn, schema, inspector
                        )

                        if lineage_info is not None:
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
                            f"Unable to get lineage of projection {schema} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(f"{schema}", f"Ingestion error: {e}")

        except Exception as e:
            print(traceback.format_exc())
            self.report.report_failure(f"{schema}", f"Projections error: {e}")

    def get_projection_properties(
        self, inspector: Inspector, schema: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_projection_comment(schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} ",
                pe,
            )
            table_info: dict = inspector.get_projection_comment(f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})

        return description, properties, location

    def _get_upstream_lineage_info_projection(
        self, dataset_urn: str, schema: str, inspector
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self.projection_lineage_map = inspector._populate_projection_lineage(schema)  # type: ignore
        dataset_name = dataset_key.name
        lineage = self.projection_lineage_map[dataset_name]  # type: ignore

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
                f" lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )

            return UpstreamLineage(upstreams=upstream_tables)

        return None

    def loop_profiler_requests(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        """Function is used for collecting profiling related information for every projections
            inside an schema.

        Args: schema: schema name

        """
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        yield from super().loop_profiler_requests(inspector, schema, sql_config)
        for projection in inspector.get_projection_names(schema):  # type: ignore
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

    def loop_models(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is for iterating over the ml models in vertica db

        Args:
            inspector (Inspector) : inspector obj from reflection engine
            schema (str): schema name
            sql_config (SQLAlchemyConfig): config

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]:
        """
        models_seen: Set[str] = set()
        try:
            for models in inspector.get_models_names(schema):  # type: ignore
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
                    logger.debug("has already been seen, skipping... %s", dataset_name)
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="models")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    columns: List[Dict[Any, Any]] = []
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
                    description, properties, location = self.get_model_properties(
                        inspector, schema, models
                    )
                    # Tablename might be different from the real table if we ran some normalisation ont it.
                    # Getting normalized table name from the dataset_name
                    # Table is the last item in the dataset name

                    normalised_table = models
                    splits = dataset_name.split(".")
                    if splits:
                        normalised_table = splits[-1]
                        if properties and normalised_table != models:
                            properties["original_table_name"] = models
                    dataset_properties = DatasetPropertiesClass(
                        name=normalised_table,
                        description=description,
                        customProperties=properties,
                    )

                    dataset_snapshot.aspects.append(dataset_properties)

                    schema_fields = self.get_schema_fields(dataset_name, columns)

                    schema_metadata = get_schema_metadata(
                        self.report,
                        dataset_name,
                        self.platform,
                        columns,
                        schema_fields,  # type: ignore
                    )

                    dataset_snapshot.aspects.append(schema_metadata)
                    db_name = self.get_db_name(inspector)

                    yield from self.add_table_to_schema_container(
                        dataset_urn, db_name, schema
                    )
                    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                    yield SqlWorkUnit(id=dataset_name, mce=mce)
                    dpi_aspect = self.get_dataplatform_instance_aspect(
                        dataset_urn=dataset_urn
                    )
                    if dpi_aspect:
                        yield dpi_aspect
                    yield MetadataChangeProposalWrapper(
                        entityType="dataset",
                        changeType=ChangeTypeClass.UPSERT,
                        entityUrn=dataset_urn,
                        aspectName="subTypes",
                        aspect=SubTypesClass(typeNames=["ML Models"]),
                    ).as_workunit()
                    if self.config.domain:
                        assert self.domain_registry
                        yield from get_domain_wu(
                            dataset_name=dataset_name,
                            entity_urn=dataset_urn,
                            domain_config=self.config.domain,
                            domain_registry=self.domain_registry,
                        )
                except Exception as error:
                    logger.warning(
                        f"Unable to ingest {schema}.{models} due to an exception. %s {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{models}", f"Ingestion error: {error}"
                    )
        except Exception as error:
            self.report.report_failure(f"{schema}", f"Model error: {error}")

    def get_model_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Returns ml models related metadata information to show in properties tab
            eg. ml model attribute and ml model specification information.

        Args:
            inspector (Inspector): inspector obj from reflection engine
            schema (str): schema name
            model (str): ml model name
        Returns:
            Tuple[Optional[str], Dict[str, str], Optional[str]]
        """
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            table_info: dict = inspector.get_model_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as error:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema}  %s",
                error,
            )
            table_info: dict = inspector.get_model_comment(model, f'"{schema}"')  # type: ignore
        description = table_info.get("text")
        if type(description) is tuple:
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location
