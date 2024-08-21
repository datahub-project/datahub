import logging
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic
from pydantic.class_validators import validator
from vertica_sqlalchemy_dialect.base import VerticaInspector

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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
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
    SubTypesClass,
    UpstreamClass,
    _Aspect,
)
from datahub.utilities import config_clean

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class VerticaSourceReport(SQLSourceReport):
    projection_scanned: int = 0
    models_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a model.
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

    include_view_lineage: bool = pydantic.Field(
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
        super().__init__(config, ctx, "vertica")
        self.report: SQLSourceReport = VerticaSourceReport()
        self.config: VerticaConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_workunits_internal()
        sql_config = self.config

        for inspector in self.get_inspectors():
            profiler = None
            profile_requests: List["GEProfilerRequest"] = []
            if sql_config.is_profiling_enabled():
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

    def get_identifier(
        self, *, schema: str, entity: str, inspector: VerticaInspector, **kwargs: Any
    ) -> str:
        regular = f"{schema}.{entity}"
        if self.config.database:
            return f"{self.config.database}.{regular}"
        current_database = self.get_db_name(inspector)
        return f"{current_database}.{regular}"

    def get_database_properties(
        self, inspector: VerticaInspector, database: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_database_properties(database)
            return custom_properties

        except Exception as ex:
            self.report.report_failure(
                f"{database}", f"unable to get extra_properties : {ex}"
            )
        return None

    def get_schema_properties(
        self, inspector: VerticaInspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        try:
            custom_properties = inspector._get_schema_properties(schema)
            return custom_properties
        except Exception as ex:
            self.report.report_failure(
                f"{database}.{schema}", f"unable to get extra_properties : {ex}"
            )
        return None

    def _process_table(
        self,
        dataset_name: str,
        inspector: VerticaInspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        table_owner = inspector.get_table_owner(table, schema)
        yield from add_owner_to_entity_wu(
            entity_type="dataset",
            entity_urn=dataset_urn,
            owner_urn=f"urn:li:corpuser:{table_owner}",
        )
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config, data_reader
        )

    def loop_views(
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            for view in inspector.get_view_names(schema):
                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )

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
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{view}", f"Ingestion error: {e}"
                    )
                if self.config.include_view_lineage:
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
                            dataset_urn,
                            inspector,
                            view,
                            schema,
                        )

                        if lineage_info is not None:
                            # upstream_column_props = []
                            yield MetadataChangeProposalWrapper(
                                entityUrn=dataset_snapshot.urn,
                                aspect=lineage_info,
                            ).as_workunit()

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lineage of view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(
                            f"{schema}.{view}", f"Ingestion error: {e}"
                        )
        except Exception as e:
            self.report.report_failure(f"{schema}", f"Views error: {e}")

    def _process_view(
        self,
        dataset_name: str,
        inspector: VerticaInspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is used for performing operation and gets data for every view inside a schema

        Args:
            dataset_name (str)
            inspector (Inspector)
            schema (str): schema name
            view (str): name of the view to inspect
            sql_config (SQLCommonConfig)
            table_tags (Dict[str, str], optional) Defaults to dict().

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]
        """

        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

        view_owner = inspector.get_view_owner(view, schema)
        yield from add_owner_to_entity_wu(
            entity_type="dataset",
            entity_urn=dataset_urn,
            owner_urn=f"urn:li:corpuser:{view_owner}",
        )

        yield from super()._process_view(
            dataset_name, inspector, schema, view, sql_config
        )

    def loop_projections(
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        this function loop through all the projection in the given schema.
        projection is just like table is introduced by vertica db to store data
        a super projection is automatically created when a table is created

        Args:
            inspector (Inspector): inspector obj from reflection
            schema (str): schema name
            sql_config (SQLCommonConfig): config

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]: [description]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]: [description]
        """
        projections_seen: Set[str] = set()

        try:
            for projection in inspector.get_projection_names(schema):
                dataset_name = self.get_identifier(
                    schema=schema, entity=projection, inspector=inspector
                )
                if dataset_name not in projections_seen:
                    projections_seen.add(dataset_name)
                else:
                    logger.debug("has already been seen, skipping... %s", dataset_name)
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="projection")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_projections(
                        dataset_name, inspector, schema, projection, sql_config
                    )
                except Exception as ex:
                    logger.warning(
                        f"Unable to ingest {schema}.{projection} due to an exception %s",
                        ex,
                    )
                    self.report.report_warning(
                        f"{schema}.{projection}", f"Ingestion error: {ex}"
                    )
                if self.config.include_projection_lineage:
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
                            dataset_urn, inspector, projection, schema
                        )

                        if lineage_info is not None:
                            yield MetadataChangeProposalWrapper(
                                entityUrn=dataset_snapshot.urn, aspect=lineage_info
                            ).as_workunit()

                    except Exception as e:
                        logger.warning(
                            f"Unable to get lineage of projection {projection} due to an exception.\n {traceback.format_exc()}"
                        )
                        self.report.report_warning(f"{schema}", f"Ingestion error: {e}")
        except Exception as ex:
            self.report.report_failure(f"{schema}", f"Projection error: {ex}")

    def _process_projections(
        self,
        dataset_name: str,
        inspector: VerticaInspector,
        schema: str,
        projection: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = inspector.get_projection_columns(projection, schema)
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
        description, properties, location_urn = self.get_projection_properties(
            inspector, schema, projection
        )

        dataset_properties = DatasetPropertiesClass(
            name=projection,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        projection_owner = inspector.get_projection_owner(projection, schema)
        yield from add_owner_to_entity_wu(
            entity_type="dataset",
            entity_urn=dataset_urn,
            owner_urn=f"urn:li:corpuser:{projection_owner}",
        )
        # extra_tags = self.get_extra_tags(inspector, schema, projection)
        pk_constraints: dict = inspector.get_pk_constraint(projection, schema)
        foreign_keys = self._get_foreign_keys(
            dataset_urn, inspector, schema, projection
        )
        schema_fields = self.get_schema_fields(
            dataset_name,
            columns,
            inspector,
            pk_constraints,
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
        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
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

    def loop_profiler_requests(
        self,
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable["GEProfilerRequest"]:
        """Function is used for collecting profiling related information for every projections
            inside an schema.

            We have extended original loop_profiler_requests and added functionality for projection
            profiling . All the logics are same only change is we have ran the loop to get projection name from inspector.get_projection_name .

        Args: schema: schema name

        """
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        yield from super().loop_profiler_requests(inspector, schema, sql_config)

        for projection in inspector.get_projection_names(schema):
            dataset_name = self.get_identifier(
                schema=schema, entity=projection, inspector=inspector
            )

            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue
            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
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
        inspector: VerticaInspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is for iterating over the ml models in vertica db

        Args:
            inspector (Inspector) : inspector obj from reflection engine
            schema (str): schema name
            sql_config (SQLCommonConfig): config

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]:
        """
        models_seen: Set[str] = set()
        try:
            for models in inspector.get_models_names(schema):
                dataset_name = self.get_identifier(
                    schema="Entities", entity=models, inspector=inspector
                )

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
                    yield from self._process_models(
                        dataset_name,
                        inspector,
                        schema,
                        models,
                        sql_config,
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

    def _process_models(
        self,
        dataset_name: str,
        inspector: VerticaInspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        To fetch ml models related information of ml_model from vertica db
        Args:
            dataset_name (str): dataset name
            inspector (Inspector): inspector obj from reflection
            schema (str): schema name entity
            table (str): name of ml model
            sql_config (SQLCommonConfig)

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]: [description]
        """
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
            inspector, schema, table
        )

        dataset_properties = DatasetPropertiesClass(
            name=table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        schema_fields = self.get_schema_fields(dataset_name, columns, inspector)

        schema_metadata = get_schema_metadata(
            self.report,
            dataset_name,
            self.platform,
            columns,
            schema_fields,  # type: ignore
        )

        dataset_snapshot.aspects.append(schema_metadata)
        db_name = self.get_db_name(inspector)

        yield from self.add_table_to_schema_container(dataset_urn, db_name, schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
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

    def get_projection_properties(
        self, inspector: VerticaInspector, schema: str, projection: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Returns projection related metadata information to show in properties tab
            eg. projection type like super, segmented etc
                partition key, segmentation and so on

        Args:
            inspector (Inspector): inspector obj from reflection engine
            schema (str): schema name
            projection (str): projection name
        Returns:
            Tuple[Optional[str], Dict[str, str], Optional[str]]: [description]
        """
        description: Optional[str] = None
        properties: Dict[str, str] = {}
        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None
        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            projection_info: dict = inspector.get_projection_comment(projection, schema)
        except NotImplementedError:
            return description, properties, location
        description = projection_info.get("text")
        if isinstance(description, tuple):
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = projection_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = projection_info.get("properties", {})
        return description, properties, location

    def get_model_properties(
        self, inspector: VerticaInspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Returns ml models related metadata information to show in properties tab
            eg. ml model attribute and ml model specification information.

        Args:
            inspector (VerticaInspector): inspector obj from reflection engine
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
            table_info: dict = inspector.get_model_comment(model, schema)
        except NotImplementedError:
            return description, properties, location
        description = table_info.get("text")

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

    def _get_upstream_lineage_info(
        self,
        dataset_urn: str,
        inspector: VerticaInspector,
        view: str,
        schema: str,
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        view_lineage_map = inspector._populate_view_lineage(view, schema)
        if dataset_key.name is not None:
            dataset_name = dataset_key.name

        else:
            # Handle the case when dataset_key.name is None
            # You can raise an exception, log a warning, or take any other appropriate action
            logger.warning("Invalid dataset name")

        lineage = view_lineage_map[dataset_name]

        if lineage is None:
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

    def _get_upstream_lineage_info_projection(
        self,
        dataset_urn: str,
        inspector: VerticaInspector,
        projection: str,
        schema: str,
    ) -> Optional[_Aspect]:
        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        projection_lineage = inspector._populate_projection_lineage(projection, schema)
        dataset_name = dataset_key.name
        lineage = projection_lineage[dataset_name]

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
