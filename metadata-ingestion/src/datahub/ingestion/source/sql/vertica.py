import logging
import traceback
from collections import defaultdict
from dataclasses import dataclass
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic
from pydantic.class_validators import validator
from sqlalchemy import create_engine, sql
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError

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
    oauth_scanned: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a projection or a models or Oauth .
        """

        if ent_type == "projection":
            self.projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        elif ent_type == "oauth":
            self.oauth_scanned += 1
        else:
            super().report_entity_scanned(name, ent_type)


# Extended BasicSQLAlchemyConfig to config for projections,models and oauth metadata.
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
    include_oauth: Optional[bool] = pydantic.Field(
        default=True, description="Whether Oauth should be ingested."
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
        self.config: VerticaConfig = config

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

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

            oauth_schema = "Entities"
            if sql_config.include_oauth:
                yield from self.loop_oauth(inspector, oauth_schema, sql_config)

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

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        table_owner = self._get_owner_information(table, "table")
        yield from add_owner_to_entity_wu(
            entity_type="dataset",
            entity_urn=dataset_urn,
            owner_urn=f"urn:li:corpuser:{table_owner}",
        )
        yield from super()._process_table(
            dataset_name, inspector, schema, table, sql_config
        )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
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
                    )
                except Exception as e:
                    logger.warning(
                        f"Unable to ingest view {schema}.{view} due to an exception.\n {traceback.format_exc()}"
                    )
                    self.report.report_warning(
                        f"{schema}.{view}", f"Ingestion error: {e}"
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
                            dataset_urn, view
                        )
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
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is used for performing operation and gets data for every view inside a schema

        Args:
            dataset_name (str)
            inspector (Inspector)
            schema (str): schame name
            view (str): name of the view to inspect
            sql_config (SQLAlchemyConfig)
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
        view_owner = self._get_owner_information(view, "view")
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
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        this function loop through all the projection in the given schema.
        projection is just like table is introduced by vertica db to store data
        a super projection is automatically created when a table is created

        Args:
            inspector (Inspector): inspector obj from reflection
            schema (str): schema name
            sql_config (SQLAlchemyConfig): config

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]: [description]

        Yields:
            Iterator[Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]]: [description]
        """
        projections_seen: Set[str] = set()

        try:
            # table_tags = self.get_extra_tags(inspector, schema, "projection")
            for projection in inspector.get_projection_names(schema):  # type: ignore

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
                if sql_config.include_projection_lineage:  # type: ignore

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
                        lineage_info = self._get_upstream_lineage_info_projection(
                            dataset_urn, projection
                        )

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

                    except Exception as ex:
                        logger.warning(
                            f"Unable to get lineage of Projection {schema}.{projection} due to an exception %s",
                            ex,
                        )
                        self.report.report_warning(
                            f"{schema}.{projection}", f"Ingestion error: {ex}"
                        )
        except Exception as ex:
            self.report.report_failure(f"{schema}", f"Projection error: {ex}")

    def _process_projections(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        projection: str,
        sql_config: SQLAlchemyConfig,
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

        if location_urn:
            external_upstream_table = UpstreamClass(
                dataset=location_urn,
                type=DatasetLineageTypeClass.COPY,
            )
            lineage_mcpw = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_snapshot.urn,
                aspectName="upstreamLineage",
                aspect=UpstreamLineage(upstreams=[external_upstream_table]),
            )
            lineage_wu = MetadataWorkUnit(
                id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                mcp=lineage_mcpw,
            )
            self.report.report_workunit(lineage_wu)
            yield lineage_wu

        projection_owner = self._get_owner_information(projection, "projection")
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
        schema_fields = self.get_schema_fields(dataset_name, columns, pk_constraints)
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

        if self.config.domain:
            assert self.domain_registry
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
                report=self.report,
            )

    def get_projection_properties(
        self, inspector: Inspector, schema: str, projection: str
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
            projection_info: dict = inspector.get_projection_comment(projection, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as error:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {properties} %s",
                error,
            )
            projection_info: dict = inspector.get_projection_comment(properties, f'"{schema}"')  # type: ignore
        description = projection_info.get("text")
        if isinstance(description, tuple):
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = projection_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = projection_info.get("properties", {})
        return description, properties, location

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
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        To fetch ml models related information of ml_model from vertica db
        Args:
            dataset_name (str): dataset name
            inspector (Inspector): inspector obj from reflection
            schema (str): schema name entity
            table (str): name of ml model
            sql_config (SQLAlchemyConfig)

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
        if self.config.domain:
            assert self.domain_registry
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
                report=self.report,
            )

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
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and {model} %s",
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

    def loop_oauth(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        This function is for iterating over the oauth in vertica db
        Args:
            inspector (Inspector):
            schema (str): schema name
            sql_config (SQLAlchemyConfig): configuration

        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]: [description]
        """
        oauth_seen: Set[str] = set()
        try:

            for oauth in inspector.get_Oauth_names(schema):  # type: ignore

                schema, oauth = self.standardize_schema_table_names(
                    schema=schema, entity=oauth
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=oauth, inspector=inspector
                )

                dataset_name = self.normalise_dataset_name(dataset_name)
                if dataset_name not in oauth_seen:
                    oauth_seen.add(dataset_name)
                else:
                    logger.debug("has already been seen, skipping %s", dataset_name)
                    continue
                self.report.report_entity_scanned(dataset_name, ent_type="oauth")
                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue
                try:
                    yield from self._process_oauth(
                        dataset_name,
                        inspector,
                        schema,
                        oauth,
                        sql_config,
                    )
                except Exception as error:
                    self.report.report_warning(
                        f"{schema}.{oauth}", f"Ingestion error: {error}"
                    )
        except Exception as error:
            self.report.report_failure(f"{schema}", f"oauth error: {error}")

    def _process_oauth(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        oauth: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        To fetch oauth related metadata of oauth from vertica db
        Args:
            dataset_name (str): dataset name
            inspector (Inspector): inspector object from reflection
            schema (str): schema name
            Oauth (str): oauth name
            sql_config (SQLAlchemyConfig): configuration
        Returns:
            Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]
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
        description, properties, location_urn = self.get_oauth_properties(
            inspector, schema, oauth
        )
        # Tablename might be different from the real table if we ran some normalisation ont it.
        # Getting normalized table name from the dataset_name
        # Table is the last item in the dataset name
        normalised_table = oauth
        splits = dataset_name.split(".")
        if splits:
            normalised_table = splits[-1]
            if properties and normalised_table != oauth:
                properties["original_table_name"] = oauth
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
                aspect=SubTypesClass(typeNames=["oauth"]),
            ),
        )
        self.report.report_workunit(subtypes_aspect)
        yield subtypes_aspect

        if self.config.domain:
            assert self.domain_registry
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
                report=self.report,
            )

    def get_oauth_properties(
        self, inspector: Inspector, schema: str, model: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Returns oauth related metadata information to show in properties tab
            eg. is_auth_enabled , auth_priority and all auth related info.

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

            table_info: dict = inspector.get_oauth_comment(model, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as error:
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and oauth {model} %s",
                error,
            )
            table_info: dict = inspector.get_oauth_comment(model, f'"{schema}"')  # type: ignore
        description = table_info.get("text")
        if isinstance(description, tuple):
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]
        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

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

    def _get_upstream_lineage_info(
        self, dataset_urn: str, view: str
    ) -> Optional[_Aspect]:

        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self._populate_view_lineage(view)
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

    def _populate_view_lineage(self, view: str) -> None:
        """Collects upstream and downstream lineage information for views .

        Args:
            view (str): name of the view

        """

        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)

        get_refrence_table = sql.text(
            dedent(
                """ select reference_table_name from v_catalog.view_tables where table_name = '%(view)s' """
                % {"view": view}
            )
        )

        refrence_table = ""
        for data in engine.execute(get_refrence_table):
            # refrence_table.append(data)
            refrence_table = data["reference_table_name"]

        view_upstream_lineage_query = sql.text(
            dedent(
                """
            select reference_table_name ,reference_table_schema from v_catalog.view_tables where table_name = '%(view)s' """
                % {"view": view}
            )
        )

        view_downstream_query = sql.text(
            dedent(
                """
            select table_name ,table_schema from v_catalog.view_tables where reference_table_name = '%(view)s'
        """
                % {"view": refrence_table}
            )
        )
        num_edges: int = 0

        try:
            self.view_lineage_map = defaultdict(list)
            for db_row_key in engine.execute(view_downstream_query):

                downstream = f"{db_row_key['table_schema']}.{db_row_key['table_name']}"

                for db_row_value in engine.execute(view_upstream_lineage_query):

                    upstream = f"{db_row_value['reference_table_schema']}.{db_row_value['reference_table_name']}"

                    view_upstream: str = upstream
                    view_name: str = downstream
                    self.view_lineage_map[view_name].append(
                        # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                        (view_upstream, "[]", "[]")
                    )

                    num_edges += 1

        except Exception as e:
            self.warn(
                logger,
                "view_upstream_lineage",
                "Extracting the upstream & Downstream view lineage from vertica failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )

        logger.info(
            f"A total of {num_edges} View upstream edges found found for {view}"
        )

    def _get_upstream_lineage_info_projection(
        self, dataset_urn: str, projection: str
    ) -> Optional[_Aspect]:

        dataset_key = dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        self._populate_projection_lineage(projection)
        dataset_name = dataset_key.name
        lineage = self.projection_lineage_map[dataset_name]  # type: ignore

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

    def _populate_projection_lineage(self, projection: str) -> None:
        """
        Collects upstream and downstream lineage information for views .

        Args:
            projection (str): name of the projection


        """
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        view_upstream_lineage_query = sql.text(
            dedent(
                """ select basename , schemaname from vs_projections where name ='%(projection)s'
        """
                % {"projection": projection}
            )
        )

        num_edges: int = 0

        try:
            self.projection_lineage_map = defaultdict(list)
            for db_row_key in engine.execute(view_upstream_lineage_query):
                basename = db_row_key["basename"]
                upstream = f"{db_row_key['schemaname']}.{db_row_key['basename']}"

                view_downstream_query = sql.text(
                    dedent(
                        """
                    select name,schemaname
                    from vs_projections
                    where basename='%(basename)s'
                    """
                        % {"basename": basename}
                    )
                )
                for db_row_value in engine.execute(view_downstream_query):
                    downstream = f"{db_row_value['schemaname']}.{db_row_value['name']}"
                    projection_upstream: str = upstream
                    projection_name: str = downstream
                    self.projection_lineage_map[projection_name].append(
                        # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                        (projection_upstream, "[]", "[]")
                    )
                    num_edges += 1

        except Exception as error:
            logger.warning(
                "Extracting the upstream & downstream Projection lineage from Vertica failed %s",
                error,
            )

        logger.info(
            f"A total of {num_edges} Projection lineage edges found for {projection}."
        )

    def _get_owner_information(self, table: str, label: str) -> Optional[str]:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        if label == "table":
            get_owner_query = sql.text(
                dedent(
                    """
                    SELECT owner_name
                    FROM v_catalog.tables
                    WHERE table_name = '%(table)s'
                    """
                    % {"table": table}
                )
            )

            for each in engine.execute(get_owner_query):
                return each["owner_name"]
        elif label == "view":
            get_owner_query = sql.text(
                dedent(
                    """
                    SELECT owner_name
                    FROM v_catalog.views
                    WHERE table_name =  '%(view)s'
                    """
                    % {"view": table}
                )
            )

            for each in engine.execute(get_owner_query):
                return each["owner_name"]

        elif label == "projection":
            get_owner_query = sql.text(
                dedent(
                    """
                    SELECT owner_name
                    FROM v_catalog.projections
                    WHERE projection_name = '%(projection)s'
                    """
                    % {"projection": table}
                )
            )

            for each in engine.execute(get_owner_query):
                return each["owner_name"]

        return None

    def close(self):
        self.prepare_for_commit()
