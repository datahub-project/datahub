import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
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
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.hightouch.config import (
    Constant,
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    DESTINATION_CONFIG_TABLE_KEY_MAPPING,
    HIGHTOUCH_PLATFORM,
    KNOWN_DESTINATION_PLATFORM_MAPPING,
    KNOWN_SOURCE_PLATFORM_MAPPING,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_assertion import (
    HightouchAssertionsHandler,
)
from datahub.ingestion.source.hightouch.hightouch_schema import HightouchSchemaHandler
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSchemaField,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


class WorkspaceKey(ContainerKey):
    workspace_id: str


class FolderKey(ContainerKey):
    folder_id: str
    workspace_id: str


def normalize_column_name(name: str) -> str:
    """
    Normalize column name for fuzzy matching.
    Converts to lowercase and removes underscores/hyphens.
    """
    return name.lower().replace("_", "").replace("-", "")


@platform_name("Hightouch")
@config_class(HightouchSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, showing data flow from source -> model -> sync -> destination",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default via configuration `include_column_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase):
    """
    This plugin extracts Hightouch reverse ETL metadata including sources,
    models, syncs, destinations, and sync run history.
    """

    config: HightouchSourceConfig
    report: HightouchSourceReport
    platform: str = "hightouch"

    def __init__(self, config: HightouchSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = HightouchSourceReport()
        self.api_client = HightouchAPIClient(self.config.api_config)

        self.graph: Optional[DataHubGraph] = None
        if ctx.graph:
            self.graph = ctx.graph
            logger.info(
                "DataHub graph client available - will fetch schemas from DataHub when possible"
            )
        else:
            logger.debug(
                "No DataHub graph connection - schema fetching from DataHub disabled"
            )

        self._sources_cache: Dict[str, HightouchSourceConnection] = {}
        self._models_cache: Dict[str, HightouchModel] = {}
        self._destinations_cache: Dict[str, HightouchDestination] = {}
        self._users_cache: Dict[str, HightouchUser] = {}
        self._emitted_containers: set = set()

        self._urn_builder = HightouchUrnBuilder(self)

        self._schema_handler = HightouchSchemaHandler(
            report=self.report, graph=self.graph, urn_builder=self._urn_builder
        )

        self._assertions_handler = HightouchAssertionsHandler(
            source=self,
            config=self.config,
            report=self.report,
            api_client=self.api_client,
            urn_builder=self._urn_builder,
        )

    def _get_source(self, source_id: str) -> Optional[HightouchSourceConnection]:
        if source_id not in self._sources_cache:
            self.report.report_api_call()
            source = self.api_client.get_source_by_id(source_id)
            if source:
                self._sources_cache[source_id] = source
        return self._sources_cache.get(source_id)

    def _get_model(self, model_id: str) -> Optional[HightouchModel]:
        if model_id not in self._models_cache:
            self.report.report_api_call()
            model = self.api_client.get_model_by_id(model_id)
            if model:
                self._models_cache[model_id] = model
        return self._models_cache.get(model_id)

    def _get_destination(self, destination_id: str) -> Optional[HightouchDestination]:
        if destination_id not in self._destinations_cache:
            self.report.report_api_call()
            destination = self.api_client.get_destination_by_id(destination_id)
            if destination:
                self._destinations_cache[destination_id] = destination
        return self._destinations_cache.get(destination_id)

    def _get_user(self, user_id: str) -> Optional[HightouchUser]:
        if user_id not in self._users_cache:
            self.report.report_api_call()
            user = self.api_client.get_user_by_id(user_id)
            if user:
                self._users_cache[user_id] = user
        return self._users_cache.get(user_id)

    def _get_platform_for_source(
        self, source: HightouchSourceConnection
    ) -> PlatformDetail:
        source_details = self.config.sources_to_platform_instance.get(
            source.id, PlatformDetail()
        )

        if source_details.platform is None:
            if source.type.lower() in KNOWN_SOURCE_PLATFORM_MAPPING:
                source_details.platform = KNOWN_SOURCE_PLATFORM_MAPPING[
                    source.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown source platform type",
                    message=f"Source type '{source.type}' is not in the known platform mapping. "
                    f"Using source type as platform name. Consider adding a mapping in "
                    f"sources_to_platform_instance config for source_id '{source.id}'.",
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                source_details.platform = source.type.lower()

        if source_details.env is None:
            source_details.env = self.config.env

        return source_details

    def _get_platform_for_destination(
        self, destination: HightouchDestination
    ) -> PlatformDetail:
        destination_details = self.config.destinations_to_platform_instance.get(
            destination.id, PlatformDetail()
        )

        if destination_details.platform is None:
            if destination.type.lower() in KNOWN_DESTINATION_PLATFORM_MAPPING:
                destination_details.platform = KNOWN_DESTINATION_PLATFORM_MAPPING[
                    destination.type.lower()
                ]
            else:
                self.report.info(
                    title="Unknown destination platform type",
                    message=f"Destination type '{destination.type}' is not in the known platform mapping. "
                    f"Using destination type as platform name. Consider adding a mapping in "
                    f"destinations_to_platform_instance config for destination_id '{destination.id}'.",
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                destination_details.platform = destination.type.lower()

        if destination_details.env is None:
            destination_details.env = self.config.env

        return destination_details

    def _generate_workspace_container(
        self, workspace_id: str
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = WorkspaceKey(
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        container_workunits = gen_containers(
            container_key=container_key,
            name=f"Workspace {workspace_id}",
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_WORKSPACE)],
            extra_properties={
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            self.report.workspaces_emitted += 1
            yield workunit

    def _generate_folder_container(
        self,
        folder_id: str,
        workspace_id: str,
        parent_container_key: Optional[ContainerKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = FolderKey(
            folder_id=folder_id,
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        container_workunits = gen_containers(
            container_key=container_key,
            name=f"Folder {folder_id}",
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_FOLDER)],
            parent_container_key=parent_container_key,
            extra_properties={
                "folder_id": folder_id,
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            self.report.folders_emitted += 1
            yield workunit

    def _add_entity_to_container(
        self, entity_urn: str, container_key: ContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_workunits = add_dataset_to_container(container_key, entity_urn)
        for workunit in container_workunits:
            yield workunit

    def _parse_model_sql_lineage(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
    ) -> Optional[UpstreamLineageClass]:
        if not model.raw_sql:
            return None

        self.report.sql_parsing_attempts += 1

        source_platform = self._get_platform_for_source(source)
        if not source_platform.platform:
            logger.debug(
                f"Skipping SQL parsing for model {model.id}: unknown source platform"
            )
            return None

        try:
            sql_result: SqlParsingResult = create_lineage_sql_parsed_result(
                query=model.raw_sql,
                default_db=source_platform.database,
                platform=source_platform.platform,
                platform_instance=source_platform.platform_instance,
                env=source_platform.env,
                graph=None,
                schema_aware=False,
            )

            if sql_result.debug_info.error:
                logger.debug(
                    f"Failed to parse SQL for model {model.id}: {sql_result.debug_info.error}"
                )
                self.report.sql_parsing_failures += 1
                self.report.warning(
                    title="SQL parsing failed for model",
                    message=f"Could not extract table lineage from model '{model.name}'",
                    context=f"model_id: {model.id}, error: {sql_result.debug_info.error}",
                )
                return None

            if not sql_result.in_tables:
                logger.debug(f"No upstream tables found for model {model.id}")
                self.report.sql_parsing_successes += 1
                return None

            upstreams = []
            for table_urn in sql_result.in_tables:
                upstreams.append(
                    UpstreamClass(
                        dataset=str(table_urn),
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )

            self.report.sql_parsing_successes += 1
            logger.info(
                f"Extracted {len(upstreams)} upstream tables for model {model.id}"
            )
            return UpstreamLineageClass(upstreams=upstreams)

        except Exception as e:
            logger.debug(f"Error parsing SQL for model {model.id}: {e}")
            self.report.sql_parsing_failures += 1
            self.report.warning(
                title="SQL parsing error",
                message=f"Unexpected error while parsing SQL for model '{model.name}'",
                context=f"model_id: {model.id}",
                exc=e,
            )
            return None

    def _generate_destination_dataset(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Optional[Dataset]:
        destination_urn = self._get_outlet_urn_for_sync(sync, destination)
        if not destination_urn:
            return None

        dest_details = self._get_platform_for_destination(destination)

        dataset = Dataset(
            name=sync.slug,  # Use sync slug as identifier
            platform=dest_details.platform or destination.type,
            env=dest_details.env,
            platform_instance=dest_details.platform_instance,
            display_name=sync.slug,
            description=f"Destination dataset for Hightouch sync: {sync.slug}",
        )

        if self.graph:
            try:
                logger.debug(
                    f"Sync {sync.id} ({sync.slug}): Fetching schema from DataHub for destination {destination_urn}"
                )

                schema_metadata = self.graph.get_schema_metadata(str(destination_urn))

                if schema_metadata and schema_metadata.fields:
                    schema_fields = []
                    for field in schema_metadata.fields:
                        schema_fields.append(
                            HightouchSchemaField(
                                name=field.fieldPath,
                                type=field.nativeDataType or "UNKNOWN",
                                description=field.description
                                if field.description
                                else None,
                            )
                        )

                    formatted_fields = [
                        (field.name, field.type)
                        if field.description is None
                        else (field.name, field.type, field.description)
                        for field in schema_fields
                    ]
                    dataset._set_schema(formatted_fields)

                    logger.info(
                        f"Sync {sync.id} ({sync.slug}): Fetched {len(schema_fields)} fields from DataHub for destination {destination_urn}"
                    )
                    self.report.report_destination_schema_from_datahub()
                else:
                    logger.debug(
                        f"Sync {sync.id} ({sync.slug}): No schema found in DataHub for destination {destination_urn}"
                    )

            except Exception as e:
                logger.warning(
                    f"Sync {sync.id} ({sync.slug}): Error fetching destination schema from DataHub: {e}",
                    exc_info=True,
                )

        return dataset

    def _generate_model_dataset(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> Dataset:
        custom_properties = {
            "model_id": model.id,
            "query_type": model.query_type,
            "is_schema": str(model.is_schema),
        }

        if model.primary_key:
            custom_properties["primary_key"] = model.primary_key

        if source:
            custom_properties["source_id"] = source.id
            custom_properties["source_name"] = source.name
            custom_properties["source_type"] = source.type

        if model.tags:
            for key, value in model.tags.items():
                custom_properties[f"tag_{key}"] = value

        if model.folder_id:
            custom_properties["folder_id"] = model.folder_id

        if model.raw_sql:
            sql_truncated = (
                model.raw_sql[:2000] if len(model.raw_sql) > 2000 else model.raw_sql
            )
            custom_properties["raw_sql"] = sql_truncated
            if len(model.raw_sql) > 2000:
                custom_properties["raw_sql_truncated"] = "true"
                custom_properties["raw_sql_length"] = str(len(model.raw_sql))

        platform_id = HIGHTOUCH_PLATFORM
        table_name = model.slug
        platform_instance = self.config.platform_instance
        env = self.config.env

        if self.config.emit_models_on_source_platform and source:
            source_details = self._get_platform_for_source(source)
            if source_details.platform:
                platform_id = source_details.platform
                platform_instance = source_details.platform_instance
                env = source_details.env

                if source_details.database:
                    table_name = f"{source_details.database}.{model.slug}"

                custom_properties["hightouch_model"] = "true"
                logger.debug(
                    f"Emitting model {model.slug} as sibling on platform {platform_id}"
                )

        dataset = Dataset(
            name=table_name,
            platform=platform_id,
            env=env,
            platform_instance=platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=custom_properties,
        )

        schema_fields = self._schema_handler.resolve_schema(
            model=model, source=source, referenced_columns=referenced_columns
        )

        if schema_fields:
            formatted_fields = [
                (field.name, field.type)
                if field.description is None
                else (field.name, field.type, field.description)
                for field in schema_fields
            ]
            dataset._set_schema(formatted_fields)

        if source:
            if (
                self.config.parse_model_sql
                and model.raw_sql
                and model.query_type == "raw_sql"
            ):
                upstream_lineage = self._parse_model_sql_lineage(model, source)
                if upstream_lineage:
                    dataset.set_upstreams(upstream_lineage)
                    custom_properties["sql_parsed"] = "true"
                    custom_properties["upstream_tables_count"] = str(
                        len(upstream_lineage.upstreams)
                    )
                else:
                    custom_properties["sql_parsed"] = "true"
                    custom_properties["upstream_tables_count"] = "0"
            elif model.query_type == "table" and model.name:
                table_name = model.name

                if source.configuration:
                    database = source.configuration.get("database", "")
                    schema = source.configuration.get("schema", "")

                    source_details = self._urn_builder._get_cached_source_details(
                        source
                    )

                    if source_details.include_schema_in_urn and schema:
                        table_name = f"{database}.{schema}.{table_name}"
                    elif database and "." not in table_name:
                        table_name = f"{database}.{table_name}"

                upstream_urn = self._urn_builder.make_upstream_table_urn(
                    table_name, source
                )
                dataset.set_upstreams([upstream_urn])
                custom_properties["table_lineage"] = "true"
                custom_properties["upstream_table"] = table_name

                if self.config.emit_models_on_source_platform:
                    custom_properties["sibling_of_table"] = model.name
                    custom_properties["model_type"] = "table_reference"

        dataset.set_custom_properties(custom_properties)

        return dataset

    def _generate_dataflow_from_sync(self, sync: HightouchSync) -> DataFlow:
        return DataFlow(
            platform=Constant.ORCHESTRATOR,
            name=sync.id,
            env=self.config.env,
            display_name=sync.slug,
            platform_instance=self.config.platform_instance,
        )

    def _normalize_and_match_column(
        self,
        source_field: str,
        destination_field: str,
        model_schema: Optional[List[str]] = None,
        dest_schema: Optional[List[str]] = None,
    ) -> tuple[str, str]:
        """
        Normalize and validate column names for lineage, attempting fuzzy matching.

        Returns: (validated_source_field, validated_destination_field)
        """
        if not model_schema and not dest_schema:
            return (source_field, destination_field)

        validated_source = source_field
        validated_dest = destination_field

        if model_schema:
            normalized_source = normalize_column_name(source_field)
            for schema_field in model_schema:
                if normalize_column_name(schema_field) == normalized_source:
                    validated_source = schema_field
                    logger.debug(
                        f"Fuzzy matched source field '{source_field}' to schema field '{schema_field}'"
                    )
                    break

        if dest_schema:
            normalized_dest = normalize_column_name(destination_field)
            for schema_field in dest_schema:
                if normalize_column_name(schema_field) == normalized_dest:
                    validated_dest = schema_field
                    logger.debug(
                        f"Fuzzy matched destination field '{destination_field}' to schema field '{schema_field}'"
                    )
                    break

        return (validated_source, validated_dest)

    def _get_inlet_urn_for_model(
        self, model: HightouchModel, sync: HightouchSync
    ) -> Union[str, DatasetUrn, None]:
        source = self._get_source(model.source_id)
        if not source:
            self.report.warning(
                title="Failed to get source for model",
                message="Could not retrieve source information for lineage creation.",
                context=f"sync_slug: {sync.slug} (model_id: {model.source_id})",
            )
            return None

        if not self.config.emit_models_as_datasets:
            if model.query_type == "table" and model.name:
                table_name = model.name
                if source.configuration:
                    database = source.configuration.get("database", "")
                    schema = source.configuration.get("schema", "")
                    source_details = self._urn_builder._get_cached_source_details(
                        source
                    )
                    if source_details.include_schema_in_urn and schema:
                        table_name = f"{database}.{schema}.{table_name}"
                    elif database and "." not in table_name:
                        table_name = f"{database}.{table_name}"

                return self._urn_builder.make_upstream_table_urn(table_name, source)
            else:
                logger.warning(
                    f"Sync {sync.slug}: emit_models_as_datasets=False but model {model.slug} is not a table type. "
                    f"Cannot create direct lineage without model. Model query_type: {model.query_type}"
                )
                return None

        return self._urn_builder.make_model_urn(model, source)

    def _get_outlet_urn_for_sync(
        self, sync: HightouchSync, destination: HightouchDestination
    ) -> Union[str, DatasetUrn, None]:
        dest_table = None

        if sync.configuration:
            dest_type = destination.type.lower()
            expected_key = DESTINATION_CONFIG_TABLE_KEY_MAPPING.get(dest_type)

            if expected_key and isinstance(expected_key, str):
                dest_table = sync.configuration.get(expected_key)
                if dest_table:
                    logger.debug(
                        f"Found destination table '{dest_table}' using expected key '{expected_key}' "
                        f"for {dest_type} destination"
                    )

            if not dest_table:
                fallback_keys = DESTINATION_CONFIG_TABLE_KEY_MAPPING.get(
                    "_fallback_keys", []
                )
                for key in fallback_keys:
                    dest_table = sync.configuration.get(key)
                    if dest_table:
                        logger.debug(
                            f"Found destination table '{dest_table}' using fallback key '{key}' "
                            f"for {dest_type} destination"
                        )
                        break

        if not dest_table:
            # Last resort: use sync name with a prefix to distinguish from job name
            dest_table = f"{sync.slug}_destination"
            logger.warning(
                f"Could not find destination table name in sync configuration for sync {sync.slug} (id: {sync.id}). "
                f"Destination type: {destination.type}. Using fallback name: {dest_table}"
            )

        return self._urn_builder.make_destination_urn(dest_table, destination)

    def _generate_datajob_from_sync(self, sync: HightouchSync) -> DataJob:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=Constant.ORCHESTRATOR,
            flow_id=sync.id,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        datajob = DataJob(
            name=sync.id,
            flow_urn=dataflow_urn,
            platform_instance=self.config.platform_instance,
            display_name=sync.slug,
        )

        model = self._get_model(sync.model_id)
        destination = self._get_destination(sync.destination_id)

        inlets: List[Union[str, DatasetUrn]] = []
        if model:
            inlet_urn = self._get_inlet_urn_for_model(model, sync)
            if inlet_urn:
                inlets.append(inlet_urn)

        datajob.set_inlets(inlets)

        fine_grained_lineages = []
        outlet_urn = None

        if destination:
            outlet_urn = self._get_outlet_urn_for_sync(sync, destination)

        if model and destination and outlet_urn:
            field_mappings = self.api_client.extract_field_mappings(sync)
            inlet_urn = inlets[0] if inlets else None

            if field_mappings and inlet_urn:
                model_schema_fields = None
                dest_schema_fields = None

                if self.graph:
                    try:
                        model_schema_metadata = self.graph.get_schema_metadata(
                            str(inlet_urn)
                        )
                        if model_schema_metadata and model_schema_metadata.fields:
                            model_schema_fields = [
                                f.fieldPath for f in model_schema_metadata.fields
                            ]
                    except Exception as e:
                        logger.debug(
                            f"Could not fetch model schema for column matching: {e}"
                        )

                    try:
                        dest_schema_metadata = self.graph.get_schema_metadata(
                            str(outlet_urn)
                        )
                        if dest_schema_metadata and dest_schema_metadata.fields:
                            dest_schema_fields = [
                                f.fieldPath for f in dest_schema_metadata.fields
                            ]
                    except Exception as e:
                        logger.debug(
                            f"Could not fetch destination schema for column matching: {e}"
                        )

                for mapping in field_mappings:
                    source_field, dest_field = self._normalize_and_match_column(
                        mapping.source_field,
                        mapping.destination_field,
                        model_schema_fields,
                        dest_schema_fields,
                    )

                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                make_schema_field_urn(str(inlet_urn), source_field)
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                make_schema_field_urn(str(outlet_urn), dest_field)
                            ],
                        )
                    )

                if fine_grained_lineages:
                    datajob.set_fine_grained_lineages(fine_grained_lineages)
                    self.report.column_lineage_emitted += len(fine_grained_lineages)
                    logger.debug(
                        f"Emitted {len(fine_grained_lineages)} column lineage edges for sync {sync.slug}"
                    )

        if outlet_urn:
            datajob.set_outlets([outlet_urn])

        custom_properties: Dict[str, str] = {
            "sync_id": sync.id,
            "model_id": sync.model_id,
            "destination_id": sync.destination_id,
            "disabled": str(sync.disabled),
        }

        if sync.schedule:
            custom_properties["schedule"] = str(sync.schedule)

        if model:
            custom_properties["model_name"] = model.name
            custom_properties["model_slug"] = model.slug
            custom_properties["query_type"] = model.query_type
            if model.description:
                custom_properties["model_description"] = model.description

        if destination:
            custom_properties["destination_name"] = destination.name
            custom_properties["destination_type"] = destination.type

        if sync.tags:
            # DataJob SDK doesn't directly support tags, so store as custom property
            tag_list = [
                f"{key}:{value}" for key, value in sync.tags.items() if key and value
            ]
            if tag_list:
                custom_properties["hightouch_tags"] = ", ".join(tag_list)
                self.report.tags_emitted += len(tag_list)

        datajob.set_custom_properties(custom_properties)

        return datajob

    def _generate_dpi_from_sync_run(
        self, sync_run: HightouchSyncRun, datajob: DataJob, sync_id: str
    ) -> DataProcessInstance:
        datajob_v1 = DataJobV1(
            id=datajob.name,
            flow_urn=datajob.flow_urn,
            platform_instance=self.config.platform_instance,
            name=datajob.name,
            inlets=datajob.inlets,
            outlets=datajob.outlets,
        )

        dpi = DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=sync_run.id,
            clone_inlets=True,
            clone_outlets=True,
        )

        custom_props = {
            "sync_run_id": sync_run.id,
            "sync_id": sync_id,
            "status": sync_run.status,
        }
        if sync_run.planned_rows:
            custom_props["planned_rows_added"] = str(
                sync_run.planned_rows.get("added", 0)
            )
            custom_props["planned_rows_changed"] = str(
                sync_run.planned_rows.get("changed", 0)
            )
            custom_props["planned_rows_removed"] = str(
                sync_run.planned_rows.get("removed", 0)
            )
            custom_props["planned_rows_total"] = str(
                sum(sync_run.planned_rows.values())
            )

        if sync_run.successful_rows:
            custom_props["successful_rows_added"] = str(
                sync_run.successful_rows.get("added", 0)
            )
            custom_props["successful_rows_changed"] = str(
                sync_run.successful_rows.get("changed", 0)
            )
            custom_props["successful_rows_removed"] = str(
                sync_run.successful_rows.get("removed", 0)
            )
            custom_props["successful_rows_total"] = str(
                sum(sync_run.successful_rows.values())
            )

        if sync_run.failed_rows:
            failed_total = sum(sync_run.failed_rows.values())
            custom_props["failed_rows_total"] = str(failed_total)
            custom_props["failed_rows_added"] = str(
                sync_run.failed_rows.get("added", 0)
            )
            custom_props["failed_rows_changed"] = str(
                sync_run.failed_rows.get("changed", 0)
            )
            custom_props["failed_rows_removed"] = str(
                sync_run.failed_rows.get("removed", 0)
            )

        if sync_run.query_size:
            custom_props["query_size_bytes"] = str(sync_run.query_size)

        if sync_run.completion_ratio is not None:
            custom_props["completion_ratio"] = f"{sync_run.completion_ratio * 100:.1f}%"
        if sync_run.error:
            if isinstance(sync_run.error, str):
                custom_props["error_message"] = sync_run.error
            else:
                custom_props["error_message"] = sync_run.error.get(
                    "message", "Unknown error"
                )
                if "code" in sync_run.error:
                    custom_props["error_code"] = sync_run.error["code"]

        dpi.properties.update(custom_props)

        return dpi

    def _get_dpi_workunits(
        self, sync_run: HightouchSyncRun, dpi: DataProcessInstance
    ) -> Iterable[MetadataWorkUnit]:
        status_map = {
            "success": InstanceRunResult.SUCCESS,
            "failed": InstanceRunResult.FAILURE,
            "cancelled": InstanceRunResult.SKIPPED,
            "interrupted": InstanceRunResult.SKIPPED,
            "warning": InstanceRunResult.SUCCESS,
        }

        status = status_map.get(sync_run.status.lower(), InstanceRunResult.SUCCESS)

        start_timestamp_millis = int(sync_run.started_at.timestamp() * 1000)

        for mcp in dpi.generate_mcp(
            created_ts_millis=start_timestamp_millis, materialize_iolets=False
        ):
            yield mcp.as_workunit()

        for mcp in dpi.start_event_mcp(start_timestamp_millis):
            yield mcp.as_workunit()

        end_timestamp_millis = (
            int(sync_run.finished_at.timestamp() * 1000)
            if sync_run.finished_at
            else start_timestamp_millis
        )

        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=status,
            result_type=Constant.ORCHESTRATOR,
        ):
            yield mcp.as_workunit()

    def _emit_destination_lineage(
        self,
        sync: HightouchSync,
        destination_dataset: Dataset,
        datajob: DataJob,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit upstream lineage and fine-grained lineage on the destination dataset.
        This makes the lineage visible from the destination dataset entity page.
        """
        destination_urn = str(destination_dataset.urn)

        upstreams = []
        for inlet_urn in datajob.inlets:
            upstreams.append(
                UpstreamClass(
                    dataset=str(inlet_urn),
                    type=DatasetLineageTypeClass.COPY,
                )
            )

        fine_grained_lineages = (
            datajob.fine_grained_lineages if datajob.fine_grained_lineages else None
        )

        if upstreams:
            yield MetadataChangeProposalWrapper(
                entityUrn=destination_urn,
                aspect=UpstreamLineageClass(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained_lineages,
                ),
            ).as_workunit()

            logger.debug(
                f"Emitted upstream lineage for destination {destination_urn} "
                f"with {len(upstreams)} upstreams and "
                f"{len(fine_grained_lineages) if fine_grained_lineages else 0} fine-grained lineages"
            )

    def _emit_model_aspects(
        self, model: HightouchModel, model_dataset: Dataset
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit all aspects for a model dataset (subtypes, view properties, tags).
        This ensures consistency whether the model is emitted standalone or as part of a sync.
        """
        dataset_urn = str(model_dataset.urn)

        subtypes: List[str] = [str(DatasetSubTypes.HIGHTOUCH_MODEL)]
        if model.raw_sql:
            subtypes.append(str(DatasetSubTypes.VIEW))
        elif model.query_type == "table":
            subtypes.append(str(DatasetSubTypes.TABLE))
        else:
            subtypes.append(str(DatasetSubTypes.VIEW))

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=subtypes),
        ).as_workunit()

        if model.raw_sql:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=model.raw_sql,
                ),
            ).as_workunit()

        if model.tags:
            tags_to_emit = [
                TagAssociationClass(tag=f"urn:li:tag:ht_{key}_{value}")
                for key, value in model.tags.items()
                if key and value
            ]
            if tags_to_emit:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags_to_emit),
                ).as_workunit()
                self.report.tags_emitted += len(tags_to_emit)
                logger.debug(f"Emitted {len(tags_to_emit)} tags for model {model.slug}")

    def _get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_syncs_scanned()

        model = None
        if self.config.emit_models_as_datasets:
            model = self._get_model(sync.model_id)
            if model and self.config.model_patterns.allowed(model.name):
                source = self._get_source(model.source_id)
                model_dataset = self._generate_model_dataset(
                    model, source, referenced_columns=sync.referenced_columns
                )
                self.report.report_models_emitted()
                yield model_dataset
                yield from self._emit_model_aspects(model, model_dataset)

        destination = self._get_destination(sync.destination_id)
        destination_dataset = None
        if destination:
            destination_dataset = self._generate_destination_dataset(sync, destination)
            if destination_dataset:
                self.report.report_destinations_emitted()
                yield destination_dataset

        dataflow = self._generate_dataflow_from_sync(sync)
        yield dataflow

        datajob = self._generate_datajob_from_sync(sync)
        yield datajob

        if destination_dataset and datajob.inlets:
            yield from self._emit_destination_lineage(
                sync, destination_dataset, datajob
            )

        if self.config.include_sync_runs:
            self.report.report_api_call()
            sync_runs = self.api_client.get_sync_runs(
                sync.id, limit=self.config.max_sync_runs_per_sync
            )

            for sync_run in sync_runs:
                self.report.report_sync_runs_scanned()
                dpi = self._generate_dpi_from_sync_run(sync_run, datajob, sync.id)
                yield from self._get_dpi_workunits(sync_run, dpi)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _get_model_workunits(
        self, model: HightouchModel
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_models_scanned()

        source = self._get_source(model.source_id)
        model_dataset = self._generate_model_dataset(model, source)
        self.report.report_models_emitted()
        yield model_dataset

        yield from self._emit_model_aspects(model, model_dataset)

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        logger.info("Starting Hightouch metadata extraction")

        emitted_model_ids = set()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()
        logger.info(f"Found {len(syncs)} syncs")

        filtered_syncs = [
            sync for sync in syncs if self.config.sync_patterns.allowed(sync.slug)
        ]

        logger.info(f"Processing {len(filtered_syncs)} syncs after filtering")

        for sync in filtered_syncs:
            try:
                if self.config.emit_models_as_datasets:
                    emitted_model_ids.add(sync.model_id)

                yield from self._get_sync_workunits(sync)
            except Exception as e:
                self.report.warning(
                    title="Failed to process sync",
                    message=f"An error occurred while processing sync: {str(e)}",
                    context=f"sync_slug: {sync.slug} (sync_id: {sync.id})",
                    exc=e,
                )

        if self.config.emit_models_as_datasets:
            logger.info("Fetching standalone models")
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            logger.info(f"Found {len(all_models)} models total")

            standalone_models = [
                model
                for model in all_models
                if model.id not in emitted_model_ids
                and self.config.model_patterns.allowed(model.name)
            ]

            logger.info(
                f"Processing {len(standalone_models)} standalone models after filtering"
            )

            for model in standalone_models:
                try:
                    yield from self._get_model_workunits(model)
                except Exception as e:
                    self.report.warning(
                        title="Failed to process model",
                        message=f"An error occurred while processing model: {str(e)}",
                        context=f"model_name: {model.name} (model_id: {model.id})",
                        exc=e,
                    )

        if self.config.include_contracts:
            logger.info("Fetching event contracts")
            self.report.report_api_call()
            contracts = self.api_client.get_contracts()
            logger.info(f"Found {len(contracts)} contracts")

            yield from self._assertions_handler.get_assertion_workunits(
                contracts=contracts
            )

    def get_report(self) -> SourceReport:
        return self.report
