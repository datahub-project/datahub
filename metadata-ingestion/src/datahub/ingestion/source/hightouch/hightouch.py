import logging
from typing import Dict, Iterable, List, Optional, Set, Union

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
    MetadataWorkUnitProcessor,
    SourceReport,
    StructuredLogCategory,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    KNOWN_DESTINATION_PLATFORM_MAPPING,
    KNOWN_SOURCE_PLATFORM_MAPPING,
)
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.hightouch_assertion import (
    HightouchAssertionsHandler,
)
from datahub.ingestion.source.hightouch.hightouch_container import (
    HightouchContainerHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_model import HightouchModelHandler
from datahub.ingestion.source.hightouch.hightouch_schema import HightouchSchemaHandler
from datahub.ingestion.source.hightouch.hightouch_sync import HightouchSyncHandler
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchDestinationLineageInfo,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
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
    SchemaFieldClass,
)
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


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
    "Enabled by default, emitting column-level lineage from field mappings",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class HightouchSource(StatefulIngestionSourceBase):
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
        self._registered_urns: Set[str] = set()  # Track URNs with loaded schemas
        self._model_schema_fields_cache: Dict[
            str, List[SchemaFieldClass]
        ] = {}  # Cache normalized schema fields

        self._urn_builder = HightouchUrnBuilder(
            config=self.config,
            get_platform_for_source=self._get_platform_for_source,
            get_platform_for_destination=self._get_platform_for_destination,
        )
        self._sql_aggregators: Dict[str, Optional[SqlParsingAggregator]] = {}
        self._destination_lineage: Dict[str, HightouchDestinationLineageInfo] = {}

        self._container_handler = HightouchContainerHandler(config=self.config)

        self._schema_handler = HightouchSchemaHandler(
            report=self.report, graph=self.graph, urn_builder=self._urn_builder
        )

        self._lineage_handler = HightouchLineageHandler(
            api_client=self.api_client,
            report=self.report,
            urn_builder=self._urn_builder,
            graph=self.graph,
            registered_urns=self._registered_urns,
            model_schema_fields_cache=self._model_schema_fields_cache,
            destination_lineage=self._destination_lineage,
            sql_aggregators=self._sql_aggregators,
        )

        self._model_handler = HightouchModelHandler(
            config=self.config,
            report=self.report,
            urn_builder=self._urn_builder,
            schema_handler=self._schema_handler,
            lineage_handler=self._lineage_handler,
            container_handler=self._container_handler,
            get_platform_for_source=self._get_platform_for_source,
            get_aggregator_for_platform=self._get_aggregator_for_platform,
            model_schema_fields_cache=self._model_schema_fields_cache,
        )

        self._sync_handler = HightouchSyncHandler(
            config=self.config,
            report=self.report,
            api_client=self.api_client,
            urn_builder=self._urn_builder,
            lineage_handler=self._lineage_handler,
            container_handler=self._container_handler,
            model_handler=self._model_handler,
            model_schema_fields_cache=self._model_schema_fields_cache,
            get_model=self._get_model,
            get_source=self._get_source,
            get_destination=self._get_destination,
        )

        self._assertions_handler = HightouchAssertionsHandler(
            config=self.config,
            report=self.report,
            api_client=self.api_client,
            urn_builder=self._urn_builder,
            get_model=self._get_model,
            get_source=self._get_source,
        )

    def _get_aggregator_for_platform(
        self, source_platform: PlatformDetail
    ) -> Optional[SqlParsingAggregator]:
        # Returns None if platform unknown; SQL parsing is optional
        platform = source_platform.platform
        if not platform:
            logger.debug("No platform specified, skipping SQL aggregator creation")
            return None

        if platform not in self._sql_aggregators:
            try:
                logger.info(
                    f"Creating SQL parsing aggregator for platform: {platform} "
                    f"(instance: {source_platform.platform_instance}, env: {source_platform.env})"
                )

                self._sql_aggregators[platform] = SqlParsingAggregator(
                    platform=platform,
                    platform_instance=source_platform.platform_instance,
                    env=source_platform.env or self.config.env,
                    graph=self.graph,
                    eager_graph_load=False,
                    generate_lineage=True,
                    generate_queries=False,
                    generate_usage_statistics=False,
                    generate_operations=False,
                )
            except (AttributeError, TypeError, ValueError) as e:
                logger.error(
                    f"Programming error creating SQL aggregator for platform {platform}: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                logger.warning(
                    f"Failed to create SQL aggregator for platform {platform}. "
                    f"SQL parsing will be disabled for this platform, but basic lineage will still be emitted. "
                    f"Error: {e}"
                )
                self._sql_aggregators[platform] = None
                return None

        return self._sql_aggregators[platform]

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
                    f"Using source type as platform name. To fix this, add a mapping in your recipe:\n\n"
                    f"sources_to_platform_instance:\n"
                    f'  "{source.id}":\n'
                    f'    platform: "your_platform_name"  # e.g., snowflake, bigquery, postgres\n'
                    f'    platform_instance: "your_instance"  # optional\n'
                    f'    env: "PROD"  # optional',
                    context=f"source_name: {source.name} (source_id: {source.id}, type: {source.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                source_details.platform = source.type.lower()

        if source_details.env is None:
            source_details.env = self.config.env

        if source_details.database is None and source.configuration:
            source_details.database = source.configuration.get("database")

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
                    f"Using destination type as platform name. To fix this, add a mapping in your recipe:\n\n"
                    f"destinations_to_platform_instance:\n"
                    f'  "{destination.id}":\n'
                    f'    platform: "your_platform_name"  # e.g., salesforce, hubspot, postgres\n'
                    f'    platform_instance: "your_instance"  # optional\n'
                    f'    env: "PROD"  # optional',
                    context=f"destination_name: {destination.name} (destination_id: {destination.id}, type: {destination.type})",
                    log_category=StructuredLogCategory.LINEAGE,
                )
                destination_details.platform = destination.type.lower()

        if destination_details.env is None:
            destination_details.env = self.config.env

        return destination_details

    def _get_sync_workunits(
        self, sync: HightouchSync
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        yield from self._sync_handler.get_sync_workunits(sync)

    def _get_model_workunits(
        self, model: HightouchModel
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        source = self._get_source(model.source_id)
        yield from self._model_handler.get_model_workunits(model, source)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def test_connection(self) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            self.api_client.get_workspaces()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to Hightouch API: {str(e)}",
            )
            return test_report

        return test_report

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        logger.info("Starting Hightouch metadata extraction")

        yield from self._container_handler.emit_models_container()
        yield from self._container_handler.emit_syncs_container()

        emitted_model_ids = set()

        self.report.report_api_call()
        syncs = self.api_client.get_syncs()
        logger.info(f"Found {len(syncs)} syncs")

        filtered_syncs = [
            sync for sync in syncs if self.config.sync_patterns.allowed(sync.slug)
        ]

        logger.info(f"Processing {len(filtered_syncs)} syncs after filtering")

        all_models = []
        if self.config.emit_models_as_datasets:
            self.report.report_api_call()
            all_models = self.api_client.get_models()
            logger.info(f"Found {len(all_models)} models total")

        if self.graph and (filtered_syncs or all_models):
            self._schema_handler.preload_schemas_for_sql_parsing(
                models=all_models,
                syncs=filtered_syncs,
                registered_urns=self._registered_urns,
                get_source=self._get_source,
                get_model=self._get_model,
                get_destination=self._get_destination,
                get_platform_for_source=self._get_platform_for_source,
                get_aggregator_for_platform=self._get_aggregator_for_platform,
                extract_table_urns_fn=self._model_handler.extract_table_urns_from_sql,
                get_outlet_urn_for_sync=self._sync_handler.get_outlet_urn_for_sync,
            )

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

        if self.config.emit_models_as_datasets and all_models:
            logger.info("Processing standalone models")

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

        if self._destination_lineage:
            logger.info(
                f"Emitting consolidated lineage for {len(self._destination_lineage)} destination(s)"
            )
            yield from self._lineage_handler.emit_all_destination_lineage()

        if self._sql_aggregators:
            active_aggregators = {
                platform: aggregator
                for platform, aggregator in self._sql_aggregators.items()
                if aggregator is not None
            }

            if active_aggregators:
                logger.info(
                    f"Generating lineage from {len(active_aggregators)} SQL aggregator(s)"
                )
                for platform, aggregator in active_aggregators.items():
                    logger.info(f"Generating lineage from {platform} aggregator")
                    try:
                        for mcp in aggregator.gen_metadata():
                            yield mcp.as_workunit()
                    except Exception as e:
                        logger.warning(
                            f"Failed to generate metadata from {platform} aggregator: {e}",
                            exc_info=True,
                        )
                    finally:
                        try:
                            aggregator.close()
                        except Exception as e:
                            logger.debug(f"Error closing {platform} aggregator: {e}")
            else:
                logger.info(
                    "No active SQL aggregators - SQL-based lineage enrichment was not available. "
                    "Basic known lineage was still emitted."
                )
        else:
            logger.debug("No SQL aggregators created - SQL parsing was not used")

    def get_report(self) -> SourceReport:
        return self.report
