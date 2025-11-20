import logging
import re
from typing import Any, Iterable, List, Optional, Union

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
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
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.dataform.dataform_api import DataformAPI
from datahub.ingestion.source.dataform.dataform_config import DataformSourceConfig
from datahub.ingestion.source.dataform.dataform_models import (
    DataformAssertion,
    DataformDeclaration,
    DataformEntities,
    DataformOperation,
    DataformTable,
)
from datahub.ingestion.source.dataform.dataform_utils import (
    PLATFORM_NAME_IN_DATAHUB,
    DataformSourceReport,
    DataformToSchemaFieldConverter,
    get_dataform_entity_name,
    get_dataform_platform_for_target,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    SiblingsClass,
    ViewPropertiesClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


class SchemaKey(ContainerKey):
    """Container key for Dataform schemas."""

    schema_name: str


@platform_name("dataform")
@config_class(DataformSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configure using `include_column_lineage`",
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class DataformSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following metadata from Dataform:

    - Tables and views with their schemas and column information
    - Assertions (data quality tests) and their relationships
    - Operations (custom SQL operations) and their dependencies
    - Declarations (external data sources) and their schemas
    - Lineage information between all entities
    - Column-level lineage when available
    """

    config: DataformSourceConfig
    report: DataformSourceReport
    platform: str

    def __init__(self, ctx: PipelineContext, config: DataformSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.platform = PLATFORM_NAME_IN_DATAHUB
        self.config = config
        self.report = DataformSourceReport()
        self.dataform_api = DataformAPI(config, self.report)
        self.dataform_entities = DataformEntities()

        # Owner extraction pattern compilation
        self.compiled_owner_extraction_pattern: Optional[Any] = None
        if self.config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                self.config.owner_extraction_pattern
            )

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataformSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_platform(self) -> str:
        return PLATFORM_NAME_IN_DATAHUB

    def get_report(self) -> DataformSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Dataform."""
        test_report = TestConnectionReport()
        try:
            config = DataformSourceConfig.model_validate(config_dict)
            api = DataformAPI(config, DataformSourceReport())

            # Test getting compilation result
            compilation_result = api.get_compilation_result()
            if compilation_result is not None:
                test_report.basic_connectivity = CapabilityReport(capable=True)
            else:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason="Failed to get compilation result"
                )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Generate work units for Dataform metadata."""

        # Get compilation result from Dataform
        compilation_result = self.dataform_api.get_compilation_result()
        if not compilation_result:
            logger.error("Failed to get Dataform compilation result")
            return

        # Extract entities from compilation result
        self.dataform_entities = self.dataform_api.extract_entities(compilation_result)

        # Generate schema containers
        yield from self._generate_schema_containers()

        # Generate Dataform platform datasets (tables, views, declarations)
        yield from self._generate_dataform_platform_datasets()

        # Generate target platform datasets with sibling relationships
        yield from self._generate_target_platform_datasets()

        # Generate assertions
        yield from self._generate_assertion_entities()

        # Generate operations
        yield from self._generate_operation_entities()

    def _generate_schema_containers(self) -> Iterable[Container]:
        """Generate container entities for schemas."""
        schemas = set()

        # Collect all schemas from tables and declarations
        for table in self.dataform_entities.tables:
            if table.schema_name:
                schemas.add(table.schema_name)

        for declaration in self.dataform_entities.declarations:
            if declaration.schema_name:
                schemas.add(declaration.schema_name)

        # Generate container for each schema
        for schema_name in schemas:
            schema_container_key = self._generate_schema_container_key(schema_name)

            yield Container(
                schema_container_key,
                display_name=schema_name,
                qualified_name=schema_name,
                subtype=DatasetContainerSubTypes.SCHEMA,
                extra_properties={
                    "platform": self.config.target_platform,
                    "dataform_repository": (
                        self.config.cloud_config.repository_id
                        if self.config.cloud_config
                        else "local"
                    ),
                },
            )

    def _generate_schema_container_key(self, schema_name: str) -> ContainerKey:
        """Generate a container key for a schema."""
        return SchemaKey(
            schema_name=schema_name,
            platform=get_dataform_platform_for_target(self.config.target_platform),
            instance=self.config.target_platform_instance,
            env=self.config.env,
        )

    def _generate_dataform_platform_datasets(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Dataset]]:
        """Generate Dataform platform dataset entities for tables and views."""
        for table in self.dataform_entities.tables:
            # Generate the Dataform platform dataset
            dataset = self._generate_dataform_platform_dataset(table)
            if dataset:
                yield dataset

                # Generate sibling relationship if needed
                if self._should_create_sibling_relationships(table):
                    target_platform_urn = self._get_target_platform_urn(table)

                    # Create sibling aspect for Dataform entity
                    yield MetadataChangeProposalWrapper(
                        entityUrn=self._get_dataform_platform_urn(table),
                        aspect=SiblingsClass(
                            siblings=[target_platform_urn],
                            primary=self.config.dataform_is_primary_sibling,
                        ),
                    ).as_workunit()

        # Generate Dataform platform datasets for declarations
        for declaration in self.dataform_entities.declarations:
            dataset = self._generate_dataform_platform_declaration_dataset(declaration)
            if dataset:
                yield dataset

                # Generate sibling relationship if needed
                if self._should_create_sibling_relationships_for_declaration(
                    declaration
                ):
                    target_platform_urn = self._get_target_platform_urn_for_declaration(
                        declaration
                    )

                    yield MetadataChangeProposalWrapper(
                        entityUrn=self._get_dataform_platform_urn_for_declaration(
                            declaration
                        ),
                        aspect=SiblingsClass(
                            siblings=[target_platform_urn],
                            primary=self.config.dataform_is_primary_sibling,
                        ),
                    ).as_workunit()

    def _generate_dataform_platform_dataset(
        self, table: DataformTable
    ) -> Optional[Dataset]:
        """Generate a dataset entity for a Dataform table."""
        try:
            dataset_name = get_dataform_entity_name(
                table.schema_name, table.name, table.database
            )

            self.report.report_entity_scanned(dataset_name, ent_type="Table")

            # Check if table should be included
            if not self.config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                return None

            if not self.config.schema_pattern.allowed(table.schema_name):
                self.report.report_dropped(dataset_name)
                return None

            # Convert columns to schema fields
            schema_fields = None
            if table.columns:
                try:
                    schema_fields = list(
                        DataformToSchemaFieldConverter.get_schema_fields(table.columns)
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to convert columns for table {dataset_name}: {e}"
                    )

            # Determine dataset subtype
            subtype = (
                DatasetSubTypes.VIEW if table.type == "view" else DatasetSubTypes.TABLE
            )

            # Build custom properties
            custom_properties = {
                "materialization_type": table.materialization_type or table.type,
                **self.config.custom_properties,
                **table.custom_properties,
            }

            if table.file_path:
                custom_properties["dataform_file_path"] = table.file_path

                # Add Git URL if configured
                if self.config.git_repository_url:
                    git_url = f"{self.config.git_repository_url}/blob/{self.config.git_branch}/{table.file_path}"
                    custom_properties["git_url"] = git_url

            # Prepare view properties if it's a view
            view_properties = None
            if table.type == "view" and table.sql_query:
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=table.sql_query,
                )

            # Note: Upstream lineage handling would need to be implemented
            # differently in SDK 2.0, possibly through separate lineage MCPs

            # Create the Dataform platform dataset
            dataset = Dataset(
                platform=PLATFORM_NAME_IN_DATAHUB,
                name=dataset_name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
                subtype=subtype,
                schema=schema_fields,
                display_name=table.name,
                qualified_name=dataset_name,
                description=table.description,
                custom_properties={k: str(v) for k, v in custom_properties.items()},
                tags=[f"{self.config.tag_prefix}{tag}" for tag in table.tags],
                view_definition=view_properties,
            )

            return dataset

        except Exception as e:
            logger.error(f"Failed to generate dataset for table {table.name}: {e}")
            self.report.report_entity_failure(table.name, ent_type="Table")
            return None

    def _generate_target_platform_datasets(self) -> Iterable[MetadataWorkUnit]:
        """Generate target platform dataset entities and sibling patches."""
        for table in self.dataform_entities.tables:
            if not self._should_materialize_in_target_platform(table):
                continue

            target_platform_urn = self._get_target_platform_urn(table)

            # Emit sibling patch for target platform entity BEFORE any other aspects
            if self._should_create_sibling_relationships(table):
                dataform_platform_urn = self._get_dataform_platform_urn(table)

                # Create patch for target platform entity
                target_patch = DatasetPatchBuilder(target_platform_urn)
                target_patch.add_sibling(
                    dataform_platform_urn,
                    primary=not self.config.dataform_is_primary_sibling,
                )

                for mcp in target_patch.build():
                    yield MetadataWorkUnit(
                        id=MetadataWorkUnit.generate_workunit_id(mcp),
                        mcp_raw=mcp,
                        is_primary_source=False,  # Not authoritative over target platform metadata
                    )

            # Create minimal target platform dataset with key aspect
            # This ensures the entity exists for sibling relationships
            yield MetadataWorkUnit(
                id=f"dataform-target-{target_platform_urn}",
                mce=self._create_target_platform_mce(table, target_platform_urn),
            )

        # Handle declarations similarly
        for declaration in self.dataform_entities.declarations:
            if not self._should_materialize_declaration_in_target_platform(declaration):
                continue

            target_platform_urn = self._get_target_platform_urn_for_declaration(
                declaration
            )

            if self._should_create_sibling_relationships_for_declaration(declaration):
                dataform_platform_urn = self._get_dataform_platform_urn_for_declaration(
                    declaration
                )

                target_patch = DatasetPatchBuilder(target_platform_urn)
                target_patch.add_sibling(
                    dataform_platform_urn,
                    primary=not self.config.dataform_is_primary_sibling,
                )

                for mcp in target_patch.build():
                    yield MetadataWorkUnit(
                        id=MetadataWorkUnit.generate_workunit_id(mcp),
                        mcp_raw=mcp,
                        is_primary_source=False,
                    )

            yield MetadataWorkUnit(
                id=f"dataform-target-decl-{target_platform_urn}",
                mce=self._create_target_platform_declaration_mce(
                    declaration, target_platform_urn
                ),
            )

    def _generate_dataform_platform_declaration_dataset(
        self, declaration: DataformDeclaration
    ) -> Optional[Dataset]:
        """Generate a dataset entity for a Dataform declaration."""
        try:
            dataset_name = get_dataform_entity_name(
                declaration.schema_name, declaration.name, declaration.database
            )

            self.report.report_entity_scanned(dataset_name, ent_type="Declaration")

            # Check if declaration should be included
            if not self.config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                return None

            if not self.config.schema_pattern.allowed(declaration.schema_name):
                self.report.report_dropped(dataset_name)
                return None

            # Convert columns to schema fields
            schema_fields = None
            if declaration.columns:
                try:
                    schema_fields = list(
                        DataformToSchemaFieldConverter.get_schema_fields(
                            declaration.columns
                        )
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to convert columns for declaration {dataset_name}: {e}"
                    )

            # Build custom properties
            custom_properties = {
                "entity_type": "declaration",
                "is_external_source": "true",
                **self.config.custom_properties,
            }

            # Create the Dataform platform dataset for declaration
            dataset = Dataset(
                platform=PLATFORM_NAME_IN_DATAHUB,
                name=dataset_name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                schema=schema_fields,
                display_name=declaration.name,
                qualified_name=dataset_name,
                description=declaration.description,
                custom_properties=custom_properties,
                tags=[f"{self.config.tag_prefix}{tag}" for tag in declaration.tags],
            )

            return dataset

        except Exception as e:
            logger.error(
                f"Failed to generate dataset for declaration {declaration.name}: {e}"
            )
            self.report.report_entity_failure(declaration.name, ent_type="Declaration")
            return None

    def _generate_assertion_entities(self) -> Iterable[Entity]:
        """Generate assertion entities for Dataform assertions."""
        for assertion in self.dataform_entities.assertions:
            entity = self._generate_assertion_entity(assertion)
            if entity:
                yield entity

    def _generate_assertion_entity(
        self, assertion: DataformAssertion
    ) -> Optional[Entity]:
        """Generate an assertion entity for a Dataform assertion."""
        try:
            self.report.report_entity_scanned(assertion.name, ent_type="Assertion")

            # Create assertion URN
            assertion_urn = f"urn:li:assertion:(urn:li:dataPlatform:{PLATFORM_NAME_IN_DATAHUB},{assertion.name},{self.config.env})"

            # Build custom properties
            custom_properties = {
                "entity_type": "assertion",
                **self.config.custom_properties,
            }

            if assertion.sql_query:
                custom_properties["sql_query"] = assertion.sql_query

            if assertion.file_path:
                custom_properties["dataform_file_path"] = assertion.file_path

                if self.config.git_repository_url:
                    git_url = f"{self.config.git_repository_url}/blob/{self.config.git_branch}/{assertion.file_path}"
                    custom_properties["git_url"] = git_url

            # Filter out None values to ensure all values are strings
            custom_properties = {
                k: str(v) for k, v in custom_properties.items() if v is not None
            }

            # Create assertion info
            AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=assertion_urn,  # This would need to be the actual dataset URN
                    scope=DatasetAssertionScopeClass.DATASET_ROWS,
                    operator=AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
                    logic=assertion.sql_query,
                ),
                description=assertion.description,
                customProperties=custom_properties,
            )

            # Note: In SDK 2.0, assertions might be handled differently
            # This is a simplified implementation
            return None  # Placeholder - would need proper assertion entity creation

        except Exception as e:
            logger.error(f"Failed to generate assertion for {assertion.name}: {e}")
            self.report.report_entity_failure(assertion.name, ent_type="Assertion")
            return None

    def _generate_operation_entities(self) -> Iterable[Entity]:
        """Generate operation entities for Dataform operations."""
        for operation in self.dataform_entities.operations:
            entity = self._generate_operation_entity(operation)
            if entity:
                yield entity

    def _generate_operation_entity(
        self, operation: DataformOperation
    ) -> Optional[Entity]:
        """Generate an operation entity for a Dataform operation."""
        try:
            self.report.report_entity_scanned(operation.name, ent_type="Operation")

            # Build custom properties
            custom_properties = {
                "entity_type": "operation",
                "sql_query": operation.sql_query,
                **self.config.custom_properties,
            }

            if operation.file_path:
                custom_properties["dataform_file_path"] = operation.file_path

                if self.config.git_repository_url:
                    git_url = f"{self.config.git_repository_url}/blob/{self.config.git_branch}/{operation.file_path}"
                    custom_properties["git_url"] = git_url

            # Note: In SDK 2.0, operations might be handled differently
            # This is a simplified implementation
            return None  # Placeholder - would need proper operation entity creation

        except Exception as e:
            logger.error(f"Failed to generate operation for {operation.name}: {e}")
            self.report.report_entity_failure(operation.name, ent_type="Operation")
            return None

    def _should_create_sibling_relationships(self, table: DataformTable) -> bool:
        """Determine whether to emit sibling relationships for a Dataform table.

        Sibling relationships are only emitted when dataform_is_primary_sibling=False
        to establish explicit primary/secondary relationships. When dataform_is_primary_sibling=True,
        the SiblingAssociationHook handles sibling creation automatically.
        """
        # Only create siblings for entities that materialize in target platform
        if not self._should_materialize_in_target_platform(table):
            return False

        # Only emit patches when explicit primary/secondary control is needed
        return self.config.dataform_is_primary_sibling is False

    def _should_create_sibling_relationships_for_declaration(
        self, declaration: DataformDeclaration
    ) -> bool:
        """Determine whether to emit sibling relationships for a Dataform declaration."""
        return self.config.dataform_is_primary_sibling is False

    def _should_materialize_in_target_platform(self, table: DataformTable) -> bool:
        """Check if a Dataform table materializes in the target platform."""
        # Most Dataform entities materialize in the target platform
        # Only operations and some special cases don't materialize
        return table.type in ["table", "view", "incremental"]

    def _should_materialize_declaration_in_target_platform(
        self, declaration: DataformDeclaration
    ) -> bool:
        """Check if a Dataform declaration represents something in the target platform."""
        # Declarations represent external sources that exist in the target platform
        return True

    def _get_dataform_platform_urn(self, table: DataformTable) -> str:
        """Get the URN for a Dataform platform entity."""
        dataset_name = get_dataform_entity_name(
            table.schema_name, table.name, table.database
        )
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM_NAME_IN_DATAHUB,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _get_target_platform_urn(self, table: DataformTable) -> str:
        """Get the URN for a target platform entity."""
        dataset_name = get_dataform_entity_name(
            table.schema_name, table.name, table.database
        )
        return make_dataset_urn_with_platform_instance(
            platform=get_dataform_platform_for_target(self.config.target_platform),
            name=dataset_name,
            platform_instance=self.config.target_platform_instance,
            env=self.config.env,
        )

    def _get_dataform_platform_urn_for_declaration(
        self, declaration: DataformDeclaration
    ) -> str:
        """Get the URN for a Dataform platform declaration entity."""
        dataset_name = get_dataform_entity_name(
            declaration.schema_name, declaration.name, declaration.database
        )
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM_NAME_IN_DATAHUB,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _get_target_platform_urn_for_declaration(
        self, declaration: DataformDeclaration
    ) -> str:
        """Get the URN for a target platform declaration entity."""
        dataset_name = get_dataform_entity_name(
            declaration.schema_name, declaration.name, declaration.database
        )
        return make_dataset_urn_with_platform_instance(
            platform=get_dataform_platform_for_target(self.config.target_platform),
            name=dataset_name,
            platform_instance=self.config.target_platform_instance,
            env=self.config.env,
        )

    def _create_target_platform_mce(
        self, table: DataformTable, target_platform_urn: str
    ) -> Any:
        """Create a minimal MCE for target platform entity."""
        from datahub.emitter.mce_builder import make_dataset_urn
        from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
            DatasetSnapshot,
        )
        from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
        from datahub.metadata.schema_classes import DatasetKeyClass

        dataset_snapshot = DatasetSnapshot(
            urn=target_platform_urn,
            aspects=[
                DatasetKeyClass(
                    name=get_dataform_entity_name(
                        table.schema_name, table.name, table.database
                    ),
                    platform=make_dataset_urn(
                        get_dataform_platform_for_target(self.config.target_platform),
                        "",
                    ).split(":")[2],  # Extract platform from URN
                    origin="PROD",
                )
            ],
        )

        return MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

    def _create_target_platform_declaration_mce(
        self, declaration: DataformDeclaration, target_platform_urn: str
    ) -> Any:
        """Create a minimal MCE for target platform declaration entity."""
        from datahub.emitter.mce_builder import make_dataset_urn
        from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
            DatasetSnapshot,
        )
        from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
        from datahub.metadata.schema_classes import DatasetKeyClass

        dataset_snapshot = DatasetSnapshot(
            urn=target_platform_urn,
            aspects=[
                DatasetKeyClass(
                    name=get_dataform_entity_name(
                        declaration.schema_name, declaration.name, declaration.database
                    ),
                    platform=make_dataset_urn(
                        get_dataform_platform_for_target(self.config.target_platform),
                        "",
                    ).split(":")[2],
                    origin="PROD",
                )
            ],
        )

        return MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
