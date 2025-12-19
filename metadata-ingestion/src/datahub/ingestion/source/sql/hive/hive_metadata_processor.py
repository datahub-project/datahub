"""
Hive Metadata Processor - Shared WorkUnit generation for HMS connectors.

This module provides the HiveMetadataProcessor class that generates DataHub
MetadataWorkUnits from Hive Metastore data. It is data-fetcher agnostic and
works with any implementation of the HiveDataFetcher Protocol.

The processor handles:
- Table and view WorkUnit generation
- Container (catalog/database/schema) generation
- Schema field extraction with complex type support
- Storage lineage emission
- View lineage integration via SqlParsingAggregator
"""

import base64
import dataclasses
import json
import logging
from collections import namedtuple
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.hive.exceptions import InvalidDatasetIdentifierError
from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit, get_schema_metadata
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_domain_wu,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_data_fetcher import HiveDataFetcher
    from datahub.ingestion.source.sql.hive.hive_metastore_source import (
        HiveMetastore,
        HiveMetastoreSourceReport,
    )
    from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
    from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

TableKey = namedtuple("TableKey", ["schema", "table"])


@dataclasses.dataclass
class ViewDataset:
    """Container for view metadata during processing."""

    dataset_name: str
    schema_name: str
    columns: List[Dict[str, Any]]
    view_definition: Optional[str] = None


class HiveMetadataProcessor:
    """
    Processes Hive Metastore data and generates DataHub WorkUnits.

    This class is data-fetcher agnostic and works with any implementation
    of the HiveDataFetcher Protocol (SQLAlchemyDataFetcher, ThriftDataFetcher).

    It extracts shared metadata processing logic from HiveMetastoreSource
    to enable composition-based design.
    """

    # Presto view markers for base64-encoded view definitions
    _PRESTO_VIEW_PREFIX = "/* Presto View: "
    _PRESTO_VIEW_SUFFIX = " */"

    def __init__(
        self,
        config: "HiveMetastore",
        fetcher: "HiveDataFetcher",
        report: "HiveMetastoreSourceReport",
        platform: str,
        aggregator: Optional["SqlParsingAggregator"] = None,
        domain_registry: Optional["DomainRegistry"] = None,
    ):
        """
        Initialize the metadata processor.

        Args:
            config: HiveMetastore configuration
            fetcher: Data fetcher implementing HiveDataFetcher Protocol
            report: Source report for tracking progress and warnings
            platform: Platform name (e.g., "hive", "presto")
            aggregator: Optional SqlParsingAggregator for view lineage
            domain_registry: Optional domain registry for domain assignment
        """
        self.config = config
        self.fetcher = fetcher
        self.report = report
        self.platform = platform
        self.aggregator = aggregator
        self.domain_registry = domain_registry

        # Initialize subtypes based on config
        self.database_container_subtype = (
            DatasetContainerSubTypes.CATALOG
            if config.use_catalog_subtype
            else DatasetContainerSubTypes.DATABASE
        )
        self.view_subtype = (
            DatasetSubTypes.VIEW.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.VIEW.lower()
        )
        self.table_subtype = (
            DatasetSubTypes.TABLE.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.TABLE.lower()
        )

        # Initialize storage lineage handler
        self.storage_lineage = HiveStorageLineage(
            config=config,
            env=config.env,
        )

    def _get_db_name(self) -> str:
        """Get the database/catalog name for URN generation."""
        if self.config.catalog_name:
            return self.config.catalog_name
        if self.config.metastore_db_name:
            return self.config.metastore_db_name
        if self.config.database:
            return self.config.database
        return "hive"

    def _get_identifier(self, schema: str, entity: str) -> str:
        """Generate dataset identifier from schema and entity names."""
        return f"{schema}.{entity}"

    def _get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        """Generate DataPlatformInstance aspect if platform_instance is configured."""
        if self.config.platform_instance:
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        return None

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        """
        Extract database and schema from a dataset identifier.

        Args:
            dataset_identifier: Fully qualified dataset name (e.g., "catalog.db.table")

        Returns:
            Tuple of (database, schema) for view lineage context

        Raises:
            InvalidDatasetIdentifierError: If identifier is invalid
        """
        if dataset_identifier is None:
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be None")
        elif not dataset_identifier.strip():
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be empty")

        parts = dataset_identifier.split(".")
        parts = [p for p in parts if p]
        if not parts:
            raise InvalidDatasetIdentifierError(
                f"Invalid dataset identifier: {dataset_identifier}"
            )

        if self.config.include_catalog_name_in_ids:
            if len(parts) >= 3:
                return parts[0], parts[1]
            elif len(parts) == 2:
                return None, parts[0]
            else:
                return None, parts[0]
        else:
            if len(parts) >= 2:
                return None, parts[0]
            else:
                return None, parts[0]

    @staticmethod
    def _get_table_key(row: Dict[str, Any]) -> TableKey:
        """Extract table key (schema, table) from a row."""
        return TableKey(schema=row["schema_name"], table=row["table_name"])

    def get_schema_fields_for_column(
        self,
        column: Dict[str, Any],
    ) -> List[SchemaField]:
        """
        Generate SchemaField(s) for a column.

        Args:
            column: Column metadata dict with col_name, col_type, col_description

        Returns:
            List of SchemaField objects (may be multiple for complex types)
        """
        return get_schema_fields_for_hive_column(
            column["col_name"],
            column["col_type"],
            description=column.get("col_description", ""),
            default_nullable=True,
        )

    def get_schema_fields(
        self, dataset_name: str, columns: List[Dict[str, Any]]
    ) -> List[SchemaField]:
        """
        Generate all SchemaFields for a table/view.

        Args:
            dataset_name: Dataset name for logging
            columns: List of column metadata dicts

        Returns:
            List of all SchemaField objects
        """
        schema_fields: List[SchemaField] = []
        for column in columns:
            fields = self.get_schema_fields_for_column(column)
            schema_fields.extend(fields)
        return schema_fields

    @staticmethod
    def _set_partition_key(
        columns: List[Dict[str, Any]], schema_fields: List[SchemaField]
    ) -> None:
        """Mark partition columns in schema fields."""
        if not columns:
            return

        partition_key_names = {
            col["col_name"] for col in columns if col.get("is_partition_col")
        }

        for schema_field in schema_fields:
            name = schema_field.fieldPath.split(".")[-1]
            if name in partition_key_names:
                schema_field.isPartitioningKey = True

    def _get_presto_view_column_metadata(
        self, view_original_text: str
    ) -> Tuple[List[Dict[str, Any]], str]:
        """Extract column metadata from base64-encoded Presto view definition."""
        encoded_view_info = view_original_text.split(self._PRESTO_VIEW_PREFIX, 1)[
            -1
        ].rsplit(self._PRESTO_VIEW_SUFFIX, 1)[0]

        decoded_view_info = base64.b64decode(encoded_view_info)
        view_definition = json.loads(decoded_view_info).get("originalSql")

        columns = json.loads(decoded_view_info).get("columns")
        for col in columns:
            col["col_name"], col["col_type"] = col["name"], col["type"]

        return list(columns), view_definition

    # =========================================================================
    # WorkUnit Generation Methods
    # =========================================================================

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Generate all WorkUnits for tables, views, and containers.

        This is the main entry point for WorkUnit generation.
        """
        db_name = self._get_db_name()

        # Generate database container
        yield from self.gen_database_containers(db_name)

        # Generate schema containers
        yield from self.gen_schema_containers(db_name)

        # Process tables (respecting include_tables config)
        if self.config.include_tables:
            yield from self.process_tables(db_name)

        # Process views (respecting include_views config)
        if self.config.include_views:
            yield from self.process_views(db_name)

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate container WorkUnits for the database/catalog level."""
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[self.database_container_subtype],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def gen_schema_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate container WorkUnits for each schema/database."""
        for row in self.fetcher.fetch_schema_rows():
            schema = row["schema"]
            if not self.config.database_pattern.allowed(schema):
                continue

            database_container_key = gen_database_key(
                database,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            schema_container_key = gen_schema_key(
                db_name=database,
                schema=schema,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_schema_container(
                database=database,
                schema=schema,
                sub_types=[DatasetContainerSubTypes.SCHEMA],
                database_container_key=database_container_key,
                schema_container_key=schema_container_key,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
                extra_properties=extra_properties,
            )

    def _add_dataset_to_container(
        self, dataset_urn: str, db_name: str, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        """Add a dataset to its parent schema container."""
        schema_container_key = gen_schema_key(
            db_name=db_name,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=schema_container_key,
        )

    def _get_table_properties(self, db_name: str) -> Dict[str, Dict[str, str]]:
        """Build a cache of table properties keyed by dataset name."""
        table_properties: Dict[str, Dict[str, str]] = {}
        for row in self.fetcher.fetch_table_properties_rows():
            dataset_name = f"{row['schema_name']}.{row['table_name']}"
            if self.config.include_catalog_name_in_ids:
                dataset_name = f"{db_name}.{dataset_name}"
            if row["PARAM_KEY"] and row["PARAM_VALUE"]:
                table_properties.setdefault(dataset_name, {})[row["PARAM_KEY"]] = row[
                    "PARAM_VALUE"
                ]
        return table_properties

    def process_tables(
        self, db_name: str
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        Process all tables and generate WorkUnits.

        Args:
            db_name: Database/catalog name for URN generation
        """
        # Build properties cache
        properties_cache = self._get_table_properties(db_name)

        # Fetch and process table rows
        table_rows = self.fetcher.fetch_table_rows()

        for key, group in groupby_unsorted(table_rows, self._get_table_key):
            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self._get_identifier(schema=schema_name, entity=key.table)

            self.report.report_entity_scanned(dataset_name, ent_type="table")

            # Apply database pattern filter
            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

            # Apply table pattern filter
            if not self.config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = list(group)
            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            dataset_urn: str = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            # Generate schema fields
            schema_fields = self.get_schema_fields(dataset_name, columns)
            self._set_partition_key(columns, schema_fields)

            schema_metadata = get_schema_metadata(
                cast(SQLSourceReport, self.report),
                dataset_name,
                self.platform,
                columns,
                None,
                None,
                schema_fields,
                self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # Build properties
            properties: Dict[str, str] = properties_cache.get(dataset_name, {})
            properties["table_type"] = str(columns[-1]["table_type"] or "")
            properties["table_location"] = str(columns[-1]["table_location"] or "")
            properties["create_date"] = str(columns[-1]["create_date"] or "")

            par_columns: str = ", ".join(
                [c["col_name"] for c in columns if c["is_partition_col"]]
            )
            if par_columns != "":
                properties["partitioned_columns"] = par_columns

            table_description = properties.get("comment")

            # Add to container
            yield from self._add_dataset_to_container(
                dataset_urn=dataset_urn, db_name=db_name, schema=key.schema
            )

            # Emit properties
            if self.config.enable_properties_merge:
                from datahub.specific.dataset import DatasetPatchBuilder

                patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                    urn=dataset_snapshot.urn
                )
                patch_builder.set_display_name(key.table)

                if table_description:
                    patch_builder.set_description(description=table_description)

                for prop, value in properties.items():
                    patch_builder.add_custom_property(key=prop, value=value)
                yield from [
                    MetadataWorkUnit(
                        id=f"{mcp_raw.entityUrn}-{DatasetPropertiesClass.ASPECT_NAME}",
                        mcp_raw=mcp_raw,
                    )
                    for mcp_raw in patch_builder.build()
                ]
            else:
                dataset_properties = DatasetPropertiesClass(
                    name=key.table,
                    description=table_description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset_name, mce=mce)

            # Emit subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[self.table_subtype]),
            ).as_workunit()

            # Emit platform instance
            dpi_aspect = self._get_dataplatform_instance_aspect(dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            # Emit domain
            if self.config.domain and self.domain_registry:
                yield from get_domain_wu(
                    dataset_name=dataset_name,
                    entity_urn=dataset_urn,
                    domain_config=self.config.domain,
                    domain_registry=self.domain_registry,
                )

            # Emit storage lineage
            if self.config.emit_storage_lineage and properties.get("table_location"):
                table_dict = {
                    "StorageDescriptor": {"Location": properties["table_location"]}
                }
                yield from self.storage_lineage.get_lineage_mcp(
                    dataset_urn=dataset_urn,
                    table=table_dict,
                    dataset_schema=schema_metadata,
                )

    def _get_hive_view_datasets(self, db_name: str) -> Iterable[ViewDataset]:
        """Iterate over Hive view datasets from view rows."""
        view_rows = self.fetcher.fetch_view_rows()

        for key, group in groupby_unsorted(view_rows, self._get_table_key):
            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self._get_identifier(schema=schema_name, entity=key.table)

            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

            columns = list(group)

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=key.schema,
                columns=columns,
                view_definition=columns[-1].get("view_expanded_text")
                if columns
                else None,
            )

    def _get_presto_view_datasets(self, db_name: str) -> Iterable[ViewDataset]:
        """
        Iterate over Presto/Trino view datasets.

        Note: This requires SQL fetcher as it needs to query VIEW_ORIGINAL_TEXT
        which contains base64-encoded view metadata.
        """
        from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
            SQLAlchemyDataFetcher,
        )

        if not isinstance(self.fetcher, SQLAlchemyDataFetcher):
            logger.warning(
                "Presto/Trino view extraction requires SQL connection. "
                "Skipping Presto views."
            )
            return

        for row in self.fetcher.fetch_presto_view_rows():
            schema_name = (
                f"{db_name}.{row['schema']}"
                if self.config.include_catalog_name_in_ids
                else row["schema"]
            )
            dataset_name = self._get_identifier(schema=schema_name, entity=row["name"])

            columns, view_definition = self._get_presto_view_column_metadata(
                row["view_original_text"]
            )

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=row["schema"],
                columns=columns,
                view_definition=view_definition,
            )

    def process_views(
        self, db_name: str
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        """
        Process all views and generate WorkUnits.

        Args:
            db_name: Database/catalog name for URN generation
        """
        from datahub.ingestion.source.sql.hive.hive_metastore_config import (
            HiveMetastoreConfigMode,
        )

        # Select view iterator based on mode
        if self.config.mode in [HiveMetastoreConfigMode.hive]:
            view_datasets = self._get_hive_view_datasets(db_name)
        else:
            view_datasets = self._get_presto_view_datasets(db_name)

        for dataset in view_datasets:
            self.report.report_entity_scanned(dataset.dataset_name, ent_type="view")

            # Apply view pattern filter
            if not self.config.view_pattern.allowed(dataset.dataset_name):
                self.report.report_dropped(dataset.dataset_name)
                continue

            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset.dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            # Generate schema fields
            schema_fields = self.get_schema_fields(
                dataset.dataset_name, dataset.columns
            )

            schema_metadata = get_schema_metadata(
                cast(SQLSourceReport, self.report),
                dataset.dataset_name,
                self.platform,
                dataset.columns,
                canonical_schema=schema_fields,
                simplify_nested_field_paths=self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            # Add properties
            properties: Dict[str, str] = {"is_view": "True"}
            dataset_properties = DatasetPropertiesClass(
                name=dataset.dataset_name.split(".")[-1],
                description=None,
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)

            # Add view properties
            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLogic=dataset.view_definition if dataset.view_definition else "",
                viewLanguage="SQL",
            )
            dataset_snapshot.aspects.append(view_properties)

            # Add to container
            yield from self._add_dataset_to_container(
                dataset_urn=dataset_urn, db_name=db_name, schema=dataset.schema_name
            )

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset.dataset_name, mce=mce)

            # Emit subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[self.view_subtype]),
            ).as_workunit()

            # Emit platform instance
            dpi_aspect = self._get_dataplatform_instance_aspect(dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            # Emit view properties again via MCP
            view_properties_aspect = ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=dataset.view_definition if dataset.view_definition else "",
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

            # Emit domain
            if self.config.domain and self.domain_registry:
                yield from get_domain_wu(
                    dataset_name=dataset.dataset_name,
                    entity_urn=dataset_urn,
                    domain_registry=self.domain_registry,
                    domain_config=self.config.domain,
                )

            # Add view lineage
            if dataset.view_definition and self.config.include_view_lineage:
                if self.aggregator is not None:
                    default_db = None
                    default_schema = None
                    try:
                        default_db, default_schema = self.get_db_schema(
                            dataset.dataset_name
                        )
                    except InvalidDatasetIdentifierError as e:
                        logger.warning(
                            f"Invalid view identifier '{dataset.dataset_name}': {e}"
                        )
                        continue

                    self.aggregator.add_view_definition(
                        view_urn=dataset_urn,
                        view_definition=dataset.view_definition,
                        default_db=default_db,
                        default_schema=default_schema,
                    )
