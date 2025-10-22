"""
SAP HANA Schema Generator for DataHub metadata generation.

This module generates DataHub metadata work units for SAP HANA database objects
following the same patterns as the Snowflake connector.
"""

import logging
from typing import Iterable, Optional, Union

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaDataset,
    HanaTable,
    HanaView,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TimeStampClass,
    TimeTypeClass,
    ViewPropertiesClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

# SAP HANA data type mapping
HANA_TYPES_MAP = {
    "BOOLEAN": BooleanTypeClass,
    "TINYINT": NumberTypeClass,
    "SMALLINT": NumberTypeClass,
    "INTEGER": NumberTypeClass,
    "BIGINT": NumberTypeClass,
    "SMALLDECIMAL": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "REAL": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "VARCHAR": StringTypeClass,
    "NVARCHAR": StringTypeClass,
    "ALPHANUM": StringTypeClass,
    "SHORTTEXT": StringTypeClass,
    "VARBINARY": BytesTypeClass,
    "BLOB": BytesTypeClass,
    "CLOB": StringTypeClass,
    "NCLOB": StringTypeClass,
    "TEXT": StringTypeClass,
    "ARRAY": ArrayTypeClass,
    "ST_GEOMETRY": RecordTypeClass,
    "ST_POINT": RecordTypeClass,
    "DATE": DateTypeClass,
    "TIME": TimeTypeClass,
    "SECONDDATE": TimeTypeClass,
    "TIMESTAMP": TimeTypeClass,
}


class HanaSchemaGenerator:
    """Generates DataHub metadata work units for SAP HANA objects."""

    def __init__(
        self,
        config: HanaConfig,
        report: SQLSourceReport,
        data_dictionary: HanaDataDictionary,
        identifiers: HanaIdentifierBuilder,
        domain_registry: Optional[DomainRegistry] = None,
    ):
        """Initialize the schema generator.

        Args:
            config: HANA source configuration
            report: Report object for tracking statistics
            data_dictionary: Data dictionary for metadata extraction
            identifiers: Identifier builder for URN generation
            domain_registry: Optional domain registry for domain assignment
        """
        self.config = config
        self.report = report
        self.data_dictionary = data_dictionary
        self.identifiers = identifiers
        self.domain_registry = domain_registry
        self.calculation_view_parser = SAPCalculationViewParser()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate work units for all HANA objects."""
        # Get databases
        databases = self.data_dictionary.get_databases()

        for database in databases:
            if not self.config.database_pattern.allowed(database.name):
                self.report.report_dropped(f"{database.name}.*")
                continue

            # Process schemas in the database
            schemas = self.data_dictionary.get_schemas_for_database(database.name)

            for schema in schemas:
                if not self.config.schema_pattern.allowed(schema.name):
                    self.report.report_dropped(f"{database.name}.{schema.name}.*")
                    continue

                # Handle special _sys_bic schema for calculation views
                if schema.name.lower() == "_sys_bic":
                    yield from self._process_calculation_views()
                else:
                    # Process regular tables and views
                    yield from self._process_schema(database.name, schema.name)

    def _process_schema(
        self, db_name: str, schema_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process tables and views in a regular schema."""
        # Process tables
        tables = self.data_dictionary.get_tables_for_schema(schema_name, db_name)
        for table in tables:
            if self.config.table_pattern.allowed(
                f"{db_name}.{schema_name}.{table.name}"
            ):
                # Get columns for the table
                table.columns = self.data_dictionary.get_columns_for_table(
                    table.name, schema_name, db_name
                )
                yield from self._process_table(table, schema_name, db_name)
                self.report.report_entity_scanned(table.name, "table")

        # Process views
        views = self.data_dictionary.get_views_for_schema(schema_name, db_name)
        for view in views:
            if self.config.view_pattern.allowed(f"{db_name}.{schema_name}.{view.name}"):
                # Get columns for the view
                view.columns = self.data_dictionary.get_columns_for_table(
                    view.name, schema_name, db_name
                )
                yield from self._process_view(view, schema_name, db_name)
                self.report.report_entity_scanned(view.name, "view")

    def _process_calculation_views(self) -> Iterable[MetadataWorkUnit]:
        """Process SAP HANA calculation views."""
        calc_views = self.data_dictionary.get_calculation_views()

        for calc_view in calc_views:
            # Get columns for the calculation view
            calc_view.columns = self.data_dictionary.get_columns_for_calculation_view(
                calc_view.full_name
            )
            yield from self._process_calculation_view(calc_view)
            self.report.report_entity_scanned(calc_view.name, "view")

    def _process_table(
        self, table: HanaTable, schema_name: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single table and generate work units."""
        yield from self._gen_dataset_workunits(table, schema_name, db_name)

    def _process_view(
        self, view: HanaView, schema_name: str, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single view and generate work units."""
        yield from self._gen_dataset_workunits(view, schema_name, db_name)

    def _process_calculation_view(
        self, calc_view: HanaCalculationView
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single calculation view and generate work units."""
        yield from self._gen_dataset_workunits(calc_view, "_sys_bic", None)

    def _gen_dataset_workunits(
        self, dataset: HanaDataset, schema_name: str, db_name: Optional[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate work units for a dataset (table, view, or calculation view)."""
        # Generate dataset URN
        if isinstance(dataset, HanaCalculationView):
            dataset_name = f"_sys_bic.{dataset.full_name.lower()}"
        else:
            dataset_name = self.identifiers.get_dataset_identifier(
                dataset.name, schema_name, db_name or ""
            )

        dataset_urn = self.identifiers.gen_dataset_urn(dataset_name)

        # Status aspect
        status = StatusClass(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=status
        ).as_workunit()

        # Schema metadata aspect
        schema_metadata = self._gen_schema_metadata(dataset, schema_name, db_name)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

        # Dataset properties aspect
        dataset_properties = self._gen_dataset_properties(dataset, schema_name, db_name)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

        # Subtype aspect
        subtype = SubTypesClass(typeNames=[dataset.get_subtype()])
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subtype
        ).as_workunit()

        # View properties for views and calculation views
        if isinstance(dataset, (HanaView, HanaCalculationView)):
            view_properties = self._gen_view_properties(dataset)
            if view_properties:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=view_properties
                ).as_workunit()

    def _gen_schema_metadata(
        self, dataset: HanaDataset, schema_name: str, db_name: Optional[str]
    ) -> SchemaMetadataClass:
        """Generate schema metadata for a dataset."""
        fields = []

        for column in dataset.columns:
            # Map HANA data type to DataHub type
            datahub_type_class = HANA_TYPES_MAP.get(column.data_type, StringTypeClass)

            field = SchemaFieldClass(
                fieldPath=column.name,
                type=SchemaFieldDataTypeClass(type=datahub_type_class()),
                nativeDataType=column.get_precise_native_type(),
                description=column.comment or "",
                nullable=column.nullable,
            )
            fields.append(field)

        # Generate schema name
        if isinstance(dataset, HanaCalculationView):
            schema_name_full = f"_sys_bic.{dataset.name}"
        else:
            schema_name_full = f"{schema_name}.{dataset.name}"

        return SchemaMetadataClass(
            schemaName=schema_name_full,
            platform=make_data_platform_urn("hana"),
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchemaClass(""),
        )

    def _gen_dataset_properties(
        self, dataset: HanaDataset, schema_name: str, db_name: Optional[str]
    ) -> DatasetPropertiesClass:
        """Generate dataset properties for a dataset."""
        custom_properties = {}

        if isinstance(dataset, HanaTable):
            custom_properties["table_type"] = dataset.type or "TABLE"
            if dataset.rows_count is not None:
                custom_properties["row_count"] = str(dataset.rows_count)
            if dataset.size_in_bytes is not None:
                custom_properties["size_in_bytes"] = str(dataset.size_in_bytes)
        elif isinstance(dataset, HanaView):
            custom_properties["view_type"] = dataset.view_type or "VIEW"
        elif isinstance(dataset, HanaCalculationView):
            custom_properties["view_type"] = "CALCULATION_VIEW"
            custom_properties["package_id"] = dataset.package_id

        # Add creation timestamp if available
        created_timestamp = None
        if hasattr(dataset, "created") and dataset.created:
            created_timestamp = TimeStampClass(
                time=int(dataset.created.timestamp()) * 1000
            )

        return DatasetPropertiesClass(
            name=dataset.name,
            description=getattr(dataset, "comment", None) or "",
            customProperties=custom_properties,
            created=created_timestamp,
        )

    def _gen_view_properties(
        self, dataset: Union[HanaView, HanaCalculationView]
    ) -> Optional[ViewPropertiesClass]:
        """Generate view properties for views and calculation views."""
        if isinstance(dataset, HanaView):
            return ViewPropertiesClass(
                materialized=dataset.materialized,
                viewLanguage="SQL",
                viewLogic=dataset.view_definition or "",
            )
        elif isinstance(dataset, HanaCalculationView):
            return ViewPropertiesClass(
                materialized=False,
                viewLanguage="XML",
                viewLogic=dataset.definition,
            )

        return None
