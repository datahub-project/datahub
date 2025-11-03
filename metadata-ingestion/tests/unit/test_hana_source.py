"""
Unit tests for SAP HANA source.

This module contains comprehensive unit tests for the SAP HANA DataHub source,
including tests for configuration, schema generation, calculation view parsing,
and work unit generation.
"""

import platform
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.hana import HanaConfig, HanaSource
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaColumn,
    HanaDatabase,
    HanaTable,
    HanaView,
)
from datahub.ingestion.source.sql.hana.hana_schema_gen import HanaSchemaGenerator
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    NumberTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


@pytest.mark.skipif(
    platform.machine().lower() == "aarch64",
    reason="The hdbcli dependency is not available for aarch64",
)
class TestHanaSource:
    """Test cases for the main HanaSource class."""

    @patch("datahub.ingestion.source.sql.hana.hana.create_engine")
    def test_platform_correctly_set_hana(self, mock_create_engine):
        """Test that the platform is correctly set to 'hana'."""
        mock_create_engine.return_value = MagicMock()

        config = HanaConfig(
            username="test", password="test", host_port="localhost:39041"
        )
        source = HanaSource(
            ctx=PipelineContext(run_id="hana-source-test"),
            config=config,
        )
        assert source.get_platform() == "hana"

    def test_hana_uri_native(self):
        """Test native HANA URI generation without database."""
        config = HanaConfig.parse_obj(
            {
                "username": "user",
                "password": "password",
                "host_port": "host:39041",
                "scheme": "hana+hdbcli",
            }
        )
        assert config.get_sql_alchemy_url() == "hana+hdbcli://user:password@host:39041"

    def test_hana_uri_native_with_database(self):
        """Test native HANA URI generation with database."""
        config = HanaConfig.parse_obj(
            {
                "username": "user",
                "password": "password",
                "host_port": "host:39041",
                "scheme": "hana+hdbcli",
                "database": "database",
            }
        )
        assert (
            config.get_sql_alchemy_url()
            == "hana+hdbcli://user:password@host:39041/database"
        )

    def test_config_with_patterns(self):
        """Test configuration with schema and table patterns."""
        config = HanaConfig.parse_obj(
            {
                "username": "user",
                "password": "password",
                "host_port": "host:39041",
                "schema_pattern": {"allow": ["PROD_.*"], "deny": [".*_TEMP"]},
                "table_pattern": {"allow": ["CUSTOMER_.*"], "deny": [".*_BACKUP"]},
            }
        )

        assert config.schema_pattern.allowed("PROD_SALES")
        assert not config.schema_pattern.allowed("TEST_TEMP")
        assert config.table_pattern.allowed("CUSTOMER_DATA")
        assert not config.table_pattern.allowed("CUSTOMER_BACKUP")

    @patch("datahub.ingestion.source.sql.hana.hana.create_engine")
    def test_source_initialization(self, mock_create_engine):
        """Test proper initialization of HanaSource components."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = HanaConfig(
            username="user",
            password="password",
            host_port="host:39041",
        )

        source = HanaSource(
            config=config,
            ctx=PipelineContext(run_id="test"),
        )

        assert source.config == config
        assert isinstance(source.identifiers, HanaIdentifierBuilder)
        assert isinstance(source.data_dictionary, HanaDataDictionary)
        assert isinstance(source.schema_generator, HanaSchemaGenerator)


class TestHanaIdentifierBuilder:
    """Test cases for HanaIdentifierBuilder."""

    def test_dataset_identifier_generation(self):
        """Test dataset identifier generation."""
        config = HanaConfig(host_port="localhost:39041")
        builder = HanaIdentifierBuilder(config)

        # Test with database
        identifier = builder.get_dataset_identifier("CUSTOMERS", "SALES", "PROD")
        assert identifier == "prod.sales.customers"

        # Test without database
        identifier = builder.get_dataset_identifier("CUSTOMERS", "SALES", "")
        assert identifier == "sales.customers"

    def test_dataset_urn_generation(self):
        """Test dataset URN generation."""
        config = HanaConfig(
            host_port="localhost:39041", platform_instance="hana-prod", env="PROD"
        )
        builder = HanaIdentifierBuilder(config)

        urn = builder.gen_dataset_urn("prod.sales.customers")
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:hana,hana-prod.prod.sales.customers,PROD)"
            in urn
        )

    def test_user_identifier_generation(self):
        """Test user identifier generation."""
        config = HanaConfig(host_port="localhost:39041")
        builder = HanaIdentifierBuilder(config)

        # With email
        user_id = builder.get_user_identifier("john.doe", "john.doe@company.com")
        assert user_id == "john.doe@company.com"

        # Without email
        user_id = builder.get_user_identifier("JOHN.DOE", None)
        assert user_id == "john.doe"


class TestHanaDataDictionary:
    """Test cases for HanaDataDictionary."""

    @patch("datahub.ingestion.source.sql.hana.hana_data_dictionary.HanaDataDictionary")
    def test_get_databases(self, mock_dd_class):
        """Test database retrieval."""
        mock_dd = mock_dd_class.return_value
        mock_dd.get_databases.return_value = [
            HanaDatabase(name="PROD", comment="Production database"),
            HanaDatabase(name="TEST", comment="Test database"),
        ]

        databases = mock_dd.get_databases()
        assert len(databases) == 2
        assert databases[0].name == "PROD"
        assert databases[1].name == "TEST"

    @patch("datahub.ingestion.source.sql.hana.hana_data_dictionary.HanaDataDictionary")
    def test_get_tables_for_schema(self, mock_dd_class):
        """Test table retrieval for schema."""
        mock_dd = mock_dd_class.return_value
        mock_dd.get_tables_for_schema.return_value = [
            HanaTable(
                name="CUSTOMERS", schema_name="SALES", type="ROW", rows_count=1000
            ),
            HanaTable(
                name="ORDERS", schema_name="SALES", type="COLUMN", rows_count=5000
            ),
        ]

        tables = mock_dd.get_tables_for_schema("SALES", "PROD")
        assert len(tables) == 2
        assert tables[0].name == "CUSTOMERS"
        assert tables[1].name == "ORDERS"

    @patch("datahub.ingestion.source.sql.hana.hana_data_dictionary.HanaDataDictionary")
    def test_get_calculation_views(self, mock_dd_class):
        """Test calculation view retrieval."""
        mock_dd = mock_dd_class.return_value
        mock_dd.get_calculation_views.return_value = [
            HanaCalculationView(
                name="SALES_ANALYSIS",
                package_id="analytics.sales",
                definition="<xml>...</xml>",
            ),
        ]

        calc_views = mock_dd.get_calculation_views()
        assert len(calc_views) == 1
        assert calc_views[0].name == "SALES_ANALYSIS"
        assert calc_views[0].package_id == "analytics.sales"


class TestHanaSchemaGenerator:
    """Test cases for HanaSchemaGenerator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = HanaConfig(host_port="localhost:39041")
        self.report = MagicMock()
        self.data_dictionary = MagicMock()
        self.identifiers = HanaIdentifierBuilder(self.config)

        self.schema_generator = HanaSchemaGenerator(
            config=self.config,
            report=self.report,
            data_dictionary=self.data_dictionary,
            identifiers=self.identifiers,
        )

    def test_gen_dataset_workunits_for_table(self):
        """Test work unit generation for a table."""
        table = HanaTable(
            name="CUSTOMERS",
            schema_name="SALES",
            type="ROW",
            comment="Customer master data",
            rows_count=1000,
            columns=[
                HanaColumn(
                    name="ID",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=1,
                ),
                HanaColumn(
                    name="NAME",
                    data_type="VARCHAR",
                    character_maximum_length=100,
                    nullable=True,
                    ordinal_position=2,
                ),
            ],
        )

        workunits = list(
            self.schema_generator._gen_dataset_workunits(table, "SALES", "PROD")
        )

        # Should generate multiple work units (status, schema, properties, subtype)
        assert len(workunits) >= 4

        # Check that we have the expected aspects
        aspect_types = [
            wu.metadata.aspect.__class__.__name__
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
        ]
        assert "StatusClass" in aspect_types
        assert "SchemaMetadataClass" in aspect_types
        assert "DatasetPropertiesClass" in aspect_types
        assert "SubTypesClass" in aspect_types

    def test_gen_dataset_workunits_for_view(self):
        """Test work unit generation for a view."""
        view = HanaView(
            name="CUSTOMER_VIEW",
            schema_name="SALES",
            view_definition="SELECT * FROM CUSTOMERS WHERE ACTIVE = 'Y'",
            comment="Active customers view",
            columns=[
                HanaColumn(
                    name="ID",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=1,
                ),
            ],
        )

        workunits = list(
            self.schema_generator._gen_dataset_workunits(view, "SALES", "PROD")
        )

        # Should generate work units including view properties
        aspect_types = [
            wu.metadata.aspect.__class__.__name__
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
        ]
        assert "ViewPropertiesClass" in aspect_types

    def test_gen_dataset_workunits_for_calculation_view(self):
        """Test work unit generation for a calculation view."""
        calc_view = HanaCalculationView(
            name="SALES_ANALYSIS",
            package_id="analytics.sales",
            definition="<calculationView>...</calculationView>",
            columns=[
                HanaColumn(
                    name="SALES_AMOUNT",
                    data_type="DECIMAL",
                    numeric_precision=15,
                    numeric_scale=2,
                    ordinal_position=1,
                ),
            ],
        )

        workunits = list(
            self.schema_generator._gen_dataset_workunits(calc_view, "_sys_bic", None)
        )

        # Should generate work units including view properties for calculation view
        aspect_types = [
            wu.metadata.aspect.__class__.__name__
            for wu in workunits
            if hasattr(wu.metadata, "aspect")
        ]
        assert "ViewPropertiesClass" in aspect_types

    def test_gen_schema_metadata(self):
        """Test schema metadata generation."""
        table = HanaTable(
            name="CUSTOMERS",
            schema_name="SALES",
            columns=[
                HanaColumn(
                    name="ID",
                    data_type="INTEGER",
                    nullable=False,
                    ordinal_position=1,
                ),
                HanaColumn(
                    name="NAME",
                    data_type="VARCHAR",
                    character_maximum_length=100,
                    nullable=True,
                    ordinal_position=2,
                ),
            ],
        )

        schema_metadata = self.schema_generator._gen_schema_metadata(
            table, "SALES", "PROD"
        )

        assert isinstance(schema_metadata, SchemaMetadataClass)
        assert schema_metadata.schemaName == "SALES.CUSTOMERS"
        assert len(schema_metadata.fields) == 2

        # Check field details
        id_field = schema_metadata.fields[0]
        assert id_field.fieldPath == "ID"
        assert isinstance(id_field.type.type, NumberTypeClass)
        assert not id_field.nullable

        name_field = schema_metadata.fields[1]
        assert name_field.fieldPath == "NAME"
        assert isinstance(name_field.type.type, StringTypeClass)
        assert name_field.nullable

    def test_gen_dataset_properties(self):
        """Test dataset properties generation."""
        table = HanaTable(
            name="CUSTOMERS",
            schema_name="SALES",
            type="ROW",
            comment="Customer master data",
            rows_count=1000,
            size_in_bytes=50000,
        )

        properties = self.schema_generator._gen_dataset_properties(
            table, "SALES", "PROD"
        )

        assert isinstance(properties, DatasetPropertiesClass)
        assert properties.name == "CUSTOMERS"
        assert properties.description == "Customer master data"
        assert properties.customProperties["table_type"] == "ROW"
        assert properties.customProperties["row_count"] == "1000"
        assert properties.customProperties["size_in_bytes"] == "50000"


class TestSAPCalculationViewParser:
    """Test cases for SAP Calculation View Parser."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = SAPCalculationViewParser()

    def test_parse_simple_calculation_view(self):
        """Test parsing of a simple calculation view."""
        xml_content = """
        <calculationView xmlns="http://www.sap.com/ndb/BiModelCalculation.ecore">
            <dataSources>
                <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
                    <columnObject schemaName="SALES" columnObjectName="CUSTOMERS"/>
                </DataSource>
            </dataSources>
            <logicalModel>
                <attributes>
                    <attribute id="CUSTOMER_ID">
                        <keyMapping columnObjectName="CUSTOMERS" columnName="ID"/>
                    </attribute>
                </attributes>
                <baseMeasures>
                    <measure id="TOTAL_SALES">
                        <measureMapping columnObjectName="CUSTOMERS" columnName="SALES_AMOUNT"/>
                    </measure>
                </baseMeasures>
            </logicalModel>
        </calculationView>
        """

        xml_root = ET.fromstring(xml_content)
        result = self.parser._parse_calc_view("TEST_VIEW", xml_root)

        assert result["viewName"] == "TEST_VIEW"
        assert "CUSTOMERS" in result["sources"]
        assert result["sources"]["CUSTOMERS"]["type"] == "DATA_BASE_TABLE"
        assert result["sources"]["CUSTOMERS"]["name"] == "CUSTOMERS"
        assert result["sources"]["CUSTOMERS"]["path"] == "SALES"

        assert "CUSTOMER_ID" in result["outputs"]
        assert result["outputs"]["CUSTOMER_ID"]["type"] == "attribute"
        assert result["outputs"]["CUSTOMER_ID"]["source"] == "ID"

        assert "TOTAL_SALES" in result["outputs"]
        assert result["outputs"]["TOTAL_SALES"]["type"] == "measure"

    def test_extract_columns_from_formula(self):
        """Test extraction of column names from formulas."""
        formula = 'if("SALES_AMOUNT" > 1000, "HIGH", "LOW")'
        columns = self.parser._extract_columns_from_formula(formula)

        assert "SALES_AMOUNT" in columns
        assert "HIGH" in columns
        assert "LOW" in columns

    def test_format_column_lineage(self):
        """Test formatting of column lineage for DataHub."""
        xml_content = """
        <calculationView xmlns="http://www.sap.com/ndb/BiModelCalculation.ecore">
            <dataSources>
                <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
                    <columnObject schemaName="SALES" columnObjectName="CUSTOMERS"/>
                </DataSource>
            </dataSources>
            <logicalModel>
                <attributes>
                    <attribute id="CUSTOMER_ID">
                        <keyMapping columnObjectName="CUSTOMERS" columnName="ID"/>
                    </attribute>
                </attributes>
            </logicalModel>
        </calculationView>
        """

        lineage = self.parser.format_column_lineage("TEST_VIEW", xml_content)

        assert len(lineage) > 0

        # Check structure of lineage information
        for col_lineage in lineage:
            assert "downstream_column" in col_lineage
            assert "upstream" in col_lineage
            assert isinstance(col_lineage["upstream"], list)

            for upstream in col_lineage["upstream"]:
                assert "upstream_table" in upstream
                assert "upstream_column" in upstream


class TestHanaSchema:
    """Test cases for HANA schema data classes."""

    def test_hana_column_precise_native_type(self):
        """Test precise native type generation for columns."""
        # Test decimal type
        decimal_col = HanaColumn(
            name="AMOUNT",
            data_type="DECIMAL",
            numeric_precision=15,
            numeric_scale=2,
        )
        assert decimal_col.get_precise_native_type() == "DECIMAL(15,2)"

        # Test varchar type
        varchar_col = HanaColumn(
            name="NAME",
            data_type="VARCHAR",
            character_maximum_length=100,
        )
        assert varchar_col.get_precise_native_type() == "VARCHAR(100)"

        # Test basic type
        int_col = HanaColumn(name="ID", data_type="INTEGER")
        assert int_col.get_precise_native_type() == "INTEGER"

    def test_hana_table_subtype(self):
        """Test table subtype."""
        table = HanaTable(name="TEST_TABLE", schema_name="TEST_SCHEMA")
        assert table.get_subtype() == DatasetSubTypes.TABLE

    def test_hana_view_subtype(self):
        """Test view subtype."""
        view = HanaView(name="TEST_VIEW", schema_name="TEST_SCHEMA")
        assert view.get_subtype() == DatasetSubTypes.VIEW

    def test_hana_calculation_view_subtype(self):
        """Test calculation view subtype."""
        calc_view = HanaCalculationView(
            name="TEST_CALC_VIEW",
            package_id="test.package",
            definition="<xml/>",
        )
        assert calc_view.get_subtype() == DatasetSubTypes.VIEW
        assert calc_view.full_name == "test.package/TEST_CALC_VIEW"


@pytest.mark.integration
class TestHanaSourceIntegration:
    """Integration tests for HanaSource (requires actual HANA connection)."""

    @pytest.mark.skip(reason="Requires actual HANA database connection")
    def test_end_to_end_ingestion(self):
        """Test end-to-end ingestion process."""
        config = HanaConfig(
            username="testuser",
            password="testpass",
            host_port="localhost:39041",
            database="testdb",
        )

        source = HanaSource(
            config=config,
            ctx=PipelineContext(run_id="integration-test"),
        )

        workunits = list(source.get_workunits_internal())

        # Should generate work units for discovered entities
        assert len(workunits) > 0

        # Check that work units are properly formed
        for wu in workunits:
            assert isinstance(wu, MetadataWorkUnit)
            assert wu.metadata is not None
            assert (
                hasattr(wu.metadata, "entityUrn") and wu.metadata.entityUrn is not None
            )
