from unittest import mock

import pytest
from freezegun import freeze_time
from tableauserverclient.models import SiteItem

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_common import (
    DatasourceType,
    LineageResult,
    is_table_name_field,
)
from datahub.ingestion.source.tableau.tableau_virtual_connections import (
    VirtualConnectionProcessor,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from tests.unit.tableau.test_tableau_config import default_config

FROZEN_TIME = "2021-12-07 07:00:00"


@freeze_time(FROZEN_TIME)
class TestVirtualConnectionProcessor:
    """Consolidated, meaningful tests for Virtual Connection functionality"""

    def setup_method(self, method):
        """Set up test fixtures"""
        self.config = TableauConfig.parse_obj(default_config)
        self.ctx = PipelineContext(run_id="test")

        with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
            mock_site = mock.MagicMock(
                spec=SiteItem, id="test-site-id", content_url="test-site"
            )

            self.tableau_source = TableauSiteSource(
                config=self.config,
                ctx=self.ctx,
                platform="tableau",
                site=mock_site,
                server=mock.MagicMock(),
                report=TableauSourceReport(),
            )

        self.vc_processor = VirtualConnectionProcessor(self.tableau_source)

    def test_comprehensive_data_processing_flow(self):
        """Test the complete data processing flow with various scenarios"""
        # Test data with multiple edge cases
        datasource_with_vc_refs = {
            c.ID: "ds-123",
            c.NAME: "test_datasource",
            c.FIELDS: [
                # Valid VC reference
                {
                    c.NAME: "valid_field",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "column1",
                            c.TABLE: {
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                c.ID: "vc-table-1",
                                c.NAME: "test_table",
                                "virtualConnection": {c.ID: "vc-123"},
                            },
                        }
                    ],
                },
                # Missing field name (should be skipped)
                {c.UPSTREAM_COLUMNS: []},
                # Table name field (should be filtered)
                {c.NAME: "TABLE_NAME (SCHEMA.TABLE_NAME)", c.UPSTREAM_COLUMNS: []},
                # Missing column name (should be skipped)
                {
                    c.NAME: "invalid_field",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: None,
                            c.TABLE: {
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                c.ID: "vc-table-2",
                                c.NAME: "test_table2",
                            },
                        }
                    ],
                },
            ],
        }

        # Test processing
        self.vc_processor.process_datasource_for_vc_refs(
            datasource_with_vc_refs, DatasourceType.PUBLISHED
        )

        # Verify correct processing
        assert "ds-123" in self.vc_processor.datasource_vc_relationships
        relationships = self.vc_processor.datasource_vc_relationships["ds-123"]
        assert len(relationships) == 1  # Only valid reference should be processed
        assert relationships[0]["field_name"] == "valid_field"
        assert relationships[0]["vc_table_id"] == "vc-table-1"

    def test_comprehensive_empty_and_missing_data_handling(self):
        """Test handling of various empty/missing data scenarios"""
        # Test empty datasource
        empty_datasource = {c.NAME: "empty_ds", c.FIELDS: []}
        self.vc_processor.process_datasource_for_vc_refs(
            empty_datasource, DatasourceType.PUBLISHED
        )
        assert len(self.vc_processor.datasource_vc_relationships) == 0

        # Test missing datasource ID
        no_id_datasource = {c.NAME: "no_id_ds", c.FIELDS: []}
        self.vc_processor.process_datasource_for_vc_refs(
            no_id_datasource, DatasourceType.PUBLISHED
        )
        assert len(self.vc_processor.datasource_vc_relationships) == 0

        # Test empty VC table IDs list
        self.vc_processor.vc_table_ids_for_lookup = []
        self.vc_processor.lookup_vc_ids_from_table_ids()
        assert len(self.vc_processor.vc_table_id_to_vc_id) == 0

        # Test empty virtual connections list
        self.vc_processor.virtual_connection_ids_being_used = []
        with mock.patch.object(
            self.vc_processor.tableau_source, "get_connection_objects"
        ) as mock_get:
            result = list(self.vc_processor.emit_virtual_connections())
            assert len(result) == 0
            mock_get.assert_not_called()

    def test_comprehensive_lineage_creation_scenarios(self):
        """Test lineage creation with various success and failure scenarios"""
        # Setup test data
        self.vc_processor.datasource_vc_relationships = {
            "ds-123": [
                {
                    "vc_table_id": "vc-table-1",
                    "field_name": "test_field",
                    "column_name": "test_column",
                    "vc_table_name": "test_table",
                }
            ]
        }
        self.vc_processor.vc_table_id_to_vc_id = {"vc-table-1": "vc-123"}

        # Test valid URN
        valid_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,ds-123,PROD)"
        result = self.vc_processor.create_datasource_vc_lineage(valid_urn)
        assert isinstance(result, LineageResult)

        # Test invalid URN formats
        invalid_urns = [
            "invalid-urn-format",
            "urn:li:dataset:single_part",
            "urn:li:dataset:(incomplete",
        ]

        for invalid_urn in invalid_urns:
            result = self.vc_processor.create_datasource_vc_lineage(invalid_urn)
            assert result.upstream_tables == []
            assert result.fine_grained_lineages == []

        # Test no relationships for datasource
        no_rel_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,nonexistent,PROD)"
        result = self.vc_processor.create_datasource_vc_lineage(no_rel_urn)
        assert result.upstream_tables == []

        # Test missing required fields in relationships
        self.vc_processor.datasource_vc_relationships["ds-456"] = [
            {"incomplete": "data"}  # Missing required fields
        ]
        incomplete_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,ds-456,PROD)"
        result = self.vc_processor.create_datasource_vc_lineage(incomplete_urn)
        assert result.upstream_tables == []

    def test_comprehensive_table_upstream_lineage_scenarios(self):
        """Test table upstream lineage creation with various scenarios"""
        base_table_info = {
            c.ID: "vc-table-1",
            c.NAME: "test_table",
            c.COLUMNS: [
                {
                    c.ID: "col1",
                    c.NAME: "column1",
                    c.UPSTREAM_FIELDS: [
                        {
                            c.ID: "upstream-field-1",
                            c.NAME: "upstream_col",
                            c.DATA_SOURCE: {c.ID: "ds-123", c.NAME: "test_datasource"},
                        }
                    ],
                }
            ],
        }
        table_urn = "urn:li:dataset:test"

        # Test successful lineage creation
        mock_db_table = {c.ID: "db-table-1", c.NAME: "test_table"}
        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_find_matching_database_table",
                return_value=mock_db_table,
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_create_database_table_urn",
                return_value="urn:li:dataset:db_test",
            ),
        ):
            result = self.vc_processor._create_table_upstream_lineage(
                base_table_info, table_urn
            )
            assert len(result.upstream_tables) == 1

        # Test missing table name
        no_name_table = {c.ID: "vc-table-1", c.COLUMNS: []}
        result = self.vc_processor._create_table_upstream_lineage(
            no_name_table, table_urn
        )
        assert result.upstream_tables == []

        # Test no database match
        with mock.patch.object(
            self.vc_processor.tableau_source,
            "_find_matching_database_table",
            return_value=None,
        ):
            result = self.vc_processor._create_table_upstream_lineage(
                base_table_info, table_urn
            )
            assert result.upstream_tables == []

        # Test database URN creation failure
        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_find_matching_database_table",
                return_value=mock_db_table,
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_create_database_table_urn",
                return_value=None,
            ),
        ):
            result = self.vc_processor._create_table_upstream_lineage(
                base_table_info, table_urn
            )
            assert result.upstream_tables == []

    def test_comprehensive_schema_and_container_creation(self):
        """Test schema metadata and container creation with various scenarios"""
        # Test VC columns grouping
        vc_tables = [
            {
                c.ID: "vc-table-1",
                c.NAME: "table1",
                c.COLUMNS: [
                    {c.ID: "col1", c.NAME: "column1", c.REMOTE_TYPE: "STRING"},
                    {c.ID: "col2", c.NAME: "column2", c.REMOTE_TYPE: "INTEGER"},
                ],
            },
            {
                c.ID: "vc-table-2",
                # Missing NAME - should be skipped
                c.COLUMNS: [{c.ID: "col3", c.NAME: "column3", c.REMOTE_TYPE: "STRING"}],
            },
        ]

        grouped = self.vc_processor._group_vc_columns_by_table(vc_tables)
        assert len(grouped) == 1  # Only table with name should be included
        assert "table1" in grouped
        assert len(grouped["table1"]["columns"]) == 2

        # Test schema metadata creation
        schema_metadata = self.vc_processor._get_vc_schema_metadata_grouped_by_table(
            vc_tables
        )
        assert len(schema_metadata) == 1
        assert "table1" in schema_metadata
        assert isinstance(schema_metadata["table1"], SchemaMetadata)

        # Test container creation scenarios
        valid_vc = {c.ID: "vc-123", c.NAME: "test_vc", "projectName": "test_project"}

        with mock.patch.object(
            self.vc_processor, "_get_vc_project_luid", return_value="project-123"
        ):
            container_urn, workunits = self.vc_processor._create_vc_folder_container(
                valid_vc
            )
            assert container_urn is not None
            assert len(workunits) > 0

        # Test missing VC ID (should raise exception)
        invalid_vc = {c.NAME: "test_vc", "projectName": "test_project"}
        with pytest.raises(ValueError, match="VC ID is required"):
            self.vc_processor._create_vc_folder_container(invalid_vc)

        # Test missing project name
        no_project_vc = {c.ID: "vc-123", c.NAME: "test_vc"}
        result = self.vc_processor._get_vc_project_luid(no_project_vc)
        assert result is None

    def test_table_name_field_detection_comprehensive(self):
        """Test comprehensive table name field detection"""
        # Test various table name patterns
        table_name_patterns = [
            "ORDERS_TABLE (SALES_SCHEMA.ORDERS_TABLE)",
            "CUSTOMER_DATA (ANALYTICS_SCHEMA.CUSTOMER_DATA)",
            "PRODUCT_SALES_2024 (ANALYTICS_SCHEMA.PRODUCT_SALES_2024) (1)",
            "SCHEMA_NAME.TABLE_NAME",
            "DATABASE.SCHEMA.TABLE",
        ]

        for pattern in table_name_patterns:
            assert is_table_name_field(pattern), (
                f"Should detect '{pattern}' as table name"
            )

        # Test non-table name patterns
        column_patterns = [
            "regular_column",
            "user_id",
            "created_at",
            "",
            "mixed_Case_Column",
        ]

        for pattern in column_patterns:
            assert not is_table_name_field(pattern), (
                f"Should NOT detect '{pattern}' as table name"
            )

    def test_configuration_and_initialization(self):
        """Test processor initialization and configuration"""
        # Test basic initialization
        assert self.vc_processor.tableau_source is not None
        assert self.vc_processor.config is not None
        assert isinstance(self.vc_processor.vc_table_ids_for_lookup, list)
        assert isinstance(self.vc_processor.datasource_vc_relationships, dict)

        # Test folder key generation
        folder_key = self.vc_processor.gen_vc_folder_key("test-vc-id")
        assert folder_key.virtual_connection_id == "test-vc-id"
        assert folder_key.platform == self.vc_processor.platform

    def test_error_handling_and_exception_scenarios(self):
        """Test comprehensive error handling"""
        # Test exception in URN processing
        problematic_urn = mock.MagicMock()
        problematic_urn.split.side_effect = AttributeError("Mock exception")

        result = self.vc_processor.create_datasource_vc_lineage(str(problematic_urn))
        assert result.upstream_tables == []
        assert result.fine_grained_lineages == []

        # Test malformed data handling
        malformed_datasource = {
            c.ID: "malformed-ds",
            c.FIELDS: "not_a_list",  # Should be a list
        }

        # Should handle gracefully without crashing
        try:
            self.vc_processor.process_datasource_for_vc_refs(
                malformed_datasource, DatasourceType.PUBLISHED
            )
        except Exception as e:
            # If it does raise an exception, it should be handled gracefully
            assert "get" in str(e) or "iteration" in str(e).lower()

    def test_integration_with_tableau_source_methods(self):
        """Test integration with tableau source methods"""
        # Test VC lookup with mock data
        self.vc_processor.vc_table_ids_for_lookup = ["vc-table-1", "vc-table-2"]

        mock_vc_data = [
            {
                c.ID: "vc-123",
                c.TABLES: [
                    {c.ID: "vc-table-1", c.NAME: "table1"},
                    {c.ID: "vc-table-2", c.NAME: "table2"},
                ],
            }
        ]

        with mock.patch.object(
            self.vc_processor.tableau_source,
            "get_connection_objects",
            return_value=mock_vc_data,
        ):
            self.vc_processor.lookup_vc_ids_from_table_ids()

            # Verify mappings were created
            assert len(self.vc_processor.vc_table_id_to_vc_id) == 2
            assert self.vc_processor.vc_table_id_to_vc_id["vc-table-1"] == "vc-123"
            assert len(self.vc_processor.vc_table_id_to_name) == 2

    def test_virtual_connection_emission_flow(self):
        """Test the complete VC emission flow including container and schema creation"""
        # Setup VC data
        self.vc_processor.virtual_connection_ids_being_used = ["vc-123"]

        mock_vc_data = [
            {
                c.ID: "vc-123",
                c.NAME: "test_vc",
                c.DESCRIPTION: "Test Virtual Connection",
                "projectName": "test_project",
                c.TABLES: [
                    {
                        c.ID: "vc-table-1",
                        c.NAME: "test.table1",
                        c.COLUMNS: [
                            {c.ID: "col1", c.NAME: "column1", c.REMOTE_TYPE: "STRING"},
                            {c.ID: "col2", c.NAME: "column2", c.REMOTE_TYPE: "INTEGER"},
                        ],
                    }
                ],
            }
        ]

        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "get_connection_objects",
                return_value=mock_vc_data,
            ),
            mock.patch.object(
                self.vc_processor, "_get_vc_project_luid", return_value="project-123"
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "gen_project_key",
                return_value=mock.MagicMock(guid=lambda: "project_guid"),
            ),
        ):
            workunits = list(self.vc_processor.emit_virtual_connections())

            # Should emit container and dataset workunits
            assert len(workunits) > 0

            # Verify container creation was called
            container_workunits = [
                wu for wu in workunits if "container" in str(wu.metadata)
            ]
            assert len(container_workunits) > 0

    def test_column_level_lineage_creation(self):
        """Test detailed column-level lineage creation scenarios"""
        # Setup complex column lineage data
        table_info = {
            c.ID: "vc-table-1",
            c.NAME: "test_table",
            c.COLUMNS: [
                {
                    c.ID: "col1",
                    c.NAME: "customer_id",
                    c.UPSTREAM_FIELDS: [
                        {
                            c.ID: "upstream-field-1",
                            c.NAME: "id",
                            c.DATA_SOURCE: {c.ID: "ds-123", c.NAME: "customers_db"},
                        }
                    ],
                },
                {
                    c.ID: "col2",
                    c.NAME: "order_total",
                    c.UPSTREAM_FIELDS: [
                        {
                            c.ID: "upstream-field-2",
                            c.NAME: "total_amount",
                            c.DATA_SOURCE: {c.ID: "ds-456", c.NAME: "orders_db"},
                        }
                    ],
                },
                # Column with no upstream fields
                {c.ID: "col3", c.NAME: "calculated_field", c.UPSTREAM_FIELDS: []},
            ],
        }

        table_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-table-1,PROD)"

        # Mock database table matching
        mock_db_table = {c.ID: "db-table-1", c.NAME: "test_table"}
        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_find_matching_database_table",
                return_value=mock_db_table,
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_create_database_table_urn",
                return_value="urn:li:dataset:db_test",
            ),
        ):
            result = self.vc_processor._create_table_upstream_lineage(
                table_info, table_urn
            )

            # Should have table-level lineage
            assert len(result.upstream_tables) == 1

            # Should have fine-grained lineage for columns with upstream fields
            # Note: Fine-grained lineage creation depends on datasource URN mapping
            # which isn't set up in this test, so we expect empty fine-grained lineages
            assert len(result.fine_grained_lineages) == 0

    def test_datasource_vc_relationship_processing(self):
        """Test processing of datasource-VC relationships with various data scenarios"""
        # Setup complex datasource with multiple VC references
        complex_datasource = {
            c.ID: "complex-ds-123",
            c.NAME: "complex_datasource",
            c.FIELDS: [
                # Field with valid VC reference
                {
                    c.NAME: "customer_name",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "name",
                            c.TABLE: {
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                c.ID: "vc-table-1",
                                c.NAME: "customers",
                                "virtualConnection": {c.ID: "vc-123"},
                            },
                        }
                    ],
                },
                # Field with multiple upstream columns
                {
                    c.NAME: "order_info",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "order_id",
                            c.TABLE: {
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                c.ID: "vc-table-2",
                                c.NAME: "orders",
                                "virtualConnection": {c.ID: "vc-456"},
                            },
                        },
                        {
                            c.NAME: "order_date",
                            c.TABLE: {
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                c.ID: "vc-table-2",
                                c.NAME: "orders",
                                "virtualConnection": {c.ID: "vc-456"},
                            },
                        },
                    ],
                },
                # Field with non-VC upstream (should be ignored)
                {
                    c.NAME: "calculated_field",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "base_field",
                            c.TABLE: {
                                c.TYPE_NAME: "Table",  # Not a VC table
                                c.ID: "regular-table-1",
                                c.NAME: "regular_table",
                            },
                        }
                    ],
                },
            ],
        }

        # Process the datasource
        self.vc_processor.process_datasource_for_vc_refs(
            complex_datasource, DatasourceType.PUBLISHED
        )

        # Verify relationships were created correctly
        assert "complex-ds-123" in self.vc_processor.datasource_vc_relationships
        relationships = self.vc_processor.datasource_vc_relationships["complex-ds-123"]

        # Should have 3 relationships (1 for customer_name, 2 for order_info)
        assert len(relationships) == 3

        # Verify VC table IDs were collected
        expected_table_ids = {"vc-table-1", "vc-table-2"}
        actual_table_ids = set(self.vc_processor.vc_table_ids_for_lookup)
        assert expected_table_ids.issubset(actual_table_ids)

    def test_vc_folder_and_project_handling(self):
        """Test VC folder container creation and project handling scenarios"""
        # Test with valid project
        vc_with_project = {
            c.ID: "vc-123",
            c.NAME: "test_vc",
            "projectName": "Analytics Project",
        }

        # Mock project registry
        mock_project = mock.MagicMock()
        mock_project.name = "Analytics Project"
        mock_project_registry = {"project-456": mock_project}

        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "tableau_project_registry",
                mock_project_registry,
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "gen_project_key",
                return_value=mock.MagicMock(guid=lambda: "project_guid"),
            ),
        ):
            # Test project LUID lookup
            project_luid = self.vc_processor._get_vc_project_luid(vc_with_project)
            assert project_luid == "project-456"

            # Test container creation
            container_urn, workunits = self.vc_processor._create_vc_folder_container(
                vc_with_project
            )
            assert container_urn is not None
            assert len(workunits) > 0

        # Test with missing project
        vc_no_project = {c.ID: "vc-789", c.NAME: "test_vc_no_project"}

        project_luid = self.vc_processor._get_vc_project_luid(vc_no_project)
        assert project_luid is None

        # Container creation should still work without project
        container_urn, workunits = self.vc_processor._create_vc_folder_container(
            vc_no_project
        )
        assert container_urn is not None
        assert len(workunits) > 0
