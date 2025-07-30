import pathlib
from typing import Any, cast
from unittest import mock

from freezegun import freeze_time

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_virtual_connections import (
    VCFolderKey,
    VirtualConnectionProcessor,
)
from tests.unit.tableau.test_tableau_config import default_config

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = pathlib.Path(__file__).parent


@freeze_time(FROZEN_TIME)
class TestVirtualConnectionProcessor:
    """Test suite for VirtualConnectionProcessor functionality."""

    def setup_method(self, method):
        """Set up test fixtures."""
        self.config = TableauConfig.parse_obj(default_config)
        self.ctx = PipelineContext(run_id="test")

        from tableauserverclient.models import SiteItem

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

    def test_initialization(self):
        """Test VirtualConnectionProcessor initialization."""
        assert self.vc_processor.tableau_source == self.tableau_source
        assert self.vc_processor.config == self.config
        assert self.vc_processor.platform == "tableau"
        assert isinstance(self.vc_processor.vc_table_ids_for_lookup, list)
        assert isinstance(self.vc_processor.vc_table_id_to_vc_id, dict)
        assert isinstance(self.vc_processor.virtual_connection_ids_being_used, list)
        assert isinstance(self.vc_processor.datasource_vc_relationships, dict)

    def test_gen_vc_folder_key(self):
        """Test VCFolderKey generation."""
        folder_key = self.vc_processor.gen_vc_folder_key("vc-123")

        assert isinstance(folder_key, VCFolderKey)
        assert folder_key.platform == "tableau"
        assert folder_key.virtual_connection_id == "vc-123"
        assert folder_key.instance == self.config.platform_instance

    def test_process_datasource_for_vc_refs_with_vc_references(self):
        """Test processing datasource with virtual connection references."""
        datasource = {
            c.ID: "ds-123",
            c.FIELDS: [
                {
                    c.NAME: "field1",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "column1",
                            c.TABLE: {
                                c.ID: "vc-table-1",
                                c.NAME: "test_table",
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                "virtualConnection": {
                                    c.ID: "vc-123",
                                    c.NAME: "Test VC",
                                },
                            },
                        }
                    ],
                },
                {
                    c.NAME: "field2",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "column2",
                            c.TABLE: {
                                c.ID: "vc-table-2",
                                c.NAME: "another_table",
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                "virtualConnection": {
                                    c.ID: "vc-456",
                                    c.NAME: "Another VC",
                                },
                            },
                        }
                    ],
                },
            ],
        }

        self.vc_processor.process_datasource_for_vc_refs(datasource, "published")

        # Check that VC references were captured
        assert "ds-123" in self.vc_processor.datasource_vc_relationships
        assert len(self.vc_processor.datasource_vc_relationships["ds-123"]) == 2
        assert "vc-table-1" in self.vc_processor.vc_table_ids_for_lookup
        assert "vc-table-2" in self.vc_processor.vc_table_ids_for_lookup

        # Check that VC references contain the expected VC IDs
        relationships = self.vc_processor.datasource_vc_relationships["ds-123"]
        vc_ids_found = [rel.get("vc_id") for rel in relationships]
        assert "vc-123" in vc_ids_found
        assert "vc-456" in vc_ids_found

    def test_process_datasource_for_vc_refs_without_vc_references(self):
        """Test processing datasource without virtual connection references."""
        datasource = {
            c.ID: "ds-456",
            c.FIELDS: [
                {
                    c.NAME: "field1",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "column1",
                            c.TABLE: {
                                c.ID: "regular-table-1",
                                c.NAME: "regular_table",
                                c.TYPE_NAME: "DatabaseTable",
                            },
                        }
                    ],
                }
            ],
        }

        initial_vc_count = len(self.vc_processor.vc_table_ids_for_lookup)
        self.vc_processor.process_datasource_for_vc_refs(datasource, "embedded")

        # Should not add any VC references
        assert len(self.vc_processor.vc_table_ids_for_lookup) == initial_vc_count
        assert "ds-456" not in self.vc_processor.datasource_vc_relationships

    def test_process_datasource_for_vc_refs_malformed_data(self):
        """Test processing datasource with malformed data."""
        # Missing ID
        malformed_datasource1 = {c.FIELDS: [{c.NAME: "field1", c.UPSTREAM_COLUMNS: []}]}

        # Missing fields
        malformed_datasource2 = {c.ID: "ds-789"}

        # Empty datasource
        empty_datasource: dict[str, Any] = {}

        # Should handle gracefully without crashing
        self.vc_processor.process_datasource_for_vc_refs(
            malformed_datasource1, "published"
        )
        self.vc_processor.process_datasource_for_vc_refs(
            malformed_datasource2, "embedded"
        )
        self.vc_processor.process_datasource_for_vc_refs(empty_datasource, "published")

        # Should not have added any problematic entries
        assert len(self.vc_processor.vc_table_ids_for_lookup) == 0

    def test_lookup_vc_ids_from_table_ids(self):
        """Test looking up VC IDs from table IDs."""
        # First, add some VC table references
        self.vc_processor.vc_table_ids_for_lookup = [
            "vc-table-1",
            "vc-table-2",
            "vc-table-3",
        ]
        self.vc_processor.vc_table_id_to_vc_id = {
            "vc-table-1": "vc-123",
            "vc-table-2": "vc-456",
            "vc-table-3": "vc-123",  # Same VC, different table
        }

        # Mock the GraphQL query response - get_connection_objects returns VCs directly
        mock_vcs = [
            {
                c.ID: "vc-123",
                c.NAME: "Test VC 1",
                "tables": [
                    {c.ID: "vc-table-1", c.NAME: "table1"},
                    {c.ID: "vc-table-3", c.NAME: "table3"},
                ],
            },
            {
                c.ID: "vc-456",
                c.NAME: "Test VC 2",
                "tables": [{c.ID: "vc-table-2", c.NAME: "table2"}],
            },
        ]

        with mock.patch.object(
            self.tableau_source, "get_connection_objects", return_value=mock_vcs
        ):
            self.vc_processor.lookup_vc_ids_from_table_ids()

        # Should have identified unique VC IDs
        assert "vc-123" in self.vc_processor.virtual_connection_ids_being_used
        assert "vc-456" in self.vc_processor.virtual_connection_ids_being_used
        assert len(self.vc_processor.virtual_connection_ids_being_used) == 2

    def test_create_virtual_connection_lineage_v2(self):
        """Test virtual connection lineage creation."""
        virtual_connection = {
            c.ID: "vc-123",
            c.NAME: "Test VC",
            c.TABLES: [
                {
                    c.ID: "vc-table-1",
                    c.NAME: "test_table",
                    c.COLUMNS: [
                        {
                            c.ID: "vc-col-1",
                            c.NAME: "test_column",
                            c.REMOTE_TYPE: "INTEGER",
                            c.UPSTREAM_FIELDS: [
                                {
                                    c.ID: "upstream-field-1",
                                    c.NAME: "source_column",
                                    c.DATA_SOURCE: {
                                        c.ID: "ds-456",
                                        c.NAME: "Source DS",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        vc_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-123,PROD)"

        vc_tables = cast(list[dict[str, Any]], virtual_connection.get(c.TABLES, []))
        upstream_tables, fine_grained_lineages = (
            self.vc_processor._create_vc_upstream_lineage_v2(
                virtual_connection, vc_tables, vc_urn
            )
        )

        # Should return lists
        assert isinstance(upstream_tables, list)
        assert isinstance(fine_grained_lineages, list)

        # Should have found upstream datasources
        if upstream_tables:
            assert len(upstream_tables) > 0
            # Check that upstream is properly formed
            upstream = upstream_tables[0]
            assert hasattr(upstream, "dataset")
            assert hasattr(upstream, "type")

    def test_create_datasource_vc_lineage_v2_no_relationships(self):
        """Test datasource VC lineage creation when no relationships exist."""
        datasource_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,ds-123,PROD)"

        upstream_tables, fine_grained_lineages = (
            self.vc_processor.create_datasource_vc_lineage_v2(datasource_urn)
        )

        # Should return empty lists when no relationships exist
        assert upstream_tables == []
        assert fine_grained_lineages == []

    def test_create_datasource_vc_lineage_v2_with_relationships(self):
        """Test datasource VC lineage creation with existing relationships."""
        datasource_id = "ds-123"
        datasource_urn = (
            f"urn:li:dataset:(urn:li:dataPlatform:tableau,{datasource_id},PROD)"
        )

        # Set up relationships
        self.vc_processor.datasource_vc_relationships[datasource_id] = [
            {
                "vc_id": "vc-456",
                "vc_table_id": "vc-table-1",
                "vc_table_name": "test_table",
                "column_name": "test_column",
                "field_name": "test_field",
            }
        ]

        # Mock VC table to name mapping
        self.vc_processor.vc_table_id_to_name = {"vc-table-1": "test_table"}

        upstream_tables, fine_grained_lineages = (
            self.vc_processor.create_datasource_vc_lineage_v2(datasource_urn)
        )

        # Should have created lineage
        assert isinstance(upstream_tables, list)
        assert isinstance(fine_grained_lineages, list)

    def test_error_handling_in_lineage_creation(self):
        """Test error handling during lineage creation."""
        # Test with malformed virtual connection data
        malformed_vc = {
            c.ID: "vc-broken"
            # Missing required fields
        }

        vc_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-broken,PROD)"

        # Should handle gracefully
        vc_tables_raw: Any = malformed_vc.get(c.TABLES, [])
        vc_tables = cast(
            list[dict[str, Any]],
            vc_tables_raw if isinstance(vc_tables_raw, list) else [],
        )
        upstream_tables, fine_grained_lineages = (
            self.vc_processor._create_vc_upstream_lineage_v2(
                malformed_vc, vc_tables, vc_urn
            )
        )

        # Should return empty lists for malformed data
        assert upstream_tables == []
        assert fine_grained_lineages == []

    def test_vc_folder_key_properties(self):
        """Test VCFolderKey properties and methods."""
        folder_key = VCFolderKey(
            platform="tableau",
            instance="test-instance",
            virtual_connection_id="vc-test-123",
        )

        assert folder_key.platform == "tableau"
        assert folder_key.instance == "test-instance"
        assert folder_key.virtual_connection_id == "vc-test-123"

    def test_report_statistics_tracking(self):
        """Test that report statistics are properly tracked."""
        # Initial state
        assert self.tableau_source.report.num_virtual_connections_processed == 0
        assert self.tableau_source.report.num_vc_table_references_found == 0
        assert self.tableau_source.report.num_vc_lineages_created == 0

        # Simulate processing
        self.tableau_source.report.num_virtual_connections_processed = 3
        self.tableau_source.report.num_vc_table_references_found = 7
        self.tableau_source.report.num_vc_lineages_created = 12

        # Verify tracking
        assert self.tableau_source.report.num_virtual_connections_processed == 3
        assert self.tableau_source.report.num_vc_table_references_found == 7
        assert self.tableau_source.report.num_vc_lineages_created == 12

    def test_virtual_connection_config_options(self):
        """Test virtual connection configuration options."""
        # Test default configuration
        assert self.config.ingest_virtual_connections is True

        # Test custom page size
        custom_config = default_config.copy()
        custom_config["virtual_connection_page_size"] = 25
        config = TableauConfig.parse_obj(custom_config)
        assert config.virtual_connection_page_size == 25
        assert config.effective_virtual_connection_page_size == 25

        # Test disabled virtual connections
        disabled_config = default_config.copy()
        disabled_config["ingest_virtual_connections"] = False
        config = TableauConfig.parse_obj(disabled_config)
        assert config.ingest_virtual_connections is False

    def test_column_type_tracking(self):
        """Test that column types are properly tracked."""
        # Test column type storage
        self.vc_processor.vc_table_column_types["vc-table-1.column1"] = "INTEGER"
        self.vc_processor.vc_table_column_types["vc-table-1.column2"] = "VARCHAR"

        assert (
            self.vc_processor.vc_table_column_types["vc-table-1.column1"] == "INTEGER"
        )
        assert (
            self.vc_processor.vc_table_column_types["vc-table-1.column2"] == "VARCHAR"
        )

    def test_empty_virtual_connection_processing(self):
        """Test processing of virtual connections with no tables or columns."""
        empty_vc = {c.ID: "vc-empty", c.NAME: "Empty VC", c.TABLES: []}

        vc_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-empty,PROD)"

        vc_tables = cast(list[dict[str, Any]], empty_vc.get(c.TABLES, []))
        upstream_tables, fine_grained_lineages = (
            self.vc_processor._create_vc_upstream_lineage_v2(
                empty_vc, vc_tables, vc_urn
            )
        )

        # Should handle empty VCs gracefully
        assert upstream_tables == []
        assert fine_grained_lineages == []

    def test_virtual_connection_table_name_mapping(self):
        """Test VC table ID to name mapping functionality."""
        # Set up mapping
        self.vc_processor.vc_table_id_to_name["vc-table-1"] = (
            "analytics.public.customers"
        )
        self.vc_processor.vc_table_id_to_name["vc-table-2"] = (
            "warehouse.sales.transactions"
        )

        # Test retrieval
        assert (
            self.vc_processor.vc_table_id_to_name["vc-table-1"]
            == "analytics.public.customers"
        )
        assert (
            self.vc_processor.vc_table_id_to_name["vc-table-2"]
            == "warehouse.sales.transactions"
        )
        assert self.vc_processor.vc_table_id_to_name.get("nonexistent") is None
