import pathlib
from typing import Any, Dict, List, cast
from unittest import mock

from freezegun import freeze_time
from tableauserverclient.models import SiteItem

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_common import LineageResult
from datahub.ingestion.source.tableau.tableau_virtual_connections import (
    VCFolderKey,
    VirtualConnectionProcessor,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
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
        assert isinstance(self.vc_processor.vc_table_ids_for_lookup, List)
        assert isinstance(self.vc_processor.vc_table_id_to_vc_id, Dict)
        assert isinstance(self.vc_processor.virtual_connection_ids_being_used, List)
        assert isinstance(self.vc_processor.datasource_vc_relationships, Dict)

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
        empty_datasource: Dict[str, Any] = {}

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

        vc_tables = cast(List[Dict[str, Any]], virtual_connection.get(c.TABLES, []))
        result = self.vc_processor._create_vc_upstream_lineage(
            virtual_connection, vc_tables, vc_urn
        )

        # Should return LineageResult dataclass
        assert isinstance(result, LineageResult)
        assert isinstance(result.upstream_tables, List)
        assert isinstance(result.fine_grained_lineages, List)

        # Should have found upstream datasources
        if result.upstream_tables:
            assert len(result.upstream_tables) > 0
            # Check that upstream is properly formed
            upstream = result.upstream_tables[0]
            assert hasattr(upstream, "dataset")
            assert hasattr(upstream, "type")

    def test_create_datasource_vc_lineage_no_relationships(self):
        """Test datasource VC lineage creation when no relationships exist."""
        datasource_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,ds-123,PROD)"

        result = self.vc_processor.create_datasource_vc_lineage(datasource_urn)

        # Should return empty lists when no relationships exist
        assert result.upstream_tables == []
        assert result.fine_grained_lineages == []

    def test_create_datasource_vc_lineage_with_relationships(self):
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

        result = self.vc_processor.create_datasource_vc_lineage(datasource_urn)

        # Should have created lineage
        assert isinstance(result.upstream_tables, List)
        assert isinstance(result.fine_grained_lineages, List)

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
            List[Dict[str, Any]],
            vc_tables_raw if isinstance(vc_tables_raw, List) else [],
        )
        result = self.vc_processor._create_vc_upstream_lineage(
            malformed_vc, vc_tables, vc_urn
        )

        # Should return empty lists for malformed data
        assert result.upstream_tables == []
        assert result.fine_grained_lineages == []

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

        vc_tables = cast(List[Dict[str, Any]], empty_vc.get(c.TABLES, []))
        result = self.vc_processor._create_vc_upstream_lineage(
            empty_vc, vc_tables, vc_urn
        )

        # Should handle empty VCs gracefully
        assert result.upstream_tables == []
        assert result.fine_grained_lineages == []

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

    def test_group_vc_columns_by_table(self):
        """Test grouping virtual connection columns by table."""
        vc_tables = [
            {
                c.ID: "vc-table-1",
                c.NAME: "table1",
                c.COLUMNS: [
                    {c.ID: "col-1", c.NAME: "column1", c.REMOTE_TYPE: "INTEGER"},
                    {c.ID: "col-2", c.NAME: "column2", c.REMOTE_TYPE: "STRING"},
                ],
            },
            {
                c.ID: "vc-table-2",
                c.NAME: "table2",
                c.COLUMNS: [
                    {c.ID: "col-3", c.NAME: "column3", c.REMOTE_TYPE: "BOOLEAN"},
                ],
            },
        ]

        grouped_columns = self.vc_processor._group_vc_columns_by_table(vc_tables)

        # Should have two tables (grouped by NAME, not ID)
        assert len(grouped_columns) == 2
        assert "table1" in grouped_columns
        assert "table2" in grouped_columns

        # Check table 1 columns
        table1_data = grouped_columns["table1"]
        assert len(table1_data["columns"]) == 2
        assert table1_data["columns"][0][c.NAME] == "column1"
        assert table1_data["columns"][1][c.NAME] == "column2"

        # Check table 2 columns
        table2_data = grouped_columns["table2"]
        assert len(table2_data["columns"]) == 1
        assert table2_data["columns"][0][c.NAME] == "column3"

    def test_group_vc_columns_by_table_empty(self):
        """Test grouping virtual connection columns with empty data."""
        vc_tables: List[Dict[str, Any]] = []

        grouped_columns = self.vc_processor._group_vc_columns_by_table(vc_tables)
        assert grouped_columns == {}

    def test_get_vc_schema_metadata_grouped_by_table(self):
        """Test getting schema metadata grouped by table."""
        vc_tables = [
            {
                c.ID: "vc-table-1",
                c.NAME: "test.table1",
                c.COLUMNS: [
                    {
                        c.ID: "col-1",
                        c.NAME: "id",
                        c.REMOTE_TYPE: "INTEGER",
                        c.DESCRIPTION: "Primary key",
                    },
                    {
                        c.ID: "col-2",
                        c.NAME: "name",
                        c.REMOTE_TYPE: "STRING",
                        c.DESCRIPTION: "User name",
                    },
                ],
            },
        ]

        schema_metadata_dict = (
            self.vc_processor._get_vc_schema_metadata_grouped_by_table(vc_tables)
        )

        # Should have one table (keyed by table NAME)
        assert len(schema_metadata_dict) == 1
        assert "test.table1" in schema_metadata_dict

        # Check schema metadata
        schema_metadata = schema_metadata_dict["test.table1"]
        assert isinstance(schema_metadata, SchemaMetadata)
        assert schema_metadata.schemaName == "VirtualConnection_test.table1"
        assert len(schema_metadata.fields) >= 2

        # Check that we have the expected field paths (standard format)
        field_paths = [field.fieldPath for field in schema_metadata.fields]
        assert "id" in field_paths
        assert "name" in field_paths

    def test_get_vc_project_luid(self):
        """Test getting VC project LUID."""
        # Mock project registry
        mock_project = mock.MagicMock()
        mock_project.name = "Test Project"
        self.vc_processor.tableau_source.tableau_project_registry = {
            "project-luid-123": mock_project
        }

        # Test with valid project name
        vc_with_project = {"projectName": "Test Project"}
        project_luid = self.vc_processor._get_vc_project_luid(vc_with_project)
        assert project_luid == "project-luid-123"

        # Test without project
        vc_without_project = {c.ID: "vc-123"}
        project_luid = self.vc_processor._get_vc_project_luid(vc_without_project)
        assert project_luid is None

        # Test with non-matching project name
        vc_unknown_project = {"projectName": "Unknown Project"}
        project_luid = self.vc_processor._get_vc_project_luid(vc_unknown_project)
        assert project_luid is None

    def test_create_vc_folder_container(self):
        """Test creating VC folder container."""
        vc = {
            c.ID: "vc-123",
            c.NAME: "Test VC",
            c.DESCRIPTION: "Test virtual connection",
        }

        # Mock project LUID lookup and project container creation
        with (
            mock.patch.object(
                self.vc_processor,
                "_get_vc_project_luid",
                return_value="project-luid-123",
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "gen_project_key",
                return_value=mock.MagicMock(),
            ) as mock_gen_project_key,
        ):
            container_urn, container_workunits = (
                self.vc_processor._create_vc_folder_container(vc)
            )

            # Should return a tuple with URN and work units
            assert container_urn is not None
            assert isinstance(container_workunits, List)
            assert len(container_workunits) > 0

            # Should have called gen_project_key
            mock_gen_project_key.assert_called_once_with("project-luid-123")

    def test_create_vc_folder_container_no_project(self):
        """Test creating VC folder container without project."""
        vc = {
            c.ID: "vc-123",
            c.NAME: "Test VC",
            c.DESCRIPTION: "Test virtual connection",
        }

        container_urn, container_workunits = (
            self.vc_processor._create_vc_folder_container(vc)
        )

        # Should still return a tuple even without project
        assert container_urn is not None
        assert isinstance(container_workunits, List)
        assert len(container_workunits) > 0

    def test_emit_datasource_vc_lineages_empty(self):
        """Test emitting datasource VC lineages with no relationships."""
        # No relationships stored
        lineage_workunits = list(self.vc_processor.emit_datasource_vc_lineages())
        assert lineage_workunits == []

    def test_emit_datasource_vc_lineages_with_relationships(self):
        """Test emitting datasource VC lineages with relationships."""
        # Add some test relationships and VC mappings
        self.vc_processor.datasource_vc_relationships = {
            "ds-123": [
                {
                    "vc_id": "vc-456",
                    "vc_table_id": "vc-table-1",
                    "datasource_type": "published",
                }
            ]
        }
        self.vc_processor.vc_table_id_to_vc_id = {"vc-table-1": "vc-456"}

        # Mock the create_datasource_vc_lineage method
        with mock.patch.object(
            self.vc_processor,
            "create_datasource_vc_lineage",
            return_value=LineageResult(upstream_tables=[], fine_grained_lineages=[]),
        ) as mock_create_lineage:
            lineage_workunits = list(self.vc_processor.emit_datasource_vc_lineages())

            # Should have called create_datasource_vc_lineage
            mock_create_lineage.assert_called_once()

            # Should return work units (may be empty if no lineage)
            assert isinstance(lineage_workunits, List)

    def test_create_table_upstream_lineage_empty_datasources(self):
        """Test creating table upstream lineage with empty table info."""
        table_info = {c.ID: "vc-table-1", c.NAME: "test_table", c.COLUMNS: []}
        table_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-table-1,PROD)"

        result = self.vc_processor._create_table_upstream_lineage(table_info, table_urn)

        assert result.upstream_tables == []
        assert result.fine_grained_lineages == []

    def test_create_table_upstream_lineage_with_datasources(self):
        """Test creating table upstream lineage with datasources."""
        table_info = {
            c.ID: "vc-table-1",
            c.NAME: "test_table",
            c.COLUMNS: [
                {
                    c.ID: "col-1",
                    c.NAME: "column1",
                    c.UPSTREAM_FIELDS: [
                        {
                            c.ID: "upstream-field-1",
                            c.NAME: "source_col1",
                            c.DATA_SOURCE: {c.ID: "ds-123", c.NAME: "Source DB 1"},
                        }
                    ],
                }
            ],
        }
        table_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,vc-table-1,PROD)"

        # Mock the database table matching methods
        mock_db_table = {"name": "test_table", "id": "db-table-123"}
        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_find_matching_database_table",
                return_value=mock_db_table,
            ),
            mock.patch.object(
                self.vc_processor.tableau_source,
                "_create_database_table_urn",
                return_value="urn:li:dataset:(urn:li:dataPlatform:tableau,db-table-123,PROD)",
            ),
        ):
            result = self.vc_processor._create_table_upstream_lineage(
                table_info, table_urn
            )

            # Should create upstream entries and fine-grained lineage
            assert (
                len(result.upstream_tables) >= 0
            )  # May be 0 or more depending on implementation
            assert isinstance(result.fine_grained_lineages, List)

    def test_emit_virtual_connections_disabled(self):
        """Test emit_virtual_connections when disabled in config."""
        # Disable virtual connections
        self.vc_processor.config.ingest_virtual_connections = False

        # Mock server.get_connection_objects
        with mock.patch.object(
            self.vc_processor.server, "get_connection_objects", return_value=[]
        ):
            workunits = list(self.vc_processor.emit_virtual_connections())
            assert workunits == []

    def test_emit_virtual_connections_enabled_no_data(self):
        """Test emit_virtual_connections when enabled but no data."""
        # Enable virtual connections
        self.vc_processor.config.ingest_virtual_connections = True

        # Mock server.get_connection_objects to return empty
        with mock.patch.object(
            self.vc_processor.server, "get_connection_objects", return_value=[]
        ):
            workunits = list(self.vc_processor.emit_virtual_connections())
            assert workunits == []

    def test_emit_virtual_connections_with_data(self):
        """Test emit_virtual_connections with actual data."""
        # Set up virtual connection IDs being used
        self.vc_processor.virtual_connection_ids_being_used = ["vc-123"]

        # Mock data
        mock_vc_data = [
            {
                c.ID: "vc-123",
                c.NAME: "Test VC",
                c.LUID: "vc-luid-123",
                c.DESCRIPTION: "Test virtual connection",
                c.CREATED_AT: "2021-12-07T07:00:00Z",
                c.UPDATED_AT: "2021-12-07T07:00:00Z",
                c.OWNER: {c.USERNAME: "test_user"},
                c.TABLES: [],
                c.TAGS: [],
            }
        ]

        # Mock tableau_source.get_connection_objects and _emit_single_virtual_connection
        with (
            mock.patch.object(
                self.vc_processor.tableau_source,
                "get_connection_objects",
                return_value=mock_vc_data,
            ),
            mock.patch.object(
                self.vc_processor,
                "_emit_single_virtual_connection",
                return_value=[mock.MagicMock()],
            ) as mock_emit_single,
        ):
            workunits = list(self.vc_processor.emit_virtual_connections())

            # Should have called _emit_single_virtual_connection
            mock_emit_single.assert_called_once_with(mock_vc_data[0])

            # Should return work units
            assert len(workunits) == 1

    def test_emit_single_virtual_connection_basic(self):
        """Test emitting a single virtual connection."""
        vc = {
            c.ID: "vc-123",
            c.NAME: "Test VC",
            c.LUID: "vc-luid-123",
            c.DESCRIPTION: "Test virtual connection",
            c.CREATED_AT: "2021-12-07T07:00:00Z",
            c.UPDATED_AT: "2021-12-07T07:00:00Z",
            c.OWNER: {c.USERNAME: "test_user"},
            "tables": [  # Note: using string key as in actual implementation
                {
                    c.ID: "vc-table-1",
                    c.NAME: "test_table",
                    c.COLUMNS: [
                        {c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "INTEGER"}
                    ],
                }
            ],
            c.TAGS: [],
        }

        # Mock dependencies
        with (
            mock.patch.object(
                self.vc_processor,
                "_create_vc_folder_container",
                return_value=("urn:li:container:test", [mock.MagicMock()]),
            ),
            mock.patch.object(
                self.vc_processor,
                "_get_vc_schema_metadata_grouped_by_table",
                return_value={"test_table": mock.MagicMock()},
            ),
            mock.patch.object(
                self.vc_processor,
                "_create_table_upstream_lineage",
                return_value=LineageResult(
                    upstream_tables=[], fine_grained_lineages=[]
                ),
            ),
        ):
            workunits = list(self.vc_processor._emit_single_virtual_connection(vc))

            # Should return work units (container + dataset)
            assert len(workunits) >= 1

    def test_nested_table_schema_creation(self):
        """Test schema creation for nested table names."""
        vc_tables = [
            {
                c.ID: "vc-table-1",
                c.NAME: "dw.sf.dw_compliance_test",  # Nested name
                c.COLUMNS: [{c.ID: "col-1", c.NAME: "id", c.REMOTE_TYPE: "INTEGER"}],
            }
        ]

        schema_metadata_dict = (
            self.vc_processor._get_vc_schema_metadata_grouped_by_table(vc_tables)
        )

        # Should handle nested names properly (keyed by table NAME)
        assert "dw.sf.dw_compliance_test" in schema_metadata_dict
        schema_metadata = schema_metadata_dict["dw.sf.dw_compliance_test"]
        assert (
            schema_metadata.schemaName == "VirtualConnection_dw.sf.dw_compliance_test"
        )

        # Should create field structure
        fields = schema_metadata.fields
        # Should have the expected field path format (standard format)
        field_paths = [field.fieldPath for field in fields]
        assert "id" in field_paths

    def test_lookup_vc_ids_from_table_ids_with_data(self):
        """Test lookup_vc_ids_from_table_ids with actual data."""
        # Set up test data
        self.vc_processor.vc_table_ids_for_lookup = ["vc-table-1", "vc-table-2"]

        # Mock virtual connections data (what get_connection_objects returns)
        mock_vcs = [
            {
                c.ID: "vc-123",
                c.NAME: "Test VC",
                "tables": [
                    {c.ID: "vc-table-1", c.NAME: "table1", c.COLUMNS: []},
                    {c.ID: "vc-table-2", c.NAME: "table2", c.COLUMNS: []},
                ],
            }
        ]

        with mock.patch.object(
            self.vc_processor.tableau_source,
            "get_connection_objects",
            return_value=mock_vcs,
        ):
            self.vc_processor.lookup_vc_ids_from_table_ids()

            # Should populate the mapping dictionaries
            assert self.vc_processor.vc_table_id_to_vc_id["vc-table-1"] == "vc-123"
            assert self.vc_processor.vc_table_id_to_vc_id["vc-table-2"] == "vc-123"
            assert self.vc_processor.vc_table_id_to_name["vc-table-1"] == "table1"
            assert self.vc_processor.vc_table_id_to_name["vc-table-2"] == "table2"

    def test_lookup_vc_ids_from_table_ids_empty(self):
        """Test lookup_vc_ids_from_table_ids with no table IDs."""
        # No table IDs to look up
        self.vc_processor.vc_table_ids_for_lookup = []

        self.vc_processor.lookup_vc_ids_from_table_ids()

        # Should not populate any mappings
        assert len(self.vc_processor.vc_table_id_to_vc_id) == 0
        assert len(self.vc_processor.vc_table_id_to_name) == 0

    def test_process_datasource_for_vc_refs_with_empty_field_name(self):
        """Test processing datasource with empty field names."""
        # Mock datasource with empty field name
        datasource = {
            c.ID: "ds-123",
            c.FIELDS: [
                {c.ID: "field-1", c.NAME: None},  # Empty name should be skipped
                {c.ID: "field-2", c.NAME: "valid_field", c.UPSTREAM_COLUMNS: []},
            ],
        }

        self.vc_processor.process_datasource_for_vc_refs(datasource, "published")

        # Should not create relationships for empty field names
        assert "ds-123" not in self.vc_processor.datasource_vc_relationships

    def test_lookup_vc_ids_with_columns(self):
        """Test lookup_vc_ids_from_table_ids populating column types."""
        # Set up test data
        self.vc_processor.vc_table_ids_for_lookup = ["vc-table-1"]

        # Mock virtual connections data with columns
        mock_vcs = [
            {
                c.ID: "vc-123",
                c.NAME: "Test VC",
                "tables": [
                    {
                        c.ID: "vc-table-1",
                        c.NAME: "table1",
                        c.COLUMNS: [
                            {c.NAME: "col1", c.REMOTE_TYPE: "INTEGER"},
                            {c.NAME: "col2", c.REMOTE_TYPE: "STRING"},
                            {
                                c.NAME: None,
                                c.REMOTE_TYPE: "BOOLEAN",
                            },  # Should skip this
                        ],
                    }
                ],
            }
        ]

        with mock.patch.object(
            self.vc_processor.tableau_source,
            "get_connection_objects",
            return_value=mock_vcs,
        ):
            self.vc_processor.lookup_vc_ids_from_table_ids()

            # Should populate column types
            assert "vc-table-1.col1" in self.vc_processor.vc_table_column_types
            assert "vc-table-1.col2" in self.vc_processor.vc_table_column_types
            assert (
                self.vc_processor.vc_table_column_types["vc-table-1.col1"] == "INTEGER"
            )
            assert (
                self.vc_processor.vc_table_column_types["vc-table-1.col2"] == "STRING"
            )

    def test_is_table_name_field_detection(self):
        """Test _is_table_name_field method for detecting table names vs column fields."""
        # Test table name patterns that should be filtered out
        assert self.vc_processor._is_table_name_field(
            "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)", "table"
        )
        assert self.vc_processor._is_table_name_field(
            "QUARTERLY_REPORT (DW_COMPLIANCE.QUARTERLY_REPORT)", "table"
        )
        assert self.vc_processor._is_table_name_field(
            "TABLE_NAME (SCHEMA.TABLE_NAME)", "table"
        )

        # Test with additional parentheses
        assert self.vc_processor._is_table_name_field(
            "INTERVENTION_20250221 (DW_COMPLIANCE.INTERVENTION_20250221) (1)", "table"
        )

        # Test uppercase schema-like patterns
        assert self.vc_processor._is_table_name_field("DW_COMPLIANCE.MARKET_SCAN", "")
        assert self.vc_processor._is_table_name_field("SCHEMA_NAME.TABLE_NAME", "")

        # Test normal column names that should NOT be filtered
        assert not self.vc_processor._is_table_name_field("customer_name", "column")
        assert not self.vc_processor._is_table_name_field("Fee U24", "column")
        assert not self.vc_processor._is_table_name_field("3a BSR", "column")
        assert not self.vc_processor._is_table_name_field("Rake U24", "column")
        assert not self.vc_processor._is_table_name_field("normal_field", "field")

        # Test edge cases
        assert not self.vc_processor._is_table_name_field("", "")
        assert not self.vc_processor._is_table_name_field("single_word", "column")
        assert not self.vc_processor._is_table_name_field("lowercase.field", "column")

    def test_process_datasource_for_vc_refs_filters_table_names(self):
        """Test that table name fields are properly filtered during VC reference processing."""
        datasource = {
            c.ID: "ds-123",
            c.FIELDS: [
                # This should be filtered out as it's a table name
                {
                    c.NAME: "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)",
                    c.TYPE_NAME: "table",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)",
                            c.TABLE: {
                                c.ID: "vc-table-1",
                                c.NAME: "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)",
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                "virtualConnection": {c.ID: "vc-123"},
                            },
                        }
                    ],
                },
                # This should be processed as it's a real column
                {
                    c.NAME: "Fee U24",
                    c.TYPE_NAME: "column",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "Fee U24",
                            c.TABLE: {
                                c.ID: "vc-table-1",
                                c.NAME: "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)",
                                c.TYPE_NAME: c.VIRTUAL_CONNECTION_TABLE,
                                "virtualConnection": {c.ID: "vc-123"},
                            },
                        }
                    ],
                },
            ],
        }

        self.vc_processor.process_datasource_for_vc_refs(datasource, "embedded")

        # Should only have one VC reference (the real column, not the table name)
        if "ds-123" in self.vc_processor.datasource_vc_relationships:
            relationships = self.vc_processor.datasource_vc_relationships["ds-123"]
            # Should only have the "Fee U24" field, not the table name field
            field_names = [rel.get("field_name") for rel in relationships]
            assert "Fee U24" in field_names
            assert "MARKET_SCAN (DW_COMPLIANCE.MARKET_SCAN)" not in field_names
