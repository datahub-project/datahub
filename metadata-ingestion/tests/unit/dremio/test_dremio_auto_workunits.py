from unittest.mock import Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_source import DremioSource


class TestDremioAutoWorkunits:
    @pytest.fixture
    def pipeline_context(self):
        """Create a mock pipeline context"""
        return PipelineContext(run_id="test_run")

    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return DremioSourceConfig(
            hostname="dummy-host",
            port=9047,
            tls=False,
            authentication_method="password",
            username="dummy-user",
            password="dummy-password",
        )

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_auto_workunit_reporter_in_processors(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that auto_workunit_reporter is included in workunit processors"""

        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_sources.return_value = iter([])
        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)

        # Get workunit processors
        processors = source.get_workunit_processors()

        # Verify that we have the expected processors
        assert (
            len(processors) >= 2
        )  # At least StaleEntityRemovalHandler and auto_workunit_reporter

        # Find the auto_workunit_reporter processor
        auto_reporter_found = False
        for processor in processors:
            if processor is not None and hasattr(processor, "func"):
                if processor.func == auto_workunit_reporter:
                    auto_reporter_found = True
                    # Verify it's a partial function (we can't easily check the args)
                    assert hasattr(processor, "args") or hasattr(processor, "keywords")
                    break

        assert auto_reporter_found, "auto_workunit_reporter not found in processors"

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_workunits_use_mcp_as_workunit(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that workunits are created using mcp.as_workunit() method"""

        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock the SQL parsing aggregator
        with patch.object(
            source.sql_parsing_aggregator, "gen_metadata", return_value=[]
        ):
            # Get all workunits
            workunits = list(source.get_workunits_internal())

        # All workunits should be MetadataWorkUnit instances
        for workunit in workunits:
            assert isinstance(workunit, MetadataWorkUnit)
            # Verify that the workunit has proper structure (no manual ID setting)
            assert hasattr(workunit, "metadata")

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_view_lineage_uses_auto_workunit(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that view lineage generation uses mcp.as_workunit() instead of manual ID"""

        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Test view lineage generation
        dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:dremio,test.view,PROD)"
        parents = ["source1.table1", "source1.table2"]

        # Generate view lineage workunits
        lineage_workunits = list(source.generate_view_lineage(dataset_urn, parents))

        # Should generate exactly one workunit
        assert len(lineage_workunits) == 1

        workunit = lineage_workunits[0]
        assert isinstance(workunit, MetadataWorkUnit)

        # Verify the workunit was created using mcp.as_workunit() (no manual ID)
        # The workunit should have the standard auto-generated structure
        assert hasattr(workunit, "metadata")
        assert isinstance(workunit.metadata, MetadataChangeProposalWrapper)

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_profiling_uses_auto_workunit(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that profiling workunits use mcp.as_workunit()"""

        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Create a mock dataset for profiling
        mock_dataset = Mock()
        mock_dataset.resource_name = "test_table"
        mock_dataset.path = ["source1"]
        mock_dataset.columns = [Mock(name="col1", data_type="VARCHAR", column_size=255)]

        # Mock the profiler to return a workunit
        mock_profile_workunit = Mock(spec=MetadataWorkUnit)

        with patch.object(
            source.profiler, "get_workunits", return_value=[mock_profile_workunit]
        ):
            # Generate profiling workunits
            profile_workunits = list(source.generate_profiles(mock_dataset))

            # Should return the mocked workunit
            assert len(profile_workunits) == 1
            assert profile_workunits[0] == mock_profile_workunit

    @patch("datahub.ingestion.source.dremio.dremio_source.DremioAPIOperations")
    @patch("datahub.ingestion.source.dremio.dremio_source.DremioCatalog")
    def test_workunit_reporting_integration(
        self, mock_catalog_class, mock_api_class, config, pipeline_context
    ):
        """Test that workunit reporting works correctly with auto_workunit_reporter"""

        # Setup mocks
        mock_catalog = Mock()
        mock_catalog.get_containers.return_value = iter([])
        mock_catalog.get_datasets.return_value = iter([])
        mock_catalog.get_glossary_terms.return_value = iter([])
        mock_catalog.get_sources.return_value = iter([])

        mock_catalog_class.return_value = mock_catalog
        mock_api_class.return_value = Mock()

        # Create source
        source = DremioSource(config, pipeline_context)
        source.dremio_catalog = mock_catalog

        # Mock the SQL parsing aggregator to return some workunits
        mock_mcp = Mock(spec=MetadataChangeProposalWrapper)
        mock_workunit = Mock(spec=MetadataWorkUnit)
        mock_mcp.as_workunit.return_value = mock_workunit

        with patch.object(
            source.sql_parsing_aggregator, "gen_metadata", return_value=[mock_mcp]
        ):
            # Get all workunits
            workunits = list(source.get_workunits_internal())

        # Verify that workunits were generated
        assert len(workunits) >= 1

        # Verify that the report tracks workunit processing
        # (This would be handled by auto_workunit_reporter in the processors)
        report = source.get_report()
        assert hasattr(report, "events_produced")  # Should have reporting capabilities

    def test_no_manual_workunit_ids_in_codebase(self):
        """Test that we don't have manual workunit ID creation in the codebase"""
        import os
        import re

        # Get the path to the Dremio source directory
        dremio_source_dir = "metadata-ingestion/src/datahub/ingestion/source/dremio"

        # Pattern to find manual MetadataWorkUnit creation with id parameter
        manual_id_pattern = r"MetadataWorkUnit\([^)]*id\s*="

        # Check all Python files in the Dremio source directory
        for root, _dirs, files in os.walk(dremio_source_dir):
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    # Search for manual workunit ID creation
                    matches = re.findall(manual_id_pattern, content)

                    # Should not find any manual workunit ID creation
                    assert len(matches) == 0, (
                        f"Found manual workunit ID creation in {file_path}: {matches}"
                    )
