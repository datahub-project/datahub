"""
Test incremental lineage functionality for Fivetran connector.

This module tests the incremental lineage feature which allows emitting
lineage as patches rather than full overwrites, enabling:
- Preservation of manually curated lineage
- Gradual building up lineage from multiple sources
- Avoiding conflicts when multiple ingestion processes target the same datasets
"""

from typing import List
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceConfig,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


class TestFivetranIncrementalLineage:
    """Test incremental lineage functionality."""

    def test_incremental_lineage_config_default_false(self):
        """Test that incremental_lineage defaults to False."""
        config = FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_key",
                api_secret="test_secret",
            )
        )
        assert config.incremental_lineage is False

    def test_incremental_lineage_config_can_be_enabled(self):
        """Test that incremental_lineage can be enabled."""
        config = FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_key",
                api_secret="test_secret",
            ),
            incremental_lineage=True,
        )
        assert config.incremental_lineage is True

    def test_workunit_processors_include_incremental_lineage(self):
        """Test that get_workunit_processors includes incremental lineage processor when enabled."""
        config = FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_key",
                api_secret="test_secret",
            ),
            incremental_lineage=True,
        )

        ctx = PipelineContext(run_id="test_run")
        source = FivetranSource(config, ctx)

        processors = source.get_workunit_processors()

        # Should have at least 3 processors: super().get_workunit_processors(),
        # incremental_lineage, and stale entity removal
        assert len(processors) >= 3

        # Check that one of the processors is the incremental lineage processor
        incremental_processor_found = False
        for processor in processors:
            if processor is not None:
                # Check if it's a partial function with auto_incremental_lineage
                if (
                    hasattr(processor, "func")
                    and processor.func == auto_incremental_lineage
                ):
                    incremental_processor_found = True
                    # Verify it's configured with the right setting
                    if hasattr(processor, "args"):
                        assert processor.args == (True,)  # incremental_lineage=True
                    break
                # Check if it's the function directly
                elif processor == auto_incremental_lineage:
                    incremental_processor_found = True
                    break

        assert incremental_processor_found, (
            "Incremental lineage processor not found in workunit processors"
        )

    def test_workunit_processors_exclude_incremental_lineage_when_disabled(self):
        """Test that incremental lineage processor uses False when disabled."""
        config = FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_key",
                api_secret="test_secret",
            ),
            incremental_lineage=False,
        )

        ctx = PipelineContext(run_id="test_run")
        source = FivetranSource(config, ctx)

        processors = source.get_workunit_processors()

        # Check that the incremental lineage processor is configured with False
        incremental_processor_found = False
        for processor in processors:
            if processor is not None:
                # Check if it's a partial function with auto_incremental_lineage
                if (
                    hasattr(processor, "func")
                    and processor.func == auto_incremental_lineage
                ):
                    incremental_processor_found = True
                    # Verify it's configured with the right setting
                    if hasattr(processor, "args"):
                        assert processor.args == (False,)  # incremental_lineage=False
                    break
                # Check if it's the function directly
                elif processor == auto_incremental_lineage:
                    incremental_processor_found = True
                    break

        assert incremental_processor_found, (
            "Incremental lineage processor not found in workunit processors"
        )

    def test_auto_incremental_lineage_converts_lineage_to_patch(self):
        """Test that auto_incremental_lineage converts UpstreamLineage to patch format."""
        # Create a mock workunit with UpstreamLineage
        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset="urn:li:dataset:(urn:li:dataPlatform:postgres,test.source_table,PROD)",
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ]
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dest_table,PROD)",
            aspect=upstream_lineage,
        )

        workunit = MetadataWorkUnit(
            id="test-workunit",
            mcp=mcp,
        )

        # Process through incremental lineage helper
        processed_workunits = list(auto_incremental_lineage(True, [workunit]))

        # Should get one workunit back (the patch)
        assert len(processed_workunits) == 1

        processed_wu = processed_workunits[0]
        assert processed_wu.id.endswith("-upstreamLineage")

        # Verify it's a patch operation
        assert hasattr(processed_wu.metadata, "changeType")
        assert processed_wu.metadata.changeType == "PATCH"
        assert hasattr(processed_wu.metadata, "aspectName")
        assert processed_wu.metadata.aspectName == "upstreamLineage"

    def test_auto_incremental_lineage_passthrough_when_disabled(self):
        """Test that auto_incremental_lineage passes through workunits when disabled."""
        # Create a mock workunit with UpstreamLineage
        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset="urn:li:dataset:(urn:li:dataPlatform:postgres,test.source_table,PROD)",
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ]
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dest_table,PROD)",
            aspect=upstream_lineage,
        )

        workunit = MetadataWorkUnit(
            id="test-workunit",
            mcp=mcp,
        )

        # Process through incremental lineage helper with disabled flag
        processed_workunits = list(auto_incremental_lineage(False, [workunit]))

        # Should get the same workunit back unchanged
        assert len(processed_workunits) == 1
        assert processed_workunits[0] is workunit  # Same object reference

    def test_incremental_lineage_integration_with_fivetran_source(self):
        """Test that FivetranSource properly integrates incremental lineage processing."""
        config = FivetranSourceConfig(
            api_config=FivetranAPIConfig(
                api_key="test_key",
                api_secret="test_secret",
            ),
            incremental_lineage=True,
        )

        ctx = PipelineContext(run_id="test_run")
        source = FivetranSource(config, ctx)

        # Mock the fivetran access to avoid actual API calls
        mock_access = MagicMock()
        mock_access.get_allowed_connectors_stream.return_value = []
        source.fivetran_access = mock_access

        # Get processors
        processors = source.get_workunit_processors()

        # Verify incremental lineage processor is present and properly configured
        incremental_processor = None
        for processor in processors:
            if processor is not None:
                if (
                    hasattr(processor, "func")
                    and processor.func == auto_incremental_lineage
                ) or processor == auto_incremental_lineage:
                    incremental_processor = processor
                    break

        assert incremental_processor is not None
        if hasattr(incremental_processor, "args"):
            assert incremental_processor.args == (True,)  # incremental_lineage=True

        # Test that the processor can be called
        mock_workunits: List[MetadataWorkUnit] = []
        result = list(incremental_processor(mock_workunits))
        assert result == mock_workunits  # Should pass through empty list
