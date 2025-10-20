from typing import cast
from unittest.mock import Mock

from acryl_datahub_cloud.lineage_features.source import (
    DataHubLineageFeaturesSource,
    LineageFeaturesSourceConfig,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import LineageFeaturesClass


class TestLineageFeaturesSource:
    """Test the lineage features source."""

    def test_emit_lineage_features(self) -> None:
        """Test that lineage features are emitted correctly."""
        config = LineageFeaturesSourceConfig()
        ctx = Mock(spec=PipelineContext)
        source = DataHubLineageFeaturesSource(config, ctx)

        # Set up test data
        source.valid_urns = {
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset2,PROD)",
        }
        source.upstream_counts = {
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset1,PROD)": 2,
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset2,PROD)": 0,
        }
        source.downstream_counts = {
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset1,PROD)": 1,
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset2,PROD)": 3,
        }

        # Get workunits
        workunits = list(source._emit_lineage_features())

        # Should emit 2 workunits (one for each dataset)
        assert len(workunits) == 2

        # Check first workunit
        wu1 = workunits[0]
        wu2 = workunits[1]

        metadata1_cast = cast(MetadataChangeProposalWrapper, wu1.metadata)
        metadata2_cast = cast(MetadataChangeProposalWrapper, wu2.metadata)
        assert set([metadata1_cast.entityUrn, metadata2_cast.entityUrn]) == {
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,dataset2,PROD)",
        }

        metadata1 = cast(MetadataChangeProposalWrapper, wu1.metadata)
        aspect1 = cast(LineageFeaturesClass, metadata1.aspect)
        metadata2 = cast(MetadataChangeProposalWrapper, wu2.metadata)
        aspect2 = cast(LineageFeaturesClass, metadata2.aspect)
        assert {
            (
                aspect1.upstreamCount,
                aspect1.downstreamCount,
                aspect1.hasAssetLevelLineage,
            ),
            (
                aspect2.upstreamCount,
                aspect2.downstreamCount,
                aspect2.hasAssetLevelLineage,
            ),
        } == {
            (2, 1, True),
            (0, 3, True),
        }

    def test_cleanup_old_features(self) -> None:
        """Test that old lineage features are cleaned up correctly."""
        config = LineageFeaturesSourceConfig(cleanup_old_features_days=2)

        # Mock the graph context
        mock_graph = Mock()
        mock_graph.get_urns_by_filter.return_value = [
            "urn:li:dataset:(urn:li:dataPlatform:test,old_dataset1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,old_dataset2,PROD)",
        ]

        ctx = Mock(spec=PipelineContext)
        ctx.require_graph.return_value = mock_graph

        source = DataHubLineageFeaturesSource(config, ctx)

        # Get cleanup workunits
        workunits = list(source._cleanup_old_features())

        # Should emit 2 cleanup workunits
        assert len(workunits) == 2

        # Verify graph filter was called correctly
        mock_graph.get_urns_by_filter.assert_called_once()
        call_args = mock_graph.get_urns_by_filter.call_args
        assert call_args[1]["batch_size"] == config.cleanup_batch_size

        # Check cleanup workunits have zero counts and false hasAssetLevelLineage
        for wu in workunits:
            metadata = cast(MetadataChangeProposalWrapper, wu.metadata)
            aspect = cast(LineageFeaturesClass, metadata.aspect)
            assert aspect.upstreamCount == 0
            assert aspect.downstreamCount == 0
            assert aspect.hasAssetLevelLineage is False
            assert aspect.computedAt is not None
            assert aspect.computedAt.actor == "urn:li:corpuser:__datahub_system"
