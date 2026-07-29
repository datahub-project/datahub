"""Tests for the migration engine and CLI orchestration."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.metadata.schema_classes import DataPlatformInstanceClass
from datahub.migration import engine
from datahub.migration.models import (
    ConflictStrategy,
    MergeResult,
    MigrationOptions,
    MigrationPair,
    MigrationReport,
)
from datahub.utilities.urns.urn import guess_entity_type

SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,old.db.table,PROD)"
DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,new.db.table,PROD)"


def _instance() -> DataPlatformInstanceClass:
    return DataPlatformInstanceClass(
        platform=make_data_platform_urn("snowflake"),
        instance=make_dataplatform_instance_urn("snowflake", "new"),
    )


def _options(**overrides: object) -> MigrationOptions:
    defaults: dict = dict(
        run_id="test-run", dry_run=True, hard=False, keep=True, on_conflict=None
    )
    defaults.update(overrides)
    return MigrationOptions(**defaults)  # type: ignore[arg-type]


# --- guess_entity_type ---


class TestGuessEntityType:
    def test_extracts_dataset(self) -> None:
        """A dataset URN resolves to the 'dataset' entity type."""
        assert (
            guess_entity_type(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)"
            )
            == "dataset"
        )

    def test_extracts_chart(self) -> None:
        """A chart URN resolves to the 'chart' entity type."""
        assert guess_entity_type("urn:li:chart:(powerbi,my_chart)") == "chart"

    def test_extracts_container(self) -> None:
        """A container URN resolves to the 'container' entity type."""
        assert guess_entity_type("urn:li:container:abc123") == "container"


# --- engine.migrate_pair ---


class TestMigratePair:
    """Tests for the single-pair migration path."""

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_dry_run_does_not_emit(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """A dry-run migration emits nothing to the graph."""
        graph = MagicMock()
        graph.exists.return_value = False
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
            _options(dry_run=True),
            MigrationReport("test-run", dry_run=True, keep=True),
        )
        graph.emit_mcp.assert_not_called()

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_emits_platform_instance_when_provided(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """A non-dry-run pair carrying a dataPlatformInstance stamps it on the target."""
        graph = MagicMock()
        graph.exists.return_value = False
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
            _options(dry_run=False),
            MigrationReport("test-run", dry_run=False, keep=True),
        )
        graph.emit_mcp.assert_called()

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_no_instance_aspect_when_pair_has_none(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """A urns-mapping style pair with no dataPlatformInstance emits nothing when
        there are no aspects to clone (no instance aspect is synthesized)."""
        graph = MagicMock()
        graph.exists.return_value = False
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN),
            _options(dry_run=False),
            MigrationReport("test-run", dry_run=False, keep=True),
        )
        graph.emit_mcp.assert_not_called()

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch(
        "datahub.cli.migration_utils.merge_entity",
        return_value=MergeResult(merged=3, skipped=1),
    )
    def test_merge_path_when_target_exists(
        self, mock_merge: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """When the target already exists, the pair is routed through merge_entity
        and its merged/skipped counts flow into the report."""
        graph = MagicMock()
        graph.exists.return_value = True
        report = MigrationReport("test-run", dry_run=True, keep=True)
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
            _options(dry_run=True, on_conflict=ConflictStrategy.PATCH),
            report,
        )
        mock_merge.assert_called_once()
        assert report.aspects_merged == 3
        assert report.conflicts_skipped == 1

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.merge_entity")
    def test_preserve_skips_merge_but_repoints_and_deletes(
        self, mock_merge: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """preserve leaves an existing target untouched (no merge) but still deletes
        the source."""
        graph = MagicMock()
        graph.exists.return_value = True
        report = MigrationReport("test-run", dry_run=False, keep=False)
        with patch("datahub.cli.delete_cli._delete_one_urn") as mock_delete:
            engine.migrate_pair(
                graph,
                MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
                _options(
                    dry_run=False, keep=False, on_conflict=ConflictStrategy.PRESERVE
                ),
                report,
            )
        mock_merge.assert_not_called()
        assert report.conflicts_skipped == 1
        mock_delete.assert_called_once()

    @patch("datahub.cli.delete_cli._delete_one_urn")
    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_deletes_source_when_not_keep(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock, mock_delete: MagicMock
    ) -> None:
        """Without --keep, the source is soft-deleted after migration."""
        graph = MagicMock()
        graph.exists.return_value = False
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
            _options(dry_run=False, keep=False),
            MigrationReport("test-run", dry_run=False, keep=False),
        )
        mock_delete.assert_called_once_with(
            graph, SRC_URN, soft=True, run_id="test-run"
        )

    @patch("datahub.cli.delete_cli._delete_one_urn")
    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_skips_delete_on_dry_run(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock, mock_delete: MagicMock
    ) -> None:
        """A dry run never deletes the source, even without --keep."""
        graph = MagicMock()
        graph.exists.return_value = False
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN, data_platform_instance=_instance()),
            _options(dry_run=True, keep=False),
            MigrationReport("test-run", dry_run=True, keep=False),
        )
        mock_delete.assert_not_called()

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    def test_cross_entity_type_pair_raises(
        self, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """Migrating between different entity types is rejected up front."""
        graph = MagicMock()
        with pytest.raises(ValueError, match="same entity type"):
            engine.migrate_pair(
                graph,
                MigrationPair(SRC_URN, "urn:li:chart:(powerbi,c)"),
                _options(),
                MigrationReport("test-run", dry_run=True, keep=True),
            )


# --- engine.migrate_pairs (skip-on-error behavior) ---


class TestMigratePairs:
    """Tests for the batch orchestrator."""

    PAIRS = [
        MigrationPair(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t1,PROD)",
        ),
        MigrationPair(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t2,PROD)",
        ),
    ]

    @patch("datahub.migration.engine.migrate_pair")
    def test_skip_on_error_continues(self, mock_single: MagicMock) -> None:
        """With skip_on_error, a failing pair is recorded and the batch continues."""
        mock_single.side_effect = [RuntimeError("boom"), None]
        report = engine.migrate_pairs(
            MagicMock(), self.PAIRS, _options(skip_on_error=True)
        )
        assert len(report.entities_errored) == 1
        assert report.entities_errored[0][0] == self.PAIRS[0].source_urn
        assert mock_single.call_count == 2

    @patch("datahub.migration.engine.migrate_pair")
    def test_raises_without_skip_on_error(self, mock_single: MagicMock) -> None:
        """Without skip_on_error, a failing pair aborts the batch."""
        mock_single.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            engine.migrate_pairs(
                MagicMock(), self.PAIRS[:1], _options(skip_on_error=False)
            )

    @patch("datahub.migration.engine.migrate_pair")
    def test_returns_report(self, _mock_single: MagicMock) -> None:
        """The batch orchestrator returns a MigrationReport."""
        report = engine.migrate_pairs(MagicMock(), self.PAIRS[:1], _options())
        assert isinstance(report, MigrationReport)


# --- clone_aspect dry-run behavior ---


class TestCloneAspectDryRun:
    """Verify clone_aspect always yields MCPs regardless of dry_run."""

    @patch("datahub.cli.migration_utils.get_default_graph")
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_clone_aspect_yields_mcps_for_dry_run_reporting(
        self, mock_get_aspects: MagicMock, _mock_graph: MagicMock
    ) -> None:
        """clone_aspect yields one MCP per found aspect (retargeted to the new URN)
        so the migration report can count them even on a dry run."""
        from datahub.cli.migration_utils import clone_aspect
        from datahub.metadata.schema_classes import DatasetPropertiesClass

        mock_get_aspects.return_value = {
            "datasetProperties": DatasetPropertiesClass(description="test")
        }

        mcps = list(
            clone_aspect(
                src_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,src,PROD)",
                aspect_names=["datasetProperties"],
                dst_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,dst,PROD)",
            )
        )

        assert len(mcps) == 1
        assert (
            mcps[0].entityUrn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,dst,PROD)"
        )
        assert mcps[0].aspectName == "datasetProperties"


# --- merge_entity orchestrator ---


class TestMergeEntity:
    """Tests for the top-level merge_entity orchestrator."""

    MERGE_SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a1.db.t,PROD)"
    MERGE_DST = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.t,PROD)"

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_routes_additive_aspects_to_patch(
        self, mock_get_aspects: MagicMock
    ) -> None:
        """Additive aspects (ownership, tags) are merged into the target with no
        conflicts skipped."""
        from datahub.cli.migration_utils import merge_entity
        from datahub.metadata.schema_classes import (
            GlobalTagsClass,
            OwnerClass,
            OwnershipClass,
            OwnershipTypeClass,
            TagAssociationClass,
        )

        mock_get_aspects.return_value = {
            "ownership": OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:alice",
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
        }

        result = merge_entity(
            self.MERGE_SRC,
            self.MERGE_DST,
            ConflictStrategy.PATCH,
            MagicMock(),
            dry_run=True,
        )

        assert result.merged > 0
        assert result.skipped == 0

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_routes_non_additive_to_conflict_check(
        self, mock_get_aspects: MagicMock
    ) -> None:
        """A conflicting non-additive aspect (viewProperties) is skipped under PATCH."""
        from datahub.cli.migration_utils import merge_entity
        from datahub.metadata.schema_classes import ViewPropertiesClass

        src_view = ViewPropertiesClass(
            materialized=False, viewLogic="SELECT 1", viewLanguage="SQL"
        )
        dst_view = ViewPropertiesClass(
            materialized=False, viewLogic="SELECT 2", viewLanguage="SQL"
        )
        # First call: src aspects; subsequent calls: dst aspects
        mock_get_aspects.side_effect = [
            {"viewProperties": src_view},
            {"viewProperties": dst_view},
        ]

        result = merge_entity(
            self.MERGE_SRC,
            self.MERGE_DST,
            ConflictStrategy.PATCH,
            MagicMock(),
            dry_run=True,
        )

        assert result.skipped == 1

    def test_preserve_leaves_target_untouched(self) -> None:
        """PRESERVE short-circuits merge_entity: nothing is emitted and the conflict
        is counted as skipped."""
        from datahub.cli.migration_utils import merge_entity

        graph = MagicMock()
        result = merge_entity(
            self.MERGE_SRC,
            self.MERGE_DST,
            ConflictStrategy.PRESERVE,
            graph,
            dry_run=False,
        )
        assert result.merged == 0
        assert result.skipped == 1
        graph.emit_mcp.assert_not_called()
