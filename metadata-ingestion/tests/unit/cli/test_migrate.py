"""Tests for the migration engine and CLI orchestration."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.migrate import _read_urns_from_file, snowflake_semantic_views
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    TagAssociationClass,
)
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
        return_value=MergeResult(
            merged=3,
            skipped=1,
            merged_aspects=["ownership", "globalTags", "schema"],
            skipped_aspects=["viewProperties"],
        ),
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

    @patch("datahub.cli.delete_cli._delete_one_urn")
    def test_identity_pair_raises_before_any_write(
        self, mock_delete: MagicMock
    ) -> None:
        """An identical source/target is rejected — never cloned or deleted."""
        graph = MagicMock()
        with pytest.raises(ValueError, match="identical"):
            engine.migrate_pair(
                graph,
                MigrationPair(SRC_URN, SRC_URN),
                _options(dry_run=False, keep=False),
                MigrationReport("test-run", dry_run=False, keep=False),
            )
        graph.emit_mcp.assert_not_called()
        mock_delete.assert_not_called()

    @patch("datahub.cli.delete_cli._delete_one_urn")
    def test_exists_error_aborts_pair(self, mock_delete: MagicMock) -> None:
        """A transient exists() failure aborts the pair rather than being read as
        'target absent' (which would overwrite the target and delete the source)."""
        graph = MagicMock()
        graph.exists.side_effect = RuntimeError("gms unreachable")
        with pytest.raises(RuntimeError, match="Could not determine whether target"):
            engine.migrate_pair(
                graph,
                MigrationPair(SRC_URN, DST_URN),
                _options(
                    dry_run=False, keep=False, on_conflict=ConflictStrategy.OVERWRITE
                ),
                MigrationReport("test-run", dry_run=False, keep=False),
            )
        mock_delete.assert_not_called()

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_editable_schema_metadata_with_tags_is_migrated(
        self, mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """editableSchemaMetadata carrying column-level tags is cloned to the target
        and the aspect body is emitted as-is (tags preserved)."""
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        editable = EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="customer_id",
                    globalTags=GlobalTagsClass(
                        tags=[TagAssociationClass(tag="urn:li:tag:pii")]
                    ),
                ),
            ]
        )
        mock_clone.return_value = [
            MetadataChangeProposalWrapper(
                entityUrn=DST_URN,
                aspect=editable,
            )
        ]

        graph = MagicMock()
        graph.exists.return_value = False
        report = MigrationReport("test-run", dry_run=False, keep=True)
        engine.migrate_pair(
            graph,
            MigrationPair(SRC_URN, DST_URN),
            _options(dry_run=False),
            report,
        )

        # The aspect was emitted to the graph
        emitted = [
            c.args[0]
            for c in graph.emit_mcp.call_args_list
            if isinstance(c.args[0].aspect, EditableSchemaMetadataClass)
        ]
        assert len(emitted) == 1
        emitted_aspect = emitted[0].aspect
        assert emitted[0].entityUrn == DST_URN
        field = emitted_aspect.editableSchemaFieldInfo[0]
        assert field.fieldPath == "customer_id"
        assert field.globalTags.tags[0].tag == "urn:li:tag:pii"


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

    @patch("datahub.migration.engine.migrate_pair")
    def test_on_pair_done_called_for_each_pair(self, _mock_single: MagicMock) -> None:
        """on_pair_done is invoked once per pair, after the pair is processed."""
        completed: list = []
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS,
            _options(),
            on_pair_done=lambda p: completed.append(p),
        )
        assert completed == self.PAIRS

    @patch("datahub.migration.engine.migrate_pair")
    def test_on_pair_done_called_even_on_skip(self, mock_single: MagicMock) -> None:
        """on_pair_done fires for skipped (errored) pairs too."""
        mock_single.side_effect = [RuntimeError("boom"), None]
        completed: list = []
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS,
            _options(skip_on_error=True),
            on_pair_done=lambda p: completed.append(p),
        )
        assert completed == self.PAIRS


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


# --- _read_urns_from_file ---


class TestReadUrnsFromFile:
    def test_skips_blank_lines_and_comments(self, tmp_path: Path) -> None:
        urn_file = tmp_path / "urns.txt"
        urn_file.write_text(
            "urn:li:dataset:(a,b,PROD)\n\n# a comment\nurn:li:dataset:(c,d,PROD)\n"
        )

        assert _read_urns_from_file(str(urn_file)) == [
            "urn:li:dataset:(a,b,PROD)",
            "urn:li:dataset:(c,d,PROD)",
        ]


# --- snowflake_semantic_views CLI command ---


class TestSnowflakeSemanticViewsCli:
    SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.my_view,PROD)"

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.filter_by_semantic_view_subtype")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_force_skips_confirmation_prompt(
        self,
        mock_get_graph: MagicMock,
        mock_filter: MagicMock,
        mock_run_migration: MagicMock,
    ) -> None:
        mock_filter.return_value = ([self.SRC_URN], [])
        mock_run_migration.return_value = MagicMock()

        result = CliRunner().invoke(
            snowflake_semantic_views,
            ["--direction", "dataset-to-sm", "--urn", self.SRC_URN, "--force"],
        )

        assert result.exit_code == 0, result.output
        mock_run_migration.assert_called_once()

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.filter_by_semantic_view_subtype")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_confirmation_prompt_aborts_on_no(
        self,
        mock_get_graph: MagicMock,
        mock_filter: MagicMock,
        mock_run_migration: MagicMock,
    ) -> None:
        mock_filter.return_value = ([self.SRC_URN], [])

        result = CliRunner().invoke(
            snowflake_semantic_views,
            ["--direction", "dataset-to-sm", "--urn", self.SRC_URN],
            input="n\n",
        )

        assert result.exit_code != 0
        mock_run_migration.assert_not_called()

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.filter_by_semantic_view_subtype")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_confirmation_prompt_proceeds_on_yes(
        self,
        mock_get_graph: MagicMock,
        mock_filter: MagicMock,
        mock_run_migration: MagicMock,
    ) -> None:
        mock_filter.return_value = ([self.SRC_URN], [])
        mock_run_migration.return_value = MagicMock()

        result = CliRunner().invoke(
            snowflake_semantic_views,
            ["--direction", "dataset-to-sm", "--urn", self.SRC_URN],
            input="y\n",
        )

        assert result.exit_code == 0, result.output
        mock_run_migration.assert_called_once()

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.filter_by_semantic_view_subtype")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_urn_file_is_read_and_combined_with_explicit_urns(
        self,
        mock_get_graph: MagicMock,
        mock_filter: MagicMock,
        mock_run_migration: MagicMock,
        tmp_path: Path,
    ) -> None:
        file_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.other_view,PROD)"
        )
        urn_file = tmp_path / "urns.txt"
        urn_file.write_text(f"# comment\n\n{file_urn}\n")

        mock_filter.side_effect = lambda graph, urns, force: (list(urns), [])
        mock_run_migration.return_value = MagicMock()

        result = CliRunner().invoke(
            snowflake_semantic_views,
            [
                "--direction",
                "dataset-to-sm",
                "--urn",
                self.SRC_URN,
                "--urn-file",
                str(urn_file),
                "--force",
            ],
        )

        assert result.exit_code == 0, result.output
        migrated_urns = mock_run_migration.call_args.kwargs["urns"]
        assert set(migrated_urns) == {self.SRC_URN, file_urn}

    @patch("datahub.cli.migrate.discover_semantic_model_urns")
    @patch("datahub.cli.migrate.discover_semantic_view_dataset_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_no_entities_found_generic_message(
        self,
        mock_get_graph: MagicMock,
        mock_discover_dataset: MagicMock,
        mock_discover_sm: MagicMock,
    ) -> None:
        mock_discover_dataset.return_value = []

        result = CliRunner().invoke(
            snowflake_semantic_views, ["--direction", "dataset-to-sm"]
        )

        assert result.exit_code == 0, result.output
        assert "No entities found to migrate." in result.output
        mock_discover_sm.assert_not_called()

    @patch("datahub.cli.migrate.discover_semantic_view_dataset_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_no_live_entities_hints_at_soft_deleted(
        self, mock_get_graph: MagicMock, mock_discover_dataset: MagicMock
    ) -> None:
        # First call is the live discovery (empty); second is the
        # only_soft_deleted probe (finds some).
        mock_discover_dataset.side_effect = [[], [self.SRC_URN, self.SRC_URN + "2"]]

        result = CliRunner().invoke(
            snowflake_semantic_views, ["--direction", "dataset-to-sm"]
        )

        assert result.exit_code == 0, result.output
        assert "2 soft-deleted" in result.output
        assert "--include-soft-deleted" in result.output

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.filter_by_semantic_view_subtype")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_explicit_urns_all_lacking_subtype_shows_specific_message(
        self,
        mock_get_graph: MagicMock,
        mock_filter: MagicMock,
        mock_run_migration: MagicMock,
    ) -> None:
        mock_filter.return_value = ([], [self.SRC_URN])

        result = CliRunner().invoke(
            snowflake_semantic_views,
            ["--direction", "dataset-to-sm", "--urn", self.SRC_URN],
        )

        assert result.exit_code == 0, result.output
        assert "lack the 'Semantic View' subtype" in result.output
        assert "--force" in result.output
        mock_run_migration.assert_not_called()

    @patch("datahub.cli.migrate.run_migration")
    @patch("datahub.cli.migrate.discover_semantic_model_urns")
    @patch("datahub.cli.migrate.get_default_graph")
    def test_sm_to_dataset_direction_discovers_semantic_models(
        self,
        mock_get_graph: MagicMock,
        mock_discover_sm: MagicMock,
        mock_run_migration: MagicMock,
    ) -> None:
        sm_urn = (
            "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,db.schema,my_view)"
        )
        mock_discover_sm.return_value = [sm_urn]
        mock_run_migration.return_value = MagicMock()

        result = CliRunner().invoke(
            snowflake_semantic_views,
            ["--direction", "sm-to-dataset", "--env", "PROD", "--force"],
        )

        assert result.exit_code == 0, result.output
        mock_discover_sm.assert_called_once()
        assert mock_run_migration.call_args.kwargs["urns"] == [sm_urn]


# --- container migration ---


class TestMigrateContainers:
    """Container migration must regenerate the target instance aspect."""

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_reemits_new_instance_aspect(
        self, mock_get: MagicMock, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """A migrated container is stamped with a dataPlatformInstance for the new
        instance — it is excluded from the clone, so the migration regenerates it."""
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [
            {
                "urn": "urn:li:container:oldguid",
                "aspects": {
                    "subTypes": {"value": {"typeNames": ["Database"]}},
                    "containerProperties": {
                        "value": {
                            "customProperties": {
                                "platform": "snowflake",
                                "instance": "oldinst",
                                "env": "PROD",
                                "database": "db1",
                            }
                        }
                    },
                },
            }
        ]
        emitter = MagicMock()
        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
        )
        emitted = [c.args[0].aspect for c in emitter.emit_mcp.call_args_list]
        instances = [a for a in emitted if isinstance(a, DataPlatformInstanceClass)]
        assert instances, "no dataPlatformInstance emitted for the migrated container"
        assert instances[-1].instance == make_dataplatform_instance_urn(
            "snowflake", "newinst"
        )

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_dry_run_emits_nothing(
        self, mock_get: MagicMock, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """A dry-run container migration emits no MCPs."""
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [
            {
                "urn": "urn:li:container:oldguid",
                "aspects": {
                    "subTypes": {"value": {"typeNames": ["Database"]}},
                    "containerProperties": {
                        "value": {
                            "customProperties": {
                                "platform": "snowflake",
                                "instance": "oldinst",
                                "env": "PROD",
                                "database": "db1",
                            }
                        }
                    },
                },
            }
        ]
        emitter = MagicMock()
        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=True,
            hard=False,
            keep=True,
            rest_emitter=emitter,
        )
        emitter.emit_mcp.assert_not_called()

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_skips_containers_missing_subtypes(
        self, mock_get: MagicMock, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """Containers without subTypes must not abort the migration (KeyError)."""
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [
            {
                "urn": "urn:li:container:missing-subtypes",
                "aspects": {
                    "containerProperties": {
                        "value": {
                            "customProperties": {
                                "platform": "snowflake",
                                "instance": "oldinst",
                                "env": "PROD",
                                "database": "db1",
                            }
                        }
                    },
                },
            }
        ]
        emitter = MagicMock()
        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
        )
        emitter.emit_mcp.assert_not_called()

    @patch("datahub.cli.migrate._process_container_relationships")
    @patch("datahub.cli.migration_utils.clone_aspect", return_value=[])
    @patch("datahub.cli.migrate._get_containers_for_migration")
    def test_skips_containers_missing_container_properties(
        self, mock_get: MagicMock, _mock_clone: MagicMock, _mock_rels: MagicMock
    ) -> None:
        """Containers without containerProperties are skipped, not crashed."""
        from datahub.cli.migrate import _migrate_containers

        mock_get.return_value = [
            {
                "urn": "urn:li:container:missing-props",
                "aspects": {
                    "subTypes": {"value": {"typeNames": ["Database"]}},
                },
            },
            {"urn": "urn:li:container:no-aspects", "aspects": {}},
        ]
        emitter = MagicMock()
        _migrate_containers(
            env="PROD",
            platform="snowflake",
            target_instance="newinst",
            should_migrate=lambda props: True,
            dry_run=False,
            hard=False,
            keep=True,
            rest_emitter=emitter,
        )
        emitter.emit_mcp.assert_not_called()


# --- checkpoint / resume ---


class TestCheckpointResume:
    """Tests for the checkpoint/resume feature in migrate_pairs."""

    PAIRS = [
        MigrationPair(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t1,PROD)",
        ),
        MigrationPair(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t2,PROD)",
        ),
        MigrationPair(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t3,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t3,PROD)",
        ),
    ]

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_skips_already_migrated(
        self, mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """Pairs whose source URN appears in the checkpoint file are skipped."""
        ckpt = tmp_path / "ckpt.txt"
        ckpt.write_text(self.PAIRS[0].source_urn + "\n")

        report = engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:2],
            _options(checkpoint_file=str(ckpt)),
        )
        # Only the second pair should have been migrated
        assert mock_single.call_count == 1
        assert (
            mock_single.call_args_list[0].args[1].source_urn == self.PAIRS[1].source_urn
        )
        assert report.pairs_checkpoint_skipped == 1

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_appends_on_success(
        self, _mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """Successfully migrated pairs are appended to the checkpoint file."""
        ckpt = tmp_path / "ckpt.txt"
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:2],
            _options(dry_run=False, checkpoint_file=str(ckpt)),
        )
        lines = ckpt.read_text().strip().splitlines()
        assert lines == [self.PAIRS[0].source_urn, self.PAIRS[1].source_urn]

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_not_written_on_dry_run(
        self, _mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """Dry-run does not write to the checkpoint file."""
        ckpt = tmp_path / "ckpt.txt"
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:1],
            _options(dry_run=True, checkpoint_file=str(ckpt)),
        )
        assert not ckpt.exists()

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_not_written_on_error(
        self, mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """Errored pairs (under skip_on_error) are not written to the checkpoint."""
        mock_single.side_effect = [RuntimeError("boom"), None]
        ckpt = tmp_path / "ckpt.txt"
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:2],
            _options(dry_run=False, skip_on_error=True, checkpoint_file=str(ckpt)),
        )
        lines = ckpt.read_text().strip().splitlines()
        # Only the second (successful) pair should be checkpointed
        assert lines == [self.PAIRS[1].source_urn]

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_file_created_on_first_write(
        self, _mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """The checkpoint file is created automatically on the first successful pair."""
        ckpt = tmp_path / "subdir" / "ckpt.txt"
        ckpt.parent.mkdir(parents=True)
        assert not ckpt.exists()
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:1],
            _options(dry_run=False, checkpoint_file=str(ckpt)),
        )
        assert ckpt.exists()
        assert ckpt.read_text().strip() == self.PAIRS[0].source_urn

    @patch("datahub.migration.engine.migrate_pair")
    def test_checkpoint_on_pair_done_fires_for_skipped(
        self, _mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """on_pair_done is called for checkpoint-skipped pairs too."""
        ckpt = tmp_path / "ckpt.txt"
        ckpt.write_text(self.PAIRS[0].source_urn + "\n")
        completed: list = []
        engine.migrate_pairs(
            MagicMock(),
            self.PAIRS[:2],
            _options(checkpoint_file=str(ckpt)),
            on_pair_done=lambda p: completed.append(p),
        )
        assert completed == self.PAIRS[:2]


# --- MigrationReport lightweight counters ---


class TestMigrationReportCounters:
    """Tests for the lightweight counter-based MigrationReport."""

    def test_unique_entity_counting_via_last_urn(self) -> None:
        """on_entity_create deduplicates by tracking _last_created_urn —
        consecutive calls with the same URN increment aspects but not entities."""
        report = MigrationReport("test", dry_run=False, keep=True)
        report.on_entity_create("urn:li:dataset:a", "ownership")
        report.on_entity_create("urn:li:dataset:a", "tags")
        report.on_entity_create("urn:li:dataset:a", "schema")
        report.on_entity_create("urn:li:dataset:b", "ownership")
        report.on_entity_create("urn:li:dataset:b", "tags")

        assert report.num_entities_created == 2
        assert report.num_aspects_created == 5

    def test_unique_entity_affected_counting(self) -> None:
        """on_entity_affected deduplicates by tracking _last_affected_urn."""
        report = MigrationReport("test", dry_run=False, keep=True)
        report.on_entity_affected("urn:li:dataset:x", "ownership")
        report.on_entity_affected("urn:li:dataset:x", "lineage")
        report.on_entity_affected("urn:li:dataset:y", "ownership")

        assert report.num_entities_affected == 2
        assert report.num_aspects_affected == 3

    def test_migrated_counts_each_call(self) -> None:
        """on_entity_migrated increments the counter on every call (one per pair)."""
        report = MigrationReport("test", dry_run=False, keep=True)
        report.on_entity_migrated("urn:li:dataset:a", "COMPLETED")
        report.on_entity_migrated("urn:li:dataset:b", "COMPLETED")

        assert report.num_entities_migrated == 2

    def test_repr_uses_int_counters(self) -> None:
        """repr surfaces the integer counters without URN details."""
        report = MigrationReport("test", dry_run=False, keep=False)
        report.on_entity_create("urn:li:dataset:a", "ownership")
        report.on_entity_create("urn:li:dataset:b", "ownership")
        report.on_entity_affected("urn:li:dataset:c", "lineage")
        report.on_entity_migrated("urn:li:dataset:a", "COMPLETED")

        text = repr(report)
        assert "Num entities created = 2" in text
        assert "Num entities affected = 1" in text
        assert "Num entities migrated = 1" in text
        # The old "Details:" section with URN sets is gone
        assert "Details:" not in text


# --- Report file output ---


class TestMigrationReportFile:
    """Tests for the --migration-report file output."""

    def test_report_file_captures_actions(self, tmp_path: Path) -> None:
        """The report file captures 4-column TSV lines with pair context."""
        report_path = tmp_path / "report.tsv"
        report = MigrationReport("test", dry_run=False, keep=True)
        report.open_report_file(str(report_path))
        report.set_current_pair("urn:li:dataset:src", "urn:li:dataset:tgt")
        report.on_entity_create("urn:li:dataset:tgt", "ownership")
        report.on_entity_affected("urn:li:dataset:ref", "lineage")
        report.on_entity_migrated("urn:li:dataset:src", "COMPLETED")
        report.close_report_file()

        lines = report_path.read_text().strip().splitlines()
        assert len(lines) == 3
        # create/migrated: action, source_urn, target_urn, aspect
        assert lines[0] == "create\turn:li:dataset:src\turn:li:dataset:tgt\townership"
        # affected: action, referrer_urn, target_urn, aspect
        assert lines[1] == "affected\turn:li:dataset:ref\turn:li:dataset:tgt\tlineage"
        assert lines[2] == "migrated\turn:li:dataset:src\turn:li:dataset:tgt\tCOMPLETED"

    def test_no_report_file_when_not_configured(self) -> None:
        """When no report file is opened, _write_report is a no-op."""
        report = MigrationReport("test", dry_run=False, keep=True)
        # Should not raise
        report.on_entity_create("urn:li:dataset:a", "ownership")
        assert report._report_fh is None

    def test_close_is_idempotent(self) -> None:
        """Closing a report file twice does not raise."""
        report = MigrationReport("test", dry_run=False, keep=True)
        report.close_report_file()
        report.close_report_file()

    def test_merge_and_skip_lines_in_report(self, tmp_path: Path) -> None:
        """on_aspect_merged and on_aspect_skipped write merge/skip lines to the file."""
        report_path = tmp_path / "report.tsv"
        report = MigrationReport("test", dry_run=False, keep=True)
        report.open_report_file(str(report_path))
        report.set_current_pair("urn:li:dataset:src", "urn:li:dataset:tgt")
        report.on_aspect_merged("globalTags")
        report.on_aspect_merged("ownership")
        report.on_aspect_skipped("viewProperties")
        report.on_aspect_skipped("*")
        report.close_report_file()

        lines = report_path.read_text().strip().splitlines()
        assert len(lines) == 4
        assert lines[0] == "merge\turn:li:dataset:src\turn:li:dataset:tgt\tglobalTags"
        assert lines[1] == "merge\turn:li:dataset:src\turn:li:dataset:tgt\townership"
        assert (
            lines[2] == "skip\turn:li:dataset:src\turn:li:dataset:tgt\tviewProperties"
        )
        assert lines[3] == "skip\turn:li:dataset:src\turn:li:dataset:tgt\t*"

    @patch("datahub.cli.migration_utils.get_incoming_relationships", return_value=[])
    @patch(
        "datahub.cli.migration_utils.merge_entity",
        return_value=MergeResult(
            merged=2,
            skipped=1,
            merged_aspects=["ownership", "globalTags"],
            skipped_aspects=["viewProperties"],
        ),
    )
    def test_engine_merge_path_writes_to_report(
        self, _mock_merge: MagicMock, _mock_rels: MagicMock, tmp_path: Path
    ) -> None:
        """The engine merge path writes merge/skip lines to the report file."""
        report_path = tmp_path / "report.tsv"
        report = MigrationReport("test", dry_run=True, keep=True)
        report.open_report_file(str(report_path))
        engine.migrate_pair(
            MagicMock(exists=MagicMock(return_value=True)),
            MigrationPair(SRC_URN, DST_URN),
            _options(dry_run=True, on_conflict=ConflictStrategy.PATCH),
            report,
        )
        report.close_report_file()

        content = report_path.read_text()
        lines = content.strip().splitlines()
        actions = [line.split("\t")[0] for line in lines]
        assert "merge" in actions
        assert "skip" in actions
        assert "migrated" in actions

    @patch("datahub.migration.engine.migrate_pair")
    def test_engine_opens_report_file(
        self, _mock_single: MagicMock, tmp_path: Path
    ) -> None:
        """migrate_pairs opens and closes the report file when report_file is set."""
        report_path = tmp_path / "engine_report.tsv"
        pairs = [
            MigrationPair(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,b.t1,PROD)",
            ),
        ]
        report = engine.migrate_pairs(
            MagicMock(),
            pairs,
            _options(report_file=str(report_path)),
        )
        # The report file should have been closed
        assert report._report_fh is None
        # The file exists (even if empty — migrate_pair is mocked)
        assert report_path.exists()
