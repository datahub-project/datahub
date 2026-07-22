"""Tests for datahub.cli.migrate — core migration orchestration logic."""

from typing import List
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.migrate import (
    MigrationReport,
    _migrate_entities,
    _migrate_single_entity,
    _read_urns_from_file,
    snowflake_semantic_views,
)
from datahub.cli.migration_utils import ConflictStrategy
from datahub.metadata.schema_classes import SystemMetadataClass
from datahub.utilities.urns.urn import guess_entity_type

# --- guess_entity_type ---


class TestGuessEntityType:
    def test_extracts_dataset(self) -> None:
        assert (
            guess_entity_type(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)"
            )
            == "dataset"
        )

    def test_extracts_chart(self) -> None:
        assert guess_entity_type("urn:li:chart:(powerbi,my_chart)") == "chart"

    def test_extracts_container(self) -> None:
        assert guess_entity_type("urn:li:container:abc123") == "container"


# --- _migrate_single_entity ---


class TestMigrateSingleEntity:
    """Tests for the single-entity migration path."""

    SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,old.db.table,PROD)"
    DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,new.db.table,PROD)"

    def _make_deps(self, dry_run: bool = True, keep: bool = True) -> dict:
        """Build common dependencies for _migrate_single_entity."""
        graph = MagicMock()
        graph.exists.return_value = False
        report = MigrationReport("test-run", dry_run=dry_run, keep=keep)
        system_metadata = SystemMetadataClass(runId="test-run")
        return dict(
            graph=graph,
            migration_report=report,
            system_metadata=system_metadata,
        )

    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    @patch("datahub.cli.migrate.migration_utils.clone_aspect", return_value=[])
    def test_dry_run_does_not_emit(
        self,
        mock_clone: MagicMock,
        mock_rels: MagicMock,
    ) -> None:
        deps = self._make_deps(dry_run=True)
        _migrate_single_entity(
            src_entity_urn=self.SRC_URN,
            make_new_urn=lambda _: self.DST_URN,
            platform="snowflake",
            target_instance="new",
            dry_run=True,
            hard=False,
            keep=True,
            run_id="test-run",
            on_conflict=None,
            **deps,
        )
        deps["graph"].emit_mcp.assert_not_called()

    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    @patch("datahub.cli.migrate.migration_utils.clone_aspect", return_value=[])
    def test_emits_platform_instance_when_not_dry_run(
        self,
        mock_clone: MagicMock,
        mock_rels: MagicMock,
    ) -> None:
        deps = self._make_deps(dry_run=False)
        _migrate_single_entity(
            src_entity_urn=self.SRC_URN,
            make_new_urn=lambda _: self.DST_URN,
            platform="snowflake",
            target_instance="new",
            dry_run=False,
            hard=False,
            keep=True,
            run_id="test-run",
            on_conflict=None,
            **deps,
        )
        # Should emit at least the dataPlatformInstance MCP
        deps["graph"].emit_mcp.assert_called()

    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    @patch("datahub.cli.migrate.merge_entity", return_value=(3, 1))
    def test_merge_path_when_target_exists(
        self,
        mock_merge: MagicMock,
        mock_rels: MagicMock,
    ) -> None:
        deps = self._make_deps(dry_run=True)
        deps["graph"].exists.return_value = True

        _migrate_single_entity(
            src_entity_urn=self.SRC_URN,
            make_new_urn=lambda _: self.DST_URN,
            platform="snowflake",
            target_instance="new",
            dry_run=True,
            hard=False,
            keep=True,
            run_id="test-run",
            on_conflict=ConflictStrategy.PATCH,
            **deps,
        )
        mock_merge.assert_called_once()
        assert deps["migration_report"].aspects_merged == 3
        assert deps["migration_report"].conflicts_skipped == 1

    @patch("datahub.cli.migrate.delete_cli._delete_one_urn")
    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    @patch("datahub.cli.migrate.migration_utils.clone_aspect", return_value=[])
    def test_deletes_source_when_not_keep(
        self,
        mock_clone: MagicMock,
        mock_rels: MagicMock,
        mock_delete: MagicMock,
    ) -> None:
        deps = self._make_deps(dry_run=False, keep=False)
        _migrate_single_entity(
            src_entity_urn=self.SRC_URN,
            make_new_urn=lambda _: self.DST_URN,
            platform="snowflake",
            target_instance="new",
            dry_run=False,
            hard=False,
            keep=False,
            run_id="test-run",
            on_conflict=None,
            **deps,
        )
        mock_delete.assert_called_once_with(
            deps["graph"], self.SRC_URN, soft=True, run_id="test-run"
        )

    @patch("datahub.cli.migrate.delete_cli._delete_one_urn")
    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    @patch("datahub.cli.migrate.migration_utils.clone_aspect", return_value=[])
    def test_skips_delete_on_dry_run(
        self,
        mock_clone: MagicMock,
        mock_rels: MagicMock,
        mock_delete: MagicMock,
    ) -> None:
        deps = self._make_deps(dry_run=True, keep=False)
        _migrate_single_entity(
            src_entity_urn=self.SRC_URN,
            make_new_urn=lambda _: self.DST_URN,
            platform="snowflake",
            target_instance="new",
            dry_run=True,
            hard=False,
            keep=False,
            run_id="test-run",
            on_conflict=None,
            **deps,
        )
        mock_delete.assert_not_called()


# --- _migrate_entities (skip-on-error behavior) ---


class TestMigrateEntities:
    """Tests for the batch migration orchestrator."""

    SRC_URNS: List[str] = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,a.t2,PROD)",
    ]

    @patch("datahub.cli.migrate._migrate_single_entity")
    def test_skip_on_error_continues(self, mock_single: MagicMock) -> None:
        mock_single.side_effect = [RuntimeError("boom"), None]
        graph = MagicMock()

        report = _migrate_entities(
            urns_to_migrate=self.SRC_URNS,
            make_new_urn=lambda u: u.replace("a.", "b."),
            platform="snowflake",
            target_instance="b",
            dry_run=True,
            force=True,
            hard=False,
            keep=True,
            run_id="test",
            graph=graph,
            skip_on_error=True,
        )
        assert len(report.entities_errored) == 1
        assert report.entities_errored[0][0] == self.SRC_URNS[0]
        # Second entity should still have been attempted
        assert mock_single.call_count == 2

    @patch("datahub.cli.migrate._migrate_single_entity")
    def test_raises_without_skip_on_error(self, mock_single: MagicMock) -> None:
        mock_single.side_effect = RuntimeError("boom")
        graph = MagicMock()

        with pytest.raises(RuntimeError, match="boom"):
            _migrate_entities(
                urns_to_migrate=self.SRC_URNS[:1],
                make_new_urn=lambda u: u,
                platform="snowflake",
                target_instance="b",
                dry_run=True,
                force=True,
                hard=False,
                keep=True,
                run_id="test",
                graph=graph,
                skip_on_error=False,
            )

    @patch("datahub.cli.migrate._migrate_single_entity")
    def test_force_skips_confirmation(self, mock_single: MagicMock) -> None:
        """With force=True, no click.confirm should be triggered."""
        graph = MagicMock()

        report = _migrate_entities(
            urns_to_migrate=self.SRC_URNS[:1],
            make_new_urn=lambda u: u,
            platform="snowflake",
            target_instance="b",
            dry_run=False,
            force=True,
            hard=False,
            keep=True,
            run_id="test",
            graph=graph,
        )
        # Should complete without prompting
        assert isinstance(report, MigrationReport)


# --- clone_aspect dry-run behavior ---


class TestCloneAspectDryRun:
    """Verify clone_aspect always yields MCPs regardless of dry_run."""

    @patch("datahub.cli.migration_utils.get_default_graph")
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_clone_aspect_yields_mcps_for_dry_run_reporting(
        self,
        mock_get_aspects: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
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

    SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a1.db.t,PROD)"
    DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.t,PROD)"

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_routes_additive_aspects_to_patch(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
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

        graph = MagicMock()
        merged, skipped = merge_entity(
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        assert merged > 0
        assert skipped == 0

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_routes_non_additive_to_conflict_check(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
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

        graph = MagicMock()
        merged, skipped = merge_entity(
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        # PATCH mode: conflicting viewProperties should be skipped
        assert skipped == 1


# --- make_urn_builder edge cases ---


class TestMakeUrnBuilderEdgeCases:
    def test_unsupported_entity_type_raises(self) -> None:
        from datahub.cli.migration_utils import make_urn_builder

        with pytest.raises(ValueError, match="Unsupported entity type"):
            make_urn_builder("mlModel", new_instance="inst")

    def test_dataflow_via_make_urn_builder_directly(self) -> None:
        from datahub.cli.migration_utils import make_urn_builder

        builder = make_urn_builder("dataFlow", new_instance="new", old_instance="old")
        result = builder("urn:li:dataFlow:(airflow,old.my_dag,PROD)")
        assert result == "urn:li:dataFlow:(airflow,new.my_dag,PROD)"

    def test_replace_instance_prefix_with_dotted_instance(self) -> None:
        from datahub.cli.migration_utils import replace_instance_prefix

        result = replace_instance_prefix("a.b.schema.table", "a.b", "x.y")
        assert result == "x.y.schema.table"

    def test_replace_instance_prefix_raises_on_missing_prefix(self) -> None:
        from datahub.cli.migration_utils import replace_instance_prefix

        with pytest.raises(ValueError, match="does not start with expected"):
            replace_instance_prefix("unrelated.table", "old_inst", "new_inst")


# --- _read_urns_from_file ---


class TestReadUrnsFromFile:
    def test_skips_blank_lines_and_comments(self, tmp_path) -> None:
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
        self, mock_get_graph, mock_filter, mock_run_migration
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
        self, mock_get_graph, mock_filter, mock_run_migration
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
        self, mock_get_graph, mock_filter, mock_run_migration
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
        self, mock_get_graph, mock_filter, mock_run_migration, tmp_path
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
        self, mock_get_graph, mock_discover_dataset, mock_discover_sm
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
        self, mock_get_graph, mock_discover_dataset
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
        self, mock_get_graph, mock_filter, mock_run_migration
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
        self, mock_get_graph, mock_discover_sm, mock_run_migration
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
