"""Tests for datahub.cli.migration_utils — relationship-to-aspect mapping and URN rewriting."""

from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
from avrogen.dict_wrapper import DictWrapper

from datahub.cli.migration_utils import (
    get_migratable_aspect_names,
    merge_additive_aspects,
    merge_mixed_aspects,
    should_overwrite_non_additive,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.migration.models import ConflictStrategy, MigrationReport
from datahub.migration.transform import (
    make_i2i_chart_urn,
    make_i2i_dashboard_urn,
    make_i2i_dataflow_urn,
    make_i2i_datajob_urn,
    make_i2i_dataset_urn,
    make_p2i_chart_urn,
    make_p2i_dashboard_urn,
    make_p2i_dataflow_urn,
    make_p2i_datajob_urn,
    make_p2i_dataset_urn,
    replace_instance_prefix,
)


def _emitted_patch_values(graph: MagicMock) -> list:
    """(aspectName, decoded-patch-JSON) for every patch MCP emitted via graph.emit."""
    return [
        (call.args[0].aspectName, call.args[0].aspect.value.decode())
        for call in graph.emit.call_args_list
    ]


# --- instance2instance helper tests ---


class TestReplaceInstancePrefix:
    """Tests for the instance prefix replacement logic used by instance2instance."""

    def test_replaces_old_prefix_with_new(self):
        """Swaps the leading old-instance prefix for the new one, keeping the rest of the name."""
        result = replace_instance_prefix("old_inst.db.table", "old_inst", "new_inst")
        assert result == "new_inst.db.table"

    def test_raises_when_name_missing_old_prefix(self):
        """If name doesn't start with old instance, raise ValueError."""
        with pytest.raises(ValueError, match="does not start with expected"):
            replace_instance_prefix("db.table", "old_inst", "new_inst")

    def test_only_replaces_first_occurrence(self):
        """Should only strip the leading prefix, not occurrences deeper in the name."""
        result = replace_instance_prefix(
            "old_inst.old_inst.schema.table", "old_inst", "new_inst"
        )
        assert result == "new_inst.old_inst.schema.table"

    def test_handles_single_segment_name(self):
        """Replaces the prefix on a two-segment instance.name."""
        result = replace_instance_prefix("old_inst.table", "old_inst", "new_inst")
        assert result == "new_inst.table"

    def test_preserves_complex_names(self):
        """Swaps the prefix while preserving a multi-segment, mixed-case name."""
        result = replace_instance_prefix(
            "prod_sf.MY_DB.MY_SCHEMA.MY_TABLE", "prod_sf", "shared_sf"
        )
        assert result == "shared_sf.MY_DB.MY_SCHEMA.MY_TABLE"


# --- ConflictStrategy and merge logic tests ---


# --- Conflict resolution tests ---


class TestShouldOverwriteNonAdditive:
    """Tests for non-additive aspect conflict resolution."""

    SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a1.db.table,PROD)"
    DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.table,PROD)"

    def _make_props(self, description: str) -> DatasetPropertiesClass:
        return DatasetPropertiesClass(description=description)

    def test_no_conflict_when_same(self):
        """Identical aspects should always return True (safe to overwrite)."""
        src = self._make_props("same description")
        dst = self._make_props("same description")
        assert should_overwrite_non_additive(
            "datasetProperties",
            src,
            dst,
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.PATCH,
        )

    def test_overwrite_strategy_returns_true(self):
        """OVERWRITE strategy returns True even when source and target differ."""
        src = self._make_props("source desc")
        dst = self._make_props("target desc")
        assert should_overwrite_non_additive(
            "datasetProperties",
            src,
            dst,
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.OVERWRITE,
        )

    def test_patch_strategy_returns_false_on_conflict(self):
        """PATCH strategy returns False when source and target values conflict."""
        src = self._make_props("source desc")
        dst = self._make_props("target desc")
        assert not should_overwrite_non_additive(
            "datasetProperties",
            src,
            dst,
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.PATCH,
        )


# --- Merge logic tests (with mocked graph) ---


class TestMergeAdditiveAspects:
    """Tests for merge_additive_aspects using mocked graph."""

    DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.table,PROD)"

    def test_merges_ownership(self):
        """The source owner lands in an emitted ownership patch."""
        owner = OwnerClass(
            owner="urn:li:corpuser:alice", type=OwnershipTypeClass.DATAOWNER
        )
        src_aspects: Dict[str, DictWrapper] = {
            "ownership": OwnershipClass(owners=[owner])
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result > 0
        assert any(
            name == "ownership" and "urn:li:corpuser:alice" in value
            for name, value in _emitted_patch_values(graph)
        )

    def test_merges_tags(self):
        """The source tag lands in an emitted globalTags patch."""
        tag = TagAssociationClass(tag="urn:li:tag:pii")
        src_aspects: Dict[str, DictWrapper] = {
            "globalTags": GlobalTagsClass(tags=[tag])
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result > 0
        assert any(
            name == "globalTags" and "urn:li:tag:pii" in value
            for name, value in _emitted_patch_values(graph)
        )

    def test_merges_terms(self):
        """The source glossary term lands in an emitted glossaryTerms patch."""
        term = GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:Revenue")
        src_aspects: Dict[str, DictWrapper] = {
            "glossaryTerms": GlossaryTermsClass(
                terms=[term],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:test"),
            )
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result > 0
        assert any(
            name == "glossaryTerms" and "urn:li:glossaryTerm:Revenue" in value
            for name, value in _emitted_patch_values(graph)
        )

    def test_merges_lineage(self):
        """The source upstream lands in an emitted upstreamLineage patch."""
        upstream = UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,src.table,PROD)",
            type="TRANSFORMED",
        )
        src_aspects: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(upstreams=[upstream])
        }
        graph = MagicMock()
        graph.get_aspect.return_value = UpstreamLineageClass(upstreams=[])

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result > 0
        graph.emit_mcp.assert_not_called()
        assert any(
            name == "upstreamLineage" and "src.table" in value
            for name, value in _emitted_patch_values(graph)
        )

    def test_upserts_lineage_when_target_has_none(self):
        """Missing target lineage is created with UPSERT, not a no-op PATCH."""
        upstream = UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,src.table,PROD)",
            type="TRANSFORMED",
        )
        src_aspects: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(upstreams=[upstream])
        }
        graph = MagicMock()
        graph.get_aspect.return_value = None

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result == 1
        graph.emit.assert_not_called()
        graph.emit_mcp.assert_called_once()
        mcp = graph.emit_mcp.call_args.args[0]
        assert mcp.entityUrn == self.DST_URN
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        assert mcp.aspect.upstreams[0].dataset == upstream.dataset

    def test_skips_empty_lineage_upsert(self):
        src_aspects: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(upstreams=[])
        }
        graph = MagicMock()
        graph.get_aspect.return_value = None

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, False)

        assert result == 0
        graph.get_aspect.assert_not_called()
        graph.emit.assert_not_called()
        graph.emit_mcp.assert_not_called()

    def test_upserts_lineage_dry_run_does_not_emit(self):
        """Dry-run still counts a lineage UPSERT but does not emit it."""
        upstream = UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,src.table,PROD)",
            type="TRANSFORMED",
        )
        src_aspects: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(upstreams=[upstream])
        }
        graph = MagicMock()
        graph.get_aspect.return_value = None

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, True)

        assert result == 1
        graph.emit.assert_not_called()
        graph.emit_mcp.assert_not_called()

    def test_empty_aspects_no_patches(self):
        """No patches should be emitted when there are no additive aspects."""
        graph = MagicMock()

        result = merge_additive_aspects({}, self.DST_URN, graph, True)

        assert result == 0

    def test_dry_run_does_not_emit(self):
        """Dry-run additive merge builds patches but never calls graph.emit."""
        owner = OwnerClass(
            owner="urn:li:corpuser:bob", type=OwnershipTypeClass.DATAOWNER
        )
        src_aspects: Dict[str, DictWrapper] = {
            "ownership": OwnershipClass(owners=[owner])
        }
        graph = MagicMock()

        merge_additive_aspects(src_aspects, self.DST_URN, graph, True)
        graph.emit.assert_not_called()


class TestMergeMixedAspects:
    """Tests for merge_mixed_aspects — customProperties + description merge."""

    SRC_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a1.db.table,PROD)"
    DST_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.table,PROD)"

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_merges_non_overlapping_custom_properties(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """Non-overlapping customProperties merge in with no skips."""
        src_props = DatasetPropertiesClass(
            description="", customProperties={"team": "alpha"}
        )
        dst_props = DatasetPropertiesClass(
            description="", customProperties={"env": "prod"}
        )
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        merged, skipped = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert merged > 0
        assert skipped == 0

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_skips_conflicting_custom_property_in_patch_mode(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A customProperty whose key collides with a different value is skipped under PATCH."""
        src_props = DatasetPropertiesClass(
            description="", customProperties={"team": "alpha"}
        )
        dst_props = DatasetPropertiesClass(
            description="", customProperties={"team": "beta"}
        )
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        merged, skipped = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert skipped == 1

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_overwrites_conflicting_custom_property_in_overwrite_mode(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A conflicting customProperty is overwritten (not skipped) under OVERWRITE."""
        src_props = DatasetPropertiesClass(
            description="", customProperties={"team": "alpha"}
        )
        dst_props = DatasetPropertiesClass(
            description="", customProperties={"team": "beta"}
        )
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        merged, skipped = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.OVERWRITE,
            True,
        )
        assert skipped == 0
        assert merged > 0

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_skips_description_conflict_in_patch_mode(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A description that differs from the target's is skipped under PATCH."""
        src_props = DatasetPropertiesClass(description="source desc")
        dst_props = DatasetPropertiesClass(description="target desc")
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        merged, skipped = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert skipped == 1

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_adds_description_when_target_has_none(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """The source description is applied when the target has no description."""
        src_props = DatasetPropertiesClass(description="source desc")
        dst_props = DatasetPropertiesClass(description="")
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        merged, skipped = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert skipped == 0
        assert merged > 0


# --- URN builder tests for all entity types ---


class TestUrnBuilders:
    """Tests for URN construction across all entity types."""

    def test_dataset_urn_builder(self):
        """i2i dataset builder swaps the instance prefix inside the dataset name."""
        make_urn = make_i2i_dataset_urn("old_inst", "new_inst")
        result = make_urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,old_inst.db.table,PROD)"
        )
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,new_inst.db.table,PROD)"
        )

    def test_chart_urn_builder(self):
        """i2i chart builder swaps the instance prefix in the chart id."""
        make_urn = make_i2i_chart_urn("old_inst", "new_inst")
        result = make_urn("urn:li:chart:(powerbi,old_inst.my_chart)")
        assert result == "urn:li:chart:(powerbi,new_inst.my_chart)"

    def test_dashboard_urn_builder(self):
        """i2i dashboard builder swaps the instance prefix in the dashboard id."""
        make_urn = make_i2i_dashboard_urn("old_inst", "new_inst")
        result = make_urn("urn:li:dashboard:(powerbi,old_inst.my_dashboard)")
        assert result == "urn:li:dashboard:(powerbi,new_inst.my_dashboard)"

    def test_dataflow_urn_builder(self):
        """i2i dataFlow builder swaps the instance prefix in the flow id."""
        make_urn = make_i2i_dataflow_urn("old_inst", "new_inst")
        result = make_urn("urn:li:dataFlow:(powerbi,old_inst.my_flow,PROD)")
        assert result == "urn:li:dataFlow:(powerbi,new_inst.my_flow,PROD)"

    def test_datajob_urn_builder(self):
        """i2i dataJob builder rewrites the embedded flow id and drops the old instance, keeping the task."""
        make_urn = make_i2i_datajob_urn("old_inst", "new_inst")
        result = make_urn(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,old_inst.my_dag,PROD),my_task)"
        )
        assert "new_inst.my_dag" in result
        assert "old_inst" not in result
        assert "my_task" in result

    def test_datajob_preserves_job_id(self):
        """dataJob migration rewrites the flow_id but preserves the job_id."""
        make_urn = make_i2i_datajob_urn("prod_af", "shared_af")
        result = make_urn(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,prod_af.etl_pipeline,PROD),load_step)"
        )
        assert "shared_af.etl_pipeline" in result
        assert "load_step" in result

    def test_preserves_complex_chart_id(self):
        """Chart builder swaps only the leading instance in a deeply dotted chart id."""
        make_urn = make_i2i_chart_urn("musement", "shared")
        result = make_urn("urn:li:chart:(powerbi,musement.reports.abc123.pages.page1)")
        assert result == "urn:li:chart:(powerbi,shared.reports.abc123.pages.page1)"


# --- Platform-to-instance URN builder tests ---


class TestP2iUrnBuilders:
    """Tests for dataplatform2instance URN builders (prepend instance)."""

    def test_p2i_dataset_urn(self):
        """p2i dataset builder prepends the instance to the dataset name."""
        make_urn = make_p2i_dataset_urn("myinst")
        result = make_urn(
            "urn:li:dataset:(urn:li:dataPlatform:powerbi,some.table,PROD)"
        )
        assert (
            result
            == "urn:li:dataset:(urn:li:dataPlatform:powerbi,myinst.some.table,PROD)"
        )

    def test_p2i_chart_urn(self):
        """p2i chart builder prepends the instance to the chart id."""
        make_urn = make_p2i_chart_urn("myinst")
        result = make_urn("urn:li:chart:(powerbi,my_chart)")
        assert result == "urn:li:chart:(powerbi,myinst.my_chart)"

    def test_p2i_dashboard_urn(self):
        """p2i dashboard builder prepends the instance to the dashboard id."""
        make_urn = make_p2i_dashboard_urn("myinst")
        result = make_urn("urn:li:dashboard:(powerbi,my_dashboard)")
        assert result == "urn:li:dashboard:(powerbi,myinst.my_dashboard)"

    def test_p2i_dataflow_urn(self):
        """p2i dataFlow builder prepends the instance to the flow id."""
        make_urn = make_p2i_dataflow_urn("myinst")
        result = make_urn("urn:li:dataFlow:(powerbi,my_flow,PROD)")
        assert result == "urn:li:dataFlow:(powerbi,myinst.my_flow,PROD)"

    def test_p2i_datajob_urn(self):
        """p2i dataJob builder prepends the instance to the embedded flow id, keeping the task."""
        make_urn = make_p2i_datajob_urn("myinst")
        result = make_urn(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,PROD),my_task)"
        )
        assert "myinst.my_dag" in result
        assert "my_task" in result


# --- skip-on-error and MigrationReport tests ---


class TestMigrationReportErrorTracking:
    """Tests for error tracking in MigrationReport."""

    def test_entities_errored_initially_empty(self):
        """A fresh MigrationReport starts with an empty entities_errored list."""
        report = MigrationReport("test", dry_run=True, keep=True)
        assert report.entities_errored == []

    def test_entities_errored_in_repr(self):
        """repr surfaces the errored count and each urn/error-message pair."""
        report = MigrationReport("test", dry_run=True, keep=True)
        report.entities_errored.append(("urn:li:dataset:foo", "some error"))
        text = repr(report)
        assert "Entities errored = 1" in text
        assert "urn:li:dataset:foo" in text
        assert "some error" in text

    def test_no_error_section_when_empty(self):
        """repr omits the errored section entirely when nothing errored."""
        report = MigrationReport("test", dry_run=True, keep=True)
        text = repr(report)
        assert "errored" not in text


# --- Non-dataset merge fallback tests ---


class TestMergeEntityNonDataset:
    """Verify merge_entity falls back to overwrite for non-dataset entity types."""

    CHART_SRC = "urn:li:chart:(powerbi,old_inst.my_chart)"
    CHART_DST = "urn:li:chart:(powerbi,new_inst.my_chart)"

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_chart_merge_falls_back_to_overwrite(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """Merging a chart takes the clone_aspect overwrite path (no PATCH skips)."""
        from datahub.cli.migration_utils import merge_entity

        # clone_aspect should be called (overwrite path), not DatasetPatchBuilder
        mock_clone.return_value = iter([])
        graph = MagicMock()

        result = merge_entity(
            self.CHART_SRC,
            self.CHART_DST,
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        mock_clone.assert_called_once()
        assert result.skipped == 0

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_overwrite_rewrites_urns_in_cloned_aspects(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """The overwrite fallback applies transform_urns so self-references
        (and batch cross-references) in cloned aspects are rewritten."""
        from datahub.cli.migration_utils import merge_entity
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        # Simulate a cloned aspect whose owner URN embeds the old chart URN.
        # transform_urns walks @Relationship/Urn fields and should rewrite it.
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=self.CHART_SRC,
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        mock_clone.return_value = iter(
            [MetadataChangeProposalWrapper(entityUrn=self.CHART_DST, aspect=aspect)]
        )
        graph = MagicMock()

        result = merge_entity(
            self.CHART_SRC,
            self.CHART_DST,
            ConflictStrategy.OVERWRITE,
            graph,
            dry_run=True,
        )

        assert result.merged == 1
        # The owner URN should have been rewritten from src to dst
        assert aspect.owners[0].owner == self.CHART_DST

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_dataflow_merge_falls_back_to_overwrite(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """Merging a dataFlow takes the clone_aspect overwrite path (no PATCH skips)."""
        from datahub.cli.migration_utils import merge_entity

        mock_clone.return_value = iter([])
        graph = MagicMock()

        result = merge_entity(
            "urn:li:dataFlow:(airflow,old.dag,PROD)",
            "urn:li:dataFlow:(airflow,new.dag,PROD)",
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        mock_clone.assert_called_once()
        assert result.skipped == 0

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_overwrite_excludes_status_aspect(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """The overwrite fallback (non-dataset merge) does not clone the status
        aspect — the target's own soft-delete state is authoritative."""
        from datahub.cli.migration_utils import merge_entity

        mock_clone.return_value = iter([])
        graph = MagicMock()

        merge_entity(
            self.CHART_SRC,
            self.CHART_DST,
            ConflictStrategy.OVERWRITE,
            graph,
            dry_run=True,
        )

        cloned_aspects = mock_clone.call_args.kwargs["aspect_names"]
        assert "status" not in cloned_aspects


class TestMergeExcludesStatus:
    """The merge path must never overwrite the target's status aspect."""

    MERGE_SRC = "urn:li:dataset:(urn:li:dataPlatform:snowflake,a1.db.t,PROD)"
    MERGE_DST = "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.t,PROD)"

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_dataset_merge_does_not_include_status(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """merge_entity for a dataset never writes the source's status aspect to
        the target — a soft-deleted source must not soft-delete a live target."""
        from datahub.cli.migration_utils import merge_entity
        from datahub.metadata.schema_classes import StatusClass

        mock_get_aspects.return_value = {
            "status": StatusClass(removed=True),
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
        }

        result = merge_entity(
            self.MERGE_SRC,
            self.MERGE_DST,
            ConflictStrategy.OVERWRITE,
            MagicMock(),
            dry_run=True,
        )

        assert "status" not in result.merged_aspects
        assert "globalTags" in result.merged_aspects

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_dataset_merge_does_not_include_container(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """merge_entity for a dataset never writes the source's container aspect
        to the target — the target's parent container is authoritative."""
        from datahub.cli.migration_utils import merge_entity
        from datahub.metadata.schema_classes import ContainerClass

        mock_get_aspects.return_value = {
            "container": ContainerClass(container="urn:li:container:old"),
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
        }

        result = merge_entity(
            self.MERGE_SRC,
            self.MERGE_DST,
            ConflictStrategy.OVERWRITE,
            MagicMock(),
            dry_run=True,
        )

        assert "container" not in result.merged_aspects
        assert "globalTags" in result.merged_aspects

    def test_clone_path_still_includes_status_and_container(self) -> None:
        """get_migratable_aspect_names includes status and container — the clone
        path (target does not exist) should carry both to the new entity."""
        assert "status" in get_migratable_aspect_names("dataset")
        assert "container" in get_migratable_aspect_names("dataset")
        assert "status" in get_migratable_aspect_names("chart")
