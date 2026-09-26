"""Tests for datahub.cli.migration_utils — relationship-to-aspect mapping and URN rewriting."""

import logging
from typing import Callable, Dict
from unittest.mock import MagicMock, patch

import pytest
from avrogen.dict_wrapper import DictWrapper

import datahub.cli.migration_utils as migration_utils
from datahub.cli.migration_utils import (
    get_migratable_aspect_names,
    merge_additive_aspects,
    merge_entity,
    merge_mixed_aspects,
    should_overwrite_non_additive,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ENTITY_TYPE_TO_ASPECT_NAMES,
    AuditStampClass,
    ChangeAuditStampsClass,
    ContainerClass,
    ContainerPropertiesClass,
    CorpGroupInfoClass,
    DatasetPropertiesClass,
    DeprecationClass,
    DomainPropertiesClass,
    GlobalTagsClass,
    GlossaryNodeInfoClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MLModelPropertiesClass,
    NotebookInfoClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.migration.models import ConflictStrategy, MergeResult, MigrationReport
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

    @patch("datahub.cli.migration_utils.click.prompt")
    def test_prompt_strategy_follows_user_choice(self, mock_prompt: MagicMock) -> None:
        """PROMPT resolves the conflict interactively: 's' takes source, 't' keeps
        target. This is the only path that reaches click.prompt."""
        src = self._make_props("source desc")
        dst = self._make_props("target desc")
        args = (
            "datasetProperties",
            src,
            dst,
            self.SRC_URN,
            self.DST_URN,
            ConflictStrategy.PROMPT,
        )

        mock_prompt.return_value = "s"
        assert should_overwrite_non_additive(*args)

        mock_prompt.return_value = "t"
        assert not should_overwrite_non_additive(*args)

    @patch("datahub.cli.migration_utils.click.prompt")
    def test_prompt_strategy_scalar_follows_user_choice(
        self, mock_prompt: MagicMock
    ) -> None:
        args = ("description", "source", "target", self.SRC_URN, self.DST_URN)

        mock_prompt.return_value = "s"
        assert migration_utils.should_overwrite_scalar(*args, ConflictStrategy.PROMPT)

        mock_prompt.return_value = "t"
        assert not migration_utils.should_overwrite_scalar(
            *args, ConflictStrategy.PROMPT
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

        assert result == ["ownership"]
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

        assert result == ["globalTags"]
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

        assert result == ["glossaryTerms"]
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

        assert result == ["upstreamLineage"]
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

        assert result == ["upstreamLineage"]
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

        assert result == []
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

        assert result == ["upstreamLineage"]
        graph.emit.assert_not_called()
        graph.emit_mcp.assert_not_called()

    def test_empty_aspects_no_patches(self):
        """No patches should be emitted when there are no additive aspects."""
        graph = MagicMock()

        result = merge_additive_aspects({}, self.DST_URN, graph, True)

        assert result == []

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
        result = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert result.merged > 0
        assert result.skipped == 0

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
        result = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert result.skipped == 1

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
        result = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.OVERWRITE,
            True,
        )
        assert result.skipped == 0
        assert result.merged > 0

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_skips_description_conflict_in_patch_mode(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A description that differs from the target's is skipped under PATCH."""
        src_props = DatasetPropertiesClass(description="source desc")
        dst_props = DatasetPropertiesClass(description="target desc")
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        result = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert result.skipped == 1

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_adds_description_when_target_has_none(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """The source description is applied when the target has no description."""
        src_props = DatasetPropertiesClass(description="source desc")
        dst_props = DatasetPropertiesClass(description="")
        mock_get_aspects.return_value = {"datasetProperties": dst_props}
        result = merge_mixed_aspects(
            {"datasetProperties": src_props},
            self.DST_URN,
            self.SRC_URN,
            MagicMock(),
            ConflictStrategy.PATCH,
            True,
        )
        assert result.skipped == 0
        assert result.merged > 0


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
    """merge_entity for non-datasets: builder-backed types (chart/dashboard/dataFlow/
    dataJob/dataProduct) overwrite under every strategy; other types union under
    PATCH/PROMPT and overwrite under OVERWRITE."""

    CHART_SRC = "urn:li:chart:(powerbi,old_inst.my_chart)"
    CHART_DST = "urn:li:chart:(powerbi,new_inst.my_chart)"

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_chart_patch_falls_back_to_overwrite(
        self,
        mock_clone: MagicMock,
    ) -> None:
        # chart lineage lives in chartInfo, which the entity-agnostic builder can't
        # union, so PATCH fully overwrites rather than taking the additive path.
        aspect = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pii")])
        mock_clone.return_value = iter(
            [MetadataChangeProposalWrapper(entityUrn=self.CHART_DST, aspect=aspect)]
        )
        graph = MagicMock()

        result = merge_entity(
            self.CHART_SRC,
            self.CHART_DST,
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        mock_clone.assert_called_once()
        graph.emit.assert_not_called()  # overwrite emits full aspects, not Patch MCPs
        assert result.merged == 1
        assert "globalTags" in result.merged_aspects

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_overwrite_rewrites_urns_in_cloned_aspects(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """The overwrite fallback applies transform_urns so cloned self-references are rewritten."""
        # transform_urns walks @Relationship/Urn fields; the owner URN embeds the old chart URN.
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
    def test_dataflow_patch_falls_back_to_overwrite(
        self,
        mock_clone: MagicMock,
    ) -> None:
        # dataFlow lineage lives in dataFlowInfo (non-unionable), so PATCH overwrites
        # the target with the source rather than keeping the conflicting target value.
        actor = "urn:li:corpuser:datahub"
        aspect = DeprecationClass(deprecated=True, note="src", actor=actor)
        mock_clone.return_value = iter(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataFlow:(airflow,new.dag,PROD)", aspect=aspect
                )
            ]
        )
        graph = MagicMock()

        result = merge_entity(
            "urn:li:dataFlow:(airflow,old.dag,PROD)",
            "urn:li:dataFlow:(airflow,new.dag,PROD)",
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        mock_clone.assert_called_once()
        # Overwrite applies the source and never skips on conflict.
        assert result.merged == 1
        assert result.skipped == 0
        assert "deprecation" in result.merged_aspects

    def test_builder_backed_types_never_take_generic_path(self) -> None:
        # Regression guard for the i2i default (patch): chart/dashboard/dataFlow/
        # dataJob/dataProduct must overwrite so a migrated source's lineage reaches
        # the target instead of being stranded by an additive keep-target merge.
        assert (
            frozenset({"chart", "dashboard", "dataFlow", "dataJob", "dataProduct"})
            == migration_utils.NON_ADDITIVE_MERGE_ENTITY_TYPES
        )
        for entity_type in migration_utils.NON_ADDITIVE_MERGE_ENTITY_TYPES:
            with (
                patch(
                    "datahub.cli.migration_utils._overwrite_entity"
                ) as mock_overwrite,
                patch(
                    "datahub.cli.migration_utils._merge_generic_entity"
                ) as mock_generic,
            ):
                mock_overwrite.return_value = MergeResult(merged=0, skipped=0)
                merge_entity(
                    f"urn:li:{entity_type}:(powerbi,old.x)",
                    f"urn:li:{entity_type}:(powerbi,new.x)",
                    ConflictStrategy.PATCH,
                    MagicMock(),
                    dry_run=True,
                )
                mock_overwrite.assert_called_once()
                mock_generic.assert_not_called()

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_overwrite_excludes_status_aspect(
        self,
        mock_clone: MagicMock,
    ) -> None:
        """The non-dataset overwrite fallback does not clone status (target's soft-delete state wins)."""
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


class TestMergeGenericEntity:
    """The additive union path for non-dataset entities (e.g. schemaField)."""

    SF_SRC = (
        "urn:li:schemaField:"
        "(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.t,PROD),col_a)"
    )
    SF_DST = (
        "urn:li:schemaField:"
        "(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.t,PROD),Col_A)"
    )

    @patch("datahub.cli.migration_utils.clone_aspect")
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_schemafield_patch_unions_tags_terms_and_structured_properties(
        self,
        mock_get_aspects: MagicMock,
        mock_clone: MagicMock,
    ) -> None:
        """A schemaField merge unions tags, terms and structured properties."""
        mock_get_aspects.return_value = {
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
            "glossaryTerms": GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:pii")],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            ),
            "structuredProperties": StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn="urn:li:structuredProperty:tier",
                        values=["gold"],
                    )
                ]
            ),
        }
        graph = MagicMock()

        result = merge_entity(
            self.SF_SRC,
            self.SF_DST,
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        mock_clone.assert_not_called()
        assert set(result.merged_aspects) == {
            "globalTags",
            "glossaryTerms",
            "structuredProperties",
        }
        emitted = _emitted_patch_values(graph)
        assert {name for name, _ in emitted} == {
            "globalTags",
            "glossaryTerms",
            "structuredProperties",
        }
        # The union is load-bearing on arrayPrimaryKeys: GMS only unions (rather
        # than clobbers) when the patch carries the {"arrayPrimaryKeys": ...,
        # "patch": ...} envelope. A bare JSON-Patch array would still be a valid
        # PATCH MCP and pass the name check above while clobbering in production.
        for _name, body in emitted:
            assert "arrayPrimaryKeys" in body

    def test_structured_properties_union_for_dataset(self) -> None:
        """structuredProperties now unions on the dataset path too (not clobbered)."""
        src = StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn="urn:li:structuredProperty:tier",
                    values=["gold"],
                )
            ]
        )
        graph = MagicMock()
        n = merge_additive_aspects(
            {"structuredProperties": src},
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.db.t,PROD)",
            graph,
            dry_run=False,
        )
        assert n == ["structuredProperties"]
        emitted = _emitted_patch_values(graph)
        assert [name for name, _ in emitted] == ["structuredProperties"]
        assert "arrayPrimaryKeys" in emitted[0][1]

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_upstream_lineage_copied_not_dropped(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """upstreamLineage on a non-dataset carrier is copied, not silently dropped.

        semanticModel is a real upstreamLineage carrier (dataset/semanticModel/aiAgent
        are the only three), so guess_entity_type + the registry agree with the mock.
        """
        upstream = UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.up,PROD)",
            type="TRANSFORMED",
        )
        mock_get_aspects.side_effect = [
            {"upstreamLineage": UpstreamLineageClass(upstreams=[upstream])},  # src
            {},  # dst has no upstreamLineage yet
        ]
        graph = MagicMock()

        result = merge_entity(
            "urn:li:semanticModel:(urn:li:dataPlatform:looker,old)",
            "urn:li:semanticModel:(urn:li:dataPlatform:looker,new)",
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        assert "upstreamLineage" in result.merged_aspects
        assert graph.emit_mcp.called

    @staticmethod
    def _schema_metadata(name: str) -> SchemaMetadataClass:
        return SchemaMetadataClass(
            schemaName=name,
            platform="urn:li:dataPlatform:glossary",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[],
        )

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_non_additive_aspect_on_non_dataset_is_copied_not_dropped(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """schemaMetadata (NON_ADDITIVE) on a glossaryTerm survives a PATCH merge.

        This is the exact regression the complement refactor closes: it lives in
        neither the union-able nor always-overwrite bucket, and the old code excluded
        it from the default bucket, so it was silently dropped before the source got
        deleted. glossaryTerm is the one registry entity that carries it off-dataset.
        """
        mock_get_aspects.side_effect = [
            {"schemaMetadata": self._schema_metadata("src")},  # src fetch
            {},  # dst has none yet -> copied
        ]
        graph = MagicMock()

        result = merge_entity(
            "urn:li:glossaryTerm:old",
            "urn:li:glossaryTerm:new",
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        assert "schemaMetadata" in result.merged_aspects
        assert graph.emit_mcp.called

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_non_additive_conflict_keeps_target_under_patch(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A conflicting NON_ADDITIVE aspect keeps the target under PATCH."""
        mock_get_aspects.side_effect = [
            {"schemaMetadata": self._schema_metadata("src")},  # src
            {"schemaMetadata": self._schema_metadata("dst")},  # dst differs
        ]
        graph = MagicMock()

        result = merge_entity(
            "urn:li:glossaryTerm:old",
            "urn:li:glossaryTerm:new",
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        assert "schemaMetadata" in result.skipped_aspects
        graph.emit_mcp.assert_not_called()

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_generic_merge_dry_run_emits_nothing(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """dry_run must not emit across any of the generic path's write points."""
        mock_get_aspects.return_value = {
            "containerProperties": ContainerPropertiesClass(name="db.sch"),
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
        }
        graph = MagicMock()

        merge_entity(
            "urn:li:container:oldguid",
            "urn:li:container:newguid",
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        graph.emit.assert_not_called()
        graph.emit_mcp.assert_not_called()

    def test_unknown_entity_type_raises_rather_than_fetching_everything(self) -> None:
        """An entity type the CLI can't model must fail loudly, not fetch+delete all.

        get_aspects_for_entity treats an empty aspect list as 'no filter', so without
        this guard the merge would copy every system aspect and then delete the source.
        """
        with pytest.raises(ValueError, match="no migratable aspects"):
            merge_entity(
                "urn:li:madeUpEntity:old",
                "urn:li:madeUpEntity:new",
                ConflictStrategy.PATCH,
                MagicMock(),
                dry_run=True,
            )

    def test_generic_buckets_are_disjoint_and_registry_is_populated(self) -> None:
        """The three explicit generic buckets never overlap (a double bucket would
        double-write), and the registry actually models the aspects we classify.
        """
        unioned = migration_utils._GENERIC_UNIONABLE_ASPECTS
        reseated = migration_utils.ALWAYS_OVERWRITE_ASPECTS
        excluded = migration_utils.MERGE_EXCLUDED_ASPECTS

        assert unioned.isdisjoint(reseated)
        assert unioned.isdisjoint(excluded)
        assert reseated.isdisjoint(excluded)

        # Sanity: the loop below is only meaningful if the registry is populated.
        generic_types = [
            t
            for t in ENTITY_TYPE_TO_ASPECT_NAMES
            if t != "dataset"
            and t not in migration_utils.NON_ADDITIVE_MERGE_ENTITY_TYPES
        ]
        assert generic_types
        all_generic_aspects = {
            a for t in generic_types for a in get_migratable_aspect_names(t)
        }
        assert all_generic_aspects

        # Concrete classifications the generic path relies on. If the registry drops
        # one of these, or a refactor moves it out of its bucket, this fails.
        assert {"ownership", "structuredProperties"} <= unioned
        assert "containerProperties" in reseated
        assert {"status", "container"} <= excluded

        # Identity aspects for the entity types this PR flipped to additive-merge fall
        # to the copied (conflict-aware) complement — not unioned, reseated, excluded.
        handled = unioned | reseated | excluded
        for props in (
            "glossaryTermInfo",
            "glossaryNodeInfo",
            "domainProperties",
            "tagProperties",
        ):
            assert props in all_generic_aspects, props
            assert props not in handled, props

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_container_patch_reseats_container_properties(
        self,
        mock_get_aspects: MagicMock,
    ) -> None:
        """A container merged via urns-mapping (PATCH) still reseats containerProperties."""
        mock_get_aspects.return_value = {
            "containerProperties": ContainerPropertiesClass(name="db.sch"),
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
        }
        graph = MagicMock()

        result = merge_entity(
            "urn:li:container:oldguid",
            "urn:li:container:newguid",
            ConflictStrategy.PATCH,
            graph,
            dry_run=False,
        )

        # containerProperties is always reseated; globalTags unions.
        assert "containerProperties" in result.merged_aspects
        assert "globalTags" in result.merged_aspects
        assert graph.emit_mcp.called  # full-aspect emit for containerProperties
        assert graph.emit.called  # patch emit for the tag union

    # (src_urn, dst_urn, identity aspect, factory) for each entity type this PR
    # flipped from full-overwrite to additive-merge on the generic path.
    _SCOPE_CASES = [
        (
            "urn:li:domain:old",
            "urn:li:domain:new",
            "domainProperties",
            lambda m: DomainPropertiesClass(name=m),
        ),
        (
            "urn:li:tag:old",
            "urn:li:tag:new",
            "tagProperties",
            lambda m: TagPropertiesClass(name=m),
        ),
        (
            "urn:li:mlModel:(urn:li:dataPlatform:science,old,PROD)",
            "urn:li:mlModel:(urn:li:dataPlatform:science,new,PROD)",
            "mlModelProperties",
            lambda m: MLModelPropertiesClass(description=m),
        ),
        (
            "urn:li:corpGroup:old",
            "urn:li:corpGroup:new",
            "corpGroupInfo",
            lambda m: CorpGroupInfoClass(
                admins=[], members=[], groups=[], displayName=m
            ),
        ),
        (
            "urn:li:glossaryNode:old",
            "urn:li:glossaryNode:new",
            "glossaryNodeInfo",
            lambda m: GlossaryNodeInfoClass(definition=m),
        ),
        (
            "urn:li:notebook:(querybook,old)",
            "urn:li:notebook:(querybook,new)",
            "notebookInfo",
            lambda m: NotebookInfoClass(
                title=m, changeAuditStamps=ChangeAuditStampsClass()
            ),
        ),
    ]

    @pytest.mark.parametrize("src_urn,dst_urn,props_aspect,make_props", _SCOPE_CASES)
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_scope_expansion_keeps_conflicting_identity_and_unions_tags(
        self,
        mock_get_aspects: MagicMock,
        src_urn: str,
        dst_urn: str,
        props_aspect: str,
        make_props: Callable[[str], object],
    ) -> None:
        """The types this PR flipped to additive-merge must union union-able aspects
        AND keep the target's own identity aspect on a PATCH conflict — the whole
        point of not overwriting these wholesale is that curated target data survives.
        """
        src_map = {
            "globalTags": GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:pii")]
            ),
            props_aspect: make_props("source"),
        }
        mock_get_aspects.side_effect = [src_map, {props_aspect: make_props("target")}]
        graph = MagicMock()

        result = merge_entity(
            src_urn, dst_urn, ConflictStrategy.PATCH, graph, dry_run=False
        )

        assert "globalTags" in result.merged_aspects
        assert props_aspect in result.skipped_aspects
        assert props_aspect not in result.merged_aspects

    @pytest.mark.parametrize("src_urn,dst_urn,props_aspect,make_props", _SCOPE_CASES)
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_scope_expansion_copies_identity_when_target_absent(
        self,
        mock_get_aspects: MagicMock,
        src_urn: str,
        dst_urn: str,
        props_aspect: str,
        make_props: Callable[[str], object],
    ) -> None:
        """When the target has no identity aspect yet, the source's is copied over
        rather than silently dropped before the source is deleted.
        """
        mock_get_aspects.side_effect = [{props_aspect: make_props("source")}, {}]
        graph = MagicMock()

        result = merge_entity(
            src_urn, dst_urn, ConflictStrategy.PATCH, graph, dry_run=False
        )

        assert props_aspect in result.merged_aspects
        assert graph.emit_mcp.called

    def test_apply_union_patches_isolates_a_bad_item(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """One item that fails to add must not void the entity's other additive
        items (owners, good tags, terms) — matching the batch's per-item isolation.
        """
        builder = MagicMock()

        def _add_tag(tag: TagAssociationClass) -> None:
            if tag.tag == "urn:li:tag:bad":
                raise ValueError("simulated bad tag")

        builder.add_tag.side_effect = _add_tag
        src: Dict[str, DictWrapper] = {
            "ownership": OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:a", type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            ),
            "globalTags": GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag="urn:li:tag:bad"),
                    TagAssociationClass(tag="urn:li:tag:good"),
                ]
            ),
            "glossaryTerms": GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:x")],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:t"),
            ),
        }

        with caplog.at_level(logging.WARNING):
            migration_utils._apply_union_patches(
                builder, src, "urn:li:schemaField:(urn:li:dataset:x,y)"
            )

        # The bad tag raised but owners, terms, and the good tag were still attempted.
        builder.add_owner.assert_called_once()
        builder.add_term.assert_called_once()
        added_tags = {c.args[0].tag for c in builder.add_tag.call_args_list}
        assert added_tags == {"urn:li:tag:bad", "urn:li:tag:good"}
        # A skipped item is warned, never silent.
        assert any("Skipping a tag" in r.message for r in caplog.records)

    def test_apply_union_patches_raises_when_all_items_fail(self) -> None:
        """Every item of an aspect failing signals a systemic bug (e.g. a schema
        mismatch in the mixin), not bad data — abort so the caller doesn't delete the
        source and report success with the aspect silently gone.
        """
        builder = MagicMock()
        builder.add_tag.side_effect = ValueError("mixin schema mismatch")
        src: Dict[str, DictWrapper] = {
            "globalTags": GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag="urn:li:tag:a"),
                    TagAssociationClass(tag="urn:li:tag:b"),
                ]
            ),
        }
        with pytest.raises(RuntimeError, match="All 2 tag"):
            migration_utils._apply_union_patches(
                builder, src, "urn:li:schemaField:(urn:li:dataset:x,y)"
            )

    def test_additive_patch_builder_rejects_non_generic_patch(self) -> None:
        """A patch without arrayPrimaryKeys would be silently dropped by GMS on a
        non-dataset entity, so the builder fails at merge time instead.
        """
        builder = migration_utils._AdditivePatchBuilder(
            "urn:li:schemaField:(urn:li:dataset:x,y)"
        )
        builder._add_patch(
            "globalTags", "add", ("tags", "urn:li:tag:x"), {"tag": "urn:li:tag:x"}
        )
        with pytest.raises(TypeError, match="non-generic patch"):
            builder.build()

    def test_overwrite_unknown_entity_type_raises(self) -> None:
        """OVERWRITE on an unmodeled entity type must fail loudly, not write zero
        aspects and delete the source (the _overwrite_entity guard)."""
        with pytest.raises(ValueError, match="no migratable aspects"):
            merge_entity(
                "urn:li:madeUpEntity:old",
                "urn:li:madeUpEntity:new",
                ConflictStrategy.OVERWRITE,
                MagicMock(),
                dry_run=True,
            )


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
