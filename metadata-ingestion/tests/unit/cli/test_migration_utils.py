"""Tests for datahub.cli.migration_utils — relationship-to-aspect mapping and URN rewriting."""

from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
from avrogen.dict_wrapper import DictWrapper

from datahub.cli.migrate import (
    MigrationReport,
)
from datahub.cli.migration_utils import (
    ConflictStrategy,
    get_aspect_name_from_relationship,
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
    merge_additive_aspects,
    merge_mixed_aspects,
    modify_urn_list_for_aspect,
    replace_instance_prefix,
    should_overwrite_non_additive,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    EdgeClass,
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

# --- get_aspect_name_from_relationship tests ---


class TestGetAspectNameFromRelationship:
    """Tests for the relationship-type-to-aspect-name mapping."""

    def test_consumes_dashboard_returns_dashboard_info(self):
        """Dashboard entities with Consumes relationship should map to dashboardInfo."""
        result = get_aspect_name_from_relationship("Consumes", "dashboard")
        assert result == "dashboardInfo"

    def test_consumes_chart_returns_chart_info(self):
        result = get_aspect_name_from_relationship("Consumes", "chart")
        assert result == "chartInfo"

    def test_consumes_datajob_returns_data_job_input_output(self):
        result = get_aspect_name_from_relationship("Consumes", "datajob")
        assert result == "dataJobInputOutput"

    def test_downstream_of_dataset_returns_upstream_lineage(self):
        result = get_aspect_name_from_relationship("DownstreamOf", "dataset")
        assert result == "upstreamLineage"

    def test_is_part_of_dataset_returns_container(self):
        result = get_aspect_name_from_relationship("IsPartOf", "dataset")
        assert result == "container"

    def test_unknown_relationship_raises(self):
        with pytest.raises(Exception, match="Unable to map aspect name"):
            get_aspect_name_from_relationship("UnknownRelType", "dataset")

    def test_unknown_entity_type_raises(self):
        with pytest.raises(Exception, match="Unable to map aspect name"):
            get_aspect_name_from_relationship("Consumes", "unknownEntity")


# --- DashboardInfo modifier tests ---


def _make_audit_stamps() -> ChangeAuditStampsClass:
    stamp = AuditStampClass(time=0, actor="urn:li:corpuser:test")
    return ChangeAuditStampsClass(created=stamp, lastModified=stamp)


class TestDashboardInfoModifier:
    """UrnListModifier must handle dashboardInfo for Consumes relationships."""

    OLD_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,myDataset,PROD)"
    NEW_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,instance.myDataset,PROD)"

    def test_rewrites_deprecated_datasets_field(self):
        """The deprecated 'datasets' array should have old URNs replaced."""
        dashboard_info = DashboardInfoClass(
            title="Test Dashboard",
            description="",
            lastModified=_make_audit_stamps(),
            datasets=[
                self.OLD_URN,
                "urn:li:dataset:(urn:li:dataPlatform:powerbi,other,PROD)",
            ],
        )

        result = modify_urn_list_for_aspect(
            "dashboardInfo", dashboard_info, "Consumes", self.OLD_URN, self.NEW_URN
        )

        assert isinstance(result, DashboardInfoClass)
        assert self.NEW_URN in result.datasets
        assert self.OLD_URN not in result.datasets
        # Other URNs should be untouched
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:powerbi,other,PROD)" in result.datasets
        )

    def test_rewrites_dataset_edges_field(self):
        """The newer 'datasetEdges' array should have old URNs replaced."""
        dashboard_info = DashboardInfoClass(
            title="Test Dashboard",
            description="",
            lastModified=_make_audit_stamps(),
            datasetEdges=[
                EdgeClass(destinationUrn=self.OLD_URN),
                EdgeClass(
                    destinationUrn="urn:li:dataset:(urn:li:dataPlatform:powerbi,other,PROD)"
                ),
            ],
        )

        result = modify_urn_list_for_aspect(
            "dashboardInfo", dashboard_info, "Consumes", self.OLD_URN, self.NEW_URN
        )

        assert isinstance(result, DashboardInfoClass)
        assert result.datasetEdges is not None
        dest_urns = [e.destinationUrn for e in result.datasetEdges]
        assert self.NEW_URN in dest_urns
        assert self.OLD_URN not in dest_urns

    def test_handles_empty_datasets(self):
        """Should not crash when datasets and datasetEdges are empty/None."""
        dashboard_info = DashboardInfoClass(
            title="Test Dashboard",
            description="",
            lastModified=_make_audit_stamps(),
        )

        result = modify_urn_list_for_aspect(
            "dashboardInfo", dashboard_info, "Consumes", self.OLD_URN, self.NEW_URN
        )

        assert isinstance(result, DashboardInfoClass)


# --- instance2instance helper tests ---


class TestReplaceInstancePrefix:
    """Tests for the instance prefix replacement logic used by instance2instance."""

    def test_replaces_old_prefix_with_new(self):
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
        result = replace_instance_prefix("old_inst.table", "old_inst", "new_inst")
        assert result == "new_inst.table"

    def test_preserves_complex_names(self):
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
        owner = OwnerClass(
            owner="urn:li:corpuser:alice", type=OwnershipTypeClass.DATAOWNER
        )
        src_aspects: Dict[str, DictWrapper] = {
            "ownership": OwnershipClass(owners=[owner])
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, True)

        assert result > 0

    def test_merges_tags(self):
        tag = TagAssociationClass(tag="urn:li:tag:pii")
        src_aspects: Dict[str, DictWrapper] = {
            "globalTags": GlobalTagsClass(tags=[tag])
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, True)

        assert result > 0

    def test_merges_terms(self):
        term = GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:Revenue")
        src_aspects: Dict[str, DictWrapper] = {
            "glossaryTerms": GlossaryTermsClass(
                terms=[term],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:test"),
            )
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, True)

        assert result > 0

    def test_merges_lineage(self):
        upstream = UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,src.table,PROD)",
            type="TRANSFORMED",
        )
        src_aspects: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(upstreams=[upstream])
        }
        graph = MagicMock()

        result = merge_additive_aspects(src_aspects, self.DST_URN, graph, True)

        assert result > 0

    def test_empty_aspects_no_patches(self):
        """No patches should be emitted when there are no additive aspects."""
        graph = MagicMock()

        result = merge_additive_aspects({}, self.DST_URN, graph, True)

        assert result == 0

    def test_dry_run_does_not_emit(self):
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
        make_urn = make_i2i_dataset_urn("old_inst", "new_inst")
        result = make_urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,old_inst.db.table,PROD)"
        )
        assert "new_inst.db.table" in result
        assert "old_inst" not in result

    def test_chart_urn_builder(self):
        make_urn = make_i2i_chart_urn("old_inst", "new_inst")
        result = make_urn("urn:li:chart:(powerbi,old_inst.my_chart)")
        assert result == "urn:li:chart:(powerbi,new_inst.my_chart)"

    def test_dashboard_urn_builder(self):
        make_urn = make_i2i_dashboard_urn("old_inst", "new_inst")
        result = make_urn("urn:li:dashboard:(powerbi,old_inst.my_dashboard)")
        assert result == "urn:li:dashboard:(powerbi,new_inst.my_dashboard)"

    def test_dataflow_urn_builder(self):
        make_urn = make_i2i_dataflow_urn("old_inst", "new_inst")
        result = make_urn("urn:li:dataFlow:(powerbi,old_inst.my_flow,PROD)")
        assert result == "urn:li:dataFlow:(powerbi,new_inst.my_flow,PROD)"

    def test_datajob_urn_builder(self):
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
        make_urn = make_i2i_chart_urn("musement", "shared")
        result = make_urn("urn:li:chart:(powerbi,musement.reports.abc123.pages.page1)")
        assert result == "urn:li:chart:(powerbi,shared.reports.abc123.pages.page1)"


# --- Platform-to-instance URN builder tests ---


class TestP2iUrnBuilders:
    """Tests for dataplatform2instance URN builders (prepend instance)."""

    def test_p2i_dataset_urn(self):
        make_urn = make_p2i_dataset_urn("myinst")
        result = make_urn(
            "urn:li:dataset:(urn:li:dataPlatform:powerbi,some.table,PROD)"
        )
        assert "myinst.some.table" in result

    def test_p2i_chart_urn(self):
        make_urn = make_p2i_chart_urn("myinst")
        result = make_urn("urn:li:chart:(powerbi,my_chart)")
        assert result == "urn:li:chart:(powerbi,myinst.my_chart)"

    def test_p2i_dashboard_urn(self):
        make_urn = make_p2i_dashboard_urn("myinst")
        result = make_urn("urn:li:dashboard:(powerbi,my_dashboard)")
        assert result == "urn:li:dashboard:(powerbi,myinst.my_dashboard)"

    def test_p2i_dataflow_urn(self):
        make_urn = make_p2i_dataflow_urn("myinst")
        result = make_urn("urn:li:dataFlow:(powerbi,my_flow,PROD)")
        assert result == "urn:li:dataFlow:(powerbi,myinst.my_flow,PROD)"

    def test_p2i_datajob_urn(self):
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
        report = MigrationReport("test", dry_run=True, keep=True)
        assert report.entities_errored == []

    def test_entities_errored_in_repr(self):
        report = MigrationReport("test", dry_run=True, keep=True)
        report.entities_errored.append(("urn:li:dataset:foo", "some error"))
        text = repr(report)
        assert "Entities errored = 1" in text
        assert "urn:li:dataset:foo" in text
        assert "some error" in text

    def test_no_error_section_when_empty(self):
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
        from datahub.cli.migration_utils import merge_entity

        # clone_aspect should be called (overwrite path), not DatasetPatchBuilder
        mock_clone.return_value = iter([])
        graph = MagicMock()

        merged, skipped = merge_entity(
            self.CHART_SRC,
            self.CHART_DST,
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        mock_clone.assert_called_once()
        assert skipped == 0

    @patch("datahub.cli.migration_utils.clone_aspect")
    def test_dataflow_merge_falls_back_to_overwrite(
        self,
        mock_clone: MagicMock,
    ) -> None:
        from datahub.cli.migration_utils import merge_entity

        mock_clone.return_value = iter([])
        graph = MagicMock()

        merged, skipped = merge_entity(
            "urn:li:dataFlow:(airflow,old.dag,PROD)",
            "urn:li:dataFlow:(airflow,new.dag,PROD)",
            ConflictStrategy.PATCH,
            graph,
            dry_run=True,
        )

        mock_clone.assert_called_once()
        assert skipped == 0
