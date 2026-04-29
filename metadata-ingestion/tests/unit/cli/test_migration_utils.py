"""Tests for datahub.cli.migration_utils — relationship-to-aspect mapping and URN rewriting."""

import pytest

from datahub.cli.migration_utils import (
    get_aspect_name_from_relationship,
    modify_urn_list_for_aspect,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    EdgeClass,
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
