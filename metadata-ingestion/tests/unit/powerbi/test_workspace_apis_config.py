from typing import Any, Dict

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiMetadataApiMode,
)

BASE: Dict[str, Any] = {"tenant_id": "fake", "client_id": "foo", "client_secret": "bar"}


def test_workspace_apis_mode_accepts_lowercase_and_enrichment_flags() -> None:
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            **BASE,
            "metadata_api_mode": "workspace_apis",
            "extract_model_relationships": True,
            "extract_report_field_usage": True,
            "extract_refresh_history": True,
            "extract_dax_reference_lineage": True,
            "extract_composite_model_lineage": True,
        }
    )
    assert config.metadata_api_mode == PowerBiMetadataApiMode.WORKSPACE_APIS
    assert config.extract_model_relationships
    assert config.extract_report_field_usage
    assert config.extract_refresh_history


@pytest.mark.parametrize(
    "overrides,match",
    [
        pytest.param({"admin_apis_only": True}, "admin_apis_only", id="admin_only"),
        pytest.param(
            {"modified_since": "2023-02-10T00:00:00.0000000Z"},
            "modified_since",
            id="modified_since",
        ),
        pytest.param({"extract_app": True}, "extract_app", id="app"),
        pytest.param(
            {"extract_endorsements_to_tags": True},
            "extract_endorsements_to_tags",
            id="endorsements",
        ),
        pytest.param(
            {"filter_dataset_endorsements": {"allow": ["Certified"]}},
            "filter_dataset_endorsements",
            id="endorsement_filter",
        ),
        pytest.param({"environment": "GOVERNMENT"}, "COMMERCIAL", id="gcc"),
    ],
)
def test_workspace_apis_rejects_admin_only_features(
    overrides: Dict[str, Any], match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        PowerBiDashboardSourceConfig.model_validate(
            {**BASE, "metadata_api_mode": "workspace_apis", **overrides}
        )


@pytest.mark.parametrize(
    "overrides,match",
    [
        pytest.param(
            {"extract_model_relationships": True},
            "requires metadata_api_mode: workspace_apis",
            id="relationships_need_mode",
        ),
        pytest.param(
            {"extract_report_field_usage": True},
            "requires metadata_api_mode: workspace_apis",
            id="field_usage_needs_mode",
        ),
        pytest.param(
            {"extract_dataflows": True},
            "requires metadata_api_mode: workspace_apis",
            id="dataflows_need_mode",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_report_field_usage": True,
                "extract_reports": False,
            },
            "extract_reports",
            id="field_usage_needs_reports",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_model_relationships": True,
                "extract_dataset_schema": False,
                "extract_column_level_lineage": False,
            },
            "extract_dataset_schema",
            id="relationships_need_schema",
        ),
        pytest.param(
            {"extract_dax_reference_lineage": True},
            "requires metadata_api_mode: workspace_apis",
            id="dax_lineage_needs_mode",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_dax_reference_lineage": True,
                "extract_column_level_lineage": False,
            },
            "extract_column_level_lineage",
            id="dax_lineage_needs_column_lineage",
        ),
        pytest.param(
            {"extract_model_properties": True},
            "requires metadata_api_mode: workspace_apis",
            id="model_properties_need_mode",
        ),
        pytest.param(
            {"extract_composite_model_lineage": True},
            "requires metadata_api_mode: workspace_apis",
            id="composite_lineage_needs_mode",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_model_properties": True,
                "extract_dataset_schema": False,
                "extract_column_level_lineage": False,
            },
            "extract_dataset_schema",
            id="model_properties_need_schema",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_composite_model_lineage": True,
                "extract_lineage": False,
            },
            "extract_lineage",
            id="composite_lineage_needs_lineage",
        ),
        pytest.param(
            {
                "metadata_api_mode": "workspace_apis",
                "extract_report_visuals": True,
            },
            "extract_report_field_usage",
            id="visuals_need_field_usage",
        ),
    ],
)
def test_definition_enrichment_flag_dependencies(
    overrides: Dict[str, Any], match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        PowerBiDashboardSourceConfig.model_validate({**BASE, **overrides})


def test_refresh_history_allowed_in_admin_scan_but_not_admin_only() -> None:
    config = PowerBiDashboardSourceConfig.model_validate(
        {**BASE, "extract_refresh_history": True}
    )
    assert config.extract_refresh_history
    with pytest.raises(ValidationError, match="admin_apis_only"):
        PowerBiDashboardSourceConfig.model_validate(
            {**BASE, "extract_refresh_history": True, "admin_apis_only": True}
        )


@pytest.mark.parametrize(
    "overrides,match",
    [
        pytest.param(
            {"metadata_api_mode": "workspace_apis"},
            "always reads the item definitions",
            id="rejected_with_workspace_apis",
        ),
        pytest.param(
            {"admin_apis_only": True},
            "admin_apis_only",
            id="rejected_with_admin_apis_only",
        ),
        pytest.param({"environment": "GOVERNMENT"}, "COMMERCIAL", id="gcc"),
        # Dataflows are only listed in workspace_apis mode; the overlay cannot
        # unlock them.
        pytest.param(
            {"extract_dataflows": True},
            "extract_dataflows requires metadata_api_mode: workspace_apis",
            id="dataflows_not_unlocked_by_enrichment",
        ),
    ],
)
def test_enrich_from_item_definitions_rejections(
    overrides: Dict[str, Any], match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        PowerBiDashboardSourceConfig.model_validate(
            {**BASE, "enrich_from_item_definitions": True, **overrides}
        )


@pytest.mark.parametrize(
    "flag,extra",
    [
        pytest.param("extract_model_relationships", {}, id="relationships"),
        pytest.param(
            "extract_report_field_usage", {"extract_reports": True}, id="field_usage"
        ),
        pytest.param("extract_dax_reference_lineage", {}, id="dax_lineage"),
        pytest.param("extract_model_properties", {}, id="model_properties"),
        pytest.param(
            "extract_composite_model_lineage", {}, id="composite_model_lineage"
        ),
    ],
)
def test_enrichment_flags_accepted_in_admin_scan_with_item_definitions(
    flag: str, extra: Dict[str, Any]
) -> None:
    config = PowerBiDashboardSourceConfig.model_validate(
        {**BASE, "enrich_from_item_definitions": True, flag: True, **extra}
    )
    assert config.metadata_api_mode == PowerBiMetadataApiMode.ADMIN_SCAN
    assert config.item_definitions_available
    assert getattr(config, flag)
    # Without the opt-in the same flag is still rejected in admin_scan mode,
    # and the error names both ways to unlock it.
    with pytest.raises(ValidationError, match="enrich_from_item_definitions"):
        PowerBiDashboardSourceConfig.model_validate({**BASE, flag: True, **extra})
