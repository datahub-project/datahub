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
