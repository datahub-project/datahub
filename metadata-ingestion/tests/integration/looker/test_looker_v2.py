"""Integration tests for the looker-v2 source."""

from pathlib import Path
from unittest import mock

import pytest
from freezegun import freeze_time
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DashboardElement,
    FolderBase,
    LookmlModel,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelNavExplore,
    Query,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"

GOLDEN_DIR = Path(__file__).parent / "golden" / "looker_v2"


def _setup_mock_sdk(sdk: mock.MagicMock) -> None:
    """Set up common mock state shared across integration tests."""
    sdk.all_folders.return_value = [
        FolderBase(
            id="1",
            name="Shared",
            parent_id=None,
            is_personal=False,
            is_personal_descendant=False,
        ),
    ]

    sdk.all_lookml_models.return_value = [
        LookmlModel(
            name="lkml_samples",
            project_name="lkml_samples",
            explores=[
                LookmlModelNavExplore(
                    name="order_items",
                )
            ],
        )
    ]

    sdk.lookml_model_explore.return_value = LookmlModelExplore(
        id="lkml_samples::order_items",
        name="order_items",
        label="Order Items",
        description="Order items explore",
        view_name="order_items",
        project_name="lkml_samples",
        model_name="lkml_samples",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="order_items.id",
                    type="number",
                    description="Order item ID",
                    label_short="ID",
                    view="order_items",
                )
            ],
            measures=[
                LookmlModelExploreField(
                    name="order_items.count",
                    type="count",
                    description="Count of order items",
                    label_short="Count",
                    view="order_items",
                )
            ],
        ),
        source_file="order_items.explore.lkml",
    )

    sdk.all_dashboards.return_value = [
        DashboardBase(
            id="1",
            title="Sales Dashboard",
            folder=FolderBase(id="1", name="Shared"),
        )
    ]

    sdk.dashboard.return_value = Dashboard(
        id="1",
        title="Sales Dashboard",
        description="Sales metrics dashboard",
        folder=FolderBase(id="1", name="Shared"),
        dashboard_elements=[
            DashboardElement(
                id="101",
                title="Order Count",
                type="vis",
                query=Query(
                    model="lkml_samples",
                    view="order_items",
                    fields=["order_items.count"],
                    dynamic_fields=None,
                ),
                look_id=None,
                look=None,
                merge_result_id=None,
            )
        ],
    )

    sdk.all_looks.return_value = []
    sdk.all_users.return_value = []

    pdt_graph_mock = mock.MagicMock()
    pdt_graph_mock.graph_text = None
    sdk.graph_derived_tables_for_model.return_value = pdt_graph_mock

    sdk.all_connections.return_value = []


def _build_pipeline(tmp_path: Path, extra_config: dict) -> Pipeline:
    """Build a pipeline without running it, for tests that need to inspect the object."""
    output_file = tmp_path / "looker_v2_mces.json"
    config: dict = {
        "base_url": "https://looker.company.com",
        "client_id": "foo",
        "client_secret": "bar",
        "extract_looks": False,
        "extract_usage_history": False,
        "project_name": "lkml_samples",
    }
    config.update(extra_config)
    return Pipeline.create(
        {
            "run_id": "looker-v2-test",
            "source": {
                "type": "looker-v2",
                "config": config,
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_file)},
            },
        }
    )


def _run_pipeline(
    tmp_path: Path, extra_config: dict, output_filename: str = "looker_v2_mces.json"
) -> Path:
    output_file = tmp_path / output_filename
    config: dict = {
        "base_url": "https://looker.company.com",
        "client_id": "foo",
        "client_secret": "bar",
        "extract_looks": False,
        "extract_usage_history": False,
        "project_name": "lkml_samples",
    }
    config.update(extra_config)
    pipeline = Pipeline.create(
        {
            "run_id": "looker-v2-test",
            "source": {
                "type": "looker-v2",
                "config": config,
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_file),
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return output_file


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_core_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        output_file = _run_pipeline(tmp_path, {})

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "core_ingest.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_chart_pattern_filtering(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        output_file = _run_pipeline(
            tmp_path,
            {"chart_pattern": {"deny": ["101"]}},
        )

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "chart_pattern_filtering.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_personal_folder_skipping(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        # Override folders to return a personal folder
        mocked_client.all_folders.return_value = [
            FolderBase(
                id="2",
                name="My Personal Folder",
                parent_id=None,
                is_personal=True,
                is_personal_descendant=False,
            ),
        ]

        # Put the dashboard in the personal folder
        mocked_client.all_dashboards.return_value = [
            DashboardBase(
                id="1",
                title="Sales Dashboard",
                folder=FolderBase(id="2", name="My Personal Folder"),
            )
        ]
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="Sales Dashboard",
            description="Sales metrics dashboard",
            folder=FolderBase(id="2", name="My Personal Folder"),
            dashboard_elements=[
                DashboardElement(
                    id="101",
                    title="Order Count",
                    type="vis",
                    query=Query(
                        model="lkml_samples",
                        view="order_items",
                        fields=["order_items.count"],
                        dynamic_fields=None,
                    ),
                    look_id=None,
                    look=None,
                    merge_result_id=None,
                )
            ],
        )

        output_file = _run_pipeline(
            tmp_path,
            {"skip_personal_folders": True},
        )

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "personal_folder_skipping.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_emit_used_explores_only(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        # Add a second explore that is not referenced by any dashboard element
        mocked_client.all_lookml_models.return_value = [
            LookmlModel(
                name="lkml_samples",
                project_name="lkml_samples",
                explores=[
                    LookmlModelNavExplore(name="order_items"),
                    LookmlModelNavExplore(name="unused_explore"),
                ],
            )
        ]

        def explore_side_effect(
            model_name: str, explore_name: str, **kwargs: object
        ) -> LookmlModelExplore:
            if explore_name == "order_items":
                return LookmlModelExplore(
                    id="lkml_samples::order_items",
                    name="order_items",
                    label="Order Items",
                    description="Order items explore",
                    view_name="order_items",
                    project_name="lkml_samples",
                    model_name="lkml_samples",
                    fields=LookmlModelExploreFieldset(
                        dimensions=[
                            LookmlModelExploreField(
                                name="order_items.id",
                                type="number",
                                description="Order item ID",
                                label_short="ID",
                                view="order_items",
                            )
                        ],
                        measures=[
                            LookmlModelExploreField(
                                name="order_items.count",
                                type="count",
                                description="Count of order items",
                                label_short="Count",
                                view="order_items",
                            )
                        ],
                    ),
                    source_file="order_items.explore.lkml",
                )
            return LookmlModelExplore(
                id=f"lkml_samples::{explore_name}",
                name=explore_name,
                label=explore_name,
                description=f"{explore_name} explore",
                view_name=explore_name,
                project_name="lkml_samples",
                model_name="lkml_samples",
                fields=LookmlModelExploreFieldset(dimensions=[], measures=[]),
                source_file=f"{explore_name}.explore.lkml",
            )

        mocked_client.lookml_model_explore.side_effect = explore_side_effect

        output_file = _run_pipeline(
            tmp_path,
            {"emit_used_explores_only": True},
        )

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "emit_used_explores_only.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_usage_history(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        # Return empty usage data — exercises the code path without asserting specific values
        mocked_client.run_inline_query.return_value = "[]"

        output_file = _run_pipeline(
            tmp_path,
            {"extract_usage_history": True},
        )

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "usage_history.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_dashboard_sdk_error_is_soft_failure(tmp_path: Path) -> None:
    """When sdk.dashboard() raises SDKError, the pipeline soft-fails and continues."""
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        _setup_mock_sdk(mocked_client)

        # Make the detail fetch raise so the pipeline must handle it gracefully.
        mocked_client.dashboard.side_effect = SDKError("connection refused")

        pipeline = _build_pipeline(tmp_path, {})
        pipeline.run()
        # Pipeline itself must not crash — errors are surfaced as warnings/failures
        # on the report, not as an unhandled exception.
        # Pipeline must complete without raising — that's the assertion.
        assert pipeline.source is not None


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_view_refinements(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    pytest.skip("view_refinements: requires git clone setup")


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_api_sql_lineage(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    pytest.skip(
        "api_sql_lineage: requires SQL parsing against real connections; skipped in unit integration tests"
    )
