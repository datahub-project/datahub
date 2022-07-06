import time
from datetime import datetime
from unittest import mock

from freezegun import freeze_time
from looker_sdk.sdk.api31.models import (
    Dashboard,
    DashboardElement,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelExploreJoins,
    Query,
)

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
def test_looker_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.all_dashboards.return_value = [Dashboard(id="1")]
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="foo",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="lorem ipsum",
            dashboard_elements=[
                DashboardElement(
                    id="2",
                    type="",
                    subtitle_text="Some text",
                    query=Query(
                        model="data",
                        view="my_view",
                        dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                    ),
                )
            ],
        )
        setup_mock_explore(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(
            {
                "run_id": "looker-test",
                "source": {
                    "type": "looker",
                    "config": {
                        "base_url": "https://looker.company.com",
                        "client_id": "foo",
                        "client_secret": "bar",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/looker_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_joins(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.all_dashboards.return_value = [Dashboard(id="1")]
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="foo",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="lorem ipsum",
            dashboard_elements=[
                DashboardElement(
                    id="2",
                    type="",
                    subtitle_text="Some text",
                    query=Query(
                        model="data",
                        view="my_view",
                        dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                    ),
                )
            ],
        )
        setup_mock_explore_with_joins(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(
            {
                "run_id": "looker-test",
                "source": {
                    "type": "looker",
                    "config": {
                        "base_url": "https://looker.company.com",
                        "client_id": "foo",
                        "client_secret": "bar",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/looker_mces_joins.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_ingest_joins.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces_joins.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_unaliased_joins(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.all_dashboards.return_value = [Dashboard(id="1")]
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="foo",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="lorem ipsum",
            dashboard_elements=[
                DashboardElement(
                    id="2",
                    type="",
                    subtitle_text="Some text",
                    query=Query(
                        model="data",
                        view="my_view",
                        dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                    ),
                )
            ],
        )
        setup_mock_explore_unaliased_with_joins(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(
            {
                "run_id": "looker-test",
                "source": {
                    "type": "looker",
                    "config": {
                        "base_url": "https://looker.company.com",
                        "client_id": "foo",
                        "client_secret": "bar",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/looker_mces_unaliased_joins.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_ingest_unaliased_joins.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces_unaliased_joins.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


def setup_mock_explore_with_joins(mocked_client):
    mock_model = mock.MagicMock(project_name="lkml_samples")
    mocked_client.lookml_model.return_value = mock_model
    mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
        id="1",
        name="my_explore_name",
        label="My Explore View",
        description="lorem ipsum",
        view_name="underlying_view",
        project_name="lkml_samples",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="dim1", type="string", dimension_group=None
                )
            ]
        ),
        source_file="test_source_file.lkml",
        joins=[
            LookmlModelExploreJoins(
                name="my_joined_view",
                dependent_fields=["my_joined_view.field", "bare_field"],
            ),
            LookmlModelExploreJoins(
                name="my_view_has_no_fields",
                view_label="My Labeled View",
                relationship="one_to_one",
                sql_on="1=1",
            ),
            LookmlModelExploreJoins(
                name="my_joined_view_join_name",
                from_="my_joined_view_original_name",
            ),
        ],
    )


def setup_mock_explore_unaliased_with_joins(mocked_client):
    mock_model = mock.MagicMock(project_name="lkml_samples")
    mocked_client.lookml_model.return_value = mock_model
    mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
        id="1",
        name="my_view",
        label="My Explore View",
        description="lorem ipsum",
        project_name="lkml_samples",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="dim1", type="string", dimension_group=None
                )
            ]
        ),
        source_file="test_source_file.lkml",
        joins=[
            LookmlModelExploreJoins(
                name="my_view_has_no_fields",
                view_label="My Labeled View",
                relationship="one_to_one",
                sql_on="1=1",
            ),
            LookmlModelExploreJoins(
                name="my_joined_view_join_name",
                from_="my_joined_view_original_name",
            ),
        ],
    )


def setup_mock_explore(mocked_client):
    mock_model = mock.MagicMock(project_name="lkml_samples")
    mocked_client.lookml_model.return_value = mock_model
    mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
        id="1",
        name="my_explore_name",
        label="My Explore View",
        description="lorem ipsum",
        view_name="underlying_view",
        project_name="lkml_samples",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="dim1", type="string", dimension_group=None
                )
            ]
        ),
        source_file="test_source_file.lkml",
    )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_allow_pattern(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        mocked_client.all_dashboards.return_value = [Dashboard(id="1")]
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="foo",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="lorem ipsum",
            dashboard_elements=[
                DashboardElement(
                    id="2",
                    type="",
                    subtitle_text="Some text",
                    query=Query(
                        model="data",
                        view="my_view",
                        dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                    ),
                ),
                DashboardElement(
                    id="10",
                    type="",
                    subtitle_text="Some other text",
                    query=Query(
                        model="bogus data",
                        view="my_view",
                        dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                    ),
                ),
            ],
        )
        setup_mock_explore(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(
            {
                "run_id": "looker-test",
                "source": {
                    "type": "looker",
                    "config": {
                        "base_url": "https://looker.company.com",
                        "client_id": "foo",
                        "client_secret": "bar",
                        "chart_pattern": {"allow": ["2"]},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/looker_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_allow_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )
