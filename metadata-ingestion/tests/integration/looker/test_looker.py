import json
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
    User,
    WriteQuery,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.looker import usage_queries
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


def setup_mock_user(mocked_client):
    mocked_client.user.return_value = User(id=1, email="test@looker.com")


def side_effect_query_inline(result_format: str, body: WriteQuery) -> str:
    query_type = None
    if result_format == "sql":
        return ""  # Placeholder for sql text
    for query_name, query_template in usage_queries.items():
        if body.fields == query_template["fields"]:
            query_type = query_name

    if query_type == "counts_per_day_per_dashboard":
        return json.dumps(
            [
                {
                    "history.dashboard_id": "11",
                    "history.created_date": "2022-07-05",
                    "history.dashboard_user": 1,
                    "history.dashboard_run_count": 14,
                },
                {
                    "history.dashboard_id": "12",
                    "history.created_date": "2022-07-05",
                    "history.dashboard_user": 1,
                    "history.dashboard_run_count": 14,
                },
                {
                    "history.dashboard_id": "37",
                    "history.created_date": "2022-07-05",
                    "history.dashboard_user": 1,
                    "history.dashboard_run_count": 5,
                },
            ]
        )

    if query_type == "counts_per_day_per_user_per_dashboard":
        return json.dumps(
            [
                {
                    "history.created_date": "2022-07-05",
                    "history.dashboard_id": "11",
                    "user.id": 1,
                    "history.dashboard_run_count": 14,
                },
                {
                    "history.created_date": "2022-07-05",
                    "history.dashboard_id": "12",
                    "user.id": 1,
                    "history.dashboard_run_count": 14,
                },
                {
                    "history.created_date": "2022-07-05",
                    "history.dashboard_id": "37",
                    "user.id": 1,
                    "history.dashboard_run_count": 5,
                },
            ]
        )

    raise Exception("Unknown Query")


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


@freeze_time(FROZEN_TIME)
def test_looker_ingest_usage_history(pytestconfig, tmp_path, mock_time):
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
            favorite_count=5,
            view_count=25,
            last_viewed_at=datetime.utcfromtimestamp(time.time()),
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
        mocked_client.run_inline_query.side_effect = side_effect_query_inline
        setup_mock_explore(mocked_client)
        setup_mock_user(mocked_client)

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
                        "extract_usage_history": True,
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
        mce_out_file = "looker_mces_usage_history.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )
