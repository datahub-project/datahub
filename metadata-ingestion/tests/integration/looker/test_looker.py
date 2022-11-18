import json
import time
from datetime import datetime
from typing import List, Optional, cast
from unittest import mock

from freezegun import freeze_time
from looker_sdk.rtl import transport
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api31.models import (
    Dashboard,
    DashboardElement,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelExploreJoins,
    LookWithQuery,
    Query,
    User,
    WriteQuery,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.looker import looker_usage
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookViewField,
    UserViewField,
)
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.looker_state import LookerCheckpointState
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


@freeze_time(FROZEN_TIME)
def test_looker_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
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
        setup_mock_dashboard(mocked_client)
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
                        fields=["dim1"],
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


def setup_mock_dashboard(mocked_client):
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
                    fields=["dim1"],
                    view="my_view",
                    dynamic_fields='[{"table_calculation":"calc","label":"foobar","expression":"offset(${my_table.value},1)","value_format":null,"value_format_name":"eur","_kind_hint":"measure","_type_hint":"number"}]',
                ),
            )
        ],
    )


def setup_mock_dashboard_multiple_charts(mocked_client):
    mocked_client.all_dashboards.return_value = [Dashboard(id="1")]
    mocked_client.dashboard.return_value = Dashboard(
        id="11",
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
                    fields=["dim1"],
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


def setup_mock_dashboard_with_usage(mocked_client):
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
            ),
            DashboardElement(id="3", type="", look=LookWithQuery(id=3, view_count=30)),
        ],
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
                    name="dim1",
                    type="string",
                    description="dimension one description",
                    label_short="Dimensions One Label",
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
                    name="dim1",
                    type="string",
                    dimension_group=None,
                    description="dimension one description",
                    label_short="Dimensions One Label",
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
                    name="dim1",
                    type="string",
                    dimension_group=None,
                    description="dimension one description",
                    label_short="Dimensions One Label",
                )
            ]
        ),
        source_file="test_source_file.lkml",
    )


def setup_mock_user(mocked_client):
    def get_user(
        id_: int,
        fields: Optional[str] = None,
        transport_options: Optional[transport.TransportOptions] = None,
    ) -> User:
        return User(id=id_, email=f"test-{id_}@looker.com")

    mocked_client.user.side_effect = get_user


def side_effect_query_inline(
    result_format: str, body: WriteQuery, transport_options: Optional[TransportOptions]
) -> str:
    query_type: looker_usage.QueryId
    if result_format == "sql":
        return ""  # Placeholder for sql text

    for query_id, query_template in looker_usage.query_collection.items():
        if body.fields == query_template.to_write_query().fields:
            query_type = query_id
            break

    query_id_vs_response = {
        looker_usage.QueryId.DASHBOARD_PER_DAY_USAGE_STAT: json.dumps(
            [
                {
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_DASHBOARD_USER: 1,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 14,
                },
                {
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-06",
                    HistoryViewField.HISTORY_DASHBOARD_USER: 1,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 14,
                },
                {
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-07",
                    HistoryViewField.HISTORY_DASHBOARD_USER: 1,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 5,
                },
            ]
        ),
        looker_usage.QueryId.DASHBOARD_PER_USER_PER_DAY_USAGE_STAT: json.dumps(
            [
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    UserViewField.USER_ID: 1,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 16,
                },
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    UserViewField.USER_ID: 2,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 14,
                },
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-07",
                    HistoryViewField.HISTORY_DASHBOARD_ID: "1",
                    UserViewField.USER_ID: 1,
                    HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT: 5,
                },
            ]
        ),
        looker_usage.QueryId.LOOK_PER_DAY_USAGE_STAT: json.dumps(
            [
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_COUNT: 10,
                    LookViewField.LOOK_ID: 3,
                },
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-06",
                    HistoryViewField.HISTORY_COUNT: 20,
                    LookViewField.LOOK_ID: 3,
                },
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-07",
                    HistoryViewField.HISTORY_COUNT: 35,
                    LookViewField.LOOK_ID: 3,
                },
            ]
        ),
        looker_usage.QueryId.LOOK_PER_USER_PER_DAY_USAGE_STAT: json.dumps(
            [
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_COUNT: 10,
                    LookViewField.LOOK_ID: 3,
                    UserViewField.USER_ID: 1,
                },
                {
                    HistoryViewField.HISTORY_CREATED_DATE: "2022-07-05",
                    HistoryViewField.HISTORY_COUNT: 20,
                    LookViewField.LOOK_ID: 3,
                    UserViewField.USER_ID: 2,
                },
            ]
        ),
    }

    if query_id_vs_response.get(query_type) is None:
        raise Exception("Unknown Query")

    return query_id_vs_response[query_type]


@freeze_time(FROZEN_TIME)
def test_looker_ingest_allow_pattern(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard_multiple_charts(mocked_client)
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
        pipeline.pretty_print_summary()
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
        setup_mock_dashboard_with_usage(mocked_client)
        mocked_client.run_inline_query.side_effect = side_effect_query_inline
        setup_mock_explore(mocked_client)
        setup_mock_user(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        temp_output_file = f"{tmp_path}/looker_mces.json"
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
                        "max_threads": 1,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": temp_output_file,
                    },
                },
            }
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status()
        mce_out_file = "looker_mces_usage_history.json"

        # There should be 4 dashboardUsageStatistics aspects (one absolute and 3 timeseries)
        dashboard_usage_aspect_count = 0
        # There should be 4 chartUsageStatistics (one absolute and 3 timeseries)
        chart_usage_aspect_count = 0
        with open(temp_output_file) as f:
            temp_output_dict = json.load(f)
            for element in temp_output_dict:
                if (
                    element.get("entityType") == "dashboard"
                    and element.get("aspectName") == "dashboardUsageStatistics"
                ):
                    dashboard_usage_aspect_count = dashboard_usage_aspect_count + 1
                if (
                    element.get("entityType") == "chart"
                    and element.get("aspectName") == "chartUsageStatistics"
                ):
                    chart_usage_aspect_count = chart_usage_aspect_count + 1

            assert dashboard_usage_aspect_count == 4
            assert chart_usage_aspect_count == 4

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=temp_output_file,
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    output_file_name: str = "looker_mces.json"
    golden_file_name: str = "golden_looker_mces.json"
    output_file_deleted_name: str = "looker_mces_deleted_stateful.json"
    golden_file_deleted_name: str = "looker_mces_golden_deleted_stateful.json"

    test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

    def looker_source_config(sink_file_name):
        return {
            "run_id": "looker-test",
            "pipeline_name": "stateful-looker-pipeline",
            "source": {
                "type": "looker",
                "config": {
                    "base_url": "https://looker.company.com",
                    "client_id": "foo",
                    "client_secret": "bar",
                    "stateful_ingestion": {
                        "enabled": True,
                        "remove_stale_metadata": True,
                        "fail_safe_threshold": 100.0,
                        "state_provider": {
                            "type": "datahub",
                            "config": {"datahub_api": {"server": GMS_SERVER}},
                        },
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{sink_file_name}",
                },
            },
        }

    mocked_client = mock.MagicMock()
    pipeline_run1 = None
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init31") as mock_sdk:

        mock_checkpoint.return_value = mock_datahub_graph
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard_multiple_charts(mocked_client)
        setup_mock_explore(mocked_client)

        pipeline_run1 = Pipeline.create(looker_source_config(output_file_name))
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        pipeline_run1.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name,
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = None
    mocked_client = mock.MagicMock()
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init31") as mock_sdk:

        mock_checkpoint.return_value = mock_datahub_graph
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
        setup_mock_explore(mocked_client)

        pipeline_run2 = Pipeline.create(looker_source_config(output_file_deleted_name))
        pipeline_run2.run()
        pipeline_run2.raise_from_status()
        pipeline_run2.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_deleted_name,
            golden_path=f"{test_resources_dir}/{golden_file_deleted_name}",
        )

    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2
    assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted table should not be
    # part of the second state
    state1 = cast(LookerCheckpointState, checkpoint1.state)
    state2 = cast(LookerCheckpointState, checkpoint2.state)

    difference_dataset_urns = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert len(difference_dataset_urns) == 1
    deleted_dataset_urns: List[str] = [
        "urn:li:dataset:(urn:li:dataPlatform:looker,bogus data.explore.my_view,PROD)"
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)

    difference_chart_urns = list(
        state1.get_urns_not_in(type="chart", other_checkpoint_state=state2)
    )
    assert len(difference_chart_urns) == 1
    deleted_chart_urns = ["urn:li:chart:(looker,dashboard_elements.10)"]
    assert sorted(deleted_chart_urns) == sorted(difference_chart_urns)

    difference_dashboard_urns = list(
        state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
    )
    assert len(difference_dashboard_urns) == 1
    deleted_dashboard_urns = ["urn:li:dashboard:(looker,dashboards.11)"]
    assert sorted(deleted_dashboard_urns) == sorted(difference_dashboard_urns)


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    dbt_source = cast(LookerDashboardSource, pipeline.source)
    return dbt_source.get_current_checkpoint(
        dbt_source.stale_entity_removal_handler.job_id
    )
