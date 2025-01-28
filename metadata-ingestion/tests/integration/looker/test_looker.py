import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, cast
from unittest import mock

import pytest
from _pytest.config import Config
from freezegun import freeze_time
from looker_sdk.rtl import transport
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api40.models import (
    Category,
    Dashboard,
    DashboardElement,
    Folder,
    FolderBase,
    Look,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    LookmlModelExploreJoins,
    LookWithQuery,
    Query,
    User,
    WriteQuery,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.run.pipeline import Pipeline, PipelineInitError
from datahub.ingestion.source.looker import looker_common, looker_usage
from datahub.ingestion.source.looker.looker_common import (
    LookerDashboardSourceReport,
    LookerExplore,
)
from datahub.ingestion.source.looker.looker_config import LookerCommonConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
)
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookViewField,
    UserViewField,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import GlobalTagsClass, MetadataChangeEventClass
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def get_default_recipe(output_file_path: str) -> Dict[Any, Any]:
    return {
        "run_id": "looker-test",
        "source": {
            "type": "looker",
            "config": {
                "base_url": "https://looker.company.com",
                "client_id": "foo",
                "client_secret": "bar",
                "extract_usage_history": False,
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": output_file_path,
            },
        },
    }


@freeze_time(FROZEN_TIME)
def test_looker_ingest(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
        mocked_client.run_inline_query.side_effect = side_effect_query_inline
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
                        "extract_usage_history": False,
                        "platform_instance": "ap-south-1",
                        "include_platform_instance_in_urns": True,
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


def setup_mock_external_project_view_explore(mocked_client):
    mock_model = mock.MagicMock(project_name="lkml_samples")
    mocked_client.lookml_model.return_value = mock_model
    mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
        id="1",
        name="my_explore_name",
        label="My Explore View",
        description="lorem ipsum",
        view_name="faa_flights",
        project_name="looker_hub",
        fields=LookmlModelExploreFieldset(
            dimensions=[
                LookmlModelExploreField(
                    name="dim1",
                    type="string",
                    dimension_group=None,
                    description="dimension one description",
                    label_short="Dimensions One Label",
                    view="faa_flights",
                    source_file="imported_projects/datahub-demo/views/datahub-demo/datasets/faa_flights.view.lkml",
                )
            ]
        ),
        source_file="test_source_file.lkml",
    )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_external_project_view(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
        setup_mock_external_project_view_explore(mocked_client)

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
                        "extract_usage_history": False,
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
        mce_out_file = "golden_test_external_project_view_mces.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


@freeze_time(FROZEN_TIME)
def test_looker_ingest_joins(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init40") as mock_sdk:
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
                        "extract_usage_history": False,
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
    with mock.patch("looker_sdk.init40") as mock_sdk:
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
                        "extract_usage_history": False,
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
        folder=FolderBase(name="Shared", id="shared-folder-id"),
        dashboard_elements=[
            DashboardElement(
                id="2",
                type="vis",
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


def setup_mock_look(mocked_client):
    mocked_client.all_looks.return_value = [
        Look(
            id="1",
            user_id="1",
            title="Outer Look",
            description="I am not part of any Dashboard",
            query_id="1",
            folder=FolderBase(name="Shared", id="shared-folder-id"),
        ),
        Look(
            id="2",
            title="Personal Look",
            user_id="2",
            description="I am not part of any Dashboard and in personal folder",
            query_id="2",
            folder=FolderBase(
                name="Personal",
                id="personal-folder-id",
                is_personal=True,
                is_personal_descendant=True,
            ),
        ),
    ]

    mocked_client.look.side_effect = [
        LookWithQuery(
            query=Query(
                id="1",
                view="sales_explore",
                model="sales_model",
                fields=[
                    "sales.profit",
                ],
                dynamic_fields=None,
                filters=None,
            )
        ),
        LookWithQuery(
            query=Query(
                id="2",
                view="order_explore",
                model="order_model",
                fields=[
                    "order.placed_date",
                ],
                dynamic_fields=None,
                filters=None,
            )
        ),
    ]


def setup_mock_soft_deleted_look(mocked_client):
    mocked_client.search_looks.return_value = [
        Look(
            id="2",
            title="Soft Deleted",
            description="I am not part of any Dashboard",
            query_id="1",
        )
    ]


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


def setup_mock_dashboard_with_usage(
    mocked_client: mock.MagicMock, skip_look: bool = False
) -> None:
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
            DashboardElement(
                id="3",
                type="" if skip_look else "vis",  # Looks only ingested if type == `vis`
                look=LookWithQuery(
                    id="3",
                    view_count=30,
                    query=Query(model="look_data", view="look_view"),
                ),
            ),
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


def setup_mock_explore(
    mocked_client: Any,
    additional_lkml_fields: List[LookmlModelExploreField] = [],
    **additional_explore_fields: Any,
) -> None:
    mock_model = mock.MagicMock(project_name="lkml_samples")
    mocked_client.lookml_model.return_value = mock_model

    lkml_fields: List[LookmlModelExploreField] = [
        LookmlModelExploreField(
            name="dim1",
            type="string",
            dimension_group=None,
            description="dimension one description",
            label_short="Dimensions One Label",
        )
    ]
    lkml_fields.extend(additional_lkml_fields)

    mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
        id="1",
        name="my_explore_name",
        label="My Explore View",
        description="lorem ipsum",
        view_name="underlying_view",
        project_name="lkml_samples",
        fields=LookmlModelExploreFieldset(
            dimensions=lkml_fields,
        ),
        source_file="test_source_file.lkml",
        **additional_explore_fields,
    )


def setup_mock_user(mocked_client):
    def get_user(
        id_: str,
        fields: Optional[str] = None,
        transport_options: Optional[transport.TransportOptions] = None,
    ) -> User:
        return User(id=id_, email=f"test-{id_}@looker.com")

    mocked_client.user.side_effect = get_user


def setup_mock_all_user(mocked_client):
    def all_users(
        fields: Optional[str] = None,
        transport_options: Optional[transport.TransportOptions] = None,
    ) -> List[User]:
        return [
            User(id="1", email="test-1@looker.com"),
            User(id="2", email="test-2@looker.com"),
            User(id="3", email="test-3@looker.com"),
        ]

    mocked_client.all_users.side_effect = all_users


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
                {
                    HistoryViewField.HISTORY_DASHBOARD_ID: "5",
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

    with mock.patch("looker_sdk.init40") as mock_sdk:
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
                        "extract_usage_history": False,
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
    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard_with_usage(mocked_client)
        mocked_client.run_inline_query.side_effect = side_effect_query_inline
        setup_mock_explore(mocked_client)
        setup_mock_user(mocked_client)
        setup_mock_all_user(mocked_client)

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
def test_looker_filter_usage_history(pytestconfig, tmp_path, mock_time):
    mocked_client = mock.MagicMock()
    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_dashboard_with_usage(mocked_client, skip_look=True)
        mocked_client.run_inline_query.side_effect = side_effect_query_inline
        setup_mock_explore(mocked_client)
        setup_mock_user(mocked_client)

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

        # There should be 4 dashboardUsageStatistics aspects (one absolute and 3 timeseries)
        dashboard_usage_aspect_count = 0
        # There should be 0 chartUsageStatistics -- filtered by set of ingested charts
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
        assert chart_usage_aspect_count == 0

        source_report = cast(LookerDashboardSourceReport, pipeline.source.get_report())
        # From timeseries query
        assert str(source_report.dashboards_skipped_for_usage) == str(["5"])
        # From dashboard element
        assert str(source_report.charts_skipped_for_usage) == str(["3"])


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
                    "extract_usage_history": False,
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
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
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
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
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
    state1 = cast(GenericCheckpointState, checkpoint1.state)
    state2 = cast(GenericCheckpointState, checkpoint2.state)

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


@freeze_time(FROZEN_TIME)
def test_independent_look_ingestion_config(pytestconfig, tmp_path, mock_time):
    """
    if extract_independent_looks is enabled, then stateful_ingestion.enabled should also be enabled
    """
    new_recipe = get_default_recipe(output_file_path=f"{tmp_path}/output")
    new_recipe["source"]["config"]["extract_independent_looks"] = True

    with pytest.raises(
        expected_exception=PipelineInitError,
        match="stateful_ingestion.enabled should be set to true",
    ):
        # Config error should get raise
        Pipeline.create(new_recipe)


def ingest_independent_looks(
    pytestconfig: Config,
    tmp_path: Path,
    mock_time: float,
    mock_datahub_graph: mock.MagicMock,
    skip_personal_folders: bool,
    golden_file_name: str,
) -> None:
    mocked_client = mock.MagicMock()
    new_recipe = get_default_recipe(output_file_path=f"{tmp_path}/looker_mces.json")
    new_recipe["source"]["config"]["extract_independent_looks"] = True
    new_recipe["source"]["config"]["skip_personal_folders"] = skip_personal_folders
    new_recipe["source"]["config"]["stateful_ingestion"] = {
        "enabled": True,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": GMS_SERVER}},
        },
    }
    new_recipe["pipeline_name"] = "execution-1"

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
        mock_checkpoint.return_value = mock_datahub_graph

        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
        setup_mock_explore(mocked_client)
        setup_mock_user(mocked_client)
        setup_mock_all_user(mocked_client)
        setup_mock_look(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(new_recipe)
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )


@freeze_time(FROZEN_TIME)
def test_independent_looks_ingest_with_personal_folder(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    ingest_independent_looks(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
        mock_time=mock_time,
        mock_datahub_graph=mock_datahub_graph,
        skip_personal_folders=False,
        golden_file_name="golden_test_independent_look_ingest.json",
    )


@freeze_time(FROZEN_TIME)
def test_independent_looks_ingest_without_personal_folder(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    ingest_independent_looks(
        pytestconfig=pytestconfig,
        tmp_path=tmp_path,
        mock_time=mock_time,
        mock_datahub_graph=mock_datahub_graph,
        skip_personal_folders=True,
        golden_file_name="golden_test_non_personal_independent_look.json",
    )


@freeze_time(FROZEN_TIME)
def test_file_path_in_view_naming_pattern(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    mocked_client = mock.MagicMock()
    new_recipe = get_default_recipe(output_file_path=f"{tmp_path}/looker_mces.json")
    new_recipe["source"]["config"]["view_naming_pattern"] = (
        "{project}.{file_path}.view.{name}"
    )

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
        mock_checkpoint.return_value = mock_datahub_graph

        mock_sdk.return_value = mocked_client
        setup_mock_dashboard(mocked_client)
        setup_mock_explore(
            mocked_client,
            additional_lkml_fields=[
                LookmlModelExploreField(
                    name="dim2",
                    type="string",
                    dimension_group=None,
                    description="dimension one description",
                    label_short="Dimensions One Label",
                    view="underlying_view",
                    source_file="views/underlying_view.view.lkml",
                )
            ],
        )
        setup_mock_look(mocked_client)
        setup_mock_external_project_view_explore(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(new_recipe)
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_file_path_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )


@freeze_time(FROZEN_TIME)
def test_independent_soft_deleted_looks(
    pytestconfig,
    tmp_path,
    mock_time,
):
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client
        setup_mock_look(mocked_client)
        setup_mock_soft_deleted_look(mocked_client)
        looker_api = LookerAPI(
            config=LookerAPIConfig(
                base_url="https://fake.com",
                client_id="foo",
                client_secret="bar",
            )
        )
        looks: List[Look] = looker_api.all_looks(
            fields=["id"],
            soft_deleted=True,
        )

        assert len(looks) == 3
        assert looks[0].title == "Outer Look"
        assert looks[1].title == "Personal Look"
        assert looks[2].title == "Soft Deleted"


@freeze_time(FROZEN_TIME)
def test_upstream_cll(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    mocked_client = mock.MagicMock()

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
        mock_checkpoint.return_value = mock_datahub_graph

        mock_sdk.return_value = mocked_client
        setup_mock_explore(
            mocked_client,
            additional_lkml_fields=[
                LookmlModelExploreField(
                    name="dim2",
                    type="string",
                    dimension_group=None,
                    description="dimension one description",
                    label_short="Dimensions One Label",
                    view="underlying_view",
                    source_file="views/underlying_view.view.lkml",
                ),
                LookmlModelExploreField(
                    category=Category.dimension,
                    dimension_group="my_explore_name.createdon",
                    field_group_label="Createdon Date",
                    field_group_variant="Date",
                    label="Dataset Lineages Explore Createdon Date",
                    label_short="Createdon Date",
                    lookml_link="/projects/datahub-demo/files/views%2Fdatahub-demo%2Fdatasets%2Fdataset_lineages.view.lkml?line=5",
                    name="my_explore_name.createdon_date",
                    project_name="datahub-demo",
                    source_file="views/datahub-demo/datasets/dataset_lineages.view.lkml",
                    source_file_path="datahub-demo/views/datahub-demo/datasets/dataset_lineages.view.lkml",
                    sql='${TABLE}."CREATEDON" ',
                    suggest_dimension="my_explore_name.createdon_date",
                    suggest_explore="my_explore_name",
                    type="date_date",
                    view="my_explore_name",
                    view_label="Dataset Lineages Explore",
                    original_view="dataset_lineages",
                ),
            ],
        )
        config = mock.MagicMock()

        config.view_naming_pattern.replace_variables.return_value = "dataset_lineages"
        config.platform_name = "snowflake"
        config.platform_instance = "sales"
        config.env = "DEV"

        looker_explore: Optional[LookerExplore] = looker_common.LookerExplore.from_api(
            model="fake",
            explore_name="my_explore_name",
            client=mocked_client,
            reporter=mock.MagicMock(),
            source_config=config,
        )

        assert looker_explore is not None
        assert looker_explore.name == "my_explore_name"
        assert looker_explore.fields is not None
        assert len(looker_explore.fields) == 3

        assert (
            looker_explore.fields[2].upstream_fields[0].table
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "sales.dataset_lineages,DEV)"
        )

        assert looker_explore.fields[2].upstream_fields[0].column == "createdon"


@freeze_time(FROZEN_TIME)
def test_explore_tags(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    mocked_client = mock.MagicMock()

    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint, mock.patch("looker_sdk.init40") as mock_sdk:
        mock_checkpoint.return_value = mock_datahub_graph

        tags: List[str] = ["metrics", "all"]

        mock_sdk.return_value = mocked_client
        setup_mock_explore(
            mocked_client,
            tags=tags,
        )

        looker_explore: Optional[LookerExplore] = looker_common.LookerExplore.from_api(
            model="fake",
            explore_name="my_explore_name",
            client=mocked_client,
            reporter=mock.MagicMock(),
            source_config=mock.MagicMock(),
        )

        assert looker_explore is not None
        assert looker_explore.name == "my_explore_name"
        assert looker_explore.tags == tags

        mcps: Optional[
            List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]
        ] = looker_explore._to_metadata_events(
            config=LookerCommonConfig(),
            reporter=SourceReport(),
            base_url="fake",
            extract_embed_urls=False,
        )

        expected_tag_urns: List[str] = ["urn:li:tag:metrics", "urn:li:tag:all"]

        actual_tag_urns: List[str] = []
        if mcps:
            for mcp in mcps:
                if isinstance(mcp, MetadataChangeEventClass):
                    for aspect in mcp.proposedSnapshot.aspects:
                        if isinstance(aspect, GlobalTagsClass):
                            actual_tag_urns = [
                                tag_association.tag for tag_association in aspect.tags
                            ]

        assert expected_tag_urns == actual_tag_urns


def side_effect_function_for_dashboards(*args: Tuple[str], **kwargs: Any) -> Dashboard:
    assert kwargs["dashboard_id"] in ["1", "2", "3"], "Invalid dashboard id"

    if kwargs["dashboard_id"] == "1":
        return Dashboard(
            id=kwargs["dashboard_id"],
            title="first dashboard",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="first",
            folder=FolderBase(name="A", id="a"),
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

    if kwargs["dashboard_id"] == "2":
        return Dashboard(
            id=kwargs["dashboard_id"],
            title="second dashboard",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="second",
            folder=FolderBase(name="B", id="b"),
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

    if kwargs["dashboard_id"] == "3":
        return Dashboard(
            id=kwargs["dashboard_id"],
            title="third dashboard",
            created_at=datetime.utcfromtimestamp(time.time()),
            updated_at=datetime.utcfromtimestamp(time.time()),
            description="third",
            folder=FolderBase(name="C", id="c"),
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

    # Default return to satisfy the linter
    return Dashboard(
        id="unknown",
        title="unknown",
        created_at=datetime.utcfromtimestamp(time.time()),
        updated_at=datetime.utcfromtimestamp(time.time()),
        description="unknown",
        folder=FolderBase(name="Unknown", id="unknown"),
        dashboard_elements=[],
    )


def side_effect_function_folder_ancestors(
    *args: Tuple[Any], **kwargs: Any
) -> Sequence[Folder]:
    assert args[0] in ["a", "b", "c"], "Invalid folder id"

    if args[0] == "a":
        # No parent
        return ()

    if args[0] == "b":
        return (Folder(id="a", name="A"),)

    if args[0] == "c":
        return Folder(id="a", name="A"), Folder(id="b", name="B")

    # Default return to satisfy the linter
    return (Folder(id="unknown", name="Unknown"),)


def setup_mock_dashboard_with_folder(mocked_client):
    mocked_client.all_dashboards.return_value = [
        Dashboard(id="1"),
        Dashboard(id="2"),
        Dashboard(id="3"),
    ]
    mocked_client.dashboard.side_effect = side_effect_function_for_dashboards
    mocked_client.folder_ancestors.side_effect = side_effect_function_folder_ancestors


@freeze_time(FROZEN_TIME)
def test_folder_path_pattern(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    mocked_client = mock.MagicMock()
    new_recipe = get_default_recipe(output_file_path=f"{tmp_path}/looker_mces.json")
    new_recipe["source"]["config"]["folder_path_pattern"] = {
        "allow": ["A/B/C"],
    }

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client

        setup_mock_dashboard_with_folder(mocked_client)

        setup_mock_explore(mocked_client)

        setup_mock_look(mocked_client)

        test_resources_dir = pytestconfig.rootpath / "tests/integration/looker"

        pipeline = Pipeline.create(new_recipe)
        pipeline.run()
        pipeline.raise_from_status()
        mce_out_file = "golden_test_folder_path_pattern_ingest.json"

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "looker_mces.json",
            golden_path=f"{test_resources_dir}/{mce_out_file}",
        )
