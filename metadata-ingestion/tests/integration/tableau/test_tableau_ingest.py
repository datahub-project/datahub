import json
import pathlib
from typing import Optional, cast
from unittest import mock

import pytest
from freezegun import freeze_time
from tableauserverclient.models import ViewItem

from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.tableau_state import TableauCheckpointState
from datahub.ingestion.source.tableau import TableauSource
from datahub.ingestion.source.tableau_common import (
    TableauLineageOverrides,
    make_table_urn,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2021-12-07 07:00:00"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

test_resources_dir = None


def read_response(pytestconfig, file_name):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/tableau"
    )
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def side_effect_usage_stat(*arg, **kwargs):
    mock_pagination = mock.MagicMock()
    mock_pagination.total_available = None

    dashboard_stat: ViewItem = ViewItem()

    # Added as luid of Dashboard in workbooksConnection_state_all.json
    dashboard_stat._id = "fc9ea488-f810-4fa8-ac19-aa96018b5d66"
    dashboard_stat._total_views = 3

    # Added as luid of Sheet in workbooksConnection_state_all.json
    sheet_stat: ViewItem = ViewItem()
    sheet_stat._id = "f0779f9d-6765-47a9-a8f6-c740cfd27783"
    sheet_stat._total_views = 5

    return [dashboard_stat, sheet_stat], mock_pagination


def tableau_ingest_common(
    pytestconfig,
    tmp_path,
    side_effect_query_metadata_response,
    golden_file_name,
    output_file_name,
    mock_datahub_graph,
):
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/tableau"
    )
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        with mock.patch("datahub.ingestion.source.tableau.Server") as mock_sdk:
            mock_client = mock.Mock()
            mocked_metadata = mock.Mock()
            mocked_metadata.query.side_effect = side_effect_query_metadata_response
            mock_client.metadata = mocked_metadata
            mock_client.auth = mock.Mock()
            mock_client.views = mock.Mock()
            mock_client.views.get.side_effect = side_effect_usage_stat
            mock_client.auth.sign_in.return_value = None
            mock_client.auth.sign_out.return_value = None
            mock_sdk.return_value = mock_client
            mock_sdk._auth_token = "ABC"

            pipeline = Pipeline.create(
                {
                    "run_id": "tableau-test",
                    "pipeline_name": "tableau-test-pipeline",
                    "source": {
                        "type": "tableau",
                        "config": {
                            "username": "username",
                            "password": "pass`",
                            "connect_uri": "https://do-not-connect",
                            "site": "acryl",
                            "projects": ["default", "Project 2"],
                            "page_size": 10,
                            "ingest_tags": True,
                            "ingest_owner": True,
                            "ingest_tables_external": True,
                            "default_schema_map": {
                                "dvdrental": "public",
                                "someotherdb": "schema",
                            },
                            "platform_instance_map": {
                                "postgres": "demo_postgres_instance"
                            },
                            "extract_usage_stats": True,
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
                            "filename": f"{tmp_path}/{output_file_name}",
                        },
                    },
                }
            )
            pipeline.run()
            pipeline.raise_from_status()

            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=f"{tmp_path}/{output_file_name}",
                golden_path=test_resources_dir / golden_file_name,
                ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
            )
            return pipeline


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    tableau_source = cast(TableauSource, pipeline.source)
    return tableau_source.get_current_checkpoint(
        tableau_source.stale_entity_removal_handler.job_id
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.slow_unit
def test_tableau_ingest(pytestconfig, tmp_path, mock_datahub_graph):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    tableau_ingest_common(
        pytestconfig,
        tmp_path,
        [
            read_response(pytestconfig, "workbooksConnection_all.json"),
            read_response(pytestconfig, "embeddedDatasourcesConnection_all.json"),
            read_response(pytestconfig, "publishedDatasourcesConnection_all.json"),
            read_response(pytestconfig, "customSQLTablesConnection_all.json"),
        ],
        golden_file_name,
        output_file_name,
        mock_datahub_graph,
    )


def test_lineage_overrides():
    # Simple - specify platform instance to presto table
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_presto_instance"},
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # Transform presto urn to hive urn
    # resulting platform instance for hive = mapped platform instance + presto_catalog
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "presto_catalog",
            "presto",
            "test-schema",
            "presto_catalog.test-schema.test-table",
            platform_instance_map={"presto": "my_instance"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"presto": "hive"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # tranform hive urn to presto urn
    assert (
        make_table_urn(
            DEFAULT_ENV,
            "",
            "hive",
            "test-schema",
            "test-schema.test-table",
            platform_instance_map={"hive": "my_presto_instance.presto_catalog"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"hive": "presto"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )


@freeze_time(FROZEN_TIME)
def test_tableau_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    output_file_name: str = "tableau_mces.json"
    golden_file_name: str = "tableau_mces_golden.json"
    output_file_deleted_name: str = "tableau_mces_deleted_stateful.json"
    golden_file_deleted_name: str = "tableau_mces_golden_deleted_stateful.json"

    pipeline_run1 = tableau_ingest_common(
        pytestconfig,
        tmp_path,
        [
            read_response(pytestconfig, "workbooksConnection_all.json"),
            read_response(pytestconfig, "embeddedDatasourcesConnection_all.json"),
            read_response(pytestconfig, "publishedDatasourcesConnection_all.json"),
            read_response(pytestconfig, "customSQLTablesConnection_all.json"),
        ],
        golden_file_name,
        output_file_name,
        mock_datahub_graph,
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = tableau_ingest_common(
        pytestconfig,
        tmp_path,
        [read_response(pytestconfig, "workbooksConnection_all_stateful.json")],
        golden_file_deleted_name,
        output_file_deleted_name,
        mock_datahub_graph,
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
    state1 = cast(TableauCheckpointState, checkpoint1.state)
    state2 = cast(TableauCheckpointState, checkpoint2.state)

    difference_dataset_urns = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert len(difference_dataset_urns) == 12
    deleted_dataset_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:tableau,00cce29f-b561-bb41-3557-8e19660bb5dd,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,6cbbeeb2-9f3a-00f6-2342-17139d6e97ae,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,801c95e3-b07e-7bfe-3789-a561c7beccd3,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,4644ccb1-2adc-cf26-c654-04ed1dcc7090,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,618c87db-5959-338b-bcc7-6f5f4cc0b6c6,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,d00f4ba6-707e-4684-20af-69eb47587cc2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,06c3e060-8133-4b58-9b53-a0fced25e056,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,3ade7817-ae27-259e-8e48-1570e7f932f6,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,dfe2c02a-54b7-f7a2-39fc-c651da2f6ad8,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,d8d4c0ea-3162-fa11-31e6-26675da44a38,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,22b0b4c3-6b85-713d-a161-5a87fdd78f40,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:tableau,4fb670d5-3e19-9656-e684-74aa9729cf18,PROD)",
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)

    difference_chart_urns = list(
        state1.get_urns_not_in(type="chart", other_checkpoint_state=state2)
    )
    assert len(difference_chart_urns) == 24
    deleted_chart_urns = [
        "urn:li:chart:(tableau,222d1406-de0e-cd8d-0b94-9b45a0007e59)",
        "urn:li:chart:(tableau,38130558-4194-2e2a-3046-c0d887829cb4)",
        "urn:li:chart:(tableau,692a2da4-2a82-32c1-f713-63b8e4325d86)",
        "urn:li:chart:(tableau,f4317efd-c3e6-6ace-8fe6-e71b590bbbcc)",
        "urn:li:chart:(tableau,8a6a269a-d6de-fae4-5050-513255b40ffc)",
        "urn:li:chart:(tableau,c57a5574-db47-46df-677f-0b708dab14db)",
        "urn:li:chart:(tableau,e604255e-0573-3951-6db7-05bee48116c1)",
        "urn:li:chart:(tableau,20fc5eb7-81eb-aa18-8c39-af501c62d085)",
        "urn:li:chart:(tableau,2b5351c1-535d-4a4a-1339-c51ddd6abf8a)",
        "urn:li:chart:(tableau,2b73b9dd-4ec7-75ca-f2e9-fa1984ca8b72)",
        "urn:li:chart:(tableau,373c6466-bb0c-b319-8752-632456349261)",
        "urn:li:chart:(tableau,53b8dc2f-8ada-51f7-7422-fe82e9b803cc)",
        "urn:li:chart:(tableau,58af9ecf-b839-da50-65e1-2e1fa20e3362)",
        "urn:li:chart:(tableau,618b3e76-75c1-cb31-0c61-3f4890b72c31)",
        "urn:li:chart:(tableau,721c3c41-7a2b-16a8-3281-6f948a44be96)",
        "urn:li:chart:(tableau,7ef184c1-5a41-5ec8-723e-ae44c20aa335)",
        "urn:li:chart:(tableau,7fbc77ba-0ab6-3727-0db3-d8402a804da5)",
        "urn:li:chart:(tableau,8385ea9a-0749-754f-7ad9-824433de2120)",
        "urn:li:chart:(tableau,b207c2f2-b675-32e3-2663-17bb836a018b)",
        "urn:li:chart:(tableau,b679da5e-7d03-f01e-b2ea-01fb3c1926dc)",
        "urn:li:chart:(tableau,c14973c2-e1c3-563a-a9c1-8a408396d22a)",
        "urn:li:chart:(tableau,e70a540d-55ed-b9cc-5a3c-01ebe81a1274)",
        "urn:li:chart:(tableau,f76d3570-23b8-f74b-d85c-cc5484c2079c)",
        "urn:li:chart:(tableau,130496dc-29ca-8a89-e32b-d73c4d8b65ff)",
    ]
    assert sorted(deleted_chart_urns) == sorted(difference_chart_urns)

    difference_dashboard_urns = list(
        state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
    )
    assert len(difference_dashboard_urns) == 4
    deleted_dashboard_urns = [
        "urn:li:dashboard:(tableau,5dcaaf46-e6fb-2548-e763-272a7ab2c9b1)",
        "urn:li:dashboard:(tableau,8f7dd564-36b6-593f-3c6f-687ad06cd40b)",
        "urn:li:dashboard:(tableau,20e44c22-1ccd-301a-220c-7b6837d09a52)",
        "urn:li:dashboard:(tableau,39b7a1de-6276-cfc7-9b59-1d22f3bbb06b)",
    ]
    assert sorted(deleted_dashboard_urns) == sorted(difference_dashboard_urns)
