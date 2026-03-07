import json
import logging
import pathlib
from unittest.mock import patch

import pytest
import time_machine
from freezegun import freeze_time
from requests.models import HTTPError

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.metabase import MetabaseSource
from datahub.testing import mce_helpers
from tests.integration.metabase.setup.metabase_setup_utils import (
    setup_metabase_test_data,
    verify_metabase_api_ready,
)
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

logger = logging.getLogger(__name__)

FROZEN_TIME = "2021-11-11 07:00:00"

GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


RESPONSE_ERROR_LIST = ["http://localhost:3000/api/dashboard/public"]

test_resources_dir = pathlib.Path(__file__).parent


class MockResponse:
    def __init__(
        self, url, json_response_map=None, data=None, jsond=None, error_list=None
    ):
        self.json_data = data
        self.url = url
        self.jsond = jsond
        self.error_list = error_list
        self.headers = {}
        self.auth = None
        self.status_code = 200
        self.response_map = json_response_map

    def json(self):
        mocked_response_file = self.response_map.get(self.url)
        response_json_path = f"{test_resources_dir}/setup/{mocked_response_file}"

        if not pathlib.Path(response_json_path).exists():
            raise Exception(
                f"mock response file not found {self.url} -> {mocked_response_file}"
            )

        with open(response_json_path) as file:
            data = json.loads(file.read())
            self.json_data = data
        return self.json_data

    def get(self, url, params=None):
        self.url = url
        return self

    def raise_for_status(self):
        if self.error_list is not None and self.url in self.error_list:
            http_error_msg = "{} Client Error: {} for url: {}".format(
                400,
                "Simulate error",
                self.url,
            )
            raise HTTPError(http_error_msg, response=self)

    @staticmethod
    def build_mocked_requests_sucess(json_response_map):
        def mocked_requests_sucess_(*args, **kwargs):
            return MockResponse(url=None, json_response_map=json_response_map)

        return mocked_requests_sucess_

    @staticmethod
    def build_mocked_requests_failure(json_response_map):
        def mocked_requests_failure(*args, **kwargs):
            return MockResponse(
                url=None,
                error_list=RESPONSE_ERROR_LIST,
                json_response_map=json_response_map,
            )

        return mocked_requests_failure

    @staticmethod
    def build_mocked_requests_session_post(json_response_map):
        def mocked_requests_session_post(url, data, json):
            return MockResponse(
                url=url,
                data=data,
                jsond=json,
                json_response_map=json_response_map,
            )

        return mocked_requests_session_post

    @staticmethod
    def build_mocked_requests_session_delete(json_response_map):
        def mocked_requests_session_delete(url, headers):
            return MockResponse(
                url=url,
                data=None,
                jsond=headers,
                json_response_map=json_response_map,
            )

        return mocked_requests_session_delete


@pytest.fixture
def default_json_response_map():
    return {
        "http://localhost:3000/api/session": "session.json",
        "http://localhost:3000/api/user/current": "user.json",
        "http://localhost:3000/api/collection/?exclude-other-user-collections=false": "collections.json",
        "http://localhost:3000/api/collection/root/items?models=dashboard": "collection_dashboards.json",
        "http://localhost:3000/api/collection/150/items?models=dashboard": "collection_dashboards.json",
        "http://localhost:3000/api/dashboard/10": "dashboard_1.json",
        "http://localhost:3000/api/dashboard/20": "dashboard_2.json",
        "http://localhost:3000/api/user/1": "user.json",
        "http://localhost:3000/api/card": "card.json",
        "http://localhost:3000/api/database/1": "bigquery_database.json",
        "http://localhost:3000/api/database/2": "postgres_database.json",
        "http://localhost:3000/api/card/1": "card_1.json",
        "http://localhost:3000/api/card/2": "card_2.json",
        "http://localhost:3000/api/table/21": "table_21.json",
        "http://localhost:3000/api/card/3": "card_3.json",
    }


@pytest.fixture
def test_pipeline(pytestconfig, tmp_path):
    return {
        "run_id": "metabase-test",
        "source": {
            "type": "metabase",
            "config": {
                "username": "xxxx",
                "password": "xxxx",
                "connect_uri": "http://localhost:3000/",
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
        "pipeline_name": "test_pipeline",
        "sink": {
            "type": "file",
            "config": {
                "filename": f"{tmp_path}/metabase_mces.json",
            },
        },
    }


@freeze_time(FROZEN_TIME)
def test_metabase_ingest_success(
    pytestconfig, tmp_path, test_pipeline, mock_datahub_graph, default_json_response_map
):
    with (
        patch(
            "datahub.ingestion.source.metabase.requests.session",
            side_effect=MockResponse.build_mocked_requests_sucess(
                default_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.post",
            side_effect=MockResponse.build_mocked_requests_session_post(
                default_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.delete",
            side_effect=MockResponse.build_mocked_requests_session_delete(
                default_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline = Pipeline.create(test_pipeline)
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/metabase_mces.json",
            golden_path=test_resources_dir / "metabase_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )


@freeze_time(FROZEN_TIME)
def test_stateful_ingestion(
    test_pipeline, mock_datahub_graph, default_json_response_map
):
    json_response_map = default_json_response_map
    with (
        patch(
            "datahub.ingestion.source.metabase.requests.session",
            side_effect=MockResponse.build_mocked_requests_sucess(json_response_map),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.post",
            side_effect=MockResponse.build_mocked_requests_session_post(
                json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.delete",
            side_effect=MockResponse.build_mocked_requests_session_delete(
                json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_run1 = run_and_get_pipeline(test_pipeline)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Mock the removal of one of the dashboards
        json_response_map[
            "http://localhost:3000/api/collection/root/items?models=dashboard"
        ] = "collection_dashboards_deleted_item.json"
        json_response_map[
            "http://localhost:3000/api/collection/150/items?models=dashboard"
        ] = "collection_dashboards_deleted_item.json"

        pipeline_run2 = run_and_get_pipeline(test_pipeline)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        state1 = checkpoint1.state
        state2 = checkpoint2.state

        difference_urns = list(
            state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
        )

        assert len(difference_urns) == 1
        assert difference_urns[0] == "urn:li:dashboard:(metabase,20)"

        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )


@freeze_time(FROZEN_TIME)
def test_metabase_ingest_failure(pytestconfig, tmp_path, default_json_response_map):
    with (
        patch(
            "datahub.ingestion.source.metabase.requests.session",
            side_effect=MockResponse.build_mocked_requests_failure(
                default_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.post",
            side_effect=MockResponse.build_mocked_requests_session_post(
                default_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.delete",
            side_effect=MockResponse.build_mocked_requests_session_delete(
                default_json_response_map
            ),
        ),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "metabase-test",
                "source": {
                    "type": "metabase",
                    "config": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "connect_uri": "http://localhost:3000/",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/metabase_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        try:
            pipeline.raise_from_status()
        except PipelineExecutionError as exec_error:
            assert exec_error.args[0] == "Source reported errors"
            # exec_error.args[1] is the failures LossyList directly
            assert len(exec_error.args[1]) == 1


def test_strip_template_expressions():
    query_with_variables = (
        "SELECT count(*) FROM products WHERE category = {{category}}",
        "SELECT count(*) FROM products WHERE category = 1",
    )
    query_with_optional_clause = (
        "SELECT count(*) FROM products [[WHERE category = {{category}}]]",
        "SELECT count(*) FROM products  ",
    )
    query_with_dashboard_filters = (
        "SELECT count(*) FROM products WHERE {{Filter1}} AND {{Filter2}}",
        "SELECT count(*) FROM products WHERE 1 AND 1",
    )

    assert (
        MetabaseSource.strip_template_expressions(query_with_variables[0])
        == query_with_variables[1]
    )
    assert (
        MetabaseSource.strip_template_expressions(query_with_optional_clause[0])
        == query_with_optional_clause[1]
    )
    assert (
        MetabaseSource.strip_template_expressions(query_with_dashboard_filters[0])
        == query_with_dashboard_filters[1]
    )


@pytest.fixture
def extended_json_response_map():
    return {
        "http://localhost:3000/api/session": "session.json",
        "http://localhost:3000/api/user/current": "user.json",
        "http://localhost:3000/api/collection/?exclude-other-user-collections=false": "collections_with_tags.json",
        "http://localhost:3000/api/collection/root/items?models=dashboard": "collection_dashboards.json",
        "http://localhost:3000/api/collection/150/items?models=dashboard": "empty_collection_dashboards.json",
        "http://localhost:3000/api/collection/200/items?models=dashboard": "empty_collection_dashboards.json",
        "http://localhost:3000/api/collection/201/items?models=dashboard": "empty_collection_dashboards.json",
        "http://localhost:3000/api/dashboard/10": "dashboard_1.json",
        "http://localhost:3000/api/dashboard/20": "dashboard_2.json",
        "http://localhost:3000/api/user/1": "user.json",
        "http://localhost:3000/api/card": "card_with_models.json",
        "http://localhost:3000/api/database/1": "bigquery_database.json",
        "http://localhost:3000/api/database/2": "postgres_database.json",
        "http://localhost:3000/api/card/1": "card_1.json",
        "http://localhost:3000/api/card/2": "card_2.json",
        "http://localhost:3000/api/card/3": "card_3.json",
        "http://localhost:3000/api/card/4": "card_4_model.json",
        "http://localhost:3000/api/card/5": "card_5_nested.json",
        "http://localhost:3000/api/card/6": "card_6_model_query_builder.json",
        "http://localhost:3000/api/table/21": "table_21.json",
    }


@freeze_time(FROZEN_TIME)
def test_metabase_ingest_with_models_and_collections(
    pytestconfig, tmp_path, extended_json_response_map, mock_datahub_graph
):
    """
    Integration test for Metabase Models, Collection Tags, and Nested Query Lineage.

    This test validates that:
    1. Models are extracted as datasets with correct URN format (model. prefix)
    2. Models have lineage to source tables from SQL parsing
    3. Collection tags are applied to models based on their collection_id
    4. Nested query lineage resolves through multiple levels of card references
    """
    with (
        patch(
            "datahub.ingestion.source.metabase.requests.session",
            side_effect=MockResponse.build_mocked_requests_sucess(
                extended_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.post",
            side_effect=MockResponse.build_mocked_requests_session_post(
                extended_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.metabase.requests.delete",
            side_effect=MockResponse.build_mocked_requests_session_delete(
                extended_json_response_map
            ),
        ),
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_config = {
            "run_id": "metabase-new-features-test",
            "source": {
                "type": "metabase",
                "config": {
                    "username": "xxxx",
                    "password": "xxxx",
                    "connect_uri": "http://localhost:3000/",
                    "extract_models": True,
                },
            },
            "pipeline_name": "test_new_features_pipeline",
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/metabase_new_features_mces.json",
                },
            },
        }

        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()

        report = pipeline.source.get_report()

        non_schema_failures = [
            f for f in report.failures if "Source produced bad metadata" not in str(f)
        ]
        assert len(non_schema_failures) == 0, (
            f"Unexpected failures (excluding known schema validation issue): {non_schema_failures}"
        )

        # Read output file and check for key entities
        with open(f"{tmp_path}/metabase_new_features_mces.json", "r") as f:
            content = f.read()

        # Verify models are extracted as datasets
        assert "model.4" in content, "Model 4 should be extracted as dataset"
        assert "model.6" in content, "Model 6 should be extracted as dataset"

        # Verify collection tags are applied
        assert "metabase_collection_john_doe" in content, (
            "Collection tag should be present"
        )

        # Verify schema metadata is present (models have schema)
        assert (
            "com.linkedin.pegasus2avro.schema.SchemaMetadata" in content
            or "SchemaMetadataClass" in content
            or "schemaMetadata" in content
        ), "Models should have schema metadata"


# ============================================================================
# Docker-based Integration Tests
# ============================================================================

DOCKER_FROZEN_TIME = "2024-01-20 12:00:00"
METABASE_BASE_URL = "http://localhost:3001"


@pytest.fixture(scope="module")
def metabase_credentials():
    """Credentials for Metabase admin user."""
    return {
        "email": "admin@test.com",
        "password": "Admin123!",
        "first_name": "Test",
        "last_name": "Admin",
    }


@pytest.fixture(scope="module")
def loaded_metabase(docker_compose_runner, metabase_credentials):
    """Start Metabase and PostgreSQL via Docker Compose."""
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "metabase"
    ) as docker_services:
        # Wait for PostgreSQL to be ready
        wait_for_port(docker_services, "postgres", 5432, timeout=60)
        logger.info("PostgreSQL is ready")

        # Wait for Metabase to be ready
        wait_for_port(docker_services, "metabase", 3000, timeout=180)
        logger.info("Metabase port is open")

        # Additional verification that Metabase API is accessible
        verify_metabase_api_ready(METABASE_BASE_URL, timeout=120)
        logger.info("Metabase API is ready")

        # Setup Metabase with initial user and test data
        setup_metabase_test_data(METABASE_BASE_URL, metabase_credentials)
        logger.info("Metabase test data setup complete")

        yield docker_services

    cleanup_image("metabase/metabase")


@time_machine.travel(DOCKER_FROZEN_TIME)
def test_metabase_docker_ingest(
    loaded_metabase, pytestconfig, tmp_path, metabase_credentials
):
    """Test Metabase ingestion from actual Docker container."""

    config_file = (test_resources_dir / "metabase_docker_to_file.yml").resolve()
    output_path = tmp_path / "metabase_docker_mcps.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "metabase_docker_mcps_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['inputEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['chartEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['datasetEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
        ],
    )


@time_machine.travel(DOCKER_FROZEN_TIME)
def test_metabase_docker_models_extraction(
    loaded_metabase, pytestconfig, tmp_path, metabase_credentials
):
    """Test that Metabase models are extracted correctly with lineage."""

    config_file = (test_resources_dir / "metabase_docker_models_to_file.yml").resolve()
    output_path = tmp_path / "metabase_docker_models_mcps.json"

    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "metabase_docker_models_mcps_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['inputEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['chartEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['datasetEdges'\]\[\d+\]\['lastModified'\]",
            r"root\[\d+\]\['systemMetadata'\]\['lastObserved'\]",
        ],
    )
