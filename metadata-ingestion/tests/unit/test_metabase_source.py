from unittest.mock import MagicMock, patch

import pydantic

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
    MetabaseConfig,
    MetabaseReport,
    MetabaseSource,
)


class TestMetabaseSource(MetabaseSource):
    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        self.config = config
        self.report = MetabaseReport()


def test_get_platform_instance():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    # config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    # config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    metabase = TestMetabaseSource(ctx, config)

    # no mappings defined
    assert metabase.get_platform_instance("clickhouse", 42) is None

    # database_id_to_instance_map is defined, key is present
    metabase.config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    assert metabase.get_platform_instance(None, 42) == "my_main_clickhouse"

    # database_id_to_instance_map is defined, key is missing
    assert metabase.get_platform_instance(None, 999) is None

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key present
    metabase.config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None

    # database_id_to_instance_map is missing, platform_instance_map is defined and key present
    metabase.config.database_id_to_instance_map = None
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = MetabaseConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "localhost:3000"
    assert config.display_uri == display_uri


@patch("requests.session")
def test_connection_uses_api_key_if_in_config(mock_session):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", api_key=pydantic.SecretStr("key")
    )
    ctx = PipelineContext(run_id="metabase-test-apikey")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_session_instance.get.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    mock_session_instance.get.assert_called_once_with("localhost:3000/api/user/current")
    request_headers = mock_session_instance.headers
    assert request_headers["x-api-key"] == "key"


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_create_session_from_config_username_password(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    kwargs_post = mock_post.call_args
    assert kwargs_post[0][0] == "localhost:3000/api/session"
    assert kwargs_post[0][2]["password"] == "pwd"
    assert kwargs_post[0][2]["username"] == "un"

    kwargs_get = mock_get.call_args
    assert kwargs_get[0][0] == "localhost:3000/api/user/current"

    mock_delete.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_fail_session_delete(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response

    mock_response_delete = MagicMock()
    mock_response_delete.status_code = 400
    mock_delete.return_value = mock_response_delete

    mock_report = MagicMock()

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.report = mock_report
    metabase_source.close()

    mock_report.report_failure.assert_called_once()
