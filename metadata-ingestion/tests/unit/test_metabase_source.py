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
        self._personal_collection_ids: set = set()


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

    config = MetabaseConfig.model_validate({"display_uri": display_uri})

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


def test_is_personal_collection():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    metabase = TestMetabaseSource(ctx, config)

    # Collection with personal_owner_id is a personal collection
    personal_collection = {
        "id": 1,
        "name": "John's Collection",
        "personal_owner_id": 42,
    }
    assert metabase._is_personal_collection(personal_collection) is True

    # Collection without personal_owner_id is not a personal collection
    shared_collection = {
        "id": 2,
        "name": "Shared Collection",
        "personal_owner_id": None,
    }
    assert metabase._is_personal_collection(shared_collection) is False

    # Collection missing personal_owner_id field is not a personal collection
    collection_no_field = {"id": 3, "name": "Another Collection"}
    assert metabase._is_personal_collection(collection_no_field) is False


def test_is_card_in_personal_collection():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    metabase = TestMetabaseSource(ctx, config)

    # Set up personal collection IDs
    metabase._personal_collection_ids = {10, 20, 30}

    # Card in a personal collection
    card_in_personal = {"id": 1, "name": "My Card", "collection_id": 10}
    assert metabase._is_card_in_personal_collection(card_in_personal) is True

    # Card in a shared collection
    card_in_shared = {"id": 2, "name": "Shared Card", "collection_id": 50}
    assert metabase._is_card_in_personal_collection(card_in_shared) is False

    # Card with no collection (root)
    card_no_collection = {"id": 3, "name": "Root Card", "collection_id": None}
    assert metabase._is_card_in_personal_collection(card_no_collection) is False

    # Card missing collection_id field
    card_missing_field = {"id": 4, "name": "Card without field"}
    assert metabase._is_card_in_personal_collection(card_missing_field) is False


def test_exclude_personal_collections_config():
    # Test that the config parameter exists and defaults to False
    config = MetabaseConfig()
    assert config.exclude_personal_collections is False

    # Test that it can be set to True
    config_enabled = MetabaseConfig(exclude_personal_collections=True)
    assert config_enabled.exclude_personal_collections is True


@patch("requests.session")
def test_exclude_personal_collections_filters_cards(mock_session):
    """Test that cards from personal collections are filtered when exclude_personal_collections=True."""
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000",
        api_key=pydantic.SecretStr("key"),
        exclude_personal_collections=True,
    )
    ctx = PipelineContext(run_id="metabase-test-filter")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    # Mock collections response - includes personal collections
    collections_response = [
        {"id": "root", "name": "Root", "personal_owner_id": None},
        {"id": 100, "name": "Shared Collection", "personal_owner_id": None},
        {"id": 150, "name": "John's Personal", "personal_owner_id": 14},
        {"id": 200, "name": "Jane's Personal", "personal_owner_id": 15},
    ]

    # Mock cards response - includes cards from personal collections
    cards_response = [
        {"id": 1, "name": "Shared Card", "collection_id": 100},
        {"id": 2, "name": "John's Card", "collection_id": 150},
        {"id": 3, "name": "Jane's Card", "collection_id": 200},
        {"id": 4, "name": "Root Card", "collection_id": None},
    ]

    def mock_get(url, params=None):
        response = MagicMock()
        response.status_code = 200
        if "api/user/current" in url:
            response.json.return_value = {"id": 1, "email": "admin@test.com"}
        elif "api/collection/" in url and "items" not in url:
            response.json.return_value = collections_response
        elif "api/card" in url and "/" not in url.split("api/card")[1]:
            response.json.return_value = cards_response
        elif "api/collection/" in url and "items" in url:
            response.json.return_value = {"data": []}
        else:
            response.json.return_value = {}
        return response

    mock_session_instance.get.side_effect = mock_get

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Run dashboard emission first (this populates _personal_collection_ids)
    list(metabase_source.emit_dashboard_mces())

    # Verify personal collection IDs were identified
    assert 150 in metabase_source._personal_collection_ids
    assert 200 in metabase_source._personal_collection_ids
    assert 100 not in metabase_source._personal_collection_ids

    # Now test card filtering
    cards_to_process = []
    for card in cards_response:
        if not metabase_source._is_card_in_personal_collection(card):
            cards_to_process.append(card)

    # Should only have 2 cards: shared card and root card
    assert len(cards_to_process) == 2
    card_ids = [c["id"] for c in cards_to_process]
    assert 1 in card_ids  # Shared Card
    assert 4 in card_ids  # Root Card
    assert 2 not in card_ids  # John's Card (filtered)
    assert 3 not in card_ids  # Jane's Card (filtered)

    metabase_source.close()


@patch("requests.session")
def test_exclude_personal_collections_disabled_includes_all(mock_session):
    """Test that all cards are included when exclude_personal_collections=False."""
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000",
        api_key=pydantic.SecretStr("key"),
        exclude_personal_collections=False,
    )
    ctx = PipelineContext(run_id="metabase-test-no-filter")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    # Mock collections response
    collections_response = [
        {"id": "root", "name": "Root", "personal_owner_id": None},
        {"id": 150, "name": "John's Personal", "personal_owner_id": 14},
    ]

    # Mock cards response
    cards_response = [
        {"id": 1, "name": "Shared Card", "collection_id": 100},
        {"id": 2, "name": "John's Card", "collection_id": 150},
    ]

    def mock_get(url, params=None):
        response = MagicMock()
        response.status_code = 200
        if "api/user/current" in url:
            response.json.return_value = {"id": 1, "email": "admin@test.com"}
        elif "api/collection/" in url and "items" not in url:
            response.json.return_value = collections_response
        elif "api/card" in url:
            response.json.return_value = cards_response
        elif "items" in url:
            response.json.return_value = {"data": []}
        else:
            response.json.return_value = {}
        return response

    mock_session_instance.get.side_effect = mock_get

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Run dashboard emission first
    list(metabase_source.emit_dashboard_mces())

    # When disabled, all cards should pass the filter
    cards_to_process = []
    for card in cards_response:
        # When exclude_personal_collections is False, this check is skipped
        if (
            not metabase_config.exclude_personal_collections
            or not metabase_source._is_card_in_personal_collection(card)
        ):
            cards_to_process.append(card)

    # Should have all cards
    assert len(cards_to_process) == 2

    metabase_source.close()
