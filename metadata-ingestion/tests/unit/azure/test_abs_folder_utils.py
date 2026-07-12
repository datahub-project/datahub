from types import SimpleNamespace
from unittest.mock import Mock

from datahub.ingestion.source.azure.abs_folder_utils import list_folders


def test_list_folders_with_nested_prefix() -> None:
    azure_config = Mock()
    blob_service_client = Mock()
    container_client = Mock()

    azure_config.get_blob_service_client.return_value = blob_service_client
    blob_service_client.get_container_client.return_value = container_client
    # Sub-folders end with "/"; the blob at this level (rootfile.json) is skipped.
    container_client.walk_blobs.return_value = [
        SimpleNamespace(name="database/table_one/"),
        SimpleNamespace(name="database/rootfile.json"),
        SimpleNamespace(name="database/table_two/"),
    ]

    folders = list(list_folders("container", "database", azure_config))

    assert folders == ["database/table_one", "database/table_two"]
    container_client.walk_blobs.assert_called_once_with(
        name_starts_with="database/", delimiter="/"
    )
    container_client.list_blobs.assert_not_called()


def test_list_folders_from_container_root() -> None:
    azure_config = Mock()
    blob_service_client = Mock()
    container_client = Mock()

    azure_config.get_blob_service_client.return_value = blob_service_client
    blob_service_client.get_container_client.return_value = container_client
    container_client.walk_blobs.return_value = [
        SimpleNamespace(name="database/"),
        SimpleNamespace(name="landing/"),
    ]

    folders = list(list_folders("container", "", azure_config))

    assert folders == ["database", "landing"]
    container_client.walk_blobs.assert_called_once_with(
        name_starts_with="", delimiter="/"
    )
    container_client.list_blobs.assert_not_called()
