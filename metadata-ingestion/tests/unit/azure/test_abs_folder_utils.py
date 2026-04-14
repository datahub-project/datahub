from types import SimpleNamespace
from unittest.mock import Mock

from datahub.ingestion.source.azure.abs_folder_utils import list_folders


def test_list_folders_with_nested_prefix() -> None:
    azure_config = Mock()
    blob_service_client = Mock()
    container_client = Mock()

    azure_config.get_blob_service_client.return_value = blob_service_client
    blob_service_client.get_container_client.return_value = container_client
    container_client.list_blobs.return_value = [
        SimpleNamespace(name="database/table_one/_delta_log/000.json"),
        SimpleNamespace(name="database/table_one/part-000.parquet"),
        SimpleNamespace(name="database/table_two/_delta_log/000.json"),
    ]

    folders = list(list_folders("container", "database", azure_config))

    assert folders == ["database/table_one", "database/table_two"]


def test_list_folders_from_container_root() -> None:
    azure_config = Mock()
    blob_service_client = Mock()
    container_client = Mock()

    azure_config.get_blob_service_client.return_value = blob_service_client
    blob_service_client.get_container_client.return_value = container_client
    container_client.list_blobs.return_value = [
        SimpleNamespace(name="database/table_one/_delta_log/000.json"),
        SimpleNamespace(name="landing/file.json"),
    ]

    folders = list(list_folders("container", "", azure_config))

    assert folders == ["database", "landing"]
