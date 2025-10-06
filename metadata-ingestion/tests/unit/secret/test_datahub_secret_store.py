from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.secret.datahub_secret_store import (
    DataHubSecretStore,
    DataHubSecretStoreConfig,
)
from datahub.secret.datahub_secrets_client import DataHubSecretsClient


class TestDataHubSecretStore:
    def test_init_with_graph_client(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        config = DataHubSecretStoreConfig(graph_client=mock_graph)
        store = DataHubSecretStore(config)

        assert store.client is not None
        assert isinstance(store.client, DataHubSecretsClient)
        mock_graph.test_connection.assert_called_once()

    def test_init_with_graph_client_config(self):
        mock_client_config = Mock(spec=DatahubClientConfig)

        with patch(
            "datahub.secret.datahub_secret_store.DataHubGraph"
        ) as mock_graph_class:
            mock_graph = Mock(spec=DataHubGraph)
            mock_graph_class.return_value = mock_graph

            config = DataHubSecretStoreConfig(graph_client_config=mock_client_config)
            store = DataHubSecretStore(config)

            assert store.client is not None
            mock_graph_class.assert_called_once_with(mock_client_config)

    def test_init_with_no_config_raises_exception(self):
        config = DataHubSecretStoreConfig()

        with pytest.raises(Exception, match="Invalid configuration provided"):
            DataHubSecretStore(config)

    def test_get_secret_values_success(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        expected_secrets = {"secret1": "value1", "secret2": "value2"}

        with patch(
            "datahub.secret.datahub_secret_store.DataHubSecretsClient"
        ) as mock_client_class:
            mock_client = Mock(spec=DataHubSecretsClient)
            mock_client.get_secret_values.return_value = expected_secrets
            mock_client_class.return_value = mock_client

            config = DataHubSecretStoreConfig(graph_client=mock_graph)
            store = DataHubSecretStore(config)

            result = store.get_secret_values(["secret1", "secret2"])

            assert result == expected_secrets
            mock_client.get_secret_values.assert_called_once_with(
                ["secret1", "secret2"]
            )

    def test_get_secret_values_exception_handling(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        with patch(
            "datahub.secret.datahub_secret_store.DataHubSecretsClient"
        ) as mock_client_class:
            mock_client = Mock(spec=DataHubSecretsClient)
            mock_client.get_secret_values.side_effect = Exception("Connection failed")
            mock_client_class.return_value = mock_client

            config = DataHubSecretStoreConfig(graph_client=mock_graph)
            store = DataHubSecretStore(config)

            with patch("datahub.secret.datahub_secret_store.logger") as mock_logger:
                result = store.get_secret_values(["secret1"])

                assert result == {}
                mock_logger.exception.assert_called_once()

    def test_get_secret_value(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        with patch(
            "datahub.secret.datahub_secret_store.DataHubSecretsClient"
        ) as mock_client_class:
            mock_client = Mock(spec=DataHubSecretsClient)
            mock_client.get_secret_values.return_value = {"secret1": "value1"}
            mock_client_class.return_value = mock_client

            config = DataHubSecretStoreConfig(graph_client=mock_graph)
            store = DataHubSecretStore(config)

            result = store.get_secret_value("secret1")

            assert result == "value1"
            mock_client.get_secret_values.assert_called_once_with(["secret1"])

    def test_get_secret_value_not_found(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        with patch(
            "datahub.secret.datahub_secret_store.DataHubSecretsClient"
        ) as mock_client_class:
            mock_client = Mock(spec=DataHubSecretsClient)
            mock_client.get_secret_values.return_value = {}
            mock_client_class.return_value = mock_client

            config = DataHubSecretStoreConfig(graph_client=mock_graph)
            store = DataHubSecretStore(config)

            result = store.get_secret_value("nonexistent")

            assert result is None

    def test_get_id(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        config = DataHubSecretStoreConfig(graph_client=mock_graph)
        store = DataHubSecretStore(config)

        assert store.get_id() == "datahub"

    def test_create_classmethod(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        config_dict = {"graph_client": mock_graph}

        store = DataHubSecretStore.create(config_dict)

        assert isinstance(store, DataHubSecretStore)
        assert store.client is not None

    def test_close(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        with patch(
            "datahub.secret.datahub_secret_store.DataHubSecretsClient"
        ) as mock_client_class:
            mock_client = Mock(spec=DataHubSecretsClient)
            mock_client.graph = mock_graph
            mock_client_class.return_value = mock_client

            config = DataHubSecretStoreConfig(graph_client=mock_graph)
            store = DataHubSecretStore(config)

            store.close()

            mock_graph.close.assert_called_once()

    def test_config_validator_with_working_connection(self):
        mock_graph = Mock(spec=DataHubGraph)
        mock_graph.test_connection.return_value = True

        config = DataHubSecretStoreConfig(graph_client=mock_graph)

        assert config.graph_client == mock_graph
        mock_graph.test_connection.assert_called_once()

    def test_config_validator_with_none_graph_client(self):
        config = DataHubSecretStoreConfig(graph_client=None)

        assert config.graph_client is None
