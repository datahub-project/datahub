from unittest.mock import Mock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery


class TestDatahubExecutorDiscovery:

    def setup_method(self) -> None:
        pass

    def test_backend_revision_check_legacy(self) -> None:
        graph = Mock(spec=DataHubGraph)
        graph.get_config.return_value = {}
        discovery = DatahubExecutorDiscovery(graph)

        assert not discovery.is_backend_discovery_capable()

    def test_backend_revision_check_new(self) -> None:
        graph = Mock(spec=DataHubGraph)
        graph.get_config.return_value = {
            "baseUrl": "http://localhost.localdomain",
            "remoteExecutorBackend": {
                "revision": 1,
                "instanceId": "test-customer-0",
            },
        }
        discovery = DatahubExecutorDiscovery(graph)
        assert discovery.is_backend_discovery_capable()
