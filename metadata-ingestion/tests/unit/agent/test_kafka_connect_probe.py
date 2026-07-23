from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DataFlowSubTypes
from datahub.ingestion.source.kafka_connect.kafka_connect_probe import (
    list_kafka_connect_children,
)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, connector_names):
        self._connector_names = connector_names
        self.closed = False

    def get(self, url):
        return _FakeResponse(self._connector_names)

    def close(self):
        self.closed = True


def _config(connector_names):
    session = _FakeSession(connector_names)
    config = SimpleNamespace(
        connector_patterns=AllowDenyPattern(allow=[".*"], deny=["^_.*"]),
        get_effective_connect_uri=lambda: "http://localhost:8083",
        get_connect_session=lambda: session,
    )
    return config, session


def test_list_connectors_reuses_connector_patterns_verdict():
    config, session = _config(["orders-sink", "_internal-connector"])
    result = list_kafka_connect_children(config, [], 100)
    assert result.supported
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders-sink"].kind == DataFlowSubTypes.DLT_PIPELINE
    assert by_name["orders-sink"].pattern_field == "connector_patterns"
    assert by_name["orders-sink"].included is True
    # The connector's own connector_patterns verdict is reused, not re-implemented.
    assert by_name["_internal-connector"].included is False
    assert by_name["_internal-connector"].excluded_by == "connector_patterns"
    assert session.closed


def test_connectors_are_a_flat_level():
    config, session = _config(["orders-sink"])
    # Connectors have no children, so a non-empty parent path lists nothing.
    result = list_kafka_connect_children(config, ["orders-sink"], 100)
    assert result.nodes == []
