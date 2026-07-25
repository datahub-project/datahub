from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.pinecone.pinecone_probe import list_pinecone_children
from tests.unit.agent.pattern_hint_fixtures import config_with_hints


class _Index:
    def __init__(self, name):
        self.name = name


class _Namespace:
    def __init__(self, name):
        self.name = name


class _PineconeClient:
    def __init__(self, indexes, namespaces):
        self._indexes = indexes
        self._namespaces = namespaces

    def list_indexes(self):
        return [_Index(n) for n in self._indexes]

    def list_namespaces(self, index_name):
        return [_Namespace(n) for n in self._namespaces.get(index_name, [])]


def _config():
    return config_with_hints(
        {
            "index_pattern": DatasetContainerSubTypes.PINECONE_INDEX,
            "namespace_pattern": DatasetContainerSubTypes.PINECONE_NAMESPACE,
        },
        get_client=lambda: _PineconeClient(
            ["products", "scratch"], {"products": ["prod", "__default__"]}
        ),
        index_pattern=AllowDenyPattern(allow=[".*"], deny=["^scratch$"]),
        namespace_pattern=AllowDenyPattern(allow=[".*"], deny=["^__default__$"]),
    )


def test_pinecone_lists_indexes_with_pattern_verdict():
    result = list_pinecone_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["products"].kind == DatasetContainerSubTypes.PINECONE_INDEX
    assert by_name["products"].pattern_field == "index_pattern"
    assert by_name["products"].included is True
    assert by_name["scratch"].included is False
    assert by_name["scratch"].excluded_by == "index_pattern"


def test_pinecone_lists_namespaces_reusing_namespace_pattern():
    result = list_pinecone_children(_config(), ["products"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["prod"].kind == DatasetContainerSubTypes.PINECONE_NAMESPACE
    assert by_name["prod"].included is True
    assert by_name["__default__"].included is False
    assert by_name["__default__"].excluded_by == "namespace_pattern"
