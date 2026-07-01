from unittest.mock import MagicMock, patch

from lib.graphql_adapt import (
    GraphqlQueryRegistry,
    create_graphql_query_registry,
    setup_graphql_queries,
)
from lib.personas import BenchmarkPack, PersonaBenchmark, Scenario
from lib.query_spec import QuerySpec


@patch("lib.graphql_adapt.GraphqlQueryRegistry")
def test_create_graphql_query_registry(mock_cls: MagicMock) -> None:
    specs = {"getMe": MagicMock(spec=QuerySpec)}
    registry = create_graphql_query_registry("https://dev04.example.com/gms", "token", specs)
    assert registry is mock_cls.return_value
    mock_cls.assert_called_once_with(
        gms_url="https://dev04.example.com/gms",
        token="token",
        specs=specs,
    )


@patch("lib.graphql_adapt.create_graphql_query_registry")
def test_setup_graphql_queries(mock_create: MagicMock) -> None:
    registry = MagicMock()
    mock_create.return_value = registry
    pack = BenchmarkPack(
        query_specs={"getMe": MagicMock(spec=QuerySpec)},
        benchmarks={},
    )
    result = setup_graphql_queries("http://localhost:8080", "token", pack)
    assert result is registry
    registry.build_all.assert_called_once()
