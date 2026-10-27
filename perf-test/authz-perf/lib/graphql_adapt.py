from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from lib.graphql import log
from lib.personas import BenchmarkPack
from lib.query_build import build_operation_document
from lib.query_spec import QuerySpec


@dataclass
class GraphqlQueryRegistry:
    """Build and cache schema-driven GraphQL documents for benchmark operations."""

    gms_url: str
    token: str
    specs: Dict[str, QuerySpec]
    _projector: object = field(init=False, repr=False)
    _graph: object = field(init=False, repr=False)
    _schema: object = field(init=False, repr=False)
    _documents: Dict[str, str] = field(default_factory=dict, init=False)
    _omitted: Dict[str, List[str]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        try:
            from datahub.ingestion.graph.client import DataHubGraph
            from datahub.ingestion.graph.config import DatahubClientConfig
            from datahub.utilities.graphql_query_adapter import QueryProjector
        except ImportError as exc:
            raise RuntimeError(
                "GraphQL schema setup requires acryl-datahub with "
                "datahub.utilities.graphql_query_adapter (QueryProjector). "
                "Install a recent acryl-datahub or editable metadata-ingestion."
            ) from exc

        self._projector = QueryProjector()
        self._graph = DataHubGraph(
            DatahubClientConfig(server=self.gms_url, token=self.token)
        )
        self._schema = self._projector._get_schema(self._graph)  # type: ignore[attr-defined]

    def build_all(self) -> None:
        for operation, spec in self.specs.items():
            if operation in self._documents:
                continue
            document, omitted = build_operation_document(spec, self._schema)  # type: ignore[arg-type]
            if omitted:
                preview = ", ".join(omitted[:5])
                suffix = f" (+{len(omitted) - 5} more)" if len(omitted) > 5 else ""
                log(
                    f"graphql generate ({operation}): omitted {len(omitted)} "
                    f"unsupported path(s): {preview}{suffix}"
                )
            self._documents[operation] = document
            self._omitted[operation] = omitted

    def document(self, operation: str) -> str:
        doc = self._documents.get(operation)
        if doc is None:
            spec = self.specs.get(operation)
            if spec is None:
                raise KeyError(f"No query_specs entry for operation {operation!r}")
            document, omitted = build_operation_document(spec, self._schema)  # type: ignore[arg-type]
            self._documents[operation] = document
            self._omitted[operation] = omitted
            return document
        return doc

    def omitted_paths(self, operation: str) -> List[str]:
        return list(self._omitted.get(operation, []))


def create_graphql_query_registry(
    gms_url: str,
    token: str,
    specs: Dict[str, QuerySpec],
) -> GraphqlQueryRegistry:
    return GraphqlQueryRegistry(gms_url=gms_url, token=token, specs=specs)


def setup_graphql_queries(
    gms_url: str,
    token: str,
    pack: BenchmarkPack,
) -> GraphqlQueryRegistry:
    """Introspect target GMS and generate all benchmark operations before timing."""
    log(f"GraphQL schema setup: introspecting {gms_url}")
    registry = create_graphql_query_registry(gms_url, token, pack.query_specs)
    registry.build_all()
    log(f"graphql schema ready: {len(pack.query_specs)} operation(s) generated for target")
    return registry


def ensure_graphql_queries(registry: GraphqlQueryRegistry, pack: BenchmarkPack) -> None:
    seen: set[str] = set()
    for benchmark in pack.benchmarks.values():
        for scenario in benchmark.scenarios:
            if scenario.operation in seen:
                continue
            seen.add(scenario.operation)
            registry.document(scenario.operation)
