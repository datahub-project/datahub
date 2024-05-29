import contextlib
import contextvars
from typing import Iterator, Optional

from datahub.ingestion.graph.client import DataHubGraph

_graph_context = contextvars.ContextVar[Optional[DataHubGraph]]("datahub_graph_context")


def get_graph_context() -> Optional[DataHubGraph]:
    try:
        return _graph_context.get()
    except LookupError:
        return None


@contextlib.contextmanager
def set_graph_context(graph: Optional[DataHubGraph]) -> Iterator[None]:
    token = _graph_context.set(graph)
    try:
        yield
    finally:
        _graph_context.reset(token)
