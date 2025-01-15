from typing import overload

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DataJobUrn, DatasetUrn, Urn
from datahub.sdk._shared import UrnOrStr
from datahub.sdk.dataset import DatajobUrnOrStr, DatasetUrnOrStr


class LineageClient:
    def __init__(self, graph: DataHubGraph):
        self._graph = graph

    # TODO support column-level lineage
    @overload
    def add_lineage(
        self, *, upstream: DatasetUrnOrStr, downstream: DatasetUrnOrStr
    ) -> None:
        ...

    @overload
    # TODO support column-level lineage
    def add_lineage(
        self, *, upstream: DatajobUrnOrStr, downstream: DatasetUrnOrStr
    ) -> None:
        ...

    @overload
    # TODO support column-level lineage
    def add_lineage(
        self, *, upstream: DatasetUrnOrStr, downstream: DatajobUrnOrStr
    ) -> None:
        ...

    def add_lineage(self, *, upstream: UrnOrStr, downstream: UrnOrStr) -> None:
        upstream = Urn.from_string(upstream)
        downstream = Urn.from_string(downstream)

        if isinstance(upstream, DataJobUrn) or isinstance(downstream, DataJobUrn):
            # TODO assert that both are not datajobs
            raise NotImplementedError("DataJobUrn is not supported for add_lineage")
        elif isinstance(downstream, DatasetUrn):
            if not isinstance(upstream, DatasetUrn):
                raise ValueError("upstream must be a DatasetUrn")

            raise NotImplementedError("TODO")
        else:
            raise TypeError(
                f"Cannot add lineage between {upstream.entity_type} and {downstream.entity_type}: {upstream} -> {downstream}"
            )
