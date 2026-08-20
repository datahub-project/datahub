"""Materialize the datasets consumed by the demo agents as real entities.

The agents declare consumed datasets via upstreamLineage (urn references). This
script reads each agent's upstreamLineage and emits datasetProperties for every
referenced dataset so they render as named nodes in the lineage graph (rather
than bare URNs).

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    # export DATAHUB_GMS_TOKEN=<token>             # if auth is enabled
    python materialize_consumed_datasets.py
"""

from __future__ import annotations

import logging
from typing import Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper as MCP
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import DatasetPropertiesClass, UpstreamLineageClass
from datahub.metadata.urns import DatasetUrn

logger = logging.getLogger(__name__)

AGENTS = [
    "urn:li:aiAgent:fx-risk-scoring-agent",
    "urn:li:aiAgent:customer-triage-agent",
    "urn:li:aiAgent:data-quality-sentinel-agent",
    "urn:li:aiAgent:marketing-insights-agent",
    "urn:li:aiAgent:revenue-forecasting-agent",
]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    graph = get_default_graph()
    dataset_urns: Set[str] = set()
    for agent in AGENTS:
        upstream = graph.get_aspect(agent, UpstreamLineageClass)
        if upstream:
            for up in upstream.upstreams:
                dataset_urns.add(up.dataset)
    logger.info("distinct consumed datasets: %d", len(dataset_urns))
    for ds in sorted(dataset_urns):
        u = DatasetUrn.from_string(ds)
        platform = u.platform.split(":")[-1]
        graph.emit(
            MCP(
                entityUrn=ds,
                aspect=DatasetPropertiesClass(
                    name=u.name.split(".")[-1],
                    qualifiedName=u.name,
                    description=f"Source dataset consumed by AI agents (platform: {platform}).",
                ),
            )
        )
        logger.info("materialized: %s %s", platform, u.name)
    print("Done.")


if __name__ == "__main__":
    main()
