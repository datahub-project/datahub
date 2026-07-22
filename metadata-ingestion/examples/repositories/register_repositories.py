"""Register source-code repositories into DataHub as first-class ``Repository`` entities.

A ``Repository`` (``urn:li:repository:<id>``) is a catalog entry for a source-code
repository (GitHub, GitLab, internal SCM, etc.). It is the genesis node of the
software-to-data lifecycle (repo -> service -> api -> app -> dataset): it *produces*
the services / APIs / apps cataloged elsewhere. Unlike a Dataset it is NOT tabular
-- no columns, queries, stats, quality, or preview.

The Repository is platform-agnostic: the platform is carried in the
``dataPlatformInstance`` aspect, not the key. The id is constructed by convention
as ``<platform>.<org>/<name>`` (e.g. ``github.acme/payments``).

This script also wires the ``repo -> service -> api`` chain by setting
``ServiceProperties.sourceRepository`` on the existing ``order-entry-api``
service -- the ``SourcedFrom`` provenance edge. The endpoints are reached one
hop further down, via the service's ``ServiceComposesApi`` lineage edge, so the
chain nests as ``repo -> service -> endpoint`` (the repo does not link to each
endpoint directly). The Repository profile shows incoming ``SourcedFrom`` edges
as "what this repo produces".

Everything upserts by URN, so the script is idempotent.

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080   # GMS, not the frontend
    # export DATAHUB_GMS_TOKEN=<token>             # if auth is enabled
    python register_repositories.py

    # View a profile at (frontend):
    #   http://localhost:9002/repository/urn:li:repository:github.acme/payments
"""

from __future__ import annotations

import logging

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    RepositoryPropertiesClass,
    RepositorySourceClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REPOSITORY_ID = "github.acme/payments"
REPOSITORY_URN = f"urn:li:repository:{REPOSITORY_ID}"
PLATFORM = "github"

# The service (and its APIs) this repository produces -- the SourcedFrom edge.
ORDER_ENTRY_SERVICE_URN = "urn:li:service:order-entry-api"

# Subtype stored in the standard subTypes aspect (escape hatch for future repo kinds).
GIT_REPOSITORY_SUBTYPE = "GIT_REPOSITORY"


def register_repository(graph: DataHubGraph) -> None:
    aspects = [
        RepositoryPropertiesClass(
            name="payments",
            description=(
                "Payments platform service repository. Produces the Order Entry API "
                "and downstream trade-order services."
            ),
            defaultBranch="main",
            languages=["Java", "Kotlin"],
            license="Apache-2.0",
            homepageUrl="https://github.com/acme/payments",
            archived=False,
        ),
        RepositorySourceClass(
            externalUrl="https://github.com/acme/payments",
            externalId="acme/payments",
        ),
        SubTypesClass(typeNames=[GIT_REPOSITORY_SUBTYPE]),
        DataPlatformInstanceClass(platform=make_data_platform_urn(PLATFORM)),
        StatusClass(removed=False),
    ]
    for aspect in aspects:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=REPOSITORY_URN, aspect=aspect)
        )
    logger.info("Registered Repository %s", REPOSITORY_URN)


def wire_sourced_from(graph: DataHubGraph) -> None:
    """Set the SourcedFrom edge on the order-entry-api service and its APIs.

    Reads the existing aspect and re-emits it with ``sourceRepository`` added so
    we never clobber displayName / apis / other populated fields.
    """
    service_props = graph.get_aspect(ORDER_ENTRY_SERVICE_URN, ServicePropertiesClass)
    if service_props is None:
        logger.warning(
            "Service %s not found -- run register_rest_services.py first to see the "
            "repo -> service -> api chain. Repository was still registered.",
            ORDER_ENTRY_SERVICE_URN,
        )
        return

    service_props.sourceRepository = REPOSITORY_URN
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=ORDER_ENTRY_SERVICE_URN, aspect=service_props
        )
    )
    logger.info("Wired SourcedFrom: %s -> %s", ORDER_ENTRY_SERVICE_URN, REPOSITORY_URN)

    # The endpoints are intentionally NOT sourced-from the repo directly: they
    # hang off their service via the ServiceComposesApi lineage edge, so the
    # chain nests as repo -> service -> endpoint rather than the repo fanning out
    # to the service and every endpoint in parallel.


def main() -> None:
    graph = get_default_graph()
    register_repository(graph)
    wire_sourced_from(graph)


if __name__ == "__main__":
    main()
