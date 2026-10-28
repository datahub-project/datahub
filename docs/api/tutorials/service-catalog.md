---
description: "Populate the DataHub Service Catalog — ingest an OpenAPI spec as a Service, or register services, APIs, and repositories explicitly with the SDK."
---

# Registering Services, APIs, and Repositories

This tutorial shows how to populate the [Service Catalog](../../features/feature-guides/service-catalog.md)
programmatically. There are two ways in:

- **Ingest an OpenAPI spec** — the paved path: point a script at an OpenAPI document and it creates
  the Service, one API per endpoint (with typed signatures), and stores the whole spec as the
  service's contract. Best for REST services you already describe with OpenAPI.
- **Register explicitly** — describe services, APIs, and repositories with the SDK. Use this for
  gRPC/GraphQL/MCP services, source repositories, or anything without an OpenAPI doc.

The three entities form a chain — a **repository** builds a **service**, and a service composes its
**APIs** (`ServiceComposesApi`) — so the graph nests as repository → service → endpoint.

## Prerequisites

```bash
pip install acryl-datahub
datahub init   # writes ~/.datahubenv with your GMS URL and token
```

```python
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()  # or set DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN
```

## Ingest an OpenAPI spec (paved path)

The `ingest_openapi_as_service.py` example parses an OpenAPI document and emits the whole Service
shape for you:

```bash
python metadata-ingestion/examples/services/ingest_openapi_as_service.py \
  --spec-url https://api.example.com/openapi.json \
  --service-id order-entry-api
```

It emits:

- one **Service** (subtype `REST_API`) carrying the entire spec verbatim on a `serviceDefinition`
  aspect (`format = OPENAPI`), stored as a compressed large string so big specs don't hit the
  aspect-size limit — this is the **source of truth** the Contract tab renders;
- one **API** (subtype `REST_ENDPOINT`) per path + method, with input/output signatures parsed from
  the operation's parameters, request body, and `200` response schema;
- a `ServiceProperties.apis` edge (`ServiceComposesApi`) linking the service to every endpoint.

Run with no arguments and it targets DataHub GMS's own OpenAPI spec — a quick way to see the shape.
Very large specs are capped (the per-endpoint API entities are limited so a 1000-endpoint spec
doesn't flood the graph); the full document is always preserved on the contract.

## Register an endpoint (API) explicitly

An **API** (`urn:li:api:<id>`) is a named callable with a typed signature. The `Api` helper is the
same one used for agent tools — see the [Agent Registry tutorial](agent-registry.md#register-a-tool)
for the full reference. In brief:

```python
from datahub.api.entities.agent.api import Api, ApiParam, API_SUBTYPE_REST_ENDPOINT

place_order = Api(
    id="order-entry-api./orders.POST",
    name="POST /orders",
    subtypes=[API_SUBTYPE_REST_ENDPOINT],
    description="Place a trade order.",
    parameters=[ApiParam(name="body", data_type="object", required=True)],
    returns=[ApiParam(name="order", data_type="object")],
    method="POST",
    path="/orders",
)
api_urn = place_order.emit(graph)
```

## Register a service explicitly

Services carry identity and a contract but no schema/columns, so they're emitted as aspects. A
service composes its endpoints through `ServiceProperties.apis`:

```python
from datahub.api.entities.common.large_string import make_large_string
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    ServiceDefinitionClass,
    ServiceDefinitionFormatClass,
    ServiceLifecycleClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)

service_urn = "urn:li:service:order-entry-api"
aspects = [
    ServicePropertiesClass(
        displayName="Order Entry API",
        description="Place, look up, and cancel trade orders.",
        apis=[api_urn],                       # ServiceComposesApi edges
        lifecycle=ServiceLifecycleClass.PRODUCTION,
    ),
    ServiceDefinitionClass(                   # the contract, rendered on the Contract tab
        format=ServiceDefinitionFormatClass.OPENAPI,
        rawSpec=make_large_string(open_api_yaml),
        version="1.4.0",
        externalUrl="https://api.example.com/docs",
    ),
    SubTypesClass(typeNames=["REST_API"]),    # or GRAPHQL, GRPC, MCP
    DataPlatformInstanceClass(platform=make_data_platform_urn("openapi")),
    StatusClass(removed=False),
]
for aspect in aspects:
    graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=service_urn, aspect=aspect))
```

Runnable examples: `examples/services/register_rest_services.py` and
`examples/services/register_graphql_grpc_services.py`.

### MCP servers

An MCP server is a Service sub-typed `MCP`: its tools are `Api` entities (subtype `MCP_TOOL`) linked
via `ServiceProperties.apis`, and its `serviceDefinition` holds the `tools/list` contract as JSON
Schema. Connection details (transport, URL, headers, timeout) go on an `McpServerProperties` aspect.
See `examples/services/register_mcp_servers.py` for the full pattern.

## Register a repository

A **repository** (`urn:li:repository:<id>`) is the genesis node — it produces services. Register it
with its properties and source, then wire the `SourcedFrom` edge by setting `sourceRepository` on the
service it builds:

```python
from datahub.metadata.schema_classes import (
    RepositoryPropertiesClass,
    RepositorySourceClass,
)

repo_urn = "urn:li:repository:acme.payments"
for aspect in [
    RepositoryPropertiesClass(
        name="payments",
        description="Payments platform service repository.",
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
    SubTypesClass(typeNames=["GIT_REPOSITORY"]),
    DataPlatformInstanceClass(platform=make_data_platform_urn("github")),
    StatusClass(removed=False),
]:
    graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=repo_urn, aspect=aspect))

# Wire repo -> service without clobbering the service's other fields:
service_props = graph.get_aspect(service_urn, ServicePropertiesClass)
service_props.sourceRepository = repo_urn
graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=service_urn, aspect=service_props))
```

This nests the chain as **repository → service → endpoint** (endpoints hang off their service via
`ServiceComposesApi`, not off the repo directly). Runnable example:
`examples/repositories/register_repositories.py`. For applications and service health, see
`examples/services/register_app_and_health.py`.

## Related

- [Service Catalog](../../features/feature-guides/service-catalog.md) — the feature guide
- [Agent Registry tutorial](agent-registry.md) — the `Api` helper reference, and agents that invoke APIs
- [Applications](../tutorials/applications.md) — grouping assets by business purpose
