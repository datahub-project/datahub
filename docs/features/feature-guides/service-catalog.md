---
description: "The DataHub Service Catalog makes software systems — repositories, services, APIs, and applications — first-class, typed, and governed metadata entities, right beside your data."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';
import ProductTour from '@site/src/components/ProductTour';

# About the DataHub Service Catalog

<FeatureAvailability saasOnly />

Introduced in DataHub Cloud v2.1, the **Service Catalog** brings the software layer of your stack
into DataHub as typed, governed entities — right beside the data those systems produce. Repositories
build services, services expose APIs, applications call those APIs, and APIs produce and consume
datasets. A service is modeled as what it is — a REST, GraphQL, gRPC, or MCP service — owned,
versioned, health-checked, and connected to everything downstream.

This closes a long-standing gap: the engineering view of your systems and the business-and-data
view now live in **one connected graph**, so you can trace an outage, a schema change, or a data
dependency from source code all the way to a dashboard — or to an AI agent.

<ProductTour name="service-catalog" title="Service Catalog" />

## Concepts

The Service Catalog introduces four entity types that model the software-to-data lifecycle:

| Entity          | What it represents                                                                                                    |
| --------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Repository**  | A source-code repository (GitHub, GitLab, internal SCM). The genesis node — it builds services, APIs, and apps.       |
| **Service**     | A running service — REST, GraphQL, gRPC, or an **MCP server** — with connection details and a lifecycle stage.        |
| **API**         | A single named, callable operation (an endpoint, a gRPC method, a GraphQL field, an MCP tool) with a typed signature. |
| **Application** | A business-level system that calls APIs and produces data — the bridge between the engineering and data views.        |

These connect into a single lineage spine, and everything downstream — datasets, dashboards, and
AI agents — hangs off the same graph:

```text
repository ──builds──▶ service ──exposes──▶ api ──produces/consumes──▶ dataset ──▶ dashboard
                                             ▲
                        application ──calls──┘   aiAgent ──invokes──┘
```

## Setup, Prerequisites, and Permissions

Services, APIs, repositories, and applications are populated through ingestion or emitted directly
via the SDK/API, the same way you catalog any other asset — see the
[API tutorial](../../api/tutorials/service-catalog.md), including ingesting a REST service straight
from its OpenAPI spec. To browse and manage them, a user needs standard entity privileges:

- **View Entity Page** to discover and open service, API, and repository profiles.
- **Edit Entity** (and the relevant sub-privileges — ownership, tags, glossary terms, documentation)
  to enrich them.

Reach out to your DataHub representative to enable the Service Catalog for your organization.

## Using the Service Catalog

### Discovering services

Your services live in DataHub, right beside the data. A single search, filtered to services, brings
everything back as exactly what it is — REST, GraphQL, gRPC, even MCP servers — each one typed,
owned, and health-checked. Use search filters for **sub-type** (REST/GraphQL/gRPC/MCP), **domain**,
**owner**, and **health** to narrow to the systems you care about.

### Services

Open a service — for example an **Order Entry API** — to see its Service profile. On one screen you
get the service's **endpoints**, its **contract**, its **production lifecycle stage**, and the
**team that owns it** — everything you need to trust and depend on a service.

MCP servers are catalogued here too, as Services sub-typed **MCP**, complete with real connection
details. The server, the tools (APIs) it exposes, and the agents that call them all live in the same
graph.

:::note Service (catalog entity) vs. the DataHub MCP Server
This page is about cataloging **external** services and MCP servers as metadata entities. It is
distinct from the [DataHub MCP Server](mcp.md), which exposes _DataHub itself_ as an MCP server for
AI agents to query your catalog.
:::

### APIs and endpoints

Every operation a service exposes is modeled as its own entity. An endpoint like **Place Order**
carries a **typed signature** — required inputs and typed outputs — rendered with the same rich
schema views your teams already use for datasets. The result is strongly-typed, discoverable
contracts for every endpoint you run, whether it's a REST path, a gRPC method, a GraphQL field, or
an MCP tool.

### The software-to-data chain

Modeling these as distinct entities makes the chain between them explicit: the **payments
repository** builds the **Order Entry service**, and the service **exposes** its endpoints —
repository, to service, to endpoint — nested the way your systems really run. Each hop is a typed
graph edge, rather than opaque strings buried in a description.

### Applications

Zoom out to the business. An **Application** — say **Checkout**, in the **Commerce** domain — is
owned, documented, and connected to the APIs it calls and the data it produces. The business view
and the engineering view become one and the same graph.

:::note Relationship to the Applications feature
Service Catalog reuses the same [Application](applications.md) entity used to group assets by
business purpose. In the Service Catalog, Applications additionally carry their **API call** and
**data production** relationships, so an Application's profile shows the services it depends on.
:::

### Contracts

A service's contract isn't a link to a wiki — it's the full specification, versioned right in the
catalog. For a REST service that's the complete **OpenAPI** document (every path, every schema); for
an MCP server it's the live **tool list**, with inputs and outputs in JSON Schema. One authoritative
spec for your developers, your partners, and your AI agents — kept in sync and versioned alongside
the service.

### Impact analysis

Because services, APIs, applications, and agents share one graph, impact analysis spans all of them.
You can ask which assets depend on the **Place Order** endpoint and get the answer directly — for
example, the **Checkout** application and the **FX Risk Scoring agent** — across services, apps, and
AI agents, from the same lineage view you already use for data.

### Incidents and health

Service health flows through the same [incident](/docs/incidents/incidents.md) system as the rest of the
catalog. If the **Auction Service** has an active, high-priority incident, its badge reads
_unhealthy_ the moment you look. Open the **Incidents** tab and the whole story is there — what
broke, how severe it is, and who's on it.

### OAuth Authorization Servers

When DataHub or an agent needs to _call_ an external API, an **OAuth Authorization Server** entity
models where tokens come from. Cataloging the authorization server keeps the credential-issuance
step visible and governed, rather than hidden in configuration.

## Additional Resources

### API Tutorials

- [Registering Services, APIs, and Repositories](../../api/tutorials/service-catalog.md) — ingest an
  OpenAPI spec, or register services, APIs, and repositories with the SDK

## FAQ and Troubleshooting

**How is a Service different from a Dataset?**

A Dataset models tabular data — it has columns, schemas, queries, and statistics. A Service models a
running piece of software: endpoints, a contract, a lifecycle stage, and connection details. Before
the Service Catalog, teams often shoehorned services into schemaless datasets; modeling them as
Services gives them the right profile, typed API signatures, and real graph edges.

**Do I have to catalog my repositories to use Services?**

No. Each entity is independently useful. Repositories add the genesis node of the chain (repo →
service → API), but you can catalog services and APIs on their own and add repositories later.

**Can an AI agent depend on an API?**

Yes — that's a core reason the API is a shared entity. See the [Agent Registry](agent-registry.md):
an agent's tools are API entities, so "which agents call this endpoint" is answered by the same
impact-analysis view.

### Related Features

- [Agent Registry](agent-registry.md) — AI agents, skills, and the APIs they invoke
- [Applications](applications.md) — grouping assets by business purpose
- [DataHub MCP Server](mcp.md) — DataHub itself as an MCP server for AI agents
- [Lineage](lineage.md) — end-to-end dependency tracing and impact analysis
- [Incidents](/docs/incidents/incidents.md) — health and incident management across the catalog
