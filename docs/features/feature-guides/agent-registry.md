---
description: "The DataHub Agent Registry catalogs your AI agents, skills, and the tools they invoke as first-class, governed, versioned metadata entities — with lineage to the data they touch."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';
import ProductTour from '@site/src/components/ProductTour';

# About the DataHub Agent Registry

<FeatureAvailability saasOnly />

Your board just asked: _how many AI agents are running, what data are they touching, who owns them,
and can we trust them?_ Most teams can't answer today — their agents are shadow AI, invisible to
governance. The **Agent Registry** closes that gap by cataloging AI agents, the skills they adopt,
and the tools they invoke as **first-class, governed, versioned metadata entities**, sitting in the
same lineage graph as the data they consume.

Because agents live in the graph alongside your datasets, the governance you already run on data —
ownership, classification, lineage, incidents, change history — extends to your AI layer for free.

:::note Agent Registry vs. Agents
This guide is about **cataloging AI agents as metadata** — making them discoverable, owned, and
governed. If you're looking to **build** AI agents that autonomously execute tasks on a schedule or
in response to events, see the [Agents](agents.md) guide instead. The two are complementary: agents
you build can be catalogued here, and agents catalogued here can be governed like any asset.
:::

<ProductTour name="agent-registry" title="Agent Registry" />

## Concepts

The Agent Registry introduces two headline entity types and reuses the Service Catalog's API and
Service entities for an agent's tools:

| Entity         | What it represents                                                                                                          |
| -------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **AI Agent**   | An AI agent with instructions, a scope, and dependencies. Can be native (DataHub-managed), system, or external (cataloged). |
| **Skill**      | A reusable capability bundle (prompts + tools + domain expertise), defined in git, that agents adopt.                       |
| **Tool**       | A capability an agent invokes — modeled as an [API](service-catalog.md#apis-and-endpoints) entity with a typed signature.   |
| **MCP Server** | The server that exposes an agent's tools — modeled as a [Service](service-catalog.md#services) entity, sub-typed MCP.       |

An agent's dependencies and the data it reads all connect into one lineage graph:

```text
repository ─▶ service (MCP) ─▶ api (tool) ◀─invokes─ aiAgent ─adopts─▶ skill ─requires─▶ api (tool)
                                                        │
                                                        └─reads──▶ dataset (upstream lineage)
```

## Setup, Prerequisites, and Permissions

AI agents, skills, and tools are populated through ingestion or emitted directly via the SDK/API,
like any other asset. To browse and manage them, a user needs standard entity privileges:

- **View Entity Page** to discover and open agent, skill, and tool profiles.
- **Edit Entity** (and the relevant sub-privileges) to enrich them with ownership, tags, glossary
  terms, and documentation.

Reach out to your DataHub representative to enable the Agent Registry for your organization.

## Using the Agent Registry

### The agent profile

Open an agent — for example the **FX Risk Scoring Agent** — and its profile is a first-class entity
page. It tells you what the agent does, the skill and tools it uses, the data it reads across
BigQuery, Snowflake, and dbt, and how reliable it is (eval scores, invocation volume, latency). It's
owned by a real team — the Quant Risk Team. This is the opposite of shadow AI: an agent you can find,
attribute, and trust.

### Dependencies

An agent's dependencies are explicit and clickable — the **skill** it adopts, the **tools** it
invokes, and the **model** it runs on. Because each is a first-class entity, a new team can discover
and reuse them instead of rebuilding an agent from scratch.

### Skills

Skills are first-class citizens too. Open a skill — the **FX Risk Scoring skill** — and you get its
definition, a source-of-truth spec, and the exact tools it requires. A skill is one reusable, vetted
capability that teams adopt rather than reimplement.

### Tools, signatures, and contracts

Every tool an agent invokes is a first-class [API](service-catalog.md#apis-and-endpoints) entity with
a **typed signature** — input parameters and an output shape — rendered with the same schema
components your datasets use. The MCP servers that expose these tools are catalogued as
[Service](service-catalog.md#services) entities, and each server carries its full **contract** in the
graph — the live tool list, with inputs and outputs in JSON Schema, versioned in place. (A REST API
tool renders its OpenAPI spec the same way.) See the [Service Catalog](service-catalog.md) for the
full treatment of APIs, services, and contracts.

### Lineage to your data

Here's the payoff: the agent sits in your lineage graph, **downstream of the very BigQuery,
Snowflake, and dbt tables it consumes**. That means you can answer _"what AI touches my regulated
data?"_ from the same [lineage](lineage.md) graph you already use for impact analysis — no separate
tool, no separate inventory.

### Versioning and change history

Agents are versioned. Versions `1.0` through `2.0` are grouped in a **version set** with the latest
clearly flagged, because different versions often run in production side by side.

And every change is on the **timeline** — tag changes, ownership updates, eval-score bumps, new
documentation, and the version milestones themselves. It's a full, trustworthy audit trail for your
AI — just like data quality history, but for agents.

### Governance that rides on lineage

The deepest payoff is that governance flows across this same lineage, automatically. Consider a
[Glossary Term Propagation](/docs/automations/glossary-term-propagation.md) automation, switched on.
Someone classifies a source table — `fx-positions` — as **Highly Confidential**. Because the FX Risk
Scoring Agent consumes that table, DataHub propagates the classification straight onto the agent: it
now carries the same **Highly Confidential** label, inherited through lineage. Its health turns red,
and an [incident](/docs/incidents/incidents.md) is opened for review.

So the moment sensitive data reaches an AI system, you see it — and you can act.

## FAQ and Troubleshooting

**How is this different from the Agents feature?**

The [Agents](agents.md) feature is about _building_ AI agents that run tasks autonomously. The Agent
Registry is about _cataloging_ AI agents (whether built in DataHub or elsewhere) as governed metadata
— so they're discoverable, owned, versioned, and connected to the data they touch.

**What does an agent's "tool" map to in the metadata model?**

A tool is an [API](service-catalog.md#apis-and-endpoints) entity — the same entity the Service Catalog
uses for REST endpoints, gRPC methods, and GraphQL fields. That's what lets "which agents invoke this
endpoint" be answered by ordinary impact analysis.

**Can an external agent (built outside DataHub) be catalogued?**

Yes. Agents can be native (DataHub-managed), system (bootstrapped), or **external** (catalogued from
another platform) — the profile, lineage, versioning, and governance work the same way regardless.

### Related Features

- [Service Catalog](service-catalog.md) — services, APIs, repositories, and applications
- [Agents](agents.md) — building AI agents that autonomously execute tasks
- [Lineage](lineage.md) — end-to-end dependency tracing and impact analysis
- [Glossary Term Propagation](/docs/automations/glossary-term-propagation.md) — automated
  classification across lineage
