---
description: "Hierarchy Propagation Actions roll tags, glossary terms, owners, domains, and structured properties up an asset's physical and logical hierarchy — and back down onto the datasets a container holds."
---

# Hierarchy Propagation

<!-- Set Support Status -->

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

## Overview

The Hierarchy Propagation Actions aggregate an asset's metadata onto the groupings it
belongs to. Where the lineage-based `tag_propagation` / `term_propagation` actions
flow metadata _downstream along lineage_, these actions roll metadata **up the
hierarchy** (and, optionally, back **down** onto a container's datasets).

There is one action per payload, all sharing the same configuration and behaviour:

| Action (`type`)                             | Payload propagated    |
| ------------------------------------------- | --------------------- |
| `tag_hierarchy_propagation`                 | Tags                  |
| `term_hierarchy_propagation`                | Glossary terms        |
| `owner_hierarchy_propagation`               | Owners                |
| `domain_hierarchy_propagation`              | Domain                |
| `structured_property_hierarchy_propagation` | Structured properties |

### Capabilities

- **Roll up from any level.** The source of a roll-up can be a **column**
  (schemaField), a **dataset**, or a **container**:
  - a column rolls onto its parent dataset and that dataset's ancestors;
  - a dataset rolls onto its containers, data products, domain, and applications;
  - a container rolls onto its parent container(s) and its own logical groupings.
- **Roll back down (data sources).** With `contained_datasets` enabled, a value on a
  **container** is pushed **down** onto every dataset it contains, at any depth.
  Combining upward container roll-up with downward roll-up gives **set-union
  semantics** across a subtree: a value applied to any dataset is aggregated up to the
  shared container and shared back down onto its sibling datasets.
- **Physical and logical targets.** Each target is an independent toggle, so you can
  roll onto physical containers, logical data products / domains / applications, or any
  combination.
- **Safe, attributed writes.** Every propagated value is stamped with
  `MetadataAttribution` so it can be maintained and removed without touching values a
  user applied directly.
- **Scoping filters.** Restrict roll-up to specific tags, terms, or structured
  properties so only meaningful classifications spread.

### Supported Events

- `EntityChangeEvent_v1` — for the `TAG`, `GLOSSARY_TERM`, `OWNERSHIP`, `DOMAIN`, and
  `STRUCTURED_PROPERTY` categories, on `schemaField`, `dataset`, and `container`
  entities.

## Targets

Each action propagates the value onto any combination of these targets:

| Target                | Direction      | Description                                                            |
| --------------------- | -------------- | ---------------------------------------------------------------------- |
| `containers`          | physical, up   | Every browse-path container (schema, database, folder, …).             |
| `data_products`       | logical, up    | The data product(s) the asset belongs to.                              |
| `domain`              | logical, up    | The asset's domain(s).                                                 |
| `data_product_domain` | logical, up    | The domain(s) of the asset's data product(s).                          |
| `applications`        | logical, up    | The application(s) the asset belongs to.                               |
| `contained_datasets`  | physical, down | When the source is a container, the datasets it contains (all depths). |

## Action Quickstart

### Prerequisites

The account associated with your DataHub access token needs permission to edit the
relevant metadata (tags, terms, owners, domains, or structured properties) on the
target entities.

### Install the Plugin(s)

Each action ships as a plugin extra of `acryl-datahub-actions`:

```shell
pip install 'acryl-datahub-actions[term_hierarchy_propagation]'
# or: tag_hierarchy_propagation, owner_hierarchy_propagation,
#     domain_hierarchy_propagation, structured_property_hierarchy_propagation
```

### Configure the Action Config

Roll the `Confidential` glossary term up onto containers, data products, and the
domain, and share it back down onto sibling datasets:

```yml
name: "roll-up-classification"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
action:
  type: "term_hierarchy_propagation"
  config:
    target_terms:
      - "urn:li:glossaryTerm:Classification.Confidential"
    remove_on_delete: true
    targets:
      containers: true
      contained_datasets: true
      data_products: true
      domain: true
      data_product_domain: false
      applications: false
datahub:
  server: "http://localhost:8080"
```

<details>
  <summary>View All Configuration Options</summary>

| Field                         |      Required       | Default | Description                                                                                         |
| ----------------------------- | :-----------------: | :-----: | --------------------------------------------------------------------------------------------------- |
| `enabled`                     |         ❌          | `true`  | Whether roll-up is enabled for this action.                                                         |
| `remove_on_delete`            |         ❌          | `true`  | When the value is removed from a source, also remove the propagated copy once no member carries it. |
| `targets.containers`          |         ❌          | `true`  | _Physical, up_: roll onto every browse-path container.                                              |
| `targets.contained_datasets`  |         ❌          | `false` | _Physical, down_: push a container's value onto the datasets it contains.                           |
| `targets.data_products`       |         ❌          | `false` | _Logical, up_: roll onto the data product(s) the asset is in.                                       |
| `targets.domain`              |         ❌          | `false` | _Logical, up_: roll onto the asset's domain(s).                                                     |
| `targets.data_product_domain` |         ❌          | `false` | _Logical, up_: roll onto the domain(s) of the asset's data product(s).                              |
| `targets.applications`        |         ❌          | `false` | _Logical, up_: roll onto the application(s) the asset is in.                                        |
| `target_terms`                | ❌ (`term_*` only)  | _(all)_ | Restrict term roll-up to these terms (urns or names, plus terms related via `IsA`).                 |
| `target_tags`                 |  ❌ (`tag_*` only)  | _(all)_ | Restrict tag roll-up to these tags (urns or bare names).                                            |
| `properties`                  | ❌ (`structured_*`) | _(all)_ | Allowlist of structured properties to roll up (urns or qualified names). Recommended.               |
| `max_propagation_fanout`      |         ❌          | `1000`  | Upper bound on members/datasets processed per target.                                               |

</details>

## How It Works

- **Attribution.** Every propagated value carries a `MetadataAttribution` that records
  this action as the source, the origin entity that changed, the relationship as
  `HIERARCHY`, and the direction as `UP` or `DOWN`. Values a user applied directly are
  never overwritten.
- **Loop guard.** A propagated write re-fires as its own change event. It is never
  rolled **up** again (the value on the source is attributed to this action). With
  `contained_datasets` on, that same event still pushes **down**, which is how a value
  reaches sibling datasets. The cascade terminates because a downward target that
  already carries the value is left untouched, and datasets contain no further datasets.
- **Multi-level, single pass.** Because a column's browse path already lists every
  ancestor container, a single event reaches the parent dataset and all ancestor
  containers at once — there is no per-level hop for upward roll-up.
- **Safe removal.** When `remove_on_delete` is on and a value is removed from a source,
  the propagated copy is removed from an _upward_ target only once no other member of
  that target still carries it (checked with a search for containers/domains, or via the
  relationship graph for data products/applications). Removing a value from a
  **container** strips the rolled-down copy from every dataset below it — clearing a
  physical layer clears that layer's assets.

## Bidirectional (up-then-down) Propagation

Enabling both `containers` and `contained_datasets` makes a value applied to any single
dataset spread across all sibling datasets under the shared container:

1. A user adds `Confidential` to `db.schema.events`.
2. **Up:** the term rolls onto the `schema` and `db` containers.
3. **Down:** because the `schema` container now carries `Confidential`, it is pushed onto
   every other dataset in that schema (`db.schema.orders`, `db.schema.customers`, …).

This is powerful but broad. To keep it bounded:

- Scope with `target_terms` / `target_tags` / `properties` so only intended
  classifications spread.
- Disable one direction if you only want roll-up **or** roll-down.
- Note that combining upward `containers` with `contained_datasets` spreads a value
  across the **entire physical subtree** of the shared container.

## Troubleshooting

- **A value did not roll down.** `contained_datasets` only acts when the **source** of
  the event is a container. A value applied to a dataset first rolls _up_ to its
  containers; the downward pass then runs off the resulting container change.
- **Roll-down stopped early.** Downward fanout is bounded by `max_propagation_fanout`;
  raise it if a container holds more datasets than the limit.
- **Structured-property roll-down conflicts.** A structured property holds one
  assignment per property, so a downward target reflects the most recent origin's
  values rather than a union.
