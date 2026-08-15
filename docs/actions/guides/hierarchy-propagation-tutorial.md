# Tutorial: Rolling Classification Up and Back Down

In this guide we will stand up a [Hierarchy Propagation Action](../actions/hierarchy_propagation.md)
that keeps a sensitivity classification consistent across a physical hierarchy. We
will roll the `Confidential` glossary term **up** from a table onto its schema and
database containers, and then share it **back down** onto the other tables in that
schema.

## Scenario

A steward tags one table, `db.schema.events`, with `Classification.Confidential`. We
want DataHub to:

1. mark the parent `schema` and `db` containers as confidential (roll **up**), and
2. mark every other table in that schema confidential too (roll **back down**),

so the whole subtree carries a consistent classification without manual tagging.

## Step 1: Install the plugin

The term action ships as a plugin extra of `acryl-datahub-actions`:

```shell
pip install 'acryl-datahub-actions[term_hierarchy_propagation]'
```

## Step 2: Write the action config

Create `roll-up-classification.yaml`. `containers: true` performs the upward roll-up
and `contained_datasets: true` performs the downward share. We scope roll-up to a
single term so only classification spreads.

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
datahub:
  server: ${DATAHUB_GMS_URL:-http://localhost:8080}
  token: ${DATAHUB_GMS_TOKEN:-}
```

## Step 3: Run the action

```shell
datahub actions -c roll-up-classification.yaml
```

The action now listens for glossary-term changes.

## Step 4: Watch it propagate

Add `Classification.Confidential` to `db.schema.events` (in the UI or via the SDK). The
action reacts in two passes:

1. **Up.** The term is written onto the `schema` and `db` containers, each stamped with
   a `HIERARCHY` / `UP` attribution whose origin is `db.schema.events`.
2. **Down.** The `schema` container's own change then pushes the term onto the other
   tables in that schema — `db.schema.orders`, `db.schema.customers`, and so on — each
   stamped with a `HIERARCHY` / `DOWN` attribution.

Propagated values appear with an attribution marker so it is clear they were applied by
the automation rather than a person, and the origin is preserved.

## Step 5: Verify removal

Remove `Confidential` from the `schema` container. Because removal from a physical layer
clears that layer's assets, the action strips its rolled-down copy from every table
under that schema. (Values a user applied by hand are left untouched.)

## Choosing a direction

- **Up only** — set `containers: true`, `contained_datasets: false`. Classifications
  bubble up to containers for summary/rollup views, but sibling tables are untouched.
- **Down only** — set `containers: false`, `contained_datasets: true`. Tag a container
  once and every dataset it holds inherits the value; datasets do not roll back up.
- **Both** — the union behaviour above. Best for keeping a whole schema/database
  consistently classified. Keep it scoped with `target_terms` so it stays bounded.

## Beyond terms

The same pattern works for tags, owners, domains, and structured properties — swap the
`type` to `tag_hierarchy_propagation`, `owner_hierarchy_propagation`,
`domain_hierarchy_propagation`, or `structured_property_hierarchy_propagation`, and use
the matching scoping filter (`target_tags`, `properties`). You can also add logical
targets (`data_products`, `domain`, `applications`) alongside the physical ones. See the
[Hierarchy Propagation reference](../actions/hierarchy_propagation.md) for the full list.
