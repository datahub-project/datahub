

# Querying Your Ontology

> **Availability:** DataHub Cloud only — _Public Beta_

This page covers reading the relationship graph from code, and from an AI agent.

All the surfaces here run the same **multi-hop relationship walk**: start at an entity, follow a
named set of relationship types, up to a maximum number of hops, and return everything reached.

Available in DataHub Cloud **v2.2.1** and later; no feature flag is required.

## Discovering what you can traverse

There is no "all relationship types" wildcard — you name the types you want to walk. To find out
which types exist on your instance, export the relationship-graph **schema**. It returns every
relationship type with the entity types it connects, its direction semantics, and whether it is
built in (`NATIVE`) or user-defined (`CUSTOM`):

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
  "$DATAHUB_GMS_URL/openapi/v3/relationship/schema?entityType=glossaryTerm"
```

```turtle
PREFIX dh:     <urn:li:ontology:>
PREFIX et:     <urn:li:entityType:datahub.>
PREFIX rel:    <urn:li:relationshipType:>
PREFIX schema: <https://schema.org/>

rel:governedBy  rdf:type       rdf:Property ;
        rdfs:label             "Governed By" ;
        rdfs:comment           "The regulation or policy term that governs this concept." ;
        schema:domainIncludes  et:glossaryTerm ;
        schema:rangeIncludes   et:glossaryTerm ;
        dh:origin              "CUSTOM" ;
        dh:reverseDisplayName  "Governs" .
```

The subjects it returns are exactly the values you pass as relationship types below. Add
`?format=json-ld` (or `n-triples`, `rdf-xml`) if Turtle is not what you want.

## Traversing from the REST API

`GET /openapi/v3/relationship/traverse` walks the graph and returns the entities it reached:

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
  "$DATAHUB_GMS_URL/openapi/v3/relationship/traverse\
?urn=urn:li:glossaryTerm:customer\
&relationshipTypes=IsA\
&relationshipTypes=urn:li:relationshipType:governedBy\
&direction=UNDIRECTED\
&maxHops=2"
```

```json
{
  "count": 2,
  "total": 2,
  "partial": false,
  "relationships": [
    {
      "relationshipType": "urn:li:relationshipType:governedBy",
      "entityType": "glossaryTerm",
      "urn": "urn:li:glossaryTerm:gdpr",
      "degree": 1,
      "degrees": [1],
      "directionMode": "DIRECTED",
      "transitive": false,
      "reverseDisplayName": "Governs"
    },
    {
      "relationshipType": "IsA",
      "entityType": "glossaryTerm",
      "urn": "urn:li:glossaryTerm:businessCustomer",
      "degree": 1,
      "degrees": [1],
      "directionMode": "DIRECTED",
      "transitive": false
    }
  ]
}
```

### Parameters

| Parameter            | Meaning                                                                                                                                                                                                                                                                    |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `urn`                | The entity to start from. Required.                                                                                                                                                                                                                                        |
| `relationshipTypes`  | Repeatable. Built-in names (`IsA`, `HasA`, `DownstreamOf`, …) and/or `urn:li:relationshipType:*` URNs, traversed uniformly. A `urn:li:structuredProperty:*` URN is accepted too and resolves to the relationship type it points at. Required unless `includeLineage=true`. |
| `direction`          | `OUTGOING` (default), `INCOMING`, or `UNDIRECTED`. Edges point from the entity holding the metadata to the entity it references, so `INCOMING` from X returns entities whose edges point at X. Symmetric relationship types are walked both ways regardless.               |
| `maxHops`            | Traversal depth, 1–20. Defaults to 1.                                                                                                                                                                                                                                      |
| `entityTypes`        | Repeatable. Constrains the entity types visited **at every hop**, not just the final results.                                                                                                                                                                              |
| `includeLineage`     | Also walk the full native lineage edge set in the same call, without enumerating lineage relationship types.                                                                                                                                                               |
| `lineageDirection`   | `UPSTREAM`, `DOWNSTREAM`, or `BOTH` (default). Only meaningful with `includeLineage=true`.                                                                                                                                                                                 |
| `resolveEquivalents` | Expand a requested standard-vocabulary predicate (e.g. `skos:broader`) to the relationship types that declare it as an equivalent.                                                                                                                                         |

`degree` is the hop distance from the start entity; `degrees` lists every depth at which the entity
was reached. `partial: true` means the walk hit a traversal limit or timed out — narrow the query.

## Traversing from GraphQL

The `searchAcrossRelationships` query is the same walk with hydrated entities, plus paging. This is
what the ontology graph UI itself calls:

```graphql
query {
  searchAcrossRelationships(
    input: {
      urn: "urn:li:glossaryTerm:customer"
      relationshipTypes: ["IsA", "HasA", "urn:li:relationshipType:governedBy"]
      direction: UNDIRECTED
      maxHops: 2
      count: 50
    }
  ) {
    total
    isPartial
    relationships {
      relationshipType
      degree
      directionMode
      transitive
      reverseDisplayName
      entity {
        urn
        ... on GlossaryTerm {
          properties {
            name
            description
          }
        }
      }
    }
  }
}
```

Page with `start` and `count`. `isPartial` carries the same meaning as `partial` above.

:::note Lineage is a separate axis
`relationshipTypes` and `direction` apply only to the named-relationship walk. When
`includeLineage` is true, the full native lineage edge set is traversed as well — controlled solely
by `lineageDirection` — and the two walks are unioned. For pure data-flow questions ("what feeds
this table?"), use the lineage APIs instead; they are built for it.
:::

## Querying from an AI agent

DataHub's MCP server exposes the same walk as the **`get_relationships`** tool, so an assistant can
answer ontology questions directly. Paired with **`get_metadata_graph`** — the MCP equivalent of the
schema export above — an agent can discover what relationship types exist and then traverse them,
without any of them being hardcoded.

A useful pattern to give an agent:

> Call `get_metadata_graph` first to see which relationship types connect which entity types on this
> instance, then call `get_relationships` with the ones relevant to the question.

`get_relationships` mirrors the REST parameters (`urn`, `relationship_types`, `direction`,
`max_hops`, `entity_types`, `include_lineage`, `lineage_direction`), and adds a `fields` allowlist
so the agent only pulls the entity fields it needs — keeping more results inside its token budget.
Results are paged: pass the response's `nextOffset` back as `offset` and stop when `hasMore` is
false.

See [DataHub MCP Server](../mcp.md) for connecting an agent to your instance.

## Practical notes

- **Name your types.** There is no wildcard. A walk over the wrong types returns nothing, and it
  looks identical to a walk over an entity with no relationships.
- **Be careful with `entityTypes` on multi-hop walks.** The filter applies at every hop, so a path
  that passes through an excluded entity type is cut short. Prefer omitting it when `maxHops > 1`.
- **Watch for `isPartial` / `partial`.** A capped walk is not an empty one. Narrow the relationship
  types or entity types, or reduce the hop count, rather than trusting a truncated answer.
- **Authorization applies.** A walk only returns entities the caller is allowed to view.

If you need to express the query itself in a standard graph query language rather than as walk
parameters, see [Advanced Querying with SPARQL](sparql-api.md).
