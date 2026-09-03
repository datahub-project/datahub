---
title: Advanced Querying with SPARQL
description: "Query DataHub's relationship graph with standard SPARQL — compiled at query time into DataHub's native graph walk, with no triple store and no materialization."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Advanced Querying with SPARQL

<FeatureAvailability saasOnly stage="private-beta" />

The [traverse APIs](querying-your-ontology.md) express a walk as parameters: start here, follow
these types, this many hops. That covers most questions, but not ones that need a particular shape —
joins across several relationships, alternations, closures, or conditions on intermediate nodes.

For those, DataHub supports **SPARQL**.

Available in DataHub Cloud **v2.2.1** and later.

## What this is (and isn't)

DataHub uses SPARQL as a **query language over the relationship graph**. Queries run against your
live metadata, so there is nothing to sync or keep up to date.

DataHub is not a full RDF database. It does not store triples, and only a subset of SPARQL is
supported — see [Supported subset](#supported-subset) below. If you need more than that,
[export your graph](#exporting-your-graph) and load it into a dedicated triple store.

## Authorization

Callers need `RELATIONSHIP` `READ` authorization. View-based access control is applied as the walk
produces bindings — an edge resolves only between endpoints the caller may view, and a path only
when every entity along it is viewable — so no filter or expression above the leaf can observe an
entity the caller is not allowed to see.

## Making a query

The endpoint is `/openapi/v3/sparql`, and it accepts the three standard SPARQL protocol forms:

```bash
# GET with ?query=
curl -H "Authorization: Bearer $DATAHUB_TOKEN" -G "$DATAHUB_GMS_URL/openapi/v3/sparql" \
  --data-urlencode 'query=SELECT ?t WHERE { ?t <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr> }'

# POST with Content-Type: application/sparql-query
curl -H "Authorization: Bearer $DATAHUB_TOKEN" -X POST "$DATAHUB_GMS_URL/openapi/v3/sparql" \
  -H 'Content-Type: application/sparql-query' \
  --data 'SELECT ?t WHERE { ?t <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr> }'

# POST as a form parameter
curl -H "Authorization: Bearer $DATAHUB_TOKEN" -X POST "$DATAHUB_GMS_URL/openapi/v3/sparql" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'query=SELECT ?t WHERE { ... }'
```

### The vocabulary

DataHub URNs **are** the IRIs. Entities are their URNs, and predicates are relationship-type URNs:

```sparql
<urn:li:glossaryTerm:customer>            # a glossary term
<urn:li:relationshipType:governedBy>      # a custom relationship type
```

Built-in relationships have relationship-type URNs too — use the
[schema export](querying-your-ontology.md#discovering-what-you-can-traverse) to discover them.

### Query forms and output

| Form          | Returns      | Negotiated formats                                         |
| ------------- | ------------ | ---------------------------------------------------------- |
| **SELECT**    | Bindings     | `application/sparql-results+json` (default), XML, CSV, TSV |
| **ASK**       | A boolean    | Same as SELECT                                             |
| **CONSTRUCT** | An RDF graph | Turtle (default), JSON-LD, N-Triples, RDF/XML              |

The output family follows the query form, and the exact serialization is negotiated from the
`Accept` header.

## Examples

Everything governed by GDPR:

```sparql
SELECT ?term WHERE {
  ?term <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr>
}
```

Walk a transitive closure — every concept beneath _Customer_, at any depth. Here
`rdfs_subClassOf` is the well-known relationship type that the RDF ingestion source writes
`rdfs:subClassOf` axioms to; any transitive relationship type works the same way:

```sparql
SELECT ?concept WHERE {
  ?concept <urn:li:relationshipType:rdfs_subClassOf>+ <urn:li:glossaryTerm:customer>
}
```

A join across two relationships — assets governed by a term that is itself derived from a regulated
concept:

```sparql
SELECT ?asset WHERE {
  ?mid   <urn:li:relationshipType:governedBy>  <urn:li:glossaryTerm:gdpr> .
  ?asset <urn:li:relationshipType:derivedFrom>+ ?mid
}
```

An alternation over several relationship types:

```sparql
SELECT ?asset WHERE {
  ?asset (<urn:li:relationshipType:governedBy>|<urn:li:relationshipType:derivedFrom>)+ <urn:li:glossaryTerm:pii>
}
```

Aggregate, then emit a graph rather than bindings:

```sparql
SELECT (COUNT(?asset) AS ?count) WHERE {
  ?asset <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr>
}
```

```sparql
CONSTRUCT { ?asset <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr> }
WHERE     { ?asset <urn:li:relationshipType:governedBy> <urn:li:glossaryTerm:gdpr> }
```

## Entailment

By default, queries run under **simple entailment**: a single-hop pattern stays single-hop, and
nothing is inferred. Standard-vocabulary mappings and transitivity are _opt in_, so a query never
silently returns more than it asked for.

To enable RDFS-style inference over your declared relationship semantics, send the
`X-DataHub-SPARQL-Entailment: rdfs` header (or the equivalent `?entailment=rdfs` query parameter).

Under the RDFS regime:

- A relationship type's **equivalent properties** resolve: querying a standard predicate such as
  `skos:broader` reaches edges stored under any relationship type that declared it as an equivalent.
- A **transitive** relationship answers a single-hop question over its whole chain.
- Class-hierarchy anchors expand over `rdfs:subClassOf` and `owl:sameAs` / `owl:equivalentClass`
  edges.

Declaring equivalents and transitivity is covered in
[Adding Custom Relationships](custom-relationships.md#relationship-semantics).

### Querying by an equivalent property

Say you have a custom relationship type `broaderThan` that declares `skos:broader` as an equivalent
property. Edges are still stored under `urn:li:relationshipType:broaderThan`, and querying that URN
works with or without entailment:

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
     -G "$DATAHUB_GMS_URL/openapi/v3/sparql" \
     --data-urlencode 'query=SELECT ?broader WHERE {
       <urn:li:glossaryTerm:individualCustomer> <urn:li:relationshipType:broaderThan> ?broader
     }'
```

```json
{
  "head": { "vars": ["broader"] },
  "results": {
    "bindings": [
      { "broader": { "type": "uri", "value": "urn:li:glossaryTerm:customer" } }
    ]
  }
}
```

With the entailment header, you can ask the same question in standard SKOS instead — without
knowing what the relationship is called in DataHub:

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
     -H 'X-DataHub-SPARQL-Entailment: rdfs' \
     -G "$DATAHUB_GMS_URL/openapi/v3/sparql" \
     --data-urlencode 'query=PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
     SELECT ?broader WHERE {
       <urn:li:glossaryTerm:individualCustomer> skos:broader ?broader
     }'
```

This returns the same binding. Drop the header and it returns nothing:

```json
{ "head": { "vars": ["broader"] }, "results": { "bindings": [] } }
```

That empty result is the point of the opt-in — the mapping from `skos:broader` to your relationship
type only applies when you ask for it, so a query never quietly picks up edges you did not name.

This is what makes standards-based tooling work against your own vocabulary: a client that knows
SKOS can query your graph without knowing that you called the relationship `broaderThan`.

## Supported subset

The compiler answers what it can map onto a bounded graph walk, and **refuses** — with a clear
`501`, never a silently partial answer — what it cannot.

**Supported:** `SELECT`, `ASK`, `CONSTRUCT`; basic graph patterns; `FILTER`, `BIND`, `VALUES`,
`OPTIONAL`, `UNION`; `DISTINCT`, `ORDER BY`, `LIMIT`/`OFFSET`; aggregates; property paths with `+`,
`*`, `?`, inverse (`^`), alternation over simple predicates (`(a|b)+`), and bounded repetition
(`p{0,m}`, `p{1,m}`).

**Not supported:**

| Not supported                                          | What to do instead                                                 |
| ------------------------------------------------------ | ------------------------------------------------------------------ |
| Sequence paths (`p1/p2`)                               | Write explicit triple patterns with a named intermediate variable. |
| Variable predicates in an unanchored position          | Bind the predicate to an IRI.                                      |
| Unanchored patterns and unanchored transitive closures | Bind one endpoint to an IRI, or bind it via another pattern.       |
| Alternation mixing forward and inverse (`a\|^b`)       | Split into a `UNION`.                                              |
| Bounded repetition with a minimum above 1 (`p{2,5}`)   | Use `p{0,m}` or `p{1,m}`.                                          |
| Named graphs (`GRAPH`), quads, `SERVICE` federation    | Not applicable — there is one graph.                               |
| `DESCRIBE`, updates (`INSERT`/`DELETE`)                | Use the entity and relationship APIs to read and write metadata.   |

The **anchoring** rule is the one to internalize: every pattern needs a bound IRI endpoint, or a
variable bound by another pattern. `?s ?p ?o` over the whole graph is not a query DataHub can answer
efficiently, so it is refused rather than attempted.

### Error codes

| Code  | Meaning                                                                           |
| ----- | --------------------------------------------------------------------------------- |
| `400` | The query is not valid SPARQL.                                                    |
| `501` | Valid SPARQL, but outside the supported subset. The message says which construct. |

Queries that would exceed the engine's bounds — an unanchored scan over too many edges, a join whose
intermediate set explodes, a class-hierarchy expansion that is too large, or a walk that hit the
traversal timeout — are also rejected with an explanatory message, rather than returning a partial
result you might mistake for the whole graph.

## When to use what

| Question shape                                                  | Reach for                                                                      |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| "What is connected to X over these relationships?"              | [traverse API / `searchAcrossRelationships`](querying-your-ontology.md)        |
| "Let an assistant answer an ontology question"                  | [MCP `get_relationships`](querying-your-ontology.md#querying-from-an-ai-agent) |
| Joins, alternations, closures, conditions on intermediate nodes | SPARQL                                                                         |
| Integrating with existing RDF/SPARQL tooling                    | SPARQL                                                                         |

## Exporting your graph

Export happens in two parts: the relationship types, and the edges between entities.

**1. The relationship types.** This is the same schema export the traverse APIs use for discovery.
It returns every relationship type as an RDF ontology:

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
  "$DATAHUB_GMS_URL/openapi/v3/relationship/schema?format=n-triples" > schema.nt
```

**2. The edges.** A `CONSTRUCT` with a variable predicate exports every relationship type:

```bash
curl -H "Authorization: Bearer $DATAHUB_TOKEN" \
     -H "Accept: application/n-triples" \
     -G "$DATAHUB_GMS_URL/openapi/v3/sparql" \
     --data-urlencode 'query=CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }' > edges.nt
```

Concatenating `schema.nt` and `edges.nt` gives you a self-contained RDF graph to load into the tool
of your choice.

### If the export is too large

Any single scan is capped at **100,000 edges**. Over that, the query is refused with a `501` rather
than silently truncated:

```json
{
  "error": "scan exceeds the supported result cap of 100000; add a bound endpoint or narrow the query."
}
```

The cap is per query, not per export, so a graph larger than 100,000 edges has to come out in
pieces. Narrow along whichever axis splits your graph most evenly:

```bash
# One relationship type at a time
--data-urlencode 'query=CONSTRUCT { ?s <urn:li:relationshipType:governedBy> ?o }
                        WHERE     { ?s <urn:li:relationshipType:governedBy> ?o }'

# Everything attached to one entity, across all relationship types
--data-urlencode 'query=CONSTRUCT { <urn:li:glossaryTerm:customer> ?p ?o }
                        WHERE     { <urn:li:glossaryTerm:customer> ?p ?o }'
```

Append each result to the same file. A relationship type that is on its own over the cap has to be
split by anchoring, since there is no paging on this endpoint.

## Enabling this feature

Contact your DataHub representative to have this enabled for your instance.

For self-managed deployments, set the flag on GMS and restart:

```bash
SPARQL_API_ENABLED=true
```

Or via Helm:

```yaml
datahub-gms:
  sparqlApiEnabled: true
```
