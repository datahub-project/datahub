---
title: What is an Ontology?
description: "A business ontology extends your Business Glossary with typed, machine-readable relationships between terms, turning a flat list of definitions into a connected model of your business."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# What is an Ontology?

<FeatureAvailability />

A **Business Glossary** answers _"what does this word mean?"_. A **business ontology** answers the
next question: _"how do these words relate to each other?"_

An ontology is the Business Glossary plus a layer of **typed, directional relationships** between
terms. Instead of a flat list of definitions, you get a connected model that both people and
machines can read: _Individual Customer_ **is a** _Customer_, _Customer_ **has a** _Customer ID_,
_Client_ is a **synonym of** _Customer_, and _Customer_ is **governed by** _GDPR_.

## Why relationships matter

A definition on its own is only useful to whoever reads it. A relationship is information the rest
of DataHub can use:

- **Discovery.** Someone searching for _Customer_ should also find assets tagged _Individual
  Customer_ and _Business Customer_ — because the ontology says those are kinds of Customer.
- **Governance.** If _Customer_ is governed by GDPR, every concept beneath _Customer_ inherits that
  obligation. The relationship is what lets you propagate the policy instead of restating it on
  every term.
- **Impact analysis.** Retiring or redefining a term is only safe once you can see everything that
  depends on it — the terms that inherit from it, contain it, or translate it.
- **AI and agents.** An LLM answering _"which tables hold revenue data?"_ does far better when it
  can walk _Revenue → Turnover (synonym) → MRR/ARR (kinds of)_ than when it can only string-match
  the word "revenue".

## How DataHub models an ontology

DataHub does not introduce a separate "ontology" entity. Your ontology **is** your glossary, viewed
through its relationships:

| Piece                      | What it is                                                                                                                                                |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Glossary Term**          | A concept — the node in your ontology.                                                                                                                    |
| **Glossary Term Group**    | A folder for organizing terms. Hierarchy for humans, not a semantic relationship.                                                                         |
| **Built-in relationships** | The relationship vocabulary DataHub ships with — inheritance, containment, synonyms, antonyms, translations, and valid values.                            |
| **Custom relationships**   | Relationship types you define yourself, for vocabulary your business needs that the built-ins do not cover — _governed by_, _derived from_, _supersedes_. |

Every relationship you create becomes an edge in DataHub's metadata graph — the same graph that
holds lineage, ownership, and domains. That is what makes the ontology queryable alongside the rest
of your metadata rather than as a side artifact.

## Relationship semantics

Each relationship type carries semantics that govern how it is traversed and displayed:

- **Direction.** A **directed** relationship runs source → destination and reads differently in each
  direction (_Individual Customer_ **inherits** _Customer_; read backwards, _Customer_ is
  **inherited by** _Individual Customer_). An **undirected** (symmetric) relationship reads the same
  both ways — _Client_ is a **synonym of** _Customer_ implies the reverse without a second edge.
- **Transitivity.** Some relationships chain: if A is a kind of B and B is a kind of C, then A is a
  kind of C. Marking a relationship transitive tells DataHub that closure over it is meaningful.
- **Standard vocabulary.** A relationship type can declare the RDF predicate it corresponds to (for
  example `skos:broader`, `rdfs:subClassOf`), so queries written against W3C standard vocabularies
  resolve to your relationships.

Both kinds behave identically once created: they materialize as edges in the same graph, are drawn
in the same views, and are traversed by the same APIs. Which relationships exist and when to use
each is covered in [Relating Glossary Terms](relating-glossary-terms.md) and
[Adding Custom Relationships](custom-relationships.md).

## Where to go next

1. **[Relating Glossary Terms](relating-glossary-terms.md)** — build the ontology by connecting the
   terms you already have.
2. **[Visualizing Your Ontology](visualizing-your-ontology.md)** — explore the resulting graph in the
   UI, globally or from a single term.
3. **[Querying Your Ontology](querying-your-ontology.md)** — walk the relationship graph from the
   API, or let an AI agent walk it for you.
4. **[Adding Custom Relationships](custom-relationships.md)** — extend the vocabulary with your own
   relationship types.
5. **[Advanced Querying with SPARQL](sparql-api.md)** — query the graph with standard SPARQL.

## Importing an existing ontology

If your organization already maintains an ontology in RDF, OWL, or SKOS, you do not have to rebuild
it by hand. DataHub's **RDF ingestion source** imports SKOS concepts as glossary terms, derives term
groups from IRI path hierarchies, and maps standard predicates such as `skos:broader` /
`skos:narrower` onto DataHub relationships:

```yaml
source:
  type: rdf
  config:
    source: path/to/glossary.ttl
    environment: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    token: "${DATAHUB_TOKEN}"
```

## Related reading

- [Business Glossary](../../../glossary/business-glossary.md) — creating and organizing the terms
  themselves.
- [Structured Properties](../properties/overview.md) — the mechanism behind custom relationship
  types.
