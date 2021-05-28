---
title: The Metadata Model
sidebar_label: The Metadata Model
slug: /metadata-modeling/metadata-model
---

# How does DataHub model metadata?

DataHub uses the Pegasus schema (PDL) language with a custom annotation language to model metadata.

Conceptually, metadata is modeled as:

**Entities**: These are the big node types in the metadata graph. For example, a Dataset or a User is an Entity. An entity is made up of a unique identifier (a primary key that can be a complex struct) and a list of aspects.

**Aspects**: These are collections of attributes that describe facets of the entities. Aspects can be keys for entities and are declared as such through an annotation. For example, DashboardInfo contains a collection of attributes that describes information attached to a Dashboard. Aspects can be thought of as groups of columns organized together for convenience of management and access. They also allow for sharing of common models across entities. E.g. The Ownership Aspect is re-used across all the Entities that have ownership attached to them.

**Relationships**: These are implied through a custom annotation language that connects Entities to other Entities through fields stored inside Aspects. If you are coming from a relational world, think foreign keys. These allow references from one entity to another to be queried in reverse. For example, Charts reference CorpUser as their owner. CorpUser uses the OwnedBy relationship to understand all the entities they own.


Here is an example graph consisting of 3 types of entities (User, Chart, Dashboard), 2 types of relationships (OwnedBy, Contains), and 3 types of metadata aspects (Ownership, ChartInfo, and DashboardInfo).

![metadata-modeling](../imgs/metadata-model-chart.png)

# Querying the metadata graph
DataHub’s modeling language allows you to optimize metadata storage to align with how metadata will be queried.

There are three primary ways to query the metadata graph: by Key, through a search query, and through relationships.

### Entity Lookup

A Key-based query looks like:

```
curl --location --request GET 'http://localhost:8080/entities?action=get&urn=urn:li:chart:customers
```

This fetches metadata about a user given a serialized key, or Urn, of the Entity. Note that the `urn` must be url encoded first. The response would be a Snapshot of the Entity containing all of that Entity’s Aspects.


### Search
A search based query looks like:

```
curl --location --request POST 'http://localhost:8080/entities?action=search' \                           
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--data-raw '{
    "input": "\"customers\"",
    "entity": "chart",
    "start": 0,
    "count": 10
}'
```

The key parameters are `input` and `entity`. `input` specifies the query we are issuing and `entity` specifies the entity we want to search over. The response contains a list of serialized keys, or Urns, that can be used to fetch the full entity via Key.

### Relationship

A relationship query looks like:

```
curl --location --request GET --header 'X-RestLi-Protocol-Version: 2.0.0' 'http://localhost:8080/relationships?direction=INCOMING&urn=urn:li:chart:customers&types=OwnedBy,DownstreamOf'
```

The key parameters are `direction`, `urn` and `types`. Note that the `urn` must be url encoded first. This responds with the serialized keys, or Urns, of all the entities that are related to the target urn via the supplied relationships in the designated direction.
