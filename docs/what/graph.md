<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

# What is GMA graph?

All the [entities](entity.md) and [relationships](relationship.md) are stored in a graph database, Neo4j.
The graph always represents the current state of the world and has no direct support for versioning or history.
However, as stated in the [Metadata Modeling](../modeling/metadata-model.md) section,
the graph is merely a derived view of all metadata [aspects](aspect.md) thus can always be rebuilt directly from historic [MAEs](mxe.md#metadata-audit-event-mae).
Consequently, it is possible to build a specific snapshot of the graph in time by replaying MAEs up to that point.

In theory, the system can work with any generic [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) graph DB that supports the following operations:

- Dynamical creation, modification, and removal of nodes and edges
- Dynamical attachment of key-value properties to each node and edge
- Transactional partial updates of properties of a specific node or edge
- Fast ID-based retrieval of nodes & edges
- Efficient queries involving both graph traversal and properties value filtering
- Support efficient bidirectional graph traversal
