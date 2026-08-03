# What is GMA graph?

All the [entities](entity.md) and [relationships](relationship.md) are stored in a graph store.
Historically this was Neo4j; production DataHub deployments typically use the **Elasticsearch / OpenSearch graph index** (`graph_service_v1`) via `ElasticSearchGraphService`. Neo4j remains an optional backend.

The graph always represents the current state of the world and has no direct support for versioning or history.
However, as stated in the [Metadata Modeling](../modeling/metadata-model.md) section,
the graph is merely a derived view of all metadata [aspects](aspect.md) thus can always be rebuilt directly from historic [MAEs / MCLs](mxe.md#metadata-audit-event-mae).
Consequently, it is possible to build a specific snapshot of the graph in time by replaying events up to that point.

In theory, the system can work with any generic [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) graph DB that supports the following operations:

- Dynamical creation, modification, and removal of nodes and edges
- Dynamical attachment of key-value properties to each node and edge
- Transactional partial updates of properties of a specific node or edge
- Fast ID-based retrieval of nodes & edges
- Efficient queries involving both graph traversal and properties value filtering
- Support efficient bidirectional graph traversal

## How graph edges are written (ES / OpenSearch)

Aspects own their outgoing relationship types. Ownership is implied by unique
`(source entity type, relationship type, destination entity type)` per aspect — there is no
`aspectName` field on the edge document.

When `GRAPH_SERVICE_DIFF_MODE_ENABLED=true` (the default), MAE / MCL consumers apply a
**pairwise edge diff** between the previous and new aspect payloads:

- remove edges present only in the previous aspect (hard delete of the edge document)
- add edges present only in the new aspect
- update (upsert) edges present in both when edge properties change

An empty / missing `previousAspect` is **add-only**: it does not clear existing graph edges for
that relationship type. Force reindex paths that wipe and rebuild use a different code path
(`FORCE_INDEXING` / non-diff mode).

### `graphWriteVersion` fencing

Every edge write produced from an MCL is stamped with `graphWriteVersion` equal to the **new**
aspect `SystemMetadata.version` for that event (including subtractive deletes). That version is
stored on the graph document and used for:

1. **Conditional upsert** — if the edge document already exists and its `graphWriteVersion` is
   strictly greater than the incoming write, the upsert is a no-op. Documents **without**
   `graphWriteVersion` (pre-upgrade edges) are treated as unversioned: any versioned write applies
   and stamps the field. Missing documents are still created (hard deletes leave no tombstone).
2. **In-process bulk requeue fence** — when a bulk item fails and would be requeued
   (`ES_BULK_ITEM_REQUEUE_*`), the writer declines requeue if a **newer** version for the same
   edge docId was already submitted in this JVM. That prevents a late requeued upsert from
   resurrecting an edge after a newer hard delete (and the reverse: a stale delete after a newer
   upsert).

Diff mode never fully reconciles the live graph against the aspect. Orphans left by exhausted
non-retriable deletes are out of scope for automatic heal; use
[restore indices](../how/restore-indices.md) when repairing historical ghost edges.
