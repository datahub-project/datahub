# What the impact walk needs from a graph edge

Written 2026-10-08. This is the reference for what `graph_service_v1` stores, what the impact walk reads, and what the in-memory server keeps. Use it to decide what memory can be cut and before sizing an instance. The findings come from reading the code on branch `lineage-graph-server`, not from runs.

## 1. Relationship types and lineage

- Every aspect field annotated with `@Relationship` produces graph edges. The annotation's name becomes the edge's `relationshipType`.
  - Lineage examples: `upstreamLineage` gives `DownstreamOf`, and `dataJobInputOutput` gives `Consumes` and `Produces`.
  - Non-lineage examples: `ownership` gives `OwnedBy`, `globalTags` gives `TaggedWith`, and `container` gives `IsPartOf`.
- `graph_service_v1` holds one document per edge, for every relationship type, lineage or not.
- `searchAcrossLineage`, `scrollAcrossLineage` and `getLineage` walk only the lineage types. `LineageRegistry` declares them per entity type and direction.
- Each hop is restricted to (entity type, relationship type, direction, opposing entity type) combinations: `LineageGraphFilters.containsEdgeInfo` in OpenSearch, and `EdgeFilter` in the server.
- The server loads only these 17 types, as logged at startup:

  ```
  AgentUsesTool, Consumes, ConsumesMetric, Contains, DashboardContainsDashboard,
  DataProcessInstanceConsumes, DataProcessInstanceProduces, DerivedFrom, DownstreamOf,
  MemberOf, Produces, RepositoryForkOf, ServiceComposesApi, SourcedFrom, TrainedBy,
  UsedBy, consumesField
  ```

## 2. A `graph_service_v1` document

The document is written by `ElasticSearchGraphService.toDocument`. The mapping is in `GraphRelationshipMappingsBuilder`.

| Field | Notes |
|---|---|
| `source.urn`, `source.entityType` | always present |
| `destination.urn`, `destination.entityType` | always present |
| `relationshipType` | always present |
| `createdOn`, `createdActor`, `updatedOn`, `updatedActor` | optional |
| `properties` | string map; `properties.source = "UI"` marks a manual edge |
| `via` | optional. An intermediate entity, e.g. the query or data job that produced a column-level edge. |
| `lifecycleOwner` | optional. The entity whose aspect declared the edge. Set on fine-grained (column-level) `DownstreamOf` edges built in `GraphIndexUtils`. |
| `source.removed`, `destination.removed`, `viaRemoved`, `lifecycleOwnerRemoved` | soft-delete status, set by `setEdgeStatus` |

- **Document id:** a hash of (source, relationship type, destination, lifecycle owner). `via` is not part of it. The server uses the same edge identity.
- **Bench index:** 2,097,495 docs, 168.5 MiB on disk, 1 shard, no replica, about 84 B/edge. The bench docs carry only source, destination, relationship type, timestamps and actors.

## 3. What each field is used for, and what the server keeps

Callers checked:
- `getImpactLineage` is called only from `LineageSearchService`.
- `LineageSearchService` reads only these `LineageRelationship` fields: `entity`, `degree`, `degrees`, `paths`, `explored`, `truncatedChildren` and `ignoredAsHop`.
- `getLineage` (the lineage visualization) is not served by the server. It still goes to OpenSearch through the delegate.

| Field | Read by the impact walk for | Read after the walk by impact search? | Server keeps | Verdict |
|---|---|---|---|---|
| `source.urn`, `destination.urn` | traversal, paths | yes (entity, paths) | URN dictionary + adjacency | required |
| `*.entityType` | per-entity lineage rules | yes (entity type facet) | 8 bits per node in `nodeMeta`, from the URN | required |
| `relationshipType` | which edges count, direction | no | 5 bits per edge (`TypeTable` id) | required |
| `via` | via rules: vias become relationships and frontier nodes, and cut path cycles | through paths | 4 B per edge + has-via bit | required |
| `createdOn`, `updatedOn` | time-window filter, when the request has `startTimeMillis`/`endTimeMillis` | no | 8 + 8 B per edge | required for the filter. They are also copied into the output, which nobody reads. |
| `properties.source == "UI"` (manual) | **time-window filter**: manual edges always pass (`ImpactWalker.inWindow`) | no | 1 bit per edge | required |
| `createdActor`, `updatedActor` | nothing | **no** | 4 + 4 B per edge | **not needed for impact search.** Only `EntityLineageResultResolver` (visualization) shows them. Kept today so the server's `EntityLineageResult` equals OpenSearch's field for field. |
| `lifecycleOwner` | nothing | no | 4 B per edge | **needed for writes, see §4** |
| `*.removed`, `viaRemoved`, `lifecycleOwnerRemoved` | soft-delete exclusion, only when `ELASTICSEARCH_SEARCH_GRAPH_STATUS_ENABLED=true` | no | **nothing** | **gap, see §5** |
| other `properties` keys | nothing | no | nothing | not needed |

### Relationship, entity type and platform names
Names are not stored per edge or per node. `TypeTable` maps each distinct name to a small id once, case-insensitively as `LineageRegistry` does, and the arrays hold the id:
- relationship type: 5 bits per edge (at most 32 types; 17 in use);
- entity type: 8 bits per node (at most 256);
- platform: 23 bits per node.

**The platform bits are currently unread.** The walker reads only the entity type. `LightningSearch` works out platform and environment with GMS's own rules (`LightningUrnFields`), because `UrnParts` differs for charts, dashboards, data flows and data jobs. Options:
- drop the platform bits;
- or reuse them to hold the lightning facet keys, which would replace the on-heap `NodeFacets` memo.

## 4. `lifecycleOwner`: can it be dropped?

No. It is never read by a walk, but it is needed to apply writes correctly:
1. **Hard delete of the owner** (`removeNode`). OpenSearch also deletes every edge the entity owns (`ElasticSearchGraphService.removeNode`, "all edges where this entity is a lifecycle owner"). For column-level lineage, these are schemaField → schemaField edges between *other* entities' fields. Without the owner, the server would keep them after the owner is deleted, and walks would return lineage that OpenSearch no longer has.
2. **Edge identity.** Two aspects can declare the same schemaField → schemaField edge, for example a data job's and a dataset's fine-grained lineage. These are two documents distinguished by owner. When one owner's aspect stops declaring the edge, only its copy is removed. Without the owner, the server would merge the two, and one removal would drop an edge OpenSearch still has.

It can be stored more cheaply. Only fine-grained edges have an owner, so a sparse map from edge slot to owner could replace the 4 B column. The saving depends on the share of column-level edges. In real deployments these can outnumber table-level edges, so the saving may be small. The bench has no owners at all.

## 5. Gap: soft-deleted entities when graph status is enabled

- **OpenSearch behaviour.** With `ELASTICSEARCH_SEARCH_GRAPH_STATUS_ENABLED=true` (`graphStatusEnabled`, default `false`), the walk query adds `must_not` on `source.removed`, `destination.removed`, `viaRemoved` and `lifecycleOwnerRemoved` (`GraphQueryUtils.applyExcludeSoftDelete`). This is skipped when the request sets `includeSoftDeleted`.
- **Server behaviour.** `StoreGraphService.setEdgeStatus` is a no-op. Its comment ("the impact walk does not filter on soft-delete status") holds only while the flag is off.
- **Consequence.** With the flag on, impact results from the server would include edges that touch soft-deleted entities. The bench and the parity checks ran with the flag off, so they could not catch this.
- **Fix options, not done:**
  1. GMS skips the server whenever `graphStatusEnabled` is true, like the #19948 guard.
  2. Keep a soft-deleted bit per node. Status is set per URN, so 1 bit per node is enough, and `nodeMeta` has spare room if the platform bits go. Apply it in the walk unless `includeSoftDeleted` is set, and maintain it from `setEdgeStatus`. This costs about 150 KB per 1.2M nodes.

## 6. Bytes per node and per edge today

Exact off-heap sizes for bench v1 are in `research.md` §6.

| Per node (~26 B) | Bytes | Why |
|---|---|---|
| URN dictionary | ~13.7 | id → URN for every result and path, and URN → id for the start node and writes. Front-coded in blocks of 16: 84.8 MiB of raw strings become 15.6 MiB. |
| `nodeMeta` | 4 | entity type (8 bits, needed), platform (23 bits, unread, see §3) |
| `fwdOff`, `revOff` | 4 + 4 | where each node's outgoing and incoming edges start; without them, finding a node's neighbours would mean a search |

| Per edge (~46 B) | Bytes | Needed? |
|---|---|---|
| forward neighbour + attribute byte | 4 + 1 | yes |
| reverse neighbour + slot + attribute byte | 4 + 4 + 1 | yes, for upstream walks; the slot shares the cold columns |
| `createdOn`, `updatedOn` | 8 + 8 | yes (time window); could be 4 B each relative to a base, or one shared value for the common case `createdOn == updatedOn` |
| `createdActor`, `updatedActor` | 4 + 4 | no, for impact search |
| `via` | 4 | yes; sparse in practice |
| `lifecycleOwner` | 4 | for writes; sparse |

Minimum for correct impact walks without changing the layout: ~23 B per node (drop the platform bits, add a status bit) and ~38 B per edge (drop actors). Further cuts would come from relative timestamps and sparse via/owner columns: ~22 B per edge (14 B hot + 2 × 4 B relative timestamps) when most edges have no via or owner. Validate this against real data before relying on it in a capacity estimate.

## 7. Downsized footprint for bench v1 (estimate, not built)

The layout keeps only what §3–§5 mark as required, for bench v1 (1,192,270 nodes, 2,097,442 edges):
- **Per node:** the URN dictionary; 1 B `nodeMeta` (7-bit entity type, which allows up to 128 types, plus a soft-delete bit); forward and reverse offsets.
- **Per edge:** the hot neighbour and attribute arrays; no actors; via and owner as sparse columns, with a 1-bit-per-edge has-owner bitmap.

| Part | Today | Downsized |
|---|---|---|
| URN dictionary | 15.6 MiB | 15.6 MiB |
| `nodeMeta` | 4.5 MiB | 1.1 MiB |
| Offsets (fwd + rev) | 9.1 MiB | 9.1 MiB |
| Hot edge arrays (14 B/edge) | 28.0 MiB | 28.0 MiB |
| Actors | 16.0 MiB | 0 |
| Via, owner | 16.0 MiB | ~0.25 MiB (bench has none) |
| Timestamps | 32.0 MiB | 32.0 / 24.0 / 0 MiB, see below |
| **Total** | **121.3 MiB** | **86 / 78 / 54 MiB** |

Timestamp options:
- **8 + 8 B:** as today.
- **6 + 6 B:** 48-bit epoch milliseconds, which keeps exact millisecond comparisons in the time-window filter.
- **Sparse:** only edges with a timestamp store one, using the existing has-timestamps bit. The bench's timestamps are all 0, which the walk treats as missing, so this gives 0 MiB here. Real data has timestamps on most edges, so do not carry the 54 MiB figure into sizing.

4-byte relative timestamps in seconds would lose millisecond precision. Edges at the window boundary could then filter differently from OpenSearch.

These figures cover the graph only. Process RSS (~570 MiB) is mostly the JVM, the entity registry and the Kafka client, so it falls only by the graph saving (~35–45 MiB). The on-heap lightning facet memo (`NodeFacets`, ~4.5 MiB at this size) is separate.

## 8. Walk latency vs graph size (from the Phase 0 10× run, `research.md` §6)

- **Walk time ≈ (nodes visited + edges scanned) × cost per step.**
  - A walk reads only what it reaches.
  - Every edge out of a visited node is read, including edges to already-visited nodes and edges that are filtered out.
- **The cost per step grows with graph size, mostly from CPU cache misses, but far less than the graph does.**
  - prod_like root at 2.1M edges: ~50 ns per node.
  - prod_like layer1 and root at 21M edges: ~136–145 ns per node.
  - hub root at 21M edges: ~21 ns per node, because its neighbours are stored together.
- **Expected levelling off (reasoning, not measured).** Once the graph is far beyond cache size, the cost per step should stop growing.
- **Bigger graphs mean bigger walks.**
  - The 10× root reaches 5.8M nodes: 841 ms walk + 290 ms URN decode + 85 ms paths.
  - Production's `maxRelations` caps walk size.
- **Memory to plan for is per-walk scratch on the Java heap: 64 B per visited node.** Buffers are pooled and reused (`ImpactWalker`), so there is no per-walk garbage, but each concurrent walk holds one.
  - About 370 MB for one 5.8M-node walk.
  - Concurrent big walks can need more than the graph itself.
  - Swapping in a rebuilt graph briefly holds two graphs.
- **Not measured:** the server + GMS + pushdown path on the 10× graph.
