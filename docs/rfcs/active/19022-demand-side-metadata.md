- Start Date: 2026-08-09
- RFC PR: https://github.com/datahub-project/datahub/pull/19022
- Discussion Issue: (none yet)
- Implementation PR(s): (leave this empty)

# Demand-side metadata: a first-class record of assets that do not exist yet

## Summary

DataHub records what exists. This RFC proposes a first-class way to record what is
_wanted and missing_: an entity representing an asset consumers have asked for and not
found, carrying attributed demand from the consumers that asked, and a lifecycle that ends
when the asset becomes real.

## Basic example

A consumer — increasingly an agent rather than a person — searches the catalog for an
asset that is not there. Today that search returns nothing and the event is lost. Under
this proposal the miss is recordable:

```python
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import make_demand_urn
from datahub.metadata.schema_classes import DemandClass, DemandRequesterClass

emitter.emit(MetadataChangeProposalWrapper(
    entityUrn=make_demand_urn("monthly recurring revenue by segment"),
    aspect=DemandClass(
        want="monthly recurring revenue by segment",
        requesters=[DemandRequesterClass(
            requestId="query-log:2026-08-09T12:15:35Z:revenue-copilot",
            requester="urn:li:corpuser:revenue-copilot",
            requestedAt=1786261735023,
            query="SELECT segment, month, SUM(mrr) FROM ? GROUP BY segment, month",
            neededFields=["segment", "month", "mrr"],
            source="query-log",
        )],
        state="OPEN",
    ),
))
```

Three independent consumers emitting the same `want` converge on one entity with demand 3
and three attributed requesters. When someone builds the asset, the demand entity is
resolved and points at the dataset that now satisfies it.

## Motivation

**The gap between what a warehouse contains and what its consumers need is invisible, and
it is the single most decision-relevant fact a data team has.** Every catalog can tell you
what you have. None can tell you what your consumers keep asking for and not getting.

Today that signal is discarded in three places at once:

1. A search in the DataHub UI that returns nothing produces no durable record.
2. A query against a non-existent relation produces a database error and a log line that
   log rotation deletes.
3. A data request lives in Jira, Slack or a spreadsheet, disconnected from the catalog, so
   it cannot be searched, owned, deduplicated against existing assets, or wired into
   lineage when it is finally satisfied.

Two things make this newly urgent rather than merely untidy.

**Volume.** Analytics consumers used to be people, who file tickets when they cannot find
something. They are increasingly agents, which do not. An agent that misses fails silently
in its own context, and the same miss can recur hundreds of times across an organisation
without a single human ever learning that it happened.

**Convergence.** When three consumers in three different contexts want the same missing
asset, there is currently nowhere for them to discover that fact. The catalog is the only
system in the stack that could plausibly host that handshake, because it is the only one
that already has a shared namespace, an identity model, ownership and lineage.

The expected outcome: a data platform team can answer "what should we build next?" from
counted, attributed, deduplicated demand rather than from whoever asked most recently or
most loudly.

## Requirements

- A want that does not exist yet must be **addressable** — it needs a stable URN so that
  independent requesters converge on one record rather than creating N duplicates.
- Demand must be **attributed**: who asked, when, and ideally what they were trying to run.
- The record must be **searchable and ownable** using existing DataHub mechanisms, with no
  new query surface for consumers to learn.
- The lifecycle must be explicit and terminal: a demand record is eventually **resolved**
  (the asset exists), **rejected** (we will not build it, with a reason), or **expired**.
- On resolution the demand record must **link to the dataset that satisfied it**, so the
  provenance question "why does this table exist?" has an answer in the graph.
- Emission must be possible from outside DataHub — from a query-log parser, a BI tool, an
  MCP server, an agent framework — without those systems being modified by DataHub.

### Extensibility

The requester field should not assume a human. It should accept any actor URN, so that
agent identities, service accounts and users are all expressible without a schema change.
The same shape should extend to demand for entities other than datasets — a missing
glossary term, a missing data product — without redefining the aspect.

## Non-Requirements

- **Automatic fulfilment is out of scope.** This RFC records demand; it does not propose
  that DataHub write models, generate SQL, or open pull requests.
- **Prioritisation policy is out of scope.** Whether demand 5 outranks demand 3 is an
  organisational decision, not a catalog one.
- Access control semantics beyond DataHub's existing entity-level model.
- Any change to how existing datasets are searched or ranked.

## Detailed design

### The entity

A demand record is proposed as its own entity type, `demand`, keyed by a normalised want
string plus an optional namespace, rather than as a `dataset` with special properties.

The alternative — reusing `dataset` — is discussed under **Alternatives**, and it is what
our reference implementation does today. We now believe it is the wrong long-term shape,
for a specific reason: a demand record is not a dataset, and making it one means every
consumer of the dataset entity must learn to exclude it. Search results, lineage
traversals, freshness monitors and data-quality assertions all begin returning things that
do not exist. Squatting is expedient and it externalises its cost onto every other feature.

### The aspect

```
record Demand {
  /** The asset that was wanted, in the requester's own words. */
  want: string

  /** Optional namespace, e.g. a team or environment, to scope convergence. */
  namespace: optional string

  /** Every actor that has asked, with what they were trying to do. */
  requesters: array[DemandRequester]

  /** OPEN | CLAIMED | RESOLVED | REJECTED | EXPIRED */
  state: DemandState

  /** Set when state is RESOLVED: the asset that now satisfies this want. */
  resolvedBy: optional Urn

  /** Set when state is REJECTED: why, in prose. Never silent. */
  rejectionReason: optional string
}

record DemandRequester {
  requestId: string        // stable per-event key so emitter retries don't double-count
  requester: Urn           // corpuser, corpGroup, or any actor URN
  requestedAt: Time
  query: optional string   // what they were trying to run, sanitized before storage
  neededFields: array[string]
  source: string           // how we know: "search", "query-log", "api", ...
}
```

`source` is deliberately mandatory. Demand harvested from a database error log is weaker
evidence than demand emitted by an authenticated client, and a reader must be able to tell
the two apart without inspecting the emitter. `requestId` is mandatory so that an emitter
retry replaces its own prior entry instead of counting as a second requester; it is
opaque to DataHub and the emitter defines what makes two requests "the same" (e.g. a
query-log offset or a hash of actor + want + time bucket). `query`, when populated, must
be redacted of literals before emission — the aspect stores a query shape for provenance,
not a queryable copy of user data.

### Convergence

The URN is derived from a normalised form of `want` (lowercased, punctuation stripped,
whitespace collapsed) so that "MRR by segment" and "mrr by segment" converge. Normalisation
is intentionally conservative: it does not attempt semantic matching, because a false
convergence silently merges two different wants and is much harder to detect than a
duplicate.

Deduplication beyond exact-normalised matching is left to a future proposal.

Convergence on one URN is necessary but not sufficient: a plain `emit()` of the `Demand`
aspect is a whole-value UPSERT, so two requesters racing on the same URN would have the
second overwrite the first's `requesters` entry instead of appending to it. Merging
`requesters` across concurrent writers therefore needs an atomic append — either a
server-side keyed-collection mutation, or a client-side read-current-aspect-then-conditional-write
retry loop — and this RFC does not yet pick between them; see **Unresolved questions**.

### Resolution

When a demand is resolved, `resolvedBy` points at the dataset URN that satisfies it, and
the requester list is preserved. The dataset gains a reciprocal edge, so the graph can
answer "which consumers caused this table to exist, and what were they trying to run?"

This is the part we believe is genuinely novel: **provenance that points backwards past the
creation of the asset**, into the demand that motivated it.

### Emission paths

- **From DataHub itself**: a zero-result search in the UI or GraphQL can optionally offer
  to record demand. Opt-in, never automatic — most zero-result searches are typos.
- **From outside**: any client emits the aspect. Our reference implementation does this
  from an MCP server, so that tool-using agents record demand as a side effect of their
  own failed lookups, and from a Postgres error-log parser, which requires no change to the
  systems being observed.

## How we teach this

The term we have found clearest is **demand**, and the framing that lands is: _a catalog is
a map of what exists; this is a map of what is missing._

Audiences affected: platform teams (a new prioritisation input), ingestion authors (a new
optional emission path), and application developers (a new entity to render). No existing
audience is required to learn anything: an organisation that emits no demand sees no change.

Documentation would need a new page under "Metadata Modeling" and an entry in the entity
reference. We do not believe it changes how DataHub is introduced to new users, because it
is meaningless until a catalog has consumers.

## Drawbacks

**It records things that are not true.** A catalog whose defining property is that its
contents exist would begin to contain entries that do not. Anyone consuming DataHub
programmatically must now be explicit about whether they mean assets or wants. This is a
real conceptual cost and it is the main argument against.

**It can be filled with noise.** Every typo'd search is potential demand. This is why UI
emission must be opt-in and why `source` is mandatory rather than optional.

**It invites an unbounded backlog.** Demand with no expiry becomes a graveyard that makes
the catalog look neglected. `EXPIRED` is in the state machine for this reason, but the
policy question of when to expire is not answered here.

**It may belong outside the catalog.** Reasonable people will argue this is a ticketing
concern. Our answer is in **Alternatives**, and we accept it is a judgement call rather
than a proof.

## Alternatives

**Model demand as a `dataset` with custom properties.** This is what our reference
implementation does today, and it works: ghosts are searchable, ownable and lineage-bearing
with zero changes to DataHub. It is also, on reflection, the weakest part of the design —
lifecycle state ends up as key-value pairs in `customProperties` rather than a typed
aspect, and every other dataset consumer inherits entities that do not exist. We would
rather propose the honest shape and be told it is too much, than ship the squat quietly.

**Keep demand in a ticketing system.** This is the status quo, and it is where the human
version of this problem already lives — Secoda, for instance, ships data-request management
for people. It fails for three reasons specific to this proposal: a Jira ticket cannot be
an upstream in lineage, cannot be deduplicated against existing catalog assets, and cannot
be written by an agent that only speaks to the catalog. The convergence of independent
requesters — the single most valuable property here — requires a shared namespace, which is
precisely what a catalog is.

**Do nothing.** The signal continues to be discarded. As the proportion of catalog
consumers that are agents rises, the volume of silently discarded demand rises with it.

**Prior art.** Package registries solve a version of this with "requested but unpublished"
names; issue trackers solve the attribution half but not the namespace half. We are not
aware of a metadata catalog that treats a non-existent asset as a first-class entity, and
would genuinely welcome being corrected in review.

## Rollout / Adoption Strategy

Purely additive and not a breaking change. An organisation that emits no demand records has
an unchanged DataHub. Search over demand entities would be opt-in via an explicit entity
type filter, so existing queries cannot begin returning wants by accident.

Suggested sequencing: the aspect and entity first, emission from external clients second,
optional in-product emission from zero-result search last — by which point there is real
usage data to tune it against.

## Future Work

- Expiry and archival policy for stale demand.
- Semantic deduplication of wants ("MRR by segment" vs "monthly recurring revenue split by
  customer segment"), which is a hard problem and deliberately out of scope here.
- Demand for non-dataset entities.
- Aggregate demand across organisations, which raises privacy questions this RFC does not
  address.

## Unresolved questions

1. **New entity type, or an aspect on an existing one?** This RFC argues for a new `demand`
   entity; the maintainers may reasonably prefer an aspect on `dataProduct` or a structured
   property, and we would defer to that.
2. **Should DataHub itself ever emit demand from a zero-result search**, or should that
   always be an external concern?
3. **How conservative should want-normalisation be?** We have argued for stripping
   punctuation, but that collapses distinct wants that only differ by punctuation (e.g.
   `re-sign` vs `resign`, `foo.bar` vs `foobar`) onto one URN with no collision detection;
   escaping instead of stripping, or accepting the duplicate rate that comes with a more
   literal key, are both on the table.
4. **Is `EXPIRED` enough of an answer to backlog rot**, or does this need a TTL in the model
   rather than in policy?
5. **What is the concrete atomic-append mechanism for `requesters`?** A new Restli/GraphQL
   PATCH-style endpoint, or a documented read-modify-write contract against aspect version —
   we have not chosen, and the choice affects every emitter's SDK usage.
6. **What relationship, if any, does `resolvedBy` register in the graph?** As written it is
   a bare `Urn` field; making "which demand caused this dataset to exist" traversable
   requires picking a relationship name, direction, and whether the dataset side is derived
   or requires its own aspect write.
7. **Should the state machine be enforced server-side** (e.g. rejecting a `RESOLVED` write
   with no `resolvedBy`, or a write that reopens a terminal record), or is that left to
   convention the way most DataHub aspects are today?
8. **Who is allowed to assert `requester`?** As modelled, any principal permitted to write
   the aspect can attribute a want to any actor URN, including one that never asked;
   whether that requires a separate `recordedBy`/collector identity is open.

## Reference implementation

The design above is not hypothetical. A working implementation of the demand lifecycle —
convergence of independent requesters, attributed demand, contract registration, and
resolution with a reciprocal edge to the dataset that satisfied the want — runs against
DataHub OSS v1.7.0 at https://github.com/Morkeeth/nullspace, built for the Build with
DataHub agent hackathon.

It currently models demand as a `dataset` under a `nullspace` platform, which is exactly
the squat this RFC argues against. It is offered as evidence that the lifecycle is
implementable and useful, not as a proposal for how it should be modelled.
