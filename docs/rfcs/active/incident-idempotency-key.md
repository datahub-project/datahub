- Start Date: 2026-08-01
- RFC PR: (this PR)
- Discussion Issue: none yet, opening the RFC directly to get concrete design feedback
- Implementation PR(s): none, this RFC proposes no code yet

# External Source Identity for Idempotent Incident Creation

## Summary

Add an optional, caller-supplied external identity to `raiseIncident` so automated callers (agents, pipelines) can retry the mutation after a network failure without risking a duplicate Incident.

## Basic example

```graphql
mutation {
  raiseIncident(input: {
    type: OPERATIONAL
    resourceUrns: ["urn:li:dataset:(...)"]
    sourceId: "checkout-drift-2026-08-01-poll-status"
    title: "Route divergence detected on checkout status endpoint"
  })
}
```

Calling this twice with the same `resourceUrns` and `sourceId` returns the same Incident URN both times. Omitting `sourceId` preserves today's behavior exactly: a fresh random id every call.

## Motivation

Any automated caller that must guarantee at-least-once delivery (a retry after timeout, a crash-and-resume, a queue redelivery) currently has no safe way to call `raiseIncident` idempotently. `IncidentKey.id` is set from `UUID.randomUUID()` on every call (`RaiseIncidentResolver.java`), so a retried call after an ambiguous network failure, where the request succeeded server-side but the response was lost, always creates a second Incident.

The only workaround available today is client-side: search existing Incidents for a fingerprint embedded in `title` or `description` before calling `raiseIncident`. This has a race window between the search and the create, and it is not correct under concurrent writers. It also does not scale as the number of automated Incident-raising integrations grows, since every caller ends up inventing its own ad hoc fingerprinting scheme.

This is a gap in the mutation's contract, not a gap in any particular client.

## Requirements

- A caller can supply an external identity scoped to a given `resourceUrns` set.
- Retrying `raiseIncident` with the same `(resourceUrns, sourceId)` is a no-op that returns the existing Incident's URN, not an error and not a duplicate.
- Fully backward compatible: omitting the identity preserves current random UUID behavior exactly, with no change for existing callers.
- Authorization is unchanged: the existing `EDIT_ENTITY_INCIDENTS_PRIVILEGE` check on `resourceUrns` applies identically whether the call creates a new Incident or resolves to an existing one.
- No new synchronous uniqueness query is required beyond what deterministic identity derivation already gives, so this should not add a network round-trip to the common path.

### Extensibility

The identity scheme should not be special-cased to agent callers. Any integration that needs at-least-once-safe Incident creation (pipelines, external monitoring systems, future MCP tools) should be able to use the same field.

## Non-Requirements

- Idempotency for `updateIncident` or `updateIncidentStatus`. This RFC covers creation only.
- A general-purpose dedup mechanism for other entity types. If this pattern proves useful, extending it elsewhere is future work, not part of this proposal.
- Changing the default (no-`sourceId`) behavior in any way.

## Detailed design

**Where identity lives.** Add `sourceId: optional string` to `RaiseIncidentInput` (GraphQL) and derive the Incident's `IncidentKey.id` deterministically from it when present, instead of `UUID.randomUUID()`:

```text
id = deterministic_hash(sorted(resourceUrns), sourceId)
```

This is the same pattern DataHub already uses for most other keyed entities. A Dataset's urn, for example, is derived from `platform + name + origin`, not minted randomly. Incident is the outlier today in using an opaque random id as its only identity.

**Reuse `CreateIfNotExistsValidator` instead of a new mechanism.** DataHub already has a working create-if-not-exists primitive: a `ChangeType.CREATE_ENTITY` proposal carrying the header `If-None-Match: *` is silently dropped, no exception surfaced, if the entity's key aspect already exists (`entity-registry/.../validation/CreateIfNotExistsValidator.java`). It's already used by `CreateTagResolver`, `CreateDomainResolver`, `CreateSecretResolver`, `CreateGlossaryNodeResolver`, `CreateGlossaryTermResolver`, and `CreateBusinessAttributeResolver`.

`RaiseIncidentResolver` doesn't reach this today because the helper it calls, `MutationUtils.buildMetadataChangeProposalWithKey`, hardcodes `ChangeType.UPSERT` and never sets `headers`. When `sourceId` is present, the resolver should build its own proposal instead of using that shared helper (it's used by eight other resolvers, so changing its default isn't appropriate) and set:

```text
changeType = ChangeType.CREATE_ENTITY
headers    = {"If-None-Match": "*"}
```

Since the deterministically-derived urn either already has a key aspect (entity exists) or doesn't (it's new), the validator either drops the proposal as a filtered no-op or lets the create through. The resolver already knows the target urn before making the call, since it's computed from `(resourceUrns, sourceId)`, so whether the write landed or was dropped, it returns the same urn either way. No separate existence check or read-then-write race is needed, and no new concurrency guarantee needs to be invented: this relies on the same one `CreateIfNotExistsValidator` already provides for every other `CREATE_ENTITY` path in the codebase.

**Authorization when a key already exists.** Unchanged. The resolver checks `EDIT_ENTITY_INCIDENTS_PRIVILEGE` against `resourceUrns` before doing anything else, exactly as it does today. Whether the call turns out to create or resolve to an existing Incident is decided after that check, not before it.

**Conflict and update semantics.** If a caller repeats `(resourceUrns, sourceId)` with different `title`, `description`, or `priority` values, the write is dropped by the validator exactly as any other duplicate `CREATE_ENTITY` would be. The new field values are ignored and the existing Incident's URN is returned unchanged. A caller that wants to change fields on an existing Incident should call `updateIncident` explicitly. This keeps `raiseIncident`'s contract narrow: idempotent by identity, not upsert-by-value.

**Compatibility.** Fully additive. Existing Incidents (random ids, created via `UPSERT` as today) and new ones (deterministic ids via `CREATE_ENTITY` plus the precondition header, when `sourceId` is supplied) are both just opaque `id: string` values to every other part of the system. Nothing downstream needs to change.

## How we teach this

Document `sourceId` alongside `raiseIncident` in the GraphQL API reference, and note it in Agent Context guidance as the correct way for automated callers to make Incident creation retry-safe. This is additive documentation, not a change to how Incidents are taught to end users in the UI.

## Drawbacks

- Two code paths in the resolver (random-id `UPSERT` vs. deterministic-id `CREATE_ENTITY`) add a small amount of complexity, and the new path can't reuse the shared `buildMetadataChangeProposalWithKey` helper as-is.
- If a Dataset (or other resource) is later renamed or its urn changes, a previously-computed deterministic id tied to the old urn no longer matches on retry. Worth resolving explicitly rather than leaving implicit.
- The exact hash and derivation function needs to be pinned down precisely (algorithm, encoding, collision behavior) before implementation, not left to resolver code to decide ad hoc.

## Alternatives

- **Server-side dedup by searching a stored `sourceId` field before insert.** Rejected as the primary mechanism: it has the same search-then-insert race window as the client-side workaround it's meant to replace, just moved server-side, and it ignores that DataHub already has a race-free primitive for this that Incident simply isn't wired to yet.
- **Status quo (client-side fingerprinting).** Rejected. This is the problem being solved; every caller currently reinvents a weaker version of the same thing.
- **A dedicated idempotency-key store** (Stripe-style key and response cache). Rejected as unnecessary: it would duplicate a guarantee the create-if-not-exists validator already provides at the aspect layer, for a bigger implementation cost.

## Rollout / Adoption Strategy

Purely additive, no migration. Existing callers are unaffected until they opt in by supplying `sourceId`.

## Future Work

The same pattern, deterministic identity for idempotent creation, could extend to other mutations that currently mint random keys for automation-facing writes, if this proves out.

## Unresolved questions

- Exact derivation function for the deterministic id (hash choice, input encoding, handling of `resourceUrns` ordering).
- Whether `raiseIncident` should signal "created" vs. "already existed" to the caller, or stay silent and just return the URN either way. Since the resolver already knows the urn up front, this is a minor API-ergonomics question, not a correctness one.
- Whether identity should live on `IncidentKey` (proposed here) or as a field on the `IncidentInfo.source` aspect instead. Key placement keeps identity immutable and query-cheap, but it's a bigger schema decision than a single aspect field and deserves maintainer input.
- Whether `CreateIfNotExistsValidator` is already registered against the Incident entity's plugin config, or needs to be added there. This RFC assumes it can be, based on how other `CREATE_ENTITY` resolvers use it, but the entity-registry plugin wiring for Incident specifically wasn't traced line by line.
