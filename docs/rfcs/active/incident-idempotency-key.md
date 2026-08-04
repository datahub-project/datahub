- Start Date: 2026-08-01
- RFC PR: https://github.com/datahub-project/datahub/pull/18804
- Discussion Issue: none yet, opening the RFC directly to get concrete design feedback
- Implementation PR(s): none, this RFC proposes no code yet

# Caller-Provided Identity for Idempotent Incident Creation

## Summary

Add an optional, caller-provided `id` to `raiseIncident` so automated callers (agents, pipelines) can retry the mutation after a network failure without risking a duplicate Incident. Retrying with an `id` that already exists fails with a conflict rather than silently succeeding, so the caller can treat the conflict as confirmation that the first write landed and recover by fetching the Incident at the URN it already chose.

## Basic example

```graphql
mutation {
  raiseIncident(
    input: {
      type: OPERATIONAL
      resourceUrns: ["urn:li:dataset:(...)"]
      id: "checkout-drift-2026-08-01-poll-status"
      title: "Route divergence detected on checkout status endpoint"
    }
  )
}
```

Calling this once creates the Incident at `urn:li:incident:checkout-drift-2026-08-01-poll-status`. Calling it again with the same `id` fails with a conflict: the Incident already exists at that URN, so no second Incident is created and the mutation does not silently return success. Because the caller chose `id` up front, it already knows the target URN and can fetch it directly on conflict instead of treating the mutation's response as evidence of a new creation. Omitting `id` preserves today's behavior exactly: a fresh random id, and thus a new Incident, on every call.

## Motivation

Any automated caller that must guarantee at-least-once delivery (a retry after timeout, a crash-and-resume, a queue redelivery) currently has no safe way to call `raiseIncident` idempotently. `IncidentKey.id` is set from `UUID.randomUUID()` on every call (`RaiseIncidentResolver.java`), so a retried call after an ambiguous network failure, where the request succeeded server-side but the response was lost, always creates a second Incident.

The only workaround available today is client-side: search existing Incidents for a fingerprint embedded in `title` or `description` before calling `raiseIncident`. This has a race window between the search and the create, and it is not correct under concurrent writers. It also does not scale as the number of automated Incident-raising integrations grows, since every caller ends up inventing its own ad hoc fingerprinting scheme.

This is a gap in the mutation's contract, not a gap in any particular client.

## Requirements

- A caller can supply an optional `id` that is used directly as `IncidentKey.id` (`urn:li:incident:{id}`). It is an opaque caller-chosen string, not derived from or scoped to `resourceUrns` or any other input field.
- Retrying `raiseIncident` with an `id` that already exists produces an explicit conflict, not a silent success and not a duplicate Incident. `raiseIncident` stays create-only; it never becomes get-or-create.
- Fully backward compatible: omitting `id` preserves current random UUID behavior exactly, with no change for existing callers.
- Authorization is unchanged: the existing `EDIT_ENTITY_INCIDENTS_PRIVILEGE` check on `resourceUrns` runs before the write is attempted, identically whether or not `id` is supplied.
- The create-or-conflict decision must be atomic with respect to concurrent callers racing on the same `id`. No separate existence check followed by a write.

The resulting contract:

| Input         | Result                                                               |
| ------------- | -------------------------------------------------------------------- |
| `id` omitted  | Existing random UUID behavior; create a new Incident                 |
| New `id`      | Create the Incident at `urn:li:incident:{id}`                        |
| Existing `id` | GraphQL conflict; never update or return the existing URN as success |

Authorization runs against the requested `resourceUrns` before the write in all three cases. The caller's known URN (for a new or previously-supplied `id`) is the retry handle it uses to recover after a conflict, not something a server-side get-or-create response hands back.

### Extensibility

The identity scheme should not be special-cased to agent callers. Any integration that needs at-least-once-safe Incident creation (pipelines, external monitoring systems, future MCP tools) should be able to use the same field.

## Non-Requirements

This proposal is **create idempotency through client-owned identity**. It is explicitly not:

- **Deduplication of active incidents.** `raiseIncident` does not search for or fold into any existing Incident that isn't at the exact `id` the caller supplied.
- **A permanent get-or-create operation.** An `id` conflict is an error the caller reacts to, not a mutation that transparently hands back an existing URN as if it had just been created.
- **Re-firing or reopening a resolved incident.** If a caller reuses an `id` whose Incident has since been resolved, the result is still a conflict, not a reopen. A permanent, deterministic get-or-create would be actively wrong here: it would keep returning a stale, resolved Incident as if raising a fresh one, exactly when the caller expects a new signal.
- **An update or upsert mutation.** A caller that wants to change fields on an existing Incident must call `updateIncident` explicitly. `raiseIncident` never modifies an Incident that already exists at the given `id`.
- **Idempotency for `updateIncident` or `updateIncidentStatus`.** This RFC covers creation only.
- **A general-purpose dedup mechanism for other entity types.** If this pattern proves useful, extending it elsewhere is future work, not part of this proposal.
- **A change to the default (no-`id`) behavior**, in any way.

## Detailed design

**Where identity lives.** Add `id: optional string` to `RaiseIncidentInput` (GraphQL). When present, use it directly as `IncidentKey.id`, producing `urn:li:incident:{id}`. When absent, preserve today's `UUID.randomUUID()` path exactly, unchanged. Identity has no derivation step and no coupling to `resourceUrns`: because the caller owns `id` outright, a later rename of a referenced asset cannot change or invalidate an identity the caller already committed to on retry.

**GraphQL contract precedent: `CreateTagResolver`.** `datahub-graphql-core/.../resolvers/tag/CreateTagResolver.java` already has this exact input shape: `final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();`, followed by `_entityClient.exists(...)` and, if the key already exists, `throw new IllegalArgumentException("This Tag already exists!")`. This is the precedent for `raiseIncident`'s input field and its create-only, conflict-on-existing contract as seen by the caller.

It is not the precedent for the underlying atomicity: Tag's `exists()`-then-create has its own read-then-write race window between the check and the proposal. Incident should not copy that gap.

**Atomic create precondition: `CreateIfNotExistsValidator`.** `entity-registry/.../aspect/validation/CreateIfNotExistsValidator.java` is the mechanism that avoids Tag's race: a `ChangeType.CREATE_ENTITY` proposal carrying the header `If-None-Match: *` is validated against whether the entity's key aspect is already present in the batch. If the key aspect is missing from the batch (meaning the entity already exists), the validator raises a `FILTER`-subtype `AspectValidationException` for that item, quoting `"Dropping write per precondition header If-None-Match: *"`.

`RaiseIncidentResolver` doesn't reach this today because the helper it calls, `MutationUtils.buildMetadataChangeProposalWithKey`, hardcodes `ChangeType.UPSERT` and never sets `headers`. When `id` is present, the resolver should build its own proposal instead of using that shared helper (it's used by eight other resolvers, so changing its default isn't appropriate) and set:

```text
changeType = ChangeType.CREATE_ENTITY
headers    = {"If-None-Match": "*"}
```

**Surfacing the conflict through GraphQL, not swallowing it.** This is the part the original design got wrong, and the part that needs the most care. `AspectValidationException.forFilter(...)` produces a `ValidationSubType.FILTER` exception, and `ValidationExceptionCollection.hasFatalExceptions()` explicitly excludes `FILTER` from what counts as fatal: `subTypeHashCodes.keySet().stream().anyMatch(subType -> !ValidationSubType.FILTER.equals(subType))`. A filtered `CREATE_ENTITY` item does not throw all the way up to the caller by default; it is simply excluded from the batch's successful items (`ValidationExceptionCollection.isSuccessful`). That silent-drop behavior is exactly right for the validator's existing producer/emitter use cases (idempotent MCP replay), but it is exactly wrong for a synchronous GraphQL mutation that must tell an already-authenticated caller whether their write happened.

So `RaiseIncidentResolver` must distinguish "my `CREATE_ENTITY` proposal was applied" from "my proposal was filtered because the key already existed," using the per-item success/filter signal the ingest path already produces (the same distinction `ValidationExceptionCollection` tracks internally), and translate a filtered outcome into a GraphQL error, the same way `CreateTagResolver` throws `IllegalArgumentException` on conflict today. Exact plumbing (how that per-item signal is exposed from `EntityClient.ingestProposal` up to the resolver) is an implementation detail for the eventual PR, not this RFC, but the contract requirement is not optional: **an existing `id` must produce a caller-visible GraphQL error, never a 200-shaped response carrying the existing URN.**

**Authorization when a key already exists.** Unchanged. The resolver checks `EDIT_ENTITY_INCIDENTS_PRIVILEGE` against `resourceUrns` before doing anything else, exactly as it does today, before the create-or-conflict outcome is known.

**Conflict and retry semantics.** If a caller repeats `id`, the write is rejected as a conflict regardless of whether `title`, `description`, or `priority` differ from the original call. The new field values are never applied and the Incident is never modified as a side effect of the conflicting call. The retry story this supports:

1. The client chooses `id`, so it already knows the target URN (`urn:li:incident:{id}`) before calling `raiseIncident`.
2. A first request may create the Incident while its response is lost to the client (timeout, connection drop).
3. A retry with the same input either creates the Incident, if the first write never landed, or receives a conflict error, if the first write did land.
4. The client treats that conflict as recovery evidence, not failure, and fetches the Incident at the URN it already knew, rather than `raiseIncident` itself silently becoming get-or-create.

A caller that wants to change fields on an existing Incident calls `updateIncident` explicitly. This keeps `raiseIncident`'s contract narrow: idempotent by caller-owned identity, not upsert-by-value.

**Compatibility.** Fully additive. Existing Incidents (random ids, created via `UPSERT` as today) and new ones (caller-provided ids via `CREATE_ENTITY` plus the precondition header, when `id` is supplied) are both just opaque `id: string` values to every other part of the system. Nothing downstream needs to change.

## How we teach this

Document `id` alongside `raiseIncident` in the GraphQL API reference, and note it in Agent Context guidance as the correct way for automated callers to make Incident creation retry-safe, including that a conflict on `id` means "already created, fetch by URN" rather than an error to retry blindly. This is additive documentation, not a change to how Incidents are taught to end users in the UI.

## Drawbacks

- Two code paths in the resolver (random-id `UPSERT` vs. caller-provided-id `CREATE_ENTITY`) add a small amount of complexity, and the new path can't reuse the shared `buildMetadataChangeProposalWithKey` helper as-is.
- The resolver needs new logic to distinguish a filtered `CREATE_ENTITY` from an applied one and turn the former into a GraphQL conflict. `CreateIfNotExistsValidator`'s `FILTER` subtype is non-fatal by design at the batch/ingest layer today, so this signal has to be threaded up rather than simply caught as a thrown exception.
- Callers take on responsibility for choosing a unique-enough `id` within their own domain. This is the same tradeoff `CreateTagResolver` already accepts for Tag; it is not a new category of risk for DataHub, but it is a new responsibility for Incident callers specifically.

## Alternatives

- **Deterministic identity derived from a hash of the resource set plus a caller-supplied fingerprint.** This was the design in the original version of this RFC. Rejected: deriving identity from `resourceUrns` ties it to the resource set, so a Dataset rename or any change to the resources referenced silently derives a different id on retry, defeating idempotency exactly when the underlying infrastructure is already unreliable. Caller-owned `id` removes this coupling entirely and matches the precedent DataHub already has for Tag.
- **Permanent deterministic get-or-create (always return the existing URN on a repeat `id`).** Rejected as the default contract: it is a materially different, stronger operation than create idempotency. It would give `id` permanent, evergreen meaning, so retrying against a resolved Incident's `id` would silently resurface a closed Incident instead of failing or being available for a new one. This RFC scopes `raiseIncident` to create-only; a caller that wants dedup or get-or-create semantics needs a separate, explicit product decision outside this RFC.
- **Server-side dedup by searching a stored identity field before insert.** Rejected as the primary mechanism: it has the same search-then-insert race window as the client-side workaround it's meant to replace, just moved server-side, and it ignores that DataHub already has a race-free create-if-not-exists primitive that Incident simply isn't wired to yet.
- **Status quo (client-side fingerprinting).** Rejected. This is the problem being solved; every caller currently reinvents a weaker version of the same thing.
- **A dedicated idempotency-key store** (Stripe-style key and response cache). Rejected as unnecessary: it would duplicate a guarantee the create-if-not-exists validator already provides at the aspect layer, for a bigger implementation cost.

## Rollout / Adoption Strategy

Purely additive, no migration. Existing callers are unaffected until they opt in by supplying `id`.

## Future Work

- The same pattern, caller-provided identity for idempotent creation, could extend to other mutations that currently mint random keys for automation-facing writes, if this proves out.
- If search or filtering on `id` beyond direct URN lookup becomes a real need, a copy of `id` on an `IncidentInfo`-adjacent aspect could be added alongside the key. This RFC does not propose that: identity lives on `IncidentKey` only, to avoid two sources of truth for the same value.

## Unresolved questions

- How the resolver surfaces a filtered `CREATE_ENTITY` as a GraphQL-level conflict: which exception type or error code carries that signal, given that `ValidationExceptionCollection` treats `FILTER`-subtype outcomes as non-fatal at the batch layer today and this proposal needs that outcome to become caller-visible.
- Whether the conflict should be a distinct, matchable GraphQL error code or extension (for example `ALREADY_EXISTS`), so retry-safe clients can branch on it programmatically instead of parsing an error message string.
- Whether `id` needs its own validation (charset, length) beyond what URN construction already implies, or can reuse the same constraints `CreateTagInput.id` uses today.
