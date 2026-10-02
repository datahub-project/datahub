

# Action Workflows

> **Availability:** DataHub Cloud only

> **Note**: Action Workflows is currently in **Private Beta**. To enable this feature, please reach out to the DataHub team.

Action Workflows extend [Data Access Workflows](access-workflows.md) beyond the access-request use case. The launch UX, Task Center integration, notification model, and Actions integration are unchanged — see the [Data Access Workflows](access-workflows.md) guide for that baseline. This page documents what's new.

## What's New

**Multi-actor quorum policies.** Each step declares whether `anyOf` its assigned actors must approve (the default), `allOf` of them, or `nofM: { n: <count> }` for a threshold quorum. Group and role slots expand transparently — any group member can satisfy that slot — and the audit trail records which slot each decision satisfied.

**Conditional step branching.** A step can declare a `condition` (using the same filter dialect as entrypoint visibility) that decides whether it opens. Steps whose condition evaluates false are auto-skipped with a `SKIPPED` decision in the audit log. A workflow can branch — for example, adding a privacy review step only when the launching dataset carries a PII tag, and skipping it otherwise.

**Entrypoint visibility filters.** Entrypoints can carry a `filter` evaluated against the launching entity's attributes. The launch icon only appears when the filter matches — narrowing visibility by platform, tag, glossary term, domain, owner, sub-type, or any indexed attribute. The same dialect is shared with search saved-filters and views.

**Context-aware form pickers (`dynamicSource`).** URN-valued form fields can resolve their selectable options by traversing the catalog graph at submission time. The resolver starts at the launching entity or at a URN the requester picked in a prior field, then walks documented relationships (`OWNERS_OF`, `DOMAIN_OWNERS_OF`, `TAGS_OF`, `SIBLINGS_IN_DOMAIN`, and others). An optional `filter` narrows the resolved set. Replaces the all-catalog browse with a contextually-scoped picker.

**Form-field visibility via filter expressions (`filterCondition`).** A field can declare a `filterCondition` that decides whether it renders. The condition combines launching-entity attributes (`tags`, `glossaryTerms`, `domain`, `platform`, `owners`, etc.) AND prior form-field values (`formField:<id>`). Strictly more expressive than the v1 `SINGLE_FIELD_VALUE` condition, which is still accepted but deprecated.

**Dynamic step actors via the same resolver dialect.** Step actors can be resolved at step-open time using the same `dynamicSource` used for form pickers — "the owners of the domain the requester picked in their form" is expressible as a chained resolver, not just a fixed `dynamicAssignment.type` value.

**Requester self-cancel and admin override.** The original requester can cancel their own in-flight request from the request's detail page. Platform Admins with `Manage Global Settings` can cancel any request as an override. Cancellation appends an immutable `CANCELLED` decision to the audit history.

**Beyond `ACCESS`.** The category model supports a free-form `customCategory` string when `category` is `"CUSTOM"`, so governance, certification, and proposal workflows can be grouped distinctly from access requests in the Task Center and in reporting.

## Reacting to Workflow Events

Workflow side-effects are implemented as [DataHub Actions](/docs/actions) subscribed to the workflow lifecycle events, exactly as in Data Access Workflows. The events and subscription pattern are unchanged; what shifts in practice is that v2 customers commonly drive _internal_ catalog side-effects alongside external-system provisioning — flipping a structured property to mark a dataset Certified, attaching a glossary term to record the outcome, or propagating metadata downstream when the workflow completes.

## Where to Go Next

- [Workflow Tutorial](action-workflows-tutorial.md) — an end-to-end worked example exercising every primitive above.
- [Workflow Reference](action-workflows-reference.md) — exhaustive JSON syntax for every primitive, every enum value, every resolver, and the shared Filter dialect.

## FAQ

**How does quorum work?**

Each step's `quorum` is a union: set exactly one of `anyOf: true`, `allOf: true`, or `nofM: { n: <count> }`. Group and role slots count as a single slot that any member can satisfy. The audit log records the `satisfiedSlot` URN for each approval. Omit `quorum` entirely to default to `anyOf`.

**How is conditional step skipping different from skipping a step manually?**

A skip is automatic and audited. The engine evaluates the step's `condition` against the launching entity and prior decisions when the step is about to open. If it evaluates false, the engine appends a `SKIPPED` decision to the audit history (with the condition that decided it) and opens the next step. No user input is required, and the skip is permanent for that request — even if the entity later changes such that the condition would now hold, the in-flight request continues forward.

**How do I chain dynamic resolvers across form fields?**

A resolver's `source` accepts `"launching"` (the entity from which the workflow was launched) or `"field:<form-field-id>"` (a URN the requester picked in a prior URN-valued form field). For example, a workflow can have one form field that resolves to _the data products that contain the launching dataset_ (`DATA_PRODUCT_OF` from `launching`), and a second form field whose options resolve to _the owners of the data product the requester just picked_ (`OWNERS_OF` from `field:selected_data_product`). The second resolver fires only when the first field is populated. The same `source` mechanism works for step actors.
