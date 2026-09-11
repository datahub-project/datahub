- Start Date: 2026-09-11
- RFC PR: Pending
- Discussion Issue: None; this PR starts the design discussion.
- Implementation PR(s): None; the external prototype is linked below.

# Explicit coverage for resumable catalog audits

## Summary

Define a convention for external catalog auditors to record checks that could not establish a result, and use that state when planning a later bounded audit. Reuse custom assertions and existing `SUCCESS`, `FAILURE`, and `ERROR` result types. An absent check, a completed refusal, and an unsuccessful attempt remain distinct facts.

This is an application convention and proposed SDK example, not a new scheduler, lease service, or exactly-once execution guarantee.

## Basic example

An auditor has a budget for two dataset examinations. One dataset passes, another cannot be examined because its source is unavailable, and a third is not reached.

| Dataset                    | Observation                          | Catalog result | Next-run interpretation                                 |
| -------------------------- | ------------------------------------ | -------------- | ------------------------------------------------------- |
| `example.public.orders`    | Required checks completed and passed | `SUCCESS`      | Covered only while context and policy remain applicable |
| `example.public.customers` | Source could not be reached          | `ERROR`        | Attempted, not established; still requires examination  |
| `example.public.products`  | Budget ended before examination      | No new result  | Unexamined; no claim about quality                      |

A subsequent check that completes and finds a violation records `FAILURE`, not `ERROR`. It is established coverage of a refusal, not permission to use the dataset. A later completed result on the same stable assertion identity supersedes the current gap while preserving available run history.

## Motivation

An auditor that remembers only successful checks spends later budgets inefficiently and hides why coverage is incomplete. A catalog that shows the same blank for an unexamined asset and a failed examination also leaves operators unable to distinguish missing monitoring from an unavailable dependency.

DataHub's assertion model already represents this distinction. The proposed contribution is a precise producer/consumer convention, including freshness, identity, and retry behavior, with an example that works across runs and workers.

The motivation comes from [Sidq's coverage-aware audit planning](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/src/sidq/agent/memory.py). This RFC generalizes the state semantics rather than adding Sidq's policy engine to DataHub.

## Requirements

- Record an attempted but unestablished check as `ERROR`, not success or an ordinary data violation.
- Keep the target inventory, required check set, and examined subset explicit.
- Reuse stable assertion identities so a subsequent established result supersedes the current gap.
- Revalidate policy, context, and age before allowing a previous result to save work.
- Preserve the meaning of an established refusal when counting coverage.
- Keep business consequence ahead of historical ease of examination in planning.
- Report degraded state reads and rejected writes rather than inventing successful coverage.

### Extensibility

The convention must support different external check producers and different budget units. Producers declare whether their budget counts assets, checks, source queries, or API calls. It must not imply a globally enforced shared budget across independent workers.

## Non-Requirements

- Exactly-once execution, compare-and-set, leases, or distributed locks.
- A durable audit ledger beyond the configured retention of assertion run events.
- Automatic mutation of source data, remediation, or changes to native monitor scheduling.
- Cryptographic authentication of a producer; applications needing it must add a separate provenance requirement.

## Detailed design

### Stable identity and result vocabulary

Derive an assertion identity from the producer namespace, exact subject URN, and logical check identifier using an unambiguous, versioned encoding. Do not include the run identifier in the assertion identity; that would create a new assertion on every retry. Store the run identifier with the result instead.

Use the existing custom-assertion registration and result-reporting interfaces. A producer can report an unestablished attempt with the existing SDK:

```python
client.assertions.report_assertion_result(
    urn=assertion_urn,
    timestamp_millis=observed_at_millis,
    type="ERROR",
    properties=[
        {"key": "coverage_reason", "value": "source_unavailable"},
        {"key": "producer_run_id", "value": run_id},
        {"key": "policy_identity", "value": policy_identity},
    ],
)
```

The assertion must already be registered against the intended dataset. The example's variables are application inputs, not newly added SDK fields. A successful return is a write acknowledgement; a producer requiring persistence evidence reads the result back and compares its identity and contents.

`INIT` or missing run events do not establish a result. A latest `ERROR` must not be concealed by an older `SUCCESS`. `FAILURE` means a completed check found a problem; callers must not merge it with `ERROR` merely because both prevent a positive assurance.

### Coverage accounting

For each run, identify the target inventory and applicable checks. A paginated or failed inventory read must be reported as partial; it cannot establish that the complete catalog was covered. Record separately:

- Established in this run, including completed refusals.
- Covered by a prior result that was revalidated for this run.
- Attempted but not established.
- Not attempted within the declared budget.

Avoid emitting an `ERROR` for every asset that a run never reached: there was no failed examination to report. The run's inventory and accounting identify that unexamined tail. An earlier success for one check also cannot cover another required check that has never run.

### Applicability and retirement

A prior result saves examination only when its subject, required check set, policy identity, relevant context, and age still apply. Define which catalog and source observations are part of that context; exclude the producer's own output fields so publishing a result does not immediately invalidate it.

For a stable logical check, report later `SUCCESS`, `FAILURE`, or `ERROR` under the same assertion identity. The latest applicable result determines current coverage. Do not clear a gap merely because a worker started or because a write was attempted.

If a producer uses a separate aggregate coverage assertion, it must specify and test how a later established check retires that assertion. That additional lifecycle is not required by the minimal convention.

### Planning under a bounded budget

Compute business consequence independently of previous examination outcomes. Among assets with equal consequence, a producer may prefer a never-examined asset over an asset whose last attempt could not complete. Use explicit retry limits or age-based tie breaking to avoid permanently starving the latter.

For example, an unavailable high-consequence customer dataset remains ahead of a lower-consequence lookup table. Historical difficulty is not evidence that an asset is safe or unimportant. The budget planner chooses where to look; it does not learn or weaken the policy that decides the result.

Any cost of reading prior state, checking applicability, and writing results must be included or separately disclosed in the budget definition. Do not present a limit on source examinations as a limit on total network calls.

### Concurrency and failure behavior

Workers re-read current applicable state before examining a candidate and record their result after examination. This narrows duplicate work but does not eliminate races. Two workers may examine the same asset; a worker may terminate before publication; different runs may observe different source contexts.

Never infer exactly-once execution or a complete worker history from a latest-value view. Include run and worker attribution for diagnostics, but do not count overwritten latest values as a durable execution ledger. If state cannot be read, prior coverage is unknown and the planner must disclose the degraded mode. If results cannot be stored, they remain local observations and must not be reported as shared coverage.

Only static, bounded reason codes and appropriately sanitized evidence should be exposed in broadly readable assertion properties. Source connection strings and raw exception text can contain secrets.

### Prior implementation and acceptance tests

The [offline planning demonstration](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/demos/gap_order.py), [budget regressions](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/tests/test_agent_gap_budget.py), and [assertion implementation](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/src/sidq/receipt/assertion.py) are executable prior art. The prototype uses an aggregate gap assertion and retirement sweep; this RFC's preferred per-check identity simplifies that lifecycle and remains a proposal.

A DataHub example should test partial inventory, an unexamined asset, source failure, rejected writeback, a stale success, a current refusal, policy changes, gap-to-success and gap-to-failure transitions, and equal-consequence retries. Concurrency tests should establish safe duplicate work and recoverable unpublished work without asserting exactly-once completion.

## How we teach this

Add an external-auditor example and quality-report guidance that displays coverage alongside outcomes. Show `ERROR`, `FAILURE`, and no result in the first example. Reports must name their scope and distinguish observations made now from applicable prior observations.

## Drawbacks

Additional assertion events increase storage and UI noise. Context revalidation consumes budget and may approach the cost of repeating a cheap check. Stable identity and policy evolution need care. A poorly chosen retry rule can starve persistent errors or repeatedly consume the whole budget on unavailable sources.

## Alternatives

- Keep all scheduler state externally: stronger coordination options, but another state store and less catalog visibility.
- Record only successes: simpler, but hides failed examinations and repeats incomplete work without context.
- Record gaps as ordinary failures: visible, but conflates a data violation with an inability to determine one.
- Add a dedicated coverage aspect immediately: more expressive, with greater model, indexing, and UI cost than first testing an existing-assertion convention.

## Rollout / Adoption Strategy

Start with an opt-in external-auditor example using existing custom assertions. Validate lifecycle and report semantics before proposing a common SDK planner. Native assertion schedules and current quality summaries keep their existing behavior. Any later first-party coverage UI or model changes should receive separate implementation review.

## Future Work

Optional coverage summaries, retry-policy helpers, and a documented path to stronger coordination when an application requires leases or a monotonic execution ledger.

## Unresolved questions

- Which fields need common names versus producer-specific properties?
- Should an SDK helper derive stable assertion identities, or should producers own that mapping?
- What retention and cardinality guidance is appropriate for per-check coverage events?
- How should a quality report display incomplete inventory without implying that every undiscovered asset is unhealthy?
