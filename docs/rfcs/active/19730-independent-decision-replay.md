- Start Date: 2026-09-11
- RFC PR: [#19730](https://github.com/datahub-project/datahub/pull/19730)
- Discussion Issue: None; this PR starts the design discussion.
- Implementation PR(s): None; the external reference implementation is linked below.

# Independently replayable agent decisions

## Summary

Define an optional evidence contract for external agents that write decisions back to DataHub. A decision carries the policy identity and the inputs needed to recompute its result without importing the producer's implementation or contacting DataHub.

Replay answers whether a recorded decision follows from the supplied evidence under an independently trusted policy. It does not establish that the evidence is genuine, current, or complete relative to the real world. Those are separate checks.

## Basic example

A change-review agent records `BLOCK` because a column removal reaches consumers owned by another team. A separate reader receives the evidence artifact and its expected policy bundle, re-evaluates the relevant rules, and either reproduces `BLOCK` or reports a mismatch.

Illustrative envelope fields, pending agreement on the schema:

```json
{
  "profile": "external-decision/v1",
  "subject": "urn:li:dataset:(urn:li:dataPlatform:postgres,example.public.orders,PROD)",
  "action": "remove order_total",
  "policy": {
    "profile": "example-change-policy/v1",
    "sha256": "<digest of the trusted policy bytes>"
  },
  "decision": "BLOCK",
  "evidence": [
    {
      "kind": "downstream_ownership",
      "owners": ["urn:li:corpGroup:finance", "urn:li:corpGroup:operations"]
    }
  ],
  "coverage": {
    "required": ["downstream_ownership"],
    "established": ["downstream_ownership"],
    "unestablished": []
  }
}
```

The placeholder digest is deliberately not a valid production artifact. An actual artifact also identifies the exact requested action or code revision, observation time, context snapshot, and evidence schema versions required by its profile.

## Motivation

Writing a verdict back makes knowledge available to another agent, but a verdict alone is still a claim. Calling the same producer function again from another process provides process separation without independent implementation of the judgment.

DataHub need not evaluate every external agent's policy. It can provide a convention that makes those decisions inspectable and replayable, while leaving domain policy ownership with the producer and its users.

In [Sidq's prototype](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/scripts/rederive.sh), a shell and jq implementation independently recomputes the Python engine's verdict from committed artifacts. The portable contribution is the evidence and policy boundary, rather than importing Sidq's rule set into DataHub.

## Requirements

- Identify the subject, requested action, policy, evidence schema, and applicable context.
- Pin the expected policy outside the untrusted decision artifact.
- Include every input the applicable policy requires, including explicit coverage gaps.
- Make replay deterministic and possible without the producing engine or a running catalog.
- Treat unsupported profiles, missing inputs, and evaluation errors as unavailable verification.
- Keep authentication, replay consistency, freshness, and authorization as separate outcomes.

### Extensibility

Profiles define evidence types, permitted decision values, aggregation rules, and failure semantics. A consumer supports profiles explicitly. Profiles must be versioned when their meaning changes; replay must not silently evaluate an old artifact using a newly changed policy.

## Non-Requirements

- A universal policy engine inside DataHub.
- A claim that an offline replay proves current catalog or source-system truth.
- Automatic authorization of metadata or data writes.
- Fetching or executing code named by the artifact.

## Detailed design

### Policy identity and trust

Hash the exact distributed policy bytes. If an evaluator consumes a compiled policy representation, bind that representation to the same trusted bundle and test the source-to-compiled equivalence. Hashing only a source file while accepting an unrelated compiled representation leaves a substitution gap.

The expected digest and evaluator profile come from consumer configuration or another independently trusted distribution channel. Accepting a policy and its matching digest from the same untrusted artifact only establishes self-consistency. Neither an artifact-provided URL nor a catalog property may select arbitrary executable code.

### Evidence completeness

Each policy profile declares its required inputs and how observations map to rule inputs. The artifact records relevant observations, their scope, and checks that could not complete. A missing input cannot be silently treated as an empty list or a negative finding unless the profile explicitly establishes that equivalence.

For example, absence of downstream consumers is usable evidence only when lineage retrieval for the declared scope completed. A failed or truncated lookup does not prove that a proposed column removal has no consumers.

Completeness here is relative to the declared profile and snapshot. A malicious producer can still fabricate evidence or omit facts that the profile does not require; independent replay does not solve that authenticity problem.

### Independent evaluation

The producer and replay implementation must separately implement the documented decision semantics. Their contract includes type comparisons, numeric handling, rule ordering, aggregation, and evaluation failures. Avoid calling the original engine from the verifier or copying its decision into the expected result.

A concrete interoperability trap appeared in the prototype: Python rejects an ordered comparison between a number and a string, while jq defines an ordering across types. The independent implementation must reproduce the policy's declared failure behavior rather than return an answer where the first implementation refuses to evaluate.

### Result contract

| Replay result  | Meaning                                                                                      |
| -------------- | -------------------------------------------------------------------------------------------- |
| `MATCH`        | The recorded decision agrees with the supplied inputs under the trusted policy               |
| `MISMATCH`     | The policy can be evaluated, but its decision differs from the recorded decision             |
| `UNVERIFIABLE` | Missing inputs, unknown profile, policy mismatch, or evaluation failure prevented comparison |

`MATCH` is not `PASS`. A correctly derived `BLOCK` is a successful replay of a refusal. Rechecking whether a historical decision is still applicable requires current context and age checks, which may require catalog or source access and are outside offline replay.

### Storage and transport

Keep a compact summary and evidence identity in DataHub, with a human-readable evidence document or an external artifact where size requires it. The document must explain the observation scope and verification boundary. Artifact retrieval is bounded by the consumer's configured size, origin, and data-handling policy.

Only approved metadata and evidence may be published. Row-level values, credentials, and internal queries should not be copied into a broadly readable document merely to make a decision replayable. A profile can use counts or redacted aggregates when those are sufficient inputs.

### Reference implementation and acceptance tests

The [jq evaluator](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/scripts/rederive.jq), [policy export](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/scripts/policy_to_json.py), and [agreement tests](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/tests/test_independent_rederivation.py) are executable prior art. On that pinned checkout, `scripts/rederive.sh examples/01-blocked-pii-dashboard/verdict.json` reproduces the recorded refusal without Python or DataHub.

The prototype uses its own artifact schema; it is not an implementation of the illustrative envelope above. Its policy is trusted through the checkout. Standardizing a general transport envelope and profile registry remains proposed work.

Acceptance cases must include all declared decisions, an altered verdict, a substituted policy, missing required evidence, incomplete discovery, unknown profiles, malformed types, empty findings, and stale-but-internally-consistent artifacts. The agreement suite must compare independently implemented decisions rather than two wrappers around one implementation.

## How we teach this

Document the pattern alongside external assertion and agent writeback integrations. A tutorial should start with a correct blocking decision, deliberately alter that decision, and show the independent reader detect the mismatch. State explicitly that replay is a consistency check, with separate examples for authenticated provenance and current applicability.

## Drawbacks

Maintaining two implementations costs effort and can expose semantic ambiguities. Evidence bundles can be large or sensitive. A successful replay may create false confidence if its limited claim is presented as verification of real-world truth. External policy profiles may fragment without a small, documented compatibility contract.

## Alternatives

- Re-run the original engine: useful operational checking, but retains common implementation defects.
- Sign the verdict: authenticates a producer without independently validating its computation.
- Store only a human-readable explanation: easier to publish, but cannot automatically detect a changed decision.
- Build a universal engine in DataHub: a much larger scope with domain-policy ownership and compatibility costs.

## Rollout / Adoption Strategy

Agree first on the minimal envelope and verification claims. Publish one external profile with two implementations and shared vectors before proposing SDK discovery or UI support. Existing integrations keep their current behavior. Adoption must not convert a replay `MATCH` into automatic permission to perform a write.

## Future Work

Optional SDK profile discovery, artifact-size controls, and UI links that expose the replay result separately from the underlying business decision.

## Unresolved questions

- Which fields belong in a common envelope versus individual policy profiles?
- Should DataHub host replay profiles or only link to externally maintained profiles?
- What artifact-size and retention limits should first-party examples demonstrate?
- How should a document identify a trusted compiled policy without duplicating its full contents?
