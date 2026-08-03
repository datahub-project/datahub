# Runtime Behavior Validation

DataHub gives agents the metadata they need to decide what matters: schema,
ownership, criticality, lineage, quality signals, and incidents. That context
does not, by itself, prove that a deployed API change altered runtime
behavior.

This guide describes an evidence-backed pattern for agents that must validate
behavior after a change. The agent uses DataHub to select and scope a workflow,
an external runner to execute the same workflow against two environments, and
the DataHub Incident API to record a finding only after the evidence has been
verified.

:::note

This is an integration pattern. It does not add a workflow runner, a custom
MCP server, or a new DataHub entity. The execution engine can be any tool that
produces comparable, structured traces.

:::

## When to use this pattern

Use runtime behavior validation when a metadata-only check is not enough to
answer the question:

> Did this change alter the behavior of a workflow that matters to our data
> consumers?

Schema and lineage analysis can identify a plausible blast radius. A live
comparison can confirm whether the changed environment actually took a
different route, even when both environments return HTTP 200.

This pattern is a good fit for:

- API or service changes where routing is hidden behind a stable response code.
- Contract checks that need to compare a baseline and a candidate deployment.
- Agents that should open an Incident only when a reproducible behavioral
  difference is present.
- Post-deploy checks that need to preserve the exact evidence used for a
  decision.

## Architecture

```text
DataHub Agent Context (MCP or Python SDK)
  -> target asset, schema, criticality, downstream lineage
  -> context-bound workflow selection

External workflow catalog and execution engine
  -> identical inputs against baseline and changed environments
  -> per-step response and route traces
  -> first divergent step and evidence fingerprint

Evidence gate
  -> DRIFT, NO_DRIFT, or INCONCLUSIVE

DataHub Incident API
  -> write only a verified DRIFT finding
  -> read the exact Incident back from the affected asset
```

DataHub owns the asset metadata and the resulting Incident. The workflow
definition and the system that executes requests remain an explicit external
boundary. Keeping that boundary visible prevents an agent from treating a
successful transport response as proof that the workflow behaved correctly.

## Prerequisites

- A DataHub instance and a [personal access token](../../authentication/personal-access-tokens.md).
- The [Agent Context Kit](./agent-context.md), connected through the DataHub
  MCP server or the Python SDK.
- A deterministic workflow runner that can execute the same inputs against a
  baseline and a changed environment and capture per-step traces.
- Permission to read the target asset's metadata and `Edit Incidents` on the
  asset that will receive a finding.
- A stable way to identify the workflow, its input fixture, and the two
  environment versions.

## 1. Bind the workflow to DataHub context

Start with the target asset, not with an arbitrary list of endpoints. Use the
Agent Context tools to build a small context snapshot that explains why the
workflow should be tested:

| Context                                         | Agent Context operation             | Use in the plan                                   |
| ----------------------------------------------- | ----------------------------------- | ------------------------------------------------- |
| Asset identity, owner, description, criticality | `get_entities`                      | Select the target and accountable owner           |
| Live schema and field descriptions              | `list_schema_fields`                | Identify fields that affect routing or assertions |
| Downstream and upstream dependencies            | `get_lineage`                       | Estimate impact and select related checks         |
| Existing incidents and quality signals          | `search` or the relevant asset APIs | Avoid duplicating an already-known finding        |

Persist the context snapshot or its digest alongside the execution result. A
plan should be explainable without replaying the agent conversation:

```json
{
  "target_urn": "urn:li:dataset:(urn:li:dataPlatform:openapi,checkout-service.orders.checkout,PROD)",
  "workflow_id": "checkout_paid_path",
  "context_source": "official-datahub-mcp",
  "reasons": [
    "criticality=tier_1",
    "workflow depends on the target asset",
    "downstream closure includes the retry path"
  ],
  "context_snapshot": "sha256:<context-digest>"
}
```

The workflow catalog is still external. DataHub supplies the facts that make
the selection accountable; it does not need to store every HTTP request or
test fixture.

## 2. Execute the identical workflow twice

Run the same workflow definition, inputs, seed data, and assertion set against
both environments. The environment pointer is the variable under test.

At minimum, capture these fields for every step:

- workflow ID and input fixture digest;
- step name and request correlation ID;
- response status and relevant response fields;
- the route or transition selected by the runner;
- assertion failures that explain why an edge was rejected;
- environment version or deployment identifier.

Do not collapse the result to a final HTTP status. A useful comparison keeps
the route evidence visible:

```json
{
  "workflow_id": "checkout_paid_path",
  "step": "checkout",
  "baseline": {
    "status": 200,
    "route": "poll_status"
  },
  "changed": {
    "status": 200,
    "route": "retry_queued",
    "rejected_edge": "body.status (expected exists: true, got <missing>)"
  }
}
```

The runner should produce a normalized trace before comparison. If the two
executions used different inputs, skipped a required step, or lost the route
trace, the result is not comparable and must be marked `INCONCLUSIVE`.

## 3. Gate the finding on structured evidence

Use three explicit verdicts:

| Verdict        | Meaning                                                      | Incident write-back        |
| -------------- | ------------------------------------------------------------ | -------------------------- |
| `NO_DRIFT`     | Comparable traces reached equivalent behavior                | Do not create an Incident  |
| `DRIFT`        | Comparable traces differ and identify a first divergent step | Eligible, after validation |
| `INCONCLUSIVE` | Inputs, traces, or environment evidence are incomplete       | Do not create an Incident  |

For `DRIFT`, require a non-empty first divergence and preserve the evidence
used to compute it. A compact evidence contract can look like this:

```json
{
  "verdict": "DRIFT",
  "kind": "routing_diverged",
  "root_step": "checkout",
  "baseline_route": "poll_status",
  "changed_route": "retry_queued",
  "baseline_status": 200,
  "changed_status": 200,
  "evidence_fingerprint": "sha256:<trace-digest>"
}
```

Before writing, validate all of the following:

1. The target asset URN came from the DataHub context snapshot.
2. Baseline and changed traces use the same workflow and input fixture.
3. The verdict is `DRIFT`, not merely a non-200 response.
4. `root_step`, both routes, and the assertion or transition evidence are
   present.
5. The evidence fingerprint is stable for a serial replay of the same result.

This gate keeps an agent from creating an Incident for a timeout, an
environment outage, or a guess about a schema change.

## 4. Write the verified finding to DataHub

Use the existing [Incidents API](../../api/tutorials/incidents.md) for the
mutation. The exact input can vary by incident type and DataHub version, but a
verified behavioral finding should preserve enough structured context for a
human or another agent to reproduce it:

```graphql
mutation RaiseBehaviorFinding($input: RaiseIncidentInput!) {
  raiseIncident(input: $input)
}
```

Example variables:

```json
{
  "input": {
    "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:openapi,checkout-service.orders.checkout,PROD)",
    "type": "CUSTOM",
    "customType": "runtime_behavior_drift",
    "title": "Checkout route diverged after deployment",
    "description": "{\"workflow_id\":\"checkout_paid_path\",\"verdict\":\"DRIFT\",\"root_step\":\"checkout\",\"baseline_route\":\"poll_status\",\"changed_route\":\"retry_queued\",\"evidence_fingerprint\":\"sha256:<trace-digest>\"}",
    "priority": "HIGH"
  }
}
```

Keep the description machine-readable, but make the title and status useful in
the DataHub UI. Include the impacted asset URNs in the description or in the
Incident fields supported by the target DataHub version. Do not claim that a
write succeeded until the response returns an Incident URN and the read-back
check below succeeds.

## 5. Verify the exact read-after-write

Read the Incident through the affected asset's `incidents` connection. The
[Incidents API tutorial](../../api/tutorials/incidents.md#get-incidents-for-data-asset)
shows the complete query; the essential check is:

```graphql
query ReadBehaviorFinding($urn: String!, $start: Int!, $count: Int!) {
  dataset(urn: $urn) {
    incidents(start: $start, count: $count) {
      total
      incidents {
        urn
        title
        description
        incidentType
        customType
        priority
        status {
          state
          stage
          message
        }
        entity {
          urn
        }
      }
    }
  }
}
```

The writer should paginate until it has examined the full result set and then
assert:

- exactly one Incident matches the finding identity;
- the returned Incident URN is the one returned by the mutation;
- the Incident is `ACTIVE` (or the state requested by the workflow);
- `workflow_id`, `root_step`, the route difference, and the evidence
  fingerprint match the finding that was written;
- the Incident entity is the changed asset.

If the generic MCP `get_entities` path does not expose Incident data in the
version you are using, record that as a capability result and use the
authoritative asset Incident connection instead. Do not turn an unsupported
MCP lookup into a false success.

## Replay and idempotency boundary

For serial retries, derive a stable finding identity from the changed asset
URN, workflow ID, and evidence fingerprint. Look up an exact match before
raising a new Incident. If one exists, update or return it according to the
workflow policy, then run the same read-back assertions.

This is serial replay protection, not a concurrent exactly-once guarantee. A
caller-selected Incident URN or an atomic source-identity field is required to
make concurrent writers safe. Keep that limitation visible in the proof and
do not describe a lookup-before-create sequence as a distributed lock.

## Worked example

Suppose a checkout workflow uses the `status` field to decide whether to poll
or enqueue a retry. The baseline and changed deployments both return HTTP
200, but the traces show:

| Step           | Baseline                | Changed                    |
| -------------- | ----------------------- | -------------------------- |
| `checkout`     | routes to `poll_status` | routes to `retry_queued`   |
| `poll_status`  | executes                | missing from changed trace |
| `retry_queued` | absent                  | executes                   |

The evidence gate reports `DRIFT`, identifies `checkout` as the first
divergent step, and records the failed `body.status` assertion that caused the
route change. Only then does the agent raise a high-priority custom Incident
and verify it through `dataset.incidents`.

The result is materially stronger than "the endpoint returned 200": it links
the live behavior difference to a DataHub asset, its downstream impact, the
exact workflow, and the evidence that a reviewer can inspect.

## Reference implementation

The [DataHub Causality Agent](https://github.com/Yatsuiii/datahub-causality-agent)
is a deterministic reference implementation of this pattern. It uses the
official DataHub MCP server for context, an external API execution engine for
the baseline/changed comparison, and the native Incident API for verified
write-back.

The repository's portable proof contains the corresponding artifacts:

- `plan.json` for the DataHub-derived workflow selection;
- `mcp-tool-calls.json` for the context calls and result hashes;
- `ace-diff.json` for the structured route divergence;
- `datahub-readback.json` for the exact Incident read-back;
- `proof-summary.json` for the compatibility and replay status.

To validate the portable proof bundle without a DataHub deployment or the
external execution engine, run:

```bash
make verify-sample
```

The external engine is optional and is not part of DataHub's build or runtime
dependencies. The guide's contract can be implemented with another runner as
long as it produces comparable traces and preserves the same evidence gates.

## Common failure modes

| Symptom                                                             | Correct response                                                                  |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Both environments return HTTP 200                                   | Compare route and assertion evidence; status alone is insufficient                |
| A schema changed but the workflow did not execute the affected step | Mark `INCONCLUSIVE`; do not write an Incident                                     |
| Baseline and changed runs used different fixtures                   | Reject the comparison and rerun with one immutable fixture                        |
| The same finding appears more than once                             | Stop and surface the duplicate; do not silently pick one                          |
| MCP Incident lookup returns no entity                               | Record the released-stack capability result and use the asset Incident connection |
| Read-after-write returns a different fingerprint                    | Treat the write as unverified and investigate before reporting success            |

## Boundaries

This guide intentionally does not define:

- a new DataHub MCP tool or Incident entity;
- a workflow DSL or a hosted execution service;
- concurrent Incident idempotency guarantees;
- a mandatory LLM layer;
- a DataHub dependency on ACE or any other external runner.

Those are separate design and implementation decisions. Keeping them outside
the pattern makes the evidence contract portable and keeps DataHub as the
metadata and Incident system of record.

**Links:** [Agent Context Kit](./agent-context.md) | [DataHub MCP Server](../../features/feature-guides/mcp.md) | [Incidents API](../../api/tutorials/incidents.md) | [Lineage](../../features/feature-guides/lineage.md)
