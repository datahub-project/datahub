- Start Date: 2026-08-07
- RFC PR: https://github.com/datahub-project/datahub/pull/18966
- Discussion Issue: none
- Implementation PR(s): none

# Agent Execution Evidence and Version-Level Verification

## Summary

Add a bounded, provenance-aware representation of observed agent execution evidence to
DataHub so that a governed agent version can be compared with what a real run actually
did. The proposal separates high-volume raw traces from a compact verification result in
the metadata graph.

The graph should be able to answer:

- Which exact agent version produced this evidence?
- Which tools and governed assets did the run directly use?
- Was the evidence real, synthetic, or fixture data?
- Did observed behavior satisfy the version's declared capability contract?
- Which deterministic findings and human-approved governance actions followed?

This proposal builds on DataHub's Agent Registry concepts and the Agent and AgentTool
metadata model proposed in [datahub#16012](https://github.com/datahub-project/datahub/pull/16012).
It does not replace agent, tool, dataset, lineage, incident, assertion, or tracing entities.

## Basic example

The following is illustrative JSON, not a final generated schema:

```json
{
  "runId": "run-revenue-analyst-1.3-a1b2c3d4e5f6",
  "agent": "urn:li:agent:revenue-analyst",
  "agentVersion": "1.3",
  "startedAt": 1785876105000,
  "endedAt": 1785876107600,
  "sourceKind": "REAL",
  "executionMode": "SCRIPTED",
  "integrationSurfaces": ["AGENT_CONTEXT_KIT", "SDK", "DUCKDB"],
  "observedTools": [
    "urn:li:api:(datahub-agent-context,get_entities)",
    "urn:li:api:(datahub-agent-context,get_lineage)",
    "urn:li:api:(datahub-sdk,entities.update)"
  ],
  "directDatasetReads": [
    "urn:li:dataset:(urn:li:dataPlatform:duckdb,finance.legacy_revenue,PROD)"
  ],
  "evidenceDigest": {
    "algorithm": "SHA256",
    "value": "..."
  },
  "traceUri": "s3://governed-evidence/runs/...",
  "verification": {
    "policyVersion": "1.0.0",
    "decision": "NEEDS_REVIEW",
    "findingCounts": {"HIGH": 2},
    "evaluatedAt": 1785876110000
  }
}
```

## Motivation

DataHub can govern what an agent is declared to be: its version, owner, skills, tools,
and data relationships. A deployment can still drift after that declaration is recorded.
For example, an agent may claim to be read-only and canonical-only while a current run
invokes a write tool and queries a deprecated table.

Ordinary runtime logs can show that a tool or URN appeared. They cannot, by themselves,
establish that the asset is deprecated, identify its governed replacement, or calculate
the impact of bypassing an active downstream reporting path. Conversely, a registry
declaration cannot prove that a particular deployed version honored its contract.

The missing primitive is a compact bridge between declared agent metadata, observed
execution, governed context, and a reviewable decision.

## Requirements

- Bind every evidence record to an exact agent version and run identifier.
- Preserve provenance, observation time, source kind, and integration surface.
- Distinguish direct data use from assets merely returned as lineage context.
- Reference existing agent, API/tool, dataset, and owner URNs instead of duplicating them.
- Support deterministic, versioned verification policies and explicit abstention states.
- Permit `UNVERIFIED` and `UNKNOWN`; absence of evidence must never imply safe behavior.
- Keep raw prompts, credentials, full query results, and sensitive tool payloads out of the
  metadata graph by default.
- Make high-cardinality trace retention configurable and bounded.
- Support human-approved remediation with actor, timestamp, exact target, and read-after-
  write verification.
- Work for external agents and multiple tracing frameworks.

### Extensibility

- New source kinds and integration surfaces must be additive enum values.
- Verification findings should accept namespaced policy identifiers rather than a closed
  global detector list.
- The model should support signed digests later without requiring raw trace ingestion.
- Tool and dataset observations should allow additional typed attributes while retaining
  stable URN relationships.
- A future adapter should be able to translate OpenTelemetry, A2A, MCP, LangChain, or
  provider-native events into the same normalized evidence contract.

## Non-Requirements

- Storing every token, prompt, response, or tool result in DataHub.
- Replacing a tracing backend, SIEM, workflow engine, or model observability product.
- Letting an LLM assign risk severity or approve its own remediation.
- Defining a universal agent safety benchmark.
- Requiring native DataHub-managed agents; external agents remain first-class inputs.
- Designing the final Agent Registry UI in this RFC.

## Detailed design

### 1. Separate raw execution from graph summary

Raw traces remain in the producer's governed evidence store. DataHub receives a bounded
summary plus a digest and optional URI. This prevents unbounded event volume and reduces
the chance that prompts, credentials, or business results enter the metadata graph.

The summary may use an existing run-oriented entity such as `DataProcessInstance` if the
maintainers consider its lifecycle and indexing semantics appropriate. Otherwise, add a
dedicated `AgentExecution` entity keyed by `(agent-version URN, run ID)`.

### 2. Proposed `agentExecutionEvidence` aspect

The aspect contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `agent` | Agent URN | Governed agent identity that ran |
| `agentVersion` | string | Exact producer-stable version that ran |
| `runId` | string | Producer-stable execution identifier |
| `startedAt`, `endedAt` | timestamp | Execution window |
| `sourceKind` | enum | `REAL`, `SYNTHETIC`, or `FIXTURE` |
| `executionMode` | string | Producer-declared mode, e.g. model-backed or scripted |
| `integrationSurfaces` | string array | MCP, Agent Context Kit, SDK, etc. |
| `observedTools` | API URN array | Tools that completed successfully |
| `directDatasetReads` | Dataset URN array | Assets directly used by the task |
| `mutations` | bounded record array | Redacted target, tool, executed flag, and result |
| `evidenceDigest` | algorithm + value | Integrity identifier for canonical evidence |
| `traceUri` | optional URI | Access-controlled raw evidence location |
| `producer` | string | Adapter name and version |

Relationships should use DataHub's normal foreign-key annotations so tools and data can
participate in impact analysis. A lineage query result must not automatically become a
direct data read; adapters must label the primary input separately from context nodes.

### 3. Proposed `agentVerificationResult` aspect

Verification is a derived, replaceable view over execution evidence:

| Field | Type | Meaning |
| --- | --- | --- |
| `run` | execution URN | Evidence being evaluated |
| `policyId`, `policyVersion` | string | Reproducible decision logic |
| `decision` | enum | `VERIFIED`, `NEEDS_REVIEW`, `UNVERIFIED`, `UNKNOWN` |
| `findingCounts` | map | Counts by severity |
| `findingRefs` | URI/URN array | Bounded references to detailed findings |
| `evaluatedAt` | timestamp | Decision time |
| `evaluator` | string | Deterministic engine and version |

An LLM may select tools or explain normalized findings, but the stored decision must name
the deterministic policy version that produced it. Model-generated text is an optional
annotation, not the authority for severity.

### 4. Lineage-aware risk

Lineage is a decision input, not presentation decoration. One example rule is:

1. A run directly reads a deprecated dataset.
2. DataHub identifies a governed replacement.
3. The replacement has an active downstream report.
4. The deprecated-access finding escalates from `MEDIUM` to `HIGH` because the run bypassed
   a live governed path.

The finding stores the replacement URN, impacted downstream URNs, direction, hop bound,
observation time, and lineage evidence reference. If lineage is unavailable, the engine
must retain the base risk and disclose that no impact escalation was verified.

### 5. Human-approved governance action

An optional governance-action record references the verification result and captures:

- exact allowlisted target URN and operation;
- proposed payload digest;
- requester and approver identities;
- timestamps and state (`PENDING`, `APPROVED`, `APPLIED`, `FAILED`, `REJECTED`);
- read-after-write verification outcome;
- rollback guidance.

An agent must not approve its own action. Preparing a proposal performs no mutation.

### 6. Retention and cardinality

Recommended defaults:

- store raw traces outside DataHub;
- retain the latest N execution summaries per agent version or a time-bounded window;
- preserve promoted verification results and governance actions longer;
- index decision, source kind, tool URNs, direct dataset URNs, and time fields;
- cap inline mutation and finding summaries, linking to detailed external evidence.

## How we teach this

Use the phrase **declared behavior versus observed behavior**. Agent Registry answers what
an agent version is supposed to use; execution evidence records what a run actually used;
verification explains whether those two align in governed context.

Documentation should lead with a contradiction, not a model diagram:

> This agent claims to be read-only and canonical-only. A current run writes metadata and
> queries a deprecated table. DataHub evidence shows the discrepancy and its impact.

The primary audiences are agent platform teams, data governance teams, metadata model
contributors, and integration authors.

## Drawbacks

- Run-level entities can create high cardinality and indexing cost.
- Producer-controlled source labels require trust policy and possibly signatures.
- Direct-use versus context-only classification is framework-specific and can be wrong.
- Policy results can become stale as metadata, lineage, and incidents change.
- A trace URI can expose sensitive data if access controls are weaker than DataHub's.
- Adding a new entity or aspect increases SDK, ingestion, GraphQL, and UI surface area.

## Alternatives

1. **Only store a tag or incident on the agent.** Simple, but loses run, version, evidence,
   policy, and provenance detail.
2. **Store raw trace JSON in a Document.** Easy to prototype, but weakly typed, difficult to
   query, and risky for secrets and cardinality.
3. **Use structured properties only.** Useful for experimentation, but relationships and
   versioned execution semantics deserve a typed model if the pattern stabilizes.
4. **Keep all evidence outside DataHub.** Preserves tracing-system boundaries but prevents
   governed metadata and lineage from participating in the decision graph.
5. **Attach only a latest status aspect to the agent version.** Low cardinality, but loses
   auditability. This can be a materialized summary in addition to run evidence.

## Rollout / Adoption Strategy

1. Validate terminology and entity choice with Agent Registry and metadata-model owners.
2. Ship a producer-side normalization library and JSON schema before a new server model.
3. Pilot with structured properties or Documents on external agents.
4. Add one read API for latest verification by agent version.
5. Add optional run ingestion with conservative retention defaults.
6. Add UI only after real adapters demonstrate useful queries and acceptable cardinality.

The rollout is additive. Existing Agent Registry entities, lineage, and external tracing
systems continue to work unchanged.

## Future Work

- Signed evidence digests and workload identity.
- Incident/assertion integration for failed verification.
- DataHub Actions triggered by verification state changes.
- A2A and OpenTelemetry import adapters.
- Fleet-level drift trends and policy coverage.
- Time-aware re-evaluation when governed metadata or lineage changes.

## Unresolved questions

- Reuse `DataProcessInstance` or introduce `AgentExecution`?
- Is a latest-verification aspect sufficient for the first release?
- Which relationships should be indexed versus kept in a bounded summary?
- How should retention interact with DataHub soft deletion and time-based lineage?
- Should governance actions be represented as incidents, assertions, documents, or a new
  typed aspect?
- Which producer attestations are required before `sourceKind=REAL` is trusted?

## References

- [DataHub Agent Registry](https://docs.datahub.com/docs/features/feature-guides/agent-registry)
- [DataHub Agents metadata model RFC](https://github.com/datahub-project/datahub/pull/16012)
- [DataHub Metadata Standard](https://github.com/datahub-project/datahub/blob/master/docs/metadata-standards.md)
- [DataHub RFC template](https://github.com/datahub-project/rfcs/blob/main/templates/000-template.md)
- [DataHub lineage API tutorial](https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/lineage.md)
