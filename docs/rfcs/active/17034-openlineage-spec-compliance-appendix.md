# Appendix: specification coverage and feedback to upstream PRs

Companion to [RFC #17034](./17034-openlineage-spec-compliance.md). Implementation: [PR #19257](https://github.com/datahub-project/datahub/pull/19257), [feat/openlineage-conformance-combined](https://github.com/manuschillerdev/datahub/tree/feat/openlineage-conformance-combined).

## Findings and evidence

| Document                                                                                                                | Question answered                                                                                                                    |
| ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| [Feedback to DataHub](./17034-openlineage-evidence/openlineage-pr-feedback-2026-10-01.md)                               | What do the six PRs add over master, and what bugs, incomplete mappings, or verification gaps remain?                                |
| [72-contract inventory](./17034-openlineage-evidence/openlineage-153-contract-inventory-2026-09-30.md)                  | Which event, state, identity, update, deletion, and facet contracts are in the 1.53 review scope?                                    |
| [Core and semantic assessment](./17034-openlineage-evidence/openlineage-153-core-semantic-target-audit-2026-10-01.md)   | Where do event/state semantics, accumulation, replacement, omission, tombstones, explicit-lineage precedence, and identities differ? |
| [Facet and attachment assessment](./17034-openlineage-evidence/openlineage-153-facet-target-audit-2026-10-01.md)        | Which facets are mapped, retained, ignored, or rejected at each target and attachment?                                               |
| [Independent PR provenance](./17034-openlineage-evidence/openlineage-153-independent-pr-facet-provenance-2026-10-01.md) | Which of the 27 other pinned PR heads affect this receiver route, and which still require their own checks?                          |
| [Six specification-gap dossiers](./17034-openlineage-evidence/openlineage-153-spec-gaps-pr-stack-2026-09-29.md)         | What valid multi-event payloads, source traces, readback checks, and user consequences support the main specification feedback?      |
| [34 known-gap cases](./17034-openlineage-spec-compliance-known-gap-appendix.md)                                         | Is each known bug/support gap on master, the PR stack, or ours, and how is it reproduced?                                            |
| [Captured payloads](./17034-openlineage-evidence/openlineage-known-bug-evidence/README.md)                              | Which ordered requests were executed, captured from tests, or reconstructed without live execution?                                  |
| [Local verification checkpoint](./17034-openlineage-evidence/openlineage-goal-progress-2026-09-30.md)                   | Which exact suites and persisted/read-API probes actually ran September 30–October 1?                                                |
| [Publication manifest](./17034-openlineage-evidence/publication-manifest.json)                                          | Which source blobs and artifact hashes identify the evidence published here?                                                         |

## Pinned comparison boundary

The [October 1 snapshot](./17034-openlineage-evidence/openlineage-goal-2026-10-01.snapshot.json) pins master `318467eb125a281614c27de44f37b3f76695a0fd`, all 33 PR heads, and the dirty local source digest. The six cumulative heads are #18175 → #19733 → #19734 → #19736 → #19740 → #19800. Independent PR capabilities are assessed separately and are not combined into this stack.

Upstream verdicts are source assessments: no matching master/PR-head receiver was deployed. Only the cited local runtime artifacts establish observed persisted results. Local source captures linked from the facet matrix reproduce the Git blob identities recorded in the snapshot. A source mapping, an HTTP acceptance response, and persisted/native readback have different proof strength.

The inventory contains 40 official facet definitions from 38 schema files and six bundled GCP/Iceberg registry definitions. Registry-extension coverage is a product compatibility question. Lack of a native mapping is not automatically a wire-protocol violation. Explicitly retained-only facets must be distinguished from accepted-but-discarded data, and a native projection claimed as supported must honor its update/delete contract.

## Remaining specification feedback

The PR feedback includes partial RunEvent I/O loss; COMPLETE_SNAPSHOT scope; same-name facet replacement, omitted-facet preservation, and typed tombstones; explicit lineage and its precedence; DatasetEvent lifecycle/column/explicit lineage parity; field-reference and `int64` mapping; stable identities; missing facet retention; and inherited parent identity errors. Each linked dossier states what users lose in native aspects, operation history, lineage navigation, or read APIs.

The report also identifies unresolved local limitations and verification work. It does not claim full conformance or that every row is a reproduced upstream bug ticket. Failure atomicity, universal eventTime arbitration, rename entity migration, exact raw archival, and receiver redaction are not invented specification requirements.
