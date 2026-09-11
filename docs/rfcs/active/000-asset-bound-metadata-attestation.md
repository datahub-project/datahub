- Start Date: 2026-09-11
- RFC PR: Pending
- Discussion Issue: None; this PR starts the design discussion.
- Implementation PR(s): None; the linked external prototype is prior art, not a DataHub implementation.

# Asset-bound attestation for external metadata signals

## Summary

Define an opt-in contract for authenticating a verification signal written into DataHub by an external tool. Bind the signal's complete body to its subject URN with an Ed25519 signature, and verify it using a key trusted independently of the catalog. A consumer requiring authenticated provenance must reject an unsigned signal as insufficient evidence.

This proposal does not change DataHub's authorization model. Permission to edit metadata and the ability to authenticate a particular producer's decision are different properties. Ordinary structured properties remain ordinary metadata.

## Basic example

An external auditor writes a decision about `urn:li:dataset:(urn:li:dataPlatform:postgres,example.public.orders,PROD)`. Its signed body includes that exact URN, the contract version, policy identity, observation time, context identity, and decision. A consumer verifies the signature before consulting the decision.

| Observation                                  | Attestation  | Consumer requiring provenance                      |
| -------------------------------------------- | ------------ | -------------------------------------------------- |
| Body and subject verify under a trusted key  | `SIGNED`     | Continue with separate policy and freshness checks |
| A present signature fails verification       | `TAMPERED`   | Do not authorize the proposed action               |
| No signature, or no trusted key is available | `UNATTESTED` | Do not authorize the proposed action               |

Moving a genuine signed signal from one dataset to another must fail. Removing a signature must not turn a failed verification into permission.

## Motivation

Agents increasingly consume properties such as `verified`, `certified`, or `reviewed_by` as trust signals. A reader that checks only their values cannot distinguish an approved producer's output from another metadata writer's assertion. A digest of publicly readable catalog context does not authenticate the producer: another writer can recompute it.

The need surfaced while building [Sidq's verification receipts](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/docs/RECEIPT-SPEC.md). The portable requirement is producer authentication at the point of consumption, without requiring DataHub to hold signing keys or execute an external policy engine.

## Requirements

- Bind the subject, all decision-bearing fields, and a versioned interpretation contract.
- Resolve trust from consumer configuration, not a public key supplied beside the metadata being checked.
- Distinguish missing proof from invalid proof; neither authorizes a provenance-required operation.
- Preserve ordinary metadata access controls and existing unsigned integrations.
- Specify canonical bytes and shared test vectors before standardizing an SDK helper.
- Keep signing keys out of metadata, URLs, logs, example fixtures, and public repositories.

### Extensibility

Use a versioned signing profile and key identifier. The identifier selects from an already trusted key set; it must not trigger arbitrary URL fetching. Additional algorithms require explicit profile versions and consumer support, rather than automatic algorithm negotiation from an untrusted record.

## Non-Requirements

- A signature does not prove that the producer's observations or policy are correct.
- It does not prove current freshness, prevent deletion, or provide an append-only audit ledger.
- This RFC does not add server-side key custody, a new aspect, automatic key discovery, or changes to native assertion evaluation.

## Detailed design

### Signed scope and storage

Start with a client-side contract for existing structured properties or an external evidence document. The contract declares the exact fields a consumer may use. Consumers must not verify one body and then read an unsigned `verified` flag elsewhere on the entity.

The signed payload contains a profile identifier, the exact subject URN, and the complete signal body. Required fields include producer identity, policy identity, observation time, and context identity. When a decision authorizes a particular action or code change, its action scope or commit identity is required too. The signature and key locator are carried separately from that body.

A producer computes the signature only after finalizing the complete body, including evidence-document references. It then reads the persisted values back and verifies those values. A mutation acknowledgement alone is not readback evidence. If publication involves several non-atomic writes, intermediate states must fail verification; readers must never combine fields from two versions into an accepted decision.

### Canonicalization profile

The proposed first profile uses UTF-8 JSON with sorted object keys, no insignificant whitespace, and string-valued metadata. It excludes the signature field. Unordered multi-value properties are sorted by UTF-8 byte order, with multiplicity preserved. Empty property lists and absent properties normalize alike only where the contract explicitly defines them as equivalent. Empty strings remain values.

Reject duplicate object keys, invalid Unicode, unexpected value types, unknown profile versions, and multiple competing signatures. Numeric metadata requires a separately specified encoding; this initial profile must not silently convert floating-point values to strings. The choice between this deliberately restricted profile and an established canonical JSON envelope is an unresolved design question below.

### Verification and use

1. Validate the contract and expected subject against the requested asset.
2. Resolve the key from the consumer's trusted configuration.
3. Verify the signature over the canonical bytes of the final body.
4. Return the attestation state and a reason, independently of the decision.
5. Apply current policy identity, context, age, and action-scope checks before using the decision.

Unsigned compatibility is an explicit application policy. A consumer that requires provenance accepts only `SIGNED`; it must not fall back to legacy unsigned behavior after a missing or invalid signature. A valid signature with a stale context or a blocking decision still does not authorize continuation.

### Rotation, compromise, and replay

Operators maintain trusted keys and their validity intervals outside the catalog. A key identifier does not establish trust. Consumers must support revocation or removal of a compromised key and reject records whose producer or time is outside the configured scope.

An attacker with metadata write access can delete a signal or replay an older genuinely signed signal. Age and current-context checks bound that replay window, but cannot prove monotonic history if an old context is restored too. Workflows requiring that stronger guarantee need an external monotonic record or transparency mechanism; this RFC does not claim to supply one.

### Prior implementation and validation

The external [implementation](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/src/sidq/receipt/attestation.py) and [regression tests](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/tests/test_receipt_attestation.py) exercise edited bodies, copied signatures across subjects, absent signatures, duplicate signatures, and property reordering. The [offline demonstration](https://github.com/NexuChat/sidq/blob/bb54959a350653a528280705a84e8e57a7cc189e/demos/attestation.py) needs no DataHub connection.

These establish feasibility, not completion of the proposed DataHub contract. In particular, the prototype's legacy unsigned mode is not suitable for consumers requiring provenance. Version negotiation, a general key registry, and cross-language canonicalization vectors remain proposed work.

Before an implementation merges, require conformance cases for valid records, altered bodies, wrong subjects, wrong keys, stripped signatures, duplicate signatures, stale records, rollback, key rotation, and Unicode/multi-value canonicalization. Validate with at least two independent implementations of the canonical-byte profile.

## How we teach this

Add a guide for producers of external verification metadata and consumers of those signals. Teach provenance, decision consistency, and freshness as separate checks. Examples must show unsigned and stale cases as prominently as the successful case. The default catalog UI must not relabel ordinary properties as authenticated certifications.

## Drawbacks

Key distribution and revocation become operational responsibilities. Canonicalization can produce interoperability failures if its rules are underspecified. Legitimate edits invalidate signatures. Legacy unsigned consumers remain susceptible to substituted trust signals until they explicitly require authenticated provenance.

## Alternatives

- Restrict metadata writers: useful access control, but not portable proof of a specific producer's output.
- Store evidence only in an external trusted service: stronger control over history, with an additional service dependency.
- Adopt an established signed-envelope format: potentially preferable to a new envelope, subject to metadata representation and SDK interoperability.
- Leave the mechanism entirely in external applications: avoids SDK maintenance, at the cost of incompatible contracts and repeated security mistakes.

## Rollout / Adoption Strategy

First agree on the contract and conformance vectors. Then propose an optional SDK helper and an example integration in a separate implementation PR. No existing metadata or unsigned integration changes behavior automatically, and no DataHub signing key is introduced. Consumer applications opt into a provenance requirement explicitly.

## Future Work

Cross-language SDK support, externally managed key providers, and a UI presentation that distinguishes authenticated producer output from self-reported metadata.

## Unresolved questions

- Should the envelope use an established signing format instead of a restricted JSON profile?
- Which existing metadata representation best avoids partial-read ambiguity?
- Should the common verifier live in DataHub's SDK or a separately versioned integration package?
- Which key-rotation and revocation interfaces can remain provider-neutral?
