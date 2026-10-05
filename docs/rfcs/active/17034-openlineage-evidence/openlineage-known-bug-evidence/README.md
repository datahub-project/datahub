# B1–B34 payload fixtures

Each `fixtures/B<n>.json` contains ordered OpenLineage request bodies for the matching row in the [known-bug matrix](../openlineage-known-bug-head-matrix-2026-10-01.md). `event_sequence[].payload` is the JSON body sent to `/openapi/openlineage/api/v1/lineage`; submit events in order, waiting for the relevant stored aspect after each write. `additional_cases` holds separate variants that must be replayed independently. B23 uses `payload_raw_json` and the companion [`B23.payload.json`](fixtures/B23.payload.json), because parsing its raw numeric literals as binary floating point would change the test. The [crosswalk](../openlineage-known-bug-crosswalk-2026-09-30.json) names the exact test method or saved live readback and the requested upstream action.

The [fixture validation report](fixture-validation.json) checks 153 event bodies against the pinned 1.53 schema and checks event-time offsets separately. B8 intentionally has non-UUID legacy run IDs; B3 step 4 has an offsetless timestamp; and B19's captured steps 12 and 14 contain partial typed facets accepted by our receiver. Those captured bodies remain unchanged. B19's `additional_cases` includes a full schema-valid replay sequence with the missing facet fields supplied; that variant has not been executed. Treat these exceptions as compatibility evidence, not strict 1.53 failure proofs.

`provenance` has three deliberately different meanings:

- `executed_current_live`: copied from a saved request/readback artifact. The fixture points to that artifact for native stored values. HTTP status is copied from the recorded response.
- `captured_from_existing_smoke_test`: copied from the request transport while the cited existing smoke test ran. Its linked test result records pass/fail; the test asserts native values, but the capture itself does not store full read API responses.
- `constructed_from_checked_in_test_unexecuted_payload`: reconstructed from a checked-in converter/servlet test. The JSON body was **not** submitted to a live receiver. `observed_http_status` is null, and the test may validate proposals rather than persistence. B8 intentionally uses a non-UUID legacy run ID outside strict OpenLineage 1.53.

All executed requests were against our local branch. No fixture claims execution against DataHub master or a PR head. A target that rejects JobEvent/DatasetEvent or lacks a mapped facet needs the corresponding per-target interpretation in the matrix; an accepted POST alone does not establish persistence. Fixture names and values are generic and contain no credentials.

## Current implementation locations

Implementation: [PR #19257](https://github.com/datahub-project/datahub/pull/19257), [feat/openlineage-conformance-combined](https://github.com/manuschillerdev/datahub/tree/feat/openlineage-conformance-combined). Captured smoke node IDs use the historical `smoke-test/tests/openapi/` path; current tests are in [`smoke-test/tests/e2e/openapi/test_openapi.py`](https://github.com/manuschillerdev/datahub/blob/feat/openlineage-conformance-combined/smoke-test/tests/e2e/openapi/test_openapi.py). The original JSON locator fields remain provenance. Use the publication manifest and local relative evidence links for this RFC bundle.
