# Change-aware CI image builds

Scratch design note for this branch. Delete before opening the PR.

## Problem

From workflow start to the first pytest assertion is **15.9 min**, measured on
run `31142891680`:

| phase | duration |
| --- | --- |
| `setup` | 0.9m |
| `smoke_test_matrix` | 0.6m (parallel) |
| **`Build all images`** | **9.3m** |
| per-batch: depot pull | 1.5m |
| per-batch: `run-quickstart` (boot GMS/Kafka/ES/MySQL) | 2.2m |
| per-batch: checkout, caches, deps, CLI | 1.5m |
| **pytest starts** | **at 15.9m** |

`Build all images` is the single largest item and it is strictly serial —
nothing else can start until it finishes. It runs on **every** PR regardless of
what changed, so a PR touching only `smoke-test/**` still bakes the entire
quickstart image set.

For scale: the batch-balancing work in #18960 fought over ~1.5-4 min. This is
9.3 min on a large class of PRs.

## What already exists

- `.github/actions/ci-optimization` already computes `frontend-only`,
  `backend-only`, `ingestion-only`, `connector-source-only`, and a
  `smoke-test` filter over `smoke-test/**`.
- A known-good floating **`quickstart`** tag is published by the
  `publish_images` job, but *only after smoke tests pass on master*
  (`docker buildx imagetools create -t "$quickstartImage" "$image"`). This is a
  guaranteed-green image set and is the right "pull from HEAD" source.
- `run-quickstart.sh` resolves service images from `DATAHUB_VERSION`, so
  pointing that at `quickstart` makes compose pull the known-good set. No
  compose changes needed.
- `base_build` currently gates only on `use_depot_cache`. The one existing
  conditional, `smoke_build_task`, selects a *compose profile* from a PR label —
  it is not change detection.

## Design (revised)

**Do not skip `base_build`.** Three jobs declare `needs: [..., base_build]` — they
gate on it because they need to know *a usable set of images exists*. Skipping it
breaks that contract and forces every downstream job to cope with a missing
build id.

Instead, keep `base_build` always running and make it **fly through when there is
nothing to build**. Its contract becomes:

> `base_build` outputs one resolved image tag per container. Quickstart consumes
> that map and does not care whether a tag was just built or reused.

Per image, independently:
- affected by this PR's diff → build it, tag with the PR tag
- not affected → reuse the floating `quickstart` tag (the last set that passed
  smoke tests on master)

A smoke-test-only PR builds nothing and the job finishes in seconds, but it still
*runs*, still produces a complete tag map, and every downstream `needs:` is
satisfied unchanged.

### This is already expressible — no compose redesign needed

Every service in `docker/profiles/` resolves its image as:

```
${DATAHUB_<SVC>_IMAGE:-<repo>/<name>}:${DATAHUB_<SVC>_VERSION:-${DATAHUB_VERSION:-quickstart}}
```

So a **per-service version override already exists**, falling back to the global
`DATAHUB_VERSION`:

| service | override var |
| --- | --- |
| GMS | `DATAHUB_GMS_VERSION` |
| MAE consumer | `DATAHUB_MAE_VERSION` |
| MCE consumer | `DATAHUB_MCE_VERSION` |
| upgrade | `DATAHUB_UPDATE_VERSION` |
| frontend | **none — global `DATAHUB_VERSION` only** |

`base_build` emits these; `run-quickstart.sh` passes them through. The only gap is
the frontend, which needs a `DATAHUB_FRONTEND_VERSION` added to the compose
templates (one line per profile, matching the pattern already used by the others).

### Why this beats the skip approach

- No downstream job changes. `needs: [base_build]` keeps meaning what it meant.
- Per-service granularity falls out naturally, rather than being an all-or-nothing
  switch — a frontend-only PR can rebuild just the frontend.
- The unit of correctness is explicit and inspectable: a tag map in the job output,
  which CI can assert on.

## Backstops

All of these resolve to "build it", i.e. today's behaviour:

- **Default-safe per image.** An image is reused only when its filter is
  explicitly false. Anything unrecognised is built.
- **Schema coupling forces a full build.** `metadata-models` / PDL changes
  regenerate Avro/GraphQL/schema classes consumed by GMS, the frontend, both
  consumers *and* the Python SDK, so any touch there rebuilds everything
  (AGENTS.md states this). Same for a GraphQL schema change, which would
  otherwise pair a PR-built GMS with a HEAD frontend.
- **Label override.** `ci-full-build` on the PR forces a full build.
- **Trigger scope.** Only `pull_request` can reuse; `workflow_dispatch`, `push`
  to master and `release` always build everything.
- **Tag existence check.** Verify the `quickstart` tag resolves before relying on
  it; if not, build. Fails cheaply in one job instead of inside seven parallel
  batch jobs.

## State of this branch

Done:
- `ci-optimization`: `smoke-test-only` output.
- `setup`: `image-build-decision` step + `needs_image_build` output, with the
  trigger/label/tag-existence backstops. Still useful as the coarse "can we reuse
  anything at all" signal.
- `base_build` gating was added and then **reverted** — under this design it must
  always run.

Next:
1. Add `DATAHUB_FRONTEND_VERSION` to the frontend compose templates.
2. In `base_build`, compute the per-image build/reuse decision and emit a tag map
   output (one entry per container).
3. Have `pytest_tests`, `playwright_test` and `java_integration_tests` consume the
   map instead of `DATAHUB_VERSION` alone.
4. Make the depot build target only the images that need building.
5. Confirm `docker manifest inspect` works on the setup runner without a registry
   login, or use the registry API instead.

## Validation

Only exercises on real CI. Minimum bar before merge:

1. smoke-test-only PR → nothing built, tag map is all `quickstart`, batches green,
   ~9.3m off the critical path.
2. `metadata-service/**` PR → GMS and consumers rebuilt, frontend reused.
3. `metadata-models/**` PR → everything rebuilt (schema coupling).
4. `ci-full-build` label on case 1 → everything rebuilt.
5. Assert the *resolved tags* in the job log — passing tests alone would not
   distinguish a correct reuse from silently running against stale images.
