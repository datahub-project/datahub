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

## Design

Skip the image build entirely when the diff cannot affect any image, and point
the smoke-test job at the last known-good `quickstart` tag instead.

1. **`ci-optimization`** — add a `smoke-test-only` output:
   true when `smoke-test` changed and `frontend`, `backend`, `ingestion` and
   `docker` did **not**. Mirrors the existing `*-only` outputs.

2. **`setup`** — derive `needs_image_build`:
   `false` only when `smoke-test-only` is true, the trigger is `pull_request`,
   and the backstop is not engaged. `true` otherwise (the safe default).

3. **`base_build`** — add `needs.setup.outputs.needs_image_build == 'true'` to
   its `if`.

4. **smoke-test job** — when `needs_image_build != 'true'`, skip `depot pull`
   and set `DATAHUB_VERSION=quickstart` so `run-quickstart.sh` pulls the
   known-good images.

## Backstops (the point of the exercise)

Every one of these falls back to today's behaviour — a full build:

- **Default-safe derivation.** `needs_image_build` is false *only* for an
  explicit allowlisted case. Anything unrecognised builds.
- **Label override.** A `ci-full-build` label on the PR forces the full path,
  for when someone suspects the fast path is lying.
- **Trigger scope.** `workflow_dispatch`, `push` to master and `release` always
  full-build. Only `pull_request` can take the fast path.
- **Tag existence check.** `setup` verifies the `quickstart` tag actually
  resolves in the registry before choosing the fast path; if it does not (never
  published, registry hiccup, retention), fall back to building. Without this
  the fast path fails *late*, inside seven parallel batch jobs, instead of early.
- **No change to the retry path.** A re-run that lands on the slow path still
  behaves exactly as today.

## Deliberately out of scope

Per-service selection (e.g. build only GMS when only `metadata-service`
changed) is the tempting next step and is **not** in this change. The blocker is
schema coupling: `metadata-models` PDL changes regenerate Avro/GraphQL/schema
classes consumed by GMS, the frontend, both consumers *and* the Python SDK, so
any PDL touch forces a full rebuild (AGENTS.md says as much). A GraphQL schema
change likewise means a PR-built GMS cannot safely pair with a HEAD frontend.
That work needs an explicit "these paths force a full rebuild" set and should
land only after the pull-from-HEAD path has proven itself.

## Validation

This cannot be verified locally — it only exercises on real CI. Minimum bar
before merge:

1. A PR touching only `smoke-test/**` → `base_build` skipped, batches green,
   and the ~9.3m saving visible in the critical path.
2. A PR touching `metadata-service/**` → full build, unchanged behaviour.
3. A PR touching both → full build.
4. The `ci-full-build` label on case 1 → full build.
5. Confirm the images the fast path ran against were genuinely the `quickstart`
   tag, by asserting the resolved tag in the job log — not merely that tests
   passed, since they would also pass against a stale local image.

## IMPORTANT: downstream consumers of `base_build` (found mid-implementation)

Three jobs declare `needs: [..., base_build]`, so skipping it affects all of them:

| job | consumes | behaviour when `base_build` is skipped |
| --- | --- | --- |
| `pytest_tests` | `base_build.outputs.build_id` for `depot pull` | `if:` uses `always() && !failure() && !cancelled()`, and a *skipped* need is not a failure, so the job still runs. Needs the `DATAHUB_VERSION` fallback (below). |
| `playwright_test` | passes `depot_build_id` into the reusable workflow | `depot_build_id` becomes `''`, and the reusable workflow already treats empty as "build images locally". Correct, but it would rebuild — **cancelling the saving for that job**. Needs the same `quickstart`-tag treatment. |
| `java_integration_tests` | `base_build.outputs.build_id` | **Not yet checked.** Must be verified before this ships. |

There is already precedent for `base_build` being skipped — it does not run on fork
PRs (`use_depot_cache != 'true'`), and the smoke-test job builds locally instead.
That path is the model to follow, but note it *builds* rather than *pulls*, which
is the opposite of what we want here.

## State of this branch

Done:
- `ci-optimization`: `smoke-test-only` output.
- `setup`: `image-build-decision` step + `needs_image_build` output, with all four
  backstops (non-PR triggers, image-affecting changes, `ci-full-build` label,
  registry tag-existence check).
- `base_build`: gated on `needs_image_build == 'true'`.

Not done — **the branch is not runnable as-is**:
- `pytest_tests`: skip `depot pull` and point `DATAHUB_VERSION` at `quickstart`
  when the build was skipped. There are two `DATAHUB_VERSION` lines in this job
  (currently ~804 and ~843); both need it.
- `playwright_test`: same treatment, or it silently rebuilds locally and eats the
  saving.
- `java_integration_tests`: determine whether it can tolerate a skipped build.
- Verify `docker manifest inspect` works on the setup runner without a registry
  login (it may need `docker login` first, or should use the registry API instead).

Do not open a PR until the three downstream jobs are handled — gating `base_build`
alone would leave them pulling a build id that does not exist.
