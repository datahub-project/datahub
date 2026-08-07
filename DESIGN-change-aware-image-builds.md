# Change-aware CI image builds

Scratch design note for this branch. Delete before opening the PR.

## Problem

From workflow start to the first pytest assertion is **15.9 min**, measured on
run `31142891680`:

| phase                                                 | duration        |
| ----------------------------------------------------- | --------------- |
| `setup`                                               | 0.9m            |
| `smoke_test_matrix`                                   | 0.6m (parallel) |
| **`Build all images`**                                | **9.3m**        |
| per-batch: depot pull                                 | 1.5m            |
| per-batch: `run-quickstart` (boot GMS/Kafka/ES/MySQL) | 2.2m            |
| per-batch: checkout, caches, deps, CLI                | 1.5m            |
| **pytest starts**                                     | **at 15.9m**    |

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
  `publish_images` job, but _only after smoke tests pass on master_
  (`docker buildx imagetools create -t "$quickstartImage" "$image"`). This is a
  guaranteed-green image set and is the right "pull from HEAD" source.
- `run-quickstart.sh` resolves service images from `DATAHUB_VERSION`, so
  pointing that at `quickstart` makes compose pull the known-good set.
- `base_build` currently gates only on `use_depot_cache`. The one existing
  conditional, `smoke_build_task`, selects a _compose profile_ from a PR label —
  it is not change detection.

## Design (revised)

**Do not skip `base_build`.** Three jobs declare `needs: [..., base_build]` — they
gate on it because they need to know _a usable set of images exists_. Skipping it
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
_runs_, still produces a complete tag map, and every downstream `needs:` is
satisfied unchanged.

### This is already expressible — no compose redesign needed

Every service in `docker/profiles/` resolves its image as:

```
${DATAHUB_<SVC>_IMAGE:-<repo>/<name>}:${DATAHUB_<SVC>_VERSION:-${DATAHUB_VERSION:-quickstart}}
```

So a **per-service version override already exists**, falling back to the global
`DATAHUB_VERSION`:

| service      | override var                       | gradle module                     |
| ------------ | ---------------------------------- | --------------------------------- |
| GMS          | `DATAHUB_GMS_VERSION`              | `:metadata-service:war`           |
| MAE consumer | `DATAHUB_MAE_VERSION`              | `:metadata-jobs:mae-consumer-job` |
| MCE consumer | `DATAHUB_MCE_VERSION`              | `:metadata-jobs:mce-consumer-job` |
| upgrade      | `DATAHUB_UPDATE_VERSION`           | `:datahub-upgrade`                |
| frontend     | `DATAHUB_FRONTEND_VERSION` (added) | `:datahub-frontend`               |
| actions      | `DATAHUB_ACTIONS_VERSION` (added)  | `:datahub-actions`                |

These six are exactly the module list of `:docker:buildImagesQuickstart`. Two of
them had no override and needed one line each in the compose templates. Nothing
else in compose changes, and `run-quickstart.sh` needs no plumbing — the vars are
inherited from the job environment.

### Where the decision lives

The design above says `base_build` computes the map. It is computed in `setup`
instead, and `base_build` consumes it. Same contract, one fewer failure mode:
`base_build` is skipped outright on fork PRs (`use_depot_cache != 'true'`), so a
map living in its outputs would be empty exactly where downstream jobs still need
one. `setup` always runs, so every consumer reads the map the same way on every
path.

### Why this beats the skip approach

- No downstream job changes. `needs: [base_build]` keeps meaning what it meant.
- Per-service granularity falls out naturally, rather than being an all-or-nothing
  switch — a frontend-only PR can rebuild just the frontend.
- The unit of correctness is explicit and inspectable: a tag map in the job output,
  which CI can assert on.

## Backstops

All of these resolve to "build it", i.e. today's behaviour. Implemented in
`.github/scripts/resolve_image_builds.sh`.

- **Default-safe against unclassified paths.** The original plan — reuse when
  `smoke-test == true` and every other filter is false — is _not_ default-safe.
  No filter claims the root `build.gradle`, `gradle/**`, `buildSrc/**`,
  `.github/**`, or a new top-level module, so a PR touching `smoke-test/**` _and_
  `build.gradle` would have reused every image and never tested the change.

  Replaced with an explicit classification of the whole diff. `ci-optimization`
  now emits `changed-files` (paths-filter's `list-files: json` over a `**`
  filter) and the decision script checks every path against a prefix list of
  everything whose effect is known — either it provably cannot reach an image, or
  some filter already claims it. Anything left over forces a full build, and the
  log names the offending path.

  The tidier version of this is a single filter of `**` plus `!` exclusions, but
  that needs `predicate-quantifier: some-with-excludes`, which the pinned
  paths-filter does not have — it validates the value and accepts only `some` or
  `every`, so it would hard-fail rather than degrade. Not worth bumping a pinned
  third-party action for, and keeping the classification next to the decision it
  feeds means one list to maintain instead of two.

  `.github/**` is deliberately _not_ classified: a workflow change can alter
  build args or the build graph, which is exactly what you want built rather than
  reused.

- **Empty changed-file list.** A pull request always changes something, so an
  empty or unparseable list means the list never arrived. Build everything.
- **Schema coupling forces a full build.** `metadata-models` / PDL changes
  regenerate Avro/GraphQL/schema classes consumed by GMS, the frontend, both
  consumers _and_ the Python SDK. `metadata-models/**` and `docker/**` are
  already inside the `backend` filter, and a `backend` hit rebuilds every image —
  so this falls out rather than needing its own rule. Same for a GraphQL schema
  change, which would otherwise pair a PR-built GMS with a HEAD frontend.
- **Label override.** A **`build-images`** label on the PR forces a full build of
  every image, regardless of what changed. Checked first, so the job log reports
  the human override as the reason rather than whichever condition also matched.
  Follows the existing label convention in this workflow (`depot`, `publish`,
  `publish-docker`). This is the escape hatch to reach for when someone suspects
  the reuse path is serving stale images.
- **Trigger scope.** Only `pull_request` can reuse; `workflow_dispatch`, `push`
  to master and `release` always build everything.
- **Fork PRs.** No depot cache, so `base_build` is skipped and each test job bakes
  the full set locally. Nothing to narrow.
- **Publish labels.** `publish` / `publish-docker` push this PR's images for
  people to pull, so the set has to be complete and built from this PR's source.
  Without this the publish path would run with steps its guard had skipped.
- **`smoke:` labels.** Select a different compose profile whose image set does not
  match the one the plan was computed against.
- **Tag existence check.** Each reused tag is resolved against the registry
  before it is trusted; if not, build. Done with an anonymous Docker Hub pull
  token rather than `docker manifest inspect`, since the setup runner has no
  registry login. Fails cheaply in one job instead of inside seven parallel batch
  jobs.

## Per-image rules

Only three shapes of PR can reuse anything. The couplings are deliberately
conservative — the cost of being wrong is asymmetric.

| change                   | rebuilt  |
| ------------------------ | -------- |
| nothing image-relevant   | _(none)_ |
| frontend only            | frontend |
| ingestion / actions only | actions  |
| anything backend         | all six  |

The frontend rebuilds on a backend change (GraphQL schema and generated model are
build inputs). The actions image rebuilds on an ingestion change because it
`pip install`s `../metadata-ingestion` at image build time.

## State of this branch

Implemented:

- `ci-optimization`: `changed-files` output, replacing the unsafe
  `smoke-test-only`. One new `**` filter and `list-files: json`; no change to any
  existing filter.
- `resolve_image_builds.sh`: the whole decision — path classification, the
  backstops, the per-image rules — emitting `image_build_modules` and
  `image_version_env` plus a markdown plan table in the step summary.
- `setup`: runs it; `base_build` and the three test jobs consume the outputs.
- `base_build`: always runs, skips its expensive steps and reports an empty
  build id when there is nothing to bake. Checkout stays unconditional so the
  local `uses: ./...` actions still resolve.
- `docker/build.gradle`: `-PbuildModules=` narrows both `prepareAll*` and
  `buildImages*` to a subset. Filtered silently at configuration time (the
  property is global and must not break unrelated image sets) and validated in
  `doFirst` for the task actually invoked.
- Compose: `DATAHUB_FRONTEND_VERSION`, `DATAHUB_ACTIONS_VERSION`.
- `run-quickstart.sh`: prints `docker compose config --images` before `up`.
- Playwright reusable workflow: takes the tag map, and now logs in to Docker Hub
  whenever it is non-empty — reused images come from the registry, and an
  anonymous pull across eight shards hits the rate limit.

Verified locally: compose resolves the overrides correctly and the generated
`docker-compose.quickstart-profile.yml` is byte-identical (so
`verify-quickstart-compose` stays green); `--dry-run` confirms a partial build
prepares only the requested modules; the decision script produces the right plan
for every backstop, and the anonymous registry check resolves `quickstart` and
`quickstart-slim`.

## Validation

The rest only exercises on real CI. Minimum bar before merge:

1. smoke-test-only PR → nothing built, tag map is all `quickstart`, batches green,
   ~9.3m off the critical path.
2. `datahub-web-react/**` PR → frontend rebuilt, other five reused, Playwright green.
3. `metadata-service/**` PR → everything rebuilt (backend coupling).
4. `build-images` label on case 1 → everything rebuilt.
5. Assert the _resolved tags_ in the job log — passing tests alone would not
   distinguish a correct reuse from silently running against stale images. This is
   what the `config --images` dump in `run-quickstart.sh` is for.
