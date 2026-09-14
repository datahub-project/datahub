# Local Development Performance Investigation

**Date:** 2026-09-14  
**Status:** Investigation record  
**Upstream baseline:** `datahub-project/datahub:master` at
`6ece48b05a2a260e6c9df8aec4be5f7f1e7deca6`  
**Investigation branch:** `perf/local-dev-investigation`

## Executive Summary

The largest measured delay was not Gradle dependency resolution or Yarn package resolution. It was
unnecessary Docker image-build orchestration on a warm, unchanged start. Making local debug image
builds incrementally skippable reduced the measured unchanged start from 94.01 seconds to 9.99
seconds.

The next major limit is the application lifecycle. A one-file GMS change now spends about 11 seconds
in Gradle and another 18 seconds restarting the service. Further reductions require real host
development modes, especially Spring Boot continuous compilation/restart and Play development mode,
instead of repeatedly packaging production-style artifacts and restarting containers.

Two frequently suggested migrations remain valid experiments, but they target different scenarios:

- Redpanda targets infrastructure startup time and local resource use. The Kafka broker was observed
  using approximately 530 MiB and 111% CPU during startup.
- pnpm targets clean dependency installation, disk use, and CI caching. It will not materially improve
  a warm Gradle invocation in which `yarnInstall` is already up to date and no package manager runs.

The repository's current hooks are more selective than initially suspected. Markdown checks already
run only for Markdown/MDX changes, and Java formatting hooks are scoped by project path. The remaining
hook opportunity is to consolidate multiple matching Java projects into one Gradle invocation and to
measure pre-push behavior by change type.

The development modes themselves are documented separately in
[Local Development Modes](./2026-09-14-local-development-modes.md).

## Provenance and Measurement Limits

The investigation was restarted after the fork was synchronized because the earlier checkout was
more than 300 commits behind upstream and did not represent the Java 25 build. All findings in this
document were re-evaluated from commit `6ece48b05a2a260e6c9df8aec4be5f7f1e7deca6`, which was verified
to be the then-current `datahub-project/datahub:master`, and the optimization commits are direct
descendants of it.

The numbers are local wall-clock observations on one macOS/Colima machine. They distinguish cold,
recovery, warm, and source-change cases where possible, but they are not a statistically rigorous
benchmark suite. Values from unlike cache states are retained as diagnostic history and must not be
treated as direct A/B comparisons.

No complete cold-start or clean-install benchmark has been established yet. Such measurements need a
repeatable reset procedure that defines which Gradle, package-manager, Docker, and application-data
caches are retained.

## Static Analysis

### Host toolchain and mise

The upstream repository does use mise. `mise.toml` pins Java 25, Node.js 22, Python 3.11, Yarn 1.22.22,
Actionlint, ShellCheck, and Betterleaks. This is the appropriate source of truth for host and CI tool
versions.

The setup is not fully mise-enforced, however:

- the development shell wrapper invokes `python3` directly;
- Gradle's settings logic probes for `pre-commit` and can install it with `python -m pip`; and
- frontend Gradle tasks provision/use Yarn independently of a user entering through `mise exec`.

This is primarily a reproducibility problem, not the dominant measured latency. The optimization is to
make setup and CI enter a mise-managed environment consistently, then leave project dependencies to
Gradle, uv, and the selected JavaScript package manager. Tool bootstrap time should be measured before
changing enforcement.

### Gradle

The baseline already enables:

- the Gradle daemon;
- parallel execution;
- build cache; and
- configuration on demand.

It limits Gradle to two workers and configures a two-GiB default daemon heap. Those conservative
settings are appropriate for smaller machines but may underuse larger developer machines. Worker and
memory profiles should be benchmarked rather than raised globally.

Configuration cache is not enabled. An optional trial produced 11 compatibility problems. These were
build/plugin compatibility failures caused by configuration-time/project-model assumptions, not a
stale-cache format mismatch. Deleting the configuration-cache directory would only discard stored
state; it would not make incompatible tasks compatible. The normal build does not require
configuration cache, so this does **not** mean the development setup is broken. Enabling it is a
longer-term build-logic project, not an easy local-development win.

### Frontend Gradle graph

The React project uses Yarn Classic through Gradle. Before the first optimization, `yarnInstall`
declared the complete `node_modules` directory as its output. That caused Gradle to fingerprint a tree
of approximately 86,000 files even when Yarn itself had no work to do. The correct install inputs are
`package.json` and `yarn.lock`; the useful output is a compact install-state marker.

The production graph also generated GraphQL artifacts twice: Gradle ran its cacheable generator and
the package-level `yarn build` script invoked generation again before Vite.

Yarn is deeply represented beyond the primary React module: the docs site, Playwright setup, CI cache
keys, Gradle Node plugin tasks, shell commands inside package scripts, and `mise.toml` all refer to it.
A pnpm experiment therefore needs a deliberately scoped migration and cannot be judged by changing a
single command.

### Docker quickstart

The debug quickstart generated Docker Bake definitions and invoked Buildx for the selected image set on
every start. Even with unchanged source, Buildx still evaluated contexts, exported images, and could
cause Compose to recreate services. Gradle lacked a durable output that represented both the worktree
state and the continued existence of the expected local image tags.

The quickstart uses a single Confluent Kafka 8.2.2 container in KRaft mode, so there is no separate
ZooKeeper process to remove. It exposes internal and host listeners, uses a 512-MiB JVM heap, supports
five-MiB messages, and participates in health/readiness checks. DataHub's schema-registry endpoint is
served by GMS rather than by a separate Confluent Schema Registry container.

Redpanda is still promising because it can replace the broker JVM and reduce startup/resource cost,
but compatibility must cover listener names, advertised addresses, topic initialization, message-size
settings, health checks, consumers/producers, and restart persistence.

### Commit and push hooks

The generated `.pre-commit-config.yaml` uses file filters extensively:

- Markdown formatting at pre-push is gated by `^.*\.md$` and calls the changed-files Gradle task.
- Docusaurus Markdown lint at pre-commit is gated by Markdown and MDX files and receives filenames.
- workflow, GraphQL, Playwright, docs-site, PDL, smoke-test, and Python hooks are path/type gated.
- Java Spotless hooks are generated per Gradle project and gated by Java files below that project.

Therefore the original hypothesis that Markdown formatting runs on every commit is false on the
verified upstream baseline. A Markdown-only commit during this investigation skipped all unrelated
hooks; only the Docusaurus Markdown and staged-secret checks ran.

Two narrower opportunities remain:

- nested Java project filters can cause more than one module-level Spotless hook to match, with each
  hook launching Gradle separately; and
- the always-running pre-push file-count guard is intentionally ordered after generated formatters, so
  a push of more than 500 files can do formatting work before being rejected.

Betterleaks runs for every commit by design but scans only the staged diff. That security guard should
not be disabled merely to save hook time.

### Development servers

The supported Docker debug mode mounts built artifacts, not source-aware development servers. GMS and
consumer containers do not use Spring Boot DevTools. The Play container starts a staged application
with `ProdServerStart`, not Play development mode. The Actions container mounts source without a file
watcher.

The host Vite workflow is the exception and provides real React HMR. A `playRun` task also exists and
the Play plugin supports change detection/reload compilation, but the task currently fails during
configuration with an `afterEvaluate` error involving `:datahub-web-react`. Host Spring `bootRun`
tasks exist but are not orchestrated with Docker infrastructure or automatic compilation/restart.

## Measurements

### Recorded observations

| Scenario                               | Revision/state                               | Wall time or resource           | Interpretation                                                     |
| -------------------------------------- | -------------------------------------------- | ------------------------------- | ------------------------------------------------------------------ |
| Full start after Colima recovery       | Baseline-like, caches disturbed              | 171.49 s                        | Recovery datum; not comparable to a warm run                       |
| Immediate unchanged start              | Before Docker skip optimization              | 94.01 s                         | Warm baseline for repeated start                                   |
| First start with new Docker task model | New task model, metadata not yet established | 103.02 s                        | Migration/priming run, not steady state                            |
| Immediate unchanged start              | After Docker skip optimization               | 9.99 s                          | Warm steady-state result                                           |
| Warm `yarnInstall` task                | Existing dependencies                        | 3.25-3.59 s median observations | Package manager did no install; mostly Gradle/configuration floor  |
| One-file GMS reload build phase        | Before module narrowing                      | ~17 s; 258 tasks                | Broad debug profile graph                                          |
| One-file GMS reload build phase        | After module narrowing                       | ~11 s; 229 tasks                | Selected GMS image module only                                     |
| One-file GMS reload end to end         | After module narrowing                       | 30.74 s                         | Includes ~18 s service startup/readiness                           |
| Kafka during startup                   | Confluent Kafka container                    | ~530 MiB; ~111% CPU snapshot    | Supports a Redpanda resource experiment; not a steady-state sample |

The strongest like-for-like result is the warm unchanged start: 94.01 seconds to 9.99 seconds, an
approximately 89% reduction. The one-file GMS build phase improved by roughly 35%, while the number of
tasks in the graph fell by roughly 11%. The service's startup/readiness phase is now larger than its
build/package phase.

The `yarnInstall` observations do not demonstrate that Yarn resolution is slow. They show that a warm
invocation has a multi-second Gradle floor even when the task is up to date. Replacing the enormous
output tree with Yarn's integrity file fixes the task model and prevents Gradle from walking all
installed files, but the available timings do not establish a statistically significant standalone
wall-clock improvement.

### Environment incident

Docker initially failed because Colima's content store was corrupted. This was an environment failure,
not a DataHub build failure. A non-destructive Colima stop/start restored Docker operation. After disk
space was made available, the Colima filesystem reported approximately 93 GiB total, 22 GiB used, and
67 GiB free. Gradle cache deletion would not have repaired the Docker content store.

## Implemented Optimizations

### `174b5ea7b3 perf(frontend): reduce Gradle yarn overhead`

- Changes `yarnInstall` output tracking from all of `node_modules` to
  `node_modules/.yarn-integrity` while retaining `package.json` and `yarn.lock` as inputs.
- Adds Vite-only build scripts.
- Keeps standalone `yarn build` behavior intact, but makes the Gradle `yarnBuild` task call the
  Vite-only script after Gradle's cacheable GraphQL generation has already run.

### `ceca43d1aa perf(docker): skip unchanged local image builds`

- Materializes the Bake specification as a separate Gradle output.
- Tracks a local debug build using the Bake spec, a SHA-256 fingerprint of HEAD plus tracked and
  untracked worktree changes, build metadata, and the existence of expected image tags.
- Lets Gradle skip Buildx when both source state and images are unchanged.
- Disables provenance and SBOM generation only for local debug builds; CI and Depot paths are left
  unchanged.

### `2bea0e8d84 perf(dev): narrow Gradle service reloads`

- Maps reloadable Docker services to their owning Gradle image modules.
- Passes `-PbuildModules=...` for a safely identified service change instead of traversing the full
  debug image profile.
- Retains a full-profile fallback for shared or unmapped changes.
- Still invokes Gradle when Git reports no source changes because build outputs may be stale relative
  to source even when the worktree equals HEAD.

That last behavior is intentional. A Git-only "nothing changed" short circuit is incorrect: reverting
a source file to HEAD can leave an artifact containing the previous edited version, and only Gradle's
input/output model can detect and repair it.

## Conclusions by Scenario

### Warm unchanged environment start

The Docker skip optimization addressed the dominant measured problem. At approximately ten seconds,
the remaining work is chiefly Gradle/wrapper orchestration, Compose validation, and readiness checks.
Further changes here should be justified by repeated measurements.

### Java edit/reload loop

Module narrowing helped, but packaging and container restart remain intrinsic costs. The highest-value
next experiment is a hybrid mode with Docker infrastructure and host Spring services using continuous
compilation plus DevTools. Measure method-body and structural changes separately, because they can
trigger different restart behavior.

### Play server edit/reload loop

Repairing and exposing `playRun` is likely the easiest unimplemented development-server win. It avoids
`installDist`/stage and production-server restart for server-side Play changes. It should be tested
independently from the already-supported Vite HMR path.

### Fresh frontend setup

pnpm is the most credible package-manager candidate, but this scenario has no clean-install baseline
yet. Measure Yarn and pnpm with the same empty project store/cache state and again with a warm global
store. Include wall time, transferred bytes, `node_modules` disk use, Gradle integration, codegen,
build, lint, tests, Playwright, and CI cache changes.

### Infrastructure startup

Redpanda is the most credible broker experiment. Compare it with the existing single-node KRaft Kafka
service under identical persistent-volume states. Record container-ready time, full DataHub-ready
time, peak/steady CPU and memory, image download/size, topic initialization, restart persistence, and
a smoke flow that produces and consumes DataHub events.

### Hooks

Do not add a Markdown filter: it already exists. First capture pre-commit and pre-push timings for
Markdown-only, one Java module, nested Java module, Python-only, and mixed commits. If Java formatting
shows repeated Gradle startup/configuration, replace per-project processes with one dispatcher or one
root Gradle invocation while retaining path scoping and all existing checks.

## Ordered Experiment Plan

The order depends on the latency being optimized. For everyday edit/reload latency:

1. Repair `playRun`, decouple it from production React packaging, expose it through `datahub-dev`, and
   measure Play source and route edits.
2. Prototype Docker infrastructure plus host GMS `bootRun`, continuous compilation, and Spring Boot
   DevTools; measure compile, restart, and ready phases separately.
3. Measure hook scenarios, then consolidate Java Spotless invocations only if repeated Gradle launches
   are material.
4. Benchmark Gradle worker/memory profiles on representative GMS rebuilds.
5. Treat configuration-cache compatibility as a separate build-logic project after the above work.

For environment startup and one-time setup:

1. Benchmark Kafka versus Redpanda in the quickstart profile.
2. Establish clean and warm Yarn installation baselines, then benchmark a complete pnpm prototype.
3. Measure `mise install` and make wrapper/CI entry points consistently use the pinned environment.
4. Establish a controlled cold-start baseline covering dependency download, code generation, image
   build/pull, database initialization, and search initialization.

## Benchmark Protocol for Remaining Work

Each experiment should record:

- exact commit and dirty-worktree state;
- host architecture, allocated Colima CPU/memory/disk, and tool versions from mise;
- which of Gradle cache, package-manager store, `node_modules`, Docker image cache, volumes, and
  generated outputs were retained;
- at least one priming run followed by five measured runs for warm scenarios;
- median and range, not only the best run;
- Gradle task counts and executed/up-to-date/from-cache outcomes;
- component-ready timestamps in addition to total wrapper time; and
- correctness checks appropriate to the changed component.

Change one variable at a time. In particular, do not combine Redpanda with Docker graph changes or
pnpm with unrelated frontend task changes in the same comparison.
