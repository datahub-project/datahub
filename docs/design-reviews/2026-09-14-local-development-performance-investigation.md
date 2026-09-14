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

The next major limit was the application lifecycle. The implemented host modes reduce median Play
Java and route feedback latency by approximately 60% and 70%, respectively. Host GMS reduces median
method-body and structural edit-to-ready latency by approximately 59% and 54%, respectively. GMS
still spends about five seconds compiling and nine seconds restarting the Spring context.

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

### Optional host tool bootstrap

The upstream repository offers mise as an optional tool-bootstrap path. `mise.toml` pins Java 25,
Node.js 22, Python 3.11, Yarn 1.22.22, Actionlint, ShellCheck, and Betterleaks, but normal
`datahub-dev` commands do not require contributors to launch them through mise. Direct Python,
Gradle-wrapper, and frontend-tool invocations are consistent with that optional contract.

This is not an authorization to make mise mandatory. Any change to DataHub's supported tool-bootstrap
contract requires separate agreement with the project. Performance work should preserve the current
optional behavior.

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

The host Vite workflow provides real React HMR. The investigation branch now also exposes Play's
reload compiler and a host GMS mode combining `bootRun`, continuous compilation, and Spring Boot
DevTools. Both commands temporarily replace only their corresponding Docker service and reuse the
remaining Docker infrastructure.

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

### Host development mode comparison

The host-mode benchmark ran at `dfdb81effab544d51eb6646c80ea7fffe8daab50` on Apple arm64 with 12
host CPUs and 64 GiB RAM. Colima used 4 CPUs, 32 GiB RAM, a 95 GiB disk, VirtioFS, and the Docker
runtime. Java 25.0.2 and the remaining pinned tools came from mise. Gradle and Docker caches,
generated outputs, images, and persistent volumes were retained. Each reported scenario used one or
more discarded primers followed by five unique edits; using unique edits prevented build-cache hits
from alternating between previously compiled source states.

| Scenario             | Docker samples (s)                | Docker median | Host samples (s)                  | Host median | Improvement |
| -------------------- | --------------------------------- | ------------- | --------------------------------- | ----------- | ----------- |
| Play Java edit       | 15.52, 15.77, 15.70, 15.69, 15.58 | 15.69 s       | 7.12, 6.51, 6.26, 6.05, 6.27      | 6.27 s      | 60%         |
| Play route edit      | 17.56, 17.50, 18.80, 17.94, 17.01 | 17.56 s       | 5.69, 5.25, 5.40, 5.14, 5.31      | 5.31 s      | 70%         |
| GMS method-body edit | 38.01, 39.18, 34.56, 29.65, 29.76 | 34.56 s       | 14.33, 14.13, 14.11, 14.23, 14.37 | 14.23 s     | 59%         |
| GMS structural edit  | 31.72, 31.24, 31.54, 30.91, 31.10 | 31.24 s       | 14.73, 14.28, 14.44, 14.16, 14.32 | 14.32 s     | 54%         |

The GMS method-body Docker samples retained a downward warm-up trend, so their range is wider than
the later structural series. The median remains sufficient for the high-level comparison but should
not be treated as a precise steady-state estimate. The stable Docker structural series spent 11-12
seconds in Gradle, traversed 229 tasks with 7 executed, and then spent approximately 18 seconds
reaching readiness. Play Java traversed 113 tasks with 7 executed; route changes executed 9.

Host GMS phase instrumentation produced these medians:

| Edit type   | Continuous compilation | DevTools restart/readiness | Total edit-to-ready |
| ----------- | ---------------------- | -------------------------- | ------------------- |
| Method body | 5.07 s                 | 9.09 s                     | 14.23 s             |
| Structural  | 5.01 s                 | 9.35 s                     | 14.32 s             |

Every counted run verified a unique HTTP response marker. Temporary endpoints, fields, route entries,
and response markers were removed after measurement, and both Docker services were rebuilt from the
clean source tree.

### Measurement control findings

Two setup defects materially affected discarded primers:

- A direct `scripts/dev/datahub-dev.sh rebuild` benchmark inherited an incompatible host JDK and
  failed with `release version 25 not supported`; the valid Docker samples explicitly entered the
  benchmark machine's mise environment. This was a benchmark-environment mistake, not evidence that
  `datahub-dev` should require mise.
- The git-properties plugin resolves the primary checkout's HEAD in a linked worktree. This
  worktree was at `dfdb81e`, but even a no-daemon, forced generation wrote the primary checkout's
  `f4e53a5` into `build/git.properties`. A concurrent primary-worktree commit therefore invalidated
  git properties and caused broad jar regeneration. Measurements taken during that transition were
  discarded, and counted GMS runs verified that the primary HEAD stayed fixed.

Host GMS also becomes HTTP-healthy before the separate continuous compiler clearly reports its
initial watch-ready state. This makes initial command readiness ambiguous even though steady-state
edit detection works.

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

### Host Java development servers

- Adds `datahub-dev play`, with a preflight compile, worktree-aware port and environment translation,
  Play reload compilation, and automatic restoration of the Docker frontend.
- Keeps React out of the Play development graph so Vite remains the React HMR server and no
  production React distribution is built.
- Works around the Play plugin's configuration-on-demand incompatibility only for `playRun` and
  explicitly supplies the generated Rest.li client jar missing from Play's reload classpath.
- Adds `datahub-dev gms`, Spring Boot DevTools, direct source-resource use, worktree-aware dependency
  translation, a health-gated continuous compiler, and automatic restoration of Docker GMS.
- Disables Fabric8 Kubernetes auto-configuration only in host GMS mode so local kubeconfig credential
  helpers cannot block Spring startup.
- Runs both new Gradle entry points through the Gradle wrapper in the caller's configured environment,
  consistent with the other `datahub-dev` commands.

Functional validation reached HTTP 200 on Play `/admin` and GMS `/health`. A temporary GMS source
edit was detected by the continuous compiler, caused a DevTools restart, and returned to HTTP 200.
Those initial checks established correctness; the controlled edit-to-response measurements are
reported in the host development mode comparison above.

## Conclusions by Scenario

### Warm unchanged environment start

The Docker skip optimization addressed the dominant measured problem. At approximately ten seconds,
the remaining work is chiefly Gradle/wrapper orchestration, Compose validation, and readiness checks.
Further changes here should be justified by repeated measurements.

### Java edit/reload loop

Module narrowing helped the Docker path, while host GMS reduced method-body edit latency from a
34.56-second median to 14.23 seconds and structural edit latency from 31.24 seconds to 14.32 seconds.
Method-body and structural changes are effectively identical in host mode because both restart the
Spring context. The next host-mode optimization target is therefore the roughly nine-second context
restart, followed by the roughly five-second continuous compilation.

### Play server edit/reload loop

`datahub-dev play` avoids `installDist`/stage and production-server restart. It reduced Java edit
latency from a 15.69-second median to 6.27 seconds and route latency from 17.56 seconds to 5.31
seconds. The remaining latency is Play/Gradle reload compilation rather than container lifecycle.

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

1. Make git-properties generation linked-worktree aware and correctly track each worktree's HEAD.
2. Make `datahub-dev gms` report compiler watch readiness separately from HTTP readiness.
3. Measure hook scenarios, then consolidate Java Spotless invocations only if repeated Gradle launches
   are material.
4. Benchmark Gradle worker/memory profiles on representative GMS rebuilds.
5. Treat configuration-cache compatibility as a separate build-logic project after the above work.

For environment startup and one-time setup:

1. Benchmark Kafka versus Redpanda in the quickstart profile.
2. Establish clean and warm Yarn installation baselines, then benchmark a complete pnpm prototype.
3. Establish a controlled cold-start baseline covering dependency download, code generation, image
   build/pull, database initialization, and search initialization.

## Benchmark Protocol for Remaining Work

Each experiment should record:

- exact commit and dirty-worktree state;
- host architecture, allocated Colima CPU/memory/disk, and effective tool versions;
- which of Gradle cache, package-manager store, `node_modules`, Docker image cache, volumes, and
  generated outputs were retained;
- at least one priming run followed by five measured runs for warm scenarios;
- median and range, not only the best run;
- Gradle task counts and executed/up-to-date/from-cache outcomes;
- component-ready timestamps in addition to total wrapper time; and
- correctness checks appropriate to the changed component.

Change one variable at a time. In particular, do not combine Redpanda with Docker graph changes or
pnpm with unrelated frontend task changes in the same comparison.
