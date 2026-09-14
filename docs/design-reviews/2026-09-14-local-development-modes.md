# Local Development Modes

**Date:** 2026-09-14  
**Status:** Current-state analysis and target concept  
**Baseline:** `datahub-project/datahub:master` at `6ece48b05a2a260e6c9df8aec4be5f7f1e7deca6`

## Purpose

DataHub currently mixes several distinct development workflows under the term "local development."
They optimize for different goals and provide very different feedback loops. This document names the
modes, records what is actually hot-reloaded, and separates one-time setup from day-to-day iteration.

## Mode Overview

| Mode                   | Runs on the host | Runs in Docker                                         | Edit feedback                                | Current status                   |
| ---------------------- | ---------------- | ------------------------------------------------------ | -------------------------------------------- | -------------------------------- |
| Full Docker debug      | Gradle build     | GMS, Play frontend, consumers, Actions, infrastructure | Rebuild artifact and restart service         | Supported default                |
| Docker plus Vite       | React/Vite       | Play frontend, GMS, consumers, Actions, infrastructure | React HMR; backend rebuild and restart       | Supported frontend workflow      |
| Host Spring            | GMS              | Infrastructure and remaining services                  | Continuous compile; DevTools context restart | Supported for default debug mode |
| Host Play              | Play frontend    | GMS and infrastructure                                 | Automatic server-side compile/reload         | Supported for default debug mode |
| Production-like Docker | Nothing          | Everything                                             | Rebuild images and containers                | Validation, not an inner loop    |

"Debug" in the Docker mode means development-oriented images, mounted build artifacts, and debugger
ports. It does not mean source-level hot reload.

## 1. Full Docker Debug

Start this mode with:

```bash
scripts/dev/datahub-dev.sh start
```

The wrapper invokes the Gradle `quickstartDebug` graph. Gradle builds artifacts on the host and the
debug containers consume mounted outputs, including:

- the GMS boot archive under `metadata-service/war/build/libs/`;
- the staged Play application under `datahub-frontend/build/stage/main`;
- consumer job archives under their respective `build/libs/` directories; and
- mounted Python source for DataHub Actions.

The Java backend loop is therefore:

```text
edit -> Gradle compile/package -> mounted artifact changes -> restart container/JVM -> wait for ready
```

This is not hot reload. The exposed JDWP ports permit an attached IDE to apply the JVM's limited
HotSwap support, normally method-body changes only, but the repository does not provide automatic
compilation and reload. Structural changes still require a rebuild and restart.

The Play container also runs a staged production application through `ProdServerStart`; it does not
use Play development mode. Its loop is compile, `installDist`/stage, and restart.

Actions source is mounted, but its normal Python process has no file watcher. A source mount alone
does not provide hot reload.

After narrowing service rebuilds, a measured one-file GMS change traversed 229 Gradle tasks instead of 258. The build/package phase fell from approximately 17 seconds to 11 seconds, but service startup
still took approximately 18 seconds, for a total of 30.74 seconds.

## 2. Docker Plus the Vite Frontend

Prepare and run this mode with:

```bash
scripts/dev/datahub-dev.sh start
scripts/dev/datahub-dev.sh setup frontend
scripts/dev/datahub-dev.sh frontend
```

The React application runs in the host Vite development server, while Play, GMS, consumers, Actions,
and infrastructure remain in Docker. Vite provides genuine module-level hot replacement for ordinary
React changes. API, authentication, file, and tracking traffic is proxied to the Play frontend using
`REACT_APP_PROXY_TARGET`.

GraphQL generation is part of initial frontend startup. It is not repeated for each ordinary source
edit. This is currently the fastest supported frontend loop.

## 3. Host Spring Development

Start the Docker environment, then replace its GMS container with the host server:

```bash
scripts/dev/datahub-dev.sh start
scripts/dev/datahub-dev.sh gms
```

The command precompiles GMS before stopping the healthy container, translates the container's MySQL,
OpenSearch, Kafka, Neo4j, schema-registry, port, and entity-registry settings to host endpoints, and
runs `bootRun` with Spring Boot DevTools. Once the host service is healthy, a separate continuous
Gradle `classes` build watches GMS and its project dependencies. Changed class output causes DevTools
to restart the application context without rebuilding a boot archive or restarting Docker. Ctrl-C,
startup failure, and compiler failure terminate both process groups and restore the Docker GMS.

Host development disables Fabric8 Kubernetes auto-configuration. Otherwise a developer's local
kubeconfig credential command may run during Spring initialization even though this mode is not
running in Kubernetes. This override is scoped to the host-dev `bootRun` invocation.

The context itself remains large. This mode removes packaging and container lifecycle work, but a
structural change still pays Spring's context restart cost. Its steady-state improvement must be
measured separately from the initial compile and initial application startup.

## 4. Host Play Development

Start the Docker environment, then replace its staged frontend with Play development mode:

```bash
scripts/dev/datahub-dev.sh start
scripts/dev/datahub-dev.sh play
```

The wrapper uses the worktree's assigned frontend port, translates the Docker frontend environment to
host endpoints, stops the Docker frontend only after a successful precompile, and restores it on
exit. Play watches Java, Scala, and route inputs and performs reload compilation without staging a
production distribution. React remains a separate Vite process via `datahub-dev frontend`; the
development Play graph deliberately excludes the production React asset project.

The Play Gradle plugin is incompatible with this build's configuration-on-demand mode while resolving
its runtime project graph, so only this command disables configuration on demand. The reload
classloader also drops the Rest.li API project's custom generated-client artifact; the development
graph supplies that jar explicitly. The production Play distribution and normal Gradle defaults are
unchanged.

## 5. Production-Like Docker

Production image builds and a fully containerized deployment are useful for integration and packaging
validation. They intentionally favor fidelity over edit latency and should not be treated as the
normal inner loop.

## Setup Versus Iteration

### Host tool setup

`mise.toml` is the source of truth for the primary host runtimes, including Java 25, Node.js 22,
Python 3.11, and Yarn 1.22.22. `mise install` prepares the toolchain. Project dependencies remain the
responsibility of their project package managers.

The wrapper provides separate setup actions for the Python ingestion environment and frontend
dependencies. Frontend setup currently uses Yarn; a pnpm migration remains an experiment rather than
part of the current modes.

### First environment start

The first `datahub-dev start` is much larger than a service restart. Its Gradle graph can include the
upgrade image, GMS archive, Play distribution, React build, Actions image, Docker Bake preparation,
Compose startup, infrastructure initialization, and readiness checks. A cold start may additionally
download dependencies and images, generate code, and initialize databases and search indices.

Gradle already enables the daemon, parallel execution, build cache, and configuration on demand. It
limits workers to two. Configuration cache is not enabled and is not presently compatible with the
build; it is not required for a functioning setup.

### Warm unchanged start

With artifacts and images already present, the optimized Docker path can validate the desired state
without rebuilding unchanged debug images. A measured warm, unchanged `datahub-dev start` completed in
9.99 seconds.

## Target Development Topology

The fastest useful local topology should keep stateful infrastructure in Docker and run change-heavy
applications in their native development modes:

| Component                  | Target execution                         | Target reload behavior                       |
| -------------------------- | ---------------------------------------- | -------------------------------------------- |
| MySQL and OpenSearch       | Docker                                   | Persistent, no reload                        |
| Kafka-compatible broker    | Docker                                   | Persistent; benchmark Kafka against Redpanda |
| GMS and selected consumers | Host Spring JVM                          | Continuous compile plus DevTools restart     |
| Play frontend server       | Host Play development mode               | Automatic server-side compile/reload         |
| React application          | Host Vite                                | HMR                                          |
| DataHub Actions            | Host watched Python process where needed | Process restart on source changes            |

The full Docker debug environment remains the compatibility and integration environment. It should be
fast to start when unchanged, but it should not be the only development loop.

## References

- [Spring Boot Developer Tools](https://docs.spring.io/spring-boot/reference/using/devtools.html)
- [Play development mode](https://www.playframework.com/documentation/3.0.x/PlayConsole)
