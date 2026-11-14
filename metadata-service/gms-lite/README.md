# GMS lite

Run the production `war.war` locally without Docker, Kafka, or a database server.
Requires Python 3.10+, a POSIX system (Linux or macOS), and JDK 25. Python uses only
the standard library. The first Gradle build downloads JVM dependencies.

```bash
export JAVA_HOME=~/.local/share/mise/installs/java/25.0.2
python3 metadata-service/gms-lite/bin/gms-lite-stack.py
```

The launcher builds the GMS and upgrade jars plus the embedded OpenSearch
`installDist`, gets or starts the shared OpenSearch node, then runs `datahub-upgrade.jar -u SystemUpdate`
and `-u RestoreIndices` for each new instance. Upgrade processes run serially to
limit memory use. GMS processes start after bootstrap finishes. Readiness requires
HTTP health and, when authentication is enabled, the bootstrap user's policies.

Use `--skip-build` after building those artifacts. `--heap 1g` sets each
GMS/upgrade heap cap. One OpenSearch JVM, with a 4 GB default heap, serves all
local stacks. Set `EMBEDDED_OPENSEARCH_HEAP` when **building** its distribution
to change that cap, then stop and start the shared node to apply it:

```bash
EMBEDDED_OPENSEARCH_HEAP=6g ./gradlew :metadata-service:opensearch-embedded:installDist
python3 metadata-service/gms-lite/bin/_opensearch_manager.py stop
python3 metadata-service/gms-lite/bin/_opensearch_manager.py start
```

The shared node keeps its data, logs, `port`, `pid`, and `startup.lock` under
`~/.datahub/local/opensearch-3/`. This versioned directory separates major
versions because OpenSearch data directories are not portable across major
versions. Override it with `--shared-opensearch-dir /path/to/state` on the
launcher or `GMS_LITE_OPENSEARCH_DIR` for both entry points. Manager commands
accept `--dir /path/to/state`. Always use the same directory to manage a node.

The manager serializes start/stop with an exclusive file lock, verifies process
identity before signalling, and reuses only a healthy OpenSearch 3.x node.
A dead or unhealthy node is replaced while retaining its data; a different major
version is refused. If retained data cannot recover, startup fails with the log
location; choose a different state directory or repair the data explicitly.
There is no automatic deletion of other stacks' indices.

Both HTTP and transport ports are ephemeral. Use `--private-opensearch` for a
node owned by this stack, with data under `<data-dir>/opensearch/`, that stops
with the launcher. Supplying `--opensearch-port N` also selects private mode
(`0` means ephemeral); occupied ports are refused. Private mode is useful for
CI and self-contained runs.

```bash
python3 metadata-service/gms-lite/bin/gms-lite-stack.py \
  --skip-build --data-dir /tmp/my-gms --port-base 18090 --processes 2
```

`--processes` defaults to 1. Instances are named `gms-0`, `gms-1`, etc. Each has its
own H2 file, queue spool, object-storage directory, HTTP port, and index prefix.
Prefixes include a hash of the absolute data-directory path and the instance
name, avoiding collisions between identically named directories. Instances share
one OpenSearch node but never an index. By default the shared node runs in its
own session and outlives every launcher that uses it; stopping one stack leaves
other stacks running. The launcher prints the command to stop the shared node:

```bash
python3 metadata-service/gms-lite/bin/_opensearch_manager.py status
python3 metadata-service/gms-lite/bin/_opensearch_manager.py stop
```

Stopping the shared node interrupts search for every stack using it. Stop stacks
first. The manager sends SIGTERM, waits for shutdown, and uses SIGKILL only if
necessary. `start` can also prewarm the node without starting GMS.

An optional `--env-file /path/to/overrides.env` accepts literal `KEY=VALUE` lines,
optional `export ` prefixes, quoted values, and whole-line comments. There is no
default env file. The launcher inherits the shell environment, applies this file,
and then pins its local connection settings. Keep external connection overrides
out of the inherited environment (for example `ELASTICSEARCH_URI` supersedes
legacy host/port settings in GMS). The generated `gms-N/env.sh` records settings
that differ from the inherited environment, using shell-safe quoting. It is an
output file and is never read back automatically.

## Discovery and authentication

Once ready, `<data-dir>/stack.json` is published atomically, with mode `0600`:

```json
{
  "bootId": "unique-random-id-for-this-start",
  "opensearchPort": 54321,
  "opensearchShared": true,
  "default": "gms-0",
  "instances": {
    "gms-0": "http://localhost:18090",
    "gms-1": "http://localhost:18091"
  },
  "indexPrefixes": {
    "gms-0": "gmslite0123456789abgms0",
    "gms-1": "gmslite0123456789abgms1"
  },
  "auth": true,
  "adminTokens": {
    "gms-0": "<personal-access-token>",
    "gms-1": "<personal-access-token>"
  }
}
```

`opensearchShared` says whether stopping this launcher leaves the node running.
`default` is a key in `instances`. `indexPrefixes` contains the configured
`INDEX_PREFIX` values; GMS appends an underscore before index names. `bootId`
changes on every start. `adminTokens` contains a one-day personal token for the
existing bootstrap user `datahub` in each instance. The launcher uses OSS's
shipped bootstrap policies without adding or replacing any policy. No frontend
or password login is started.

Pass the instance token as `Authorization: Bearer <token>`, or use it with the
DataHub CLI as `DATAHUB_GMS_TOKEN` alongside `DATAHUB_GMS_URL`. `/health` and
`/config` are public. For example:

```bash
curl -fsS http://localhost:18090/health
curl -fsS http://localhost:18090/config
```

`--no-auth` disables token authentication and publishes `auth: false` and an empty
`adminTokens` object. GMS binds to loopback. The system credentials and token keys
are randomly generated, saved in `<data-dir>/.secrets.json` with mode `0600`, and
reused on restart. Neither credentials nor tokens are printed to stdout.

## Persistence and recovery

Stop with Ctrl-C or SIGTERM to the launcher PID. It stops GMS and active upgrade
processes, waits for JVM shutdown, and kills remaining children in their owned
process groups if they do not exit. Shared OpenSearch stays up. In private mode,
GMS stops before the owned OpenSearch node. The spool is saved during orderly JVM shutdown;
SIGKILL or power loss can lose queued events. This is a local development stack.

Restart with the same absolute data-directory path. H2, search indices, credentials,
and queue snapshots persist. A `gms-N/system-update-done` marker is written only
after both upgrade commands succeed. Existing H2 tables are never recreated on a
retry. Remove this marker to rerun upgrades after changing bootstrap code. If
this instance's search indices are missing, upgrades run again to restore search
from H2, including when the shared node's indices were removed separately.

An exclusive lock prevents concurrent launchers from using the same data
directory, including with `--fresh`. Old `stack.json` files are removed under that
lock before startup; the discovery file is removed again on exit. `--fresh`
clears only a directory previously marked as belonging to this launcher (or an
empty directory). With shared OpenSearch it also deletes the exact index prefixes
of this stack's current and previously persisted instances, followed by `_`,
including matching aliases, data streams, and templates. Names are enumerated
before deletion; no wildcard DELETE is sent. Other prefixes are preserved.
Without `--fresh`, the same prefixes and data are reused. Do not move an active data directory or remove its sibling
`.gms-lite.lock` file. The launcher never removes H2/OpenSearch lock files or
reclaims other processes' ports. Startup errors retain data and logs for diagnosis.

Usage-event indices use the mappings from the upgrade jar's source template,
without ISM lifecycle settings. Automatic usage-event rollover and retention are
unavailable on this minimal node. The embedded distribution includes the matching
custom-codecs plugin because GMS selects `zstd_no_dict` for ordinary indices.
Semantic search remains disabled by default; the launcher does not request k-NN.

## Verification

```bash
./gradlew :metadata-service:gms-lite:check
./gradlew :metadata-ingestion:lintFix
./gradlew :metadata-service:gms-lite:stressTest
# New results directory (previous results are otherwise resumed):
./gradlew :metadata-service:gms-lite:stressTest -PstressDataDir=/tmp/gms-lite-check
# Select cases:
./gradlew :metadata-service:gms-lite:stressTest -PstressTests="persistence parallel stale"
# Or build and verify with a fresh temporary results directory:
metadata-service/gms-lite/bin/verify-stack.sh
```

`launcherSafetyTest` runs fast, offline safety tests and is part of `check`. The
module's `lint` and `lintFix` Gradle tasks use the repository's pinned Python tool
and `ruff.toml`; `check` includes `lint`. A repository pre-commit hook runs the
module's `lintFix` task for Python/config changes.

The opt-in stress suite checks authenticated ingest/read/search, H2 and spool
restart persistence, same-PID warm starts, parallel instance isolation, concurrent
cold launches of two separate stacks, replacement of dead and unresponsive nodes,
private-node shutdown, prefix-scoped `--fresh`, restoration of missing search
indices, SIGTERM during upgrades, closed owned ports, stale discovery recovery,
and explicit manager shutdown. It always uses a temporary shared state directory
and stops that node in teardown, including after failures. Each case's verdict
is appended and flushed immediately to `build/stress/results.jsonl`. Re-running
skips already recorded cases, including failures; use a new `--data-dir` or remove
that results file to rerun them. Results and service logs are retained on failure.
Use a port base with two free consecutive ports (default 18090).

```bash
tail -f metadata-service/gms-lite/build/stress/results.jsonl
python3 metadata-service/gms-lite/tests/summarize.py \
  metadata-service/gms-lite/build/stress/results.jsonl
```

## Environment contract

All settings below are read by
`metadata-service/configuration/src/main/resources/application.yaml`, except the
explicit Java/Spring bindings noted in the final column. Both GMS and upgrades
receive this environment; an option applying to only one application is inert in
the other. Unlisted inherited or user-supplied settings keep their normal OSS
meaning.

| Variable                                                   | Launcher value                                                       | Reader                                                                 |
| ---------------------------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `EBEAN_DATASOURCE_URL`                                     | `jdbc:h2:file:<data-dir>/<instance>/gms;MODE=MySQL`                  | `ebean.url`                                                            |
| `EBEAN_DATASOURCE_DRIVER`                                  | `org.h2.Driver`                                                      | `ebean.driver`                                                         |
| `EBEAN_DATASOURCE_USERNAME`                                | `sa`                                                                 | `ebean.username`                                                       |
| `EBEAN_DATASOURCE_PASSWORD`                                | empty                                                                | `ebean.password`                                                       |
| `EBEAN_AUTOCREATE`                                         | true only for SystemUpdate on a missing H2 database; otherwise false | `ebean.autoCreateDdl`                                                  |
| `DATAHUB_SQL_SETUP_ENABLED`                                | false                                                                | `SqlSetupCondition` Java                                               |
| `DATAHUB_MESSAGING_TRANSPORT`                              | `pgqueue`                                                            | `datahub.messaging.transport`                                          |
| `DATAHUB_PGQUEUE_ENABLED`                                  | true                                                                 | `postgres.pgQueue.enabled`                                             |
| `DATAHUB_PGQUEUE_STORE`                                    | `inmemory`                                                           | `postgres.pgQueue.store`                                               |
| `DATAHUB_PGQUEUE_INMEMORY_SPOOL_FILE`                      | `<data-dir>/<instance>/queue-spool.json`                             | `postgres.pgQueue.inmemorySpoolFile`                                   |
| `SCHEMA_REGISTRY_TYPE`                                     | `INTERNAL`                                                           | `kafka.schemaRegistry.type`                                            |
| `KAFKA_SCHEMAREGISTRY_URL`                                 | `http://localhost:<gms-port>/schema-registry/api`                    | `kafka.schemaRegistry.url`                                             |
| `MAE_CONSUMER_ENABLED`                                     | true                                                                 | `maeConsumer.enabled`                                                  |
| `MCE_CONSUMER_ENABLED`                                     | true                                                                 | `MetadataChangeProposalProcessorCondition` and related Java conditions |
| `PE_CONSUMER_ENABLED`                                      | true                                                                 | `PlatformEventProcessorCondition` Java                                 |
| `DATAHUB_OBJECT_STORAGE_URI`                               | instance-local `file:` URI                                           | `datahub.objectStorage.uri`                                            |
| `ELASTICSEARCH_HOST`                                       | `localhost`                                                          | `elasticsearch.host`                                                   |
| `ELASTICSEARCH_PORT`                                       | embedded HTTP port                                                   | `elasticsearch.port`                                                   |
| `ELASTICSEARCH_SHIM_ENGINE_TYPE`                           | `OPENSEARCH_3`                                                       | `elasticsearch.clusters.primary.shim.engineType`                       |
| `ELASTICSEARCH_SHIM_AUTO_DETECT`                           | false                                                                | `elasticsearch.clusters.primary.shim.autoDetectEngine`                 |
| `ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED` | true                                                                 | `elasticsearch.entityIndex.v2.docIds.schemaField.hashIdEnabled`        |
| `INDEX_PREFIX`                                             | path hash + instance name                                            | `elasticsearch.index.prefix`                                           |
| `GRAPH_SERVICE_IMPL`                                       | `elasticsearch`                                                      | `graphService.type`                                                    |
| `ELASTICSEARCH_NUM_SHARDS_PER_INDEX`                       | 1                                                                    | `elasticsearch.clusters.primary.index.numShards`                       |
| `ELASTICSEARCH_NUM_REPLICAS_PER_INDEX`                     | 0                                                                    | `elasticsearch.clusters.primary.index.numReplicas`                     |
| `SKIP_CREATE_USAGE_EVENT_INDICES_STEP`                     | true                                                                 | `CreateUsageEventIndicesStep` Java                                     |
| `BOOTSTRAP_SYSTEM_UPDATE_WAIT_FOR_SYSTEM_UPDATE`           | false                                                                | `bootstrap.waitForSystemUpdate`                                        |
| `METADATA_SERVICE_AUTH_ENABLED`                            | true; false with `--no-auth`                                         | `authentication.enabled`                                               |
| `POLICY_CACHE_REFRESH_INTERVAL_SECONDS`                    | 10                                                                   | `authorization.defaultAuthorizer.cacheRefreshIntervalSecs`             |
| `DATAHUB_SYSTEM_CLIENT_ID`                                 | `__datahub_system`                                                   | `authentication.systemClientId`                                        |
| `DATAHUB_SYSTEM_CLIENT_SECRET`                             | random, persisted                                                    | `authentication.systemClientSecret`                                    |
| `DATAHUB_TOKEN_SERVICE_SIGNING_KEY`                        | random, persisted                                                    | `authentication.tokenService.signingKey`                               |
| `DATAHUB_TOKEN_SERVICE_SALT`                               | random, persisted                                                    | `authentication.tokenService.salt`                                     |
| `ENTITY_REGISTRY_CONFIG_PATH`                              | OSS entity-registry YAML                                             | `entityRegistry.path`                                                  |
| `DATAHUB_GMS_HOST`                                         | `localhost`                                                          | `datahub.gms.host`                                                     |
| `DATAHUB_GMS_PORT`                                         | instance HTTP port                                                   | `datahub.gms.port`                                                     |
| `SERVER_PORT`                                              | instance HTTP port                                                   | Spring Boot `server.port`                                              |
| `SERVER_ADDRESS`                                           | `127.0.0.1`                                                          | Spring Boot `server.address`                                           |
| `SPRING_PROFILES_ACTIVE`                                   | existing profiles plus `upgrade`, upgrade JVMs only                  | Spring Boot profile binding                                            |
