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
A dead node or a verified node whose connection is refused is replaced while
retaining its data; a different major version or an unrelated PID is refused.
For a live node, readiness waits up to 600 seconds for yellow health. Override
this with `GMS_LITE_OPENSEARCH_HEALTH_TIMEOUT_SECONDS`. An unresponsive or slow
node that reaches the deadline causes a clear error and is left running, with
its recorded PID and port unchanged. This also applies to a newly started node:
once it reports `READY`, its port is recorded before waiting for yellow. Retry
`start` to reuse it and wait again. A child that exits, fails to report `READY`
within 900 seconds, or reports the wrong major version is stopped and its PID/port
files are removed. Interrupting startup also cleans up the new child; an already
running node being reused is left alone. Data is always retained; choose a
different state directory or repair the data explicitly if it cannot recover.
`status` snapshots PID/port under the lock, then probes outside it with one-second
HTTP timeouts and no readiness wait. It returns JSON with `healthy: false` and a
short `reason` for a red, paused, unreachable, or incompatible node.
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
and then pins its local connection settings. Search URI, credentials, TLS,
proxy, routing, and per-cluster prefix overrides are neutralised for both GMS
and upgrades so the embedded node and instance prefix always win. The full
variable-to-YAML mapping appears in the environment contract below. The generated `gms-N/env.sh` records settings
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
  "frontendPort": 19002,
  "systemClientId": "__datahub_system",
  "systemClientSecret": "<system-client-secret>",
  "adminTokens": {
    "gms-0": "<personal-access-token>",
    "gms-1": "<personal-access-token>"
  },
  "adminTokenExpiresAt": {
    "gms-0": "2026-01-02T12:00:00Z",
    "gms-1": "2026-01-02T12:00:01Z"
  }
}
```

`opensearchShared` says whether stopping this launcher leaves the node running.
`default` is a key in `instances`. `indexPrefixes` contains the configured
`INDEX_PREFIX` values; GMS appends an underscore before index names. `bootId`
changes on every start. `adminTokens` contains a one-day personal token for the
existing bootstrap user `datahub` in each instance. `adminTokenExpiresAt` maps
the same instance names to the server-issued token expiry in ISO-8601 UTC.
After expiry, authenticated requests return 401 even if `/health` still succeeds.
Restart the stack with the same data directory to mint new tokens, then reread
`stack.json`; tokens are not refreshed while the launcher runs. The launcher uses OSS's
shipped bootstrap policies without adding or replacing any policy.

`--frontend-port` publishes `frontendPort` (otherwise null). With authentication enabled,
`systemClientId` and `systemClientSecret` let Vite's lite mode verify native credentials
and mint browser sessions through the same GMS auth APIs as Play; both are null with
`--no-auth`. These fields are private, like `adminTokens`. The launcher itself does not
start a frontend; `scripts/dev/datahub-dev.sh start --local` starts and manages Vite.

Pass the instance token as `Authorization: Bearer <token>`, or use it with the
DataHub CLI as `DATAHUB_GMS_TOKEN` alongside `DATAHUB_GMS_URL`. `/health` and
`/config` are public. For example:

```bash
curl -fsS http://localhost:18090/health
curl -fsS http://localhost:18090/config
```

`--no-auth` disables token authentication and publishes `auth: false` with empty
`adminTokens` and `adminTokenExpiresAt` objects. GMS binds to loopback. The system credentials and token keys
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
after both upgrade commands succeed; it records the instance's non-usage-event
index names observed at that point. Existing H2 tables are never recreated on a
retry. Upgrades are skipped only when H2 exists, this marker contains a nonempty
recorded index set, and every recorded index still exists. If any recorded index
is missing, both upgrades run again to restore search from H2. The usage-event
index is ensured separately on every start. Older text markers trigger one
upgrade run to record the set. Remove the marker to rerun upgrades after changing
bootstrap code.

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
cold launches of two separate stacks, replacement of dead nodes, timeout without
replacement of a paused live node, private-node shutdown, prefix-scoped `--fresh`,
restoration after total or single-core-index loss, SIGTERM during upgrades, closed owned ports, stale discovery recovery,
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

### Neutralised search overrides

These values override both the shell and `--env-file`. Each YAML property is
protected by clearing or pinning both its placeholder names and its canonical
environment name. Names are generated from the paths below using
[Spring Boot's environment mapping](https://docs.spring.io/spring-boot/reference/features/external-config.html#features.external-config.typesafe-configuration-properties.relaxed-binding.environment-variables):
replace dots with underscores, remove dashes, uppercase, and surround list indices
with underscores. Camel-case words stay joined (e.g. `pathPrefix` becomes
`PATHPREFIX`). `same` means the canonical name is already a listed placeholder.

Empty URI values let `ElasticSearchConfiguration.normalizeClusters()` synthesize
the primary URI from the pinned host/port and leave the secondary unconfigured.
`elasticsearch.host` and `.port` already map to `ELASTICSEARCH_HOST` and
`ELASTICSEARCH_PORT`, pinned to `localhost` and the embedded port. Neither the YAML
nor `SearchClusterSettings` defines `clusters.primary.host` or `.port`; nested
host/port names cannot override the connection. Boolean switches use `false`;
proxy schemes use `http`.

| YAML key                                                                 | Placeholder name(s)                                                             | Canonical env name                                                       | Value     |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | --------- |
| `elasticsearch.pathPrefix`                                               | `ELASTICSEARCH_PATH_PREFIX`                                                     | `ELASTICSEARCH_PATHPREFIX`                                               | empty     |
| `elasticsearch.useSSL`                                                   | `ELASTICSEARCH_USE_SSL`                                                         | `ELASTICSEARCH_USESSL`                                                   | `false`   |
| `postgres.pgCron.iam.awsRegion`                                          | `AWS_REGION`                                                                    | `POSTGRES_PGCRON_IAM_AWSREGION`                                          | empty     |
| `elasticsearch.clusters.primary.uri`                                     | `ELASTICSEARCH_URI`                                                             | `ELASTICSEARCH_CLUSTERS_PRIMARY_URI`                                     | empty     |
| `elasticsearch.clusters.primary.username`                                | `ELASTICSEARCH_USERNAME`                                                        | `ELASTICSEARCH_CLUSTERS_PRIMARY_USERNAME`                                | empty     |
| `elasticsearch.clusters.primary.password`                                | `ELASTICSEARCH_PASSWORD`                                                        | `ELASTICSEARCH_CLUSTERS_PRIMARY_PASSWORD`                                | empty     |
| `elasticsearch.clusters.primary.region`                                  | `AWS_REGION`                                                                    | `ELASTICSEARCH_CLUSTERS_PRIMARY_REGION`                                  | empty     |
| `elasticsearch.clusters.primary.opensearchUseAwsIamAuth`                 | `OPENSEARCH_USE_AWS_IAM_AUTH`                                                   | `ELASTICSEARCH_CLUSTERS_PRIMARY_OPENSEARCHUSEAWSIAMAUTH`                 | `false`   |
| `elasticsearch.clusters.secondary.uri`                                   | `ELASTICSEARCH_CLUSTERS_SECONDARY_URI`                                          | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.username`                              | `ELASTICSEARCH_CLUSTERS_SECONDARY_USERNAME`                                     | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.password`                              | `ELASTICSEARCH_CLUSTERS_SECONDARY_PASSWORD`                                     | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.region`                                | `ELASTICSEARCH_CLUSTERS_SECONDARY_REGION`                                       | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.opensearchUseAwsIamAuth`               | `ELASTICSEARCH_CLUSTERS_SECONDARY_OPENSEARCH_USE_AWS_IAM_AUTH`                  | `ELASTICSEARCH_CLUSTERS_SECONDARY_OPENSEARCHUSEAWSIAMAUTH`               | `false`   |
| `elasticsearch.clusters.primary.sslContext.protocol`                     | `ELASTICSEARCH_SSL_PROTOCOL`                                                    | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_PROTOCOL`                     | empty     |
| `elasticsearch.clusters.primary.sslContext.secureRandomImplementation`   | `ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL`                                          | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_SECURERANDOMIMPLEMENTATION`   | empty     |
| `elasticsearch.clusters.primary.sslContext.trustStoreFile`               | `ELASTICSEARCH_SSL_TRUSTSTORE_FILE`                                             | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_TRUSTSTOREFILE`               | empty     |
| `elasticsearch.clusters.primary.sslContext.trustStoreType`               | `ELASTICSEARCH_SSL_TRUSTSTORE_TYPE`                                             | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_TRUSTSTORETYPE`               | empty     |
| `elasticsearch.clusters.primary.sslContext.trustStorePassword`           | `ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD`                                         | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_TRUSTSTOREPASSWORD`           | empty     |
| `elasticsearch.clusters.primary.sslContext.keyStoreFile`                 | `ELASTICSEARCH_SSL_KEYSTORE_FILE`                                               | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_KEYSTOREFILE`                 | empty     |
| `elasticsearch.clusters.primary.sslContext.keyStoreType`                 | `ELASTICSEARCH_SSL_KEYSTORE_TYPE`                                               | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_KEYSTORETYPE`                 | empty     |
| `elasticsearch.clusters.primary.sslContext.keyStorePassword`             | `ELASTICSEARCH_SSL_KEYSTORE_PASSWORD`                                           | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_KEYSTOREPASSWORD`             | empty     |
| `elasticsearch.clusters.primary.sslContext.keyPassword`                  | `ELASTICSEARCH_SSL_KEY_PASSWORD`                                                | `ELASTICSEARCH_CLUSTERS_PRIMARY_SSLCONTEXT_KEYPASSWORD`                  | empty     |
| `elasticsearch.clusters.secondary.sslContext.protocol`                   | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_PROTOCOL`                                 | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_PROTOCOL`                   | empty     |
| `elasticsearch.clusters.secondary.sslContext.secureRandomImplementation` | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_SECURE_RANDOM_IMPL`                       | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_SECURERANDOMIMPLEMENTATION` | empty     |
| `elasticsearch.clusters.secondary.sslContext.trustStoreFile`             | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_TRUSTSTORE_FILE`                          | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_TRUSTSTOREFILE`             | empty     |
| `elasticsearch.clusters.secondary.sslContext.trustStoreType`             | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_TRUSTSTORE_TYPE`                          | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_TRUSTSTORETYPE`             | empty     |
| `elasticsearch.clusters.secondary.sslContext.trustStorePassword`         | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_TRUSTSTORE_PASSWORD`                      | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_TRUSTSTOREPASSWORD`         | empty     |
| `elasticsearch.clusters.secondary.sslContext.keyStoreFile`               | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_KEYSTORE_FILE`                            | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_KEYSTOREFILE`               | empty     |
| `elasticsearch.clusters.secondary.sslContext.keyStoreType`               | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_KEYSTORE_TYPE`                            | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_KEYSTORETYPE`               | empty     |
| `elasticsearch.clusters.secondary.sslContext.keyStorePassword`           | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_KEYSTORE_PASSWORD`                        | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_KEYSTOREPASSWORD`           | empty     |
| `elasticsearch.clusters.secondary.sslContext.keyPassword`                | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSL_KEY_PASSWORD`                             | `ELASTICSEARCH_CLUSTERS_SECONDARY_SSLCONTEXT_KEYPASSWORD`                | empty     |
| `elasticsearch.clusters.primary.proxy.host`                              | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_HOST`, `ELASTICSEARCH_PROXY_HOST`         | same                                                                     | empty     |
| `elasticsearch.clusters.primary.proxy.port`                              | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_PORT`, `ELASTICSEARCH_PROXY_PORT`         | same                                                                     | empty     |
| `elasticsearch.clusters.primary.proxy.scheme`                            | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_SCHEME`, `ELASTICSEARCH_PROXY_SCHEME`     | same                                                                     | `http`    |
| `elasticsearch.clusters.primary.proxy.username`                          | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_USERNAME`, `ELASTICSEARCH_PROXY_USERNAME` | same                                                                     | empty     |
| `elasticsearch.clusters.primary.proxy.password`                          | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_PASSWORD`, `ELASTICSEARCH_PROXY_PASSWORD` | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.proxy.host`                            | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_HOST`                                   | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.proxy.port`                            | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_PORT`                                   | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.proxy.scheme`                          | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_SCHEME`                                 | same                                                                     | `http`    |
| `elasticsearch.clusters.secondary.proxy.username`                        | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_USERNAME`                               | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.proxy.password`                        | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_PASSWORD`                               | same                                                                     | empty     |
| `elasticsearch.clusters.primary.proxy.useSystemProxyProperties`          | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_USE_SYSTEM_PROXY_PROPERTIES`              | `ELASTICSEARCH_CLUSTERS_PRIMARY_PROXY_USESYSTEMPROXYPROPERTIES`          | `false`   |
| `elasticsearch.clusters.primary.index.prefix`                            | `ELASTICSEARCH_CLUSTERS_PRIMARY_INDEX_PREFIX`                                   | same                                                                     | empty     |
| `elasticsearch.clusters.secondary.proxy.useSystemProxyProperties`        | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_USE_SYSTEM_PROXY_PROPERTIES`            | `ELASTICSEARCH_CLUSTERS_SECONDARY_PROXY_USESYSTEMPROXYPROPERTIES`        | `false`   |
| `elasticsearch.clusters.secondary.index.prefix`                          | `ELASTICSEARCH_CLUSTERS_SECONDARY_INDEX_PREFIX`                                 | same                                                                     | empty     |
| `elasticsearch.componentCluster.searchV2`                                | `ELASTICSEARCH_COMPONENT_CLUSTER_SEARCH_V2`                                     | `ELASTICSEARCH_COMPONENTCLUSTER_SEARCHV2`                                | `primary` |
| `elasticsearch.componentCluster.searchV3`                                | `ELASTICSEARCH_COMPONENT_CLUSTER_SEARCH_V3`                                     | `ELASTICSEARCH_COMPONENTCLUSTER_SEARCHV3`                                | `primary` |
| `elasticsearch.componentCluster.semantic`                                | `ELASTICSEARCH_COMPONENT_CLUSTER_SEMANTIC`                                      | `ELASTICSEARCH_COMPONENTCLUSTER_SEMANTIC`                                | `primary` |
| `elasticsearch.componentCluster.graph`                                   | `ELASTICSEARCH_COMPONENT_CLUSTER_GRAPH`                                         | `ELASTICSEARCH_COMPONENTCLUSTER_GRAPH`                                   | `primary` |
| `elasticsearch.componentCluster.timeseries`                              | `ELASTICSEARCH_COMPONENT_CLUSTER_TIMESERIES`                                    | `ELASTICSEARCH_COMPONENTCLUSTER_TIMESERIES`                              | `primary` |
| `elasticsearch.componentCluster.systemMetadata`                          | `ELASTICSEARCH_COMPONENT_CLUSTER_SYSTEM_METADATA`                               | `ELASTICSEARCH_COMPONENTCLUSTER_SYSTEMMETADATA`                          | `primary` |
| `elasticsearch.componentCluster.usage`                                   | `ELASTICSEARCH_COMPONENT_CLUSTER_USAGE`                                         | `ELASTICSEARCH_COMPONENTCLUSTER_USAGE`                                   | `primary` |
| `elasticsearch.clusterMigration.graph.targetCluster`                     | `ELASTICSEARCH_CLUSTER_MIGRATION_GRAPH_TARGET_CLUSTER`                          | `ELASTICSEARCH_CLUSTERMIGRATION_GRAPH_TARGETCLUSTER`                     | empty     |
| `elasticsearch.clusterMigration.graph.copy.remote.host`                  | `ELASTICSEARCH_CLUSTER_MIGRATION_GRAPH_COPY_REMOTE_HOST`                        | `ELASTICSEARCH_CLUSTERMIGRATION_GRAPH_COPY_REMOTE_HOST`                  | empty     |
| `elasticsearch.clusterMigration.graph.copy.remote.username`              | `ELASTICSEARCH_CLUSTER_MIGRATION_GRAPH_COPY_REMOTE_USERNAME`                    | `ELASTICSEARCH_CLUSTERMIGRATION_GRAPH_COPY_REMOTE_USERNAME`              | empty     |
| `elasticsearch.clusterMigration.graph.copy.remote.password`              | `ELASTICSEARCH_CLUSTER_MIGRATION_GRAPH_COPY_REMOTE_PASSWORD`                    | `ELASTICSEARCH_CLUSTERMIGRATION_GRAPH_COPY_REMOTE_PASSWORD`              | empty     |
| `elasticsearch.clusterMigration.timeseries.targetCluster`                | `ELASTICSEARCH_CLUSTER_MIGRATION_TIMESERIES_TARGET_CLUSTER`                     | `ELASTICSEARCH_CLUSTERMIGRATION_TIMESERIES_TARGETCLUSTER`                | empty     |
| `elasticsearch.clusterMigration.timeseries.copy.remote.host`             | `ELASTICSEARCH_CLUSTER_MIGRATION_TIMESERIES_COPY_REMOTE_HOST`                   | `ELASTICSEARCH_CLUSTERMIGRATION_TIMESERIES_COPY_REMOTE_HOST`             | empty     |
| `elasticsearch.clusterMigration.timeseries.copy.remote.username`         | `ELASTICSEARCH_CLUSTER_MIGRATION_TIMESERIES_COPY_REMOTE_USERNAME`               | `ELASTICSEARCH_CLUSTERMIGRATION_TIMESERIES_COPY_REMOTE_USERNAME`         | empty     |
| `elasticsearch.clusterMigration.timeseries.copy.remote.password`         | `ELASTICSEARCH_CLUSTER_MIGRATION_TIMESERIES_COPY_REMOTE_PASSWORD`               | `ELASTICSEARCH_CLUSTERMIGRATION_TIMESERIES_COPY_REMOTE_PASSWORD`         | empty     |
| `elasticsearch.clusterMigration.systemMetadata.targetCluster`            | `ELASTICSEARCH_CLUSTER_MIGRATION_SYSTEM_METADATA_TARGET_CLUSTER`                | `ELASTICSEARCH_CLUSTERMIGRATION_SYSTEMMETADATA_TARGETCLUSTER`            | empty     |
| `elasticsearch.clusterMigration.systemMetadata.copy.remote.host`         | `ELASTICSEARCH_CLUSTER_MIGRATION_SYSTEM_METADATA_COPY_REMOTE_HOST`              | `ELASTICSEARCH_CLUSTERMIGRATION_SYSTEMMETADATA_COPY_REMOTE_HOST`         | empty     |
| `elasticsearch.clusterMigration.systemMetadata.copy.remote.username`     | `ELASTICSEARCH_CLUSTER_MIGRATION_SYSTEM_METADATA_COPY_REMOTE_USERNAME`          | `ELASTICSEARCH_CLUSTERMIGRATION_SYSTEMMETADATA_COPY_REMOTE_USERNAME`     | empty     |
| `elasticsearch.clusterMigration.systemMetadata.copy.remote.password`     | `ELASTICSEARCH_CLUSTER_MIGRATION_SYSTEM_METADATA_COPY_REMOTE_PASSWORD`          | `ELASTICSEARCH_CLUSTERMIGRATION_SYSTEMMETADATA_COPY_REMOTE_PASSWORD`     | empty     |
| `elasticsearch.clusterMigration.usage.targetCluster`                     | `ELASTICSEARCH_CLUSTER_MIGRATION_USAGE_TARGET_CLUSTER`                          | `ELASTICSEARCH_CLUSTERMIGRATION_USAGE_TARGETCLUSTER`                     | empty     |
| `elasticsearch.clusterMigration.usage.copy.remote.host`                  | `ELASTICSEARCH_CLUSTER_MIGRATION_USAGE_COPY_REMOTE_HOST`                        | `ELASTICSEARCH_CLUSTERMIGRATION_USAGE_COPY_REMOTE_HOST`                  | empty     |
| `elasticsearch.clusterMigration.usage.copy.remote.username`              | `ELASTICSEARCH_CLUSTER_MIGRATION_USAGE_COPY_REMOTE_USERNAME`                    | `ELASTICSEARCH_CLUSTERMIGRATION_USAGE_COPY_REMOTE_USERNAME`              | empty     |
| `elasticsearch.clusterMigration.usage.copy.remote.password`              | `ELASTICSEARCH_CLUSTER_MIGRATION_USAGE_COPY_REMOTE_PASSWORD`                    | `ELASTICSEARCH_CLUSTERMIGRATION_USAGE_COPY_REMOTE_PASSWORD`              | empty     |
