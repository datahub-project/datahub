# Docker Compose Profiles

This directory contains a set of docker compose definitions which are designed to run several configurations
for quickstart use-cases as well as development use-cases. These configurations cover a few of the wide variety of
infrastructure configurations that DataHub can operate on.

Requirements:

- Using profiles requires docker compose >= 2.20.
- If using the debug/development profiles, you will need to have built the `debug` docker images locally. See the Development Profiles section for more details.

```bash
$ cd docker/profiles
$ docker compose --profile <profile name> up
```

Alternatively, you can use the gradle tasks defined in `docker/build.gradle`:

```bash
# Run from the project root
./gradlew quickstart          # Uses the 'quickstart' profile
./gradlew quickstartDebug     # Uses the 'debug' profile
```

Use Control-c (`^c`) to terminate the running system. This will automatically stop all running containers.

To remove the containers and volumes, you can use the gradle nuke tasks:

```bash
# Remove containers and volumes for specific projects
./gradlew quickstartNuke          # For default project (datahub)
./gradlew quickstartDebugNuke     # For debug project (datahub)
```

Alternatively, you can use docker compose directly:

```bash
docker compose --profile <profile name> rm
```

Please refer to docker's documentation for more details.

The following sections detail a few of the profiles and their intended use-cases. For a complete list of profiles
and their configuration please see the table at the end of each section.

## Quickstart Profiles

Quickstart profiles are primarily a way to test drive DataHub features before committing to a production ready deployment.
A couple of these profiles are also used in our continuous integration (CI) tests.

**Search engine names:** Unversioned profiles (`quickstart`, `debug`) always use the **current
default** OpenSearch (today **2.x**). Versioned Gradle keys pin an engine across a future default
change: `./gradlew quickstartOS2` / `quickstartOS2Debug` stay on 2.x; `quickstartOS3` /
`quickstartOS3Debug` stay on 3.x. Do not treat `quickstart` as an alias of `quickstartOS3`.

Note: Quickstart profiles use docker images with the coordinated `quickstart` tag (updated together after smoke tests on `master`). This can be overridden to use a stable release tag by prefixing commands with `DATAHUB_VERSION=v0.12.1`, or to pin a specific build with `DATAHUB_VERSION=sha-<short_sha>`.

### `quickstart`

This is the default configuration MySQL and OpenSearch for the storage and GMS running with integrated consumers.

### `quickstart-consumers`

This configuration is identical to `quickstart` how it runs standalone consumers instead of consumers integrated with the GMS container.

### `quickstart-postgres`

Like `quickstart` with Postgres instead of MySQL. Uses pgQueue instead of Kafka for messaging (`DATAHUB_MESSAGING_TRANSPORT=pgqueue`). OpenSearch is still used for search/graph/timeseries.

### `quickstart-opensearch2`

Same application set as image-based `quickstart` (MySQL, Kafka, frontend, GMS, actions) with
**OpenSearch 2.x** explicitly pinned (`opensearchproject/opensearch:2.19.3`, volume `osdata`).
Unversioned `quickstart` uses this engine today; use `./gradlew quickstartOS2` when you must stay
on 2.x after the default flips to 3.x.

```bash
./gradlew quickstartOS2
```

### `quickstart-opensearch3`

Same application set as `quickstart` (MySQL, Kafka, frontend, GMS, actions), but the search
backend is **OpenSearch 3.7** (`opensearchproject/opensearch:3.7.0`; override with
`DATAHUB_OS3_SEARCH_IMAGE` / `DATAHUB_OS3_SEARCH_TAG`). GMS pins
`ELASTICSEARCH_SHIM_ENGINE_TYPE=OPENSEARCH_3` and enables schema-field document-id hashing
because 3.x enforces the 512-byte `_id` limit. Uses a separate volume from OpenSearch 2.x
(`os3data`). This is the image-based profile for CI (`./gradlew quickstartOS3`). For local
bind-mount development, use `debug-opensearch3` / `./gradlew quickstartOS3Debug`.

```bash
./gradlew quickstartOS3
```

### `quickstart-cassandra`

Uses Cassandra as the primary data store along with Neo4j as the graph database.

### `quickstart-storage`

Just run the `quickstart` data stores without the DataHub components. This mode is useful for debugging when running the frontend and GMS components outside
of docker.

### Quickstart Profiles Table

| Profile Name           | MySQL | Postgres | Cassandra | Neo4j | Frontend | GMS | Actions | SystemUpdate | MAE | MCE | Kafka | OpenSearch |
| ---------------------- | ----- | -------- | --------- | ----- | -------- | --- | ------- | ------------ | --- | --- | ----- | ---------- |
| quickstart             | X     |          |           |       | X        | X   | X       | X            |     |     | X     | X          |
| quickstart-frontend    | X     |          |           |       | X        |     |         | X            |     |     | X     | X          |
| quickstart-backend     | X     |          |           |       |          | X   | X       | X            |     |     | X     | X          |
| quickstart-postgres    |       | X        |           |       | X        | X   | X       | X            |     |     |       | X          |
| quickstart-opensearch2 | X     |          |           |       | X        | X   | X       | X            |     |     | X     | 2.x        |
| quickstart-opensearch3 | X     |          |           |       | X        | X   | X       | X            |     |     | X     | 3.x        |
| quickstart-cassandra   |       |          | X         | X     | X        | X   | X       | X            |     |     | X     | X          |
| quickstart-consumers   | X     |          |           |       | X        | X   | X       | X            | X   | X   | X     | X          |
| quickstart-storage     | X     |          |           |       |          |     |         |              |     |     | X     | X          |

## Development Profiles

- Runs `debug` tagged images
- JVM Debug Mode Enabled
- Exposes local jars and scripts to the containers
- Can run non-default one-off configurations (neo4j, cassandra, elasticsearch)
- Micrometer Actuator (Prometheus scrape and health) listens on container port **4319** by default (`MANAGEMENT_SERVER_PORT` in `start.sh`), separate from the main HTTP port. Compose **`expose`s 4319** for on-network scraping; **GMS** also publishes **`${DATAHUB_MAPPED_GMS_MANAGEMENT_PORT:-4319}:4319`** on the host, parallel to **`${DATAHUB_MAPPED_GMS_PORT:-8080}:8080`**. `./gradlew quickstartDebug` uses compose in **this directory**, not `docker/quickstart/` alone.

The docker images used are the `debug` images which are created by building locally. These images are
created by running the gradle command.

```bash
./gradlew dockerTagDebug
```

Debug GMS, system-update, MAE, and actions bind-mount `~/.aws` read-only, with `~/.aws/sso/cache` writable so SSO tokens can refresh. For a named or SSO profile, export `AWS_PROFILE` in the host environment (or `scripts/dev/datahub-dev.sh env set AWS_PROFILE=...`) before start; Compose also sets `AWS_SDK_LOAD_CONFIG=1`. Unset `AWS_PROFILE` keeps the SDK default-profile / env-credential chain.

Debug GMS, system-update, and the MAE and MCE consumers also get the host GCP session. `~/.config/gcloud` is mounted read-only, and an external service-account key in `GOOGLE_APPLICATION_CREDENTIALS` is mounted at its host path. All four build the embedding provider when they start, and with `EMBEDDING_PROVIDER_TYPE=vertex_ai` that resolves Application Default Credentials immediately, so a service without the session fails at startup (system-update failing keeps GMS from starting). GMS uses the AWS or GCP session when it embeds queries with Bedrock or Vertex; MAE also uses it for object storage (`DATAHUB_OBJECT_STORAGE_URI` on `s3://` or `gs://`). The containers have no `gcloud` CLI, so set the project explicitly (`GOOGLE_CLOUD_PROJECT`, and `VERTEX_AI_PROJECT_ID` for the Vertex provider). All four call Bedrock in `BEDROCK_EMBEDDING_AWS_REGION` when set, then `BEDROCK_AWS_REGION`, then `AWS_REGION`, otherwise `us-west-2`.

`scripts/dev/datahub-dev.sh` passes these settings to Compose, because Compose reads mount sources and pass-through values from the shell, not from env files. A value exported in the shell wins over one saved with `env set`, and an empty saved value is ignored. A per-service env file (`DATAHUB_LOCAL_GMS_ENV` and so on) cannot set these names, because a name Compose passes through hides the same name in every env file. On a host with no `~/.aws` or `~/.config/gcloud`, `datahub-dev` mounts an empty placeholder from `~/.datahub/dev/` instead, so the stack still starts. These steps run only through `datahub-dev`: when you run Gradle or `docker compose` directly, Docker creates any missing directory as root. Set `DATAHUB_DEV_AWS_DIR` / `DATAHUB_DEV_GCLOUD_DIR` yourself to avoid that.

For a complete list of profiles see the table at the end of this section.

### `quickstart-backend`

Run everything except for the `frontend` component. Useful for running just a local (non-docker) frontend.

### `quickstart-frontend`

Runs everything except for the GMS. Useful for running just a local (non-docker) GMS instance.

### `debug-opensearch2`

Same application set as `debug`, with OpenSearch **2.x** pinned. Use
`./gradlew quickstartOS2Debug` to keep a bind-mount stack on 2.x after unversioned `debug` moves to
3.x.

```bash
./gradlew quickstartOS2Debug
```

### `debug-opensearch3`

Same application set as `debug`, but the search backend is **OpenSearch 3.7**
(`opensearchproject/opensearch:3.7.0`; override with `DATAHUB_OS3_SEARCH_IMAGE` /
`DATAHUB_OS3_SEARCH_TAG`). GMS pins `ELASTICSEARCH_SHIM_ENGINE_TYPE=OPENSEARCH_3` and enables
schema-field document-id hashing because 3.x enforces the 512-byte `_id` limit. Uses a separate
volume from OpenSearch 2.x (`os3data`). Prefer this for local development; CI should use
`quickstart-opensearch3` / `./gradlew quickstartOS3` so system-update runs from the image JAR
instead of a host bind-mount.

```bash
./gradlew quickstartOS3Debug
```

### Development Profiles Table

| Profile Name             | MySQL | Postgres | Cassandra | Neo4j | Frontend | GMS | Actions | SystemUpdate | MAE | MCE | Kafka | OpenSearch | Elasticsearch | Localstack (AWS) |
| ------------------------ | ----- | -------- | --------- | ----- | -------- | --- | ------- | ------------ | --- | --- | ----- | ---------- | ------------- | ---------------- |
| debug                    | X     |          |           |       | X        | X   | X       | X            |     |     | X     | X          |               |                  |
| debug-frontend           | X     |          |           |       | X        |     |         | X            |     |     | X     | X          |               |                  |
| debug-backend            | X     |          |           |       |          | X   | X       | X            |     |     | X     | X          |               |                  |
| debug-postgres           |       | X        |           |       | X        | X   | X       | X            |     |     |       | X          |               |                  |
| debug-postgres-consumers |       | X        |           |       | X        | X   | X       | X            | X   | X   |       | X          |               |                  |
| debug-cassandra          |       |          | X         |       | X        | X   | X       | X            |     |     | X     | X          |               |                  |
| debug-consumers          | X     |          |           |       | X        | X   | X       | X            | X   | X   | X     | X          |               |                  |
| debug-neo4j              | X     |          |           | X     | X        | X   | X       | X            |     |     | X     | X          |               |                  |
| debug-elasticsearch      | X     |          |           |       | X        | X   | X       | X            |     |     | X     |            | X             |                  |
| debug-opensearch2        | X     |          |           |       | X        | X   | X       | X            |     |     | X     | 2.x        |               |                  |
| debug-opensearch3        | X     |          |           |       | X        | X   | X       | X            |     |     | X     | 3.x        |               |                  |
| debug-backend-aws        | X     |          |           |       |          | X   | X       | X            |     |     | X     | X          |               | X                |

## Advanced Setups

### Search V3 on a second OpenSearch cluster

`docker-compose.search-split-override.yml` adds a second OpenSearch container, `search-secondary` (host port
`${DATAHUB_MAPPED_ELASTIC_SECONDARY_PORT:-9201}`), and points GMS, system update and the MAE consumer at it for
Search V3 (`ELASTICSEARCH_CLUSTERS_SECONDARY_URI`, `ELASTICSEARCH_COMPONENT_CLUSTER_SEARCH_V3=secondary`). V2
entity indices stay on `search`. No base file includes it; layer it explicitly:

```shell
# Debug stack with the second cluster (same modules as quickstartDebug)
./gradlew quickstartDebugSearchSplit
# Raw compose, any profile
cd docker/profiles
docker compose -f docker-compose.yml -f docker-compose.search-split-override.yml --profile debug up -d
```

The override supplies the cluster and the routing only. Enable V3 with the usual variables
(`ELASTICSEARCH_ENTITY_INDEX_V3_ENABLED=true` to build and dual-write the V3 indices, later
`ELASTICSEARCH_ENTITY_INDEX_V3_KEYWORD_READ_ENABLED=true` to serve keyword reads from them). Run it while no other
debug stack is up, or pass the same `-p <project>`, so the two do not compete for ports and networks.

The automated equivalent is `./gradlew :datahub-upgrade:searchV3MigrationTest`: OpenSearch testcontainers run the
cutover end to end (Phase 1, live dual-writes, resumable fill, catch-up, read flip, a later mapping change and a
stale-id rebuild) with V3 on a separate cluster and on the same cluster, plus a V3-only fresh install. It is opt-in
and not part of CI.

### Version Mixing

In some cases, it might be useful to debug upgrade scenarios where there are intentional version miss-matches. It is possible
to override individual component versions.

Note: This only works for `non-debug` profiles because of the file mounts when in `debug` which would run older containers
but still pickup the latest application jars.

In this example we are interested in upgrading two components (the `mae-consumer` and the `mce-consumer`) to a fresh build `v0.15.1-SNAPSHOT`
while maintaining older components on `v0.14.1` (especially the `system-update` container).

This configuration reproduces the situation where the consumers were upgraded prior to running the latest version of `system-update`. In this
scenario we expect the consumers to block their startup waiting for the successful completion of a newer `system-update`.

`DATAHUB_VERSION` - specifies the default component version of `v0.14.1`
`DATAHUB_MAE_VERSION` - specifies an override of just the `mae-consumer` to version `v0.15.1-SNAPSHOT`[1]
`DATAHUB_MCE_VERSION` - specifies an override of just the `mce-consumer` to version `v0.15.1-SNAPSHOT`[1]

```shell
 DATAHUB_MAE_VERSION="v0.15.1-SNAPSHOT" DATAHUB_MCE_VERSION="v0.15.1-SNAPSHOT" DATAHUB_VERSION="v0.14.1" ./gradlew quickstart
```

[1] Image versions were `v0.15.1-SNAPSHOT` built locally prior to running the command.
