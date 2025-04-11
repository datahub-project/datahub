# Docker Compose Profiles

This directory contains a set of docker compose definitions which are designed to run several configurations
for quickstart use-cases as well as development use-cases. These configurations cover a few of the wide variety of
infrastructure configurations that DataHub can operate on.

Requirements:
* Using profiles requires docker compose >= 2.20.
* If using the debug/development profiles, you will need to have built the `debug` docker images locally. See the Development Profiles section for more details.

```bash
$ cd docker/profiles
$ docker compose --profile <profile name> up
```

Use Control-c (`^c`) to terminate the running system. This will automatically stop all running containers.

To remove the containers use the following:

```bash
docker compose --profile <profile name> rm
```

Please refer to docker's documentation for more details.

The following sections detail a few of the profiles and their intended use-cases. For a complete list of profiles
and their configuration please see the table at the end of each section.

## Quickstart Profiles

Quickstart profiles are primarily a way to test drive DataHub features before committing to a production ready deployment.
A couple of these profiles are also used in our continuous integration (CI) tests.

Note: Quickstart profiles use docker images with the `head` tag. These images up updated when changes are committed
to the DataHub github repository. This can be overridden to use a stable release tag by prefixing the commands with 
`DATAHUB_VERSION=v0.12.1` for example.

### `quickstart`

This is the default configuration MySQL and OpenSearch for the storage and GMS running with integrated consumers.

### `quickstart-consumers`

This configuration is identical to `quickstart` how it runs standalone consumers instead of consumers integrated with the GMS container.

### `quickstart-postgres`

Identical to `quickstart` with Postgres instead of MySQL.

### `quickstart-cassandra`

Uses Cassandra as the primary data store along with Neo4j as the graph database.

### `quickstart-storage`

Just run the `quickstart` data stores without the DataHub components. This mode is useful for debugging when running the frontend and GMS components outside
of docker.

### Quickstart Profiles Table
| Profile Name         | MySQL | Postgres | Cassandra | Neo4j | Frontend | GMS | Actions | SystemUpdate | MAE | MCE | Kafka | OpenSearch |
|----------------------|-------|----------|-----------|-------|----------|-----|---------|--------------|-----|-----|-------|------------|
| quickstart           | X     |          |           |       | X        | X   | X       | X            |     |     | X     | X          |
| quickstart-frontend  | X     |          |           |       | X        |     |         | X            |     |     | X     | X          |
| quickstart-backend   | X     |          |           |       |          | X   | X       | X            |     |     | X     | X          |
| quickstart-postgres  |       | X        |           |       | X        | X   | X       | X            |     |     | X     | X          |
| quickstart-cassandra |       |          | X         | X     | X        | X   | X       | X            |     |     | X     | X          |
| quickstart-consumers | X     |          |           |       | X        | X   | X       | X            | X   | X   | X     | X          |
| quickstart-storage   | X     |          |           |       |          |     |         |              |     |     | X     | X          |

## Development Profiles

* Runs `debug` tagged images
* JVM Debug Mode Enabled
* Exposes local jars and scripts to the containers
* Can run non-default one-off configurations (neo4j, cassandra, elasticsearch)

The docker images used are the `debug` images which are created by building locally. These images are
created by running the gradle command.

```bash
./gradlew dockerTagDebug
```

For a complete list of profiles see the table at the end of this section.

### `quickstart-backend`

Run everything except for the `frontend` component. Useful for running just a local (non-docker) frontend.

### `quickstart-frontend`

Runs everything except for the GMS. Useful for running just a local (non-docker) GMS instance.

### Development Profiles Table
| Profile Name        | MySQL | Postgres | Cassandra | Neo4j | Frontend | GMS | Actions | SystemUpdate | MAE | MCE | Kafka | OpenSearch | Elasticsearch |
|---------------------|-------|----------|-----------|-------|----------|-----|---------|--------------|-----|-----|-------|------------|---------------|
| debug               | X     |          |           |       | X        | X   | X       | X            |     |     | X     | X          |               |
| debug-frontend      | X     |          |           |       | X        |     |         | X            |     |     | X     | X          |               |
| debug-backend       | X     |          |           |       |          | X   | X       | X            |     |     | X     | X          |               |
| debug-postgres      |       | X        |           |       | X        | X   | X       | X            |     |     | X     | X          |               |
| debug-cassandra     |       |          | X         |       | X        | X   | X       | X            |     |     | X     | X          |               |
| debug-consumers     | X     |          |           |       | X        | X   | X       | X            | X   | X   | X     | X          |               |
| debug-neo4j         | X     |          |           | X     | X        | X   | X       | X            |     |     | X     | X          |               |
| debug-elasticsearch | X     |          |           |       | X        | X   | X       | X            |     |     | X     |            | X             |

## Advanced Setups

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
