# DataHub PostgreSQL image

Extends the official [`postgres`](https://hub.docker.com/_/postgres) image with packages required by DataHub when using PostgreSQL as the backing store:

- PostGIS / pgRouting / pgvector
- `pg_partman`
- **`pg_cron`** (required once quickstart compose uses this image with `shared_preload_libraries` including `pg_cron`; stock `postgres` in compose does not preload it yet.)

## Published image (multi-arch)

CI publishes a multi-arch manifest to Docker Hub (default namespace `acryldata`, repository `datahub-postgres`). The default tag is **`datahubPostgresImageTag`** in [build.gradle](build.gradle) (defaults to `17.5-extensions-v1`). After quickstart compose is pinned to that image, align compose env defaults and metadata-io test image tags to match.

### Ad-hoc or automated publish

Use the GitHub Actions workflow **Publish datahub-postgres** ([.github/workflows/publish-datahub-postgres.yml](../../.github/workflows/publish-datahub-postgres.yml)):

- **Manual:** Actions → _Publish datahub-postgres_ → _Run workflow_. Set **postgres_base_version** (default `17.5`, upstream `FROM postgres:…` tag), **extensions_image_version** (default `1`, becomes `…-extensions-v1`), and **docker_registry** if needed. The workflow builds tag `{postgres_base_version}-extensions-v{extensions_image_version}`.

Gradle entry point (equivalent to CI defaults): `./gradlew :docker:buildImagespublishPostgres -PmatrixBuild=true -PpostgresVersion=17.5 -PdatahubPostgresImageTag=17.5-extensions-v1 -Ptag=17.5-extensions-v1 -PdockerRegistry=acryldata -PdockerPush=true` (requires Depot + registry credentials as in CI).

### Bumping the extensions tag

When you need a new immutable tag (e.g. after Postgres major or package changes):

1. Bump **`datahubPostgresImageTag`** in [build.gradle](build.gradle) (or pass `-PdatahubPostgresImageTag=…` for one-off builds).
2. When adopting the image for local quickstart, update compose defaults in [docker/profiles/docker-compose.prerequisites.yml](../profiles/docker-compose.prerequisites.yml) (and any other compose files your stack uses).
3. Update test defaults in [metadata-io/build.gradle](../../metadata-io/build.gradle) and [PostgresTestUtils.java](../../metadata-io/src/test/java/com/linkedin/metadata/PostgresTestUtils.java).
4. Run the publish workflow with the new `-Ptag` substring, then merge the PR.

## Local single-arch build

```bash
./gradlew :docker:postgres:docker
```

Optional Postgres major: `./gradlew :docker:postgres:docker -PpostgresVersion=17.5`
