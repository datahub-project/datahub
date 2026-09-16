# Use the official PostgreSQL image as base (keep major aligned with DATAHUB_POSTGRES_VERSION in compose)
ARG POSTGRES_VERSION=17.5
FROM postgres:${POSTGRES_VERSION}

# Install system dependencies: PostGIS/pgRouting/pgvector (graph + search) and partman + pg_cron
# (pgQueue / pgTimeseries time-partitioned retention + in-DB maintenance scheduling).
# The PG_MAJOR environment variable is set by the base PostgreSQL image; packages come from the
# same PGDG/Debian repos as the base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-${PG_MAJOR}-pgrouting \
    postgresql-${PG_MAJOR}-postgis-3 \
    postgresql-${PG_MAJOR}-postgis-3-scripts \
    postgresql-${PG_MAJOR}-pgvector \
    postgresql-${PG_MAJOR}-partman \
    postgresql-${PG_MAJOR}-cron \
    && rm -rf /var/lib/apt/lists/*

# pg_cron must be listed in shared_preload_libraries (default cron.database_name is postgres).
# see docker/profiles/docker-compose.prerequisites.yml postgres command.
# Application schema is created by datahub-upgrade SqlSetup.
