### Overview

The `boomi-data-integration` module ingests environments, groups and rivers from Boomi Data Integration, along with table and column lineage through source-to-target rivers and logic SQL steps, recent river runs, and optionally river owners.

### Prerequisites

#### Create API tokens

A Data Integration API token belongs to one account and one environment. Create one token per environment you want to ingest.

1. Sign in as an Account Administrator and switch to the environment.
2. Open **Settings** > **API Tokens** and create a token with these scopes:
   - `river:list`
   - `environment:list`
   - `activity:list`, for run history. Without it the source plugin skips run history with a warning.
   - `user:list`, only if you set `extract_owners: true`
3. Copy the token and note the environment ID.

The source plugin only reads, so it does not need `river:execute` or any `:edit` scope. Without `user:list`, owners are skipped with one warning and the rest of the run is unaffected.

Add one entry per environment to `environment_tokens`, each with an `environment_id` and an `api_token`. If an environment in `environment_tokens` cannot be read, the run is reported as failed so that stale entity removal does not delete that environment's entities. If you only ingest one environment, you can set `api_token` instead. With `api_token`, the source plugin lists the environments the token can see and skips, with a warning, any environment the token is not allowed to read.

#### Choose the region

| `region` | API host                               |
| -------- | -------------------------------------- |
| `us`     | `https://api.rivery.io`                |
| `eu`     | `https://api.eu-west-1.rivery.io`      |
| `il`     | `https://api.il-central-1.rivery.io`   |
| `au`     | `https://api.ap-southeast-2.rivery.io` |

Set `api_url` instead of `region` to use another host. It must use `https`, except for `localhost`, `127.0.0.1` or `::1`.

#### Map source connections

Rivers and connections do not say which database a source connection points at. For source lineage from Postgres, SQL Server, Snowflake, Redshift or BigQuery, add each source connection to `connection_to_platform`, keyed by its connection ID, with its `platform` (for example `postgres` or `bigquery`) and the database (the project for BigQuery) as `database`. MySQL and MariaDB sources do not need this, because the river's schema name is the database. The connection ID is `properties.source.connection_id` in the river definition, or the ID in the connection's URL in the console.
