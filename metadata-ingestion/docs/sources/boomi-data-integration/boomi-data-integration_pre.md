### Overview

The `boomi-data-integration` module ingests environments, groups and rivers from Boomi Data Integration, along with lineage from source-to-target rivers and logic SQL steps to the tables they write, where the target is supported.

### Prerequisites

#### Create API tokens

A Data Integration API token belongs to one account and one environment. Create one token per environment you want to ingest.

1. Sign in as an Account Administrator and switch to the environment.
2. Open **Settings** > **API Tokens** and create a token with these scopes:
   - `data flow:list`
   - `environment:list`
3. Copy the token and note the environment ID.

Add one entry per environment to `environment_tokens`, each with an `environment_id` and an `api_token`. If an environment in `environment_tokens` cannot be read, the run is reported as failed so that stale entity removal does not delete that environment's entities. If you only ingest one environment, you can set `api_token` instead. With `api_token`, the source plugin lists the environments the token can see and skips, with a warning, any environment the token is not allowed to read.

#### Choose the region

| `region` | API host                               |
| -------- | -------------------------------------- |
| `us`     | `https://api.rivery.io`                |
| `eu`     | `https://api.eu-west-1.rivery.io`      |
| `il`     | `https://api.il-central-1.rivery.io`   |
| `au`     | `https://api.ap-southeast-2.rivery.io` |

Set `api_url` instead of `region` to use another host. It must use `https`, except for `localhost`, `127.0.0.1` or `::1`.
