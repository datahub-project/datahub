### Overview

The `boomi-data-integration` module ingests environments, groups and rivers from Boomi Data Integration, along with lineage from each river or logic SQL step to the tables it writes.

### Prerequisites

#### Create API tokens

A Data Integration API token belongs to one account and one environment. Create one token per environment you want to ingest.

1. Sign in as an Account Administrator and switch to the environment.
2. Open **Settings** > **API Tokens** and create a token with these scopes:
   - `data flow:list`
   - `connection:list`
   - `environment:list`
3. Copy the token and note the environment ID.

Put the tokens in `environment_tokens`, keyed by environment ID. If you only ingest one environment, you can set `api_token` instead. With `api_token`, the source plugin lists the environments the token can see and skips, with a warning, any environment the token cannot read.

#### Choose the region

| `region` | API host                               |
| -------- | -------------------------------------- |
| `us`     | `https://api.rivery.io`                |
| `eu`     | `https://api.eu-west-1.rivery.io`      |
| `il`     | `https://api.il-central-1.rivery.io`   |
| `au`     | `https://api.ap-southeast-2.rivery.io` |

Set `api_url` instead of `region` to use another host.
