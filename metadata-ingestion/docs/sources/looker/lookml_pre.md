### Pre-requisites

#### [Optional] Create an API key

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges. 

If that is not possible, read the configuration section and provide an offline specification of the `connection_to_platform_map` and the `project_name`.

### Ingestion through UI

Ingestion using lookml connector is not supported through the UI.
However, you can set up ingestion using a GitHub Action to push metadata whenever your main lookml repo changes.

#### Sample GitHub Action

Drop this file into your `.github/workflows` directory inside your Looker github repo.

```
name: lookml metadata upload
on:
  push:
    branches:
      - main
    paths-ignore:
      - "docs/**"
      - "**.md"
  pull_request:
    branches:
      - main
    paths-ignore:
      - "docs/**"
      - "**.md"
  release:
    types: [published, edited]
  workflow_dispatch:
    

jobs:
  lookml-metadata-upload:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Run LookML ingestion
        run: |
          pip install 'acryl-datahub[lookml,datahub-rest]'
          cat << EOF > lookml_ingestion.yml
          # LookML ingestion configuration
          source:
            type: "lookml"
            config:
              base_folder: ${{ github.workspace }}
              parse_table_names_from_sql: true
              github_info:
                repo: ${{ github.repository }}
                branch: ${{ github.ref }}
              # Options
              #connection_to_platform_map:
              #  acryl-snow: snowflake
                  #platform: snowflake
                  #default_db: DEMO_PIPELINE
              api:
                client_id: ${LOOKER_CLIENT_ID}
                client_secret: ${LOOKER_CLIENT_SECRET}
                base_url: ${LOOKER_BASE_URL}
          sink:
            type: datahub-rest
            config:
              server: ${DATAHUB_GMS_HOST}
              token: ${DATAHUB_TOKEN}
          EOF
          datahub ingest -c lookml_ingestion.yml
        env:
          DATAHUB_GMS_HOST: ${{ secrets.DATAHUB_GMS_HOST }}
          DATAHUB_TOKEN: ${{ secrets.DATAHUB_TOKEN }}
          LOOKER_BASE_URL: https://acryl.cloud.looker.com  # <--- replace with your Looker base URL
          LOOKER_CLIENT_ID: ${{ secrets.LOOKER_CLIENT_ID }}
          LOOKER_CLIENT_SECRET: ${{ secrets.LOOKER_CLIENT_SECRET }}
```

If you want to ingest lookml using the **datahub** cli directly, read on for instructions and configuration details.