### Pre-requisites

#### [Recommended] Create a GitHub Deploy Key

To use LookML ingestion through the UI, or automate github checkout through the cli, you must set up a GitHub deploy key for your Looker GitHub repository. Read [this](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys) document for how to set up deploy keys for your Looker git repo. 

In a nutshell, there are three steps: 
1. Generate a private-public ssh key pair. This will typically generate two files, e.g. looker_datahub_deploy_key (this is the private key) and looker_datahub_deploy_key.pub (this is the public key)
![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/ssh-key-generation.png)

2. Add the public key to your Looker git repo as a deploy key with read access (no need to provision write access). Follow the guide [here](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys) for that.
![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/git-deploy-key.png)

3. Provision the private key (paste the contents of the file) as a secret on the DataHub Ingestion UI. Give it a handy name like **LOOKER_DATAHUB_DEPLOY_KEY**
![Image](https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/gitssh/datahub-secret.png)

#### [Optional] Create an API key with admin privileges

See the [Looker authentication docs](https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk) for the steps to create a client ID and secret.
You need to ensure that the API key is attached to a user that has Admin privileges. 

If that is not possible, read the configuration section and provide an offline specification of the `connection_to_platform_map` and the `project_name`.

### Ingestion Options

You have 3 options for controlling where your ingestion of LookML is run. 
- The DataHub UI (recommended for the easiest out-of-the-box experience)
- As a GitHub Action (recommended to ensure that you have the freshest metadata pushed on change)
- Using the CLI (scheduled via an orchestrator like Airflow)

Read on to learn more about these options. 

### UI-based Ingestion [Recommended for ease of use]

To ingest LookML metadata through the UI, you must set up a GitHub deploy key using the instructions in the section [above](#optional-create-a-github-deploy-key). Once that is complete, you can follow the on-screen instructions to set up a LookML source using the Ingestion page.

### GitHub Action based Ingestion [Recommended for push-based integration]

You can set up ingestion using a GitHub Action to push metadata whenever your main lookml repo changes.
The following sample GitHub action file can be modified to emit LookML metadata whenever there is a change to your repository. This ensures that metadata is already fresh and up to date.

#### Sample GitHub Action

Drop this file into your `.github/workflows` directory inside your Looker github repo.
You need to set up the following secrets in your GitHub repository to get this workflow to work:
- DATAHUB_GMS_HOST: The endpoint where your DataHub host is running
- DATAHUB_TOKEN: An authentication token provisioned for DataHub ingestion
- LOOKER_BASE_URL: The base url where your Looker assets are hosted (e.g. https://acryl.cloud.looker.com)
- LOOKER_CLIENT_ID: A provisioned Looker Client ID
- LOOKER_CLIENT_SECRET: A provisioned Looker Client Secret

```
name: lookml metadata upload
on:
  push:
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
              #  connection-name:
                  #platform: platform-name (e.g. snowflake)
                  #default_db: default-db-name (e.g. DEMO_PIPELINE)
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
          LOOKER_BASE_URL: ${{ secrets.LOOKER_BASE_URL }}
          LOOKER_CLIENT_ID: ${{ secrets.LOOKER_CLIENT_ID }}
          LOOKER_CLIENT_SECRET: ${{ secrets.LOOKER_CLIENT_SECRET }}
```

If you want to ingest lookml using the **datahub** cli directly, read on for instructions and configuration details.