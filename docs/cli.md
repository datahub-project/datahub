---
# Display to h4 headings
# toc_min_heading_level: 2
toc_max_heading_level: 4
---

# DataHub CLI

DataHub comes with a friendly cli called `datahub` that allows you to perform a lot of common operations using just the command line. [Acryl Data](https://acryldata.io) maintains the [pypi package](https://pypi.org/project/acryl-datahub/) for `datahub`.

## Installation

### Using pip

We recommend Python virtual environments (venv-s) to namespace pip modules. Here's an example setup:

```shell
python3 -m venv venv             # create the environment
source venv/bin/activate         # activate the environment
```

**_NOTE:_** If you install `datahub` in a virtual environment, that same virtual environment must be re-activated each time a shell window or session is created.

Once inside the virtual environment, install `datahub` using the following commands

```shell
# Requires Python 3.8+
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
# validate that the install was successful
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
datahub init
# authenticate your datahub CLI with your datahub instance
```

If you run into an error, try checking the [_common setup issues_](../metadata-ingestion/developing.md#common-setup-issues).

Other installation options such as installation from source and running the cli inside a container are available further below in the guide [here](#alternate-installation-options).

## Starter Commands

The `datahub` cli allows you to do many things, such as quick-starting a DataHub docker instance locally, ingesting metadata from your sources into a DataHub server or a DataHub lite instance, as well as retrieving, modifying and exploring metadata.
Like most command line tools, `--help` is your best friend. Use it to discover the capabilities of the cli and the different commands and sub-commands that are supported.

```console
datahub --help
Usage: datahub [OPTIONS] COMMAND [ARGS]...

Options:
  --debug / --no-debug            Enable debug logging.
  --log-file FILE                 Enable debug logging.
  --debug-vars / --no-debug-vars  Show variable values in stack traces. Implies --debug. While we try to avoid
                                  printing sensitive information like passwords, this may still happen.
  --version                       Show the version and exit.
  -dl, --detect-memory-leaks      Run memory leak detection.
  --help                          Show this message and exit.

Commands:
  actions       <disabled due to missing dependencies>
  assertions    A group of commands to interact with the Assertion entity in DataHub.
  check         Helper commands for checking various aspects of DataHub.
  container     A group of commands to interact with containers in DataHub.
  datacontract  A group of commands to interact with the DataContract entity in DataHub.
  dataproduct   A group of commands to interact with the DataProduct entity in DataHub.
  dataset       A group of commands to interact with the Dataset entity in DataHub.
  delete        Delete metadata from DataHub.
  docker        Helper commands for setting up and interacting with a local DataHub instance using Docker.
  exists        A group of commands to check existence of entities in DataHub.
  forms         A group of commands to interact with forms in DataHub.
  get           A group of commands to get metadata from DataHub.
  group         A group of commands to interact with the Group entity in DataHub.
  ingest        Ingest metadata into DataHub.
  init          Configure which datahub instance to connect to
  lite          A group of commands to work with a DataHub Lite instance
  migrate       Helper commands for migrating metadata within DataHub.
  properties    A group of commands to interact with structured properties in DataHub.
  put           A group of commands to put metadata in DataHub.
  state         Managed state stored in DataHub by stateful ingestion.
  telemetry     Toggle telemetry.
  timeline      Get timeline for an entity based on certain categories
  user          A group of commands to interact with the User entity in DataHub.
  version       Print version number and exit.
```

The following top-level commands listed below are here mainly to give the reader a high-level picture of what are the kinds of things you can accomplish with the cli.
We've ordered them roughly in the order we expect you to interact with these commands as you get deeper into the `datahub`-verse.

### docker

The `docker` command allows you to start up a local DataHub instance using `datahub docker quickstart`. You can also check if the docker cluster is healthy using `datahub docker check`.

### ingest

The `ingest` command allows you to ingest metadata from your sources using ingestion configuration files, which we call recipes.
Source specific crawlers are provided by plugins and might sometimes need additional extras to be installed. See [installing plugins](#installing-plugins) for more information.
[Removing Metadata from DataHub](./how/delete-metadata.md) contains detailed instructions about how you can use the ingest command to perform operations like rolling-back previously ingested metadata through the `rollback` sub-command and listing all runs that happened through `list-runs` sub-command.

```console
Usage: datahub [datahub-options] ingest [command-options]

Command Options:
  -c / --config             Config file in .toml or .yaml format
  -n / --dry-run            Perform a dry run of the ingestion, essentially skipping writing to sink
  --preview                 Perform limited ingestion from the source to the sink to get a quick preview
  --preview-workunits       The number of workunits to produce for preview
  --strict-warnings         If enabled, ingestion runs with warnings will yield a non-zero error code
  --test-source-connection  When set, ingestion will only test the source connection details from the recipe
  --no-progress             If enabled, mute intermediate progress ingestion reports
```

#### ingest --dry-run

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to validate that the
ingestion recipe is producing the desired metadata events before ingesting them into datahub.

```shell
# Dry run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --dry-run
# Short-form
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n
```

#### ingest list-source-runs

The `list-source-runs` option of the `ingest` command lists the previous runs, displaying their run ID, source name, 
start time, status, and source URN. This command allows you to filter results using the --urn option for URN-based 
filtering or the --source option to filter by source name (partial or complete matches are supported).

```shell
# List all ingestion runs
datahub ingest list-source-runs
# Filter runs by a source name containing "demo"
datahub ingest list-source-runs --source "demo"
```

#### ingest --preview

The `--preview` option of the `ingest` command performs all of the ingestion steps, but limits the processing to only the first 10 workunits produced by the source.
This option helps with quick end-to-end smoke testing of the ingestion recipe.

```shell
# Preview
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --preview
# Preview with dry-run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n --preview
```

By default `--preview` creates 10 workunits. But if you wish to try producing more workunits you can use another option `--preview-workunits`

```shell
# Preview 20 workunits without sending anything to sink
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n --preview --preview-workunits=20
```

#### ingest --no-default-report

By default, the cli sends an ingestion report to DataHub, which allows you to see the result of all cli-based ingestion in the UI. This can be turned off with the `--no-default-report` flag.

```shell
# Running ingestion with reporting to DataHub turned off
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --no-default-report
```

The reports include the recipe that was used for ingestion. This can be turned off by adding an additional section to the ingestion recipe.

```yaml
source:
  # source configs

sink:
  # sink configs

# Add configuration for the datahub reporter
reporting:
  - type: datahub
    config:
      report_recipe: false

# Optional log to put failed JSONs into a file
# Helpful in case you are trying to debug some issue with specific ingestion failing
failure_log:
  enabled: false
  log_config:
    filename: ./path/to/failure.json
```

### ingest deploy

The `ingest deploy` command instructs the cli to upload an ingestion recipe to DataHub to be run by DataHub's [UI Ingestion](./ui-ingestion.md).
This command can also be used to schedule the ingestion while uploading or even to update existing sources. It will upload to the remote instance the
CLI is connected to, not the sink of the recipe. Use `datahub init` to set the remote if not already set.

This command will automatically create a new recipe if it doesn't exist, or update it if it does.
Note that this is a complete update, and will remove any options that were previously set.
I.e: Not specifying a schedule in the cli update command will remove the schedule from the recipe to be updated.

**Basic example**

To schedule a recipe called "Snowflake Integration", to run at 5am every day, London time with the recipe configured in a local `recipe.yaml` file:

```shell
datahub ingest deploy --name "Snowflake Integration" --schedule "5 * * * *" --time-zone "Europe/London" -c recipe.yaml
```

By default, the ingestion recipe's identifier is generated by hashing the name.
You can override the urn generation by passing the `--urn` flag to the CLI.

**Using `deployment` to avoid CLI args**

As an alternative to configuring settings from the CLI, all of these settings can also be set in the `deployment` field of the recipe.

```yml
# deployment_recipe.yml
deployment:
  name: "Snowflake Integration"
  schedule: "5 * * * *"
  time_zone: "Europe/London"

source: ...
```

```shell
datahub ingest deploy -c deployment_recipe.yml
```

This is particularly useful when you want all recipes to be stored in version control.

```shell
# Deploy every yml recipe in a directory
ls recipe_directory/*.yml | xargs -n 1 -I {} datahub ingest deploy -c {}
```

### init

The init command is used to tell `datahub` about where your DataHub instance is located. The CLI will point to localhost DataHub by default.
Running `datahub init` will allow you to customize the datahub instance you are communicating with. It has an optional `--use-password` option which allows to initialise the config using username, password. We foresee this mainly being used by admins as majority of organisations will be using SSO and there won't be any passwords to use.

**_Note_**: Provide your GMS instance's host when the prompt asks you for the DataHub host.

```
# locally hosted example
datahub init
/Users/user/.datahubenv already exists. Overwrite? [y/N]: y
Configure which datahub instance to connect to
Enter your DataHub host [http://localhost:8080]: http://localhost:8080
Enter your DataHub access token []:

# acryl example
datahub init
/Users/user/.datahubenv already exists. Overwrite? [y/N]: y
Configure which datahub instance to connect to
Enter your DataHub host [http://localhost:8080]: https://<your-instance-id>.acryl.io/gms
Enter your DataHub access token []: <token generated from https://<your-instance-id>.acryl.io/settings/tokens>
```

#### Environment variables supported

The environment variables listed below take precedence over the DataHub CLI config created through the `init` command.

- `DATAHUB_SKIP_CONFIG` (default `false`) - Set to `true` to skip creating the configuration file.
- `DATAHUB_GMS_URL` (default `http://localhost:8080`) - Set to a URL of GMS instance
- `DATAHUB_GMS_HOST` (default `localhost`) - Set to a host of GMS instance. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_PORT` (default `8080`) - Set to a port of GMS instance. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_PROTOCOL` (default `http`) - Set to a protocol like `http` or `https`. Prefer using `DATAHUB_GMS_URL` to set the URL.
- `DATAHUB_GMS_TOKEN` (default `None`) - Used for communicating with DataHub Cloud.
- `DATAHUB_TELEMETRY_ENABLED` (default `true`) - Set to `false` to disable telemetry. If CLI is being run in an environment with no access to public internet then this should be disabled.
- `DATAHUB_TELEMETRY_TIMEOUT` (default `10`) - Set to a custom integer value to specify timeout in secs when sending telemetry.
- `DATAHUB_DEBUG` (default `false`) - Set to `true` to enable debug logging for CLI. Can also be achieved through `--debug` option of the CLI. This exposes sensitive information in logs, enabling on production instances should be avoided especially if UI ingestion is in use as logs can be made available for runs through the UI.
- `DATAHUB_VERSION` (default `head`) - Set to a specific version to run quickstart with the particular version of docker images.
- `ACTIONS_VERSION` (default `head`) - Set to a specific version to run quickstart with that image tag of `datahub-actions` container.
- `DATAHUB_ACTIONS_IMAGE` (default `acryldata/datahub-actions`) - Set to `-slim` to run a slimmer actions container without pyspark/deequ features.

```shell
DATAHUB_SKIP_CONFIG=false
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_GMS_TOKEN=
DATAHUB_TELEMETRY_ENABLED=true
DATAHUB_TELEMETRY_TIMEOUT=10
DATAHUB_DEBUG=false
```

### container

A group of commands to interact with containers in DataHub.

e.g. You can use this to apply a tag to all datasets recursively in this container.
```shell
datahub container tag --container-urn "urn:li:container:0e9e46bd6d5cf645f33d5a8f0254bc2d" --tag-urn "urn:li:tag:tag1"
datahub container domain --container-urn "urn:li:container:3f2effd1fbe154a4d60b597263a41e41" --domain-urn  "urn:li:domain:ajsajo-b832-4ab3-8881-7ed5e991a44c"
datahub container owner --container-urn "urn:li:container:3f2effd1fbe154a4d60b597263a41e41" --owner-urn  "urn:li:corpGroup:eng@example.com"
datahub container term --container-urn "urn:li:container:3f2effd1fbe154a4d60b597263a41e41" --term-urn  "urn:li:term:PII"
```

### check

The datahub package is composed of different plugins that allow you to connect to different metadata sources and ingest metadata from them.
The `check` command allows you to check if all plugins are loaded correctly as well as validate an individual MCE-file.

### delete

The `delete` command allows you to delete metadata from DataHub.

The [metadata deletion guide](./how/delete-metadata.md) covers the various options for the delete command.

### exists

The exists command can be used to check if an entity exists in DataHub.

```shell
> datahub exists --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
true
> datahub exists --urn "urn:li:dataset:(urn:li:dataPlatform:hive,NonExistentHiveDataset,PROD)"
false
```

### get

The `get` command allows you to easily retrieve metadata from DataHub, by using the REST API. This works for both versioned aspects and timeseries aspects. For timeseries aspects, it fetches the latest value.
For example the following command gets the ownership aspect from the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)`

```shell-session
$ datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership
{
  "ownership": {
    "lastModified": {
      "actor": "urn:li:corpuser:jdoe",
      "time": 1680210917580
    },
    "owners": [
      {
        "owner": "urn:li:corpuser:jdoe",
        "source": {
          "type": "SERVICE"
        },
        "type": "NONE"
      }
    ]
  }
}
```

### put

The `put` group of commands allows you to write metadata into DataHub. This is a flexible way for you to issue edits to metadata from the command line.

#### put aspect

The **put aspect** (also the default `put`) command instructs `datahub` to set a specific aspect for an entity to a specified value.
For example, the command shown below sets the `ownership` aspect of the dataset `urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)` to the value in the file `ownership.json`.
The JSON in the `ownership.json` file needs to conform to the [`Ownership`](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/common/Ownership.pdl) Aspect model as shown below.

```json
{
  "owners": [
    {
      "owner": "urn:li:corpuser:jdoe",
      "type": "DEVELOPER"
    },
    {
      "owner": "urn:li:corpuser:jdub",
      "type": "DATAOWNER"
    }
  ]
}
```

```console
datahub --debug put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)" --aspect ownership -d ownership.json

[DATE_TIMESTAMP] DEBUG    {datahub.cli.cli_utils:340} - Attempting to emit to DataHub GMS; using curl equivalent to:
curl -X POST -H 'User-Agent: python-requests/2.26.0' -H 'Accept-Encoding: gzip, deflate' -H 'Accept: */*' -H 'Connection: keep-alive' -H 'X-RestLi-Protocol-Version: 2.0.0' -H 'Content-Type: application/json' --data '{"proposal": {"entityType": "dataset", "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)", "aspectName": "ownership", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": "{\"owners\": [{\"owner\": \"urn:li:corpuser:jdoe\", \"type\": \"DEVELOPER\"}, {\"owner\": \"urn:li:corpuser:jdub\", \"type\": \"DATAOWNER\"}]}"}}}' 'http://localhost:8080/aspects/?action=ingestProposal'
Update succeeded with status 200
```

#### put platform

**🤝 Version Compatibility:** `acryl-datahub>0.8.44.4`

The **put platform** command instructs `datahub` to create or update metadata about a data platform. This is very useful if you are using a custom data platform, to set up its logo and display name for a native UI experience.

```shell
datahub put platform --name longtail_schemas --display_name "Long Tail Schemas" --logo "https://flink.apache.org/img/logo/png/50/color_50.png"
✅ Successfully wrote data platform metadata for urn:li:dataPlatform:longtail_schemas to DataHub (DataHubRestEmitter: configured to talk to https://longtailcompanions.acryl.io/api/gms with token: eyJh**********Cics)
```

### timeline

The `timeline` command allows you to view a version history for entities. Currently only supported for Datasets. For example,
the following command will show you the modifications to tags for a dataset for the past week. The output includes a computed semantic version,
relevant for schema changes only currently, the target of the modification, and a description of the change including a timestamp.
The default output is sanitized to be more readable, but the full API output can be obtained by passing the `--verbose` flag and
to get the raw JSON difference in addition to the API output you can add the `--raw` flag. For more details about the feature please see [the main feature page](dev-guides/timeline.md)

```console
datahub timeline --urn "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)" --category TAG --start 7daysago
2022-02-17 14:03:42 - 0.0.0-computed
 MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:03:42.0
2022-02-17 14:17:30 - 0.0.0-computed
 MODIFY TAG dataset:mysql:User.UserAccount : A change in aspect editableSchemaMetadata happened at time 2022-02-17 20:17:30.118
```

## Entity Specific Commands

### dataset (Dataset Entity)

The `dataset` command allows you to interact with the dataset entity.

The `get` operation can be used to read in a dataset into a yaml file.

```shell
datahub dataset get --urn "$URN" --to-file "$FILE_NAME"
```

The `upsert` operation can be used to create a new user or update an existing one.

```shell
datahub dataset upsert -f dataset.yaml
```

An example of `dataset.yaml` would look like as in [dataset.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/cli_usage/dataset/dataset.yaml).

### user (User Entity)

The `user` command allows you to interact with the User entity.
It currently supports the `upsert` operation, which can be used to create a new user or update an existing one.
For detailed information, please refer to [Creating Users and Groups with Datahub CLI](/docs/api/tutorials/owners.md#upsert-users).

```shell
datahub user upsert -f users.yaml
```

An example of `users.yaml` would look like as in [bar.user.dhub.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/cli_usage/user/bar.user.dhub.yaml) file for the complete code.

```yaml
- id: bar@acryl.io
  first_name: The
  last_name: Bar
  email: bar@acryl.io
  slack: "@the_bar_raiser"
  description: "I like raising the bar higher"
  groups:
    - foogroup@acryl.io
- id: datahub
  slack: "@datahubproject"
  phone: "1-800-GOT-META"
  description: "The DataHub Project"
  picture_link: "https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/datahub-logo-color-stable.svg"
```

### group (Group Entity)

The `group` command allows you to interact with the Group entity.
It currently supports the `upsert` operation, which can be used to create a new group or update an existing one with embedded Users.
For more information, please refer to [Creating Users and Groups with Datahub CLI](/docs/api/tutorials/owners.md#upsert-users).

```shell
datahub group upsert -f group.yaml
```

An example of `group.yaml` would look like as in [foo.group.dhub.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/cli_usage/group/foo.group.dhub.yaml) file for the complete code.

```yaml
id: foogroup@acryl.io
display_name: Foo Group
admins:
  - datahub
members:
  - bar@acryl.io # refer to a user either by id or by urn
  - id: joe@acryl.io # inline specification of user
    slack: "@joe_shmoe"
    display_name: "Joe's Hub"
```

### dataproduct (Data Product Entity)

**🤝 Version Compatibility:** `acryl-datahub>=0.10.2.4`

The dataproduct group of commands allows you to manage the lifecycle of a DataProduct entity on DataHub.
See the [Data Products](./dataproducts.md) page for more details on what a Data Product is and how DataHub represents it.

```shell
datahub dataproduct --help
Commands:
  upsert*          Upsert attributes to a Data Product in DataHub
  update           Create or Update a Data Product in DataHub.
  add_asset        Add an asset to a Data Product
  add_owner        Add an owner to a Data Product
  delete           Delete a Data Product in DataHub.
  diff             Diff a Data Product file with its twin in DataHub
  get              Get a Data Product from DataHub
  remove_asset     Add an asset to a Data Product
  remove_owner     Remove an owner from a Data Product
  set_description  Set description for a Data Product in DataHub
```

Here we detail the sub-commands available under the dataproduct group of commands:

#### upsert

Use this to upsert a data product yaml file into DataHub. This will create the data product if it doesn't exist already. Remember, this will upsert all the fields that are specified in the yaml file and will not touch the fields that are not specified. For example, if you do not specify the `description` field in the yaml file, then `upsert` will not modify the description field on the Data Product entity in DataHub. To keep this file sync-ed with the metadata on DataHub use the [diff](#diff) command. The format of the yaml file is available [here](./dataproducts.md#creating-a-data-product-yaml--git).

```shell
# Usage
> datahub dataproduct upsert -f data_product.yaml

```

#### update

Use this to fully replace a data product's metadata in DataHub from a yaml file. This will create the data product if it doesn't exist already. Remember, this will update all the fields including ones that are not specified in the yaml file. For example, if you do not specify the `description` field in the yaml file, then `update` will set the description field on the Data Product entity in DataHub to empty. To keep this file sync-ed with the metadata on DataHub use the [diff](#diff) command. The format of the yaml file is available [here](./dataproducts.md#creating-a-data-product-yaml--git).

```shell
# Usage
> datahub dataproduct upsert -f data_product.yaml

```

:::note

❗**Pro-Tip: upsert versus update**

Wondering which command is right for you? Use `upsert` if there are certain elements of metadata that you don't want to manage using the yaml file (e.g. owners, assets or description). Use `update` if you want to manage the entire data product's metadata using the yaml file.

:::

#### diff

Use this to keep a data product yaml file updated from its server-side version in DataHub.

```shell
# Usage
> datahub dataproduct diff -f data_product.yaml --update
```

#### get

Use this to get a data product entity from DataHub and optionally write it to a yaml file

```shell
# Usage
> datahub dataproduct get --urn urn:li:dataProduct:pet_of_the_week --to-file pet_of_the_week_dataproduct.yaml
{
  "id": "urn:li:dataProduct:pet_of_the_week",
  "domain": "urn:li:domain:dcadded3-2b70-4679-8b28-02ac9abc92eb",
  "assets": [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
    "urn:li:dashboard:(looker,dashboards.19)",
    "urn:li:dataFlow:(airflow,snowflake_load,prod)"
  ],
  "display_name": "Pet of the Week Campaign",
  "owners": [
    {
      "id": "urn:li:corpuser:jdoe",
      "type": "BUSINESS_OWNER"
    }
  ],
  "description": "This campaign includes Pet of the Week data.",
  "tags": [
    "urn:li:tag:adoption"
  ],
  "terms": [
    "urn:li:glossaryTerm:ClientsAndAccounts.AccountBalance"
  ],
  "properties": {
    "lifecycle": "production",
    "sla": "7am every day"
  }
}
Data Product yaml written to pet_of_the_week_dataproduct.yaml
```

#### add_asset

Use this to add a data asset to a Data Product.

```shell
# Usage
> datahub dataproduct add_asset --urn "urn:li:dataProduct:pet_of_the_week"  --asset "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
```

#### remove_asset

Use this to remove a data asset from a Data Product.

```shell
# Usage
> datahub dataproduct remove_asset --urn "urn:li:dataProduct:pet_of_the_week"  --asset "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
```

#### add_owner

Use this to add an owner to a Data Product.

```shell
# Usage
> datahub dataproduct add_owner --urn "urn:li:dataProduct:pet_of_the_week"  --owner "jdoe@longtail.com" --owner-type BUSINESS_OWNER
```

#### remove_owner

Use this to remove an owner from a Data Product.

```shell
# Usage
> datahub dataproduct remove_owner --urn "urn:li:dataProduct:pet_of_the_week"  --owner "urn:li:corpUser:jdoe@longtail.com"
```

#### set_description

Use this to attach rich documentation for a Data Product in DataHub.

```shell
> datahub dataproduct set_description --urn "urn:li:dataProduct:pet_of_the_week" --description "This is the pet dataset"
# For uploading rich documentation from a markdown file, use the --md-file option
# > datahub dataproduct set_description --urn "urn:li:dataProduct:pet_of_the_week" --md-file ./pet_of_the_week.md
```

#### delete

Use this to delete a Data Product from DataHub. Default to `--soft` which preserves metadata, use `--hard` to erase all metadata associated with this Data Product.

```shell
> datahub dataproduct delete --urn "urn:li:dataProduct:pet_of_the_week"
# For Hard Delete see below:
# > datahub dataproduct delete --urn "urn:li:dataProduct:pet_of_the_week" --hard
```

## Miscellaneous Admin Commands

### lite (experimental)

The lite group of commands allow you to run an embedded, lightweight DataHub instance for command line exploration of your metadata. This is intended more for developer tool oriented usage rather than as a production server instance for DataHub. See [DataHub Lite](./datahub_lite.md) for more information about how you can ingest metadata into DataHub Lite and explore your metadata easily.

### telemetry

To help us understand how people are using DataHub, we collect anonymous usage statistics on actions such as command invocations via Mixpanel.
We do not collect private information such as IP addresses, contents of ingestions, or credentials.
The code responsible for collecting and broadcasting these events is open-source and can be found [within our GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/telemetry/telemetry.py).

Telemetry is enabled by default, and the `telemetry` command lets you toggle the sending of these statistics via `telemetry enable/disable`.

### migrate

The `migrate` group of commands allows you to perform certain kinds of migrations.

#### dataplatform2instance

The `dataplatform2instance` migration command allows you to migrate your entities from an instance-agnostic platform identifier to an instance-specific platform identifier. If you have ingested metadata in the past for this platform and would like to transfer any important metadata over to the new instance-specific entities, then you should use this command. For example, if your users have added documentation or added tags or terms to your datasets, then you should run this command to transfer this metadata over to the new entities. For further context, read the Platform Instance Guide [here](./platform-instances.md).

A few important options worth calling out:

- --dry-run / -n : Use this to get a report for what will be migrated before running
- --force / -F : Use this if you know what you are doing and do not want to get a confirmation prompt before migration is started
- --keep : When enabled, will preserve the old entities and not delete them. Default behavior is to soft-delete old entities.
- --hard : When enabled, will hard-delete the old entities.

**_Note_**: Timeseries aspects such as Usage Statistics and Dataset Profiles are not migrated over to the new entity instances, you will get new data points created when you re-run ingestion using the `usage` or sources with profiling turned on.

##### Dry Run

```console
datahub migrate dataplatform2instance --platform elasticsearch --instance prod_index --dry-run
Starting migration: platform:elasticsearch, instance=prod_index, force=False, dry-run=True
100% (25 of 25) |####################################################################################################################################################################################| Elapsed Time: 0:00:00 Time:  0:00:00
[Dry Run] Migration Report:
--------------
[Dry Run] Migration Run Id: migrate-5710349c-1ec7-4b83-a7d3-47d71b7e972e
[Dry Run] Num entities created = 25
[Dry Run] Num entities affected = 0
[Dry Run] Num entities migrated = 25
[Dry Run] Details:
[Dry Run] New Entities Created: {'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubretentionindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.schemafieldindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.system_metadata_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.tagindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_datasetprofileaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlmodelindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlfeaturetableindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajob_datahubingestioncheckpointaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahub_usage_event,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_operationaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajobindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataprocessindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.glossarytermindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataplatformindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlmodeldeploymentindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datajob_datahubingestionrunsummaryaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.graph_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.datahubpolicyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataset_datasetusagestatisticsaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dashboardindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.glossarynodeindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlfeatureindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.dataflowindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.mlprimarykeyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,prod_index.chartindex_v2,PROD)'}
[Dry Run] External Entities Affected: None
[Dry Run] Old Entities Migrated = {'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_datasetusagestatisticsaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlmodelindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlmodeldeploymentindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajob_datahubingestionrunsummaryaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahubretentionindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahubpolicyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_datasetprofileaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,glossarynodeindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataset_operationaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,graph_service_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajobindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlprimarykeyindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dashboardindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datajob_datahubingestioncheckpointaspect_v1,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,tagindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,datahub_usage_event,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,schemafieldindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlfeatureindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataprocessindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataplatformindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,mlfeaturetableindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,glossarytermindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,dataflowindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,chartindex_v2,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:elasticsearch,system_metadata_service_v1,PROD)'}
```

##### Real Migration (with soft-delete)

```
> datahub migrate dataplatform2instance --platform hive --instance
datahub migrate dataplatform2instance --platform hive --instance warehouse
Starting migration: platform:hive, instance=warehouse, force=False, dry-run=False
Will migrate 4 urns such as ['urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)']
New urns will look like ['urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_created,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_deleted,PROD)']

Ok to proceed? [y/N]:
...
Migration Report:
--------------
Migration Run Id: migrate-f5ae7201-4548-4bee-aed4-35758bb78c89
Num entities created = 4
Num entities affected = 0
Num entities migrated = 4
Details:
New Entities Created: {'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,warehouse.fct_users_created,PROD)'}
External Entities Affected: None
Old Entities Migrated = {'urn:li:dataset:(urn:li:dataPlatform:hive,logging_events,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)'}
```

## Alternate Installation Options

### Using docker

[![Docker Hub](https://img.shields.io/docker/pulls/acryldata/datahub-ingestion?style=plastic)](https://hub.docker.com/r/acryldata/datahub-ingestion)

If you don't want to install locally, you can alternatively run metadata ingestion within a Docker container.
We have prebuilt images available on [Docker hub](https://hub.docker.com/r/acryldata/datahub-ingestion). All plugins will be installed and enabled automatically.

You can use the `datahub-ingestion` docker image as explained in [Docker Images](../docker/README.md). In case you are using Kubernetes you can start a pod with the `datahub-ingestion` docker image, log onto a shell on the pod and you should have the access to datahub CLI in your kubernetes cluster.

_Limitation: the datahub_docker.sh convenience script assumes that the recipe and any input/output files are accessible in the current working directory or its subdirectories. Files outside the current working directory will not be found, and you'll need to invoke the Docker image directly._

```shell
# Assumes the DataHub repo is cloned locally.
./metadata-ingestion/scripts/datahub_docker.sh ingest -c ./examples/recipes/example_to_datahub_rest.yml
```

### Install from source

If you'd like to install from source, see the [developer guide](../metadata-ingestion/developing.md).

## Installing Plugins

We use a plugin architecture so that you can install only the dependencies you actually need. Click the plugin name to learn more about the specific source recipe and any FAQs!

### Sources

Please see our [Integrations page](https://datahubproject.io/integrations) if you want to filter on the features offered by each source.

| Plugin Name                                                                                    | Install Command                                            | Provides                                |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | --------------------------------------- |
| [metadata-file](./generated/ingestion/sources/metadata-file.md)                                | _included by default_                                      | File source and sink                    |
| [athena](./generated/ingestion/sources/athena.md)                                              | `pip install 'acryl-datahub[athena]'`                      | AWS Athena source                       |
| [bigquery](./generated/ingestion/sources/bigquery.md)                                          | `pip install 'acryl-datahub[bigquery]'`                    | BigQuery source                         |
| [datahub-lineage-file](./generated/ingestion/sources/file-based-lineage.md)                    | _no additional dependencies_                               | Lineage File source                     |
| [datahub-business-glossary](./generated/ingestion/sources/business-glossary.md)                | _no additional dependencies_                               | Business Glossary File source           |
| [dbt](./generated/ingestion/sources/dbt.md)                                                    | _no additional dependencies_                               | dbt source                              |
| [dremio](./generated/ingestion/sources/dremio.md)                                              | `pip install 'acryl-datahub[dremio]'`                      | Dremio Source                          |
| [druid](./generated/ingestion/sources/druid.md)                                                | `pip install 'acryl-datahub[druid]'`                       | Druid Source                            |
| [feast](./generated/ingestion/sources/feast.md)                                                | `pip install 'acryl-datahub[feast]'`                       | Feast source (0.26.0)                   |
| [glue](./generated/ingestion/sources/glue.md)                                                  | `pip install 'acryl-datahub[glue]'`                        | AWS Glue source                         |
| [hana](./generated/ingestion/sources/hana.md)                                                  | `pip install 'acryl-datahub[hana]'`                        | SAP HANA source                         |
| [hive](./generated/ingestion/sources/hive.md)                                                  | `pip install 'acryl-datahub[hive]'`                        | Hive source                             |
| [kafka](./generated/ingestion/sources/kafka.md)                                                | `pip install 'acryl-datahub[kafka]'`                       | Kafka source                            |
| [kafka-connect](./generated/ingestion/sources/kafka-connect.md)                                | `pip install 'acryl-datahub[kafka-connect]'`               | Kafka connect source                    |
| [ldap](./generated/ingestion/sources/ldap.md)                                                  | `pip install 'acryl-datahub[ldap]'` ([extra requirements]) | LDAP source                             |
| [looker](./generated/ingestion/sources/looker.md)                                              | `pip install 'acryl-datahub[looker]'`                      | Looker source                           |
| [lookml](./generated/ingestion/sources/looker.md#module-lookml)                                | `pip install 'acryl-datahub[lookml]'`                      | LookML source, requires Python 3.7+     |
| [metabase](./generated/ingestion/sources/metabase.md)                                          | `pip install 'acryl-datahub[metabase]'`                    | Metabase source                         |
| [mode](./generated/ingestion/sources/mode.md)                                                  | `pip install 'acryl-datahub[mode]'`                        | Mode Analytics source                   |
| [mongodb](./generated/ingestion/sources/mongodb.md)                                            | `pip install 'acryl-datahub[mongodb]'`                     | MongoDB source                          |
| [mssql](./generated/ingestion/sources/mssql.md)                                                | `pip install 'acryl-datahub[mssql]'`                       | SQL Server source                       |
| [mysql](./generated/ingestion/sources/mysql.md)                                                | `pip install 'acryl-datahub[mysql]'`                       | MySQL source                            |
| [mariadb](./generated/ingestion/sources/mariadb.md)                                            | `pip install 'acryl-datahub[mariadb]'`                     | MariaDB source                          |
| [openapi](./generated/ingestion/sources/openapi.md)                                            | `pip install 'acryl-datahub[openapi]'`                     | OpenApi Source                          |
| [oracle](./generated/ingestion/sources/oracle.md)                                              | `pip install 'acryl-datahub[oracle]'`                      | Oracle source                           |
| [postgres](./generated/ingestion/sources/postgres.md)                                          | `pip install 'acryl-datahub[postgres]'`                    | Postgres source                         |
| [redash](./generated/ingestion/sources/redash.md)                                              | `pip install 'acryl-datahub[redash]'`                      | Redash source                           |
| [redshift](./generated/ingestion/sources/redshift.md)                                          | `pip install 'acryl-datahub[redshift]'`                    | Redshift source                         |
| [sagemaker](./generated/ingestion/sources/sagemaker.md)                                        | `pip install 'acryl-datahub[sagemaker]'`                   | AWS SageMaker source                    |
| [snowflake](./generated/ingestion/sources/snowflake.md)                                        | `pip install 'acryl-datahub[snowflake]'`                   | Snowflake source                        |
| [sqlalchemy](./generated/ingestion/sources/sqlalchemy.md)                                      | `pip install 'acryl-datahub[sqlalchemy]'`                  | Generic SQLAlchemy source               |
| [superset](./generated/ingestion/sources/superset.md)                                          | `pip install 'acryl-datahub[superset]'`                    | Superset source                         |
| [tableau](./generated/ingestion/sources/tableau.md)                                            | `pip install 'acryl-datahub[tableau]'`                     | Tableau source                          |
| [trino](./generated/ingestion/sources/trino.md)                                                | `pip install 'acryl-datahub[trino]'`                       | Trino source                            |
| [starburst-trino-usage](./generated/ingestion/sources/trino.md)                                | `pip install 'acryl-datahub[starburst-trino-usage]'`       | Starburst Trino usage statistics source |
| [nifi](./generated/ingestion/sources/nifi.md)                                                  | `pip install 'acryl-datahub[nifi]'`                        | NiFi source                             |
| [powerbi](./generated/ingestion/sources/powerbi.md#module-powerbi)                             | `pip install 'acryl-datahub[powerbi]'`                     | Microsoft Power BI source               |
| [powerbi-report-server](./generated/ingestion/sources/powerbi.md#module-powerbi-report-server) | `pip install 'acryl-datahub[powerbi-report-server]'`       | Microsoft Power BI Report Server source |

### Sinks

| Plugin Name                                                       | Install Command                              | Provides                   |
| ----------------------------------------------------------------- | -------------------------------------------- | -------------------------- |
| [metadata-file](../metadata-ingestion/sink_docs/metadata-file.md) | _included by default_                        | File source and sink       |
| [console](../metadata-ingestion/sink_docs/console.md)             | _included by default_                        | Console sink               |
| [datahub-rest](../metadata-ingestion/sink_docs/datahub.md)        | `pip install 'acryl-datahub[datahub-rest]'`  | DataHub sink over REST API |
| [datahub-kafka](../metadata-ingestion/sink_docs/datahub.md)       | `pip install 'acryl-datahub[datahub-kafka]'` | DataHub sink over Kafka    |

These plugins can be mixed and matched as desired. For example:

```shell
pip install 'acryl-datahub[bigquery,datahub-rest]'
```

### Check the active plugins

```shell
datahub check plugins
```

[extra requirements]: https://www.python-ldap.org/en/python-ldap-3.3.0/installing.html#build-prerequisites

## Release Notes and CLI versions

The server release notes can be found in [github releases](https://github.com/datahub-project/datahub/releases). These releases are done approximately every week on a regular cadence unless a blocking issue or regression is discovered.

CLI release is made through a different repository and release notes can be found in [acryldata releases](https://github.com/acryldata/datahub/releases). At least one release which is tied to the server release is always made alongwith the server release. Multiple other bigfix releases are made in between based on amount of fixes that are merged between the server release mentioned above.

If server with version `0.8.28` is being used then CLI used to connect to it should be `0.8.28.x`. Tests of new CLI are not ran with older server versions so it is not recommended to update the CLI if the server is not updated.
