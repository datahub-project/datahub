---
description: "Install and use the DataHub CLI to ingest metadata, manage entities, run quickstart deployments, and perform common operations."
# Display to h4 headings
# toc_min_heading_level: 2
toc_max_heading_level: 4
---

# DataHub CLI

DataHub comes with a friendly cli called `datahub` that allows you to perform a lot of common operations using just the command line. [DataHub](https://datahub.com) maintains the [pypi package](https://pypi.org/project/acryl-datahub/) for `datahub`.

## Installation

### Using Homebrew (macOS / Linux)

The simplest way to install the DataHub CLI on macOS or Linux:

```shell
brew install datahub-project/tap/datahub
datahub version
datahub init
# authenticate your datahub CLI with your datahub instance
```

Homebrew manages an isolated Python environment for `datahub`, so there's no venv to activate and no PATH conflicts with system Python. To upgrade later, run `brew upgrade datahub`.

**Note:** The Homebrew formula installs the **core CLI only**. To add ingestion connectors (Snowflake, BigQuery, Looker, etc.), install them into the brew-managed environment:

```shell
"$(brew --prefix datahub)/libexec/bin/pip" install 'acryl-datahub[snowflake,bigquery]'
```

The full list of available connector extras is shown in the [Installing Plugins](./cli-commands/plugins.md) section below.

### Using pip

We recommend Python virtual environments (venv-s) to namespace pip modules. Here's an example setup:

```shell
python3 -m venv venv             # create the environment
source venv/bin/activate         # activate the environment
```

**_NOTE:_** If you install `datahub` in a virtual environment, that same virtual environment must be re-activated each time a shell window or session is created.

Once inside the virtual environment, install `datahub` using the following commands

```shell
# Requires Python 3.10+
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
  check         Helper commands for checking various aspects of DataHub.
  container     A group of commands to interact with containers in DataHub.
  dataproduct   A group of commands to interact with the DataProduct entity in DataHub.
  dataset       A group of commands to interact with the Dataset entity in DataHub.
  delete        Delete metadata from DataHub.
  docker        Helper commands for setting up and interacting with a local DataHub instance using Docker.
  exists        A group of commands to check existence of entities in DataHub.
  forms         A group of commands to interact with forms in DataHub.
  get           A group of commands to get metadata from DataHub.
  graphql       Execute GraphQL queries and mutations against DataHub.
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

| Command                                                                                                                                                      | What it does                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| [`init` and profiles](./cli-commands/init.md)                                                                                                                | Connect the CLI to DataHub instances                 |
| [`ingest`](./cli-commands/ingest.md)                                                                                                                         | Run, record, replay and deploy ingestion recipes     |
| [`recipe`](./cli-commands/recipe.md)                                                                                                                         | Inspect, validate and probe recipes                  |
| [`get`, `put`, `delete`, `exists`, `timeline`](./cli-commands/metadata.md)                                                                                   | Read and write metadata                              |
| [`search`](./cli-commands/search.md)                                                                                                                         | Search the catalog                                   |
| [`graphql`](./cli-commands/graphql.md)                                                                                                                       | Run GraphQL queries and mutations                    |
| [`dataset`](./cli-commands/dataset.md), [`user`](./cli-commands/user.md), [`group`](./cli-commands/group.md), [`dataproduct`](./cli-commands/dataproduct.md) | Manage entities from YAML                            |
| [`check`](./cli-commands/check.md)                                                                                                                           | Check metadata files, plugins and the server         |
| [`migrate`](./cli-commands/migrate.md)                                                                                                                       | Migrate metadata between platform instances and URNs |
| [Plugins](./cli-commands/plugins.md)                                                                                                                         | Install source and sink plugins                      |

### docker

The `docker` command allows you to start up a local DataHub instance using `datahub docker quickstart`. You can also check if the docker cluster is healthy using `datahub docker check`.

### version

The `version` command allows you to get version number of CLI that you are using.

```console
datahub version
DataHub CLI version: 1.2.0.9
Models: bundled
Python version: 3.11.11 (main, Mar 17 2025, 21:33:08) [Clang 20.1.0 ]
```

You can pass `--include-server` flag to include server information too. This is helpful to share cli and server version at once for debugging purposes. This does require `datahub init` to be run before that so there is a server that CLI is able to connect to.

```console
datahub version --include-server
DataHub CLI version: 1.2.0.9
Models: bundled
Python version: 3.11.11 (main, Mar 17 2025, 21:33:08) [Clang 20.1.0 ]
Server config: {'models': {}, 'managedIngestion': {'defaultCliVersion': '1.2.0.9', 'enabled': True}, 'timeZone': 'GMT', 'datasetUrnNameCasing': False, 'datahub': {'serverEnv': 'cloud', 'serverType': 'prod'}, 'baseUrl': 'https://xyz.acryl.io', 'patchCapable': True, 'versions': {'acryldata/datahub': {'version': 'v0.3.14rc0', 'commit': '464d94926bb21a596f3d9d81164b272c74872ce7'}}, 'statefulIngestionCapable': True, 'remoteExecutorBackend': {'revision': 2}, 'supportsImpactAnalysis': True, 'telemetry': {'enabledCli': False, 'enabledMixpanel': False, 'enabledServer': True, 'enabledIngestion': True}, 'retention': 'true', 'noCode': 'true'}
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

### graphql

The `graphql` command allows you to execute GraphQL queries and mutations against DataHub's GraphQL API. This provides full access to DataHub's metadata through its native GraphQL interface.

```shell
# Execute a GraphQL query
datahub graphql --query "query { me { username } }"

# Use named operations from DataHub's schema
datahub graphql --operation searchAcrossEntities --variables '{"input": {"query": "users"}}'

# List available operations
datahub graphql --list-operations

# Get help for a specific operation
datahub graphql --describe searchAcrossEntities

# Explore types recursively
datahub graphql --describe SearchInput --recurse

# Load queries and variables from files
datahub graphql --query ./search-tags.graphql --variables ./search-params.json

# Get JSON output for LLM integration
datahub graphql --list-operations --format json
```

The GraphQL command supports both raw GraphQL queries/mutations and operation-based execution using DataHub's introspected schema. It automatically detects whether `--query` and `--variables` arguments are file paths or literal content, enabling seamless use of both inline GraphQL and file-based queries.

Key features:

- **Schema discovery**: List and describe all available operations and types
- **File support**: Load queries and variables from `.graphql` and `.json` files
- **LLM-friendly output**: JSON format with complete type information
- **Recursive exploration**: Deep-dive into complex GraphQL types

➡️ [Learn more about the GraphQL command](./cli-commands/graphql.md)

### search

The `search` command allows you to search across all DataHub entities from the command line with support for both keyword and semantic search.

```shell
# Basic keyword search
datahub search "users"

# Semantic search
datahub search --semantic "financial reports"

# Search with filters
datahub search "*" --filter platform=snowflake --filter entity_type=dataset

# Get results in table format
datahub search "*" --table
```

Key features:

- **Dual search modes**: Keyword search (default) and semantic search
- **Flexible filtering**: Simple key=value filters and complex JSON filters with AND/OR/NOT logic
- **Multiple output formats**: JSON (default), table, and URNs-only
- **Discovery features**: List available filters and describe specific filters
- **Faceted search**: Explore data distribution with aggregation counts
- **Pagination & sorting**: Control result size and ordering

➡️ [Learn more about the search command](./cli-commands/search.md)

## Entity Specific Commands

### dataset (Dataset Entity)

The `dataset` command allows you to interact with Dataset entities in DataHub, including creating, updating, retrieving, and validating Dataset metadata.

```shell
# Get a dataset and write to YAML file
datahub dataset get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,example_table,PROD)" --to-file dataset.yaml

# Create or update dataset from YAML file
datahub dataset upsert -f dataset.yaml
```

➡️ [Learn more about the dataset command](./cli-commands/dataset.md)

## Miscellaneous Admin Commands

### lite (experimental)

The lite group of commands allow you to run an embedded, lightweight DataHub instance for command line exploration of your metadata. This is intended more for developer tool oriented usage rather than as a production server instance for DataHub. See [DataHub Lite](./datahub_lite.md) for more information about how you can ingest metadata into DataHub Lite and explore your metadata easily.

### telemetry

To help us understand how people are using DataHub, we collect anonymous usage statistics on actions such as command invocations via Mixpanel.
We do not collect private information such as IP addresses, contents of ingestions, or credentials.
The code responsible for collecting and broadcasting these events is open-source and can be found [within our GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/telemetry/telemetry.py).

Telemetry is enabled by default, and the `telemetry` command lets you toggle the sending of these statistics via `telemetry enable/disable`.

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

## Release Notes and CLI versions

The server release notes can be found in [github releases](https://github.com/datahub-project/datahub/releases). These releases are done approximately every week on a regular cadence unless a blocking issue or regression is discovered.

CLI release is made through a different repository and release notes can be found in [acryldata releases](https://github.com/acryldata/datahub/releases). At least one release which is tied to the server release is always made alongwith the server release. Multiple other bigfix releases are made in between based on amount of fixes that are merged between the server release mentioned above.

If server with version `0.8.28` is being used then CLI used to connect to it should be `0.8.28.x`. Tests of new CLI are not ran with older server versions so it is not recommended to update the CLI if the server is not updated.
