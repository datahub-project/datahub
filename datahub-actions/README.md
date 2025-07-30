# ⚡ DataHub Actions Framework

Welcome to DataHub Actions! The Actions framework makes responding to realtime changes in your Metadata Graph easy, enabling you to seamlessly integrate [DataHub](https://github.com/datahub-project/datahub) into a broader events-based architecture.

For a detailed introduction, check out the [original announcement](https://www.youtube.com/watch?v=7iwNxHgqxtg&t=2189s) of the DataHub Actions Framework at the DataHub April 2022 Town Hall. For a more in-depth look at use cases and concepts, check out [DataHub Actions Concepts](../docs/actions/concepts.md).

## Quickstart

To get started right away, check out the [DataHub Actions Quickstart](../docs/actions/quickstart.md) Guide.

## Prerequisites

The DataHub Actions CLI commands are an extension of the base `datahub` CLI commands. We recommend
first installing the `datahub` CLI:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub --version
```

> Note that the Actions Framework requires a version of `acryl-datahub` >= v0.8.34

## Installation

Next, simply install the `acryl-datahub-actions` package from PyPi:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions
datahub actions version
```

## Configuring an Action

Actions are configured using a YAML file, much in the same way DataHub ingestion sources are. An action configuration file consists of the following

1. Action Pipeline Name (Should be unique and static)
2. Source Configurations
3. Transform + Filter Configurations
4. Action Configuration
5. Pipeline Options (Optional)
6. DataHub API configs (Optional - required for select actions)

With each component being independently pluggable and configurable.

```yml
# 1. Required: Action Pipeline Name
name: <action-pipeline-name>

# 2. Required: Event Source - Where to source event from.
source:
  type: <source-type>
  config:
    # Event Source specific configs (map)

# 3a. Optional: Filter to run on events (map)
filter:
  event_type: <filtered-event-type>
  event:
    # Filter event fields by exact-match
    <filtered-event-fields>

# 3b. Optional: Custom Transformers to run on events (array)
transform:
  - type: <transformer-type>
    config:
      # Transformer-specific configs (map)

# 4. Required: Action - What action to take on events.
action:
  type: <action-type>
  config:
    # Action-specific configs (map)

# 5. Optional: Additional pipeline options (error handling, etc)
options:
  retry_count: 0 # The number of times to retry an Action with the same event. (If an exception is thrown). 0 by default.
  failure_mode: "CONTINUE" # What to do when an event fails to be processed. Either 'CONTINUE' to make progress or 'THROW' to stop the pipeline. Either way, the failed event will be logged to a failed_events.log file.
  failed_events_dir: "/tmp/datahub/actions" # The directory in which to write a failed_events.log file that tracks events which fail to be processed. Defaults to "/tmp/logs/datahub/actions".

# 6. Optional: DataHub API configuration
datahub:
  server: "http://localhost:8080" # Location of DataHub API
  # token: <your-access-token> # Required if Metadata Service Auth enabled
```

### Example: Hello World

An simple configuration file for a "Hello World" action, which simply prints all events it receives, is

```yml
# 1. Action Pipeline Name
name: "hello_world"
# 2. Event Source: Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
# 3. Action: What action to take on events.
action:
  type: "hello_world"
```

We can modify this configuration further to filter for specific events, by adding a "filter" block.

```yml
# 1. Action Pipeline Name
name: "hello_world"

# 2. Event Source - Where to source event from.
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}

# 3. Filter - Filter events that reach the Action
filter:
  event_type: "EntityChangeEvent_v1"
  event:
    category: "TAG"
    operation: "ADD"
    modifier: "urn:li:tag:pii"

# 4. Action - What action to take on events.
action:
  type: "hello_world"
```

## Running an Action

To run a new Action, just use the `actions` CLI command

```
datahub actions -c <config.yml>
```

Once the Action is running, you will see

```
Action Pipeline with name '<action-pipeline-name>' is now running.
```

### Running multiple Actions

You can run multiple actions pipeline within the same command. Simply provide multiple
config files by restating the "-c" command line argument.

For example,

```
datahub actions -c <config-1.yaml> -c <config-2.yaml>
```

### Running in debug mode

Simply append the `--debug` flag to the CLI to run your action in debug mode.

```
datahub actions -c <config.yaml> --debug
```

### Stopping an Action

Just issue a Control-C as usual. You should see the Actions Pipeline shut down gracefully, with a small
summary of processing results.

```
Actions Pipeline with name '<action-pipeline-name' has been stopped.
```

## Supported Events

Two event types are currently supported. Read more about them below.

- [Entity Change Event V1](../docs/actions/events/entity-change-event.md)
- [Metadata Change Log V1](../docs/actions/events/metadata-change-log-event.md)

## Supported Event Sources

Currently, the only event source that is officially supported is `kafka`, which polls for events
via a Kafka Consumer.

- [Kafka Event Source](../docs/actions/sources/kafka-event-source.md)

## Supported Actions

By default, DataHub supports a set of standard actions plugins. These can be found inside the folder
`src/datahub-actions/plugins`.

Some pre-included Actions include

- [Hello World](../docs/actions/actions/hello_world.md)
- [Executor](../docs/actions/actions/executor.md)

## Development

### Build and Test

Notice that we support all actions command using a separate `datahub-actions` CLI entry point. Feel free
to use this during development.

```
# Build datahub-actions module
./gradlew datahub-actions:build

# Drop into virtual env
cd datahub-actions && source venv/bin/activate

# Start hello world action
datahub-actions actions -c ../examples/hello_world.yaml

# Start ingestion executor action
datahub-actions actions -c ../examples/executor.yaml

# Start multiple actions
datahub-actions actions -c ../examples/executor.yaml -c ../examples/hello_world.yaml
```

### Developing a Transformer

To develop a new Transformer, check out the [Developing a Transformer](../docs/actions/guides/developing-a-transformer.md) guide.

### Developing an Action

To develop a new Action, check out the [Developing an Action](../docs/actions/guides/developing-an-action.md) guide.

## Contributing

Contributing guidelines follow those of the [main DataHub project](../docs/CONTRIBUTING.md). We are accepting contributions for Actions, Transformers, and general framework improvements (tests, error handling, etc).

## Resources

Check out the [original announcement](https://www.youtube.com/watch?v=7iwNxHgqxtg&t=2189s) of the DataHub Actions Framework at the DataHub April 2022 Town Hall.

## License

[Apache 2.0](./LICENSE)
