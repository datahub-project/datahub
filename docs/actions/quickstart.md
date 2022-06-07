# DataHub Actions Quickstart


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

To install DataHub Actions, you need to install the `acryl-datahub-actions` package from PyPi

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub-actions

# Verify the installation by checking the version.
datahub actions version
```

### Hello World

DataHub ships with a "Hello World" Action which logs all events it receives to the console.
To run this action, simply create a new Action configuration file:

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
action:
  type: "hello_world"
```

and then run it using the `datahub actions` command:

```shell
datahub actions -c hello_world.yaml
```

You should the see the following output if the Action has been started successfully:

```shell
Action Pipeline with name 'hello_world' is now running.
```

Now, navigate to the instance of DataHub that you've connected to and perform an Action such as

- Adding / removing a Tag
- Adding / removing a Glossary Term
- Adding / removing a Domain

If all is well, you should see some events being logged to the console

```shell
Hello world! Received event:
{
    "event_type": "EntityChangeEvent_v1",
    "event": {
        "entityType": "dataset",
        "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
        "category": "TAG",
        "operation": "ADD",
        "modifier": "urn:li:tag:pii",
        "parameters": {},
        "auditStamp": {
            "time": 1651082697703,
            "actor": "urn:li:corpuser:datahub",
            "impersonator": null
        },
        "version": 0,
        "source": null
    },
    "meta": {
        "kafka": {
            "topic": "PlatformEvent_v1",
            "offset": 1262,
            "partition": 0
        }
    }
}
```
*An example of an event emitted when a 'pii' tag has been added to a Dataset.* 

Woohoo! You've successfully started using the Actions framework. Now, let's see how we can get fancy.


#### Filtering events

If we know which Event types we'd like to consume, we can optionally add a `filter` configuration, which
will prevent events that do not match the filter from being forwarded to the action.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
action:
  type: "hello_world"
```
*Filtering for events of type EntityChangeEvent_v1 only*


#### Advanced Filtering

Beyond simply filtering by event type, we can also filter events by matching against the values of their fields. To do so,
use the `event` block. Each field provided will be compared against the real event's value. An event that matches
**all** of the fields will be forwarded to the action.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
  event: 
    category: "TAG"
    operation: "ADD"
    modifier: "urn:li:tag:pii"
action:
  type: "hello_world"
```
*This filter only matches events representing "PII" tag additions to an entity.*

And more, we can achieve "OR" semantics on a particular field by providing an array of values.

```yaml
# hello_world.yaml
name: "hello_world"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
filter:
  event_type: "EntityChangeEvent_v1"
  event: 
    category: "TAG"
    operation: [ "ADD", "REMOVE" ]
    modifier: "urn:li:tag:pii"
action:
  type: "hello_world"
```
*This filter only matches events representing "PII" tag additions to OR removals from an entity. How fancy!*
