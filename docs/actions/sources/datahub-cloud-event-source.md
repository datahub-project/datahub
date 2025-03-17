# DataHub Cloud Event Source

## Prerequisites

### Compatibility

The **DataHub Cloud Event Source** is only compatible with versions of DataHub Cloud above  `v0.3.7`.

### Privileges

By default, users do not have access to the Events API of DataHub Cloud. In order to access the API, the user or service account
associated with the access token used to configure this events source _must_ have the `Get Platform Events` platform privilege, which
can be granted using an [Access Policy](https://datahubproject.io/docs/authorization/access-policies-guide/).

## Overview

The DataHub Cloud Event Source allows you to use DataHub Actions with an instance of DataHub Cloud hosted by [Acryl](https://acryl.io).

Under the hood, the DataHub Cloud Event Source communicates with DataHub Cloud to extract change events in realtime.
The state of progress is automatically saved to DataHub Cloud after messages are processed, allowing you to seamlessly pause and restart the consumer, using the provided `name` to uniquely identify the consumer state.

On initial startup of a new consumer id, the DataHub event source will automatically begin the _latest_ events by default. Afterwards, the message stream processed offsets will be continually saved. However, the source can also optionally be configured to "look back" in time
by a certain number of days on initial bootstrap using the `lookback_days` parameter. To reset all previously saved offsets for a consumer,
you can set `reset_offsets` to `True`.

### Processing Guarantees

This event source implements an "ack" function which is invoked if and only if an event is successfully processed
by the Actions framework, meaning that the event made it through the Transformers and into the Action without
any errors. Under the hood, the "ack" method synchronously commits DataHub Cloud Consumer Offsets on behalf of the Action. This means that by default, the framework provides *at-least once* processing semantics. That is, in the unusual case that a failure occurs when attempting to commit offsets back to Kafka, that event may be replayed on restart of the Action.

If you've configured your Action pipeline `failure_mode` to be `CONTINUE` (the default), then events which
fail to be processed will simply be logged to a `failed_events.log` file for further investigation (dead letter queue). The DataHub Cloud Event Source will continue to make progress against the underlying topics and continue to commit offsets even in the case of failed messages.

If you've configured your Action pipeline `failure_mode` to be `THROW`, then events which fail to be processed result in an Action Pipeline error. This in turn terminates the pipeline before committing offsets back to DataHub Cloud. Thus the message will not be marked as "processed" by the Action consumer.

## Supported Events

The DataHub Cloud Event Source produces

- [Entity Change Event V1](../../managed-datahub/datahub-api/entity-events-api.md)

Note that the DataHub Cloud Event Source does _not_ yet support the full [Metadata Change Log V1](../events/metadata-change-log-event.md) event stream.

## Configure the Event Source

Use the following config(s) to get started with the DataHub Cloud Event Source.

### Quickstart

To start listening for new events from now, you can use the following recipe:

```yml
name: "unique-action-name"
datahub:
  server: "https://<your-organization>.acryl.io"
  token: "<your-datahub-cloud-token>"
source:
  type: "datahub-cloud"
action:
  # action configs
```

Note that the `datahub` configuration block is **required** to connect to your DataHub Cloud instance.

### Advanced Configurations

To reset the offsets for the action pipeline and start consuming events from 7 days ago, you can use the following recipe:

```yml
name: "unique-action-name"
datahub:
  server: "https://<your-organization>.acryl.io"
  token: "<your-datahub-cloud-token>"
source:
  type: "datahub-cloud"
  config:
    lookback_days: 7                           # Look back 7 days for events
    reset_offsets: true                        # Ignore stored offsets and start fresh
    kill_after_idle_timeout: true              # Enable shutdown after idle period
    idle_timeout_duration_seconds: 60          # Idle timeout set to 60 seconds
    event_processing_time_max_duration_seconds: 45  # Max processing time of 45 seconds per batch
action:
  # action configs
```

Note that the `datahub` configuration block is **required** to connect to your DataHub Cloud instance.

<details>
  <summary>View All Configuration Options</summary>

| Field                                 | Required | Default                       | Description                                                                               |
  | ------------------------------------- | :------: | :---------------------------: | ----------------------------------------------------------------------------------------- |
| `topic`                               |    ❌    | `PlatformEvent_v1`            | The name of the topic from which events will be consumed. Do not change this unless you know what you're doing!                                |
| `lookback_days`                       |    ❌    | None                           | Optional number of days to look back when polling for events.                             |
| `reset_offsets`                       |    ❌    | `False`                       | When set to `True`, the consumer will ignore any stored offsets and start fresh.          |
| `kill_after_idle_timeout`             |    ❌    | `False`                       | If `True`, stops the consumer after being idle for the specified timeout duration.        |
| `idle_timeout_duration_seconds`       |    ❌    | `30`                          | Duration in seconds after which, if no events are received, the consumer is considered idle. |
| `event_processing_time_max_duration_seconds` | ❌  | `30`                          | Maximum allowed time in seconds for processing events before timing out.                  |
</details>


## FAQ

1. Is there a way to always start processing from the end of the topics on Actions start?

Yes, simply set `reset_offsets` to True for a single run of the action. Remember to disable this for subsequent runs if you don't want to miss any events!

2. What happens if I have multiple actions with the same pipeline `name` running? Can I scale out horizontally?

Today, there is undefined behavior deploying multiple actions with the same name using the DataHub Cloud Events Source.
All events must be processed by a single running action

