# DataHub Actions Concepts

The Actions framework includes pluggable components for filtering, transforming, and reacting to important DataHub, such as  

- Tag Additions / Removals
- Glossary Term Additions / Removals
- Schema Field Additions / Removals
- Owner Additions / Removals

& more, in real time.

DataHub Actions comes with open library of freely available Transformers, Actions, Events, and more.

Finally, the framework is highly configurable & scalable. Notable highlights include:

- **Distributed Actions**: Ability to scale-out processing for a single action. Support for running the same Action configuration across multiple nodes to load balance the traffic from the event stream.
- **At-least Once Delivery**: Native support for independent processing state for each Action via post-processing acking to achieve at-least once semantics.
- **Robust Error Handling**: Configurable failure policies featuring event-retry, dead letter queue, and failed-event continuation policy to achieve the guarantees required by your organization.


### Use Cases

Real-time use cases broadly fall into the following categories:

- **Notifications**: Generate organization-specific notifications when a change is made on DataHub. For example, send an email to the governance team when a "PII" tag is added to any data asset.
- **Workflow Integration**: Integrate DataHub into your organization's internal workflows. For example, create a Jira ticket when specific Tags or Terms are proposed on a Dataset.
- **Synchronization**: Syncing changes made in DataHub into a 3rd party system. For example, reflecting Tag additions in DataHub into Snowflake.
- **Auditing**: Audit who is making what changes on DataHub through time. 

and more! 

## Concepts

The Actions Framework consists of a few core concepts--

- **Pipelines**
- **Events** and **Event Sources**
- **Transformers**
- **Actions**

Each of these will be described in detail below.

![](imgs/actions.png)
**In the Actions Framework, Events flow continuously from left-to-right.** 

### Pipelines

A **Pipeline** is a continuously running process which performs the following functions:

1. Polls events from a configured Event Source (described below)
2. Applies configured Transformation + Filtering to the Event 
3. Executes the configured Action on the resulting Event

in addition to handling initialization, errors, retries, logging, and more. 

Each Action Configuration file corresponds to a unique Pipeline. In practice,
each Pipeline has its very own Event Source, Transforms, and Actions. This makes it easy to maintain state for mission-critical Actions independently. 

Importantly, each Action must have a unique name. This serves as a stable identifier across Pipeline run which can be useful in saving the Pipeline's consumer state (ie. resiliency + reliability). For example, the Kafka Event Source (default) uses the pipeline name as the Kafka Consumer Group id. This enables you to easily scale-out your Actions by running multiple processes with the same exact configuration file. Each will simply become different consumers in the same consumer group, sharing traffic of the DataHub Events stream.

### Events

**Events** are data objects representing changes that have occurred on DataHub. Strictly speaking, the only requirement that the Actions framework imposes is that these objects must be 

a. Convertible to JSON
b. Convertible from JSON

So that in the event of processing failures, events can be written and read from a failed events file. 


#### Event Types

Each Event instance inside the framework corresponds to a single **Event Type**, which is common name (e.g. "EntityChangeEvent_v1") which can be used to understand the shape of the Event. This can be thought of as a "topic" or "stream" name. That being said, Events associated with a single type are not expected to change in backwards-breaking ways across versons.

### Event Sources

Events are produced to the framework by **Event Sources**. Event Sources may include their own guarantees, configurations, behaviors, and semantics. They usually produce a fixed set of Event Types. 

In addition to sourcing events, Event Sources are also responsible for acking the succesful processing of an event by implementing the `ack` method. This is invoked by the framework once the Event is guaranteed to have reached the configured Action successfully. 

### Transformers

**Transformers** are pluggable components which take an Event as input, and produce an Event (or nothing) as output. This can be used to enrich the information of an Event prior to sending it to an Action. 

Multiple Transformers can be configured to run in sequence, filtering and transforming an event in multiple steps.

Transformers can also be used to generate a completely new type of Event (i.e. registered at runtime via the Event Registry) which can subsequently serve as input to an Action. 

Transformers can be easily customized and plugged in to meet an organization's unqique requirements. For more information on developing a Transformer, check out [Developing a Transformer](guides/developing-a-transformer.md)


### Action

**Actions** are pluggable components which take an Event as input and perform some business logic. Examples may be sending a Slack notification, logging to a file,
or creating a Jira ticket, etc. 

Each Pipeline can be configured to have a single Action which runs after the filtering and transformations have occurred. 

Actions can be easily customized and plugged in to meet an organization's unqique requirements. For more information on developing a Action, check out [Developing a Action](guides/developing-an-action.md)


