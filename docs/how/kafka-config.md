---
title: "Configuring Kafka"
hide_title: true
---

# Configuring Kafka in DataHub

DataHub uses Kafka as the pub-sub message queue in the backend.
[Official Confluent Kafka Docker images](https://hub.docker.com/u/confluentinc) found in Docker Hub is used without
any modification. Kafka is used as a durable log that can be used to store inbound
requests to update the Metadata Graph (Metadata Change Proposal), or as a change log detailing the updates
that have been made to the Metadata Graph (Metadata Change Log).

## Environment Variables

The following environment variables can be used to customize DataHub's connection to Kafka for the following DataHub components,
each of which requires a connection to Kafka:

- `metadata-service` (datahub-gms container)
- `system-update` (dathub-system-update container if setting up topics via datahub)
- (Advanced - if standalone consumers are deployed) `mce-consumer-job` (datahub-mce-consumer container)
- (Advanced - if standalone consumers are deployed) `mae-consumer-job` (datahub-mae-consumer container)

### Connection Configuration

With the exception of `KAFKA_BOOTSTRAP_SERVER` and `KAFKA_SCHEMAREGISTRY_URL`, Kafka is configured via [spring-boot](https://spring.io/projects/spring-boot), specifically with [KafkaProperties](https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/kafka/KafkaProperties.html). See [Integration Properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties) prefixed with `spring.kafka`.

Below is an example of how SASL/GSSAPI properties can be configured via environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVER=broker:29092
export KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
export SPRING_KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
export SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
export SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

#### Example: Connecting using AWS IAM (MSK)

Here is another example of how SASL_SSL can be configured for AWS_MSK_IAM when connecting to MSK using IAM via environment variables

```bash
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION=/tmp/kafka.client.truststore.jks
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=AWS_MSK_IAM
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=software.amazon.msk.auth.iam.IAMLoginModule required;
SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

For more information about configuring these variables, check out Spring's [Externalized Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config) to see how this works.
Also see [Kafka Connect Security](https://docs.confluent.io/current/connect/security.html) for more ways to connect.

### Topic Configuration

By default, DataHub relies on the a set of Kafka topics to operate. By default, they have the following names:

1. **MetadataChangeProposal_v1**
2. **FailedMetadataChangeProposal_v1**
3. **MetadataChangeLog_Versioned_v1**
4. **MetadataChangeLog_Timeseries_v1**
5. **DataHubUsageEvent_v1**: User behavior tracking event for UI
6. (Deprecated) **MetadataChangeEvent_v4**: Metadata change proposal messages
7. (Deprecated) **MetadataAuditEvent_v4**: Metadata change log messages
8. (Deprecated) **FailedMetadataChangeEvent_v4**: Failed to process #1 event
9. **MetadataGraphEvent_v4**:
10. **MetadataGraphEvent_v4**:
11. **PlatformEvent_v1**:
12. **DataHubUpgradeHistory_v1**: Notifies the end of DataHub Upgrade job so dependants can act accordingly (_eg_, startup).
    Note this topic requires special configuration: **Infinite retention**. Also, 1 partition is enough for the occasional traffic.

How Metadata Events relate to these topics is discussed at more length in [Metadata Events](../what/mxe.md).

We've included environment variables to customize the name each of these topics, for cases where an organization has naming rules for your topics.

### Metadata Service (datahub-gms) and System Update (datahub-system-update)

The following are environment variables you can use to configure topic names used in the Metadata Service container and
the System Update container for topic setup:

- `METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted by the ingestion framework.
- `FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted when MCPs fail processing.
- `METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Versioned Aspects.
- `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Timeseries Aspects.
- `PLATFORM_EVENT_TOPIC_NAME`: The name of the topic for Platform Events (high-level semantic events).
- `DATAHUB_USAGE_EVENT_NAME`: The name of the topic for product analytics events.
- (Deprecated) `METADATA_CHANGE_EVENT_NAME`: The name of the metadata change event topic.
- (Deprecated) `METADATA_AUDIT_EVENT_NAME`: The name of the metadata audit event topic.
- (Deprecated) `FAILED_METADATA_CHANGE_EVENT_NAME`: The name of the failed metadata change event topic.

#### Topic Setup

- `DATAHUB_PRECREATE_TOPICS`: Defaults to true, set this to false if you intend to create and configure the topics yourself and not have datahub create them.

### MCE Consumer (datahub-mce-consumer)

- `METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted by the ingestion framework.
- `FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted when MCPs fail processing.
- (Deprecated) `METADATA_CHANGE_EVENT_NAME`: The name of the deprecated topic that an embedded MCE consumer will consume from.
- (Deprecated) `FAILED_METADATA_CHANGE_EVENT_NAME`: The name of the deprecated topic that failed MCEs will be written to.

### MAE Consumer (datahub-mae-consumer)

- `METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Versioned Aspects.
- `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Timeseries Aspects.
- `PLATFORM_EVENT_TOPIC_NAME`: The name of the topic for Platform Events (high-level semantic events).
- `DATAHUB_USAGE_EVENT_NAME`: The name of the topic for product analytics events.
- (Deprecated) `METADATA_AUDIT_EVENT_NAME`: The name of the deprecated metadata audit event topic.

Please ensure that these environment variables are set consistently throughout your ecosystem. DataHub has a few different applications running which communicate with Kafka (see above).

## Configuring Consumer Group Id

Kafka Consumers in Spring are configured using Kafka listeners. By default, consumer group id is same as listener id.

We've included an environment variable to customize the consumer group id, if your company or organization has specific naming rules.

### datahub-mce-consumer and datahub-mae-consumer

- `KAFKA_CONSUMER_GROUP_ID`: The name of the kafka consumer's group id.

#### datahub-mae-consumer MCL Hooks

By default, all MetadataChangeLog processing hooks execute as part of the same kafka consumer group based on the
previously mentioned `KAFKA_CONSUMER_GROUP_ID`.

The various MCL Hooks could alsp be separated into separate groups which allows for controlling parallelization and
prioritization of the hooks.

For example, the `UpdateIndicesHook` and `SiblingsHook` processing can be delayed by other hooks. Separating these
hooks into their own group can reduce latency from these other hooks. The `application.yaml` configuration
includes options for assigning a suffix to the consumer group, see `consumerGroupSuffix`.

| Environment Variable                           | Default | Description                                                                                 |
| ---------------------------------------------- | ------- | ------------------------------------------------------------------------------------------- |
| SIBLINGS_HOOK_CONSUMER_GROUP_SUFFIX            | ''      | Siblings processing hook. Considered one of the primary hooks in the `datahub-mae-consumer` |
| UPDATE_INDICES_CONSUMER_GROUP_SUFFIX           | ''      | Primary processing hook.                                                                    |
| INGESTION_SCHEDULER_HOOK_CONSUMER_GROUP_SUFFIX | ''      | Scheduled ingestion hook.                                                                   |
| INCIDENTS_HOOK_CONSUMER_GROUP_SUFFIX           | ''      | Incidents hook.                                                                             |
| ECE_CONSUMER_GROUP_SUFFIX                      | ''      | Entity Change Event hook which publishes to the Platform Events topic.                      |
| FORMS_HOOK_CONSUMER_GROUP_SUFFIX               | ''      | Forms processing.                                                                           |

## Applying Configurations

### Docker

Simply add the above environment variables to the required `docker.env` files for the containers. These can
be found inside the `docker` folder of the repository.

### Helm

On Helm, you'll need to configure these environment variables using the `extraEnvs` sections of the specific container's
configurations inside your `values.yaml` file.

```
datahub-gms:
    ...
    extraEnvs:
      - name: METADATA_CHANGE_PROPOSAL_TOPIC_NAME
        value: "CustomMetadataChangeProposal_v1"
      - name: METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
        value: "CustomMetadataChangeLogVersioned_v1"
      - name: FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME
        value: "CustomFailedMetadataChangeProposal_v1"
      - name: KAFKA_CONSUMER_GROUP_ID
        value: "my-apps-mae-consumer"
        ....
datahub-system-update:
    ...
    extraEnvs:
      - name: METADATA_CHANGE_PROPOSAL_TOPIC_NAME
        value: "CustomMetadataChangeProposal_v1"
      - name: METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
        value: "CustomMetadataChangeLogVersioned_v1"
      - name: FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME
        value: "CustomFailedMetadataChangeProposal_v1"
      - name: KAFKA_CONSUMER_GROUP_ID
        value: "my-apps-mae-consumer"
        ....


datahub-frontend:
    ...
    extraEnvs:
        - name: DATAHUB_TRACKING_TOPIC
          value: "MyCustomTrackingEvent"

# If standalone consumers are enabled
datahub-mae-consumer;
    extraEnvs:
        - name: METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME
          value: "CustomMetadataChangeLogVersioned_v1"
          ....
        - name: METADATA_AUDIT_EVENT_NAME
          value: "MetadataAuditEvent"
datahub-mce-consumer;
    extraEnvs:
        - name: METADATA_CHANGE_PROPOSAL_TOPIC_NAME
          value: "CustomMetadataChangeLogVersioned_v1"
          ....
        - name: METADATA_CHANGE_EVENT_NAME
          value: "MetadataChangeEvent"
        ....
```

## Other Components that use Kafka can be configured using environment variables:

- schema-registry

## SASL/GSSAPI properties for system-update and datahub-frontend via environment variables

```bash
KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081
KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

## SASL/GSSAPI properties for schema-registry via environment variables

```bash
SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=broker:29092
SCHEMA_REGISTRY_KAFKASTORE_SASL_KERBEROS_SERVICE_NAME=kafka
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_PLAINTEXT
SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

## SSL

### Kafka

We are using the Spring Boot framework to start our apps, including setting up Kafka. You can
[use environment variables to set system properties](https://docs.spring.io/spring-boot/docs/current/reference/html/spring-boot-features.html#boot-features-external-config-relaxed-binding-from-environment-variables),
including [Kafka properties](https://docs.spring.io/spring-boot/docs/current/reference/html/appendix-application-properties.html#integration-properties).
From there you can set your SSL configuration for Kafka.

### Schema Registry

If Schema Registry is configured to use security (SSL), then you also need to set additional values.

The [MCE](../../metadata-jobs/mce-consumer-job) and [MAE](../../metadata-jobs/mae-consumer-job) consumers can set
default Spring Kafka environment values, for example:

- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
- `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

[GMS](../what/gms.md) can set the following environment variables that will be passed as properties when creating the Schema Registry
Client.

- `KAFKA_SCHEMA_REGISTRY_SECURITY_PROTOCOL`
- `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`
- `KAFKA_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`
- `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION`
- `KAFKA_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD`

> **Note** In the logs you might see something like
> `The configuration 'kafkastore.ssl.truststore.password' was supplied but isn't a known config.` The configuration is
> not a configuration required for the producer. These WARN message can be safely ignored. Each of Datahub services are
> passed a full set of configuration but may not require all the configurations that are passed to them. These warn
> messages indicate that the service was passed a configuration that is not relevant to it and can be safely ignored.

> Other errors: `Failed to start bean 'org.springframework.kafka.config.internalKafkaListenerEndpointRegistry'; nested exception is org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [DataHubUsageEvent_v1]`. Please check ranger permissions or kafka broker logs.

### Additional Kafka Topic level configuration

For additional [Kafka topic level config properties](https://kafka.apache.org/documentation/#topicconfigs), either add them to application.yaml under `kafka.topics.<topicName>.configProperties` or `kafka.topicDefaults.configProperties` or define env vars in the following form (standard Spring conventions applied to application.yaml)
These env vars are required in datahub-system-update contained.

Examples:

1. To configure `max.message.bytes` on topic used for `metadataChangeLogVersioned`, set
   `KAFKA_TOPICS_metadataChangeLogVersioned_CONFIGPROPERTIES_max_message_bytes=10000`
2. To configure `max.message.bytes` for all topics that don't explicitly define one, set the `topicDefaults` via
   `KAFKA_TOPICDEFAULTS_CONFIGPROPERTIES_max_message_bytes=10000`

Configurations specified in `topicDefaults` are applied to all topics by merging them with any configs defined per topic, with the per-topic config taking precedence over those specified in `topicDefault`.

### Schema Registry Topic Configuration

When using Confluent Schema Registry, DataHub's system update job attempts to configure the cleanup policy for the `_schemas` topic. If your Kafka user does not have `ALTER_CONFIGS` permission on the Schema Registry topics, you can disable this step by setting `USE_CONFLUENT_SCHEMA_REGISTRY` to `false`:

```bash
USE_CONFLUENT_SCHEMA_REGISTRY=false
```

This configuration is useful in enterprise environments where:

- Schema Registry and its topics are managed separately
- DataHub's Kafka user has restricted permissions
- The `_schemas` topic cleanup policy is already configured by your Kafka administrators

**Helm configuration:**

You can use the built-in helm chart value to control this behavior:

```yaml
global:
  kafka:
    schemaregistry:
      configureCleanupPolicy: false
```

| Parameter                                            | Type    | Default | Description                                                                                                                                                                                                   |
| ---------------------------------------------------- | ------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `global.kafka.schemaregistry.configureCleanupPolicy` | boolean | null    | Whether to configure cleanup policy on schema registry. By default, a suitable default is chosen based on `global.kafka.schemaregistry.type` (true only if type is KAFKA). Set this to have explicit control. |

Alternatively, you can use the environment variable directly:

```yaml
datahub-system-update:
  extraEnvs:
    - name: USE_CONFLUENT_SCHEMA_REGISTRY
      value: "false"
```

> **Note:** Setting `configureCleanupPolicy: false` or `USE_CONFLUENT_SCHEMA_REGISTRY=false` disables the schema registry cleanup policy configuration step during upgrades. Use this if you encounter `TopicAuthorizationException` errors and your Schema Registry topics are managed externally.

## Debugging Kafka

You can install [kafkacat](https://github.com/edenhill/kafkacat) to consume and produce messaged to Kafka topics.
For example, to consume messages on MetadataAuditEvent topic, you can run below command.

```
kafkacat -b localhost:9092 -t MetadataAuditEvent
```

However, `kafkacat` currently doesn't support Avro deserialization at this point,
but they have an ongoing [work](https://github.com/edenhill/kafkacat/pull/151) for that.
