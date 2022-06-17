---
title: "Configuring Kafka"
hide_title: true
---

# Configuring Kafka in DataHub

DataHub requires Kafka to operate. Kafka is used as a durable log that can be used to store inbound
requests to update the Metadata Graph (Metadata Change Proposal), or as a change log detailing the updates
that have been made to the Metadata Graph (Metadata Change Log). 

## Environment Variables

The following environment variables can be used to customize DataHub's connection to Kafka for the following DataHub components,
each of which requires a connection to Kafka:

- `metadata-service` (datahub-gms container)
- (Advanced - if standalone consumers are deployed) `mce-consumer-job` (datahub-mce-consumer container)
- (Advanced - if standalone consumers are deployed) `mae-consumer-job` (datahub-mae-consumer container)
- (Advanced - if product analytics are enabled) datahub-frontend

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

- **MetadataChangeProposal_v1**
- **FailedMetadataChangeProposal_v1**
- **MetadataChangeLog_Versioned_v1**
- **MetadataChangeLog_Timeseries_v1**
- **DataHubUsageEvent_v1**: User behavior tracking event for UI
6. (Deprecated) **MetadataChangeEvent_v4**: Metadata change proposal messages
7. (Deprecated) **MetadataAuditEvent_v4**: Metadata change log messages
8. (Deprecated) **FailedMetadataChangeEvent_v4**: Failed to process #1 event

These topics are discussed at more length in [Metadata Events](../what/mxe.md).

We've included environment variables to customize the name each of these topics, for cases where an organization has naming rules for your topics.

### Metadata Service (datahub-gms)

The following are environment variables you can use to configure topic names used in the Metadata Service container:

- `METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted by the ingestion framework.
- `FAILED_METADATA_CHANGE_PROPOSAL_TOPIC_NAME`: The name of the topic for Metadata Change Proposals emitted when MCPs fail processing.
- `METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Versioned Aspects.
- `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: The name of the topic for Metadata Change Logs that are produced for Timeseries Aspects.
- `METADATA_CHANGE_LOG_TIMESERIES_TOPIC_NAME`: The name of the topic for Platform Events (high-level semantic events).
- `DATAHUB_USAGE_EVENT_NAME`: The name of the topic for product analytics events. 
- (Deprecated) `METADATA_CHANGE_EVENT_NAME`: The name of the metadata change event topic.
- (Deprecated) `METADATA_AUDIT_EVENT_NAME`: The name of the metadata audit event topic.
- (Deprecated) `FAILED_METADATA_CHANGE_EVENT_NAME`: The name of the failed metadata change event topic.
- (Deprecated) `KAFKA_MCE_TOPIC_NAME`: The name of the deprecated topic that an embedded MCE consumer will consume from. This should technically be
the same as `METADATA_CHANGE_EVENT_NAME` and will soon be removed. 
- (Deprecated) `KAFKA_FMCE_TOPIC_NAME`: The name of the deprecated topic that failed MCEs will be written to. This should technically be
  the same as `FAILED_METADATA_CHANGE_EVENT_NAME` and will soon be removed.
- (Deprecated) `KAFKA_TOPIC_NAME`: The name of the deprecated topic that MAEs are writtent to. This is used by the MAE consumer when
reading messages. It should contain the same value as `METADATA_AUDIT_EVENT_NAME` and will soon be removed.
  
### MCE Consumer (datahub-mce-consumer)

- (Deprecated) `KAFKA_MCE_TOPIC_NAME`: The name of the deprecated topic that an embedded MCE consumer will consume from. This should technically be
  the same as `METADATA_CHANGE_EVENT_NAME` and will soon be removed and replaced by `METADATA_CHANGE_EVENT_NAME`.
- (Deprecated) `KAFKA_FMCE_TOPIC_NAME`: The name of the deprecated topic that failed MCEs will be written to. This should technically be
  the same as `FAILED_METADATA_CHANGE_EVENT_NAME` and will soon be removed and replaced by
  `FAILED_METADATA_CHANGE_EVENT_NAME`.

### MAE Consumer (datahub-mae-consumer) 

- (Deprecated) `KAFKA_TOPIC_NAME`: The name of the deprecated metadata audit event topic. This will soon be removed
and replaced by `METADATA_AUDIT_EVENT_NAME`. 
  
### DataHub Frontend (datahub-frontend-react)

- `DATAHUB_TRACKING_TOPIC`: The name of the topic used for storing DataHub usage events.
It should contain the same value as `DATAHUB_USAGE_EVENT_NAME` in the Metadata Service container. 

Please ensure that these environment variables are set consistently throughout your ecosystem. DataHub has a few different applications running which communicate with Kafka (see above).

## Configuring Consumer Group Id

Kafka Consumers in Spring are configured using Kafka listeners. By default, consumer group id is same as listener id.

We've included an environment variable to customize the consumer group id, if your company or organization has specific naming rules.

### datahub-mce-consumer and datahub-mae-consumer

- `KAFKA_CONSUMER_GROUP_ID`: The name of the kafka consumer's group id.

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
      - name: METADATA_CHANGE_EVENT_NAME
        value: "MetadataChangeEvent"
      - name: METADATA_AUDIT_EVENT_NAME
        value: "MetadataAuditEvent"
      - name: FAILED_METADATA_CHANGE_EVENT_NAME
        value: "FailedMetadataChangeEvent"
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
        - name: KAFKA_TOPIC_NAME
          value: "MetadataAuditEvent"
datahub-mce-consumer; 
    extraEnvs:
        - name: KAFKA_MCE_TOPIC_NAME
          value: "MetadataChangeEvent"
        ....
```

## Other Components that use Kafka can be configured using environment variables:
- kafka-setup
- schema-registry

##  SASL/GSSAPI properties for kafka-setup and datahub-frontend via environment variables
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

>Other errors: `Failed to start bean 'org.springframework.kafka.config.internalKafkaListenerEndpointRegistry'; nested exception is org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [DataHubUsageEvent_v1]`. Please check ranger permissions or kafka broker logs.
