# Hive Lineage Integration for DataHub

This directory contains the Hive Lineage integration components for DataHub, organized into two main modules:

1. **hive-lineage-producer** - Hive hook that captures lineage information from Hive queries
2. **hive-lineage-consumer** - Spring Boot application that consumes lineage messages and emits MCPs to DataHub

## Directory Structure

```
acryl-hive-lineage/
├── hive-lineage-producer/     # Producer module (Hive hook)
│   ├── src/
│   │   ├── main/
│   │   │   └── java/
│   │   │       └── datahub/hive/producer/
│   │   │           ├── HiveLineageLogger.java
│   │   │           ├── KafkaProducerService.java
│   │   │           └── TimeUtils.java
│   │   └── test/
│   │       └── java/
│   │           └── datahub/hive/producer/
│   │               ├── HiveLineageLoggerTest.java
│   │               ├── KafkaProducerServiceTest.java
│   │               └── TimeUtilsTest.java
│   ├── build.gradle
│   └── README.md
│
├── hive-lineage-consumer/     # Consumer module (Spring Boot app)
│   ├── src/
│   │   ├── main/
│   │   │   └── java/
│   │   │       └── datahub/hive/consumer/
│   │   │           ├── LineageConsumerMCPEmitterApplication.java
│   │   │           ├── config/
│   │   │           │   ├── Constants.java
│   │   │           │   └── KafkaConfig.java
│   │   │           ├── model/
│   │   │           │   └── HiveLineage.java
│   │   │           ├── service/
│   │   │           │   ├── LineageConsumerMCPEmitterService.java
│   │   │           │   ├── MCPTransformerService.java
│   │   │           │   └── impl/
│   │   │           │       ├── DatasetMCPTransformerServiceImpl.java
│   │   │           │       └── QueryMCPTransformerServiceImpl.java
│   │   │           └── util/
│   │   │               ├── MCPEmitterUtil.java
│   │   │               └── TimeUtils.java
│   │   └── test/
│   │       └── java/
│   │           └── datahub/hive/consumer/
│   │               ├── config/
│   │               │   ├── KafkaConfigTest.java
│   │               │   ├── KafkaConfigIntegrationTest.java
│   │               │   └── KafkaConfigSSLTest.java
│   │               ├── model/
│   │               │   └── HiveLineageTest.java
│   │               ├── service/
│   │               │   ├── LineageConsumerMCPEmitterServiceTest.java
│   │               │   └── impl/
│   │               │       ├── DatasetMCPTransformerServiceImplTest.java
│   │               │       └── QueryMCPTransformerServiceImplTest.java
│   │               └── util/
│   │                   ├── MCPEmitterUtilTest.java
│   │                   └── TimeUtilsTest.java
│   ├── build.gradle
│   └── README.md
│
└── README.md (this file)
```

## Overview

### Producer (Hive Hook)

The producer is a Hive post-execution hook that:
- Captures lineage information from Hive CTAS (CREATE TABLE AS SELECT) queries
- Generates JSON lineage messages with source/target relationships
- Publishes messages to a Kafka topic for consumption

### Consumer (Spring Boot Application)

The consumer is a Spring Boot application that:
- Listens to the Hive lineage Kafka topic
- Transforms lineage messages into DataHub Metadata Change Proposals (MCPs)
- Emits MCPs to DataHub via Kafka for ingestion

## Building the JARs

### Prerequisites

- Java 17 or higher
- Gradle 8.7 or higher

### Building the Producer JAR

The producer builds a shadow JAR by default that includes only Kafka client dependencies (other Hive/Hadoop dependencies are provided by the runtime environment), can change the build.gradle to include other dependencies in shadow JAR if required.

```bash
cd hive-lineage-producer
./gradlew clean build

# The JAR will be created at:
# build/libs/hive-lineage-producer.jar
```

**Output:** `hive-lineage-producer.jar` (~25-30 KB)

### Building the Consumer JAR

The consumer builds a Spring Boot executable JAR with all dependencies included.

```bash
cd hive-lineage-consumer
./gradlew clean build

# The JAR will be created at:
# build/libs/hive-lineage-consumer.jar
```

**Output:** `hive-lineage-consumer.jar` (~100-120 MB)

### Building Both JARs

From the `acryl-hive-lineage` directory:

```bash
# Build producer
(cd hive-lineage-producer && ./gradlew clean build)

# Build consumer
(cd hive-lineage-consumer && ./gradlew clean build)
```

## Configuration

### Producer Configuration

The producer reads configuration from an XML file (default: `/etc/hive/conf/hive-lineage-config.xml`).

**Configuration Properties:**

```xml
<?xml version="1.0"?>
<configuration>
    <!-- Platform Configuration -->
    <property>
        <name>hive.lineage.environment</name>
        <value>DEV</value>
        <description>Environment name (e.g., DEV, QA, PROD)</description>
    </property>
    
    <property>
        <name>hive.lineage.platform.instance</name>
        <value>your-hive-cluster-name</value>
        <description>Platform instance identifier</description>
    </property>
    
    <!-- Kafka Configuration -->
    <property>
        <name>hive.lineage.kafka.bootstrap.servers</name>
        <value>kafka-broker1:9093,kafka-broker2:9093</value>
        <description>Kafka bootstrap servers</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.topic</name>
        <value>HiveLineage_v1</value>
        <description>Kafka topic for lineage messages</description>
    </property>

    <property>
        <name>hive.lineage.kafka.retries</name>
        <value>0</value>
        <description>Number of retries for failed Kafka sends</description>
    </property>

    <property>
        <name>hive.lineage.kafka.retry.backoff.ms</name>
        <value>100</value>
        <description>Backoff time in milliseconds between retry attempts</description>
    </property>

    <property>
        <name>hive.lineage.kafka.enable.idempotence</name>
        <value>false</value>
        <description>Enable idempotent producer to avoid duplicate messages</description>
    </property>

    <property>
        <name>hive.lineage.kafka.max.block.ms</name>
        <value>3000</value>
        <description>Maximum time in milliseconds to block waiting for Kafka metadata</description>
    </property>

    <property>
        <name>hive.lineage.kafka.request.timeout.ms</name>
        <value>3000</value>
        <description>Maximum time in milliseconds to wait for a Kafka request response</description>
    </property>

    <property>
        <name>hive.lineage.kafka.delivery.timeout.ms</name>
        <value>5000</value>
        <description>Maximum time in milliseconds for message delivery including retries</description>
    </property>

    <property>
        <name>hive.lineage.vdc.kafka.close.timeout.ms</name>
        <value>120000</value>
        <description>Timeout in milliseconds for closing Kafka producer gracefully</description>
    </property>
    
    <!-- SSL Configuration -->
    <property>
        <name>hive.lineage.kafka.ssl.truststore.location</name>
        <description>/path/to/truststore.jks</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.truststore.password</name>
        <description>truststore-password</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.keystore.location</name>
        <description>/path/to/keystore.jks</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.keystore.password</name>
        <description>keystore-password</description>
    </property>
    
    <!-- Thread Pool Configuration -->
    <property>
        <name>hive.lineage.thread.max.pool.size</name>
        <value>100</value>
        <description>Maximum thread pool size multiplier</description>
    </property>
    
    <property>
        <name>hive.lineage.thread.queue.capacity</name>
        <value>500</value>
        <description>Thread pool queue capacity</description>
    </property>

    <property>
        <name>hive.lineage.thread.name</name>
        <value>HiveLineageComputationThread-</value>
        <description>Thread name prefix for lineage computation threads</description>
    </property>

    <property>
        <name>hive.lineage.thread.keep.alive.time</name>
        <value>60</value>
        <description>Keep-alive time in seconds for idle threads in the pool</description>
    </property>

    <property>
        <name>hive.lineage.executor.timeout.seconds</name>
        <value>30</value>
        <description>Timeout in seconds for executor service shutdown</description>
    </property>
</configuration>
```

**Hive Configuration:**

Add to `hive-site.xml`:

```xml
<!-- Enable lineage hook -->
<property>
    <name>hive.exec.post.hooks</name>
    <value>datahub.hive.producer.HiveLineageLogger</value>
</property>

<!-- Enable lineage information collection -->
<property>
    <name>hive.lineage.hook.info.enabled</name>
    <value>true</value>
</property>

<!-- Optional: Custom config path -->
<property>
    <name>hive.lineage.custom.config.path</name>
    <value>/custom/path/to/hive-lineage-config.xml</value>
</property>
```

### Consumer Configuration

The consumer uses Spring Boot application properties (typically `application.yml` or `application.properties`).

**application.yml Example:**

```yaml
spring:
  kafka:
    consumer:
      bootstrap-servers: kafka-broker1:9093,kafka-broker2:9093
      group-id: hive-lineage-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      concurrency: 7
      retry:
        max-attempts: 3
        initial-delay: 1000
      hive:
        lineage:
          topic: HiveLineage
    
    producer:
      bootstrap-servers: kafka-broker1:9093,kafka-broker2:9093
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://schema-registry:8081
        mcp.topic: MetadataChangeProposal_v1
    
    security:
      protocol: SSL
    
    ssl:
      trust-store-location: file:/path/to/truststore.jks
      trust-store-password: truststore-password
      key-store-location: file:/path/to/keystore.jks
      key-store-password: keystore-password
```

## Deployment

### Producer Deployment

1. **Copy the JAR to Hive auxiliary path:**
   ```bash
   cp hive-lineage-producer.jar /path/to/hive/auxlib/
   ```

2. **Configure HIVE_AUX_JARS_PATH in hive-env:**
   
   Add the following to your `hive-env.sh` template under Advanced hive-env configuration:
   
   ```bash
   # Adding hive lineage logger hook jar to Hive aux jars path if present
   if [ -f "/path/to/hive/auxlib/hive-lineage-producer.jar" ]; then
     export HIVE_AUX_JARS_PATH=${HIVE_AUX_JARS_PATH}:/path/to/hive/auxlib/hive-lineage-producer.jar
   fi
   ```
   
   This ensures the lineage JAR is available in Hive's classpath.

3. **Configure Hive** (see Configuration section above)

4. **Restart Hive services** (HiveServer2, Beeline, etc.)

### Consumer Deployment

1. **Run as a standalone application:**
   ```bash
   java -jar hive-lineage-consumer.jar \
     --spring.config.location=/path/to/application.yml
   ```

## Testing

### Producer Testing

Run unit tests:

```bash
cd hive-lineage-producer
./gradlew test
```

Check test coverage:

```bash
./gradlew jacocoTestReport
# Report available at: build/reports/tests/test/index.html
```

The producer automatically logs lineage for CTAS queries. Test with:

```sql
-- Enable lineage
SET hive.lineage.hook.info.enabled=true;

-- Run a CTAS query
CREATE TABLE test_output AS 
SELECT * FROM test_input WHERE id > 100;

```

Check Hive logs for lineage messages and verify messages appear in Kafka topic.

### Consumer Testing

Run unit tests:

```bash
cd hive-lineage-consumer
./gradlew test
```

Check test coverage:

```bash
./gradlew jacocoTestReport
# Report available at: build/reports/jacoco/test/html/index.html
```

## Monitoring

### Producer Monitoring

- Check Hive logs for lineage computation times
- Monitor Kafka topic for incoming messages
- Verify thread pool metrics in logs

### Consumer Monitoring

- Monitor Spring Boot actuator endpoints (if enabled)
- Check consumer lag using Kafka tools
- Review application logs for processing errors

## Troubleshooting

### Producer Issues

1. **Hook not executing:**
   - Verify JAR is in Hive classpath
   - Check `hive.exec.post.hooks` configuration
   - Ensure `hive.lineage.hook.info.enabled=true`

2. **Kafka connection errors:**
   - Verify bootstrap servers configuration
   - Check SSL certificates and passwords
   - Ensure network connectivity to Kafka

3. **Thread pool exhaustion:**
   - Increase `hive.lineage.thread.max.pool.size`
   - Increase `hive.lineage.thread.queue.capacity`

### Consumer Issues

1. **Consumer not receiving messages:**
   - Verify Kafka topic name matches producer
   - Check consumer group offset
   - Verify SSL configuration

2. **MCP emission failures:**
   - Check schema registry connectivity
   - Verify MCP topic exists
   - Review DataHub logs for ingestion errors
