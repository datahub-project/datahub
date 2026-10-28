# Hive Lineage Consumer

A Spring Boot application that consumes Hive lineage messages from Kafka and transforms them into DataHub Metadata Change Proposals (MCPs) for ingestion into DataHub.

## Overview

The Hive Lineage Consumer is a microservice that:
- Listens to Kafka topic for Hive lineage messages
- Transforms lineage JSON into DataHub MCPs
- Emits dataset and query entities with lineage aspects to DataHub
- Supports both table-level and column-level (fine-grained) lineage
- Provides retry mechanisms and error handling

## Architecture

```
Kafka Topic (HiveLineage_v1)
        ↓
Kafka Consumer (Spring Kafka)
        ↓
LineageConsumerMCPEmitterService
        ↓
   ┌────────────────┴────────────────┐
   ↓                                  ↓
DatasetMCPTransformer          QueryMCPTransformer
   ↓                                  ↓
Dataset MCPs                      Query MCPs
   └────────────────┬────────────────┘
                    ↓
            MCPEmitterUtil
                    ↓
        Kafka Producer (Avro)
                    ↓
   Kafka Topic (MetadataChangeProposal_v1)
                    ↓
              DataHub GMS
```

## Directory Structure

```
hive-lineage-consumer/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── datahub/hive/consumer/
│   │   │       ├── LineageConsumerMCPEmitterApplication.java  # Main Spring Boot app
│   │   │       ├── config/
│   │   │       │   ├── Constants.java                         # Application constants
│   │   │       │   └── KafkaConfig.java                       # Kafka configuration
│   │   │       ├── model/
│   │   │       │   └── HiveLineage.java                       # Lineage data model
│   │   │       ├── service/
│   │   │       │   ├── LineageConsumerMCPEmitterService.java  # Main consumer service
│   │   │       │   ├── MCPTransformerService.java             # Transformer interface
│   │   │       │   └── impl/
│   │   │       │       ├── DatasetMCPTransformerServiceImpl.java  # Dataset transformer
│   │   │       │       └── QueryMCPTransformerServiceImpl.java    # Query transformer
│   │   │       └── util/
│   │   │           ├── MCPEmitterUtil.java                    # MCP emission utility
│   │   │           └── TimeUtils.java                         # Time utilities
│   │   └── resources/
│   │       └── application.properties                         # Application configuration
│   └── test/
│       └── java/
│           └── datahub/hive/consumer/
│               ├── config/
│               │   ├── KafkaConfigTest.java
│               │   ├── KafkaConfigIntegrationTest.java
│               │   └── KafkaConfigSSLTest.java
│               ├── model/
│               │   └── HiveLineageTest.java
│               ├── service/
│               │   ├── LineageConsumerMCPEmitterServiceTest.java
│               │   └── impl/
│               │       ├── DatasetMCPTransformerServiceImplTest.java
│               │       └── QueryMCPTransformerServiceImplTest.java
│               └── util/
│                   ├── MCPEmitterUtilTest.java
│                   └── TimeUtilsTest.java
├── build.gradle
└── README.md (this file)
```

## Components

### 1. LineageConsumerMCPEmitterService

The main Kafka consumer service that:
- Listens to Hive lineage Kafka topic using `@KafkaListener`
- Parses incoming JSON lineage messages
- Delegates transformation to appropriate transformer services
- Handles errors and retries
- Logs processing metrics

### 2. DatasetMCPTransformerServiceImpl

Transforms lineage into dataset MCPs:
- Creates `UpstreamLineage` aspect with table-level lineage
- Creates `FineGrainedLineage` for column-level lineage
- Generates `DataPlatformInstance` aspect
- Adds `Status` and `SubTypes` aspects
- Links to query entities via query URNs

### 3. QueryMCPTransformerServiceImpl

Transforms lineage into query MCPs:
- Creates `QueryProperties` aspect with SQL statement
- Creates `QuerySubjects` aspect linking to output datasets
- Captures query metadata (user, timestamps, etc.)
- Sets query source as MANUAL

### 4. MCPEmitterUtil

Utility for emitting MCPs to DataHub:
- Creates `MetadataChangeProposalWrapper` objects
- Sends MCPs to Kafka using Avro serialization
- Provides callback-based async emission
- Handles errors and logs results

### 5. KafkaConfig

Spring configuration for Kafka:
- Configures consumer factory with retry logic
- Sets up SSL/TLS for secure connections
- Configures producer for MCP emission
- Manages Kafka emitter bean lifecycle

## Building

### Prerequisites

- Java 17 or higher
- Gradle 8.7 or higher

### Build Commands

```bash
# Clean and build
./gradlew clean build

# Build without tests
./gradlew clean build -x test

# Build Spring Boot JAR
./gradlew bootJar

# The JAR will be created at:
# build/libs/hive-lineage-consumer.jar
```

**Output:** `hive-lineage-consumer.jar` (~100-120 MB with all dependencies)

## Deployment

### Run as Standalone Application

```bash
# Run with default configuration
java -jar hive-lineage-consumer.jar

# Run with custom configuration
java -jar hive-lineage-consumer.jar \
  --spring.config.location=/path/to/application.yml

# Run with environment-specific profile
java -jar hive-lineage-consumer.jar \
  --spring.profiles.active=production
```

## Testing

### Running Unit Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests LineageConsumerMCPEmitterServiceTest
./gradlew test --tests DatasetMCPTransformerServiceImplTest
./gradlew test --tests QueryMCPTransformerServiceImplTest

# Run with detailed output
./gradlew test --info

# Generate test coverage report
./gradlew jacocoTestReport
# Report available at: build/reports/jacoco/test/html/index.html
```

### Verification

1. **Check Consumer Logs:**
   
   The application logs to multiple locations based on logback.xml configuration:
   
   ```bash
   # Main application log (default location)
   tail -f /datahub/hive-lineage-consumer/logs/hive-lineage-consumer.log
   
   # Error log (ERROR level only)
   tail -f /datahub/hive-lineage-consumer/logs/hive-lineage-consumer.error.log
   
   # Custom log directory (set via LOG_DIR environment variable)
   export LOG_DIR=/custom/log/path
   tail -f $LOG_DIR/hive-lineage-consumer.log
   ```
   
   **Log Configuration:**
   - Default log directory: `/datahub/hive-lineage-consumer/logs/`
   - Log rotation: Daily, max 100MB per file
   - Retention: 30 days, max 10GB total
   - Log pattern: `%date{ISO8601} [%thread] %-5level %logger{36}:%L - %msg%n`


2. **Verify MCP Emission:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic MetadataChangeProposal_v1 --from-beginning
   ```

3. **Check DataHub UI:**
   - Navigate to dataset: `urn:li:dataset:(urn:li:dataPlatform:hive,<platformInstance>.default.output_table,DEV)`
   - Verify lineage tab shows upstream relationships
   - Check column-level lineage if available

## Monitoring

### Key Metrics

Monitor the following metrics:

- **Consumer Lag:** Kafka consumer group lag
- **Processing Time:** Time to process each lineage message
- **MCP Emission Success Rate:** Percentage of successful MCP emissions
- **Error Rate:** Number of processing errors
- **Throughput:** Messages processed per second

## Troubleshooting

### Consumer Not Receiving Messages

**Symptoms:** No messages being consumed from Kafka

**Solutions:**
1. Verify Kafka topic exists and has messages:
   ```bash
   kafka-topics --bootstrap-server localhost:9092 --describe --topic HiveLineage_v1
   ```
2. Check consumer group offset:
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group hive-lineage-consumer-group
   ```
3. Verify SSL configuration and certificates
4. Check network connectivity to Kafka brokers

### MCP Emission Failures

**Symptoms:** "Failed to emit MCP" errors in logs

**Solutions:**
1. Verify schema registry is accessible:
   ```bash
   curl http://schema-registry:8081/subjects
   ```
2. Check MCP topic exists:
   ```bash
   kafka-topics --bootstrap-server localhost:9092 --describe --topic MetadataChangeProposal_v1
   ```
3. Verify Avro schema compatibility
4. Check DataHub GMS logs for ingestion errors

### High Consumer Lag

**Symptoms:** Consumer lag increasing continuously

**Solutions:**
1. Increase consumer concurrency in configuration
2. Scale horizontally by adding more consumer instances
3. Check for slow network or Kafka issues

### Memory Issues

**Symptoms:** OutOfMemoryError or high heap usage

**Solutions:**
1. Increase JVM heap size:
   ```bash
   java -Xmx4g -Xms2g -jar hive-lineage-consumer.jar
   ```
2. Reduce consumer concurrency
3. Monitor for memory leaks
4. Enable GC logging for analysis

## Dependencies

### Runtime Dependencies

- Spring Boot 3.3.4
- Spring Kafka
- DataHub Client 1.1.0
- Kafka Avro Serializer
- Gson for JSON processing

### Test Dependencies

- JUnit 5 (Jupiter)
- Mockito
- Spring Boot Test
- Spring Kafka Test

## Data Model

### Input: Hive Lineage Message

See producer README for complete message format.

### Output: DataHub MCPs

**Dataset Entity MCPs:**
- UpstreamLineage aspect
- FineGrainedLineage aspect (if column lineage available)
- DataPlatformInstance aspect
- Status aspect
- SubTypes aspect

**Query Entity MCPs:**
- QueryProperties aspect
- QuerySubjects aspect
