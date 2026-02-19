# Hive Lineage Producer

A Hive post-execution hook that captures lineage information from Hive queries and publishes it to Kafka for consumption by DataHub.

## Overview

The Hive Lineage Producer is a lightweight hook that integrates with Apache Hive to:
- Capture lineage information from Hive CTAS (CREATE TABLE AS SELECT) queries
- Extract column-level lineage (fine-grained lineage) from query execution plans
- Generate JSON lineage messages with source/target relationships
- Publish messages asynchronously to a Kafka topic

## Architecture

```
Hive Query Execution
        ↓
HiveLineageLogger (Post-Execution Hook)
        ↓
Extract Lineage Info
        ↓
Build Lineage JSON
        ↓
KafkaProducerService
        ↓
Kafka Topic (HiveLineage_v1)
```

## Directory Structure

```
hive-lineage-producer/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── datahub/hive/producer/
│   │           ├── HiveLineageLogger.java      # Main hook class
│   │           ├── KafkaProducerService.java   # Kafka producer wrapper
│   │           └── TimeUtils.java              # Utility for time calculations
│   └── test/
│       └── java/
│           └── datahub/hive/producer/
│               ├── HiveLineageLoggerTest.java
│               ├── KafkaProducerServiceTest.java
│               └── TimeUtilsTest.java
├── build.gradle
└── README.md (this file)
```

## Components

### 1. HiveLineageLogger

The main hook class that:
- Implements `ExecuteWithHookContext` interface
- Runs after query execution completes
- Extracts lineage information
- Builds comprehensive lineage JSON including:
  - Table-level lineage (inputs → outputs)
  - Column-level lineage (fine-grained lineage)
  - Query metadata (user, timestamp, duration, etc.)
- Submits lineage to Kafka asynchronously using thread pool

### 2. KafkaProducerService

Manages Kafka producer lifecycle:
- Creates and configures Kafka producer with SSL/TLS support
- Sends messages with configurable timeouts and retries
- Handles connection errors gracefully
- Provides proper cleanup on shutdown

### 3. TimeUtils

Utility class for time-related operations:
- Calculates duration between timestamps
- Used for performance monitoring

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

# Build shadow JAR
./gradlew shadowJar

# The JAR will be created at:
# build/libs/hive-lineage-producer.jar
```

**Output:** `hive-lineage-producer.jar` (~25-30 KB with only Kafka client dependencies)

## Configuration

### Configuration File

The producer reads configuration from an XML file (default: `/etc/hive/conf/hive-lineage-config.xml`).

**Example Configuration:**

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

    <!-- Kafka Producer Settings -->
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
        <name>hive.lineage.kafka.close.timeout.ms</name>
        <value>120000</value>
        <description>Timeout in milliseconds for closing Kafka producer gracefully</description>
    </property>
    
    <!-- SSL Configuration -->
    <property>
        <name>hive.lineage.kafka.ssl.truststore.location</name>
        <value>/path/to/truststore.jks</value>
        <description>Path to SSL truststore file</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.truststore.password</name>
        <value>truststore-password</value>
        <description>Truststore password</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.keystore.location</name>
        <value>/path/to/keystore.jks</value>
        <description>Path to SSL keystore file</description>
    </property>
    
    <property>
        <name>hive.lineage.kafka.ssl.keystore.password</name>
        <value>keystore-password</value>
        <description>Keystore password</description>
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

### Hive Configuration

Add the following to `hive-site.xml`:

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

## Deployment

### Step 1: Copy JAR to Hive

```bash
# Copy to Hive auxiliary library path
cp hive-lineage-producer.jar /path/to/hive/auxlib/
```

### Step 2: Configure HIVE_AUX_JARS_PATH

Add the following to your `hive-env.sh` template under Advanced hive-env configuration:

```bash
# Adding hive lineage logger hook jar to Hive aux jars path if present
if [ -f "/path/to/hive/auxlib/hive-lineage-producer.jar" ]; then
  export HIVE_AUX_JARS_PATH=${HIVE_AUX_JARS_PATH}:/path/to/hive/auxlib/hive-lineage-producer.jar
fi
```

This ensures the lineage JAR is available in Hive's classpath.

### Step 3: Configure Hive

1. Create `/etc/hive/conf/hive-lineage-config.xml` with your configuration
2. Update `hive-site.xml` with hook configuration (see Configuration section)
3. Verify Kafka connectivity and SSL certificates

### Step 4: Restart Hive Services

```bash
# Restart HiveServer2
sudo systemctl restart hive-server2

# Or restart via Ambari/Cloudera Manager
```

## Testing

### Running Unit Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests TimeUtilsTest
./gradlew test --tests KafkaProducerServiceTest
./gradlew test --tests HiveLineageLoggerTest

# Run with detailed output
./gradlew test --info

# Generate test coverage report
./gradlew jacocoTestReport
# Report available at: build/reports/tests/test/index.html
```

### Manual Testing

Test the hook with a CTAS query:

```sql
-- Enable lineage
SET hive.lineage.hook.info.enabled=true;

-- Run a CTAS query
CREATE TABLE test_output AS 
SELECT col1, col2, col3
FROM test_input 
WHERE col1 > 100;
```

### Verification

1. **Check Hive Logs:**
   ```bash
   tail -f /var/log/hive/hiveserver2.log | grep "HiveLineageLogger"
   ```
   
   Look for messages like:
   - "Lineage computation completed in X ms"
   - "Successfully sent lineage message to Kafka"


2. **Verify Kafka Messages:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic HiveLineage_v1 --from-beginning
   ```

## Lineage Message Format

The producer generates JSON messages with the following structure:

```json
{
  "version": "1.0",
  "user": "hive_user",
  "timestamp": 1700000000,
  "duration": 5432,
  "jobIds": ["job_123456"],
  "engine": "hive",
  "database": "default",
  "hash": "abc123def456",
  "queryText": "CREATE TABLE output AS SELECT * FROM input",
  "environment": "DEV",
  "platformInstance": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,cluster-name)",
  "inputs": ["default.input_table"],
  "outputs": ["default.output_table"],
  "edges": [
    {
      "sources": [0],
      "targets": [1],
      "edgeType": "PROJECTION",
      "expression": "col1"
    }
  ],
  "vertices": [
    {
      "id": 0,
      "vertexType": "COLUMN",
      "vertexId": "input_table.col1"
    },
    {
      "id": 1,
      "vertexType": "COLUMN",
      "vertexId": "output_table.col1"
    }
  ]
}
```

## Monitoring

### Key Metrics

Monitor the following in Hive logs:

- **Lineage computation time:** Time taken to extract and build lineage
- **Kafka send success/failure:** Success rate of Kafka message delivery
- **Thread pool utilization:** Active threads and queue size
- **Error rates:** Exceptions during lineage extraction or Kafka send

## Troubleshooting

### Hook Not Executing

**Symptoms:** No lineage messages generated

**Solutions:**
1. Verify JAR is in Hive classpath:
   ```bash
   ls -l /path/to/hive/auxlib/hive-lineage-producer.jar
   ```
2. Check `hive.exec.post.hooks` in hive-site.xml
3. Ensure `hive.lineage.hook.info.enabled=true`
4. Review HiveServer2 logs for initialization errors

### Kafka Connection Errors

**Symptoms:** "Failed to send lineage message" errors

**Solutions:**
1. Verify Kafka bootstrap servers are reachable:
   ```bash
   telnet kafka-broker 9093
   ```
2. Check SSL certificates and passwords
3. Verify network connectivity and firewall rules
4. Test Kafka producer manually

### Thread Pool Exhaustion

**Symptoms:** "Thread pool queue is full" warnings

**Solutions:**
1. Increase `hive.lineage.thread.max.pool.size`
2. Increase `hive.lineage.thread.queue.capacity`

### Missing Lineage Information

**Symptoms:** Incomplete lineage in messages

**Solutions:**
1. Ensure query is a supported CTAS operation
2. Check that source tables exist and are accessible
3. Verify `hive.lineage.hook.info.enabled=true`
4. Review query for unsupported operations

## Performance Considerations

### Thread Pool Sizing

- Default max pool size: 100 threads
- Queue capacity: 500 tasks
- Adjust based on:
  - Query concurrency
  - Lineage computation complexity
  - Available system resources

### Kafka Timeouts

- Keep timeouts low to avoid blocking query execution
- Default: 3-5 seconds
- Failed sends are logged but don't fail the query

### Memory Usage

- Hook has minimal memory footprint
- Thread pool uses bounded queue to prevent memory issues
- Each lineage message is typically < 10KB

## Dependencies

### Runtime Dependencies

- Apache Hive (provided by Hive environment)
- Apache Hadoop (provided by Hadoop environment)
- Kafka Client (included in shadow JAR)

### Test Dependencies

- JUnit 5 (Jupiter)
- Mockito
- Kafka Test Utils

