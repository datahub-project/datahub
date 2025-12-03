# DataHubClientV2 Configuration

The `DataHubClientV2` is the primary entry point for interacting with DataHub using SDK V2. This guide covers client configuration, connection management, and operation modes.

## Creating a Client

### Basic Configuration

The minimal configuration requires only a server URL:

```java
import datahub.client.v2.DataHubClientV2;

DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .build();
```

### With Authentication

For DataHub Cloud or secured instances, provide a personal access token:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("https://your-instance.acryl.io")
    .token("your-personal-access-token")
    .build();
```

> **Getting a Token:** In DataHub UI → Settings → Access Tokens → Generate Personal Access Token

### From Environment Variables

Configure the client using environment variables:

```bash
export DATAHUB_SERVER=http://localhost:8080
export DATAHUB_TOKEN=your-token-here
```

```java
DataHubClientConfig V2 config = DataHubClientConfigV2.fromEnv();
DataHubClientV2 client = new DataHubClientV2(config);
```

**Supported environment variables:**

- `DATAHUB_SERVER` or `DATAHUB_GMS_URL` - Server URL (required)
- `DATAHUB_TOKEN` or `DATAHUB_GMS_TOKEN` - Authentication token (optional)

## Configuration Options

### Timeouts

Configure request timeouts to handle slow networks:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .timeoutMs(30000)  // 30 seconds
    .build();
```

**Default:** 10 seconds (10000ms)

### Retries

Configure automatic retries for failed requests:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .maxRetries(5)  // Retry up to 5 times
    .build();
```

**Default:** 3 retries

### SSL Certificate Verification

For testing environments, you can disable SSL verification:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("https://localhost:8443")
    .disableSslVerification(true)  // WARNING: Only for testing!
    .build();
```

> **Warning:** Never disable SSL verification in production! This makes your connection vulnerable to man-in-the-middle attacks.

## Operation Modes

SDK V2 supports two distinct operation modes that control how metadata is written to DataHub:

### SDK Mode (Default)

**Use for:** Interactive applications, user-initiated metadata edits, real-time UI updates

**Behavior:**

- Writes to **editable aspects** (e.g., `editableDatasetProperties`)
- Uses **synchronous DB writes** for immediate consistency
- Returns only after metadata is committed to database

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)  // Default
    .build();

Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.setDescription("User-provided description");
client.entities().upsert(dataset);
// Writes to editableDatasetProperties synchronously
// Metadata immediately visible after return
```

### INGESTION Mode

**Use for:** ETL pipelines, data ingestion jobs, automated metadata collection, batch processing

**Behavior:**

- Writes to **system aspects** (e.g., `datasetProperties`)
- Uses **asynchronous Kafka writes** for high throughput
- Returns immediately after message is queued

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.INGESTION)
    .build();

Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.setDescription("Ingested from Snowflake");
client.entities().upsert(dataset);
// Writes to datasetProperties asynchronously via Kafka
// High throughput for batch ingestion
```

### Mode Comparison

| Aspect              | SDK Mode                    | INGESTION Mode                   |
| ------------------- | --------------------------- | -------------------------------- |
| **Target Aspects**  | Editable aspects            | System aspects                   |
| **Write Path**      | Synchronous (direct to DB)  | Asynchronous (via Kafka)         |
| **Consistency**     | Immediate (linearizable)    | Eventual (async processing)      |
| **Throughput**      | Lower (waits for DB)        | Higher (queued)                  |
| **Use Case**        | User edits via UI/API       | Pipeline metadata extraction     |
| **Precedence**      | Higher (overrides system)   | Lower (overridden by user edits) |
| **Example Aspects** | `editableDatasetProperties` | `datasetProperties`              |
| **Latency**         | ~100-500ms                  | ~10-50ms (queueing only)         |
| **Error Handling**  | Immediate feedback          | Eventual (check logs)            |

**Why two modes?**

- **Clear provenance**: Distinguish human edits from machine-generated metadata
- **Non-destructive updates**: Ingestion can refresh without clobbering user documentation
- **UI consistency**: DataHub UI shows editable aspects as user overrides
- **Performance optimization**: Async ingestion for high-volume batch writes, sync for interactive edits

## Async Mode Control (The Escape Hatch)

By default, the async mode is automatically inferred from your operation mode:

- SDK mode → synchronous writes (immediate consistency)
- INGESTION mode → asynchronous writes (high throughput)

However, you can explicitly override this behavior using the `asyncIngest` parameter when you need full control:

### Force Synchronous in INGESTION Mode

For pipelines that need immediate consistency guarantees:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.INGESTION)
    .asyncIngest(false)  // Override: force synchronous despite INGESTION mode
    .build();

Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.setDescription("Ingested description");
client.entities().upsert(dataset);
// Writes to datasetProperties synchronously, waits for DB commit
// Use when you need guaranteed consistency before proceeding
```

**Use cases:**

- Critical ingestion jobs where you must verify writes succeeded
- Sequential processing where each step depends on previous writes
- Testing scenarios requiring deterministic behavior
- Compliance workflows requiring audit trail confirmation

### Force Asynchronous in SDK Mode

For high-volume SDK operations that can tolerate eventual consistency:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)
    .asyncIngest(true)  // Override: force async despite SDK mode
    .build();

Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.setDescription("User-provided description");
client.entities().upsert(dataset);
// Writes to editableDatasetProperties via Kafka for higher throughput
// Trade immediate consistency for performance
```

**Use cases:**

- Bulk metadata updates from admin tools
- Migration scripts moving large volumes of data
- Performance-critical batch operations
- Load testing and benchmarking

### Decision Guide

| Scenario                | Operation Mode | asyncIngest | Result                           |
| ----------------------- | -------------- | ----------- | -------------------------------- |
| User edits in web UI    | SDK            | (default)   | Sync writes to editable aspects  |
| ETL pipeline ingestion  | INGESTION      | (default)   | Async writes to system aspects   |
| Critical data migration | INGESTION      | false       | Sync writes to system aspects    |
| Bulk admin updates      | SDK            | true        | Async writes to editable aspects |

**Default behavior is best for 95% of use cases.** Only use explicit `asyncIngest` when you have specific performance or consistency requirements.

## Testing the Connection

Verify connectivity before performing operations:

```java
try {
    boolean connected = client.testConnection();
    if (connected) {
        System.out.println("Connected to DataHub!");
    } else {
        System.err.println("Failed to connect");
    }
} catch (Exception e) {
    System.err.println("Connection error: " + e.getMessage());
}
```

The `testConnection()` method performs a GET request to `/config` endpoint to verify the server is reachable.

## Client Lifecycle

### Resource Management

The client implements `AutoCloseable` for automatic resource management:

```java
try (DataHubClientV2 client = DataHubClientV2.builder()
        .server("http://localhost:8080")
        .build()) {

    // Use client
    client.entities().upsert(dataset);

} // Client automatically closed
```

### Manual Closing

If not using try-with-resources, explicitly close the client:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .build();

try {
    // Use client
} finally {
    client.close();  // Release HTTP connections
}
```

**Why close?** Closing the client releases the underlying HTTP connection pool.

## Advanced Configuration

### Complete Configuration Example

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.config.DataHubClientConfigV2;

DataHubClientV2 client = DataHubClientV2.builder()
    // Server configuration
    .server("https://your-instance.acryl.io")
    .token("your-personal-access-token")

    // Timeout configuration
    .timeoutMs(30000)  // 30 seconds

    // Retry configuration
    .maxRetries(5)

    // Operation mode (SDK or INGESTION)
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)

    // Async mode control (optional - overrides mode-based default)
    // .asyncIngest(false)  // Explicit control: true=async, false=sync

    // SSL configuration (testing only!)
    .disableSslVerification(false)

    .build();
```

### Accessing the Underlying RestEmitter

For advanced use cases, access the low-level REST emitter:

```java
RestEmitter emitter = client.getEmitter();
// Direct access to emission methods
```

> **Note:** Most users should use the high-level `client.entities()` API instead.

## Entity Operations

Once configured, use the client to perform entity operations:

### CRUD Operations

```java
// Create/Update (upsert)
client.entities().upsert(dataset);

// Update with patches
client.entities().update(dataset);

// Read
Dataset loaded = client.entities().get(datasetUrn);
```

See the [Getting Started Guide](./getting-started.md) for comprehensive examples.

## Configuration Best Practices

### Production Deployment

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server(System.getenv("DATAHUB_SERVER"))
    .token(System.getenv("DATAHUB_TOKEN"))
    .timeoutMs(30000)       // Higher timeout for production
    .maxRetries(5)          // More retries for reliability
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)
    .disableSslVerification(false)  // Always verify SSL!
    .build();
```

### ETL Pipeline

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server(System.getenv("DATAHUB_SERVER"))
    .token(System.getenv("DATAHUB_TOKEN"))
    .timeoutMs(60000)       // Higher timeout for batch jobs
    .maxRetries(3)
    .operationMode(DataHubClientConfigV2.OperationMode.INGESTION)  // Async by default
    .build();
```

### Critical Data Migration

For migrations where you need confirmation before proceeding:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server(System.getenv("DATAHUB_SERVER"))
    .token(System.getenv("DATAHUB_TOKEN"))
    .timeoutMs(60000)
    .maxRetries(5)
    .operationMode(DataHubClientConfigV2.OperationMode.INGESTION)
    .asyncIngest(false)     // Force sync for guaranteed consistency
    .build();
```

### Local Development

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    // No token needed for local quickstart
    .timeoutMs(10000)
    .build();
```

## Troubleshooting

### Connection Refused

**Error:** `java.net.ConnectException: Connection refused`

**Solutions:**

- Verify DataHub server is running
- Check server URL is correct
- Ensure port is accessible (firewall rules)

### Authentication Failed

**Error:** `401 Unauthorized`

**Solutions:**

- Verify token is valid and not expired
- Check token has correct permissions
- Ensure token matches the server environment

### Timeout

**Error:** `java.util.concurrent.TimeoutException`

**Solutions:**

- Increase `timeoutMs` configuration
- Check network latency to DataHub server
- Verify server is not overloaded

### SSL Certificate Error

**Error:** `javax.net.ssl.SSLHandshakeException`

**Solutions:**

- Ensure server SSL certificate is valid
- Add CA certificate to Java truststore
- For testing only: use `disableSslVerification(true)`

## Next Steps

- **[Entities Overview](./entities-overview.md)** - Working with different entity types
- **[Dataset Entity Guide](./dataset-entity.md)** - Comprehensive dataset operations
- **[Patch Operations](./patch-operations.md)** - Efficient incremental updates
- **[Getting Started Guide](./getting-started.md)** - Complete walkthrough

## API Reference

For complete API documentation, see:

- [DataHubClientV2.java](../../datahub-client/src/main/java/datahub/client/v2/DataHubClientV2.java)
- [DataHubClientConfigV2.java](../../datahub-client/src/main/java/datahub/client/v2/config/DataHubClientConfigV2.java)
