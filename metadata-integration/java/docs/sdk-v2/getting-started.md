# Getting Started with Java SDK V2

This guide walks you through setting up and using the DataHub Java SDK V2 to interact with DataHub's metadata platform.

## Prerequisites

- Java 8 or higher
- Access to a DataHub instance (Cloud or self-hosted)
- (Optional) A DataHub personal access token for authentication

## Installation

Add the DataHub client library to your project's build configuration.

### Gradle

Add to your `build.gradle`:

```gradle
dependencies {
    implementation 'io.acryl:datahub-client:__version__'
}
```

### Maven

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>io.acryl</groupId>
    <artifactId>datahub-client</artifactId>
    <version>__version__</version>
</dependency>
```

> **Tip:** Find the latest version on [Maven Central](https://mvnrepository.com/artifact/io.acryl/datahub-client).

## Creating a Client

The `DataHubClientV2` is your entry point to all SDK operations. Create one by specifying your DataHub server URL:

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

> **How to get a token:** In DataHub UI, go to Settings → Access Tokens → Generate Personal Access Token

### Testing the Connection

Verify your client can reach the DataHub server:

```java
try {
    boolean connected = client.testConnection();
    if (connected) {
        System.out.println("Successfully connected to DataHub!");
    } else {
        System.out.println("Failed to connect to DataHub");
    }
} catch (Exception e) {
    System.err.println("Connection error: " + e.getMessage());
}
```

## Creating Your First Entity

Let's create a dataset with some metadata.

### Step 1: Import Required Classes

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import com.linkedin.common.OwnershipType;
```

### Step 2: Build a Dataset

Use the fluent builder to construct a dataset:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("analytics.public.user_events")
    .env("PROD")
    .description("User interaction events from web and mobile")
    .displayName("User Events")
    .build();
```

**Breaking down the builder:**

- `platform` - Data platform identifier (e.g., "snowflake", "bigquery", "postgres")
- `name` - Fully qualified dataset name (database.schema.table or similar)
- `env` - Environment (PROD, DEV, STAGING, etc.)
- `description` - Human-readable description of the dataset
- `displayName` - Friendly name shown in DataHub UI

### Step 3: Add Metadata

Enrich the dataset with tags, owners, and custom properties:

```java
dataset.addTag("pii")
       .addTag("analytics")
       .addOwner("urn:li:corpuser:john_doe", OwnershipType.TECHNICAL_OWNER)
       .addCustomProperty("retention_days", "90")
       .addCustomProperty("team", "data-engineering");
```

### Step 4: Upsert to DataHub

Send the dataset to DataHub:

```java
try {
    client.entities().upsert(dataset);
    System.out.println("Successfully created dataset: " + dataset.getUrn());
} catch (IOException | ExecutionException | InterruptedException e) {
    System.err.println("Failed to create dataset: " + e.getMessage());
}
```

## Complete Example

Here's a complete, runnable example:

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import com.linkedin.common.OwnershipType;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DataHubQuickStart {
    public static void main(String[] args) {
        // Create client
        DataHubClientV2 client = DataHubClientV2.builder()
            .server("http://localhost:8080")
            .token("your-token-here")  // Optional
            .build();

        try {
            // Test connection
            if (!client.testConnection()) {
                System.err.println("Cannot connect to DataHub");
                return;
            }

            // Build dataset
            Dataset dataset = Dataset.builder()
                .platform("snowflake")
                .name("analytics.public.user_events")
                .env("PROD")
                .description("User interaction events")
                .displayName("User Events")
                .build();

            // Add metadata
            dataset.addTag("pii")
                   .addTag("analytics")
                   .addOwner("urn:li:corpuser:datateam", OwnershipType.TECHNICAL_OWNER)
                   .addCustomProperty("retention_days", "90");

            // Upsert to DataHub
            client.entities().upsert(dataset);
            System.out.println("Created dataset: " + dataset.getUrn());

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
```

For more complete examples, see the [Dataset Entity Guide](./dataset-entity.md#examples).

## Reading Entities

Load an existing entity from DataHub:

```java
import com.linkedin.common.urn.DatasetUrn;

DatasetUrn urn = new DatasetUrn(
    "snowflake",
    "analytics.public.user_events",
    "PROD"
);

try {
    Dataset loaded = client.entities().get(urn);
    if (loaded != null) {
        System.out.println("Dataset description: " + loaded.getDescription());
        System.out.println("Is read-only: " + loaded.isReadOnly());  // true
    }
} catch (IOException | ExecutionException | InterruptedException e) {
    e.printStackTrace();
}
```

> **Important:** Entities fetched from the server are **read-only by default**. Additional aspects are lazy-loaded on demand.

### Understanding Read-Only Entities

When you fetch an entity from DataHub, it's immutable to prevent accidental modifications:

```java
Dataset dataset = client.entities().get(urn);

// Reading works fine
String description = dataset.getDescription();
List<String> tags = dataset.getTags();

// But mutation throws ReadOnlyEntityException
// dataset.addTag("pii");  // ERROR: Cannot mutate read-only entity!
```

**Why?** Immutability-by-default makes mutation intent explicit, prevents accidental changes when passing entities between functions, and enables safe entity sharing.

## Updating Entities with Patches

To modify a fetched entity, create a mutable copy first:

```java
// 1. Load existing dataset (read-only)
Dataset dataset = client.entities().get(urn);

// 2. Get mutable copy
Dataset mutable = dataset.mutable();

// 3. Add new tags and owners (patch operations)
mutable.addTag("gdpr")
       .addOwner("urn:li:corpuser:new_owner", OwnershipType.TECHNICAL_OWNER);

// 4. Apply patches to DataHub
client.entities().update(mutable);
```

The `update()` method sends only the changes (patches) to DataHub, not the full entity. This is more efficient and safer for concurrent updates.

### Entity Lifecycle

Understanding when entities are mutable vs read-only:

**Builder-created entities** - Mutable from creation:

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_table")
    .build();

dataset.isMutable();  // true - can mutate immediately
dataset.addTag("test");  // Works without .mutable()
```

**Server-fetched entities** - Read-only by default:

```java
Dataset dataset = client.entities().get(urn);

dataset.isReadOnly();  // true
// dataset.addTag("test");  // ERROR!

Dataset mutable = dataset.mutable();  // Get writable copy
mutable.addTag("test");  // Now works
```

See the [Patch Operations Guide](./patch-operations.md) for details.

## Upserting vs Updating

SDK V2 provides two methods for persisting entities:

### `upsert(entity)`

- **Use for:** New entities or full replacements
- **Sends:** All aspects from the entity
- **Behavior:** Creates if doesn't exist, replaces if exists

```java
client.entities().upsert(dataset);
```

### `update(entity)`

- **Use for:** Incremental changes to existing entities
- **Sends:** Only pending patches accumulated since the entity was loaded or created
- **Behavior:** Applies surgical updates to specific fields

```java
client.entities().update(dataset);
```

## Working with Other Entities

SDK V2 supports multiple entity types beyond datasets:

### Charts

```java
import datahub.client.v2.entity.Chart;

Chart chart = Chart.builder()
    .tool("looker")
    .id("my_sales_chart")
    .title("Sales Performance by Region")
    .description("Monthly sales broken down by geographic region")
    .build();

client.entities().upsert(chart);
```

See the [Chart Entity Guide](./chart-entity.md) for details.

### Dashboards

Coming soon! Dashboard entity support is planned for a future release.

## Configuration Options

Customize the client for your environment:

```java
DataHubClientV2 client = DataHubClientV2.builder()
    .server("https://your-instance.acryl.io")
    .token("your-access-token")

    // Configure operation mode
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)  // or INGESTION

    // Customize underlying REST emitter
    .restEmitterConfig(config -> config
        .timeoutSec(30)
        .maxRetries(5)
        .retryIntervalSec(2)
    )

    .build();
```

### Operation Modes

SDK V2 supports two operation modes:

- **SDK Mode** (default): For interactive applications, provides patch-based updates and lazy loading
- **INGESTION Mode**: For ETL pipelines, optimizes for high-throughput batch operations

```java
// SDK mode (default) - interactive use
DataHubClientV2 sdkClient = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.SDK)
    .build();

// Ingestion mode - ETL pipelines
DataHubClientV2 ingestionClient = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .operationMode(DataHubClientConfigV2.OperationMode.INGESTION)
    .build();
```

See [DataHubClientV2 Configuration](./client.md) for all available options.

## Error Handling

Handle errors gracefully:

```java
try {
    client.entities().upsert(dataset);
} catch (IOException e) {
    // Network or serialization errors
    System.err.println("I/O error: " + e.getMessage());
} catch (ExecutionException e) {
    // Server-side errors
    System.err.println("Server error: " + e.getCause().getMessage());
} catch (InterruptedException e) {
    // Operation cancelled
    Thread.currentThread().interrupt();
}
```

## Resource Management

Always close the client when done to release resources:

```java
try (DataHubClientV2 client = DataHubClientV2.builder()
        .server("http://localhost:8080")
        .build()) {

    // Use client here
    client.entities().upsert(dataset);

} // Client automatically closed
```

Or close explicitly:

```java
try {
    // Use client
} finally {
    client.close();
}
```

## Next Steps

Now that you've created your first entity, explore more advanced topics:

- **[Design Principles](./design-principles.md)** - Understand the architecture behind SDK V2
- **[Dataset Entity Guide](./dataset-entity.md)** - Comprehensive dataset operations
- **[Chart Entity Guide](./chart-entity.md)** - Working with chart entities
- **[Patch Operations](./patch-operations.md)** - Deep dive into incremental updates
- **[Client Configuration](./client.md)** - Advanced client setup and options

Or check out complete examples in the entity guides:

- [Dataset Examples](./dataset-entity.md#examples)
- [Chart Examples](./chart-entity.md#examples)
- [Dashboard Examples](./dashboard-entity.md#examples)
- [DataJob Examples](./datajob-entity.md#examples)
