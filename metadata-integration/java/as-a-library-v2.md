# Java SDK V2

The DataHub Java SDK V2 provides a modern, type-safe interface for interacting with DataHub's metadata platform. Built on top of the existing DataHub infrastructure, SDK V2 offers an intuitive, fluent API for creating and managing metadata entities.

## Why SDK V2?

SDK V2 represents a significant evolution from the [V1 emitter-based approach](./as-a-library.md), offering:

### **Type-Safe Entity Builders**

Leverage Java's type system with fluent builders that guide you to create valid entities. No more manual URN construction or aspect wiring.

```java
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("my_database.my_schema.my_table")
    .env("PROD")
    .description("User profile dataset")
    .build();
```

### **Simplified CRUD Operations**

Perform create, read, update, and delete operations with a clean, intuitive API:

```java
client.entities().upsert(dataset);          // Create or update
client.entities().update(dataset);          // Update with patches
Dataset loaded = client.entities().get(urn); // Read from server
```

### **Efficient Patch-Based Updates**

Make incremental metadata changes without fetching or replacing entire aspects:

```java
dataset.addTag("pii")
       .addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER)
       .addCustomProperty("team", "data-engineering");

client.entities().update(dataset);  // Applies only the changes
```

### **Lazy Loading & Caching**

Efficiently fetch entity aspects on-demand with built-in TTL-based caching, reducing unnecessary network calls.

### **Mode-Aware Design**

Supports both interactive SDK mode and high-throughput ingestion mode to fit your use case.

## Installation

Add the DataHub client library to your project:

### Gradle

```gradle
dependencies {
    implementation 'io.acryl:datahub-client:__version__'
}
```

### Maven

```xml
<dependency>
    <groupId>io.acryl</groupId>
    <artifactId>datahub-client</artifactId>
    <version>__version__</version>
</dependency>
```

> **Note:** Check the [Maven repository](https://mvnrepository.com/artifact/io.acryl/datahub-client) for the latest version.

## Quick Start

Here's a complete example of creating a dataset with metadata using SDK V2:

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import com.linkedin.common.OwnershipType;

// Create the client
DataHubClientV2 client = DataHubClientV2.builder()
    .server("http://localhost:8080")
    .token("your-access-token")  // Optional for authentication
    .build();

// Build a dataset with metadata
Dataset dataset = Dataset.builder()
    .platform("snowflake")
    .name("analytics.public.user_events")
    .env("PROD")
    .description("User interaction events")
    .displayName("User Events")
    .build();

// Add tags and owners
dataset.addTag("pii")
       .addTag("analytics")
       .addOwner("urn:li:corpuser:datateam", OwnershipType.TECHNICAL_OWNER)
       .addCustomProperty("retention", "90_days");

// Upsert to DataHub
client.entities().upsert(dataset);

System.out.println("Created dataset: " + dataset.getUrn());

// Close the client when done
client.close();
```

## Core Concepts

### Entities

SDK V2 provides entity classes for major DataHub entities:

- **Dataset** - Tables, views, and other data containers
- **Chart** - Visualizations and reports
- **Dashboard** - Collections of charts (coming soon)

Each entity offers a fluent builder and methods for managing metadata.

### Client Operations

The `DataHubClientV2` provides centralized access to operations:

- `entities()` - CRUD operations for entities
- `testConnection()` - Verify connectivity to DataHub server

### Patch-Based Updates

Instead of replacing entire aspects, SDK V2 uses patch operations to make surgical updates to specific metadata fields. This is more efficient and reduces the risk of overwriting concurrent changes.

## Documentation

Explore detailed guides for working with SDK V2:

- **[Getting Started Guide](./docs/sdk-v2/getting-started.md)** - Comprehensive tutorial with authentication, configuration, and basic operations
- **[Design Principles](./docs/sdk-v2/design-principles.md)** - Architectural overview and engineering principles behind SDK V2
- **[DataHubClientV2](./docs/sdk-v2/client.md)** - Client configuration, connection management, and operation modes
- **[Entities Overview](./docs/sdk-v2/entities-overview.md)** - Common patterns across all entity types
- **[Dataset Entity](./docs/sdk-v2/dataset-entity.md)** - Complete guide to working with datasets
- **[Chart Entity](./docs/sdk-v2/chart-entity.md)** - Creating and managing chart entities
- **[Patch Operations](./docs/sdk-v2/patch-operations.md)** - Deep dive into efficient incremental updates
- **[Migration from V1](./docs/sdk-v2/migration-from-v1.md)** - Step-by-step guide for upgrading from SDK V1

## Example Code

Find complete, runnable examples in the [examples directory](./examples/src/main/java/io/datahubproject/examples/v2/):

- [DatasetCreateExample.java](./examples/src/main/java/io/datahubproject/examples/v2/DatasetCreateExample.java) - Basic dataset creation
- [DatasetPatchExample.java](./examples/src/main/java/io/datahubproject/examples/v2/DatasetPatchExample.java) - Adding tags, owners, and custom properties
- [DatasetFullExample.java](./examples/src/main/java/io/datahubproject/examples/v2/DatasetFullExample.java) - Comprehensive metadata management
- [ChartCreateExample.java](./examples/src/main/java/io/datahubproject/examples/v2/ChartCreateExample.java) - Chart entity creation

## Comparison with V1

| Feature             | V1 (RestEmitter)               | V2 (DataHubClientV2)            |
| ------------------- | ------------------------------ | ------------------------------- |
| **Entity Creation** | Manual MCP construction        | Fluent entity builders          |
| **Type Safety**     | Low - manual aspect wiring     | High - compile-time validation  |
| **URN Management**  | Manual string construction     | Automatic from builder          |
| **Updates**         | Replace entire aspects         | Patch-based incremental updates |
| **API Style**       | Low-level emitter              | High-level CRUD operations      |
| **Learning Curve**  | Steep - requires MCP knowledge | Gentle - intuitive builders     |

See the [detailed migration guide](./docs/sdk-v2/migration-from-v1.md) for help transitioning from V1 to V2.

## Support

For questions, issues, or contributions:

- [DataHub GitHub](https://github.com/datahub-project/datahub)
- [DataHub Slack](https://datahub.com/slack)
- [API Tutorials](../../docs/api/tutorials) - Language-agnostic guides with Java examples

## What's Next?

- **Getting Started**: Follow the [comprehensive tutorial](./docs/sdk-v2/getting-started.md) to build your first application
- **Learn the Architecture**: Read about SDK V2's [design principles](./docs/sdk-v2/design-principles.md)
- **Explore Entities**: Dive into [Dataset](./docs/sdk-v2/dataset-entity.md) or [Chart](./docs/sdk-v2/chart-entity.md) guides
- **Migrate from V1**: Use the [migration guide](./docs/sdk-v2/migration-from-v1.md) to upgrade existing code
