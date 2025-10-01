# Configuring Change Data Capture (CDC) Mode

## Overview

DataHub supports CDC (Change Data Capture) mode for generating MetadataChangeLogs (MCLs). In CDC mode, MCLs are generated from database change events rather than being produced directly by GMS. This provides stronger ordering guarantees - MCLs are generated in the exact order of database transaction commits.
Use the CDC mode if you need strict ordering guarantees for metadata changes

The default deployments do not use CDC. Explicit configuration is required to switch using the CDC mode.

## Prerequisites

Before enabling CDC mode, ensure you have:

1. **Supported Database**: MySQL 5.7+ or PostgreSQL 10+
2. **Kafka Connect**: Running Kafka Connect with Debezium connector plugin installed
3. **Database Permissions**: Ability to enable replication and create CDC users
4. **Kafka Resources**: Sufficient resources for additional CDC topic

## Architecture

In CDC mode:

```
MCP → GMS (writes to DB, no MCL emission)
         ↓
    Database Change
         ↓
    Debezium Connector (captures changes)
         ↓
    CDC Topic (datahub.metadata_aspect_v2)
         ↓
    MCE Consumer (reads CDC events, generates MCLs via EntityService)
         ↓
    MCL Topics (MetadataChangeLog_Versioned_v1)
```

## Configuration Overview

CDC mode requires configuration in three layers:

1. **Database Layer**: Enable replication, create CDC user, configure tables
2. **Debezium Layer**: Configure Kafka Connect connector (auto or manual)
3. **DataHub Layer**: Enable CDC processing in GMS and MCE Consumer

## Database Configuration

**For development environments**: DataHub can automate database setup via the system-update service when `CDC_CONFIGURE_SOURCE=true`. The system-update service will create the necessary CDC user and configure database settings during startup.

**For production environments**: Manual database configuration is recommended for better control and security practices specific to your environment.

### MySQL Requirements

Your MySQL instance must be configured to support CDC:

1. **Enable Binary Logging**: Configure MySQL to use ROW-based binary logging with FULL row images. This allows Debezium to capture all column changes.

2. **Create CDC User**: Create a dedicated database user for CDC with the following capabilities:

   - Read access to the DataHub database (specifically the `metadata_aspect_v2` table)
   - Replication client privileges to read binary logs
   - Replication slave privileges to monitor replication status
   - Database introspection privileges to discover schema changes

3. **Configure Binary Log Retention**: Set appropriate binary log retention based on your operational needs and disaster recovery requirements. Consider factors like Debezium downtime tolerance and disk space.

4. **Server ID**: Ensure each MySQL instance (including the CDC connector) has a unique server ID for replication topology.

**What DataHub automates (development mode)**:

- Creates CDC user with required permissions on the local database
- Validates binary logging configuration
- Configures Debezium connector

### PostgreSQL Requirements

Your PostgreSQL instance must be configured to support logical replication:

1. **Enable Logical Replication**: Configure PostgreSQL WAL (Write-Ahead Log) level to 'logical'. This is required for CDC to capture row-level changes.

2. **Configure Replication Slots**: Allocate sufficient replication slots (at least one per CDC connector). Replication slots track the CDC consumer's position in the WAL.

3. **Create CDC User**: Create a dedicated database user for CDC with the following capabilities:

   - Replication privileges to create and read from replication slots
   - Connect privileges on the DataHub database
   - Select privileges on the `metadata_aspect_v2` table in the public schema

4. **Set Replica Identity to FULL**: Configure the `metadata_aspect_v2` table to use FULL replica identity. This ensures Debezium receives complete before/after images of changed rows, which is critical for DataHub's MCL generation.

5. **Create Publication**: Create a PostgreSQL publication named **`dbz_publication`** for the `metadata_aspect_v2` table. Publications define which tables participate in logical replication. DataHub expects this specific publication name when auto-configuring.

6. **Create Replication Slot**: When manually configuring, create a replication slot named **`debezium`** using the `pgoutput` plugin. DataHub's default configuration uses this slot name.

7. **Configure WAL Senders**: Ensure sufficient `max_wal_senders` for the number of CDC connectors and any other replication consumers.

**What DataHub automates (development mode)**:

- Creates CDC user with required permissions
- Sets replica identity to FULL on metadata_aspect_v2 table
- Creates publication named `dbz_publication` for the table
- Creates replication slot named `debezium`
- Validates logical replication configuration
- Configures Debezium connector with correct publication and slot names

**Important PostgreSQL Notes**:

- Replication slots prevent WAL cleanup. Monitor disk space and configure `max_slot_wal_keep_size` to prevent runaway growth.
- When a CDC connector is stopped, consider dropping its replication slot to allow WAL cleanup.
- Database restarts may be required to change WAL-related configuration parameters.
- The publication name `dbz_publication` and slot name `debezium` are DataHub's defaults - if you use different names, update your Debezium connector configuration accordingly.

## Debezium Configuration

You have two options for configuring Debezium:

### Option 1: Auto-Configuration (Development/Testing)

For development and testing environments, DataHub can automatically configure Debezium via the system-update service.

**Environment Variables**:

```bash
# Enable CDC processing
CDC_MCL_PROCESSING_ENABLED=true

# Enable auto-configuration
CDC_CONFIGURE_SOURCE=true

# Database type (mysql or postgres)
CDC_DB_TYPE=mysql

# CDC user credentials (will be created if doesn't exist)
CDC_USER=datahub_cdc
CDC_PASSWORD=your_secure_password

# Kafka Connect endpoint
CDC_KAFKA_CONNECT_URL=http://kafka-connect:8083

# Connector name
DATAHUB_CDC_CONNECTOR_NAME=datahub-cdc-connector

# CDC topic name
CDC_TOPIC_NAME=datahub.metadata_aspect_v2
```

**Docker Compose Example**:

```yaml
datahub-gms:
  environment:
    - CDC_MCL_PROCESSING_ENABLED=true

mce-consumer:
  environment:
    - CDC_MCL_PROCESSING_ENABLED=true

datahub-upgrade:
  environment:
    - CDC_MCL_PROCESSING_ENABLED=true
    - CDC_CONFIGURE_SOURCE=true
    - CDC_DB_TYPE=mysql # or postgres
    - CDC_USER=datahub_cdc
    - CDC_PASSWORD=your_secure_password
    - CDC_KAFKA_CONNECT_URL=http://kafka-connect:8083
```

### Option 2: Manual Configuration (Production)

For production environments, manually configure Debezium Kafka Connect for better control.

**Key Configuration Requirements**:

Regardless of database type, your Debezium connector must be configured with the following critical settings:

1. **Table to Capture**: The connector must capture changes from the `metadata_aspect_v2` table only. This is the primary storage table for DataHub's versioned aspects.

2. **Output Topic Name**: CDC events must be routed to a single consolidated topic (default: `datahub.metadata_aspect_v2`). Use Debezium's routing transformation to override the default per-table topic naming.

3. **Message Format**:

   - **Key Converter**: Use `StringConverter` - keys will be entity URNs as plain strings
   - **Value Converter**: Use `JsonConverter` - CDC payloads must be in JSON format
   - **Schemas Disabled**: Set `value.converter.schemas.enable: false` - DataHub validates aspects internally and doesn't use Kafka schema registry for CDC events

4. **Partitioning Key**: Messages must be partitioned by the `urn` column. This ensures all changes for a given entity are processed in order by routing them to the same Kafka partition.

#### Example MySQL Connector Configuration

The following is an example configuration for MySQL similar to what datahub auto-configures for development/test

```json
{
  "name": "datahub-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",

    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "datahub_cdc",
    "database.password": "your_secure_password",
    "database.server.id": "184001",
    "database.server.name": "datahub",
    "database.include.list": "datahub",
    "table.include.list": "datahub.metadata_aspect_v2",

    "database.history.kafka.bootstrap.servers": "broker:29092",
    "database.history.kafka.topic": "datahub.schema-changes",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "message.key.columns": "datahub.metadata_aspect_v2:urn",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": ".*metadata_aspect_v2",
    "transforms.route.replacement": "datahub.metadata_aspect_v2",

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",
    "decimal.handling.mode": "string",
    "bigint.unsigned.handling.mode": "long"
  }
}
```

#### PostgreSQL Connector Configuration

The following is an example configuration for Postgres similar to what datahub auto-configures for development/test

```json
{
  "name": "datahub-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "datahub_cdc",
    "database.password": "your_secure_password",
    "database.dbname": "datahub",
    "database.server.name": "datahub",

    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "slot.name": "debezium",

    "schema.include.list": "public",
    "table.include.list": "public.metadata_aspect_v2",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "message.key.columns": "public.metadata_aspect_v2:urn",

    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": ".*metadata_aspect_v2",
    "transforms.route.replacement": "datahub.metadata_aspect_v2",

    "snapshot.mode": "initial",
    "decimal.handling.mode": "string"
  }
}
```

#### Deploy Connector

**Environment Variables for Manual Setup**:

```bash
# Enable CDC processing
CDC_MCL_PROCESSING_ENABLED=true

# Disable auto-configuration
CDC_CONFIGURE_SOURCE=false

# CDC topic name (must match connector config)
CDC_TOPIC_NAME=datahub.metadata_aspect_v2
```

## DataHub Configuration

Once database and Debezium are configured, enable CDC mode in DataHub components:

### Docker Compose Configuration

```yaml
datahub-gms:
  environment:
    # Enable CDC mode (disables direct MCL emission from GMS)
    - CDC_MCL_PROCESSING_ENABLED=true

mce-consumer:
  environment:
    # Enable CDC mode (enables CDC message processing)
    - CDC_MCL_PROCESSING_ENABLED=true
    - CDC_TOPIC_NAME=datahub.metadata_aspect_v2

datahub-upgrade:
  environment:
    - CDC_MCL_PROCESSING_ENABLED=true
    - CDC_CONFIGURE_SOURCE=false # indicates manually configured Debezium
    - CDC_DB_TYPE=mysql # or postgres
    - CDC_USER=datahub_cdc
    - CDC_PASSWORD=your_secure_password
```

## Verification

### 1. Verify Debezium Connector

```bash
# Check connector status
curl http://kafka-connect:8083/connectors/datahub-cdc-connector/status | jq

# Expected output shows "state": "RUNNING"
```

### 2. Verify CDC Topic

```bash
# List topics (should see datahub.metadata_aspect_v2)
kafka-topics --bootstrap-server broker:9092 --list | grep metadata_aspect_v2

# Consume CDC events
kafka-console-consumer --bootstrap-server broker:9092 \
  --topic datahub.metadata_aspect_v2 \
  --from-beginning --max-messages 10
```

### 3. Test End-to-End

1. Make a metadata change via DataHub UI or API
2. Verify CDC event is captured in the CDC topic (CDC events are in JSON format):

   ```bash
   kafka-console-consumer --bootstrap-server broker:9092 \
     --topic datahub.metadata_aspect_v2 \
     --from-beginning --max-messages 1
   ```

3. Verify MCL is generated in the MCL topic (MCL events are Avro-serialized and require schema registry access):

   ```bash
   # Using kafka-avro-console-consumer (requires schema registry)
   kafka-avro-console-consumer --bootstrap-server broker:9092 \
     --topic MetadataChangeLog_Versioned_v1 \
     --property schema.registry.url=http://schema-registry:8081 \
     --from-beginning --max-messages 1
   ```

   Alternatively, check the MCE consumer logs to confirm MCLs are being generated:

   ```bash
   docker logs mce-consumer | grep -i "Emitting MCL"
   ```

## Additional Resources

- [Environment Variables Reference](../deploy/environment-vars.md#change-data-capture-cdc-configuration)
- [MCP/MCL Events Documentation](../advanced/mcp-mcl.md#change-data-capture-cdc-mode-for-generating-mcls)
- [Debezium MySQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
