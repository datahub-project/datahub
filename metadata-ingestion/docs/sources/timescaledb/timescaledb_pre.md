### Prerequisites

#### Database Permissions

In order to execute this source, the user credentials need the following privileges:

**For Self-Hosted TimescaleDB:**

- `GRANT SELECT ON ALL TABLES IN SCHEMA public TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO 'USERNAME';`

**For Tiger Cloud (Managed TimescaleDB):**

- `GRANT SELECT ON ALL TABLES IN SCHEMA public TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO 'USERNAME';`
- `GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO 'USERNAME';`

The `SELECT` privilege is required to:

- Read table structure and metadata
- Access TimescaleDB-specific system views for hypertables, continuous aggregates, and jobs
- Perform data profiling (if enabled)

#### Setup Instructions

**Self-Hosted TimescaleDB:**

1. Install TimescaleDB extension in your PostgreSQL database:

   ```sql
   CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
   ```

2. Create a dedicated user for DataHub ingestion:

   ```sql
   CREATE USER datahub_user WITH PASSWORD 'your_secure_password';
   ```

3. Grant necessary permissions:

   ```sql
   GRANT CONNECT ON DATABASE your_database TO datahub_user;
   GRANT USAGE ON SCHEMA public TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO datahub_user;

   -- Grant permissions on future tables
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
   ```

**Tiger Cloud (Managed TimescaleDB):**

1. Get your connection details from the [Tiger Cloud console](https://docs.tigerdata.com/integrations/latest/find-connection-details/). You'll need:

   - Hostname
   - Port (usually 5432)
   - Database name
   - Username
   - Password

2. Create a dedicated user for DataHub ingestion (if you have admin access):

   ```sql
   CREATE USER datahub_user WITH PASSWORD 'your_secure_password';
   ```

3. Grant necessary permissions:

   ```sql
   GRANT CONNECT ON DATABASE your_database TO datahub_user;
   GRANT USAGE ON SCHEMA public TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO datahub_user;
   GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_information TO datahub_user;

   -- Grant permissions on future tables
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datahub_user;
   ```

#### Network Connectivity

**Self-Hosted TimescaleDB:**

- Ensure your TimescaleDB instance is accessible from where DataHub is running
- Default port is 5432 (PostgreSQL standard)
- Configure `pg_hba.conf` to allow connections from DataHub's IP address

**Tiger Cloud:**

- Tiger Cloud services are accessible over the internet with SSL encryption
- No additional firewall configuration needed
- SSL connections are required and enabled by default

#### SSL Configuration

**Self-Hosted TimescaleDB:**

- SSL is recommended for production deployments
- Configure SSL certificates in PostgreSQL configuration
- Use connection parameters like `sslmode=require` in the DataHub configuration

**Tiger Cloud:**

- SSL is enabled by default and required
- No additional SSL configuration needed
- Connection automatically uses encrypted connections

#### Performance Considerations

- For large TimescaleDB deployments with many hypertables, consider:
  - Setting appropriate `table_pattern` filters to limit scope
  - Using `schema_pattern` to focus on specific schemas
  - Enabling `stateful_ingestion` for incremental updates
  - Adjusting `profiling.max_number_of_fields_to_profile` if profiling large tables

#### Troubleshooting

**Common Issues:**

1. **Permission Denied on TimescaleDB Views:**

   - Ensure the user has SELECT access to `_timescaledb_*` schemas (self-hosted) or `timescaledb_information` schema (Tiger Cloud)

2. **Connection Timeout:**

   - Check network connectivity and firewall settings
   - Verify the correct hostname and port

3. **SSL Connection Issues:**

   - For Tiger Cloud: SSL is required, ensure your client supports SSL
   - For self-hosted: Configure SSL properly or use `sslmode=disable` for testing

4. **Missing Hypertables/Jobs:**
   - Verify TimescaleDB extension is installed: `SELECT * FROM pg_extension WHERE extname = 'timescaledb';`
   - Check if user has access to TimescaleDB metadata views

For more detailed connection information for TimescaleDB connectivity, see the [official Tiger Cloud integration guide](https://docs.tigerdata.com/integrations/latest/find-connection-details/).
