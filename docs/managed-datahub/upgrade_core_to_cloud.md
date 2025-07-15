# Upgrading from DataHub Core to DataHub Cloud

Looking to upgrade to **DataHub Cloud**, but don't have an account yet? Start [here](https://datahub.com/demo/). 

Once you have a **DataHub Cloud** instance, you can seamlessly transfer all metadata from your self-hosted **DataHub Core** instance
to **DataHub Cloud** using the DataHub CLI. In this guide, we'll show you how.

## Prerequisites

Before starting the upgrade process:

1. **DataHub Cloud Account**: Ensure you have an active DataHub Cloud instance with an API token
2. **Database Access**: You'll need read access to your DataHub Core MySQL or PostgreSQL database
3. **DataHub CLI**: Install the DataHub CLI with `pip install acryl-datahub`
4. **Network Connectivity**: Ensure your upgrade environment can access both your source database and DataHub Cloud
5. **Database Index**: Verify that the `createdon` column is indexed in your source database - it should be by default:
   ```sql
   CREATE INDEX timeIndex ON metadata_aspect_v2 (createdon);
   ```

## Upgrade Approaches

### Quickstart: Transferring Core Metadata

This approach transfers all essential metadata by default, including:

- Data assets (datasets, dashboards, charts, etc.)
- Users and groups
- Lineage relationships
- Documentation and descriptions
- Ownership information
- Domains and data products
- Tags and glossary terms

It does NOT transfer the following:

- Ingestion Sources
- Ingestion Source Runs
- Ingestion Secrets
- Platform Settings

Or any aspect due to the different encryption scheme employed on DataHub cloud. This method also excludes time-series metadata such as dataset profiles, column statistics, and assertion run history.

#### Step 1: Create Your Upgrade Recipe

Create a file named `upgrade_recipe.yml` with the following configuration:

```yaml
pipeline_name: datahub_cloud_upgrade
source:
  type: datahub
  config:
    # Disable version history to transfer only current state
    include_all_versions: false
    
    # Configure your source database connection
    database_connection:
      # For MySQL
      scheme: "mysql+pymysql"
      # For PostgreSQL, use: "postgresql+psycopg2"
      
      host_port: "your-database-host:3306"  # MySQL default port
      username: "your-datahub-username"
      password: "your-datahub-password"
      database: "datahub"  # Default database name

    # Disable stateful ingestion for one-time transfer.
    # If you intend to incrementally sync over time, should enable this!
    stateful_ingestion:
      enabled: false

# Preserve system metadata during transfer
flags:
  set_system_metadata: true

# Configure DataHub Cloud as destination
sink:
  type: datahub-rest
  config:
    server: "https://your-instance.acryl.io"
    token: "your-datahub-cloud-api-token"
```

#### Step 2: Run the Upgrade 

Execute the upgrade using the DataHub CLI:

```bash
datahub ingest -c upgrade_recipe.yml
```

The upgrade will display progress as it transfers your metadata. Depending on the size of your catalog, this process can take anywhere from minutes to hours.

#### Step 3: Verify the Upgrade

After completion:

1. Log into your DataHub Cloud instance
2. Navigate to the Browse page to verify your assets
3. Check a few key datasets to ensure documentation, owners, and tags transferred correctly
4. Verify lineage relationships are intact

### Advanced: Including Time-Series Metadata

For a complete transfer including historical time-series data, you'll need to connect to your **Kafka** cluster. This captures:

- Dataset and column profiling history
- Dataset operation logs
- Assertion run results
- Usage statistics

**Important**: The amount of historical data available depends on your Kafka retention policy (typically 30-90 days).

#### Enhanced Recipe Configuration

```yaml
pipeline_name: datahub_upgrade_with_timeseries
source:
  type: datahub
  config:
    include_all_versions: false
    
    # Database connection (same as quickstart)
    database_connection:
      scheme: "mysql+pymysql"
      host_port: "your-database-host:3306"
      username: "your-datahub-username"
      password: "your-datahub-password"
      database: "datahub"
    
    # Kafka configuration for time-series data
    kafka_connection:
      bootstrap: "your-kafka-broker:9092"
      schema_registry_url: "http://your-schema-registry:8081"
      consumer_config:
        # Optional: Add security configuration if needed
        # security.protocol: "SASL_SSL"
        # sasl.mechanism: "PLAIN"
        # sasl.username: "your-username"
        # sasl.password: "your-password"
    
    # Topic containing time-series data (change if doesn't match default name)
    kafka_topic_name: "MetadataChangeLog_Timeseries_v1"

    # Disable stateful ingestion for one-time transfer.
    # If you intend to incrementally sync over time, should enable this!
    stateful_ingestion:
      enabled: false

flags:
  set_system_metadata: true

sink:
  type: datahub-rest
  config:
    server: "https://your-instance.acryl.io"
    token: "your-datahub-cloud-api-token"
```

### Advanced Configuration: Transferring Specific Aspects

You can override the default aspects which are excluded from transfer during upgrade using the `exclude_aspects` configuration.
**Be careful! Some aspects**, particularly those containing encrypted secrets, will NOT transfer to DataHub Cloud due to differences in encryption schemes.

```yaml
source:
  type: datahub
  config:
    # ... other config ...
    
    # Exclude specific aspects from transfer
    exclude_aspects:
      - dataHubIngestionSourceInfo
      - datahubIngestionCheckpoint
      - dataHubExecutionRequestInput
      - dataHubIngestionSourceKey
      - dataHubExecutionRequestResult
      - globalSettingsInfo
      - datahubIngestionRunSummary
      - dataHubExecutionRequestSignal
      - globalSettingsKey
      - testResults
      - dataHubExecutionRequestKey
      - dataHubSecretValue
      - dataHubSecretKey
      # Add any other aspects you want to exclude
```

To learn about all aspects in DataHub, check out the [DataHub metadata model documentation](https://docs.datahub.com/docs/metadata-modeling/metadata-model/).

## Best Practices

### Performance Optimization

1. **Batch Size**: For large transfers, adjust the batch configuration:
   ```yaml
   source:
     type: datahub
     config:
       database_query_batch_size: 10000  # Adjust based on your system
       commit_state_interval: 1000  # Records before checkpoint
   ```

2. **Destination Settings**: For optimal performance on DataHub Cloud:
    - Enable async ingestion (usually enabled by default)
    - Consider scaling consumer replicas for large upgrade transfers
    - Increase thread count if needed in sink settings

3. **Stateful Ingestion**: For very large instances, use stateful ingestion:
   ```yaml
   stateful_ingestion:
     enabled: true
     ignore_old_state: false  # Set to true to restart from beginning!
   ```

### Troubleshooting

Common issues and solutions:

- **Authentication Errors**: Verify your API token has write permissions
- **Network Timeouts**: Check firewall rules and consider adjusting `query_timeout`
- **Memory Issues**: Reduce `database_query_batch_size` for large transfers
- **Slow Performance**: Ensure the `createdon` index exists on your source database
- **Parse Errors**: Set `commit_with_parse_errors: true` to continue despite errors

### Error Handling

By default, the upgrade job will stop committing checkpoints if errors occur, allowing you to re-run and catch missed data. 

However, in some cases it's not possible to transfer data to DataHub Cloud, particularly if you've forked and extended the DataHub
metadata model. 

To continue making progress, ignoring errors:

```yaml
source:
  type: datahub
  config:
    commit_with_parse_errors: true  # Continue even with parse errors
```

## Post-Upgrade Steps

1. **Update Integrations**: Point your ingestion pipelines to DataHub Cloud
2. **Configure SSO**: Set up authentication for your team
3. **Update API Clients**: Update any applications using DataHub APIs
4. **Verify Automations**: Test any existing automation workflows
5. **Review Policies**: Recreate any custom policies and roles as needed

## Additional Resources

For more detailed configuration options, refer to the [DataHub source documentation](https://datahubproject.io/docs/generated/ingestion/sources/datahub).

Need help? Contact DataHub Cloud support or visit our [community Slack](https://datahubproject.io/slack).