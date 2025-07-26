# Documentation Propagation Action

The Documentation Propagation Action allows you to automatically propagate documentation from schema fields to related schema fields. For example, when you add or update documentation for a column in a dataset, this action can automatically propagate that documentation to upstream, downstream, or sibling columns.

## Functionality

This action listens for documentation changes on schema fields and propagates those changes to related fields based on your configuration. It supports:

- **Downstream Propagation**: Propagate documentation to columns in downstream datasets
- **Upstream Propagation**: Propagate documentation to columns in upstream datasets
- **Sibling Propagation**: Propagate documentation to columns in sibling datasets (e.g., views of the same table)

## Configuration Options

The Documentation Propagation Action provides several configuration options:

- `enabled`: Controls whether documentation propagation is enabled (default: `true`)
- `columns_enabled`: Controls whether column-level documentation propagation is enabled (default: `true`)
- `datasets_enabled`: Controls whether dataset-level documentation propagation is enabled (default: `false`) - Note: Currently not implemented
- `column_propagation_relationships`: Specifies which relationships to use for propagation. Valid values are:
    - `UPSTREAM`: Propagate to upstream columns
    - `DOWNSTREAM`: Propagate to downstream columns
    - `SIBLING`: Propagate to sibling columns
- `max_propagation_depth`: Maximum depth for propagation chains (default: `5`)
- `max_propagation_fanout`: Maximum number of entities to propagate to in a single hop (default: `1000`)
- `max_propagation_time_millis`: Maximum time in milliseconds for a propagation chain (default: `3600000` - 1 hour)

## Example Configuration

```yaml
name: "documentation_propagation"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
    # Topic Routing - which topics to read from.
    topic_routes:
      #mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1} # Topic name for MetadataChangeLogEvent_v1 events.
      pe: ${PLATFORM_EVENT_TOPIC_NAME:PlatformEvent_v1} # Topic name for PlatformEvent_v1 events.  
filter:
  event_type: "EntityChangeEvent_v1"
  event:
    entityType: "schemaField"
    category: "DOCUMENTATION"
action:
  type: "doc_propagation"
  config:
    enabled: true
    columns_enabled: true
    max_propagation_depth: 3  # Optional: Limit propagation depth

datahub:
  server: ${DATAHUB_GMS_HOST:-http://localhost:8080}
  token: ${DATAHUB_GMS_TOKEN}
```

## Behavior

When a documentation change is detected on a schema field:

1. The action checks if propagation is enabled and if the change should be propagated
2. It determines the appropriate propagation relationships based on your configuration
3. It finds related schema fields based on those relationships
4. It propagates the documentation to those fields, preserving attribution information
5. It respects propagation limits (depth, fanout, time) to prevent excessive propagation

## Propagation Attribution

When documentation is propagated, the action adds attribution metadata to track:

- The original source of the documentation
- The propagation path
- The time of propagation
- The depth of propagation

This attribution information is stored with the propagated documentation and can be viewed in the DataHub UI.

## Caveats and Limitations

- **Upstream Propagation**: When propagating upstream, the action only propagates if there is exactly one upstream field. This prevents ambiguous propagation when multiple upstream fields exist.
- **Dataset Documentation**: Dataset-level documentation propagation is not currently supported, only schema field (column) documentation.
- **Single Upstream Field**: For downstream propagation, the action only propagates to a downstream field if it has exactly one upstream field (the field being propagated from). This ensures that documentation is only propagated in clear one-to-one relationships.
