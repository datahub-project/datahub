# Glossary Term Propagation Action

The Glossary Term Propagation Action allows you to automatically propagate glossary terms from schema fields to related schema fields. When a glossary term is applied to a column in a dataset, this action can automatically propagate that term to downstream columns based on lineage relationships, ensuring consistent terminology across your data ecosystem.

## Functionality

This action listens for glossary term changes on schema fields and propagates those changes to related fields based on your configuration. It currently supports:

- **Downstream Propagation**: Propagate glossary terms to columns in downstream datasets

## Configuration Options

The Glossary Term Propagation Action provides several configuration options:

- `enabled`: Controls whether glossary term propagation is enabled (default: `true`)
- `columns_enabled`: Controls whether column-level glossary term propagation is enabled (default: `true`)
- `allowed_terms`: Optional list of glossary term URNs to restrict which terms are propagated
- `max_propagation_depth`: Maximum depth for propagation chains (default: `5`)
- `max_propagation_fanout`: Maximum number of entities to propagate to in a single hop (default: `1000`)
- `max_propagation_time_millis`: Maximum time in milliseconds for a propagation chain (default: `3600000` - 1 hour)
- `column_propagation_relationships`: Specifies which relationships to use for propagation. Currently only supports `DOWNSTREAM`

## Example Configuration

```yaml
name: "schemaField-glossary-term-propagation"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${KAFKA_SCHEMAREGISTRY_URL}
      # Dictionary of freeform consumer configs propagated to underlying Kafka Consumer
      consumer_config:
        security.protocol: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:-SSL}
        ssl.ca.location: ${KAFKA_PROPERTIES_SSL_CA_LOCATION:-/path/to/ca.pem}
        ssl.certificate.location: ${KAFKA_PROPERTIES_SSL_CERTIFICATE_LOCATION:-/path/to/cert.pem}
        ssl.key.location: ${KAFKA_PROPERTIES_SSL_KEY_LOCATION:-/path/to/key.pem}
        ssl.key.password: ${KAFKA_PROPERTIES_SSL_KEY_PASSWORD:-dummy_password}
    # Topic Routing - which topics to read from.
    topic_routes:
      mcl: ${METADATA_CHANGE_LOG_VERSIONED_TOPIC_NAME:-MetadataChangeLog_Versioned_v1}
      pe: ${PLATFORM_EVENT_TOPIC_NAME:PlatformEvent_v1}
action:
  type: "schema_field_glossary_term_propagation"
  config:
    allowed_terms:
      - "urn:li:glossaryTerm:some-glossary"
datahub:
  server: ${DATAHUB_GMS_HOST}
  token: ${DATAHUB_GMS_TOKEN}
```

## Caveats and Limitations

- **Downstream Only**: Currently only supports downstream propagation (not upstream or sibling)
- **Single Upstream Field**: Only propagates to a downstream field if it has exactly one upstream field (the field being propagated from). This ensures that terms are only propagated in clear one-to-one relationships.
- **Complex Scenarios**: Does not handle complex propagation scenarios (e.g., merging terms from multiple sources)
