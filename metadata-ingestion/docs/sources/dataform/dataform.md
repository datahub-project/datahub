### Dataform Tag and Property Mappings

Dataform allows users to define tags and custom properties for datasets through its configuration system. Our Dataform source allows users to define actions such as add a tag, term or owner based on these properties. For example, if a Dataform table has a tag `"pii"`, we can define an action that adds a corresponding DataHub tag.

To leverage this feature, users can define custom mappings as part of the recipe configuration. The following section describes how you can build these mappings using the `custom_properties` and tag-based filtering.

```yaml
# Example of tag-based filtering and custom properties
tag_pattern:
  allow:
    - "production"
    - "critical"
    - "pii"

custom_properties:
  team: "data-engineering"
  environment: "production"
  compliance_level: "high"
```

### Sibling Relationships

The Dataform connector supports sibling relationships between Dataform entities and target platform entities, similar to dbt. This feature allows you to control which entities are considered primary (authoritative) in DataHub.

```yaml
# Configure sibling relationships
dataform_is_primary_sibling: true   # Default: Dataform entities are primary
# OR
dataform_is_primary_sibling: false  # Target platform entities are primary
```

**When `dataform_is_primary_sibling: true` (default):**

- Dataform entities are the primary source of truth
- Target platform entities are secondary
- Server-side hooks automatically manage relationships

**When `dataform_is_primary_sibling: false`:**

- Target platform entities are the primary source of truth
- Dataform entities are secondary
- Uses patch-based management for precise control

### Lineage Extraction

The Dataform connector automatically extracts lineage information from:

1. **Dataform Dependencies**: Direct dependencies declared in Dataform workflows
2. **SQL Query Analysis**: When `parse_table_names_from_sql` is enabled
3. **Cross-project References**: References to external datasets

### Git Integration

When configured, the connector adds links to your source code repository:

```yaml
git_repository_url: "https://github.com/your-org/dataform-project"
git_branch: "main"
```

This creates direct links from DataHub entities to their corresponding Dataform source files, enabling easy navigation between documentation and code.

### Advanced Configuration

#### Entity Filtering

Control which Dataform entities are ingested:

```yaml
entities_enabled:
  tables: true # Dataform tables and incremental models
  views: true # Dataform views
  assertions: true # Data quality tests
  operations: true # Custom SQL operations
  declarations: true # External data sources
```

#### Pattern-based Filtering

Fine-tune ingestion with regex patterns:

```yaml
table_pattern:
  allow:
    - "analytics\\..*"
    - "marts\\..*"
  deny:
    - ".*_temp"
    - ".*_test"

schema_pattern:
  allow:
    - "production"
    - "staging"
```

### Authentication

#### Google Cloud Dataform

For Google Cloud Dataform, use structured GCP credentials:

```yaml
cloud_config:
  credential:
    project_id: "my-project"
    private_key: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
    client_email: "service-account@my-project.iam.gserviceaccount.com"
    client_id: "123456789"
    # ... other service account fields
```

Or use legacy authentication methods:

```yaml
cloud_config:
  service_account_key_file: "/path/to/service-account.json"
  # OR
  credentials_json: '{"type": "service_account", ...}'
```

#### Dataform Core

For local Dataform Core projects, no authentication is required:

```yaml
core_config:
  project_path: "/path/to/dataform/project"
  target_name: "production" # Optional
```

### Best Practices

1. **Run Both Connectors**: Always run both Dataform and target platform connectors for complete metadata coverage
2. **Use Sibling Relationships**: Configure `dataform_is_primary_sibling` based on your organization's data governance model
3. **Enable Git Integration**: Link to source code for better developer experience
4. **Filter Appropriately**: Use pattern-based filtering to avoid ingesting temporary or test entities
5. **Monitor Compilation**: Ensure Dataform compilation succeeds before running ingestion

### Troubleshooting

#### Common Issues

1. **Compilation Failures**: Ensure your Dataform project compiles successfully
2. **Authentication Errors**: Verify GCP credentials and permissions for Cloud Dataform
3. **Missing Entities**: Check filtering patterns and entity type configurations
4. **Lineage Issues**: Enable SQL parsing for additional lineage detection

#### Debug Configuration

Enable detailed logging for troubleshooting:

```yaml
# Add to your recipe
pipeline_name: "dataform-debug"
run_id: "debug-run"
# Enable debug logging in your logging configuration
```

### Migration from Other Tools

#### From dbt

If migrating from dbt, the Dataform connector provides similar functionality:

- **Sibling relationships** work the same way as dbt
- **Lineage extraction** follows similar patterns
- **Entity filtering** uses the same configuration structure
- **Git integration** provides the same source code linking

#### Configuration Mapping

| dbt Config               | Dataform Equivalent           | Notes                                   |
| ------------------------ | ----------------------------- | --------------------------------------- |
| `dbt_is_primary_sibling` | `dataform_is_primary_sibling` | Same functionality                      |
| `node_name_pattern`      | `table_pattern`               | Similar regex filtering                 |
| `tag_prefix`             | `tag_prefix`                  | Same tag prefixing                      |
| `meta_mapping`           | `custom_properties`           | Different approach, but similar outcome |
