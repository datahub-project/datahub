# Pentaho Source

## Overview

The Pentaho source extracts metadata from Pentaho Kettle files (.ktr transformation files and .kjb job files) and creates DataHub entities for jobs with table-level lineage information.

## Supported Features

- **DataJob Metadata**: Extracts job name, type, description, and custom properties from Pentaho files
- **Table-level Lineage**: Creates lineage from `TableInput` steps (sources) to `TableOutput` steps (destinations)
- **Variable Resolution**: Optionally resolves Pentaho variables in table names and SQL queries
- **Configurable Database Mapping**: Maps database connection names to DataHub platform names
- **File Pattern Filtering**: Supports filtering files by name patterns
- **Stateful Ingestion**: Supports deletion detection for removed files

## Limitations

- **Step Coverage**: Currently only handles `TableInput` and `TableOutput` steps
- **Column-level Lineage**: Not implemented yet
- **Variable Resolution**: Limited to simple variable substitution; complex expressions not supported
- **SQL Parsing**: Variable resolution in SQL queries may fail for complex cases

## Configuration

### Basic Configuration

```yaml
source:
  type: pentaho
  config:
    kettle_file_paths:
      - "/path/to/pentaho/transformations/*.ktr"
      - "/path/to/pentaho/jobs/*.kjb"
    database_mapping:
      mysql_conn: mysql
      postgres_conn: postgres
      oracle_conn: oracle
```

### Advanced Configuration

```yaml
source:
  type: pentaho
  config:
    # Required: List of file paths or glob patterns
    kettle_file_paths:
      - "/opt/pentaho/transformations/**/*.ktr"
      - "/opt/pentaho/jobs/**/*.kjb"
    
    # Database connection mapping
    database_mapping:
      mysql_prod: mysql
      postgres_warehouse: postgres
      oracle_legacy: oracle
    
    # Default platform for unmapped connections
    default_database_platform: "unknown"
    
    # File filtering
    file_pattern:
      allow:
        - "prod_*"
        - "etl_*"
      deny:
        - "*_test*"
        - "*_backup*"
    
    # Feature toggles
    include_lineage: true
    include_job_metadata: true
    
    # Variable resolution
    resolve_variables: true
    variable_values:
      schema: "production"
      environment: "prod"
      tablePrefix: "fact_"
    
    # Customization
    job_browse_path_template: "/pentaho/{job_type}/{file_name}"
    custom_properties_prefix: "pentaho."
    
    # DataHub common configs
    env: "PROD"
    platform_instance: "pentaho-prod"
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `kettle_file_paths` | List[str] | Required | List of file paths or glob patterns to Kettle files |
| `database_mapping` | Dict[str, str] | `{}` | Maps database connection names to DataHub platform names |
| `default_database_platform` | str | `"unknown"` | Default platform for unmapped database connections |
| `file_pattern` | AllowDenyPattern | Allow all | Pattern to filter files by name |
| `include_lineage` | bool | `true` | Whether to extract lineage information |
| `include_job_metadata` | bool | `true` | Whether to extract job metadata |
| `resolve_variables` | bool | `false` | Whether to resolve Pentaho variables |
| `variable_values` | Dict[str, str] | `{}` | Variable values for resolution |
| `job_browse_path_template` | str | `"/pentaho/{job_type}/{file_name}"` | Template for job browse paths |
| `custom_properties_prefix` | str | `"pentaho."` | Prefix for custom properties |

## Pentaho File Structure

### Transformation Files (.ktr)

Transformation files contain ETL logic with steps connected by hops. The source extracts:

- **TableInput steps**: Source tables and SQL queries
- **TableOutput steps**: Destination tables and schemas
- **Metadata**: Job name, description, creator, dates

### Job Files (.kjb)

Job files contain orchestration logic with job entries. The source extracts:

- **SQL entries**: SQL scripts and statements
- **Transformation entries**: References to .ktr files
- **Metadata**: Job name, description, creator, dates

## Lineage Extraction

### TableInput Steps

For each `TableInput` step, the source:

1. Extracts the SQL query if present
2. Uses DataHub's SQL parser to identify source tables
3. Falls back to explicit table name if SQL parsing fails
4. Maps database connection to DataHub platform
5. Creates dataset URNs for source tables

### TableOutput Steps

For each `TableOutput` step, the source:

1. Extracts the target table name and schema
2. Maps database connection to DataHub platform
3. Creates dataset URNs for destination tables

### Lineage Creation

The source creates lineage edges from all input datasets to all output datasets within a job. This is a simplified approach - in complex transformations, the actual data flow might be more nuanced.

## Variable Resolution

When `resolve_variables` is enabled, the source can substitute Pentaho variables in:

- Table names in `TableOutput` steps
- Schema names
- SQL queries in `TableInput` steps

Variables follow the format `${variable_name}` and are replaced with values from the `variable_values` configuration.

### Example

Configuration:
```yaml
resolve_variables: true
variable_values:
  schema: "production"
  table_prefix: "fact_"
```

Pentaho file:
```xml
<table>${table_prefix}sales</table>
<schema>${schema}</schema>
```

Resolved:
- Table: `fact_sales`
- Schema: `production`

## Custom Properties

The source extracts various properties from Pentaho files and adds them as custom properties to DataHub jobs:

### From Transformation Info
- `pentaho.created_user`: User who created the transformation
- `pentaho.created_date`: Creation date
- `pentaho.modified_user`: User who last modified
- `pentaho.modified_date`: Last modification date
- `pentaho.size_rowset`: Row set size

### From Job Info
- `pentaho.created_user`: User who created the job
- `pentaho.created_date`: Creation date
- `pentaho.modified_user`: User who last modified
- `pentaho.modified_date`: Last modification date

### Generated Properties
- `pentaho.file_path`: Full path to the Kettle file
- `pentaho.file_type`: File type (transformation or job)
- `pentaho.steps.{step_type}`: Count of each step type

## Error Handling

The source includes comprehensive error handling:

- **File Parsing Errors**: Logged and reported, processing continues with other files
- **SQL Parsing Errors**: Logged, falls back to table name extraction
- **Variable Resolution Errors**: Logged, variables remain unresolved
- **Lineage Extraction Errors**: Logged, job metadata still extracted

## Monitoring and Reporting

The source provides detailed reporting through the `PentahoSourceReport`:

### File Processing
- Files processed, failed, and skipped
- Breakdown by file type (.ktr vs .kjb)

### Job Processing
- Jobs processed
- Jobs with and without lineage
- Step counts by type

### Lineage Statistics
- Lineage edges created and failed
- Database connections found
- Unresolved variables

### Error Tracking
- Parse errors
- SQL parsing errors
- Variable resolution errors
- Lineage extraction errors

## Usage Examples

### Basic Usage

```bash
datahub ingest -c pentaho_config.yml
```

### With Custom Variables

```yaml
# pentaho_config.yml
source:
  type: pentaho
  config:
    kettle_file_paths:
      - "/data/pentaho/**/*.ktr"
    database_mapping:
      prod_mysql: mysql
      warehouse_pg: postgres
    resolve_variables: true
    variable_values:
      env: "production"
      schema: "public"
```

### Production Setup

```yaml
source:
  type: pentaho
  config:
    kettle_file_paths:
      - "/opt/pentaho/pdi/transformations/**/*.ktr"
      - "/opt/pentaho/pdi/jobs/**/*.kjb"
    database_mapping:
      mysql_prod: mysql
      postgres_dw: postgres
      oracle_erp: oracle
    file_pattern:
      allow: ["prod_*", "etl_*"]
      deny: ["*_test*", "*_dev*"]
    env: "PROD"
    platform_instance: "pentaho-production"
    stateful_ingestion:
      enabled: true
```

## Troubleshooting

### Common Issues

1. **No files found**: Check file paths and permissions
2. **XML parsing errors**: Ensure files are valid XML
3. **Missing lineage**: Verify database mapping configuration
4. **Variable resolution failures**: Check variable names and values

### Debug Mode

Enable debug logging to see detailed processing information:

```yaml
# In your ingestion config
sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
    
# Enable debug logging
logging:
  level: DEBUG
  handlers:
    - type: console
```

## Contributing

To contribute to the Pentaho source:

1. Add support for additional step types in `parser.py`
2. Enhance SQL parsing capabilities in `lineage_extractor.py`
3. Add column-level lineage support
4. Improve variable resolution for complex expressions

## Future Enhancements

- **Column-level Lineage**: Extract field mappings from transformation steps
- **Additional Step Types**: Support more Pentaho step types beyond TableInput/TableOutput
- **Enhanced SQL Parsing**: Better handling of complex SQL with variables
- **Job Dependencies**: Extract dependencies between jobs and transformations
- **Execution Metadata**: Integration with Pentaho execution logs