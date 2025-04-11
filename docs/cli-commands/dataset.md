# DataHub Dataset Command

The `dataset` command allows you to interact with Dataset entities in DataHub. This includes creating, updating, retrieving, validating, and synchronizing Dataset metadata.

## Commands

### sync

Synchronize Dataset metadata between YAML files and DataHub.

```shell
datahub dataset sync -f PATH_TO_YAML_FILE --to-datahub|--from-datahub
```

**Options:**
- `-f, --file` - Path to the YAML file (required)
- `--to-datahub` - Push metadata from YAML file to DataHub
- `--from-datahub` - Pull metadata from DataHub to YAML file

**Example:**
```shell
# Push to DataHub
datahub dataset sync -f dataset.yaml --to-datahub

# Pull from DataHub
datahub dataset sync -f dataset.yaml --from-datahub
```

The `sync` command offers bidirectional synchronization, allowing you to keep your local YAML files in sync with the DataHub platform. The `upsert` command actually uses `sync` with the `--to-datahub` flag internally.

For details on the supported YAML format, see the [Dataset YAML Format](#dataset-yaml-format) section.

### file

Operate on a Dataset YAML file for validation or linting.

```shell
datahub dataset file [--lintCheck] [--lintFix] PATH_TO_YAML_FILE
```

**Options:**
- `--lintCheck` - Check the YAML file for formatting issues (optional)
- `--lintFix` - Fix formatting issues in the YAML file (optional)

**Example:**
```shell
# Check for linting issues
datahub dataset file --lintCheck dataset.yaml

# Fix linting issues
datahub dataset file --lintFix dataset.yaml
```

This command helps maintain consistent formatting of your Dataset YAML files. For more information on the expected format, refer to the [Dataset YAML Format](#dataset-yaml-format) section.

### upsert

Create or update Dataset metadata in DataHub.

```shell
datahub dataset upsert -f PATH_TO_YAML_FILE
```

**Options:**
- `-f, --file` - Path to the YAML file containing Dataset metadata (required)

**Example:**
```shell
datahub dataset upsert -f dataset.yaml
```

This command will parse the YAML file, validate that any entity references exist in DataHub, and then emit the corresponding metadata change proposals to update or create the Dataset.

For details on the required structure of your YAML file, see the [Dataset YAML Format](#dataset-yaml-format) section.

### get

Retrieve Dataset metadata from DataHub and optionally write it to a file.

```shell
datahub dataset get --urn DATASET_URN [--to-file OUTPUT_FILE]
```

**Options:**
- `--urn` - The Dataset URN to retrieve (required)
- `--to-file` - Path to write the Dataset metadata as YAML (optional)

**Example:**
```shell
datahub dataset get --urn "urn:li:dataset:(urn:li:dataPlatform:hive,example_table,PROD)" --to-file my_dataset.yaml
```

If the URN does not start with `urn:li:dataset:`, it will be automatically prefixed.

The output file will be formatted according to the [Dataset YAML Format](#dataset-yaml-format) section.

### add_sibling

Add sibling relationships between Datasets.

```shell
datahub dataset add_sibling --urn PRIMARY_URN --sibling-urns SECONDARY_URN [--sibling-urns ANOTHER_URN ...]
```

**Options:**
- `--urn` - URN of the primary Dataset (required)
- `--sibling-urns` - URNs of secondary sibling Datasets (required, multiple allowed)

**Example:**
```shell
datahub dataset add_sibling --urn "urn:li:dataset:(urn:li:dataPlatform:hive,example_table,PROD)" --sibling-urns "urn:li:dataset:(urn:li:dataPlatform:snowflake,example_table,PROD)"
```

Siblings are semantically equivalent datasets, typically representing the same data across different platforms or environments.

## Dataset YAML Format

The Dataset YAML file follows a structured format with various supported fields:

```yaml
# Basic identification (required)
id: "example_table"                # Dataset identifier
platform: "hive"                   # Platform name
env: "PROD"                        # Environment (PROD by default)

# Metadata (optional)
name: "Example Table"              # Display name (defaults to id if not specified)
description: "This is an example table"

# Schema definition (optional)
schema:
  fields:
    - id: "field1"                 # Field identifier
      type: "string"               # Data type
      description: "First field"   # Field description
      doc: "First field"           # Alias for description
      nativeDataType: "VARCHAR"    # Native platform type (defaults to type if not specified)
      nullable: false              # Whether field can be null (default: false)
      label: "Field One"           # Display label (optional business label for the field)
      isPartOfKey: true            # Whether field is part of primary key
      isPartitioningKey: false     # Whether field is a partitioning key
      jsonProps: {"customProp": "value"} # Custom JSON properties
      
    - id: "field2"
      type: "number"
      description: "Second field"
      nullable: true
      globalTags: ["PII", "Sensitive"]
      glossaryTerms: ["urn:li:glossaryTerm:Revenue"]
      structured_properties:
        property1: "value1"
        property2: 42
  file: example.schema.avsc      # Optional schema file (required if defining tables with nested fields)

# Additional metadata (all optional)
properties:                        # Custom properties as key-value pairs
  origin: "external"
  pipeline: "etl_daily"

subtype: "View"                    # Dataset subtype
subtypes: ["View", "Materialized"] # Multiple subtypes (if only one, use subtype field instead)

downstreams:                       # Downstream Dataset URNs
  - "urn:li:dataset:(urn:li:dataPlatform:hive,downstream_table,PROD)"

tags:                              # Tags
  - "Tier1"
  - "Verified"

glossary_terms:                    # Associated glossary terms
  - "urn:li:glossaryTerm:Revenue"

owners:                            # Dataset owners
  - "jdoe"                         # Simple format (defaults to TECHNICAL_OWNER)
  - id: "alice"                    # Extended format with ownership type
    type: "BUSINESS_OWNER"

structured_properties:             # Structured properties
  priority: "P1" 
  cost_center: 123
  
external_url: "https://example.com/datasets/example_table"
```

You can also define multiple datasets in a single YAML file by using a list format:

```yaml
- id: "dataset1"
  platform: "hive"
  description: "First dataset"
  # other properties...

- id: "dataset2"
  platform: "snowflake"
  description: "Second dataset"
  # other properties...
```

### Schema Definition

You can define Dataset schema in two ways:

1. **Direct field definitions** as shown above
   > **Important limitation**: When using inline schema field definitions, only non-nested (flat) fields are currently supported. For nested or complex schemas, you must use the Avro file approach described below.

2. **Reference to an Avro schema file**:
   ```yaml
   schema:
     file: "path/to/schema.avsc"
   ```

Even when using the Avro file approach for the basic schema structure, you can still use the `fields` section to provide additional metadata like structured properties, tags, and glossary terms for your schema fields.

#### Schema Field Properties

The Schema Field object supports the following properties:

| Property | Type | Description |
|----------|------|-------------|
| `id` | string | Field identifier/path (required if `urn` not provided) |
| `urn` | string | URN of the schema field (required if `id` not provided) |
| `type` | string | Data type (one of the supported [Field Types](#field-types)) |
| `nativeDataType` | string | Native data type in the source platform (defaults to `type` if not specified) |
| `description` | string | Field description |
| `doc` | string | Alias for description |
| `nullable` | boolean | Whether the field can be null (default: false) |
| `label` | string | Display label for the field |
| `recursive` | boolean | Whether the field is recursive (default: false) |
| `isPartOfKey` | boolean | Whether the field is part of the primary key |
| `isPartitioningKey` | boolean | Whether the field is a partitioning key |
| `jsonProps` | object | Custom JSON properties |
| `globalTags` | array | List of tags associated with the field |
| `glossaryTerms` | array | List of glossary terms associated with the field |
| `structured_properties` | object | Structured properties for the field |


**Important Note on Schema Field Types**:
When specifying fields in the YAML file, you must follow an all-or-nothing approach with the `type` field:
- If you want the command to generate the schema for you, specify the `type` field for ALL fields.
- If you only want to add field-level metadata (like tags, glossary terms, or structured properties), do NOT specify the `type` field for ANY field.

Example of fields with only metadata (no types):
```yaml
schema:   
  fields:     
    - id: "field1"                 # Field identifier
      structured_properties:
        prop1: prop_value
    - id: "field2"
      structured_properties:
        prop1: prop_value
```

### Ownership Types

When specifying owners, the following ownership types are supported:
- `TECHNICAL_OWNER` (default)
- `BUSINESS_OWNER`
- `DATA_STEWARD`

Custom ownership types can be specified using the URN format.

### Field Types

When defining schema fields, the following primitive types are supported:
- `string`
- `number`
- `int`
- `long`
- `float`
- `double`
- `boolean`
- `bytes`
- `fixed`

## Implementation Notes

- URNs are generated automatically if not provided, based on the platform, id, and env values
- The command performs validation to ensure referenced entities (like structured properties) exist
- When updating schema fields, changes are propagated correctly to maintain consistent metadata
- The Dataset object will check for existence of entity references and will skip datasets with missing references
- When using the `sync` command with `--from-datahub`, existing YAML files will be updated with metadata from DataHub while preserving comments and structure
- For structured properties, single values are simplified (not wrapped in lists) when appropriate
- Field paths are simplified for better readability
- When specifying field types, all fields must have type information or none of them should