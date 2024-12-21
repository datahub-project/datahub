# SchemaTron (Incubating)

> ⚠️ This is an incubating project in draft status. APIs and functionality may change significantly between releases.

SchemaTron is a schema translation toolkit that converts between various schema formats and DataHub's native schema representation. It currently provides robust support for Apache Avro schema translation with a focus on complex schema structures including unions, arrays, maps, and nested records.

## Modules

### CLI Module

Command-line interface for converting schemas and emitting them to DataHub.

```bash
# Execute from this directory
../../../gradlew :metadata-integration:java:datahub-schematron:cli:run --args="-i cli/src/test/resources/FlatUser.avsc"
```

#### CLI Options

- `-i, --input`: Input schema file or directory path
- `-p, --platform`: Data platform name (default: "avro")
- `-s, --server`: DataHub server URL (default: "http://localhost:8080")
- `-t, --token`: DataHub access token
- `--sink`: Output sink - "rest" or "file" (default: "rest")
- `--output-file`: Output file path when using file sink (default: "metadata.json")

### Library Module

Core translation logic and models for schema conversion. Features include:

- Support for complex Avro schema structures:
  - Union types with multiple record options
  - Nested records and arrays
  - Optional fields with defaults
  - Logical types (date, timestamp, etc.)
  - Maps with various value types
  - Enum types
  - Custom metadata and documentation

- Comprehensive path handling for schema fields
- DataHub-compatible metadata generation
- Schema fingerprinting and versioning

## Example Schema Support

The library can handle sophisticated schema structures including:

- Customer profiles with multiple identification types (passport, driver's license, national ID)
- Contact information with primary and alternative contact methods
- Address validation with verification metadata
- Subscription history tracking
- Flexible preference and metadata storage
- Tagged customer attributes

## Development

The project includes extensive test coverage through:

- Unit tests for field path handling
- Schema translation comparison tests
- Integration tests with Python reference implementation

Test resources include example schemas demonstrating various Avro schema features and edge cases.

## Contributing

As this is an incubating project, we welcome contributions and feedback on:

- Additional schema format support
- Improved handling of complex schema patterns
- Enhanced metadata translation
- Documentation and examples
- Test coverage