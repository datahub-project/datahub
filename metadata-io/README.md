# Metadata IO Module

This module contains the core metadata I/O services for DataHub, including system information collection and property management.

## Security: Configuration Property Classification

**Critical Test**: `PropertiesCollectorConfigurationTest` enforces that all configuration properties are explicitly classified as either sensitive (redacted) or non-sensitive (visible in system info).

**Why**: Prevents accidental exposure of secrets through DataHub's system information endpoints.

**When adding new properties**: The test will fail with instructions on which classification list to add your property to. The test file contains comprehensive documentation on:

- The four classification lists (sensitive/non-sensitive, exact/template)
- Template syntax for dynamic properties (`[*]` for indices, `*` for segments)
- Security guidelines and examples

**Test Command**:

```bash
./gradlew :metadata-io:test --tests "*.PropertiesCollectorConfigurationTest"
```

**Security Rule**: When in doubt, classify as sensitive. This test is a mandatory security guardrail - never disable it.
