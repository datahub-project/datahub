# Assertion Specification

**Part of**: [RDF Specification](../../../../docs/rdf-specification.md)

This document specifies how RDF SHACL constraints are extracted and converted to DataHub assertion entities.

## Overview

Data quality assertions are automatically generated from SHACL (Shapes Constraint Language) constraints defined in dataset schemas. Assertions provide runtime validation rules that DataHub can execute to verify data quality.

**Note**: Assertions are **disabled by default**. They must be explicitly enabled via context configuration:

- `create_assertions: bool = True` (main flag)
- `assertion_types: dict` (optional sub-flags for fine-grained control)

## RDF Source Patterns

Assertions are extracted from SHACL constraints in dataset schemas:

### Schema Linking Patterns

Assertions are extracted from SHACL shapes linked to datasets via:

1. **Direct Property Constraints** (inline on dataset):

```turtle
ex:TradeDataset a dcat:Dataset ;
    sh:property [
        sh:path ex:tradeId ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:minLength 10 ;
        sh:maxLength 20
    ] .
```

2. **NodeShape Reference** (via `dcterms:conformsTo`):

```turtle
ex:TradeDataset a dcat:Dataset ;
    dcterms:conformsTo ex:TradeSchema .

ex:TradeSchema a sh:NodeShape ;
    sh:property [
        sh:path ex:tradeId ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] .
```

3. **Target Class Pattern** (via `sh:targetClass`):

```turtle
ex:TradeSchema a sh:NodeShape ;
    sh:targetClass dcat:Dataset ;
    sh:property [
        sh:path ex:tradeId ;
        sh:minCount 1
    ] .
```

### Constraint Types and Assertion Mapping

| SHACL Constraint                       | DataHub Assertion Type | Operator                | Description                 |
| -------------------------------------- | ---------------------- | ----------------------- | --------------------------- |
| `sh:minCount >= 1` + `sh:maxCount = 1` | `FIELD_METRIC`         | `NOT_NULL`              | Required single-value field |
| `sh:minCount >= 1` + `sh:maxCount > 1` | `FIELD_METRIC`         | `GREATER_THAN_OR_EQUAL` | Required with minimum count |
| `sh:minCount` + `sh:maxCount > 1`      | `FIELD_METRIC`         | `BETWEEN`               | Cardinality constraint      |
| `sh:minLength`                         | `FIELD_VALUES`         | `GREATER_THAN_OR_EQUAL` | Minimum string length       |
| `sh:maxLength`                         | `FIELD_VALUES`         | `LESS_THAN_OR_EQUAL`    | Maximum string length       |
| `sh:pattern`                           | `FIELD_VALUES`         | `MATCHES`               | Regular expression pattern  |
| `sh:minInclusive`                      | `FIELD_METRIC`         | `GREATER_THAN_OR_EQUAL` | Minimum numeric value       |
| `sh:maxInclusive`                      | `FIELD_METRIC`         | `LESS_THAN_OR_EQUAL`    | Maximum numeric value       |

### Field Name Resolution

Field names are extracted in priority order:

1. `sh:path` - Direct path property
2. `sh:node` - Referenced node URI (local name extracted)
3. `sh:name` - Explicit name property

### Constraint Source Resolution

When a property shape uses `sh:node` to reference another shape, constraints are checked in **both**:

- The inline property shape
- The referenced `sh:node` shape

This allows constraints to be defined on the referenced glossary term (dual-typed as `skos:Concept, sh:PropertyShape`).

## Configuration

Assertions are controlled via context configuration:

```python
context = {
    'create_assertions': True,  # Main flag (default: False)
    'assertion_types': {
        'required_fields': True,   # minCount/maxCount → NOT_NULL (default: True when enabled)
        'field_size': True,        # minLength/maxLength (default: True when enabled)
        'value_checks': True       # minInclusive/maxInclusive, pattern (default: True when enabled)
    }
}
```

**Default Behavior**:

- If `create_assertions=True` and `assertion_types` is empty or not provided, **all assertion types are enabled**
- Individual assertion types default to `True` when `create_assertions=True`

## Assertion Types

### Required Field Assertions

Created from `sh:minCount` constraints:

- **`minCount >= 1` + `maxCount = 1`** → `NOT_NULL` assertion (required single-value)
- **`minCount >= 1` + `maxCount > 1`** → `GREATER_THAN_OR_EQUAL` with minimum count
- **`minCount >= 1` + `maxCount > 1`** → Additional `BETWEEN` assertion for cardinality

**Example**:

```turtle
ex:Schema a sh:NodeShape ;
    sh:property [
        sh:path ex:accountId ;
        sh:minCount 1 ;
        sh:maxCount 1
    ] .
```

Creates: `FIELD_METRIC` assertion with operator `NOT_NULL` for field `accountId`.

### Field Size Assertions

Created from `sh:minLength` and `sh:maxLength` constraints:

**Example**:

```turtle
ex:Schema a sh:NodeShape ;
    sh:property [
        sh:path ex:customerName ;
        sh:minLength 3 ;
        sh:maxLength 100
    ] .
```

Creates:

- `FIELD_VALUES` assertion with operator `GREATER_THAN_OR_EQUAL` (minLength: 3)
- `FIELD_VALUES` assertion with operator `LESS_THAN_OR_EQUAL` (maxLength: 100)

### Value Check Assertions

Created from `sh:minInclusive`, `sh:maxInclusive`, and `sh:pattern` constraints:

**Example**:

```turtle
ex:Schema a sh:NodeShape ;
    sh:property [
        sh:path ex:riskWeight ;
        sh:minInclusive 0.0 ;
        sh:maxInclusive 100.0 ;
        sh:pattern "^\\d{1,3}\\.\\d{2}$"
    ] .
```

Creates:

- `FIELD_METRIC` assertion with operator `GREATER_THAN_OR_EQUAL` (minValue: 0.0)
- `FIELD_METRIC` assertion with operator `LESS_THAN_OR_EQUAL` (maxValue: 100.0)
- `FIELD_VALUES` assertion with operator `MATCHES` (pattern: `^\\d{1,3}\\.\\d{2}$`)

## DataHub Integration

### Assertion Key Generation

Assertion keys are generated as: `{dataset_urn}_{field_name}_{constraint_type}`

Examples:

- `urn:li:dataset:(postgres,accounts,PROD)_accountId_not_null`
- `urn:li:dataset:(postgres,accounts,PROD)_customerName_min_length`
- `urn:li:dataset:(postgres,accounts,PROD)_riskWeight_pattern`

### Assertion Structure

```python
DataHubAssertion(
    assertion_key="...",
    assertion_type="FIELD_METRIC" | "FIELD_VALUES" | "DATASET" | "SCHEMA",
    dataset_urn="urn:li:dataset:(...)",
    field_name="accountId",
    description="Field accountId is required",
    operator="NOT_NULL" | "GREATER_THAN_OR_EQUAL" | "LESS_THAN_OR_EQUAL" | "MATCHES" | "BETWEEN",
    parameters={'minCount': 1, 'maxCount': 1}
)
```

## Limitations

1. **Standalone NodeShapes**: NodeShapes without platform associations (not linked to datasets) are skipped. They cannot create valid assertions without a dataset URN.

2. **Datatype Constraints**: `sh:datatype` constraints are **not** converted to assertions. Datatypes are schema information handled during field creation, not data quality assertions.

3. **Optional Fields**: Fields with `minCount=0` do not generate assertions (they are optional).

4. **Cross-Field Constraints**: Complex cross-field constraints (e.g., `sh:lessThan`, `sh:notEquals`) are not currently extracted as assertions.

## Platform Requirements

Assertions require a valid dataset URN, which requires platform information. Datasets without explicit platforms default to `"logical"` platform, which is sufficient for assertion creation.
