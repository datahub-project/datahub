# Assertions Test Suite

This directory contains the test suite for the DataHub Cloud Python SDK assertions module. The tests are organized into focused areas to ensure comprehensive coverage of all assertion functionality.

## Directory Structure

### `/assertion/`

**Purpose**: Tests for assertion classes and their behavior

- **What goes here**: Tests for assertion class creation, property access, validation, and entity conversion
- **Focus**: Public API behavior, data validation, readonly properties, and class instantiation

### `/assertion_input/`

**Purpose**: Tests for assertion input classes and validation logic

- **What goes here**: Tests for input validation, parameter parsing, entity creation, and configuration validation
- **Focus**: Input sanitization, type conversion, validation rules, and entity generation

### `/client/`

**Purpose**: Tests for the AssertionsClient and its methods

- **What goes here**: Tests for client operations like create, sync, merge, and CRUD operations
- **Focus**: Client API behavior, error handling, integration scenarios, and workflow testing

### `/entities/`

**Purpose**: Tests for low-level entity classes and serialization

- **What goes here**: Tests for entity creation, serialization/deserialization, and golden file comparisons
- **Focus**: Entity structure validation, JSON serialization, and data integrity

## Current Test Coverage Status

### ✅ Well Tested Areas:

- **Column Metric Assertions (Smart & Native)**: Full coverage across all directories
- **Smart Freshness Assertions**: Complete assertion class tests, comprehensive input validation
- **Freshness Assertions (Native)**: Complete assertion class tests
- **Assertion Input Validation**: Comprehensive coverage for all input types
- **Entity Serialization**: Golden file testing for all entity types
- **Detection Mechanisms**: Full validation and parsing tests

### ⚠️ Partially Tested Areas:

- **Client Operations**: Only volume and column metric sync methods have dedicated tests
- **SQL Assertions**: Basic assertion class tests only
- **Assertion Merging**: Basic tests exist but could be more comprehensive

### ❌ Missing Tests:

- **Smart Volume Assertions**: No assertion class tests (missing test file)
- **Volume Assertions (Native)**: No assertion class tests (missing test file)
- **Most Client Sync Methods**: Missing tests for smart freshness, smart volume, freshness, SQL, and smart column metric sync methods
- **Integration Workflows**: Limited end-to-end testing

## Test Coverage Checklist

**Note**: ✅ = Tests exist and provide good coverage, ⚠️ = Partial coverage, ❌ = Missing or insufficient coverage

### Smart Freshness Assertions

#### `/assertion/` ✅

- [x] Creation with minimal fields
- [x] Creation with all fields
- [x] Property access (readonly validation)
- [x] Entity conversion (`_from_entities`)
- [x] Detection mechanism parsing
- [x] Validation error handling
- [x] Attribute immutability
- [ ] **TODO**: Edge cases for exclusion window parsing
- [ ] **TODO**: Detection mechanism validation for field-based sources

#### `/assertion_input/` ✅

- [x] Input validation (dataset URN, sensitivity, exclusion windows)
- [x] Detection mechanism parsing and validation
- [x] Schedule creation and validation
- [x] Training data lookback days validation
- [x] Incident behavior parsing
- [x] Tag conversion and validation
- [x] Entity generation (assertion + monitor)
- [x] Error handling for invalid inputs
- [ ] **TODO**: Timezone handling in exclusion windows
- [ ] **TODO**: Invalid sensitivity values boundary testing

#### `/client/` ❌

- [ ] `sync_smart_freshness_assertion` method
- [ ] Assertion creation workflow
- [ ] Assertion merging logic
- [ ] Error handling and validation
- [ ] Integration with backend entities
- [ ] **TODO**: Handler delegation tests (verify calls to `smart_freshness_handler`)
- [ ] **TODO**: Deprecated method passthrough tests

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] JSON serialization/deserialization
- [x] Golden file comparisons
- [x] Entity property validation
- [ ] **TODO**: Complex monitor settings serialization

### Smart Volume Assertions

#### `/assertion/` ❌

- [ ] Creation with minimal fields **NO TEST FILE EXISTS**
- [ ] Creation with all fields
- [ ] Property access (readonly validation)
- [ ] Entity conversion (`_from_entities`)
- [ ] Detection mechanism parsing
- [ ] Validation error handling
- [ ] **TODO**: Query detection mechanism with complex filters
- [ ] **TODO**: Dataset profile detection mechanism edge cases

#### `/assertion_input/` ✅

- [x] Input validation (dataset URN, sensitivity, exclusion windows)
- [x] Detection mechanism parsing and validation
- [x] Schedule creation and validation
- [x] Training data lookback days validation
- [x] Incident behavior parsing
- [x] Tag conversion and validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: Volume source type validation

#### `/client/` ❌

- [ ] `sync_smart_volume_assertion` method
- [ ] Assertion creation workflow
- [ ] Assertion merging logic
- [ ] Error handling and validation
- [ ] **TODO**: Handler delegation tests
- [ ] **TODO**: Deprecated method passthrough tests

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] JSON serialization/deserialization
- [x] Golden file comparisons
- [ ] **TODO**: Volume parameters with different source types

### Volume Assertions (Native)

#### `/assertion/` ❌

- [ ] Creation with minimal fields **NO TEST FILE EXISTS**
- [ ] Creation with all fields
- [ ] Property access (readonly validation)
- [ ] Entity conversion (`_from_entities`)
- [ ] Criteria validation
- [ ] Detection mechanism parsing
- [ ] **TODO**: Criteria validation for complex operators (BETWEEN, etc.)

#### `/assertion_input/` ✅

- [x] Criteria validation and parsing
- [x] Detection mechanism validation
- [x] Schedule creation and validation
- [x] Input parameter validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: Criteria parameter type conversion edge cases
- [ ] **TODO**: Invalid operator combinations

#### `/client/` ⚠️

- [x] `sync_volume_assertion` method
- [ ] Assertion creation workflow
- [x] Assertion merging logic
- [ ] Criteria parameter handling
- [ ] **TODO**: Integration tests with real backend entities
- [ ] **TODO**: Merging logic for criteria parameters

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] Volume criteria serialization
- [x] Golden file comparisons
- [ ] **TODO**: Volume criteria with range parameters

### Freshness Assertions (Native)

#### `/assertion/` ✅

- [x] Creation with minimal fields
- [x] Creation with all fields
- [x] Property access (readonly validation)
- [x] Entity conversion (`_from_entities`)
- [x] Schedule type validation
- [x] Lookback window handling
- [ ] **TODO**: Fixed interval schedule edge cases
- [ ] **TODO**: Since-last-check without lookback window

#### `/assertion_input/` ✅

- [x] Schedule type validation
- [x] Lookback window validation
- [x] Detection mechanism validation
- [x] Input parameter validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: Schedule type incompatibility errors
- [ ] **TODO**: Lookback window time unit validation

#### `/client/` ❌

- [ ] `sync_freshness_assertion` method
- [ ] Assertion creation workflow
- [ ] Assertion merging logic
- [ ] Schedule handling
- [ ] **TODO**: Schedule type migration handling
- [ ] **TODO**: Lookback window validation in workflows

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] Schedule serialization
- [x] Golden file comparisons
- [ ] **TODO**: Schedule serialization for different types

### SQL Assertions

#### `/assertion/` ⚠️

- [x] Creation with minimal fields
- [x] Creation with all fields
- [x] Property access (readonly validation)
- [ ] Entity conversion (`_from_entities`) **BASIC TESTS ONLY**
- [ ] SQL statement validation
- [ ] Criteria condition parsing
- [ ] **TODO**: All SQL condition types (growth, absolute, percentage)
- [ ] **TODO**: Parameter validation for range vs single values

#### `/assertion_input/` ✅

- [x] SQL statement validation
- [x] Criteria validation (conditions, parameters)
- [x] Schedule creation and validation
- [x] Input parameter validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: SQL injection prevention tests
- [ ] **TODO**: Invalid SQL syntax handling

#### `/client/` ❌

- [ ] `sync_sql_assertion` method
- [ ] Assertion creation workflow
- [ ] Assertion merging logic
- [ ] SQL criteria handling
- [ ] **TODO**: SQL statement validation and sanitization
- [ ] **TODO**: Complex criteria merging (growth vs absolute conditions)

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] SQL criteria serialization
- [x] Golden file comparisons
- [ ] **TODO**: SQL criteria with change type parameters

### Column Metric Assertions (Smart)

#### `/assertion/` ✅

- [x] Creation with minimal fields
- [x] Creation with all fields
- [x] Property access (readonly validation)
- [x] Entity conversion (`_from_entities`)
- [x] Column metric functionality
- [x] Smart functionality (sensitivity, exclusions)
- [ ] **TODO**: All metric types (NULL_COUNT, UNIQUE_COUNT, etc.)
- [ ] **TODO**: All operators (EQUAL_TO, BETWEEN, etc.)

#### `/assertion_input/` ✅

- [x] Column metric validation
- [x] Operator validation
- [x] Criteria parameters validation
- [x] Smart functionality validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: Criteria parameters with type inference
- [ ] **TODO**: Column name validation

#### `/client/` ❌

- [ ] `sync_smart_column_metric_assertion` method
- [ ] Assertion creation workflow
- [ ] Assertion merging logic
- [ ] Column metric handling
- [ ] **TODO**: Column metric handler delegation
- [ ] **TODO**: Smart functionality integration

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] Column metric serialization
- [x] Golden file comparisons
- [ ] **TODO**: Field metric assertion complex parameters

### Column Metric Assertions (Native)

#### `/assertion/` ✅

- [x] Creation with minimal fields
- [x] Creation with all fields
- [x] Property access (readonly validation)
- [x] Entity conversion (`_from_entities`)
- [x] Column metric functionality
- [x] Criteria parameters handling
- [ ] **TODO**: Range parameter validation
- [ ] **TODO**: Single value parameter validation

#### `/assertion_input/` ✅

- [x] Column metric validation
- [x] Operator validation
- [x] Criteria parameters validation
- [x] Type validation
- [x] Entity generation (assertion + monitor)
- [ ] **TODO**: Parameter type validation (INT, FLOAT, STRING)
- [ ] **TODO**: Operator compatibility with parameter types

#### `/client/` ⚠️

- [x] `sync_column_metric_assertion` method
- [ ] Assertion creation workflow
- [x] Assertion merging logic
- [ ] Parameter type handling
- [ ] **TODO**: Non-smart column metric workflows
- [ ] **TODO**: Type handling for different parameter formats

#### `/entities/` ✅

- [x] Assertion entity structure
- [x] Monitor entity structure
- [x] Column metric serialization
- [x] Golden file comparisons
- [ ] **TODO**: Field assertion with typed parameters

## Cross-Cutting Missing Tests

### Detection Mechanisms (All Assertion Types)

- [ ] **TODO**: Custom detection mechanism validation
- [ ] **TODO**: Detection mechanism serialization/deserialization
- [ ] **TODO**: Additional filter handling in all supported types

### Error Handling (All Areas)

- [ ] **TODO**: Network failure scenarios (Client)
- [ ] **TODO**: Backend validation errors (Client)
- [ ] **TODO**: Concurrent modification handling (Client)
- [ ] **TODO**: Malformed entity recovery (All)

### Integration Testing

- [ ] **TODO**: End-to-end workflows with real backend (Client)
- [ ] **TODO**: Bulk operations (multiple assertions) (Client)
- [ ] **TODO**: Transaction rollback scenarios (Client)
- [ ] **TODO**: Cross-assertion type interactions (All)

### Performance Testing

- [ ] **TODO**: Large dataset handling (Client)
- [ ] **TODO**: Concurrent operation performance (Client)
- [ ] **TODO**: Large exclusion window lists (Input)
- [ ] **TODO**: Large entity serialization (Entities)

### Backward Compatibility

- [ ] **TODO**: Legacy assertion format support (All)
- [ ] **TODO**: API version compatibility (Client)
- [ ] **TODO**: Schema migration tests (Entities)
- [ ] **TODO**: Deprecated parameter handling (Input)

## Common Test Patterns

### Assertion Classes (`/assertion/`)

- **Creation tests**: Verify proper initialization with various parameter combinations
- **Property tests**: Ensure all properties are readonly and return expected values
- **Validation tests**: Test input validation and error handling
- **Entity conversion tests**: Test `_from_entities` class method behavior

### Input Classes (`/assertion_input/`)

- **Input validation**: Test parameter validation and type conversion
- **Entity generation**: Test conversion to DataHub entities
- **Error handling**: Test validation failures and error messages
- **Default handling**: Test default value application

### Client Tests (`/client/`)

- **CRUD operations**: Test create, read, update, delete workflows
- **Merging logic**: Test assertion merging and conflict resolution
- **Error scenarios**: Test client error handling and recovery
- **Integration**: Test end-to-end workflows

### Entity Tests (`/entities/`)

- **Serialization**: Test JSON serialization/deserialization
- **Golden files**: Compare against known-good outputs
- **Structure validation**: Verify entity structure and required fields
- **Data integrity**: Test data preservation through conversions

## Contributing Guidelines

When adding new assertion types or modifying existing ones:

1. **Add tests to all four directories** following the patterns above
2. **Update this checklist** with new test requirements
3. **Create golden files** for new entity structures
4. **Follow naming conventions**: `test_<assertion_type>_<test_category>.py`
5. **Include both positive and negative test cases**
6. **Test edge cases and error conditions**
7. **Verify backward compatibility** when modifying existing functionality

## Running Tests

```bash
# Run all assertion tests
pytest tests/sdk/assertions/

# Run tests for specific area
pytest tests/sdk/assertions/assertion/
pytest tests/sdk/assertions/assertion_input/
pytest tests/sdk/assertions/client/
pytest tests/sdk/assertions/entities/

# Run tests for specific assertion type
pytest tests/sdk/assertions/ -k "smart_freshness"
pytest tests/sdk/assertions/ -k "volume"
pytest tests/sdk/assertions/ -k "column_metric"
```
