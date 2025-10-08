# CSV Import Testing Suite

This directory contains comprehensive tests for the CSV import functionality, ensuring that all aspects of the import process work correctly, including ownership parsing, entity management, hierarchy handling, relationship management, and the complete import workflow.

## Test Structure

### 1. Unit Tests (`shared/utils/__tests__/`)

**`ownershipParsingUtils.test.ts`**
- Tests the core ownership parsing utility functions
- Covers single and multiple ownership parsing
- Validates different ownership formats (comma and pipe separators)
- Tests ownership type detection and URN generation
- Validates error handling for malformed input

**`glossaryUtils.test.ts`**
- Tests core glossary utility functions
- Covers entity ID generation and normalization
- Tests hierarchy level calculation and sorting
- Validates circular dependency detection
- Tests orphaned entity detection
- Covers GraphQL entity conversion

**`customPropertiesUtils.test.ts`**
- Tests custom properties comparison logic
- Covers JSON parsing and comparison
- Tests various data types (strings, numbers, booleans, arrays, objects)
- Validates error handling for malformed JSON
- Tests whitespace and ordering differences

**Key Test Cases:**
- ✅ Single ownership parsing (`admin:DEVELOPER` in users column)
- ✅ Multiple ownership with pipe separator (`admin:DEVELOPER|datahub:Technical Owner` in users column)
- ✅ Mixed user and group ownership (`admin:DEVELOPER` in users, `bfoo:Technical Owner` in groups)
- ✅ Owner type detection (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE)
- ✅ URN generation for CORP_USER and CORP_GROUP
- ✅ Validation of ownership string formats
- ✅ Error handling for invalid input
- ✅ Entity ID generation and uniqueness
- ✅ Hierarchy level calculation and sorting
- ✅ Circular dependency detection
- ✅ Orphaned entity detection
- ✅ Custom properties comparison
- ✅ GraphQL entity conversion

### 2. Integration Tests (`shared/hooks/__tests__/`)

**`useImportProcessing.test.tsx`**
- Tests the main import processing hook
- Covers ownership parsing within the React context
- Tests entity processing with different ownership scenarios
- Validates error handling and progress tracking

**Key Test Cases:**
- ✅ Single ownership processing
- ✅ Multiple ownership processing (both separators)
- ✅ Empty ownership handling
- ✅ Invalid ownership format error handling
- ✅ Ownership type creation with audit fields
- ✅ Mixed entity types (glossary terms and ownership types)

### 3. End-to-End Tests (`__tests__/`)

**`csvImportE2E.test.tsx`**
- Tests the complete CSV import workflow
- Simulates real-world scenarios with the updated CSV file
- Tests ownership type creation and resolution
- Validates duplicate ownership type handling
- Tests comprehensive ownership aggregation

**`completeImportWorkflow.test.tsx`**
- Tests the complete import workflow including all features
- Covers entity management and normalization
- Tests entity comparison and change detection
- Validates hierarchy management
- Tests relationship management (HasA and IsA)
- Covers domain management
- Tests error handling and recovery

**Key Test Cases:**
- ✅ Complete CSV file processing with pipe-separated ownership
- ✅ Ownership type creation and resolution
- ✅ Duplicate ownership type handling
- ✅ Mixed ownership formats (comma and pipe)
- ✅ Comprehensive ownership aggregation across entity types
- ✅ GraphQL error handling
- ✅ Network error retry logic
- ✅ Entity normalization and ID generation
- ✅ New vs updated vs no change detection
- ✅ Parent-child relationship handling
- ✅ HasA and IsA relationship management
- ✅ Domain assignment and resolution
- ✅ Hierarchy level calculation and sorting
- ✅ Circular dependency detection
- ✅ Orphaned entity detection
- ✅ Custom properties comparison
- ✅ Mixed new and updated entities
- ✅ Complete import workflow with all features

## Running Tests

### Run All Tests
```bash
# From the datahub-web-react directory
yarn test src/app/glossaryV2/import/ --run
```

### Run Specific Test Suites
```bash
# Unit tests only
yarn test src/app/glossaryV2/import/shared/utils/__tests__/ownershipParsingUtils.test.ts --run

# Integration tests only
yarn test src/app/glossaryV2/import/shared/hooks/__tests__/useImportProcessing.test.tsx --run

# End-to-end tests only
yarn test src/app/glossaryV2/import/__tests__/csvImportE2E.test.tsx --run
```

### Run with Coverage
```bash
yarn test src/app/glossaryV2/import/ --coverage --run
```

### Use the Test Script
```bash
# Make executable (if not already)
chmod +x src/app/glossaryV2/import/__tests__/runTests.sh

# Run all tests
./src/app/glossaryV2/import/__tests__/runTests.sh
```

## Test Coverage

The test suite covers:

### ✅ **Ownership Parsing**
- Single ownership entries
- Multiple ownership entries (comma and pipe separators)
- Different ownership types (TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE)
- URN generation for CORP_USER and CORP_GROUP
- Validation and error handling

### ✅ **Entity Processing**
- Glossary terms with ownership
- Ownership type creation
- Mixed entity types in batches
- Error handling and recovery

### ✅ **CSV Import Workflow**
- Complete CSV file processing
- Ownership type resolution and creation
- Duplicate handling
- Comprehensive ownership aggregation
- Network error handling and retry logic

### ✅ **Error Scenarios**
- Invalid ownership formats
- Missing ownership types
- GraphQL errors
- Network failures
- Malformed data

## Test Data

The tests use realistic data based on the updated CSV file:

```csv
entity_type,name,description,definition,term_source,source_ref,source_url,ownership,parent_nodes,related_contains,related_inherits,domain_urn,domain_name,custom_properties
glossaryTerm,Customer ID,Unique identifier for each customer,Unique identifier for each customer,INTERNAL,,,"bfoo:Technical Owner:CORP_GROUP|datahub:Technical Owner:CORP_USER",Business Terms,,Business Terms.Customer Name,,,
glossaryTerm,Customer Data,Information related to customer records and profiles,Information related to customer records and profiles,INTERNAL,,,"bfoo:test:CORP_GROUP|datahub:Technical Owner:CORP_USER",Business Terms,,Business Terms.Customer Name,,,
```

## Mocking Strategy

The tests use comprehensive mocking:

- **Apollo Client**: Mocked GraphQL operations
- **User Context**: Mocked user information
- **GraphQL Operations**: Mocked responses for different scenarios
- **Network**: Mocked success and failure scenarios

## Continuous Integration

These tests are designed to run in CI/CD pipelines:

1. **Fast execution**: Unit tests run quickly
2. **Isolated**: Tests don't depend on external services
3. **Deterministic**: Consistent results across environments
4. **Comprehensive**: High coverage of critical functionality

## Debugging Tests

If tests fail, check:

1. **Console output**: Look for error messages
2. **Test data**: Verify mock data matches expected format
3. **Dependencies**: Ensure all imports are correct
4. **Environment**: Check that test environment is properly set up

## Adding New Tests

When adding new functionality:

1. **Unit tests**: Add to `ownershipParsingUtils.test.ts`
2. **Integration tests**: Add to `useImportProcessing.test.tsx`
3. **E2E tests**: Add to `csvImportE2E.test.tsx`
4. **Update documentation**: Add test cases to this README

## Test Maintenance

- **Regular updates**: Update tests when functionality changes
- **Coverage monitoring**: Ensure new code is covered
- **Performance**: Monitor test execution time
- **Reliability**: Fix flaky tests promptly
