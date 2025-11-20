# Document Management Cypress Tests

Comprehensive end-to-end tests for DataHub's native Document Management feature.

## Overview

This test suite validates the complete document lifecycle including creation, editing, hierarchy management, and deletion. Tests are designed to run idempotently and focus on core capabilities without depending on volatile UI elements.

## Test File: `document_management.js`

### Design Principles

1. **Idempotency**: Tests can be run multiple times without manual cleanup
2. **Resilience**: Uses robust selectors and appropriate waits
3. **Isolation**: Each test suite is independent with proper setup/teardown
4. **Core Focus**: Validates essential functionality, not implementation details

### Test Suites

#### 1. Core Document CRUD Operations

Tests fundamental document operations:

- **Create Document**: Creates a new document and verifies URL navigation
- **Update Title**: Edits title and verifies persistence across page reloads
- **Update Content**: Adds rich text content using the editor
- **Update Status**: Changes document state (Draft → Published)
- **Update Type**: Sets document type (e.g., Runbook, FAQ)

#### 2. Document Hierarchy Operations

Tests parent-child relationships:

- **Move Document**: Creates hierarchy by moving one document under another
- **Expand Parent**: Verifies child documents appear when parent is expanded
- **Collapse Parent**: Verifies child documents hide when parent is collapsed

#### 3. Document Deletion

Tests deletion with cascading:

- **Delete Parent**: Deletes parent document and verifies children are also removed

### Key Improvements Over Original

#### Idempotency

```javascript
// Cleanup happens regardless of test outcome
after(() => {
  cy.login().then(() => {
    cleanupDocuments();
  });
});
```

#### Robust Selectors

```javascript
// Uses flexible parent selector instead of exact text
cy.contains("Context").parents('[role="group"]').first().trigger("mouseover");
```

#### Proper Error Handling

```javascript
// Safe cleanup that doesn't fail on already-deleted documents
cy.deleteUrn(urn).then(
  () => cy.log(`Cleaned up document: ${urn}`),
  (err) => cy.log(`Failed to cleanup ${urn}: ${err.message}`),
);
```

#### Reduced Brittle Timing

- Uses `{ force: true }` for actions that don't need precise interaction simulation
- Consistent wait times based on actual operation duration
- Proper use of Cypress's retry-ability instead of arbitrary waits

### Data Test IDs Used

**Document Editing:**

- `document-title-input` - Title input field
- `document-editor-section` - Editor container
- `document-content-editor` - Rich text editor

**Document Properties:**

- `document-status-select` - Status dropdown (Published/Draft)
- `document-type-select` - Type dropdown (Runbook, FAQ, etc.)

**Document Tree:**

- `create-document-button` - Create new document button
- `document-tree-item-{urn}` - Individual tree items (dynamic URN)
- `document-tree-expand-button-{urn}` - Expand/collapse buttons (dynamic URN)

**Document Actions:**

- `document-actions-menu-button` - Actions menu trigger
- `move-document-confirm-button` - Confirm move operation
- `move-document-cancel-button` - Cancel move operation

## Running the Tests

### From smoke-test directory

```bash
cd smoke-test/tests/cypress

# Run all document tests
npm run cypress:run -- --spec "cypress/e2e/documents/**/*"

# Run specific test
npm run cypress:run -- --spec "cypress/e2e/documents/document_management.js"

# Open Cypress UI for debugging
npm run cypress:open
```

### From smoke-test root

```bash
# Using the test script
./smoke.sh documents
```

## Prerequisites

- DataHub backend running on `localhost:8080`
- DataHub frontend running on `localhost:9002` (or configured port)
- Theme V2 enabled (set automatically by test)
- Admin credentials configured in Cypress environment

## Test Execution Flow

1. **Before All Tests**:
   - Enable Theme V2
   - Login as admin
   - Initialize cleanup array

2. **Each Test**:
   - Fresh login to ensure authenticated state
   - Navigate to required page
   - Perform operations
   - Verify expected outcomes

3. **After All Tests**:
   - Login again (in case last test logged out)
   - Delete all created documents via API
   - Clear test data

## Debugging Failed Tests

### Common Issues

1. **URN not found in tree**
   - Ensure document was successfully created (check logs)
   - Verify navigation timing is sufficient
   - Check if tree is properly loaded

2. **Timing failures**
   - Auto-save operations need 3+ seconds
   - Tree expansion needs 1.5 seconds for server round-trip
   - Increase waits if running on slow environments

3. **Element not found**
   - Use `{ force: true }` for actions that don't need visibility checks
   - Ensure Theme V2 is enabled
   - Check data-testid attributes are present in build

### Viewing Test Artifacts

```bash
# Screenshots on failure
ls smoke-test/tests/cypress/cypress/screenshots/

# Videos of test runs
ls smoke-test/tests/cypress/cypress/videos/
```

## Future Enhancements

Potential additions for expanded coverage:

- Document search and filtering within tree
- Document permissions and access control
- Related assets and bi-directional linking
- Document versioning and history
- Bulk operations (multi-select, batch delete)
- Document templates and quick-start flows
- Export/import document structures
- Collaborative editing scenarios

## Maintenance

### When UI Changes

If UI elements change, update:

1. Data-testids in React components (preferred)
2. Fallback selectors in test (if necessary)
3. Wait times if operation duration changes

### When API Changes

If document operations change:

1. Update cleanup logic if URN format changes
2. Adjust verification assertions for new response shapes
3. Update navigation patterns if routes change

## Related Documentation

- [DataHub Documents Feature](../../../../../../../docs/features/documents.md)
- [Cypress Best Practices](https://docs.cypress.io/guides/references/best-practices)
- [DataHub Testing Guide](../../../../../../../docs/testing.md)
