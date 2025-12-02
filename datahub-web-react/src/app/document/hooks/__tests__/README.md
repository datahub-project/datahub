# DocumentV2 Hooks Test Suite

This directory contains comprehensive unit tests for all hooks in the `documentV2/hooks` directory.

## Test Coverage

### ✅ Fully Tested Hooks

1. **useUpdateDocument** - Document mutation operations

    - Update contents, title, status, sub-type
    - Update related entities (assets, documents)
    - Error handling and rollback
    - Loading states

2. **useSearchDocuments** - Document search functionality

    - Default and custom queries
    - Filtering by parent, root-only, types, states
    - Pagination support
    - Various fetch policies

3. **useDocumentTreeMutations** - Tree mutation with optimistic updates

    - Create documents with optimistic UI
    - Update document titles
    - Move documents in tree
    - Delete documents with rollback on error

4. **useDocumentChildren** - Fetching child documents

    - Batch checking for children
    - Fetching children for parent
    - Error handling

5. **useDocumentPermissions** - Permission checks

    - Create, edit, delete, move permissions
    - Platform vs entity-level privileges
    - Memoization tests

6. **useDocumentTreeExpansion** - Tree expansion state

    - Expand/collapse nodes
    - Loading children on demand
    - Caching loaded children
    - External state control

7. **useExtractMentions** - URN extraction from markdown

    - Document and asset URN extraction
    - Duplicate handling
    - Edge cases (empty, malformed, special characters)

8. **useLoadDocumentTree** - Initial tree loading
    - Load root documents
    - Check for children
    - Document sorting
    - Error handling

## Running Tests

```bash
# Run all documentV2 hook tests
yarn test src/app/documentV2/hooks/__tests__/

# Run specific test file
yarn test src/app/documentV2/hooks/__tests__/useExtractMentions.test.ts

# Run with coverage
yarn test-coverage src/app/documentV2/hooks/__tests__/
```

## Test Structure

All tests follow these principles:

- **Comprehensive**: Test happy paths, error cases, and edge conditions
- **Isolated**: Each test is independent with proper setup/teardown
- **Clear**: Descriptive test names and organized test suites
- **Realistic**: Use proper mocks for GraphQL, contexts, and dependencies

## Technologies Used

- **Vitest**: Test runner
- **React Testing Library**: Component and hook testing utilities
- **Apollo MockedProvider**: GraphQL mock responses
- **Vi Mocks**: Module and function mocking

## Known Issues

Some tests involving Apollo MockedProvider may have timing issues with async mock resolution. These are being addressed but do not affect the structural correctness of the tests.
