# Playwright Test Documentation

This directory contains documentation for Playwright tests and feature analysis.

## Directory Structure

```
docs/
├── README.md           # This file
└── features/           # Feature design documents for test planning
    ├── login-feature.md
    ├── search-feature.md
    └── ...
```

## Feature Design Documents

Feature design documents are comprehensive specifications that describe:

- UI components and their selectors
- User workflows and scenarios
- Edge cases and error conditions
- Proposed test cases with priorities
- API interactions and state management

### Purpose

These documents serve as:

1. **Test Planning Reference**: Detailed specifications for writing accurate tests
2. **Selector Documentation**: Centralized list of data-testid attributes
3. **Coverage Analysis**: Identify gaps in test coverage
4. **Onboarding Material**: Help new team members understand features

### Generating Feature Docs

Use the custom Claude Code skill to generate feature documentation:

```bash
# Analyze a feature by name
/playwright-feature-analyzer login

# Analyze a specific component file
/playwright-feature-analyzer datahub-web-react/src/app/search/SearchPage.tsx

# Analyze by feature area
/playwright-feature-analyzer business-attributes
```

The analyzer will:

1. Search the codebase for related components
2. Extract all data-testid selectors
3. Map user workflows from code
4. Identify edge cases and states
5. Generate comprehensive test scenarios
6. Save document to `docs/features/[feature-name]-feature.md`

### Document Template

Each feature document includes:

- **Document Information**: Version, date, source files
- **Overview**: High-level feature description
- **Feature Purpose**: Business and technical value
- **Feature Access Points**: How users access the feature
- **UI Components**: Detailed component breakdown with selectors
- **User Workflows**: Step-by-step scenarios
- **Feature Interactions**: Validation, API calls, state
- **Edge Cases**: Unusual scenarios and error conditions
- **Testing Scenarios**: Proposed test cases with priorities
- **Selector Reference**: Quick lookup table
- **Source Code References**: Links to implementation files

### Using Feature Docs for Test Writing

When writing tests:

1. **Reference Selectors**: Use the "Selector Reference" section for data-testid attributes
2. **Follow Workflows**: Implement tests based on documented workflows
3. **Cover Edge Cases**: Ensure tests handle all documented edge cases
4. **Check Prerequisites**: Set up test environment per documented preconditions
5. **Verify Expected Results**: Assert documented success criteria

### Best Practices

- **Keep Docs Updated**: Regenerate when features change significantly
- **Review Before Writing Tests**: Always consult the feature doc first
- **Link from Tests**: Reference the feature doc in test file comments
- **Flag Missing Selectors**: If a selector is missing, update both doc and code

### Example Usage Flow

1. **Analyze Feature**:

   ```bash
   /playwright-feature-analyzer dataset-lineage
   ```

2. **Review Generated Doc**:

   ```bash
   code e2e-test/ui/playwright/docs/features/dataset-lineage-feature.md
   ```

3. **Write Tests** (reference doc for selectors and workflows):

   ```typescript
   // Reference: docs/features/dataset-lineage-feature.md
   test('should display lineage graph', async ({ page }) => {
     // Selector from feature doc
     await page.locator('[data-testid="lineage-viz"]').waitFor();
     ...
   });
   ```

4. **Update Doc** (if selectors changed during implementation):
   ```bash
   /playwright-feature-analyzer dataset-lineage
   ```

## Contributing

When adding new features to DataHub:

1. **Add data-testid attributes** to new UI components
2. **Run feature analyzer** to generate documentation
3. **Write Playwright tests** based on the generated doc
4. **Commit both** the feature doc and tests together

This ensures test coverage and documentation stay in sync with implementation.

## Related Documentation

- [../README.md](../README.md) - Main Playwright test documentation
- [../../playwright.config.ts](../../playwright.config.ts) - Playwright configuration
- [../pages/](../pages/) - Page Object Models
- [../tests/](../tests/) - Test specifications
