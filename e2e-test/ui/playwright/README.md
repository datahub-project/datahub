# DataHub Playwright E2E Tests

This directory contains Playwright-based end-to-end tests for DataHub.

## Structure

```
e2e-test/ui/playwright/
├── tests/              # Test specifications organized by feature
├── pages/              # Page Object Models (POM)
├── helpers/            # Helper utilities for tests
├── fixtures/           # Test fixtures and setup
├── factories/          # Test data factories
├── utils/              # Shared utilities and constants
└── playwright.config.ts # Playwright configuration
```

## Getting Started

### Installation

```bash
cd e2e-test/ui/playwright
npm install
npx playwright install
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests in headed mode (see browser)
npm run test:headed

# Run tests in UI mode (interactive)
npm run test:ui

# Run tests in debug mode
npm run test:debug

# Run specific test suite
npm run test:auth
npm run test:search
npm run test:entities

# Run tests in specific browser
npm run test:chromium
npm run test:firefox
npm run test:webkit
```

### Environment Variables

Create a `.env` file in this directory:

```env
BASE_URL=http://localhost:9002
TEST_USERNAME=datahub
TEST_PASSWORD=datahub
```

## Test Organization

### Page Object Model (POM)

All page interactions are encapsulated in Page Objects located in `pages/`:

- `base.page.ts` - Base page class with common functionality
- `LoginPage.ts` - Login page interactions
- `SearchPage.ts` - Search functionality
- `DatasetPage.ts` - Dataset entity page
- `BusinessAttributePage.ts` - Business attributes page

### Fixtures

Reusable test context and setup:

- `auth.ts` - Authentication fixtures
- `test-context.ts` - All page objects and helpers
- `mock-data.ts` - Mock data for tests

### Helpers

Utility classes for common operations:

- `GraphQLHelper.ts` - GraphQL request/response handling
- `NavigationHelper.ts` - Navigation utilities
- `WaitHelper.ts` - Wait strategies

### Factories

Test data generation:

- `TestDataFactory.ts` - Generate test data (URNs, names, etc.)
- `MockResponseFactory.ts` - Generate mock API responses

## Writing Tests

### Basic Test Example

```typescript
import { test, expect } from "../../fixtures/test-context";

test.describe("Feature Name", () => {
  test("should do something", async ({ loginPage, searchPage }) => {
    await loginPage.navigateToLogin();
    await loginPage.login("datahub", "datahub");

    await searchPage.search("test");
    const results = await searchPage.getResultsCount();

    expect(results).toBeGreaterThan(0);
  });
});
```

### Using Fixtures

```typescript
import { test, expect } from "../../fixtures/test-context";

test("authenticated test", async ({
  page,
  loginPage,
  datasetPage,
  graphqlHelper,
}) => {
  // All page objects and helpers are available
});
```

## Best Practices

1. **Use Page Objects** - Never interact with selectors directly in tests
2. **Use data-testid** - Prefer `data-testid` attributes for selectors
3. **Avoid hardcoded waits** - Use Playwright's auto-waiting or explicit waits
4. **Independent tests** - Each test should be independent and not rely on others
5. **Clean test data** - Use factories to generate unique test data
6. **Descriptive names** - Test names should clearly describe what they test

## CI/CD Integration

Tests run automatically in CI when:

- Pull requests are opened
- Code is pushed to main branches
- Manual workflow dispatch

## Debugging

### Visual Debugging

```bash
npm run test:ui
```

### Step-through Debugging

```bash
npm run test:debug
```

### Generate Code

```bash
npm run codegen
```

This will open a browser and record your actions as Playwright code.

## Reporting

After running tests, view the HTML report:

```bash
npm run show-report
```

## Migration from Cypress

This POC demonstrates the migration path from Cypress to Playwright. Key differences:

- **No `cy.` commands** - Use native async/await
- **Better TypeScript support** - Full type safety
- **Multiple browsers** - Test in Chromium, Firefox, and WebKit
- **Better debugging** - Built-in UI mode and trace viewer
- **Faster execution** - Parallel test execution by default
