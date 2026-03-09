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
npx playwright test

# Run tests in headed mode (see browser)
npx playwright test --headed

# Run tests in UI mode (interactive)
npx playwright test --ui

# Run tests in debug mode
npx playwright test --debug

# Run specific test suite
npx playwright test tests/auth
npx playwright test tests/search
npx playwright test tests/entities

# Run tests in specific browser
npx playwright test --project=chromium
npx playwright test --project=firefox
npx playwright test --project=webkit
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

## Authentication

### Shared Authentication State

**Login once, reuse everywhere** - Most tests use a shared authentication session for better performance and reliability.

#### How It Works

```typescript
// 1. Setup runs ONCE (tests/auth.setup.ts)
setup("authenticate", async ({ page }) => {
  await loginPage.login("datahub", "datahub");
  await page.context().storageState({ path: ".auth/user.json" });
});

// 2. All tests use the saved state (playwright.config.ts)
projects: [
  {
    name: "chromium",
    use: { storageState: ".auth/user.json" },
    dependencies: ["setup"],
  },
];

// 3. Tests start authenticated (no login needed!)
test("my feature", async ({ page }) => {
  await page.goto("/");
  // Already logged in! ✅
});
```

#### Benefits

- ⚡ **Faster**: No repeated logins (saves ~2-3 seconds per test)
- 🎯 **Simpler**: Tests focus on features, not authentication
- 🛡️ **Reliable**: Single point of authentication reduces flakiness

#### When to Use Shared Auth

**✅ Use for 95% of tests:**

- Search functionality
- Dataset/entity pages
- Dashboard features
- User management
- Settings

```typescript
import { test, expect } from "../../fixtures/test-context";

test("searches for datasets", async ({ page, searchPage }) => {
  await page.goto("/");
  // ✅ Already authenticated
  await searchPage.search("my dataset");
});
```

**❌ Don't use for authentication tests:**

- Login page tests
- Login error handling
- Welcome modal tests

```typescript
import { test as base } from "@playwright/test";

const test = base.extend({
  storageState: undefined, // Disable shared auth
});

test("shows login error", async ({ page, loginPage }) => {
  await page.goto("/login");
  // ✅ Not authenticated
  await loginPage.login("invalid", "wrong");
});
```

#### Troubleshooting

**Tests fail with "Not authenticated":**

```bash
rm -rf .auth/
npx playwright test
```

**Test with different users:**
Create additional setup files with different auth credentials

## Data Seeding

### Overview

Tests use a TypeScript-based data seeding infrastructure that loads test data via DataHub's REST API. Each test suite can have its own fixture data for isolation.

### Architecture

```
1. Global Setup (global.setup.ts)
   └─> Clean up all test data (Cypress*, Test* entities)

2. Suite Setup (e.g., business-attributes/data.setup.ts)
   └─> Load fixtures/data.json
   └─> Ingest via REST API
   └─> Wait for indexing
   └─> Store URNs for cleanup

3. Run Tests
   └─> Tests use seeded data

4. Cleanup (afterAll hook)
   └─> Delete seeded entities
```

### Key Components

**RestSeeder** (`helpers/RestSeeder.ts`)

- `loadFixture(path)` - Load JSON fixture files
- `ingestMCPs(mcps)` - Ingest Metadata Change Proposals via REST API
- `waitForSync(urns)` - Wait for entities to be indexed

```typescript
const seeder = new RestSeeder(page);
const mcps = await seeder.loadFixture("./fixtures/data.json");
const urns = await seeder.ingestMCPs(mcps);
await seeder.waitForSync(urns);
```

**CleanupHelper** (`helpers/CleanupHelper.ts`)

- `cleanupTestData()` - Delete all test entities (Cypress*, test\_*)
- `deleteEntities(urns)` - Hard delete specific entities
- `cleanupOnSuccess(testInfo, urns)` - Only clean up if tests passed

```typescript
const cleanup = new CleanupHelper(page);
await cleanup.cleanupTestData(); // Global cleanup

test.afterAll(async ({ page }) => {
  await cleanup.deleteEntities(urns); // Suite cleanup
});
```

### Adding Data Seeding to a New Suite

**1. Create fixture data:**

```bash
tests/your-suite/fixtures/data.json
```

**2. Create setup file:**

```typescript
// tests/your-suite/data.setup.ts
import { test as setup } from "@playwright/test";
import { RestSeeder } from "../../helpers/RestSeeder";

setup("seed your-suite data", async ({ page }) => {
  const seeder = new RestSeeder(page);
  const mcps = await seeder.loadFixture("./fixtures/data.json");
  const urns = await seeder.ingestMCPs(mcps);
  await seeder.waitForSync(urns);
  (global as any).__YOUR_SUITE_URNS = urns;
});
```

**3. Update playwright.config.ts:**

```typescript
projects: [
  {
    name: "your-suite-setup",
    testMatch: /your-suite\/data\.setup\.ts/,
    dependencies: ["auth-setup"],
  },
  // ...
];
```

**4. Add cleanup to tests:**

```typescript
test.afterAll(async ({ page }) => {
  const cleanup = new CleanupHelper(page);
  const urns = (global as any).__YOUR_SUITE_URNS || [];
  await cleanup.deleteEntities(urns);
});
```

### Benefits

- ✅ **Self-contained suites** - Each suite has its own data
- ✅ **Fast setup** - Parallel seeding per suite
- ✅ **Smart cleanup** - Preserves data on failure for debugging
- ✅ **Type safety** - Full TypeScript support
- ✅ **No Python dependency** - Pure TypeScript/Playwright

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
npx playwright test --ui
```

### Step-through Debugging

```bash
npx playwright test --debug
```

### Generate Code

```bash
npx playwright codegen
```

This will open a browser and record your actions as Playwright code.

## Reporting

After running tests, view the HTML report:

```bash
npx playwright show-report
```

## Migration from Cypress

This POC demonstrates the migration path from Cypress to Playwright. Key differences:

- **No `cy.` commands** - Use native async/await
- **Better TypeScript support** - Full type safety
- **Multiple browsers** - Test in Chromium, Firefox, and WebKit
- **Better debugging** - Built-in UI mode and trace viewer
- **Faster execution** - Parallel test execution by default
