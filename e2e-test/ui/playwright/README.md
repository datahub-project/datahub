# DataHub Playwright E2E Tests

Playwright-based end-to-end tests for DataHub, targeting Chromium only.

## Structure

```
e2e-test/ui/playwright/
├── playwright.config.ts          # Playwright configuration
├── fixtures/                     # Modular test fixtures (composed via mergeTests)
│   ├── base-test.ts              # Import for authenticated tests (most tests)
│   ├── login-test.ts             # Import for login UI tests (unauthenticated)
│   ├── login.fixture.ts          # Per-worker auth state management
│   ├── logger.fixture.ts         # Winston logger (auto-injected into every test)
│   ├── mocking.fixture.ts        # apiMock fixture for route interception
│   ├── auth.ts                   # Auth file path helpers + GMS token reader
│   ├── users.ts                  # Test user definitions (resolvedUsers)
│   └── cleanup.ts                # cleanup fixture (registered in base-test)
├── pages/                        # Page Object Models
│   ├── base-page.ts              # Base class: screenshot helper, logger/logDir
│   ├── login-page.ts
│   ├── search-page.ts
│   ├── dataset-page.ts
│   ├── business-attribute-page.ts
│   ├── welcome-modal-page.ts
│   ├── incidents-page.ts
│   └── common/
│       ├── searchbar-component.ts
│       └── sidebar-component.ts
├── tests/                        # Test specs, organised by feature
│   ├── auth/
│   ├── login-v2/
│   ├── onboarding/
│   ├── search/
│   ├── business-attributes/
│   └── incidents-v2/
├── helpers/                      # Standalone utility classes
│   ├── cleanup-helper.ts         # CleanupHelper / GlobalCleanupHelper
│   ├── graphql-helper.ts         # GraphQL request helpers
│   ├── graphql-seeder.ts         # Seed test data via GraphQL
│   ├── cli-seeder.ts             # Seed test data via DataHub CLI
│   ├── rest-seeder.ts            # Seed test data via REST API
│   ├── navigation-helper.ts      # Navigation utilities
│   └── wait-helper.ts            # Wait strategies
├── factories/                    # Data generation
│   ├── test-data-factory.ts      # URN builders + timestamped name generators
│   └── mock-response-factory.ts  # GraphQL mock response builders
└── utils/                        # Shared constants and utilities
    ├── constants.ts              # Timeouts, routes, entity types, data sources
    ├── logger.ts                 # createLogger factory (Winston)
    ├── api-mock.ts               # PageApiMocker class
    └── random.ts                 # withTimestamp, withRandomSuffix helpers
```

## Getting Started

### Prerequisites

- Node.js >= 22.0.0
- DataHub running at `http://localhost:9002` (or set `BASE_URL`)

### Installation

```bash
cd e2e-test/ui/playwright
npm install
npx playwright install chromium
```

### Running Tests

```bash
# All tests
npx playwright test

# Headed (see browser)
npx playwright test --headed

# Interactive UI mode
npx playwright test --ui

# Debug mode (step through)
npx playwright test --debug

# Specific suite
npx playwright test tests/search/
npx playwright test tests/business-attributes/

# Specific test by name
npx playwright test -g "should login successfully"

# View HTML report after a run
npx playwright show-report
```

## Writing Tests

### Choosing the right fixture

| Test type                                                                    | Import                      |
| ---------------------------------------------------------------------------- | --------------------------- |
| Needs an authenticated session (search, entities, business-attributes, etc.) | `../../fixtures/base-test`  |
| Tests the login UI itself (unauthenticated context)                          | `../../fixtures/login-test` |

Never import directly from `@playwright/test` in spec files.

### Authenticated test (base-test)

Authentication is handled automatically per worker by `loginFixture`.
No explicit login step is needed in `beforeEach`.

```typescript
import { test, expect } from "../../fixtures/base-test";
import { SearchPage } from "../../pages/search-page";

test.describe("Search", () => {
  let searchPage: SearchPage;

  test.beforeEach(async ({ page, logger, logDir }) => {
    searchPage = new SearchPage(page, logger, logDir);
    await searchPage.navigateToHome();
  });

  test("should return results for wildcard query", async () => {
    await searchPage.searchAndWait("*", 3000);
    await searchPage.expectHasResults();
  });
});
```

### Login UI test (login-test)

```typescript
import { test, expect } from "../../fixtures/login-test";

test.describe("Login", () => {
  test("should reject invalid credentials", async ({ loginPage }) => {
    await loginPage.navigateToLogin();
    await loginPage.login("bad", "creds");
    await loginPage.expectLoginError();
  });
});
```

### Route mocking (apiMock)

Destructure `apiMock` to activate route interception for a test:

```typescript
import { test, expect } from "../../fixtures/base-test";

test("renders with feature flag enabled", async ({ page, apiMock }) => {
  await apiMock.setFeatureFlags({ themeV2Enabled: true });
  await page.goto("/");
  // ...
});
```

### Switching test users

`base-test` defaults to the `admin` user. Override per suite:

```typescript
import { test } from "../../fixtures/base-test";
import { resolvedUsers } from "../../fixtures/users";

test.use({ user: resolvedUsers.reader });

test("reader cannot edit", async ({ page }) => {
  /* ... */
});
```

## Page Objects

Page objects live in `pages/` and extend `BasePage`. All constructors accept
optional `logger` and `logDir` for structured logging and screenshots.

```typescript
import { Page, Locator } from "@playwright/test";
import { BasePage } from "./base-page";
import { DataHubLogger } from "../utils/logger";

export class FeaturePage extends BasePage {
  readonly submitButton: Locator;

  constructor(page: Page, logger?: DataHubLogger, logDir?: string) {
    super(page, logger, logDir);
    this.submitButton = page.locator('[data-testid="feature-submit"]');
  }

  async submit(): Promise<void> {
    await this.submitButton.click();
  }
}
```

**Rules:**

- All selectors must be `readonly` properties, never inline in spec files.
- Page objects are instantiated in `beforeEach`, not registered as fixtures.
- Use `data-testid` attributes first; role selectors second; text selectors as a last resort.

## Authentication Architecture

`loginFixture` (in `fixtures/login.fixture.ts`) overrides Playwright's built-in
`context` fixture. On first run for a worker it performs a full UI login and
saves the session to `.auth/{username}.json`. On subsequent runs it reuses the
saved file — no round-trip to the server.

`base-test` composes `loginFixture` via `mergeTests`, so every test that
imports from `base-test` gets an authenticated page automatically.

`login-test` composes the logger and mocking fixtures but intentionally does
**not** include `loginFixture`, giving tests a clean unauthenticated context.

## Logging

`logger` and `logDir` fixtures are auto-injected into every test (they use
`auto: true`). In CI, logs are written to per-test files under
`test-results/<test>/logs/`. Page objects receive logger and logDir via their
constructor and use them for structured output and screenshots.

## Data Factories

Use `TestDataFactory` to build URNs and `withTimestamp` / `withRandomSuffix`
from `utils/random.ts` for unique, time-sortable test data names:

```typescript
import { TestDataFactory } from "../../factories/test-data-factory";

const name = TestDataFactory.generateTestDatasetName();
// → 'test_dataset_20260409_143022'

const urn = TestDataFactory.createDatasetUrn("hive", name);
// → 'urn:li:dataset:(urn:li:dataPlatform:hive,test_dataset_20260409_143022,PROD)'
```

## Mock Responses

`MockResponseFactory` builds typed GraphQL response objects with explicit
status codes, ready to be passed to `apiMock.interceptGraphQLResponse`:

```typescript
import { MockResponseFactory } from "../../factories/mock-response-factory";

const response = MockResponseFactory.createSearchResponse([], 0);
// → { status: 200, body: { data: { search: { searchResults: [], total: 0 } } } }
```

## Cleanup

For test-scoped cleanup, use `CleanupHelper`. For broad pre-run cleanup of all
test entities, use `GlobalCleanupHelper` (setup scripts only):

```typescript
import { CleanupHelper } from "../../helpers/cleanup-helper";

test.afterAll(async ({ page }) => {
  const cleanup = new CleanupHelper(page);
  await cleanup.deleteEntities(createdUrns);
});
```

`namePattern` is mandatory in `deleteEntitiesByType` to prevent accidental
deletion of all entities of a type.

## CI/CD

- Single Chromium project, 1 retry on failure.
- JUnit report written to `test-results/junit.xml`.
- Screenshots and traces captured on failure.
- Single worker (`workers: 1`) in CI for stable ordering.
