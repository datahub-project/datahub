# Shared Authentication State - Quick Reference

## What is Shared Auth?

**Login once, reuse everywhere** - Instead of logging in before each test, we:

1. Login ONCE in `tests/auth.setup.ts`
2. Save the session to `.auth/user.json`
3. All tests automatically use this saved session

## Benefits

- ⚡ **Faster**: No repeated logins (saves ~2-3 seconds per test)
- 🎯 **Simpler**: Tests focus on features, not authentication
- 🛡️ **Reliable**: Single point of authentication reduces flakiness

## How It Works

```typescript
// 1. Setup runs ONCE (tests/auth.setup.ts)
setup('authenticate', async ({ page }) => {
  await loginPage.login('datahub', 'datahub');
  await page.context().storageState({ path: '.auth/user.json' });
});

// 2. All tests use the saved state (playwright.config.ts)
projects: [
  {
    name: 'chromium',
    use: { storageState: '.auth/user.json' },
    dependencies: ['setup'],
  },
];

// 3. Tests start authenticated (no login needed!)
test('my feature', async ({ page }) => {
  await page.goto('/');
  // Already logged in! ✅
});
```

## When to Use Shared Auth

### ✅ Use Shared Auth (95% of tests)

**Any test that needs authentication:**

- Search functionality
- Dataset pages
- Dashboard features
- User management
- Settings
- etc.

**Example:**

```typescript
import { test, expect } from '../../fixtures/test-context';

test('searches for datasets', async ({ page, searchPage }) => {
  await page.goto('/');
  // ✅ Already authenticated
  await searchPage.search('my dataset');
});
```

### ❌ Don't Use Shared Auth

**Tests that verify login/onboarding:**

- Login page tests
- Login error handling
- Welcome modal tests
- Theme V2 login tests

**Example:**

```typescript
import { test as base } from '@playwright/test';
import { LoginPage } from '../../pages/LoginPage';

const test = base.extend<{ loginPage: LoginPage }>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  storageState: undefined, // ❌ Disable shared auth
});

test('shows login error', async ({ page, loginPage }) => {
  await page.goto('/login');
  // ✅ Not authenticated
  await loginPage.login('invalid', 'wrong');
});
```

## Files That Opt Out of Shared Auth

These tests explicitly set `storageState: undefined`:

- ✅ [`tests/auth/login.spec.ts`](tests/auth/login.spec.ts)
- ✅ [`tests/auth/login-with-welcome-modal.spec.ts`](tests/auth/login-with-welcome-modal.spec.ts)
- ✅ [`tests/login-v2/v2-login.spec.ts`](tests/login-v2/v2-login.spec.ts)
- ✅ [`tests/onboarding/welcome-modal.spec.ts`](tests/onboarding/welcome-modal.spec.ts)

## Running Tests

```bash
cd e2e-test/ui/playwright

# Normal run (setup runs first, then all tests)
npm test

# Run specific test (uses existing auth state)
npm test -- tests/search/search.spec.ts

# Force fresh login (delete saved state)
rm -rf .auth/ && npm test
```

## Troubleshooting

### Tests fail with "Not authenticated"

```bash
rm -rf .auth/
npm test
```

### Want to see setup running

```bash
npm test -- --reporter=line
# Should show "setup" project running first
```

### Testing with different users

Create multiple setup files:

```typescript
// tests/auth-admin.setup.ts
setup('authenticate as admin', async ({ page }) => {
  await loginPage.login('admin', 'admin123');
  await page.context().storageState({ path: '.auth/admin.json' });
});

// playwright.config.ts
projects: [
  { name: 'setup-admin', testMatch: /auth-admin\.setup\.ts/ },
  {
    name: 'admin-tests',
    use: { storageState: '.auth/admin.json' },
    dependencies: ['setup-admin'],
  },
];
```

## Key Files

| File                                               | Purpose                                    |
| -------------------------------------------------- | ------------------------------------------ |
| [`tests/auth.setup.ts`](tests/auth.setup.ts)       | Performs one-time login                    |
| [`playwright.config.ts`](playwright.config.ts)     | Configures setup project and storage state |
| [`.auth/user.json`](.auth/)                        | Saved authentication session (gitignored)  |
| [`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) | Full documentation                         |

## Architecture

```
┌──────────────────────────┐
│  tests/auth.setup.ts     │
│  Login once              │
│  Save to .auth/user.json │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  All Tests (*.spec.ts)   │
│  Load .auth/user.json    │
│  Start authenticated     │
└──────────────────────────┘
```

## Full Documentation

See [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md) for comprehensive guide.
