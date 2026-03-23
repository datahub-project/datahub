/**
 * Base test fixture — default import for all regular (authenticated) tests.
 *
 * Composes four framework capabilities as Playwright fixtures:
 *
 *   1. authenticated state  — per-user browser context loaded from storageState
 *   2. structured logging   — JSON-lines log file written to logs/test-run.log
 *   3. API mocking          — DataHub-aware route interception helpers
 *   4. test data loading    — feature-scoped data injection with idempotency check
 *
 * Additionally provides:
 *   - `gmsToken`  — GMS personal access token read from .auth/gms-token-{user}.json
 *   - `cleanup`   — per-test ScopedCleanup tracker backed by the GMS API
 *   - `user`      — injectable option (default: resolvedUsers.admin); override
 *                   with `test.use({ user: resolvedUsers.reader })` in a suite
 *
 * Auth state convention:
 *   The `context` fixture is overridden so that every test in this suite runs
 *   inside a browser context pre-loaded with the named user's storageState.
 *   Because `page` depends on `context`, all page objects automatically inherit
 *   the correct authenticated session — no manual login needed in tests.
 *
 * Login tests must NOT import from this file; use login-test.ts instead.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Multi-user example:
 *
 *   import { test, expect } from '../../fixtures/base-test';
 *   import { resolvedUsers } from '../../fixtures/users';
 *
 *   // All tests in this describe block run as the reader user
 *   test.use({ user: resolvedUsers.reader });
 *
 *   test('reader can view dataset', async ({ page }) => { ... });
 * ─────────────────────────────────────────────────────────────────────────────
 */


import { test as base, mergeTests } from '@playwright/test';
import { loggerFixture } from './logger.fixture';
import { loginFixture } from './login.fixture';

// import { authStatePath, readGmsToken } from './auth';
// import { resolvedUsers, type UserCredentials } from './users';
// import { FileLogger, type StructuredLogger } from '../utils/logger';
// import { PageApiMocker, type ApiMocker } from './api-mock';
// import { RestFeatureDataLoader, type FeatureDataLoader } from './test-data';
// import { ApiScopedCleanup, type ScopedCleanup } from './cleanup';

// // ── Fixture type declarations ─────────────────────────────────────────────────

// type BaseFixtures = {
//   /** GMS personal access token for the active user. Use in API helpers. */
//   gmsToken: string;
//   /** Structured logger writing JSON lines to logs/test-run.log. */
//   logger: StructuredLogger;
//   /** DataHub-aware API mocking and route interception. */
//   apiMock: ApiMocker;
//   /** Injects feature-scoped test data from {featureDir}/data/{feature}.json. */
//   featureData: FeatureDataLoader;
//   /** Per-test URN tracker; call flush() in test.afterAll() for suite cleanup. */
//   cleanup: ScopedCleanup;
// };

// type BaseOptions = {
//   /**
//    * The user whose storageState is loaded for this test.
//    * Override per suite: `test.use({ user: resolvedUsers.reader })`.
//    * Defaults to the admin user defined in users.ts.
//    */
//   user: UserCredentials;
// };

// // ── Fixture implementations ───────────────────────────────────────────────────

// export const test = base.extend<BaseFixtures, BaseOptions>({
//   // ── Option: injectable user ─────────────────────────────────────────────────
//   user: [resolvedUsers.admin, { option: true, scope: 'worker' }],

//   // ── Capability 1: authenticated state ───────────────────────────────────────
//   // Override the built-in `context` so that `page` (which depends on it)
//   // automatically carries the correct session. storageState files are written
//   // by tests/auth.setup.ts; a missing file means auth setup has not been run.
//   context: async ({ user, browser }, use) => {
//     const stateFile = authStatePath(user.username);
//     if (!fs.existsSync(stateFile)) {
//       throw new Error(
//         `Auth state missing for user '${user.username}' (expected: ${stateFile}).\n` +
//           `Run auth setup first: npx playwright test --project=auth-setup`,
//       );
//     }
//     const ctx = await browser.newContext({
//       storageState: stateFile,
//       baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
//     });
//     await use(ctx);
//     await ctx.close();
//   },

//   // GMS token — read from disk once per test (file read is fast).
//   gmsToken: async ({ user }, use) => {
//     await use(readGmsToken(user.username));
//   },

//   // ── Capability 2: structured logging ────────────────────────────────────────
//   logger: async ({}, use, testInfo) => {
//     const logsDir = path.join(process.cwd(), 'logs');
//     const fileLogger = new FileLogger(testInfo, logsDir);
//     fileLogger.info('test started');
//     await use(fileLogger);
//     fileLogger.info('test finished', { status: testInfo.status });
//     fileLogger.close();
//   },

//   // ── Capability 3: API mocking ────────────────────────────────────────────────
//   apiMock: async ({ page }, use) => {
//     await use(new PageApiMocker(page));
//   },

//   // ── Capability 4: test data loading ─────────────────────────────────────────
//   featureData: async ({ playwright, gmsToken }, use) => {
//     const baseUrl = process.env.BASE_URL ?? 'http://localhost:9002';
//     const gmsUrl = baseUrl.replace(':9002', ':8080');
//     const request = await playwright.request.newContext({ baseURL: gmsUrl });
//     try {
//       await use(new RestFeatureDataLoader(request, gmsToken, gmsUrl));
//     } finally {
//       await request.dispose();
//     }
//   },

//   // ── Cleanup: per-test URN tracker ────────────────────────────────────────────
//   // Backed by a standalone APIRequestContext authenticated with the GMS token
//   // so it works independently of any browser session.
//   cleanup: async ({ playwright, gmsToken }, use, testInfo) => {
//     const baseUrl = process.env.BASE_URL ?? 'http://localhost:9002';
//     const gmsUrl = baseUrl.replace(':9002', ':8080');
//     const request = await playwright.request.newContext({
//       baseURL: gmsUrl,
//       extraHTTPHeaders: { Authorization: `Bearer ${gmsToken}` },
//     });
//     const scopedCleanup = new ApiScopedCleanup(request, gmsUrl);
//     await use(scopedCleanup);
//     // Auto-flush on teardown, honouring the failure-preservation policy.
//     await scopedCleanup.flush(testInfo.status);
//     await request.dispose();
//   },
// });

// export { expect } from '@playwright/test';
// export type { UserCredentials } from './users';
// export type { StructuredLogger } from '../utils/logger';
// export type { ApiMocker } from './api-mock';
// export type { FeatureDataLoader } from './test-data';
// export type { ScopedCleanup } from './cleanup';



export const baseTest = mergeTests(loggerFixture, cleanup..., loginFixture)

export const tempLoginTest = mergetest(loggerFixture, cleanup,)
export const loginTest = tempLoginTest.extend <> {
  user: [admin, { option: true }],
  loginPage: async ({ logger, page }, use, testInfo) {

    use(LoginPage(page, logger, testInfo));
  }
}

