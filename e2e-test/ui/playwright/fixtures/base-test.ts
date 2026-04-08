/**
 * Base test fixture — default import for all regular (authenticated) tests.
 *
 * Composes three independent capability fixtures via mergeTests:
 *
 *   loggerFixture  — Winston structured logging, auto-injected into every test
 *   mockingFixture — DataHub GraphQL/route mocking, opt-in per test
 *   loginFixture   — Per-worker auth state management, no global setup needed
 *
 * Additionally extends the merged result with:
 *   gmsToken    — GMS personal access token read from .auth/gms-token-{user}.json
 *   featureData — Feature-scoped test data injection with idempotency check
 *   cleanup     — Per-test URN tracker flushed automatically after each test
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * How authentication works (no global auth-setup project needed):
 *
 *   The loginFixture overrides Playwright's built-in `context`. On first run
 *   for a given user the fixture logs in via the UI and saves the session to
 *   .auth/{username}.json. Subsequent tests (and parallel workers) find the
 *   file and skip the login step entirely. The `page` fixture, which derives
 *   from `context`, automatically carries the authenticated session.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * Multi-user example (user is set at describe level, never inside a test):
 *
 *   import { test, expect } from '../../fixtures/base-test';
 *   import { resolvedUsers } from '../../fixtures/users';
 *
 *   test.describe('Reader access', () => {
 *     test.use({ user: resolvedUsers.reader });
 *     test('can view datasets', async ({ page, logger }) => {
 *       logger.step('navigate to datasets');
 *       await page.goto('/datasets');
 *     });
 *   });
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Login tests must NOT import from this file — use login-test.ts instead.
 */

import { mergeTests } from '@playwright/test';
import { loggerFixture } from './logger.fixture';
import { mockingFixture } from './mocking.fixture';
import { loginFixture } from './login.fixture';
import { seedingFixture } from './seeding.fixture';
import { readGmsToken } from './auth';
import { RestFeatureDataLoader, type FeatureDataLoader } from './test-data';
import { ApiScopedCleanup, type ScopedCleanup } from './cleanup';

// ── Compose the four core capability fixtures ─────────────────────────────────

const composedTest = mergeTests(loggerFixture, mockingFixture, loginFixture, seedingFixture);

// ── Additional fixtures built on top of the composition ───────────────────────

type ExtendedFixtures = {
  /** GMS personal access token for the active user. */
  gmsToken: string;
  /** Injects feature-scoped test data from {featureDir}/data/{feature}.json. */
  featureData: FeatureDataLoader;
  /** Per-test URN tracker; auto-flushed after each test (skipped on failure). */
  cleanup: ScopedCleanup;
};

export const test = composedTest.extend<ExtendedFixtures>({
  gmsToken: async ({ user }, use) => {
    await use(readGmsToken(user.username));
  },

  featureData: async ({ playwright, gmsToken }, use) => {
    const baseUrl = process.env.BASE_URL ?? 'http://localhost:9002';
    const gmsUrl = baseUrl.replace(':9002', ':8080');
    const request = await playwright.request.newContext({ baseURL: gmsUrl });
    try {
      await use(new RestFeatureDataLoader(request, gmsToken, gmsUrl));
    } finally {
      await request.dispose();
    }
  },

  cleanup: async ({ playwright, gmsToken }, use, testInfo) => {
    const baseUrl = process.env.BASE_URL ?? 'http://localhost:9002';
    const gmsUrl = baseUrl.replace(':9002', ':8080');
    const request = await playwright.request.newContext({
      baseURL: gmsUrl,
      extraHTTPHeaders: { Authorization: `Bearer ${gmsToken}` },
    });
    const scopedCleanup = new ApiScopedCleanup(request, gmsUrl);
    await use(scopedCleanup);
    // Preserve entities on failure so engineers can inspect the broken state.
    await scopedCleanup.flush(testInfo.status);
    await request.dispose();
  },
});

export { expect } from '@playwright/test';
export type { UserCredentials } from './users';
export type { DataHubLogger } from './logger.fixture';
export type { ApiMocker } from './mocking.fixture';
export type { FeatureDataLoader } from './test-data';
export type { ScopedCleanup } from './cleanup';
