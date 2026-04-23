/**
 * Test data cleanup capability.
 *
 * Provides a `ScopedCleanup` fixture for tracking and deleting dynamically
 * created entities after a test suite completes.
 *
 * Design decisions aligned with the requirements:
 *
 *  - GMS API token used for all delete calls — no browser session required (§5)
 *  - Data is preserved on test failure to allow post-failure investigation (§5)
 *  - `ScopedCleanup` is test-scoped: each test gets its own tracker. For
 *    per-suite afterAll cleanup, tests capture the fixture value in a module-
 *    level variable and call `flush()` from `test.afterAll()` (see pattern
 *    below). This pattern avoids fighting Playwright's worker/test scope rules
 *    while still providing the afterAll cleanup behaviour described in §5.
 *
 * Usage pattern in a test suite:
 *
 *   ```typescript
 *   import { test, expect } from '../../fixtures/base-test';
 *
 *   let suiteCleanup: ScopedCleanup;
 *
 *   test.afterAll(async () => {
 *     await suiteCleanup?.flush();
 *   });
 *
 *   test('creates an entity', async ({ cleanup, gmsToken }) => {
 *     suiteCleanup ??= cleanup;           // capture once
 *     const urn = await createSomething(gmsToken);
 *     cleanup.track(urn);
 *   });
 *   ```
 *
 * The standalone `deleteEntities` function is exported for use in global
 * cleanup scripts (scripts/cleanup.ts) which run outside Playwright.
 */

import type { APIRequestContext } from '@playwright/test';

// ── Public interface ──────────────────────────────────────────────────────────

export interface ScopedCleanup {
  /** Register one or more URNs for deletion when `flush()` is called. */
  track(...urns: string[]): void;

  /**
   * Delete all tracked URNs via the GMS REST API.
   *
   * Pass `testStatus` to conditionally skip cleanup on failure:
   *   - `'passed'` / `'skipped'` → deletes tracked URNs
   *   - `'failed'` / `'timedOut'` → skips deletion (preserves data for debug)
   *   - `undefined` → always deletes (unconditional flush)
   */
  flush(testStatus?: string): Promise<void>;
}

// ── Implementation ────────────────────────────────────────────────────────────

export class ApiScopedCleanup implements ScopedCleanup {
  private readonly trackedUrns: string[] = [];

  constructor(
    private readonly request: APIRequestContext,
    private readonly gmsUrl: string,
  ) {}

  track(...urns: string[]): void {
    this.trackedUrns.push(...urns);
  }

  async flush(testStatus?: string): Promise<void> {
    const shouldClean =
      testStatus === undefined || testStatus === 'passed' || testStatus === 'skipped';

    if (!shouldClean) {
      console.log(
        `[cleanup] Test status is '${testStatus}' — preserving ${this.trackedUrns.length} entities for post-failure investigation`,
      );
      if (this.trackedUrns.length > 0) {
        console.log(`[cleanup] URNs to clean up manually:\n  ${this.trackedUrns.join('\n  ')}`);
      }
      return;
    }

    if (this.trackedUrns.length === 0) return;

    console.log(`[cleanup] Deleting ${this.trackedUrns.length} tracked entities...`);
    await deleteEntities(this.request, this.gmsUrl, this.trackedUrns);
    this.trackedUrns.length = 0;
  }
}

// ── Standalone helper (used by global cleanup scripts) ────────────────────────

/**
 * Delete a list of entity URNs via the DataHub GMS REST API.
 *
 * This is the building block used by both `ApiScopedCleanup.flush()` and
 * the standalone `scripts/cleanup.ts` script that runs outside Playwright.
 *
 * @param request  - An `APIRequestContext` authenticated with a GMS token.
 * @param gmsUrl   - Base URL of the GMS service (e.g. `http://localhost:8080`).
 * @param urns     - Entity URNs to hard-delete.
 */
export async function deleteEntities(
  request: APIRequestContext,
  gmsUrl: string,
  urns: string[],
): Promise<void> {
  const failures: string[] = [];

  await Promise.all(
    urns.map(async (urn) => {
      const response = await request.post(`${gmsUrl}/entities?action=delete`, {
        data: { urn },
        headers: { 'Content-Type': 'application/json' },
        failOnStatusCode: false,
      });
      if (!response.ok()) {
        failures.push(`${urn} (${response.status()})`);
      }
    }),
  );

  if (failures.length > 0) {
    console.warn(`[cleanup] Failed to delete ${failures.length} entities:\n  ${failures.join('\n  ')}`);
  } else {
    console.log(`[cleanup] Deleted ${urns.length} entities`);
  }
}
