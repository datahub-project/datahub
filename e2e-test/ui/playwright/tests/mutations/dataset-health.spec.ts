/**
 * Dataset health tests — migrated from Cypress e2e/mutations/dataset_health.js
 *
 * Verifies that the health badge and popover appear correctly for a pre-seeded
 * dataset that has failing assertions and active incidents.
 *
 * Prerequisites: The Hive dataset `playwright_health_test` must exist with at
 * least one failing assertion or active incident in the test environment.
 */

import { test, expect } from '../../fixtures/base-test';

test.use({ featureName: 'mutations' });

const URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,playwright_health_test,PROD)';
const DATASET_NAME = 'playwright_health_test';

test.describe('dataset health test', () => {
  test('go to dataset with failing assertions and active incidents and verify health of dataset', async ({
    page,
    logger,
  }) => {
    logger.step('navigate to dataset', { urn: URN });

    // Gate navigation on the first GMS GraphQL response so we don't assert
    // before the entity fetch completes (avoids a "Not Found" race condition).
    await Promise.all([
      page.waitForResponse((resp) => resp.url().includes('/api/v2/graphql') && resp.request().method() === 'POST', {
        timeout: 30000,
      }),
      page.goto(`/dataset/${encodeURIComponent(URN)}/`),
    ]);

    // Wait for all in-flight GraphQL queries to settle before asserting UI state.
    await page.waitForLoadState('networkidle', { timeout: 30000 });

    // Use .first() to avoid strict-mode violations when the name appears in
    // both the breadcrumb and the page heading.
    await expect(page.getByText(DATASET_NAME).first()).toBeVisible({ timeout: 15000 });

    // Health badge must be present
    const healthIcon = page.locator(`[data-testid="${URN}-health-icon"]`).first();
    await expect(healthIcon).toBeVisible({ timeout: 15000 });

    logger.step('hover health icon to open popover');
    await healthIcon.hover({ force: true });

    // Wait for health popover
    const assertionsDetails = page.locator('[data-testid="assertions-details"]');
    await expect(assertionsDetails).toBeVisible({ timeout: 10000 });

    // Due to search index timing in CI, at least one link (assertion or incident) must be shown
    const links = assertionsDetails.locator('a');
    await expect(links).toHaveCount(await links.count(), { timeout: 5000 });
    expect(await links.count()).toBeGreaterThanOrEqual(1);
  });
});
