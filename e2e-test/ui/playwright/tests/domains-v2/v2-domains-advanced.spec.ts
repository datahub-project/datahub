/**
 * Domains V2 Advanced Functionality Tests
 *
 * Tests advanced domain operations:
 * - Move domain to parent domain (hierarchy)
 * - Edit domain name
 * - Add documentation
 * - Add related links/references
 * - Add owners
 *
 * Each test creates a unique test domain, performs an operation, then cleans up.
 *
 * Prerequisites:
 * - Marketing domain must exist in the system
 *
 * Related files:
 * - v2-domains-core.spec.ts — Domain creation and navigation
 * - v2-domains-summary.spec.ts — Summary tab content verification
 */

import { test } from '../../fixtures/base-test';
import { NestedDomainsPage } from '../../pages/domains/nested-domains.page';
import { withRandomSuffix } from '../../utils/random';
import type { ScopedCleanup } from '../../utils/cleanup';

const MARKETING_DOMAIN_NAME = 'Marketing';
const TEST_DOCUMENTATION = 'Test documentation for domains';
const TEST_LINK_URL = 'https://example.com';
const TEST_LINK_LABEL = 'Example Link';
const TEST_OWNER_NAME = 'datahub';
const LOAD_STATE_DOMCONTENTLOADED = 'domcontentloaded';
const LOAD_STATE_LOAD = 'load';
const DELAY_XL = 2000;

// Helper to create domain, navigate to it, and run a test operation with automatic cleanup
async function runDomainTest(
  page: import('@playwright/test').Page,
  logger: import('../../utils/logger').DataHubLogger | undefined,
  cleanup: ScopedCleanup,
  testName: string,
  testOperation: (domainsPage: NestedDomainsPage) => Promise<void>,
): Promise<void> {
  const domainsPage = new NestedDomainsPage(page, logger);
  const testDomainName = withRandomSuffix(testName);

  // Create domain
  await page.goto('/domains', { waitUntil: LOAD_STATE_DOMCONTENTLOADED });
  await page.waitForLoadState(LOAD_STATE_LOAD);
  await page.waitForTimeout(DELAY_XL);

  const domainUrn = await domainsPage.createDomain(testDomainName);
  cleanup.track(domainUrn);

  // Navigate to domain by clicking the link (natural navigation)
  const domainLink = page.getByRole('link').filter({ hasText: testDomainName }).first();
  await domainLink.waitFor({ state: 'visible', timeout: 10000 });
  await domainLink.click();
  await page.waitForLoadState(LOAD_STATE_LOAD);
  await page.waitForTimeout(DELAY_XL);

  // Run test-specific operation
  await testOperation(domainsPage);
}

test.describe('Domains V2 Advanced Functionality', () => {
  test('Verify Move domain to parent', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'MoveTest', async (domainsPage) => {
      await domainsPage.moveDomainToParent(MARKETING_DOMAIN_NAME);
    });
  });

  test('Verify Edit domain name', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'EditTest', async (domainsPage) => {
      const newName = `Edited${Date.now()}`;
      await domainsPage.editDomainName(newName);
    });
  });

  test('Verify Add documentation to domain', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'DocsTest', async (domainsPage) => {
      await domainsPage.addDocumentation(TEST_DOCUMENTATION);
    });
  });

  test('Verify Add link to domain', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'LinkTest', async (domainsPage) => {
      await domainsPage.addLink(TEST_LINK_URL, TEST_LINK_LABEL);
    });
  });

  test('Verify Add owner to domain', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'OwnerTest', async (domainsPage) => {
      await domainsPage.addOwner(TEST_OWNER_NAME);
    });
  });
});
