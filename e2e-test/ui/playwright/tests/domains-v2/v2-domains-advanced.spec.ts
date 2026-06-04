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
 * - PlaywrightDomain must exist (seeded via fixtures)
 *
 * Related files:
 * - v2-domains-core.spec.ts — Domain creation and navigation
 * - v2-domains-summary.spec.ts — Summary tab content verification
 */

import { test } from '../../fixtures/base-test';
import { DomainEntityPage } from '../../pages/domains/domain-entity.page';
import { withRandomSuffix } from '../../utils/random';
import type { ScopedCleanup } from '../../utils/cleanup';
import { TIMEOUTS, LOAD_STATES } from '../../utils/constants';

// Ensure test parent domain is seeded before tests run
test.use({ featureName: 'domains-v2' });

const PARENT_DOMAIN_NAME = 'PlaywrightDomain';
const TEST_DOCUMENTATION = 'Test documentation for domains';
const TEST_LINK_URL = 'https://example.com';
const TEST_LINK_LABEL = 'Example Link';
const TEST_OWNER_NAME = 'datahub';

// Helper to create domain, navigate to it, and run a test operation with automatic cleanup
async function runDomainTest(
  page: import('@playwright/test').Page,
  logger: import('../../utils/logger').DataHubLogger | undefined,
  cleanup: ScopedCleanup,
  testName: string,
  testOperation: (domainEntityPage: DomainEntityPage) => Promise<void>,
): Promise<void> {
  const domainEntityPage = new DomainEntityPage(page, logger);
  const testDomainName = withRandomSuffix(testName);

  // Create domain
  await page.goto('/domains', { waitUntil: LOAD_STATES.DOMCONTENTLOADED });
  await page.waitForLoadState(LOAD_STATES.LOAD);

  const domainUrn = await domainEntityPage.createDomain(testDomainName);
  cleanup.track(domainUrn);

  // Navigate to domain by clicking the link (natural navigation)
  const domainLink = page.getByRole('link').filter({ hasText: testDomainName });
  await domainLink.waitFor({ state: 'visible', timeout: TIMEOUTS.SHORT });
  await domainLink.click();
  await page.waitForLoadState(LOAD_STATES.LOAD);

  // Wait for page to fully render after navigation before running operations
  await page.waitForLoadState(LOAD_STATES.NETWORKIDLE);

  // Run test-specific operation
  await testOperation(domainEntityPage);
}

test.describe('Domains V2 Advanced Functionality', () => {
  test('Verify Move domain to parent', async ({ page, logger, cleanup }) => {
    await runDomainTest(page, logger, cleanup, 'MoveTest', async (domainsPage) => {
      await domainsPage.moveDomainToParent(PARENT_DOMAIN_NAME);
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
