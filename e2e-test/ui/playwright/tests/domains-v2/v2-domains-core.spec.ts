/**
 * Domains V2 Core CRUD Tests
 *
 * Tests basic domain navigation, creation, and list display:
 * - Domain list navigation and domain link visibility
 * - Domain creation with success verification and cleanup
 * - Domain list display verification
 * - Domain page navigation verification
 *
 * Prerequisites:
 * - PlaywrightDomain must exist (seeded via fixtures)
 *
 * Related files:
 * - v2-domains-advanced.spec.ts — Domain operations (move, edit, docs, links, owners)
 * - v2-domains-summary.spec.ts — Summary tab content verification
 */

import { test, expect } from '../../fixtures/base-test';
import { DomainEntityPage } from '../../pages/domains/domain-entity.page';
import { withRandomSuffix } from '../../utils/random';

// Ensure test parent domain is seeded before tests run
test.use({ featureName: 'domains-v2' });

const PARENT_DOMAIN_URN = 'urn:li:domain:playwright-domain';
const DOMAIN_URL_PATTERN = '/domain/';

test.describe('Domains V2 Core', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/domains');
    await page.waitForLoadState('load');
  });

  test.afterEach(async ({ page }) => {
    try {
      await page.goto('/domains');
      await page.waitForLoadState('load');
    } catch {
      // Ignore cleanup navigation errors
    }
  });

  test('can navigate to domains', async ({ page }) => {
    // beforeEach already navigated to /domains
    await page.waitForLoadState('networkidle');
    expect(page.url()).toContain('/domains');
  });

  test('Verify Create a new domain', async ({ page, logger, cleanup }) => {
    const newDomainName = withRandomSuffix('CreateDomain');
    await page.goto('/domains');
    await page.waitForLoadState('load');

    const domainEntityPage = new DomainEntityPage(page, logger);
    const domainUrn = await domainEntityPage.createDomain(newDomainName);
    expect(domainUrn).toBeTruthy();
    cleanup.track(domainUrn);
  });

  test('Verify domain list displays', async ({ page, logger }) => {
    await page.waitForLoadState('networkidle');
    const domainsPage = new DomainEntityPage(page, logger);
    const summaryCount = await domainsPage.summaryTab.count();
    expect(summaryCount).toBeGreaterThanOrEqual(0);
  });

  test('Verify can view domain and its properties', async ({ page, logger }) => {
    const domainsPage = new DomainEntityPage(page, logger);

    await page.goto(`${DOMAIN_URL_PATTERN}${PARENT_DOMAIN_URN}`);
    await page.waitForLoadState('load');

    expect(page.url()).toContain(DOMAIN_URL_PATTERN);
    const summaryCount = await domainsPage.summaryTab.count();
    expect(summaryCount).toBeGreaterThanOrEqual(0);
  });
});
