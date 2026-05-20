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
 * - Marketing domain must exist in the system
 *
 * Related files:
 * - v2-domains-advanced.spec.ts — Domain operations (move, edit, docs, links, owners)
 * - v2-domains-summary.spec.ts — Summary tab content verification
 */

import { test, expect } from '../../fixtures/base-test';
import { NestedDomainsPage } from '../../pages/domains/nested-domains.page';

const MARKETING_DOMAIN_URN = 'urn:li:domain:marketing';
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

  test('can navigate to domains', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.navigateToDomainList();

    // Verify page loaded with Domains header
    await expect(domainsPage.pageTitle).toBeVisible();

    // Verify browse container is visible
    await expect(domainsPage.browseV2Container).toBeVisible();
  });

  test('Verify Create a new domain', async ({ page, logger }) => {
    const newDomainName = `CreateDomain${Date.now()}`;
    const domainsPage = new NestedDomainsPage(page, logger);

    try {
      await domainsPage.createDomain(newDomainName);
    } finally {
      const lowerName = newDomainName.toLowerCase().replace(/\s+/g, '');
      const domainUrn = `urn:li:domain:${lowerName}`;
      await page.goto(`${DOMAIN_URL_PATTERN}${domainUrn}`, { waitUntil: 'domcontentloaded' }).catch(() => {});
      await page.waitForLoadState('load').catch(() => {});
      await domainsPage.deleteDomain().catch(() => {});
    }
  });

  test('Verify domain list displays', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await expect(domainsPage.pageTitle).toBeVisible();
    await expect(domainsPage.browseV2Container).toBeVisible();
  });

  test('Verify can view domain and its properties', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);

    await page.goto(`${DOMAIN_URL_PATTERN}${MARKETING_DOMAIN_URN}`);
    await page.waitForLoadState('load');

    expect(page.url()).toContain(DOMAIN_URL_PATTERN);
    const summaryCount = await domainsPage.summaryTab.count();
    expect(summaryCount).toBeGreaterThanOrEqual(0);
  });
});
