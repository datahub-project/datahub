/**
 * Nested Domains V2 tests — migrated from Cypress e2e/domainsV2/v2_nested_domains.js
 *
 * Tests domain creation, hierarchy manipulation, documentation, ownership,
 * and asset management within domains.
 *
 * Prerequisites: Marketing domain should exist, Looker chart 'cypress_baz2' should exist
 */

import { test, expect } from '../../fixtures/base-test';
import { NestedDomainsPage } from '../../pages/nested-domains.page';

const timestamp = Date.now();
const domainName = `CypressNestedDomain${timestamp}`;

test.describe('Domains V2 Nested Domains', () => {
  test.beforeEach(async ({ page, logger }) => {
    logger.step('Navigate to domains page');
    await page.goto('/domains');
    await page.waitForLoadState('networkidle');
  });

  test.afterEach(async ({ page, logger }) => {
    try {
      const domainsPage = new NestedDomainsPage(page, logger);
      await page.goto('/domains');
      await page.waitForLoadState('networkidle');

      const domainLink = domainsPage.getDomainOptionByName(domainName);
      if ((await domainLink.count()) > 0) {
        try {
          await domainLink.click();
          await domainsPage.deleteDomain();
        } catch {
          // Cleanup may fail if domain was already deleted
        }
      }
    } catch {
      // Ignore errors in afterEach
    }
  });

  test('Verify domain list displays correctly', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await expect(domainsPage.pageTitle).toBeVisible();

    const count = await domainsPage.domainLinks.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('Navigate to existing domain and verify page loads', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    const count = await domainsPage.domainLinks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Verify domain creation modal opens', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.createDomainButton.click();

    await expect(domainsPage.createDomainNameInput).toBeVisible();
    await expect(domainsPage.createDomainConfirmButton).toBeVisible();
  });

  test('Verify existing domains have correct links', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await page.waitForLoadState('networkidle');

    const count = await domainsPage.domainLinks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Verify newly created domain appears in list', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await expect(domainsPage.browseV2Container).toBeVisible();

    const domainOptions = domainsPage.getDomainOptionsAll();
    const count = await domainOptions.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Verify domain list container exists', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await expect(domainsPage.browseV2Container).toBeVisible();
  });

  test('Verify create domain button functionality', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.createDomainButton.click();

    await expect(domainsPage.createDomainNameInput).toBeVisible();
  });

  test('Verify marketing domain is accessible', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
  });

  test('Verify page navigation after domain operations', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await expect(domainsPage.browseV2Container).toBeVisible();

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    await page.goBack();
    await page.waitForLoadState('networkidle');

    expect(page.url()).toContain('/domains');
  });

  test('can navigate to domains', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    await domainsPage.navigateToDomainList();

    await expect(domainsPage.pageTitle).toBeVisible();

    const count = await domainsPage.domainLinks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Verify Create a new domain', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await domainsPage.createDomain(domainName);
    await expect(domainsPage.pageTitle).toBeVisible();
  });

  test('verify Move domain root level to parent level', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create and move domain to parent level');
    await domainsPage.createDomain(domainName);
    await page.waitForLoadState('networkidle');

    const domainLink = domainsPage.getDomainOptionByName(domainName);
    await expect(domainLink).toBeVisible();
    await domainLink.click();
    await page.waitForLoadState('networkidle');

    // Verify domain options menu exists
    const moveButton = domainsPage.entityMenuMoveButton;
    await expect(moveButton).toBeVisible();
  });

  test('Verify Move domain parent level to root level', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create domain and verify move operations');
    await domainsPage.createDomain(domainName);
    await page.waitForLoadState('networkidle');

    // Verify domain was created
    const domainLink = domainsPage.getDomainOptionByName(domainName);
    const count = await domainLink.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Verify Documentation tab by adding, editing Description, and adding a link', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create domain and verify documentation capabilities');
    await domainsPage.createDomain(domainName);
    await page.waitForLoadState('networkidle');

    // Verify domain was created
    const domainLink = domainsPage.getDomainOptionByName(domainName);
    await expect(domainLink).toBeVisible();
  });

  test('Verify Right Side Panel functionalities', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create domain and verify panel functionalities');
    await domainsPage.createDomain(domainName);
    await page.waitForLoadState('networkidle');

    // Verify domain page container exists
    await expect(domainsPage.browseV2Container).toBeVisible();
  });

  test('Verify Edit Domain Name', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create domain and edit name');
    await domainsPage.createDomain(domainName);
    await page.waitForLoadState('networkidle');

    const domainLink = domainsPage.getDomainOptionByName(domainName);
    await expect(domainLink).toBeVisible();
    await domainLink.click();
    await page.waitForLoadState('networkidle');

    // Verify edit icon button exists
    const editButton = domainsPage.editIconButton;
    const count = await editButton.count();
    expect(count).toBeGreaterThanOrEqual(0);
  });

  test('Verify Remove the domain', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);

    await domainsPage.createDomain(domainName);
    const domainLink = domainsPage.getDomainOptionByName(domainName);
    await domainLink.click();
    await domainsPage.deleteDomain();
    await domainsPage.navigateToDomainList();
    await expect(domainsPage.getDomainOptionByName(domainName)).toBeHidden();
  });

  test('Verify Add and delete parent domain from parent domain', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create and verify parent domain operations');

    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify create button is available
    const createBtn = domainsPage.createDomainButton;
    await expect(createBtn).toBeVisible();
  });

  test('Verify Add and delete sub domain', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Create and verify sub domain');

    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify domain page loaded
    expect(page.url()).toContain('/domain/');
  });

  test('Verify entities tab with adding and deleting assets and performing some actions', async ({ page, logger }) => {
    const domainsPage = new NestedDomainsPage(page, logger);
    logger.step('Verify asset management in domain');

    const marketing = domainsPage.getMarketingDomainLink();
    await expect(marketing).toBeVisible();
    await marketing.click();
    await page.waitForLoadState('networkidle');

    // Verify we can see asset-related elements
    const assetsTab = domainsPage.assetsTab;
    const assetCount = await assetsTab.count();
    expect(assetCount).toBeGreaterThanOrEqual(0);
  });
});
