/**
 * Summary tab tests — migrated from Cypress e2e/summaryTab/*.js
 *
 * Covers: about section CRUD, properties display, template modules for
 * DataProduct, Domain, Glossary Node, and Glossary Term entities.
 *
 * Prerequisites (seeded via fixtures/data.json and existing test data):
 *   - urn:li:dataProduct:testing must exist with correct metadata
 *   - urn:li:domain:testing must exist with correct metadata
 *   - urn:li:glossaryNode:CypressNode must exist
 *   - urn:li:glossaryTerm:CypressNode.CypressTerm must exist
 *
 * Feature flags required: showNavBarRedesign=true, assetSummaryPageV1=true
 */

import { test, expect } from '../../fixtures/base-test';
import { BaseEntityPage } from '../../pages/entity/base-entity.page';

test.use({ featureName: 'entity-pages' });

// ── Test suites ───────────────────────────────────────────────────────────────

test.describe('summary tab - about section', () => {
  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true, assetSummaryPageV1: true });
    await page.goto('/dataProduct/urn:li:dataProduct:testing');
    await page.waitForLoadState('networkidle');
  });

  test('about section', async ({ page }) => {
    const entityPage = new BaseEntityPage(page);
    await entityPage.summary.open();
    const tab = entityPage.summary;

    await tab.expectAboutSectionVisible();
    await tab.updateDescription('description');
    await tab.expectDescriptionContains('description');
    await tab.updateDescription('updated description');
    await tab.expectDescriptionContains('updated description');

    await tab.addLink('https://test.com', 'testLink');
    await tab.expectLinkExists('https://test.com', 'testLink');

    await tab.updateLink('https://test.com', 'testLink', 'https://test-updated.com', 'testLinkUpdated');
    await tab.expectLinkExists('https://test-updated.com', 'testLinkUpdated');

    await tab.removeLink('https://test-updated.com', 'testLinkUpdated');
    await expect(tab.expectLinkNotExists('https://test-updated.com', 'testLinkUpdated')).not.toBeAttached({
      timeout: 10000,
    });
  });
});

test.describe('summary tab - data product', () => {
  const TEST_DOMAIN_NAME = 'Testing';
  const TEST_TAG_NAME = 'Cypress';
  const TEST_TERM_NAME = 'CypressTerm';
  const TEST_ASSET_NAME = 'Baz Dashboard';

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true, assetSummaryPageV1: true });
    await page.goto('/dataProduct/urn:li:dataProduct:testing');
    await page.waitForLoadState('networkidle');
  });

  test('summary tab', async ({ page }) => {
    const entityPage = new BaseEntityPage(page);
    await entityPage.summary.open();
    const tab = entityPage.summary;

    await tab.properties.expectVisible();
    for (const type of ['CREATED', 'OWNERS', 'DOMAIN', 'TAGS', 'GLOSSARY_TERMS']) {
      await tab.properties.expectPropertyVisible(type);
    }
    await tab.properties.expectPropertyContains('CREATED', 'Created');
    await tab.properties.expectPropertyContains('OWNERS', 'Owners');
    await tab.properties.expectPropertyContains('DOMAIN', 'Domain', TEST_DOMAIN_NAME);
    await tab.properties.expectPropertyContains('TAGS', 'Tags', TEST_TAG_NAME);
    await tab.properties.expectPropertyContains('GLOSSARY_TERMS', 'Glossary Terms', TEST_TERM_NAME);

    await tab.expectAboutSectionVisible();

    await tab.template.expectVisible();
    await tab.template.expectModuleExists('assets', 'Assets', TEST_ASSET_NAME);
  });
});

test.describe('summary tab - domain', () => {
  const TEST_USER_URN = 'urn:li:corpuser:jdoe';
  const TEST_ASSET_NAME = 'Baz Dashboard';
  const TEST_DOMAIN_URN = 'urn:li:domain:testing';
  const TEST_SUBDOMAIN_NAME = 'Subdomain';
  const TEST_DATA_PRODUCT_NAME = 'Testing';

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true, assetSummaryPageV1: true });
    await page.goto(`/domain/${TEST_DOMAIN_URN}`);
    await page.waitForLoadState('networkidle');
  });

  test('summary tab', async ({ page }) => {
    const entityPage = new BaseEntityPage(page);
    await entityPage.summary.open();
    const tab = entityPage.summary;

    await tab.properties.expectVisible();
    await tab.properties.expectPropertyContains('CREATED', 'Created');
    await tab.properties.expectPropertyContains('OWNERS', 'Owners', undefined, `owner-${TEST_USER_URN}`);

    await tab.expectAboutSectionVisible();

    await tab.template.expectVisible();
    await tab.template.expectModuleExists('assets', 'Assets', TEST_ASSET_NAME);
    await tab.template.expectModuleExists('hierarchy', 'Domains', TEST_SUBDOMAIN_NAME);
    await tab.template.expectModuleExists('data-products', 'Data Products', TEST_DATA_PRODUCT_NAME);
  });
});

test.describe('summary tab - glossary node', () => {
  const TEST_USER_URN = 'urn:li:corpuser:jdoe';
  const TEST_GLOSSARY_NODE_URN = 'urn:li:glossaryNode:CypressNode';
  const TEST_GLOSSARY_TERM_NAME = 'CypressTerm';

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true, assetSummaryPageV1: true });
    await page.goto(`/glossaryNode/${TEST_GLOSSARY_NODE_URN}`);
    await page.waitForLoadState('networkidle');
  });

  test('summary tab', async ({ page }) => {
    const entityPage = new BaseEntityPage(page);
    await entityPage.summary.open();
    const tab = entityPage.summary;

    await tab.properties.expectVisible();
    await tab.properties.expectPropertyContains('CREATED', 'Created');
    await tab.properties.expectPropertyContains('OWNERS', 'Owners', undefined, `owner-${TEST_USER_URN}`);

    await tab.expectAboutSectionVisible();

    await tab.template.expectVisible();
    await tab.template.expectModuleExists('hierarchy', 'Contents', TEST_GLOSSARY_TERM_NAME);
  });
});

test.describe('summary tab - glossary term', () => {
  const TEST_USER_URN = 'urn:li:corpuser:jdoe';
  const TEST_GLOSSARY_TERM_URN = 'urn:li:glossaryTerm:CypressNode.CypressTerm';
  const TEST_DOMAIN_NAME = 'Testing';
  const TEST_RELATED_TERM_NAME = 'RelatedCypressTerm';

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showNavBarRedesign: true, assetSummaryPageV1: true });
    await page.goto(`/glossaryTerm/${TEST_GLOSSARY_TERM_URN}`);
    await page.waitForLoadState('networkidle');
  });

  test('glossary term - header section', async ({ page }) => {
    const entityPage = new BaseEntityPage(page);
    await entityPage.summary.open();
    const tab = entityPage.summary;

    await tab.properties.expectVisible();
    await tab.properties.expectPropertyContains('CREATED', 'Created');
    await tab.properties.expectPropertyContains('OWNERS', 'Owners', undefined, `owner-${TEST_USER_URN}`);
    await tab.properties.expectPropertyContains('DOMAIN', 'Domain', TEST_DOMAIN_NAME);

    await tab.expectAboutSectionVisible();

    await tab.template.expectVisible();
    await tab.template.expectModuleExists('assets', 'Assets', TEST_DOMAIN_NAME);
    await tab.template.expectModuleExists('related-terms', 'Related Terms', TEST_RELATED_TERM_NAME);
  });
});
