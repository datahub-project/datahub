/**
 * Domains V2 tests — migrated from Cypress e2e/mutationsV2/v2_domains.js
 *
 * Tests domain creation, entity assignment, entity removal, and domain deletion.
 * Uses apiMock.setFeatureFlags to toggle nestedDomainsEnabled and showNavBarRedesign.
 *
 * Prerequisites: BigQuery dataset `cypress_project.jaffle_shop.customers` and
 * container `348c96555971d3f5c1ffd7dd2e7446cb` must exist in the test environment.
 */

import { test, expect } from '../../fixtures/base-test';
import { DomainsPage } from '../../pages/domains.page';

test.use({ featureName: 'mutations-v2' });

// Shared domain identifiers across tests in this suite
const testDomainId = Math.floor(Math.random() * 100000);
const testDomain = `PlaywrightDomainTest ${testDomainId}`;
const testDomainUrn = `urn:li:domain:${testDomainId}`;

const CUSTOMERS_URN = 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)';
const CONTAINER_URN = 'urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb';

// Tests share server-side state (domain that is created in test 1 is used by tests 2-4).
test.describe.configure({ mode: 'serial' });

test.describe('add remove domain', () => {
  test.beforeEach(async ({ apiMock }) => {
    // Ensure the appConfig query is intercepted for feature flag control
    await apiMock.setFeatureFlags({ showNavBarRedesign: true });
  });

  test('create domain', async ({ page, logger, logDir, apiMock }) => {
    // Enable nested domains for the create test
    await apiMock.setFeatureFlags({ nestedDomainsEnabled: true, showNavBarRedesign: true });

    const domainsPage = new DomainsPage(page, logger, logDir);
    await domainsPage.navigate();
    await domainsPage.createDomain(testDomain, testDomainId);

    await expect(page.getByText(testDomain).first()).toBeVisible({ timeout: 15000 });
  });

  test('add entities to domain', async ({ page, logger, logDir, apiMock }) => {
    // Extra budget for the 90-second entity-search retry loop (ES indexing lag after seeding)
    test.setTimeout(120000);
    await apiMock.setFeatureFlags({ nestedDomainsEnabled: false, showNavBarRedesign: true });

    const domainsPage = new DomainsPage(page, logger, logDir);
    // Navigate directly to the domain page to avoid ES indexing lag on the list search
    await domainsPage.navigateToDomain(testDomainUrn);

    await domainsPage.addEntitiesToDomain('cypress_project.jaffle_shop.customers', CUSTOMERS_URN);
  });

  test('remove entity from domain', async ({ page, logger, logDir, apiMock }) => {
    await apiMock.setFeatureFlags({ nestedDomainsEnabled: true, showNavBarRedesign: true });

    const domainsPage = new DomainsPage(page, logger, logDir);
    await domainsPage.navigate();
    await domainsPage.removeDomainFromDataset(CUSTOMERS_URN, 'customers', testDomainUrn);
  });

  test('delete a domain and ensure dangling reference is deleted on entities', async ({
    page,
    logger,
    logDir,
    apiMock,
  }) => {
    // This test navigates across multiple pages, adds entities, deletes a domain,
    // and waits for ES de-indexing — needs more than the default 30-second timeout.
    test.setTimeout(3 * 60 * 1000);
    await apiMock.setFeatureFlags({ nestedDomainsEnabled: true, showNavBarRedesign: true });

    const domainsPage = new DomainsPage(page, logger, logDir);

    // Assign the container to the domain so we can verify the dangling reference is cleaned up
    await domainsPage.navigateToDomain(testDomainUrn);
    await domainsPage.addEntitiesToDomain('jaffle_shop', CONTAINER_URN);

    await domainsPage.deleteDomain(testDomainUrn, testDomain);

    // Navigate to the list to verify the domain card is gone
    await domainsPage.navigate();
    await domainsPage.expectDomainNotVisible(testDomain);

    // Navigate to the container and confirm the domain reference is gone
    await domainsPage.navigateToContainer(CONTAINER_URN);
    await expect(page.getByText('customers')).toBeVisible({ timeout: 30000 });
    await expect(page.getByText(testDomain)).not.toBeVisible({ timeout: 10000 });
  });
});
