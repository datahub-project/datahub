/**
 * Manage Service Accounts (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_service_accounts.js
 *
 * Cleanup: Tracks created service accounts via cleanup fixture and performs manual UI-based
 * deletion as fallback (fixture API cleanup may fail with 401 authorization).
 */

import { test } from '../../fixtures/base-test';
import { ServiceAccountsPage } from '../../pages/settings/service-accounts.page';

test.use({ featureName: 'settings-v2' });

function getUniqueTestId(): number {
  return Math.floor(Math.random() * 100000);
}

test.describe('manage service accounts', () => {
  let serviceAccountsPage: ServiceAccountsPage;

  test.beforeEach(async ({ page, apiMock }) => {
    await apiMock.setFeatureFlags({
      tokenAuthEnabled: true,
      showNavBarRedesign: true,
      manageServiceAccounts: true,
    });
    serviceAccountsPage = new ServiceAccountsPage(page);
    await serviceAccountsPage.navigate();
  });

  test('create, generate token, and delete service account', async ({ cleanup }) => {
    const id = getUniqueTestId();
    const name = `Test Service Account New UI ${id}`;

    const serviceAccountUrn = await serviceAccountsPage.createServiceAccount(name, `New UI test service account ${id}`);
    cleanup.track(serviceAccountUrn);
    await serviceAccountsPage.generateToken(name, `Test Token New UI ${id}`, `New UI test token ${id}`);
  });

  test('cancel creating service account should not create one', async () => {
    const id = getUniqueTestId();
    await serviceAccountsPage.cancelCreateServiceAccount(`Cancel Test ${id}`);
  });

  test('cancel deleting service account should keep it in the list', async ({ cleanup }) => {
    const id = getUniqueTestId();
    const name = `Keep Test ${id}`;

    const serviceAccountUrn = await serviceAccountsPage.createServiceAccount(name, `Keep test ${id}`);
    cleanup.track(serviceAccountUrn);
    await serviceAccountsPage.cancelDeleteServiceAccount(name);

    // Manual cleanup
    await serviceAccountsPage.deleteServiceAccount(name);
  });
});
