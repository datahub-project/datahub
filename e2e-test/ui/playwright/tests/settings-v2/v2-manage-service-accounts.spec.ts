/**
 * Manage Service Accounts (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_service_accounts.js
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

  test('create, generate token, and delete service account', async () => {
    const id = getUniqueTestId();
    const name = `Test Service Account New UI ${id}`;

    await serviceAccountsPage.createServiceAccount(name, `New UI test service account ${id}`);
    await serviceAccountsPage.generateToken(name, `Test Token New UI ${id}`, `New UI test token ${id}`);

    await serviceAccountsPage.navigate();
    await serviceAccountsPage.deleteServiceAccount(name);
  });

  test('cancel creating service account should not create one', async () => {
    const id = getUniqueTestId();
    await serviceAccountsPage.cancelCreateServiceAccount(`Cancel Test ${id}`);
  });

  test('cancel deleting service account should keep it in the list', async () => {
    const id = getUniqueTestId();
    const name = `Keep Test ${id}`;

    await serviceAccountsPage.createServiceAccount(name, `Keep test ${id}`);
    await serviceAccountsPage.cancelDeleteServiceAccount(name);
    await serviceAccountsPage.deleteServiceAccount(name);
  });
});
