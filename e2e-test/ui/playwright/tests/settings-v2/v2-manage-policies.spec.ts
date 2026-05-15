/**
 * Manage Policies (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_policies.js
 */

import { test } from '../../fixtures/base-test';
import { PoliciesPage } from '../../pages/settings/policies.page';

test.use({ featureName: 'settings-v2' });

const testId = Math.floor(Math.random() * 100000);
const platformPolicyName = `Platform test policy ${testId}`;
const platformPolicyEdited = `Platform test policy ${testId} EDITED`;
const metadataPolicyName = `Metadata test policy ${testId}`;
const metadataPolicyEdited = `Metadata test policy ${testId} EDITED`;

// Tests share server-side state (policies created in earlier steps are used later)
test.describe.configure({ mode: 'serial' });

test.describe('create and manage platform and metadata policies', () => {
  let policiesPage: PoliciesPage;

  test.beforeEach(async ({ page }) => {
    policiesPage = new PoliciesPage(page);
    await policiesPage.navigate();
  });

  test('verify create, edit, delete platform policy', async () => {
    await policiesPage.waitForReady();
    await policiesPage.openNewPolicyWizard();
    await policiesPage.fillPolicyName(platformPolicyName);
    await policiesPage.selectPlatformType();
    await policiesPage.fillAndSaveWizard(`Platform policy description ${testId}`, platformPolicyName);
    await policiesPage.editPolicy(
      platformPolicyName,
      platformPolicyEdited,
      `Platform policy description ${testId} EDITED`,
    );
    await policiesPage.deletePolicy(platformPolicyEdited, `Delete ${platformPolicyEdited}`);
  });

  test('verify create, edit, delete metadata policy', async () => {
    await policiesPage.waitForReady();
    await policiesPage.openNewPolicyWizard();
    await policiesPage.fillPolicyName(metadataPolicyName);
    await policiesPage.verifyDefaultPolicyType('Metadata');
    await policiesPage.fillAndSaveWizard(`Metadata policy description ${testId}`, metadataPolicyName);
    await policiesPage.editPolicy(
      metadataPolicyName,
      metadataPolicyEdited,
      `Metadata policy description ${testId} EDITED`,
    );
    await policiesPage.deletePolicy(metadataPolicyEdited, `Delete ${metadataPolicyEdited}`);
  });
});
