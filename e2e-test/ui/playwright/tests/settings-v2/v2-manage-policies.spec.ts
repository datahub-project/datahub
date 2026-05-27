/**
 * Manage Policies (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_policies.js
 */

import { test } from '../../fixtures/base-test';
import { PoliciesPage } from '../../pages/settings/policies.page';
import { withRandomSuffix } from '../../utils/random';

test.describe('create and manage platform and metadata policies', () => {
  let policiesPage: PoliciesPage;

  test.beforeEach(async ({ page, cleanup }) => {
    policiesPage = new PoliciesPage(page, { cleanup });
    await policiesPage.navigate();
  });

  test('verify create, edit, delete platform policy', async () => {
    const platformPolicyName = withRandomSuffix('Platform test policy');
    const platformPolicyEdited = withRandomSuffix('Platform test policy EDITED');

    await policiesPage.waitForReady();
    await policiesPage.openNewPolicyWizard();
    await policiesPage.fillPolicyName(platformPolicyName);
    await policiesPage.selectPlatformType();
    await policiesPage.fillAndSaveWizard(`Platform policy description ${platformPolicyName}`, platformPolicyName);
    await policiesPage.editPolicy(
      platformPolicyName,
      platformPolicyEdited,
      `Platform policy description ${platformPolicyEdited}`,
    );
    await policiesPage.deletePolicy(platformPolicyEdited, `Delete ${platformPolicyEdited}`);
  });

  test('verify create, edit, delete metadata policy', async () => {
    const metadataPolicyName = withRandomSuffix('Metadata test policy');
    const metadataPolicyEdited = withRandomSuffix('Metadata test policy EDITED');

    await policiesPage.waitForReady();
    await policiesPage.openNewPolicyWizard();
    await policiesPage.fillPolicyName(metadataPolicyName);
    await policiesPage.verifyDefaultPolicyType('Metadata');
    await policiesPage.fillAndSaveWizard(`Metadata policy description ${metadataPolicyName}`, metadataPolicyName);
    await policiesPage.editPolicy(
      metadataPolicyName,
      metadataPolicyEdited,
      `Metadata policy description ${metadataPolicyEdited}`,
    );
    await policiesPage.deletePolicy(metadataPolicyEdited, `Delete ${metadataPolicyEdited}`);
  });
});
