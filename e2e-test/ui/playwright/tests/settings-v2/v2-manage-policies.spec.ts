/**
 * Manage Policies (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_policies.js
 */

import { test, expect } from '../../fixtures/base-test';
import { PoliciesPage } from '../../pages/settings/policies.page';
import { withRandomSuffix } from '../../utils/random';
import { TIMEOUTS } from '../../utils/constants';

// Both tests mutate the same global, un-namespaced system policy list with no
// per-test isolation; running them concurrently on separate workers causes
// collisions on shared backend state.
test.describe.configure({ mode: 'serial' });

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

  test('verify conditions and resource values persist after save', async ({ page }) => {
    const policyName = withRandomSuffix('Policy Conditions Persist');

    await policiesPage.waitForReady();
    await policiesPage.openNewPolicyWizard();
    await policiesPage.fillPolicyName(policyName);
    await policiesPage.fillDescriptionAndMoveToPrivilegeForm('Test conditions and resource values');

    // Set resource type condition
    await policiesPage.scrollConditionIntoView('TYPE');
    await policiesPage.setResourceTypeCondition('NotEquals');
    await expect(policiesPage.resourceTypeConditionSelect).toHaveText('Not Equals');

    // Select resource type value
    await policiesPage.resourceTypeBase.click();
    await policiesPage.datasetOption.click();
    await policiesPage.resourceTypeBase.click();
    await expect(policiesPage.resourceTypeBase).toContainText('Datasets');

    // Complete and save form
    await page.getByTestId('privileges').scrollIntoViewIfNeeded();
    await policiesPage.privilegesInput.waitFor({ state: 'visible', timeout: TIMEOUTS.MEDIUM });
    await policiesPage.privilegesInput.click();
    await policiesPage.allPrivilegesOption.click();

    await policiesPage.nextButton.click();

    await policiesPage.usersInput.scrollIntoViewIfNeeded();
    await policiesPage.usersInput.click();
    await policiesPage.allUsersOption.click();
    await policiesPage.usersInput.click();

    await policiesPage.groupsInput.scrollIntoViewIfNeeded();
    await policiesPage.groupsInput.click();
    await policiesPage.allGroupsOption.click();
    await policiesPage.groupsInput.click();

    await policiesPage.saveButton.scrollIntoViewIfNeeded();
    await policiesPage.saveButton.click();
    await expect(policiesPage.successToastMessage).toBeVisible({ timeout: TIMEOUTS.SHORT });
    await expect(policiesPage.successToastMessage).toBeHidden({ timeout: TIMEOUTS.SHORT });

    // Reopen and verify resource type value persisted
    await policiesPage.navigate();
    await policiesPage.searchForPolicy(policyName);
    await expect(page.getByText(policyName, { exact: true })).toBeVisible({ timeout: TIMEOUTS.MEDIUM });
    await policiesPage.openRowMenu(policyName);
    await policiesPage.clickMenuAction('Edit');

    await policiesPage.nextButton.click();

    await expect(policiesPage.resourceTypeConditionSelect).toHaveText('Not Equals');
    await expect(policiesPage.resourceTypeBase).toContainText('Datasets');

    // Cleanup
    await policiesPage.deletePolicy(policyName, `Delete ${policyName}`);
  });
});
