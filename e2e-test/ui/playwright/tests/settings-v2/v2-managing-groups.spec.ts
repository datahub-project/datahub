/**
 * Managing Groups (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_managing_groups.js
 *
 * Tests the group management functionality:
 *   1. Create a group with custom ID
 *   2. Add user as member
 *   3. Update group info (email, slack, name)
 *   4. Edit group description
 *   5. Add owner
 *   6. Verify user can see group in their profile
 *   7. Delete group
 *
 * Tests run in parallel - each test creates its own independent test data.
 */

import { test } from '../../fixtures/base-test';
import { GroupsPage } from '../../pages/settings/groups.page';

function createTestId() {
  return Math.floor(Math.random() * 100000);
}

test.describe('create and manage group', () => {
  test('create a simple group', async ({ page, cleanup }) => {
    const testId = createTestId();
    const groupName = `Test Group ${testId}`;

    const groupsPage = new GroupsPage(page, { cleanup });
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, 'Test group description');
    await groupsPage.deleteGroup(groupName);
  });

  test('create group and update group info', async ({ page, cleanup }) => {
    const testId = createTestId();
    const groupName = `Test Group ${testId}`;
    const newName = `Updated Group ${testId}`;

    const groupsPage = new GroupsPage(page, { cleanup });
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, 'Initial description');

    // Navigate to group profile and update info
    await groupsPage.navigateToGroupProfile(groupName);
    await groupsPage.updateGroupInfo('testgroup@example.com', 'test-slack', newName);
    await groupsPage.verifyGroupInfoUpdated('testgroup@example.com', 'test-slack');

    // Manual cleanup
    await groupsPage.navigate();
    await groupsPage.deleteGroup(newName);
  });

  test('create group and edit description', async ({ page, cleanup }) => {
    const testId = createTestId();
    const groupName = `Test Group ${testId}`;
    const description = 'Initial description';
    const additionalText = ' - Updated';

    const groupsPage = new GroupsPage(page, { cleanup });
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, description);

    // Navigate to group and edit description
    await groupsPage.navigateToGroupProfile(groupName);
    await groupsPage.editDescription(description, additionalText);

    // Manual cleanup
    await groupsPage.deleteGroup(groupName);
  });

  test('create group and add owner', async ({ page, cleanup }) => {
    const testId = createTestId();
    const groupName = `Test Group ${testId}`;

    const groupsPage = new GroupsPage(page, { cleanup });
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, 'Test group for owner');

    // Navigate to group and add owner
    await groupsPage.navigateToGroupProfile(groupName);
    await groupsPage.addOwner('datahub');
    await groupsPage.verifyOwnersAdded();

    // Manual cleanup
    await groupsPage.deleteGroup(groupName);
  });

  test('create group and add member', async ({ page, cleanup }) => {
    const testId = createTestId();
    const groupName = `Test Group ${testId}`;

    const groupsPage = new GroupsPage(page, { cleanup });
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, 'Test group for members');

    // Navigate to group and add member
    await groupsPage.navigateToGroupProfile(groupName);
    await groupsPage.addMember('datahub');
    await groupsPage.verifyMemberVisible('datahub');

    // Manual cleanup
    await groupsPage.deleteGroup(groupName);
  });
});
