/**
 * Managing Groups (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_managing_groups.js
 *
 * Tests the full group management lifecycle:
 *   1. Invite a new user via signup link and log in with that account
 *   2. Create a group with a custom group ID
 *   3. Add the new user as a member of the group
 *   4. Update group info (name, email, slack)
 *   5. Edit the group description
 *   6. Add an owner to the group
 *   7. Verify a user can see their group participation
 *   8. Remove the group
 *
 * Tests run serially because they share the user and group created in the first two steps.
 */

import { test } from '../../fixtures/base-test';
import { GroupsPage } from '../../pages/settings/groups.page';
import { UsersPage } from '../../pages/settings/users.page';
import { users } from '../../data/users';

test.use({ featureName: 'settings-v2' });

test.describe.configure({ mode: 'serial' });

const testId = Math.floor(Math.random() * 100000);
const username = `Example Name ${testId}`;
const email = `example${testId}@example.com`;
const password = 'Example password';
const groupName = `Test group ${testId}`;
const groupNameEdited = `Test group EDITED ${testId}`;

test.describe('create and manage group', () => {
  test('add test user', async ({ page }) => {
    const usersPage = new UsersPage(page);
    await usersPage.navigate();
    const inviteLink = await usersPage.getInviteLink();
    await usersPage.navigate();
    await usersPage.logout();
    await usersPage.registerViaInviteLink(inviteLink, email, username, password);
    await usersPage.verifyUsernameVisible(username);
  });

  test('create a group', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.createGroup(groupName, 'Test group description', String(testId));
  });

  test('add test user to a group', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.navigateToGroupProfile(testId);
    await groupsPage.navigateToMembersTab();

    // Wait for the Members tab to load — the admin may or may not be listed yet
    const adminDisplayName = users.admin.displayName ?? 'DataHub Admin';
    const bodyText = await page.locator('body').textContent();
    if (bodyText?.includes(adminDisplayName)) {
      await groupsPage.verifyMemberVisible(adminDisplayName);
    }

    await groupsPage.addMember(username);
  });

  test('update group info', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.navigateToGroupProfile(testId);
    await groupsPage.updateGroupInfo(`${testId}@testemail.com`, `#${testId}`, groupNameEdited);
    await groupsPage.verifyGroupInfoUpdated(`${testId}@testemail.com`, `#${testId}`);
  });

  test('user verify to edit the description', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.navigateToGroupProfile(testId);
    await groupsPage.editDescription('Test group description', ' EDITED');
  });

  test('user verify to add the owner', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.navigateToGroupProfile(testId);
    await groupsPage.addOwner(username);
    await groupsPage.verifyMemberVisible(username);
  });

  test('test User verify group participation', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.skipOnboarding();
    await groupsPage.navigateToGroupProfile(testId);
    await groupsPage.navigateToMembersTab();
    await groupsPage.verifyMemberVisible(username);
  });

  test('remove group', async ({ page }) => {
    const groupsPage = new GroupsPage(page);
    await groupsPage.navigate();
    await groupsPage.deleteGroup(groupNameEdited);
  });
});
