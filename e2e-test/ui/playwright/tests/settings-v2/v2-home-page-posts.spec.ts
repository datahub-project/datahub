/**
 * Home Page Posts (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_homePagePost.js
 */

import { test } from '../../fixtures/base-test';
import { HomePagePostsPage } from '../../pages/settings/home-page-posts.page';

test.use({ featureName: 'settings-v2' });

// Tests share server-side state (post created in step 1 is edited/deleted later)
test.describe.configure({ mode: 'serial' });

test.describe('create announcement and link post', () => {
  let postsPage: HomePagePostsPage;

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showHomePageRedesign: false });
    postsPage = new HomePagePostsPage(page);
    await postsPage.navigate();
    await postsPage.skipIntroducePage();
  });

  test('Verify create, edit and delete announcement post', async () => {
    await postsPage.openCreateForm();
    await postsPage.fillTitle('Test Announcement Title');
    await postsPage.submitCreate();

    await postsPage.navigate();
    await postsPage.navigateToAnnouncements();
    await postsPage.verifyPostVisible('Test Announcement Title');

    await postsPage.navigate();
    await postsPage.editPost('Test Announcement Title');
    await postsPage.selectPostType('Announcement');
    await postsPage.fillTitle('Test Announcement Title Updated');
    await postsPage.submitUpdate();

    await postsPage.navigate();
    await postsPage.navigateToAnnouncements();
    await postsPage.verifyPostVisible('Test Announcement Title Updated');

    await postsPage.navigate();
    await postsPage.deletePost('Test Announcement Title Updated');
    await postsPage.verifyPostRemovedFromHome('Test Announcement Title Updated');
  });

  test('Verify create, edit and delete link post', async () => {
    await postsPage.openCreateForm();
    await postsPage.selectPostType('Pinned Link');
    await postsPage.fillTitle('Test Link Title');
    await postsPage.fillLink('https://www.example.com');
    await postsPage.fillMediaLocation('https://www.example.com/images/example-image.jpg');
    await postsPage.submitCreate();

    await postsPage.navigate();
    await postsPage.verifyPostVisible('Test Link Title');

    await postsPage.editPost('Test Link Title');
    await postsPage.selectPostType('Pinned Link');
    await postsPage.fillTitle('Test Link Updated Title');
    await postsPage.fillLink('https://www.updatedexample.com');
    await postsPage.fillMediaLocation('https://www.updatedexample.com/images/example-image.jpg');
    await postsPage.submitUpdate();

    await postsPage.navigate();
    await postsPage.verifyPostVisible('Test Link Updated Title');

    await postsPage.deletePost('Test Link Updated Title');
    await postsPage.verifyPostRemovedFromHome('Test Link Updated Title');
  });
});
