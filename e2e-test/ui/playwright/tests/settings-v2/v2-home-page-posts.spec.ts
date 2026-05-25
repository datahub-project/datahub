/**
 * Home Page Posts (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_homePagePost.js
 *
 * Tests are fully isolated and can run in parallel. Each test uses unique post names
 * to avoid interference between parallel test runs.
 */

import { test } from '../../fixtures/base-test';
import { HomePagePostsPage } from '../../pages/settings/home-page-posts.page';
import { withRandomSuffix } from '../../utils/random';

test.use({ featureName: 'settings-v2' });

test.describe('create announcement and link post', () => {
  let postsPage: HomePagePostsPage;

  test.beforeEach(async ({ apiMock, page }) => {
    await apiMock.setFeatureFlags({ showHomePageRedesign: false });
    postsPage = new HomePagePostsPage(page);
    await postsPage.navigate();
    await postsPage.skipIntroducePage();
  });

  test('Verify create, edit and delete announcement post', async ({ page }) => {
    const announcementTitle = withRandomSuffix('Test Announcement');
    const announcementUpdated = `${announcementTitle} Updated`;

    await postsPage.openCreateForm();
    await postsPage.fillTitle(announcementTitle);
    await postsPage.submitCreate();

    // After creation, reload the page to ensure the data is fresh
    await page.reload();
    await postsPage.navigate();
    await postsPage.navigateToAnnouncements();
    await postsPage.verifyPostVisible(announcementTitle);

    await postsPage.navigate();
    await postsPage.editPost(announcementTitle);
    await postsPage.selectPostType('Announcement');
    await postsPage.fillTitle(announcementUpdated);
    await postsPage.submitUpdate();

    await postsPage.navigate();
    await postsPage.navigateToAnnouncements();
    await postsPage.verifyPostVisible(announcementUpdated);

    await postsPage.navigate();
    await postsPage.deletePost(announcementUpdated);
    await postsPage.verifyPostRemovedFromHome(announcementUpdated);
  });

  test('Verify create, edit and delete link post', async ({ page }) => {
    const linkTitle = withRandomSuffix('Test Link');
    const linkUpdated = `${linkTitle} Updated`;

    await postsPage.openCreateForm();
    await postsPage.selectPostType('Pinned Link');
    await postsPage.fillTitle(linkTitle);
    await postsPage.fillLink('https://www.example.com');
    await postsPage.fillMediaLocation('https://www.example.com/images/example-image.jpg');
    await postsPage.submitCreate();

    // After creation, reload the page to ensure the table is updated
    await page.reload();
    await postsPage.navigate();
    await postsPage.verifyPostVisible(linkTitle);

    await postsPage.editPost(linkTitle);
    await postsPage.selectPostType('Pinned Link');
    await postsPage.fillTitle(linkUpdated);
    await postsPage.fillLink('https://www.updatedexample.com');
    await postsPage.fillMediaLocation('https://www.updatedexample.com/images/example-image.jpg');
    await postsPage.submitUpdate();

    await postsPage.navigate();
    await postsPage.verifyPostVisible(linkUpdated);

    await postsPage.deletePost(linkUpdated);
    await postsPage.verifyPostRemovedFromHome(linkUpdated);
  });
});
