import { loginTest as test } from '@fixtures/base-test';
import { LoginPage } from '../../pages/login-page';
import { GraphQLHelper } from '../../helpers/graphql-helper';

/**
 * Login V2 tests - These tests verify login with Theme V2
 * Therefore, they MUST NOT use the shared authentication state
 */
// const test = base.extend<{
//   loginPage: LoginPage;
//   graphqlHelper: GraphQLHelper;
// }>({
//   loginPage: async ({ page }, use) => {
//     await use(new LoginPage(page));
//   },
//   graphqlHelper: async ({ page }, use) => {
//     await use(new GraphQLHelper(page));
//   },
// });

// Explicitly disable shared auth state for login tests
users = [user1, user2, user3];

users.array.forEach(user => {
  test.use({ user: user });

test.describe('Login with Theme V2', () => {
  test.beforeEach(async ({ page, graphqlHelper }) => {
    await graphqlHelper.setThemeV2Enabled(true);

    await page.addInitScript(() => {
      localStorage.setItem('isThemeV2Enabled', 'false');
      localStorage.setItem('showHomePageRedesign', 'false');
      localStorage.setItem('isNavBarRedesignEnabled', 'false');
      localStorage.setItem('skipAcrylIntroducePage', 'true');
      localStorage.setItem('skipWelcomeModal', 'true');
    });
  });

  test('logs in successfully with Theme V2 enabled', async ({ page, loginPage, user }) => {
    await page.goto('/');

    await loginPage.login(user.username, user.password);

    await expect(page.getByRole('button', { name: 'Discover' })).toBeVisible();
  });
});

});
