/**
 * @deprecated
 *
 * This fixture file organises fixtures around page objects and is superseded
 * by the capability-based fixture architecture introduced in base-test.ts.
 *
 * Migration path:
 *   1. Replace `import { test } from '../../fixtures/test-context'`
 *      with     `import { test } from '../../fixtures/base-test'`
 *   2. Instantiate page objects directly in your test:
 *        const searchPage = new SearchPage(page);
 *   3. Replace `graphqlHelper` usage with the `apiMock` fixture for mocking,
 *      or use `page.request` directly for one-off API calls.
 *
 * This file is kept temporarily to avoid breaking existing tests during
 * migration. It will be removed once all test files have been updated.
 *
 * NOTE: extends base-test (not plain @playwright/test) so that the authenticated
 * context override is inherited by all tests using this fixture.
 */

import { test as base } from './base-test';
import { LoginPage } from '../pages/login-page';
import { SearchPage } from '../pages/search-page';
import { DatasetPage } from '../pages/dataset-page';
import { BusinessAttributePage } from '../pages/business-attribute-page';
import { WelcomeModalPage } from '../pages/welcome-modal-page';
import { GraphQLHelper } from '../helpers/graphql-helper';
import { NavigationHelper } from '../helpers/navigation-helper';
import { WaitHelper } from '../helpers/wait-helper';

type TestContext = {
  loginPage: LoginPage;
  searchPage: SearchPage;
  datasetPage: DatasetPage;
  businessAttributePage: BusinessAttributePage;
  welcomeModalPage: WelcomeModalPage;
  graphqlHelper: GraphQLHelper;
  navigationHelper: NavigationHelper;
  waitHelper: WaitHelper;
};

export const test = base.extend<TestContext>({
  loginPage: async ({ page }, use) => {
    await use(new LoginPage(page));
  },
  searchPage: async ({ page }, use) => {
    await use(new SearchPage(page));
  },
  datasetPage: async ({ page }, use) => {
    await use(new DatasetPage(page));
  },
  businessAttributePage: async ({ page }, use) => {
    await use(new BusinessAttributePage(page));
  },
  welcomeModalPage: async ({ page }, use) => {
    await use(new WelcomeModalPage(page));
  },
  graphqlHelper: async ({ page }, use) => {
    await use(new GraphQLHelper(page));
  },
  navigationHelper: async ({ page }, use) => {
    await use(new NavigationHelper(page));
  },
  waitHelper: async ({ page }, use) => {
    await use(new WaitHelper(page));
  },
});

export { expect } from '@playwright/test';
