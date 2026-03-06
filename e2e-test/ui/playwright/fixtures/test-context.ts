import { test as base } from '@playwright/test';
import { LoginPage } from '../pages/LoginPage';
import { SearchPage } from '../pages/SearchPage';
import { DatasetPage } from '../pages/DatasetPage';
import { BusinessAttributePage } from '../pages/BusinessAttributePage';
import { WelcomeModalPage } from '../pages/WelcomeModalPage';
import { GraphQLHelper } from '../helpers/GraphQLHelper';
import { NavigationHelper } from '../helpers/NavigationHelper';
import { WaitHelper } from '../helpers/WaitHelper';

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
