/**
 * Asset Collection "View all" — e2e coverage for the button's gating and navigation.
 *
 * The button must appear only for dynamic-filter collections whose predicate the
 * search page can reproduce exactly; manual urn-list collections hide it. Clicking
 * navigates to /search with the collection's filter applied as native search filters.
 *
 * Test data strategy: home templates are per-user, and the shared admin user's home
 * is asserted on by homepage-modules.spec.ts (default module count/order), so this
 * suite provisions its OWN native user (invite token + /signUp, idempotent across
 * re-runs) and assigns collection modules to that user's personal home template via
 * GraphQL. UI-driven module creation is not possible in OSS (template editing is
 * disabled), which is why setup goes through the API.
 */
import { test, expect } from '../../fixtures/login-test';
import type { APIRequestContext, Page } from '@playwright/test';
import { users } from '../../data/users';

const FRONTEND_URL = process.env.BASE_URL || 'http://localhost:9002';

const COLLECTION_USER = {
  email: 'collection-viewall-e2e@example.com',
  fullName: 'Collection ViewAll E2E',
  password: 'E2eViewAll!1',
};

const DYNAMIC_MODULE_NAME = 'E2E Dynamic Collection';
const MANUAL_MODULE_NAME = 'E2E Manual Collection';

const DATA_PRODUCT_FILTER_JSON = JSON.stringify({
  type: 'logical',
  operator: 'and',
  operands: [{ type: 'property', property: '_entityType', operator: 'equals', values: ['dataProduct'] }],
});

// The urn only needs to be a syntactically valid reference; the manual tile hides
// the button based on assetUrns being non-empty, not on the entities resolving.
const MANUAL_ASSET_URNS = ['urn:li:corpuser:datahub'];

async function gql<T>(ctx: APIRequestContext, query: string, variables: Record<string, unknown> = {}): Promise<T> {
  const resp = await ctx.post('/api/v2/graphql', { data: { query, variables } });
  expect(resp.ok()).toBe(true);
  const body = await resp.json();
  expect(body.errors, `GraphQL errors: ${JSON.stringify(body.errors)}`).toBeUndefined();
  return body.data as T;
}

async function loginApi(ctx: APIRequestContext, username: string, password: string): Promise<boolean> {
  const resp = await ctx.post('/logIn', { data: { username, password } });
  return resp.ok();
}

/** Create the suite's user if it does not exist yet (re-runs are a no-op). */
async function ensureUser(adminCtx: APIRequestContext, userCtx: APIRequestContext): Promise<void> {
  if (await loginApi(userCtx, COLLECTION_USER.email, COLLECTION_USER.password)) {
    return;
  }
  const data = await gql<{ getInviteToken: { inviteToken: string } }>(
    adminCtx,
    `query getInviteToken($input: GetInviteTokenInput!) { getInviteToken(input: $input) { inviteToken } }`,
    { input: {} },
  );
  const signUp = await adminCtx.post('/signUp', {
    data: {
      fullName: COLLECTION_USER.fullName,
      email: COLLECTION_USER.email,
      password: COLLECTION_USER.password,
      title: 'Test User',
      inviteToken: data.getInviteToken.inviteToken,
    },
  });
  expect(signUp.ok()).toBe(true);
  expect(await loginApi(userCtx, COLLECTION_USER.email, COLLECTION_USER.password)).toBe(true);
}

async function createCollectionModule(
  ctx: APIRequestContext,
  name: string,
  assetUrns: string[],
  dynamicFilterJson?: string,
): Promise<string> {
  const data = await gql<{ upsertPageModule: { urn: string } }>(
    ctx,
    `mutation upsertPageModule($input: UpsertPageModuleInput!) { upsertPageModule(input: $input) { urn } }`,
    {
      input: {
        name,
        type: 'ASSET_COLLECTION',
        scope: 'PERSONAL',
        params: { assetCollectionParams: { assetUrns, ...(dynamicFilterJson ? { dynamicFilterJson } : {}) } },
      },
    },
  );
  return data.upsertPageModule.urn;
}

async function loginUi(
  page: Page,
  loginPage: { navigateToLogin(): Promise<void>; login(u: string, p: string): Promise<void> },
) {
  // Suppress the first-visit welcome modal, which overlays the module grid.
  await page.addInitScript(() => localStorage.setItem('skipWelcomeModal', 'true'));
  await loginPage.navigateToLogin();
  await loginPage.login(COLLECTION_USER.email, COLLECTION_USER.password);
  await expect(page.getByTestId('asset-collection-module').filter({ hasText: DYNAMIC_MODULE_NAME })).toBeVisible({
    timeout: 30_000,
  });
}

// Serial: with fullyParallel the two tests could land on different workers and
// each re-run beforeAll, creating duplicate modules for the suite user.
test.describe.configure({ mode: 'serial' });

test.describe('Homepage Collection View All', () => {
  test.beforeAll(async ({ playwright }) => {
    const adminCtx = await playwright.request.newContext({ baseURL: FRONTEND_URL });
    const userCtx = await playwright.request.newContext({ baseURL: FRONTEND_URL });
    try {
      expect(await loginApi(adminCtx, users.admin.username, users.admin.password)).toBe(true);
      await ensureUser(adminCtx, userCtx);

      const dynamicUrn = await createCollectionModule(userCtx, DYNAMIC_MODULE_NAME, [], DATA_PRODUCT_FILTER_JSON);
      const manualUrn = await createCollectionModule(userCtx, MANUAL_MODULE_NAME, MANUAL_ASSET_URNS);

      const template = await gql<{ upsertPageTemplate: { urn: string } }>(
        userCtx,
        `mutation upsertPageTemplate($input: UpsertPageTemplateInput!) { upsertPageTemplate(input: $input) { urn } }`,
        {
          input: {
            scope: 'PERSONAL',
            surfaceType: 'HOME_PAGE',
            rows: [{ modules: [dynamicUrn, manualUrn] }],
          },
        },
      );
      await gql(
        userCtx,
        `mutation updateUserHomePageSettings($input: UpdateUserHomePageSettingsInput!) { updateUserHomePageSettings(input: $input) }`,
        { input: { pageTemplate: template.upsertPageTemplate.urn } },
      );
    } finally {
      await adminCtx.dispose();
      await userCtx.dispose();
    }
  });

  test('view all appears only on dynamic-filter collections', async ({ page, loginPage }) => {
    await loginUi(page, loginPage);

    const dynamicTile = page.getByTestId('asset-collection-module').filter({ hasText: DYNAMIC_MODULE_NAME });
    const manualTile = page.getByTestId('asset-collection-module').filter({ hasText: MANUAL_MODULE_NAME });

    await expect(dynamicTile).toBeVisible();
    await expect(manualTile).toBeVisible();
    await expect(dynamicTile.getByTestId('view-all')).toBeVisible();
    await expect(manualTile.getByTestId('view-all')).toHaveCount(0);
  });

  test('view all navigates to search with the collection filter applied', async ({ page, loginPage }) => {
    await loginUi(page, loginPage);

    const dynamicTile = page.getByTestId('asset-collection-module').filter({ hasText: DYNAMIC_MODULE_NAME });
    await dynamicTile.getByTestId('view-all').click();

    await expect(page).toHaveURL(/\/search\?/);
    // The predicate `Type equals Data Product` must land as the native combined
    // type filter (field `_entityType␞typeNames`, %E2%90%9E-encoded) under AND.
    expect(page.url()).toContain('filter__entityType%E2%90%9EtypeNames___false___EQUAL___0=DATA_PRODUCT');
    expect(page.url()).toContain('unionType=0');
    // And the search page must parse it back into its active Type filter chip.
    await expect(page.getByTestId('active-filter-_entityType␞typeNames')).toBeVisible();
  });
});
