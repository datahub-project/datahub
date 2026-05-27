/**
 * Manage Access Tokens (Settings V2) tests — migrated from Cypress e2e/settingsV2/v2_manage_access_tokens.js
 */

import { test, expect } from '../../fixtures/base-test';
import { AccessTokensPage } from '../../pages/settings/access-tokens.page';

const testId = Math.floor(Math.random() * 100000);

test.describe('manage access tokens', () => {
  let tokensPage: AccessTokensPage;

  test.beforeEach(async ({ page, apiMock, cleanup }) => {
    await apiMock.setFeatureFlags({ tokenAuthEnabled: true, generatePersonalAccessTokens: true });
    tokensPage = new AccessTokensPage(page, { cleanup });
    await tokensPage.navigate();
  });

  test('create and revoke access token', async () => {
    await tokensPage.createPersonalToken(`Token Name${testId}`, `Token Description${testId}`);

    const tokenValue = await tokensPage.getTokenValue();
    expect(tokenValue).toMatch(/^[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+$/);

    await tokensPage.closeTokenModal();
    await tokensPage.verifyTokenInList(`Token Name${testId}`, `Token Description${testId}`);

    await tokensPage.revokeToken(`Token Name${testId}`);
    await tokensPage.verifyTokenRemoved(`Token Name${testId}`, `Token Description${testId}`);
  });
});
