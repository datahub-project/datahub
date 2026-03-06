import { test as setup, expect } from '@playwright/test';
import { LoginPage } from '../pages/LoginPage';

/**
 * Auth Setup - Runs ONCE before all tests
 *
 * This file performs authentication and saves the session state to a file.
 * All tests will reuse this authenticated state, eliminating the need to login
 * in every test.
 *
 * The storage state is saved to .auth/user.json and automatically loaded by
 * all test projects configured in playwright.config.ts.
 */

const authFile = '.auth/user.json';

setup('authenticate', async ({ page }) => {
  const loginPage = new LoginPage(page);

  console.log('🔐 Setting up authentication...');

  // Navigate to login page
  await page.goto('/');

  // Perform login
  const username = process.env.TEST_USERNAME || 'datahub';
  const password = process.env.TEST_PASSWORD || 'datahub';

  await loginPage.login(username, password);

  // Wait for successful login - check for greeting message
  await expect(
    page.getByText(/Good (morning|afternoon|evening)/)
  ).toBeVisible({ timeout: 10000 });

  console.log('✅ Authentication successful');

  // Skip welcome modal for all future tests
  await page.evaluate(() => {
    localStorage.setItem('skipWelcomeModal', 'true');
  });

  // Save signed-in state to '.auth/user.json'
  await page.context().storageState({ path: authFile });

  console.log(`💾 Auth state saved to ${authFile}`);

  // Generate GMS token for REST API authentication
  const cookies = await page.context().cookies();
  const actorCookie = cookies.find((c) => c.name === 'actor');

  if (!actorCookie) {
    throw new Error('Actor cookie not found after login');
  }

  const actorUrn = actorCookie.value;

  const tokenResponse = await page.request.post('/api/v2/graphql', {
    data: {
      query: `mutation createAccessToken($input: CreateAccessTokenInput!) {
        createAccessToken(input: $input) {
          accessToken
          metadata {
            id
          }
        }
      }`,
      variables: {
        input: {
          type: 'PERSONAL',
          actorUrn,
          duration: 'ONE_DAY',
          name: 'Playwright Test Token',
        },
      },
    },
  });

  if (!tokenResponse.ok()) {
    throw new Error(`Failed to generate GMS token: ${tokenResponse.status()}`);
  }

  const tokenData = await tokenResponse.json();
  const accessToken = tokenData.data?.createAccessToken?.accessToken;
  const tokenId = tokenData.data?.createAccessToken?.metadata?.id;

  if (!accessToken) {
    throw new Error('Failed to extract access token from response');
  }

  // Save token to file for data seeding
  const fs = require('fs');
  const tokenFile = '.auth/gms-token.json';
  fs.writeFileSync(
    tokenFile,
    JSON.stringify(
      {
        token: accessToken,
        tokenId,
        actorUrn,
      },
      null,
      2
    )
  );

  console.log(`🔑 GMS token generated and saved to ${tokenFile}`);
});