/**
 * Auth Setup — runs ONCE per Playwright run before all test projects.
 *
 * For every user in `resolvedUsers` this setup:
 *   1. Logs in via the DataHub UI and saves the Playwright storageState to
 *      .auth/{username}.json  (consumed by base-test.ts's `context` fixture)
 *   2. Obtains a personal access token via the DataHub GraphQL API and saves it
 *      to .auth/gms-token-{username}.json  (consumed by base-test.ts's `gmsToken`
 *      fixture and by data-seeding setup scripts)
 *
 * The GMS token is obtained via a standalone API request context after UI login,
 * keeping the browser session (cookies) and the API credential (token) as
 * independent artefacts as required by §2 of the framework spec.
 *
 * Adding a new user: add the entry to fixtures/users.ts. No changes needed here.
 */

import * as fs from 'fs';
import * as path from 'path';
import { test as setup, expect } from '@playwright/test';
import { resolvedUsers } from '../fixtures/users';
import { authStatePath, gmsTokenPath } from '../fixtures/login';
import { LoginPage } from '../pages/login-page';

const AUTH_DIR = path.join(__dirname, '../.auth');

setup('authenticate all users', async ({ page, playwright }) => {
  setup.setTimeout(120_000);
  fs.mkdirSync(AUTH_DIR, { recursive: true });

  for (const [key, user] of Object.entries(resolvedUsers)) {
    console.log(`\n🔐 Authenticating user '${key}' (${user.username})...`);

    // ── 1. UI login + storageState ──────────────────────────────────────────
    await page.goto('/');
    const loginPage = new LoginPage(page);
    await loginPage.login(user.username, user.password);

    await expect(page.getByText(/Good (morning|afternoon|evening)/)).toBeVisible({
      timeout: 15_000,
    });

    // Suppress the welcome modal for all subsequent tests.
    await page.evaluate(() => localStorage.setItem('skipWelcomeModal', 'true'));

    const stateFile = authStatePath(user.username);
    await page.context().storageState({ path: stateFile });
    console.log(`   ✅ storageState saved → ${stateFile}`);

    // ── 2. GMS personal access token via DataHub API ────────────────────────
    // Use a standalone request context (not the browser page) so that the
    // token is obtained through the API layer, not the UI session.
    const cookies = await page.context().cookies();
    const actorCookie = cookies.find((c) => c.name === 'actor');
    if (!actorCookie) {
      throw new Error(`'actor' cookie not found after login for user '${user.username}'`);
    }

    const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
    const apiContext = await playwright.request.newContext({
      baseURL: process.env.BASE_URL ?? 'http://localhost:9002',
      extraHTTPHeaders: { Cookie: cookieHeader },
    });

    try {
      // Revoke the previous token if one exists, to avoid accumulation.
      const tokenFile = gmsTokenPath(user.username);
      if (fs.existsSync(tokenFile)) {
        const existing = JSON.parse(fs.readFileSync(tokenFile, 'utf-8')) as {
          tokenId?: string;
        };
        if (existing.tokenId) {
          await apiContext.post('/api/v2/graphql', {
            data: {
              query: `
                mutation revokeAccessToken($tokenId: String!) {
                  revokeAccessToken(tokenId: $tokenId)
                }
              `,
              variables: { tokenId: existing.tokenId },
            },
          });
          console.log(`   🗑️  Revoked previous token (id: ${existing.tokenId})`);
        }
      }

      const tokenResponse = await apiContext.post('/api/v2/graphql', {
        data: {
          query: `
            mutation createAccessToken($input: CreateAccessTokenInput!) {
              createAccessToken(input: $input) {
                accessToken
                metadata { id }
              }
            }
          `,
          variables: {
            input: {
              type: 'PERSONAL',
              actorUrn: actorCookie.value,
              duration: 'ONE_MONTH',
              name: `Playwright Test Token — ${user.username}`,
            },
          },
        },
      });

      if (!tokenResponse.ok()) {
        throw new Error(
          `Failed to generate GMS token for '${user.username}': ${tokenResponse.status()}`,
        );
      }

      const tokenData = (await tokenResponse.json()) as {
        data?: { createAccessToken?: { accessToken?: string; metadata?: { id?: string } } };
      };
      const accessToken = tokenData.data?.createAccessToken?.accessToken;
      const tokenId = tokenData.data?.createAccessToken?.metadata?.id;

      if (!accessToken) {
        throw new Error(`Empty access token in response for '${user.username}'`);
      }

      fs.writeFileSync(
        tokenFile,
        JSON.stringify({ token: accessToken, tokenId, actorUrn: actorCookie.value }, null, 2),
      );
      console.log(`   🔑 GMS token saved → ${tokenFile}`);
    } finally {
      await apiContext.dispose();
    }

    // Navigate away so the next iteration starts from a clean state.
    await page.goto('about:blank');
  }

  console.log('\n✅ Auth setup complete for all users');
});
