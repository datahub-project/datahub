/**
 * API mocking utility — DataHub-aware route interception helpers.
 *
 * Wraps Playwright's page.route with helpers for mocking/intercepting
 * GraphQL operations and toggling feature flags.
 *
 * This is the canonical implementation. The fixture wrapper lives in
 * fixtures/mocking.fixture.ts. Import ApiMocker type here when you need
 * it in helpers or page objects.
 */

import type { Page, Route } from '@playwright/test';

// ── Public interface ──────────────────────────────────────────────────────────

export interface ApiMocker {
  /**
   * Intercept a named GraphQL operation and return responseData as its
   * data payload. All other operations are forwarded as normal.
   */
  mockGraphQL(operationName: string, responseData: unknown): Promise<void>;

  /**
   * Intercept a named GraphQL operation, fetch the real response, apply
   * transform to its JSON body, and return the modified response.
   * Useful for toggling feature flags without a full mock.
   */
  interceptGraphQLResponse(
    operationName: string,
    transform: (json: unknown) => unknown,
  ): Promise<void>;

  /**
   * Install an arbitrary route handler. Thin wrapper over page.route.
   */
  mockRoute(
    urlPattern: string | RegExp,
    handler: (route: Route) => Promise<void> | void,
  ): Promise<void>;

  /**
   * Override one or more appConfig.featureFlags values for the duration
   * of the test. Both appConfig and getMe responses are patched so that
   * the flag is consistent across the UI.
   */
  setFeatureFlags(flags: Record<string, boolean>): Promise<void>;
}

// ── Implementation ────────────────────────────────────────────────────────────

export class PageApiMocker implements ApiMocker {
  constructor(private readonly page: Page) {}

  async mockGraphQL(operationName: string, responseData: unknown): Promise<void> {
    await this.page.route('**/api/v2/graphql', async (route) => {
      const postData = route.request().postDataJSON() as { operationName?: string } | null;
      if (postData?.operationName === operationName) {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({ data: responseData }),
        });
      } else {
        await route.continue();
      }
    });
  }

  async interceptGraphQLResponse(
    operationName: string,
    transform: (json: unknown) => unknown,
  ): Promise<void> {
    await this.page.route('**/api/v2/graphql', async (route) => {
      const postData = route.request().postDataJSON() as { operationName?: string } | null;
      if (postData?.operationName !== operationName) {
        await route.continue();
        return;
      }
      const response = await route.fetch();
      const json = await response.json();
      await route.fulfill({ response, json: transform(json) });
    });
  }

  async mockRoute(
    urlPattern: string | RegExp,
    handler: (route: Route) => Promise<void> | void,
  ): Promise<void> {
    await this.page.route(urlPattern, handler);
  }

  async setFeatureFlags(flags: Record<string, boolean>): Promise<void> {
    await this.page.route('**/api/v2/graphql', async (route) => {
      const postData = route.request().postDataJSON() as { operationName?: string } | null;
      const op = postData?.operationName;

      const response = await route.fetch();
      const json = await response.json();

      if (op === 'appConfig') {
        const featureFlags = (
          json as { data?: { appConfig?: { featureFlags?: Record<string, boolean> } } }
        ).data?.appConfig?.featureFlags;
        if (featureFlags) {
          Object.assign(featureFlags, flags);
        }
      } else if (op === 'getMe') {
        // Mirror themeV2Enabled to the user appearance settings.
        if ('themeV2Enabled' in flags) {
          const appearance = (
            json as {
              data?: {
                me?: { corpUser?: { settings?: { appearance?: Record<string, boolean> } } };
              };
            }
          ).data?.me?.corpUser?.settings?.appearance;
          if (appearance) {
            appearance.showThemeV2 = flags.themeV2Enabled;
          }
        }
      }

      await route.fulfill({ response, json });
    });
  }
}
