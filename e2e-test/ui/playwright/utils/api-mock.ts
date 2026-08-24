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

import { DATAHUB_GRAPHQL_PATH } from './constants';

/**
 * Match DataHub GraphQL requests with or without ?operationName=...
 * Playwright path globs that end at /graphql only match the bare path; after the
 * UI started appending operation names for Chrome Network filtering, those
 * patterns stopped intercepting browser GraphQL calls.
 */
export function isDataHubGraphqlUrl(url: URL): boolean {
  return url.pathname.endsWith(DATAHUB_GRAPHQL_PATH);
}

/**
 * Overrides for {@link ApiMocker.setFeatureFlags}.
 * Auth toggles are booleans; allowedAccessTokenDurations is an ISO-8601 string list.
 * All other keys (feature flags, platform privileges) are booleans.
 */
export type FeatureFlagOverrides = {
  tokenAuthEnabled?: boolean;
  allowNoExpiry?: boolean;
  allowedAccessTokenDurations?: readonly string[];
} & {
  [key: string]: boolean | readonly string[] | undefined;
};

/** Union of values that may appear in {@link FeatureFlagOverrides}. */
export type FeatureFlagOverrideValue = boolean | readonly string[];

const AUTH_CONFIG_BOOLEAN_KEYS = new Set(['tokenAuthEnabled', 'allowNoExpiry']);
const AUTH_CONFIG_DURATION_KEY = 'allowedAccessTokenDurations';

type AuthConfigOverrides = {
  tokenAuthEnabled?: boolean;
  allowNoExpiry?: boolean;
  allowedAccessTokenDurations?: readonly string[];
};

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
  interceptGraphQLResponse(operationName: string, transform: (json: unknown) => unknown): Promise<void>;

  /**
   * Install an arbitrary route handler. Thin wrapper over page.route.
   */
  mockRoute(urlPattern: string | RegExp, handler: (route: Route) => Promise<void> | void): Promise<void>;

  /**
   * Override one or more appConfig.featureFlags / authConfig / platformPrivileges
   * values for the duration of the test. Both appConfig and getMe responses are
   * patched so that the flag is consistent across the UI.
   */
  setFeatureFlags(flags: FeatureFlagOverrides): Promise<void>;

  /**
   * Mock the batchGetStepStates operation to mark all requested step IDs as
   * already seen. This prevents education/onboarding modals (e.g.
   * CreateSourceEducationModal) from appearing during tests that don't
   * exercise the onboarding flow.
   */
  suppressOnboardingModals(): Promise<void>;
}

// ── Implementation ────────────────────────────────────────────────────────────

export class PageApiMocker implements ApiMocker {
  private mockMap: Map<string, unknown> = new Map();
  private mockRouteRegistered = false;

  constructor(private readonly page: Page) {}

  async mockGraphQL(operationName: string, responseData: unknown): Promise<void> {
    // Store the mock data
    this.mockMap.set(operationName, responseData);

    // Register a SINGLE route handler that checks all mocked operations
    if (!this.mockRouteRegistered) {
      await this.page.route(isDataHubGraphqlUrl, async (route) => {
        const postData = route.request().postDataJSON() as { operationName?: string } | null;
        const op = postData?.operationName;

        if (op && this.mockMap.has(op)) {
          const responseData = this.mockMap.get(op);
          await route.fulfill({
            status: 200,
            contentType: 'application/json',
            body: JSON.stringify({ data: responseData }),
          });
        } else {
          await route.fallback();
        }
      });
      this.mockRouteRegistered = true;
    }
  }

  async interceptGraphQLResponse(operationName: string, transform: (json: unknown) => unknown): Promise<void> {
    // Store the transform function
    if (!this.interceptMap) {
      this.interceptMap = new Map();
    }
    this.interceptMap.set(operationName, transform);

    // Register a SINGLE route handler that checks all intercepted operations
    if (!this.interceptRouteRegistered) {
      await this.page.route(isDataHubGraphqlUrl, async (route) => {
        const postData = route.request().postDataJSON() as { operationName?: string } | null;
        const op = postData?.operationName;

        if (op && this.interceptMap?.has(op)) {
          const transform = this.interceptMap?.get(op);
          if (!transform) {
            await route.fallback();
            return;
          }
          const response = await route.fetch();

          // Guard against non-JSON responses before attempting to parse.
          const contentType = response.headers()['content-type'] ?? '';
          if (!contentType.includes('application/json')) {
            await route.fulfill({ response });
            return;
          }

          let json: unknown;
          try {
            json = await response.json();
          } catch {
            await route.fulfill({ response });
            return;
          }

          await route.fulfill({ response, json: transform(json) });
        } else {
          await route.fallback();
        }
      });
      this.interceptRouteRegistered = true;
    }
  }

  private interceptMap?: Map<string, (json: unknown) => unknown>;
  private interceptRouteRegistered = false;

  async mockRoute(urlPattern: string | RegExp, handler: (route: Route) => Promise<void> | void): Promise<void> {
    await this.page.route(urlPattern, handler);
  }

  async setFeatureFlags(flags: FeatureFlagOverrides): Promise<void> {
    // Keys that map directly to platformPrivileges fields in the getMe response.
    const PLATFORM_PRIVILEGE_KEYS = new Set([
      'generatePersonalAccessTokens',
      'manageServiceAccounts',
      'manageTokens',
      'managePolicies',
      'manageIdentities',
    ]);

    // Separate the flags into their respective buckets.
    const featureFlagOverrides: Record<string, boolean> = {};
    const authConfigOverrides: AuthConfigOverrides = {};
    const platformPrivilegeOverrides: Record<string, boolean> = {};

    for (const [key, value] of Object.entries(flags)) {
      if (value === undefined) {
        continue;
      }
      if (key === AUTH_CONFIG_DURATION_KEY) {
        if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
          throw new Error(
            `setFeatureFlags: '${AUTH_CONFIG_DURATION_KEY}' requires a string array, got ${typeof value}`,
          );
        }
        authConfigOverrides.allowedAccessTokenDurations = value;
      } else if (AUTH_CONFIG_BOOLEAN_KEYS.has(key)) {
        if (typeof value !== 'boolean') {
          throw new Error(`setFeatureFlags: authConfig '${key}' requires a boolean, got ${typeof value}`);
        }
        authConfigOverrides[key as 'tokenAuthEnabled' | 'allowNoExpiry'] = value;
      } else if (PLATFORM_PRIVILEGE_KEYS.has(key)) {
        if (typeof value !== 'boolean') {
          throw new Error(`setFeatureFlags: platform privilege '${key}' requires a boolean, got ${typeof value}`);
        }
        platformPrivilegeOverrides[key] = value;
      } else {
        if (typeof value !== 'boolean') {
          throw new Error(`setFeatureFlags: feature flag '${key}' requires a boolean, got ${typeof value}`);
        }
        featureFlagOverrides[key] = value;
      }
    }

    await this.page.route(isDataHubGraphqlUrl, async (route) => {
      const postData = route.request().postDataJSON() as { operationName?: string } | null;
      const op = postData?.operationName;

      // Only intercept the two operations that carry feature flags. All other
      // GraphQL operations (mutations, etc.) are forwarded unchanged to avoid
      // "SyntaxError: Unexpected end of JSON input" on responses with empty or
      // non-JSON bodies (e.g. mutations that return 204 or a streaming response).
      if (op !== 'appConfig' && op !== 'getMe') {
        await route.fallback();
        return;
      }

      const response = await route.fetch();

      // Guard against non-JSON responses (e.g. 204 No Content, HTML error pages).
      const contentType = response.headers()['content-type'] ?? '';
      if (!contentType.includes('application/json')) {
        await route.fulfill({ response });
        return;
      }

      let json: unknown;
      try {
        json = await response.json();
      } catch {
        // Body was not valid JSON — forward as-is without modification.
        await route.fulfill({ response });
        return;
      }

      if (op === 'appConfig') {
        const appConfig = (json as { data?: { appConfig?: Record<string, unknown> } }).data?.appConfig;
        if (appConfig) {
          // Patch featureFlags.
          if (Object.keys(featureFlagOverrides).length > 0) {
            const featureFlags = appConfig.featureFlags as Record<string, boolean> | undefined;
            if (featureFlags) {
              Object.assign(featureFlags, featureFlagOverrides);
            }
          }
          // Patch authConfig (booleans + allowedAccessTokenDurations string[]).
          if (Object.keys(authConfigOverrides).length > 0) {
            const authConfig = appConfig.authConfig as AuthConfigOverrides | undefined;
            if (authConfig) {
              Object.assign(authConfig, authConfigOverrides);
            }
          }
        }
      } else if (op === 'getMe') {
        const meData = (
          json as {
            data?: {
              me?: {
                corpUser?: { settings?: { appearance?: Record<string, boolean> } };
                platformPrivileges?: Record<string, boolean>;
              };
            };
          }
        ).data?.me;

        if (meData) {
          // Mirror themeV2Enabled to the user appearance settings.
          if ('themeV2Enabled' in flags && typeof flags.themeV2Enabled === 'boolean') {
            const appearance = meData.corpUser?.settings?.appearance;
            if (appearance) {
              appearance.showThemeV2 = flags.themeV2Enabled;
            }
          }

          // Apply platform privilege overrides directly to the getMe response.
          if (Object.keys(platformPrivilegeOverrides).length > 0 && meData.platformPrivileges) {
            Object.assign(meData.platformPrivileges, platformPrivilegeOverrides);
          }
        }
      }

      await route.fulfill({ response, json });
    });
  }

  async suppressOnboardingModals(): Promise<void> {
    await this.page.route(isDataHubGraphqlUrl, async (route) => {
      const postData = route.request().postDataJSON() as {
        operationName?: string;
        variables?: { input?: { ids?: string[] } };
      } | null;

      if (postData?.operationName !== 'batchGetStepStates') {
        await route.fallback();
        return;
      }

      const ids = postData.variables?.input?.ids ?? [];
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          data: {
            batchGetStepStates: {
              results: ids.map((id) => ({ id, properties: [] })),
            },
          },
        }),
      });
    });
  }
}
