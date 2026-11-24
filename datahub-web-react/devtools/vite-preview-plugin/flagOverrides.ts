/**
 * Pure, dependency-free utilities for parsing and applying feature-flag overrides.
 *
 * Shared between the Vite preview plugin (dev server response overlay) and
 * Playwright's PageApiMocker (E2E test flag injection).
 */

function parseOneFlag(pair: string): [string, boolean | string] | undefined {
    const trimmed = pair.trim();
    if (!trimmed) return undefined;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex < 0) return undefined;

    const key = trimmed.slice(0, eqIndex).trim();
    const rawValue = trimmed.slice(eqIndex + 1).trim();

    if (rawValue === 'true') return [key, true];
    if (rawValue === 'false') return [key, false];
    return [key, rawValue];
}

/** Parse a `k=v[,k=v]` spec into a typed overrides map. */
export function parseFlagOverrides(spec: string): Record<string, boolean | string> {
    if (!spec) return {};

    return spec.split(',').reduce<Record<string, boolean | string>>((acc, pair) => {
        const parsed = parseOneFlag(pair);
        if (parsed) {
            const [key, value] = parsed;
            acc[key] = value;
        }
        return acc;
    }, {});
}

/**
 * Patch `data.appConfig.featureFlags` in a GraphQL response body in place.
 *
 * Mutates and returns the input for efficiency (caller owns the object).
 * No-op when `overrides` is empty or the response has no `featureFlags` path.
 *
 * `featureFlagsJson`, when the response carries it, is patched too: AppConfigProvider merges that
 * blob over the typed object (backend POJO wins), so an override left only on `featureFlags`
 * would be undone in the browser.
 */
export function applyFlagOverrides(
    appConfigResponseBody: unknown,
    overrides: Record<string, boolean | string>,
): unknown {
    if (!overrides || Object.keys(overrides).length === 0) return appConfigResponseBody;

    const body = appConfigResponseBody as {
        data?: { appConfig?: { featureFlags?: Record<string, unknown>; featureFlagsJson?: string | null } };
    };

    const appConfig = body?.data?.appConfig;
    const featureFlags = appConfig?.featureFlags;
    if (!appConfig || !featureFlags) return appConfigResponseBody;

    Object.assign(featureFlags, overrides);

    if (typeof appConfig.featureFlagsJson === 'string' && appConfig.featureFlagsJson) {
        try {
            const parsed = JSON.parse(appConfig.featureFlagsJson) as Record<string, unknown>;
            appConfig.featureFlagsJson = JSON.stringify({ ...parsed, ...overrides });
        } catch {
            appConfig.featureFlagsJson = JSON.stringify(overrides);
        }
    }

    return appConfigResponseBody;
}
