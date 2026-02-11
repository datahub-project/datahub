import { describe, expect, it } from 'vitest';

import {
    PLUGIN_SOURCES,
    detectPluginSourceName,
    getPluginSource,
} from '@app/settingsV2/platform/ai/plugins/sources/pluginSources';

// ---------------------------------------------------------------------------
// Registry integrity
// ---------------------------------------------------------------------------

describe('PLUGIN_SOURCES registry', () => {
    it('contains at least the four core sources', () => {
        const names = PLUGIN_SOURCES.map((s) => s.name);
        expect(names).toContain('custom');
        expect(names).toContain('dbt');
        expect(names).toContain('github');
        expect(names).toContain('snowflake');
    });

    it.each(PLUGIN_SOURCES.map((s) => [s.name, s]))('%s has all required fields populated', (_name, source) => {
        expect(source.name).toBeTruthy();
        expect(source.displayName).toBeTruthy();
        expect(source.description).toBeTruthy();
        expect(source.allowedAuthTypes.length).toBeGreaterThan(0);
        expect(source.visibleFields.length).toBeGreaterThan(0);
        // Every source must include displayName in visible fields
        expect(source.visibleFields).toContain('displayName');
    });

    it('every non-custom source has a configSubtitle', () => {
        PLUGIN_SOURCES.filter((s) => s.name !== 'custom').forEach((source) => {
            expect(source.configSubtitle).toBeTruthy();
        });
    });
});

// ---------------------------------------------------------------------------
// Source-specific configuration
// ---------------------------------------------------------------------------

describe('dbt source configuration', () => {
    it('has structuredHeaders defined', () => {
        const dbt = PLUGIN_SOURCES.find((s) => s.name === 'dbt');
        expect(dbt?.structuredHeaders).toBeDefined();
        expect(dbt?.structuredHeaders?.sectionTitle).toBe('Configuration');
    });

    it('has the required production environment field', () => {
        const dbt = PLUGIN_SOURCES.find((s) => s.name === 'dbt');
        const prodField = dbt?.structuredHeaders?.fields.find((f) => f.headerKey === 'x-dbt-prod-environment-id');
        expect(prodField).toBeDefined();
        expect(prodField?.required).toBe(true);
    });

    it('has development environment ID and user ID fields', () => {
        const dbt = PLUGIN_SOURCES.find((s) => s.name === 'dbt');
        const devField = dbt?.structuredHeaders?.fields.find((f) => f.headerKey === 'x-dbt-dev-environment-id');
        const userField = dbt?.structuredHeaders?.fields.find((f) => f.headerKey === 'x-dbt-user-id');
        expect(devField).toBeDefined();
        expect(userField).toBeDefined();
        // User ID should only be visible for SharedApiKey
        expect(userField?.visibleForAuthTypes).toContain('SHARED_API_KEY');
    });
});

describe('github source configuration', () => {
    it('has repo as default oauthScopes', () => {
        const github = PLUGIN_SOURCES.find((s) => s.name === 'github');
        expect(github?.defaults?.oauthScopes).toBe('repo');
    });
});

// ---------------------------------------------------------------------------
// getPluginSource
// ---------------------------------------------------------------------------

describe('getPluginSource', () => {
    it('returns the correct source by name', () => {
        expect(getPluginSource('github').name).toBe('github');
        expect(getPluginSource('dbt').name).toBe('dbt');
        expect(getPluginSource('snowflake').name).toBe('snowflake');
        expect(getPluginSource('custom').name).toBe('custom');
    });

    it('falls back to custom for unknown names', () => {
        expect(getPluginSource('unknown').name).toBe('custom');
        expect(getPluginSource('').name).toBe('custom');
    });
});

// ---------------------------------------------------------------------------
// detectPluginSourceName
// ---------------------------------------------------------------------------

describe('detectPluginSourceName', () => {
    describe('URL-based detection', () => {
        it('detects GitHub from URL', () => {
            expect(detectPluginSourceName('https://api.githubcopilot.com/mcp/')).toBe('github');
            expect(detectPluginSourceName('https://github.com/some/path')).toBe('github');
        });

        it('detects dbt from URL', () => {
            expect(detectPluginSourceName('https://cloud.getdbt.com/api/ai/v1/mcp/')).toBe('dbt');
            expect(detectPluginSourceName('https://prefix.us1.dbt.com/api/ai/v1/mcp/')).toBe('dbt');
        });

        it('detects Snowflake from URL', () => {
            expect(detectPluginSourceName('https://account.snowflakecomputing.com/api/v2/...')).toBe('snowflake');
            expect(detectPluginSourceName('https://my-snowflake.example.com/mcp')).toBe('snowflake');
        });

        it('is case insensitive', () => {
            expect(detectPluginSourceName('https://API.GITHUBCOPILOT.COM/mcp/')).toBe('github');
            expect(detectPluginSourceName('https://CLOUD.GETDBT.COM/api/')).toBe('dbt');
        });
    });

    describe('display name-based detection', () => {
        it('detects GitHub from display name', () => {
            expect(detectPluginSourceName(null, 'GitHub Copilot')).toBe('github');
            expect(detectPluginSourceName(null, 'My GitHub Plugin')).toBe('github');
        });

        it('detects dbt from display name', () => {
            expect(detectPluginSourceName(null, 'dbt Cloud')).toBe('dbt');
        });

        it('detects Snowflake from display name', () => {
            expect(detectPluginSourceName(null, 'Snowflake Analytics')).toBe('snowflake');
        });
    });

    describe('fallback behavior', () => {
        it('returns custom when no URL or display name is provided', () => {
            expect(detectPluginSourceName()).toBe('custom');
            expect(detectPluginSourceName(null, null)).toBe('custom');
            expect(detectPluginSourceName(undefined, undefined)).toBe('custom');
        });

        it('returns custom for unrecognized URL', () => {
            expect(detectPluginSourceName('https://unknown-server.com/mcp')).toBe('custom');
        });

        it('returns custom for unrecognized display name', () => {
            expect(detectPluginSourceName(null, 'My Custom Plugin')).toBe('custom');
        });

        it('prefers URL detection over display name', () => {
            // URL says GitHub, name says dbt — URL wins
            expect(detectPluginSourceName('https://api.githubcopilot.com/mcp/', 'dbt Cloud')).toBe('github');
        });
    });
});
