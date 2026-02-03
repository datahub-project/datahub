import { describe, expect, it } from 'vitest';

import {
    KNOWN_MCP_LOGOS,
    extractHostname,
    getLogoFromDisplayName,
    getPluginLogoUrl,
} from '@app/settingsV2/platform/aiPlugins/utils/pluginLogoUtils';

describe('pluginLogoUtils', () => {
    describe('extractHostname', () => {
        it('extracts hostname from URL with protocol', () => {
            expect(extractHostname('https://api.github.com/mcp')).toBe('api.github.com');
            expect(extractHostname('http://example.com/path')).toBe('example.com');
        });

        it('extracts hostname from URL without protocol', () => {
            expect(extractHostname('api.github.com/mcp')).toBe('api.github.com');
            expect(extractHostname('example.com')).toBe('example.com');
        });

        it('returns lowercase hostname', () => {
            expect(extractHostname('https://API.GitHub.COM')).toBe('api.github.com');
        });

        it('returns null for invalid URLs', () => {
            expect(extractHostname('')).toBeNull();
            expect(extractHostname('not a url at all')).toBeNull();
        });
    });

    describe('getLogoFromDisplayName', () => {
        it('returns null for empty display name', () => {
            expect(getLogoFromDisplayName('')).toBeNull();
        });

        it('matches display name to known platforms', () => {
            // dbt is in PLATFORM_URN_TO_LOGO
            const dbtLogo = getLogoFromDisplayName('dbt Cloud MCP Server');
            expect(dbtLogo).not.toBeNull();
        });

        it('matches case-insensitively', () => {
            const logo = getLogoFromDisplayName('DBT Cloud');
            expect(logo).not.toBeNull();
        });

        it('returns null for unknown platforms', () => {
            expect(getLogoFromDisplayName('Random Unknown Service')).toBeNull();
        });
    });

    describe('getPluginLogoUrl', () => {
        it('returns null when both displayName and URL are empty/null', () => {
            expect(getPluginLogoUrl(null, null)).toBeNull();
            expect(getPluginLogoUrl(undefined, undefined)).toBeNull();
            expect(getPluginLogoUrl('', '')).toBeNull();
        });

        it('prioritizes URL over display name for logo matching', () => {
            // URL takes priority - a "dbt" named plugin pointing to GitHub should show GitHub logo
            const logo = getPluginLogoUrl('dbt Cloud MCP Server', 'https://api.github.com/mcp');
            expect(logo).toBe(KNOWN_MCP_LOGOS['github.com']);
        });

        it('falls back to display name when URL has no match', () => {
            // When URL doesn't match, should fall back to display name
            const logo = getPluginLogoUrl('dbt Cloud MCP Server', 'https://unknown.com');
            expect(logo).not.toBeNull();
        });

        it('returns local logo for known URL domains', () => {
            expect(getPluginLogoUrl('Custom Plugin', 'https://cloud.getdbt.com/api')).toBe(
                KNOWN_MCP_LOGOS['cloud.getdbt.com'],
            );
            expect(getPluginLogoUrl('My Plugin', 'https://account.snowflake.com/api')).toBe(
                KNOWN_MCP_LOGOS['snowflake.com'],
            );
        });

        it('returns local logo for Slack URLs', () => {
            expect(getPluginLogoUrl('Slack Integration', 'https://slack.com/api')).toBe(KNOWN_MCP_LOGOS['slack.com']);
        });

        it('returns local logo for Notion URLs', () => {
            expect(getPluginLogoUrl('Notion Plugin', 'https://api.notion.so/mcp')).toBe(KNOWN_MCP_LOGOS['notion.so']);
        });

        it('returns local logo for GitHub URLs', () => {
            expect(getPluginLogoUrl('GitHub Plugin', 'https://api.github.com/mcp')).toBe(KNOWN_MCP_LOGOS['github.com']);
        });

        it('returns null for unknown domains (no Google Favicon fallback)', () => {
            // We intentionally don't use Google Favicon - should return null
            const url = getPluginLogoUrl('Random Service', 'https://unknown-service.com/mcp');
            expect(url).toBeNull();
        });

        it('handles URLs without protocol', () => {
            const url = getPluginLogoUrl('Slack', 'slack.com/mcp');
            expect(url).toBe(KNOWN_MCP_LOGOS['slack.com']);
        });

        it('returns null for invalid URLs with unknown display name', () => {
            expect(getPluginLogoUrl('Unknown', 'not a valid url')).toBeNull();
        });
    });

    describe('KNOWN_MCP_LOGOS', () => {
        it('has entries for dbt domains', () => {
            expect(KNOWN_MCP_LOGOS['cloud.getdbt.com']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['getdbt.com']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['dbt.com']).toBeDefined();
        });

        it('has entries for Snowflake domains', () => {
            expect(KNOWN_MCP_LOGOS['snowflake.com']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['snowflakecomputing.com']).toBeDefined();
        });

        it('has entries for GitHub, Slack and Notion', () => {
            expect(KNOWN_MCP_LOGOS['github.com']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['slack.com']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['notion.so']).toBeDefined();
            expect(KNOWN_MCP_LOGOS['notion.com']).toBeDefined();
        });
    });
});
