import { describe, expect, it } from 'vitest';

import {
    DEFAULT_INTEGRATIONS_TAB,
    INTEGRATIONS_TAB_URL_MAP,
    IntegrationsTabType,
    determineActiveTabFromUrl,
    filterIntegrationsByFeatureFlags,
    getTabFromPath,
    isBasePath,
    isIntegrationDetailPage,
} from '@app/settingsV2/platform/PlatformIntegrations.types';

describe('PlatformIntegrations.types', () => {
    describe('getTabFromPath', () => {
        it('returns correct tab for notifications path', () => {
            expect(getTabFromPath('/settings/integrations/notifications')).toBe(IntegrationsTabType.Notifications);
        });

        it('returns correct tab for data integrations path', () => {
            expect(getTabFromPath('/settings/integrations/data')).toBe(IntegrationsTabType.DataIntegrations);
        });

        it('returns correct tab for ai-plugins path', () => {
            expect(getTabFromPath('/settings/integrations/ai-plugins')).toBe(IntegrationsTabType.AiPlugins);
        });

        it('returns correct tab for nested paths', () => {
            expect(getTabFromPath('/settings/integrations/notifications/slack')).toBe(
                IntegrationsTabType.Notifications,
            );
        });

        it('returns undefined for unrecognized paths', () => {
            expect(getTabFromPath('/settings/other')).toBeUndefined();
            expect(getTabFromPath('/completely/different')).toBeUndefined();
        });
    });

    describe('isBasePath', () => {
        it('returns true for exact base path', () => {
            expect(isBasePath('/settings/integrations')).toBe(true);
        });

        it('returns true for base path with trailing slash', () => {
            expect(isBasePath('/settings/integrations/')).toBe(true);
        });

        it('returns false for tab-specific paths', () => {
            expect(isBasePath('/settings/integrations/notifications')).toBe(false);
            expect(isBasePath('/settings/integrations/ai-plugins')).toBe(false);
        });
    });

    describe('determineActiveTabFromUrl', () => {
        it('returns tab without redirect for recognized tab path', () => {
            const result = determineActiveTabFromUrl('/settings/integrations/ai-plugins');

            expect(result.tab).toBe(IntegrationsTabType.AiPlugins);
            expect(result.shouldRedirect).toBe(false);
            expect(result.redirectUrl).toBeUndefined();
        });

        it('returns default tab with redirect for base path', () => {
            const result = determineActiveTabFromUrl('/settings/integrations');

            expect(result.tab).toBe(DEFAULT_INTEGRATIONS_TAB);
            expect(result.shouldRedirect).toBe(true);
            expect(result.redirectUrl).toBe(INTEGRATIONS_TAB_URL_MAP[DEFAULT_INTEGRATIONS_TAB]);
        });

        it('returns undefined tab for unrecognized paths', () => {
            const result = determineActiveTabFromUrl('/somewhere/else');

            expect(result.tab).toBeUndefined();
            expect(result.shouldRedirect).toBe(false);
        });
    });

    describe('INTEGRATIONS_TAB_URL_MAP', () => {
        it('has entries for all tab types', () => {
            expect(INTEGRATIONS_TAB_URL_MAP[IntegrationsTabType.Notifications]).toBeDefined();
            expect(INTEGRATIONS_TAB_URL_MAP[IntegrationsTabType.DataIntegrations]).toBeDefined();
            expect(INTEGRATIONS_TAB_URL_MAP[IntegrationsTabType.AiPlugins]).toBeDefined();
        });

        it('has correct URL format', () => {
            Object.values(INTEGRATIONS_TAB_URL_MAP).forEach((url) => {
                expect(url).toMatch(/^\/settings\/integrations\//);
            });
        });
    });

    describe('filterIntegrationsByFeatureFlags', () => {
        const integrations = [
            { id: 'slack', name: 'Slack' },
            { id: 'microsoft-teams', name: 'Teams' },
            { id: 'email', name: 'Email' },
        ];

        it('includes all integrations when Teams flag is enabled', () => {
            const result = filterIntegrationsByFeatureFlags(integrations, { teamsNotificationsEnabled: true });

            expect(result).toHaveLength(3);
            expect(result.map((i) => i.id)).toContain('microsoft-teams');
        });

        it('excludes Teams when flag is disabled', () => {
            const result = filterIntegrationsByFeatureFlags(integrations, { teamsNotificationsEnabled: false });

            expect(result).toHaveLength(2);
            expect(result.map((i) => i.id)).not.toContain('microsoft-teams');
        });

        it('excludes Teams when flag is undefined', () => {
            const result = filterIntegrationsByFeatureFlags(integrations, {});

            expect(result).toHaveLength(2);
            expect(result.map((i) => i.id)).not.toContain('microsoft-teams');
        });

        it('preserves non-Teams integrations regardless of flag', () => {
            const result = filterIntegrationsByFeatureFlags(integrations, { teamsNotificationsEnabled: false });

            expect(result.map((i) => i.id)).toContain('slack');
            expect(result.map((i) => i.id)).toContain('email');
        });
    });

    describe('isIntegrationDetailPage', () => {
        const integrations = [{ id: 'slack' }, { id: 'snowflake' }, { id: 'bigquery' }];

        it('returns true for integration detail paths', () => {
            expect(isIntegrationDetailPage('/settings/integrations/slack', integrations)).toBe(true);
            expect(isIntegrationDetailPage('/settings/integrations/snowflake', integrations)).toBe(true);
        });

        it('returns false for tab paths', () => {
            expect(isIntegrationDetailPage('/settings/integrations/notifications', integrations)).toBe(false);
            expect(isIntegrationDetailPage('/settings/integrations/ai-plugins', integrations)).toBe(false);
        });

        it('returns false for base path', () => {
            expect(isIntegrationDetailPage('/settings/integrations', integrations)).toBe(false);
        });

        it('returns false for unrelated paths', () => {
            expect(isIntegrationDetailPage('/settings/other', integrations)).toBe(false);
        });
    });
});
