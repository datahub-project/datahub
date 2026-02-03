import {
    GlobalPlugin,
    UserPluginConfig,
    isUserConnected,
    mergePluginsWithUserConfig,
} from '@app/settingsV2/personal/myIntegrations/utils/pluginDataMapper';

import { AiPluginAuthType } from '@types';

describe('pluginDataMapper', () => {
    describe('isUserConnected', () => {
        it('returns true for NONE auth type regardless of config', () => {
            expect(isUserConnected(AiPluginAuthType.None, undefined)).toBe(true);
        });

        it('returns true for SHARED_API_KEY auth type regardless of config', () => {
            expect(isUserConnected(AiPluginAuthType.SharedApiKey, undefined)).toBe(true);
        });

        it('returns false for USER_OAUTH when not connected', () => {
            expect(isUserConnected(AiPluginAuthType.UserOauth, undefined)).toBe(false);
            expect(
                isUserConnected(AiPluginAuthType.UserOauth, {
                    id: 'test',
                    oauthConfig: { isConnected: false },
                }),
            ).toBe(false);
        });

        it('returns true for USER_OAUTH when connected', () => {
            expect(
                isUserConnected(AiPluginAuthType.UserOauth, {
                    id: 'test',
                    oauthConfig: { isConnected: true },
                }),
            ).toBe(true);
        });

        it('returns false for USER_API_KEY when not connected', () => {
            expect(isUserConnected(AiPluginAuthType.UserApiKey, undefined)).toBe(false);
            expect(
                isUserConnected(AiPluginAuthType.UserApiKey, {
                    id: 'test',
                    apiKeyConfig: { isConnected: false },
                }),
            ).toBe(false);
        });

        it('returns true for USER_API_KEY when connected', () => {
            expect(
                isUserConnected(AiPluginAuthType.UserApiKey, {
                    id: 'test',
                    apiKeyConfig: { isConnected: true },
                }),
            ).toBe(true);
        });
    });

    describe('mergePluginsWithUserConfig', () => {
        const mockGlobalPlugins: GlobalPlugin[] = [
            {
                id: 'plugin-1',
                enabled: true,
                authType: AiPluginAuthType.UserOauth,
                service: { properties: { displayName: 'GitHub', description: 'GitHub integration' } },
            },
            {
                id: 'plugin-2',
                enabled: true,
                authType: AiPluginAuthType.None,
                service: { properties: { displayName: 'Public Plugin' } },
            },
            {
                id: 'plugin-3',
                enabled: false,
                authType: AiPluginAuthType.UserApiKey,
                service: { properties: { displayName: 'Disabled Plugin' } },
            },
        ];

        it('filters out disabled plugins', () => {
            const result = mergePluginsWithUserConfig(mockGlobalPlugins, []);
            expect(result).toHaveLength(2);
            expect(result.find((p) => p.id === 'plugin-3')).toBeUndefined();
        });

        it('merges user config with global plugins', () => {
            const userPlugins: UserPluginConfig[] = [
                {
                    id: 'plugin-1',
                    enabled: true,
                    oauthConfig: { isConnected: true },
                    customHeaders: [{ key: 'X-Custom', value: 'test' }],
                },
            ];

            const result = mergePluginsWithUserConfig(mockGlobalPlugins, userPlugins);
            const githubPlugin = result.find((p) => p.id === 'plugin-1');

            expect(githubPlugin).toBeDefined();
            expect(githubPlugin?.isConnected).toBe(true);
            expect(githubPlugin?.isEnabled).toBe(true);
            expect(githubPlugin?.customHeaders).toHaveLength(1);
        });

        it('sets isEnabled to false when user has not enabled', () => {
            const userPlugins: UserPluginConfig[] = [
                {
                    id: 'plugin-1',
                    enabled: false,
                    oauthConfig: { isConnected: true },
                },
            ];

            const result = mergePluginsWithUserConfig(mockGlobalPlugins, userPlugins);
            const githubPlugin = result.find((p) => p.id === 'plugin-1');

            expect(githubPlugin?.isConnected).toBe(true);
            expect(githubPlugin?.isEnabled).toBe(false);
        });

        it('handles plugins with no user config', () => {
            const result = mergePluginsWithUserConfig(mockGlobalPlugins, []);
            const publicPlugin = result.find((p) => p.id === 'plugin-2');

            expect(publicPlugin?.isConnected).toBe(true); // NONE auth = always connected
            expect(publicPlugin?.isEnabled).toBe(false);
            expect(publicPlugin?.customHeaders).toEqual([]);
        });

        it('returns empty custom headers when not configured', () => {
            const userPlugins: UserPluginConfig[] = [{ id: 'plugin-1' }];

            const result = mergePluginsWithUserConfig(mockGlobalPlugins, userPlugins);
            const githubPlugin = result.find((p) => p.id === 'plugin-1');

            expect(githubPlugin?.customHeaders).toEqual([]);
        });
    });
});
