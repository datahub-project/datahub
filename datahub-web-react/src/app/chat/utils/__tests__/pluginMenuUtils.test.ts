import { describe, expect, it } from 'vitest';

import {
    PluginWithUserStatus,
    getEmptyStateMessage,
    getPluginDisplayInfo,
    requiresUserConnection,
    shouldShowPluginAnimation,
} from '@app/chat/utils/pluginMenuUtils';

import { AiPluginAuthType } from '@types';

describe('pluginMenuUtils', () => {
    describe('requiresUserConnection', () => {
        it('should return true for UserOauth', () => {
            expect(requiresUserConnection(AiPluginAuthType.UserOauth)).toBe(true);
        });

        it('should return true for UserApiKey', () => {
            expect(requiresUserConnection(AiPluginAuthType.UserApiKey)).toBe(true);
        });

        it('should return false for SharedApiKey', () => {
            expect(requiresUserConnection(AiPluginAuthType.SharedApiKey)).toBe(false);
        });

        it('should return false for None', () => {
            expect(requiresUserConnection(AiPluginAuthType.None)).toBe(false);
        });
    });

    describe('getPluginDisplayInfo', () => {
        const createMockPlugin = (overrides: Partial<PluginWithUserStatus> = {}): PluginWithUserStatus => ({
            id: 'test-plugin',
            authType: AiPluginAuthType.None,
            isConnected: false,
            isEnabled: true,
            service: {
                properties: {
                    displayName: 'Test Plugin',
                },
                mcpServerProperties: {
                    url: 'https://example.com',
                },
            },
            ...overrides,
        });

        it('should return correct display info for a basic plugin', () => {
            const plugin = createMockPlugin();
            const result = getPluginDisplayInfo(plugin);

            expect(result).toEqual({
                displayName: 'Test Plugin',
                url: 'https://example.com',
                needsUserConnection: false,
                isConnected: false,
                showConnectButton: false,
                showToggle: true,
            });
        });

        it('should use fallback displayName when not provided', () => {
            const plugin = createMockPlugin({
                service: {
                    properties: {
                        displayName: null,
                    },
                    mcpServerProperties: {
                        url: null,
                    },
                },
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.displayName).toBe('Unknown Plugin');
            expect(result.url).toBeNull();
        });

        it('should use fallback displayName when service is missing', () => {
            const plugin = createMockPlugin({
                service: undefined,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.displayName).toBe('Unknown Plugin');
            expect(result.url).toBeNull();
        });

        it('should show connect button for UserOauth when not connected', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.UserOauth,
                isConnected: false,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(true);
            expect(result.showConnectButton).toBe(true);
            expect(result.showToggle).toBe(false);
        });

        it('should show toggle for UserOauth when connected', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.UserOauth,
                isConnected: true,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(true);
            expect(result.showConnectButton).toBe(false);
            expect(result.showToggle).toBe(true);
        });

        it('should show connect button for UserApiKey when not connected', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.UserApiKey,
                isConnected: false,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(true);
            expect(result.showConnectButton).toBe(true);
            expect(result.showToggle).toBe(false);
        });

        it('should show toggle for UserApiKey when connected', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.UserApiKey,
                isConnected: true,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(true);
            expect(result.showConnectButton).toBe(false);
            expect(result.showToggle).toBe(true);
        });

        it('should always show toggle for SharedApiKey', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.SharedApiKey,
                isConnected: false,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(false);
            expect(result.showConnectButton).toBe(false);
            expect(result.showToggle).toBe(true);
        });

        it('should always show toggle for None auth type', () => {
            const plugin = createMockPlugin({
                authType: AiPluginAuthType.None,
                isConnected: false,
            });
            const result = getPluginDisplayInfo(plugin);

            expect(result.needsUserConnection).toBe(false);
            expect(result.showConnectButton).toBe(false);
            expect(result.showToggle).toBe(true);
        });
    });

    describe('shouldShowPluginAnimation', () => {
        it('should return true when user has not seen menu and plugins are available', () => {
            expect(shouldShowPluginAnimation(false, 1)).toBe(true);
            expect(shouldShowPluginAnimation(false, 5)).toBe(true);
        });

        it('should return false when user has seen menu', () => {
            expect(shouldShowPluginAnimation(true, 1)).toBe(false);
            expect(shouldShowPluginAnimation(true, 5)).toBe(false);
        });

        it('should return false when no plugins are available', () => {
            expect(shouldShowPluginAnimation(false, 0)).toBe(false);
            expect(shouldShowPluginAnimation(true, 0)).toBe(false);
        });

        it('should return false when both conditions are not met', () => {
            expect(shouldShowPluginAnimation(true, 0)).toBe(false);
        });
    });

    describe('getEmptyStateMessage', () => {
        it('should return admin message when user is admin', () => {
            const message = getEmptyStateMessage(true);
            expect(message).toBe('No AI plugins have been set up yet.');
        });

        it('should return non-admin message when user is not admin', () => {
            const message = getEmptyStateMessage(false);
            expect(message).toBe('No AI plugins have been set up. Please contact your administrator to set them up.');
        });
    });
});
