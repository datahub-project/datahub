import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { message } from 'antd';
import React from 'react';
import { vi } from 'vitest';

import { ManageAiConnections } from '@app/settingsV2/personal/aiConnections/ManageAiConnections';

import { GetAiPluginsWithUserStatusDocument } from '@graphql/aiPlugins.generated';
import { AiPluginAuthType } from '@types';

// Mock console to avoid noise
vi.spyOn(console, 'log').mockImplementation(() => {});
vi.spyOn(console, 'error').mockImplementation(() => {});

// Mock antd
vi.mock('antd', () => ({
    Empty: ({ description }: any) => <div data-testid="empty">{description}</div>,
    Spin: () => <div data-testid="loading-spinner">Loading...</div>,
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

// Mock alchemy-components
vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled }: any) => (
        <button type="button" onClick={onClick} disabled={disabled}>
            {children}
        </button>
    ),
    colors: {
        gray: { 100: '#f0f0f0', 500: '#666', 600: '#333', 1700: '#111' },
        green: { 100: '#e6ffe6', 700: '#006600' },
        yellow: { 100: '#fffde6', 700: '#666600' },
        blue: { 100: '#e6f0ff', 700: '#0066cc' },
        violet: { 100: '#f0e6ff', 600: '#6600cc' },
        red: { 500: '#ff0000' },
    },
}));

// Mock @ant-design/icons
vi.mock('@ant-design/icons', () => ({
    LinkOutlined: () => <span>🔗</span>,
    SettingOutlined: () => <span>⚙️</span>,
    CheckCircleFilled: () => <span>✓</span>,
    CheckOutlined: () => <span>✓</span>,
    KeyOutlined: () => <span>🔑</span>,
}));

// Mock useOAuthConnect
const mockInitiateOAuthConnect = vi.fn();
vi.mock('../useOAuthConnect', () => ({
    useOAuthConnect: () => ({
        initiateOAuthConnect: mockInitiateOAuthConnect,
        isConnecting: false,
    }),
}));

// Mock AiConnectionCard to simplify testing
vi.mock('../AiConnectionCard', () => ({
    default: ({ plugin, isConnected, isEnabled, onConnect, onToggleEnabled, onDisconnect }: any) => (
        <div data-testid={`plugin-card-${plugin.id}`}>
            <span data-testid="plugin-name">{plugin.service?.properties?.displayName}</span>
            <span data-testid="is-connected">{isConnected ? 'connected' : 'disconnected'}</span>
            <span data-testid="is-enabled">{isEnabled ? 'enabled' : 'disabled'}</span>
            <button type="button" data-testid="connect-btn" onClick={onConnect}>
                Connect
            </button>
            <button type="button" data-testid="toggle-btn" onClick={() => onToggleEnabled(!isEnabled)}>
                Toggle
            </button>
            <button type="button" data-testid="disconnect-btn" onClick={onDisconnect}>
                Disconnect
            </button>
        </div>
    ),
}));

// Mock ApiKeyModal
vi.mock('../ApiKeyModal', () => ({
    default: ({ open, pluginName, onClose, onSubmit }: any) =>
        open ? (
            <div data-testid="api-key-modal">
                <span data-testid="modal-plugin-name">{pluginName}</span>
                <button
                    type="button"
                    data-testid="modal-submit"
                    onClick={() => {
                        // Match real ApiKeyModal behavior: catch errors from onSubmit
                        onSubmit('test-api-key').catch(() => {
                            // Error message is handled by the parent component
                        });
                    }}
                >
                    Submit
                </button>
                <button type="button" data-testid="modal-close" onClick={onClose}>
                    Close
                </button>
            </div>
        ) : null,
}));

describe('ManageAiConnections', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.spyOn(global, 'fetch').mockResolvedValue({
            ok: true,
            json: () => Promise.resolve({}),
        } as Response);
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    const createMockPlugin = (
        id: string,
        authType: AiPluginAuthType,
        enabled = true,
        displayName = `Plugin ${id}`,
    ) => ({
        id,
        authType,
        enabled,
        service: {
            urn: `urn:li:service:${id}`,
            properties: {
                displayName,
                description: `Description for ${id}`,
            },
        },
    });

    const createMockUserPlugin = (
        id: string,
        options: { enabled?: boolean | null; oauthConnected?: boolean; apiKeyConnected?: boolean } = {},
    ) => ({
        id,
        enabled: options.enabled ?? null,
        oauthConfig: options.oauthConnected !== undefined ? { isConnected: options.oauthConnected } : null,
        apiKeyConfig: options.apiKeyConnected !== undefined ? { isConnected: options.apiKeyConnected } : null,
    });

    const createQueryMock = (globalPlugins: any[] = [], userPlugins: any[] = []) => ({
        request: {
            query: GetAiPluginsWithUserStatusDocument,
        },
        result: {
            data: {
                globalSettings: {
                    aiPlugins: globalPlugins,
                },
                me: {
                    corpUser: {
                        settings: {
                            aiPluginSettings: {
                                plugins: userPlugins,
                            },
                        },
                    },
                },
            },
        },
    });

    describe('Loading State', () => {
        it('should show loading spinner while fetching data', () => {
            const mock = createQueryMock();
            mock.result = undefined as any; // Keep loading

            render(
                <MockedProvider mocks={[{ ...mock, delay: 100000 }]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
        });
    });

    describe('Empty State', () => {
        it('should show empty state when no plugins are configured', async () => {
            const mock = createQueryMock([], []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('empty')).toBeInTheDocument();
            });
        });

        it('should show empty state when all plugins are disabled by admin', async () => {
            const plugins = [
                createMockPlugin('plugin-1', AiPluginAuthType.UserOauth, false),
                createMockPlugin('plugin-2', AiPluginAuthType.SharedApiKey, false),
            ];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('empty')).toBeInTheDocument();
            });
        });
    });

    describe('Plugin Display', () => {
        it('should display enabled plugins', async () => {
            const plugins = [
                createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth, true, 'OAuth Plugin'),
                createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey, true, 'API Key Plugin'),
            ];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-oauth-plugin')).toBeInTheDocument();
                expect(screen.getByTestId('plugin-card-apikey-plugin')).toBeInTheDocument();
            });
        });

        it('should NOT display disabled plugins', async () => {
            const plugins = [
                createMockPlugin('enabled-plugin', AiPluginAuthType.UserOauth, true),
                createMockPlugin('disabled-plugin', AiPluginAuthType.UserOauth, false),
            ];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-enabled-plugin')).toBeInTheDocument();
                expect(screen.queryByTestId('plugin-card-disabled-plugin')).not.toBeInTheDocument();
            });
        });
    });

    describe('Connection Status - USER_OAUTH', () => {
        it('should show as disconnected when user has no oauth config', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('disconnected');
            });
        });

        it('should show as connected when user has oauth connected', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('connected');
            });
        });
    });

    describe('Connection Status - USER_API_KEY', () => {
        it('should show as disconnected when user has no api key config', async () => {
            const plugins = [createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-apikey-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('disconnected');
            });
        });

        it('should show as connected when user has api key connected', async () => {
            const plugins = [createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey)];
            const userPlugins = [createMockUserPlugin('apikey-plugin', { apiKeyConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-apikey-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('connected');
            });
        });
    });

    describe('Connection Status - SHARED_API_KEY / NONE', () => {
        it('should always show as connected for SHARED_API_KEY', async () => {
            const plugins = [createMockPlugin('shared-plugin', AiPluginAuthType.SharedApiKey)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-shared-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('connected');
            });
        });

        it('should always show as connected for NONE auth type', async () => {
            const plugins = [createMockPlugin('public-plugin', AiPluginAuthType.None)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-public-plugin');
                expect(card.querySelector('[data-testid="is-connected"]')?.textContent).toBe('connected');
            });
        });
    });

    describe('Enabled Status - Default Behavior', () => {
        it('should be disabled by default for USER_OAUTH when not connected', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('disabled');
            });
        });

        it('should be disabled for USER_OAUTH when connected but no explicit enabled setting', async () => {
            // With stricter logic, connected alone is not enough - need explicit enabled=true
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('disabled');
            });
        });

        it('should be enabled for USER_OAUTH when connected AND explicitly enabled', async () => {
            // The backend sets enabled=true on OAuth connection, so this is the expected state
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { enabled: true, oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('enabled');
            });
        });

        it('should be disabled by default for SHARED_API_KEY', async () => {
            const plugins = [createMockPlugin('shared-plugin', AiPluginAuthType.SharedApiKey)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-shared-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('disabled');
            });
        });

        it('should be disabled by default for NONE auth type', async () => {
            const plugins = [createMockPlugin('public-plugin', AiPluginAuthType.None)];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-public-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('disabled');
            });
        });
    });

    describe('Enabled Status - Explicit User Setting', () => {
        it('should respect user explicit enabled=true setting', async () => {
            const plugins = [createMockPlugin('shared-plugin', AiPluginAuthType.SharedApiKey)];
            const userPlugins = [createMockUserPlugin('shared-plugin', { enabled: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-shared-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('enabled');
            });
        });

        it('should respect user explicit enabled=false setting', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth)];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { enabled: false, oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                const card = screen.getByTestId('plugin-card-oauth-plugin');
                expect(card.querySelector('[data-testid="is-enabled"]')?.textContent).toBe('disabled');
            });
        });
    });

    describe('Connect Actions', () => {
        it('should call initiateOAuthConnect for USER_OAUTH plugin', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth, true, 'OAuth Plugin')];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-oauth-plugin')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('connect-btn'));

            expect(mockInitiateOAuthConnect).toHaveBeenCalledWith('oauth-plugin');
        });

        it('should open API key modal for USER_API_KEY plugin', async () => {
            const plugins = [createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey, true, 'API Key Plugin')];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-apikey-plugin')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('connect-btn'));

            await waitFor(() => {
                expect(screen.getByTestId('api-key-modal')).toBeInTheDocument();
                expect(screen.getByTestId('modal-plugin-name').textContent).toBe('API Key Plugin');
            });
        });
    });

    describe('API Key Submission', () => {
        it('should submit API key and show success message', async () => {
            const plugins = [createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey, true, 'API Key Plugin')];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-apikey-plugin')).toBeInTheDocument();
            });

            // Open modal
            fireEvent.click(screen.getByTestId('connect-btn'));

            await waitFor(() => {
                expect(screen.getByTestId('api-key-modal')).toBeInTheDocument();
            });

            // Submit
            fireEvent.click(screen.getByTestId('modal-submit'));

            await waitFor(() => {
                expect(fetch).toHaveBeenCalledWith(
                    '/integrations/oauth/plugins/apikey-plugin/api-key',
                    expect.objectContaining({
                        method: 'POST',
                        body: JSON.stringify({ api_key: 'test-api-key' }),
                    }),
                );
                expect(message.success).toHaveBeenCalledWith('Connected to API Key Plugin successfully!');
            });
        });

        it('should show error message on API key submission failure', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 400,
                json: () => Promise.resolve({ detail: 'Invalid API key' }),
            } as Response);

            const plugins = [createMockPlugin('apikey-plugin', AiPluginAuthType.UserApiKey, true, 'API Key Plugin')];
            const mock = createQueryMock(plugins, []);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-apikey-plugin')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('connect-btn'));

            await waitFor(() => {
                expect(screen.getByTestId('api-key-modal')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('modal-submit'));

            await waitFor(() => {
                expect(message.error).toHaveBeenCalledWith('Invalid API key');
            });
        });
    });

    describe('Disconnect', () => {
        it('should call disconnect endpoint and show success message', async () => {
            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth, true, 'OAuth Plugin')];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-oauth-plugin')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('disconnect-btn'));

            await waitFor(() => {
                expect(fetch).toHaveBeenCalledWith(
                    '/integrations/oauth/plugins/oauth-plugin/disconnect',
                    expect.objectContaining({
                        method: 'DELETE',
                    }),
                );
                expect(message.success).toHaveBeenCalledWith('Disconnected from OAuth Plugin');
            });
        });

        it('should show error message on disconnect failure', async () => {
            vi.spyOn(global, 'fetch').mockResolvedValue({
                ok: false,
                status: 500,
                json: () => Promise.resolve({ detail: 'Server error' }),
            } as Response);

            const plugins = [createMockPlugin('oauth-plugin', AiPluginAuthType.UserOauth, true, 'OAuth Plugin')];
            const userPlugins = [createMockUserPlugin('oauth-plugin', { oauthConnected: true })];
            const mock = createQueryMock(plugins, userPlugins);

            render(
                <MockedProvider mocks={[mock]} addTypename={false}>
                    <ManageAiConnections />
                </MockedProvider>,
            );

            await waitFor(() => {
                expect(screen.getByTestId('plugin-card-oauth-plugin')).toBeInTheDocument();
            });

            fireEvent.click(screen.getByTestId('disconnect-btn'));

            await waitFor(() => {
                expect(message.error).toHaveBeenCalledWith('Server error');
            });
        });
    });
});
