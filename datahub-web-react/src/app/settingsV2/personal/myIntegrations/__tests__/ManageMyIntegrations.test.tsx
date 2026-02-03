import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { ManageMyIntegrations } from '@app/settingsV2/personal/myIntegrations/ManageMyIntegrations';

import { GetAiPluginsWithUserStatusDocument } from '@graphql/aiPlugins.generated';
import { AiPluginAuthType } from '@types';

// Mock antd
vi.mock('antd', () => ({
    Empty: ({ description }: { description: React.ReactNode }) => <div data-testid="empty">{description}</div>,
    Spin: () => <div data-testid="loading-spinner">Loading...</div>,
    message: {
        success: vi.fn(),
        error: vi.fn(),
    },
}));

// Mock alchemy-components
vi.mock('@src/alchemy-components', () => ({
    Button: ({
        children,
        onClick,
        disabled,
    }: {
        children: React.ReactNode;
        onClick?: () => void;
        disabled?: boolean;
    }) => (
        <button type="button" onClick={onClick} disabled={disabled}>
            {children}
        </button>
    ),
    PageTitle: ({ title, subTitle }: { title: string; subTitle?: string }) => (
        <div data-testid="page-title">
            <h1>{title}</h1>
            {subTitle && <p>{subTitle}</p>}
        </div>
    ),
    Pill: ({ label }: { label: string }) => <span data-testid="pill">{label}</span>,
    Switch: ({
        isChecked,
        onChange,
    }: {
        isChecked: boolean;
        onChange: (e: { target: { checked: boolean }; stopPropagation: () => void }) => void;
    }) => (
        <input
            type="checkbox"
            checked={isChecked}
            onChange={(e) => onChange({ target: { checked: e.target.checked }, stopPropagation: () => {} })}
            data-testid="toggle-switch"
        />
    ),
    colors: {
        gray: { 100: '#f0f0f0', 500: '#666', 600: '#333', 1700: '#111' },
        violet: { 100: '#f0e6ff', 600: '#6600cc' },
    },
}));

// Mock useOAuthConnect
const mockInitiateOAuthConnect = vi.fn();
vi.mock('@app/settingsV2/personal/aiConnections/useOAuthConnect', () => ({
    useOAuthConnect: () => ({
        initiateOAuthConnect: mockInitiateOAuthConnect,
        connectingPluginId: null,
    }),
}));

// Mock IntegrationCard to simplify testing
vi.mock('../IntegrationCard', () => ({
    default: ({
        plugin,
        isConnected,
        isEnabled,
    }: {
        plugin: { id: string; service?: { properties?: { displayName?: string } } };
        isConnected: boolean;
        isEnabled: boolean;
    }) => (
        <div data-testid={`plugin-card-${plugin.id}`}>
            <span data-testid="plugin-name">{plugin.service?.properties?.displayName}</span>
            <span data-testid="is-connected">{isConnected ? 'connected' : 'disconnected'}</span>
            <span data-testid="is-enabled">{isEnabled ? 'enabled' : 'disabled'}</span>
        </div>
    ),
}));

// Mock ApiKeyModal
vi.mock('@app/settingsV2/personal/aiConnections/ApiKeyModal', () => ({
    default: () => <div data-testid="api-key-modal" />,
}));

// Mock CustomHeadersModal
vi.mock('@app/settingsV2/personal/aiConnections/CustomHeadersModal', () => ({
    default: () => <div data-testid="custom-headers-modal" />,
}));

// Mock PluginLogo
vi.mock('@app/settingsV2/platform/aiPlugins/components/PluginLogo', () => ({
    PluginLogo: () => <div data-testid="plugin-logo" />,
}));

const createMockPlugin = (overrides = {}) => ({
    id: 'test-plugin-1',
    enabled: true,
    authType: AiPluginAuthType.UserOauth,
    service: {
        properties: {
            displayName: 'Test Plugin',
            description: 'A test plugin',
            url: 'https://test.com',
        },
    },
    ...overrides,
});

const createSuccessMock = (globalPlugins: ReturnType<typeof createMockPlugin>[], userPlugins: object[] = []) => ({
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
                    urn: 'urn:li:corpuser:test',
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

describe('ManageMyIntegrations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders the page title and subtitle', async () => {
        const mocks = [createSuccessMock([createMockPlugin()])];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('page-title')).toBeInTheDocument();
        });

        expect(screen.getByText('My Integrations')).toBeInTheDocument();
        expect(screen.getByText(/Manage your AI plugin preferences/)).toBeInTheDocument();
    });

    it('shows loading spinner while fetching data', () => {
        const mocks = [createSuccessMock([])];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
    });

    it('shows empty state when no plugins are available', async () => {
        const mocks = [createSuccessMock([])];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('empty')).toBeInTheDocument();
        });
    });

    it('renders plugin cards for enabled plugins', async () => {
        const plugins = [
            createMockPlugin({ id: 'plugin-1', service: { properties: { displayName: 'GitHub' } } }),
            createMockPlugin({ id: 'plugin-2', service: { properties: { displayName: 'Glean' } } }),
        ];
        const mocks = [createSuccessMock(plugins)];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('plugin-card-plugin-1')).toBeInTheDocument();
            expect(screen.getByTestId('plugin-card-plugin-2')).toBeInTheDocument();
        });
    });

    it('does not render disabled plugins', async () => {
        const plugins = [
            createMockPlugin({ id: 'enabled-plugin', enabled: true }),
            createMockPlugin({ id: 'disabled-plugin', enabled: false }),
        ];
        const mocks = [createSuccessMock(plugins)];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('plugin-card-enabled-plugin')).toBeInTheDocument();
        });

        expect(screen.queryByTestId('plugin-card-disabled-plugin')).not.toBeInTheDocument();
    });

    it('shows connected status for OAuth plugins with user config', async () => {
        const plugins = [createMockPlugin({ id: 'oauth-plugin', authType: AiPluginAuthType.UserOauth })];
        const userPlugins = [
            {
                id: 'oauth-plugin',
                enabled: true,
                oauthConfig: { isConnected: true },
            },
        ];
        const mocks = [createSuccessMock(plugins, userPlugins)];

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ManageMyIntegrations />
            </MockedProvider>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('plugin-card-oauth-plugin')).toBeInTheDocument();
        });

        expect(screen.getByText('connected')).toBeInTheDocument();
        expect(screen.getByText('enabled')).toBeInTheDocument();
    });
});
