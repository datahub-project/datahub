import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import IntegrationCard from '@app/settingsV2/personal/myIntegrations/IntegrationCard';

import { AiPluginAuthType, AiPluginConfig, AiPluginType } from '@types';

// Mock PluginLogo component
vi.mock('@app/settingsV2/platform/ai/plugins/components/PluginLogo', () => ({
    PluginLogo: ({ displayName }: { displayName: string }) => <span data-testid="plugin-logo">{displayName}</span>,
}));

// Mock Menu component - render menu items directly when visible
vi.mock('@components/components/Menu/Menu', () => ({
    Menu: ({ items, children }: { items: any[]; children: React.ReactNode }) => (
        <div data-testid="menu-container">
            {children}
            <div data-testid="menu-items">
                {items
                    .filter((item: any) => item.type === 'item')
                    .map((item: any) => (
                        <button
                            key={item.key}
                            type="button"
                            data-testid={`menu-item-${item.key}`}
                            onClick={item.onClick}
                        >
                            {item.title}
                        </button>
                    ))}
            </div>
        </div>
    ),
}));

// Mock alchemy-components
vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled, isLoading, icon: _icon, 'data-testid': testId }: any) => (
        <button type="button" data-testid={testId} onClick={onClick} disabled={disabled || isLoading}>
            {isLoading ? 'Loading...' : children}
        </button>
    ),
    Pill: ({ label }: any) => <span data-testid="pill">{label}</span>,
    Switch: ({ isChecked, isDisabled, onChange, 'data-testid': testId }: any) => (
        <button
            type="button"
            data-testid={testId}
            data-checked={isChecked}
            data-disabled={isDisabled}
            onClick={() => onChange({ target: { checked: !isChecked }, stopPropagation: () => {} })}
        >
            {isChecked ? 'On' : 'Off'}
        </button>
    ),
    colors: {
        gray: { 100: '#f0f0f0', 400: '#999', 500: '#666', 600: '#333', 1700: '#111' },
        green: { 100: '#e6ffe6', 700: '#006600' },
        yellow: { 100: '#fffde6', 700: '#666600' },
        blue: { 100: '#e6f0ff', 700: '#0066cc' },
        violet: { 100: '#f0e6ff', 600: '#6600cc' },
        red: { 500: '#ff0000' },
    },
}));

describe('IntegrationCard', () => {
    const mockOnConnect = vi.fn();
    const mockOnToggleEnabled = vi.fn();
    const mockOnDisconnect = vi.fn();

    const createPlugin = (
        overrides: Partial<{ authType: AiPluginAuthType; displayName: string; description: string }> = {},
    ): AiPluginConfig & { isConnected: boolean; isEnabled: boolean } =>
        ({
            id: 'test-plugin',
            enabled: true,
            authType: overrides.authType ?? AiPluginAuthType.UserOauth,
            serviceUrn: 'urn:li:service:test-plugin',
            type: AiPluginType.McpServer,
            service: {
                properties: {
                    displayName: overrides.displayName ?? 'Test Plugin',
                    description: overrides.description ?? 'A test plugin description',
                },
            },
            isConnected: false,
            isEnabled: false,
        }) as AiPluginConfig & { isConnected: boolean; isEnabled: boolean };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Display', () => {
        it('should display plugin name and description', () => {
            const plugin = createPlugin({ displayName: 'My Plugin', description: 'My description' });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            // Plugin name appears in both PluginLogo mock and title
            expect(screen.getAllByText('My Plugin')).toHaveLength(2);
            expect(screen.getByText('My description')).toBeInTheDocument();
        });

        it('should display "Unknown Plugin" when no display name provided', () => {
            const plugin = {
                ...createPlugin(),
                service: { properties: {} },
            };

            render(
                <IntegrationCard
                    plugin={plugin as any}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            // "Unknown Plugin" appears twice - once in PluginLogo mock and once in the title
            expect(screen.getAllByText('Unknown Plugin')).toHaveLength(2);
        });
    });

    describe('Auth Type Badge', () => {
        it('should display "OAuth" for USER_OAUTH auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('OAuth')).toBeInTheDocument();
        });

        it('should display "API Key" for USER_API_KEY auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('API Key')).toBeInTheDocument();
        });

        it('should display "Shared Key" for SHARED_API_KEY auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Shared Key')).toBeInTheDocument();
        });

        it('should display "Public" for NONE auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Public')).toBeInTheDocument();
        });
    });

    describe('Connect Button', () => {
        it('should show Connect button for USER_OAUTH when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('connect-button-test-plugin')).toBeInTheDocument();
            expect(screen.getByText('Connect')).toBeInTheDocument();
        });

        it('should show Connect button for USER_API_KEY when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('connect-button-test-plugin')).toBeInTheDocument();
        });

        it('should NOT show Connect button when already connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button-test-plugin')).not.toBeInTheDocument();
        });

        it('should NOT show Connect button for SHARED_API_KEY', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button-test-plugin')).not.toBeInTheDocument();
        });

        it('should NOT show Connect button for NONE auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button-test-plugin')).not.toBeInTheDocument();
        });

        it('should show "Connecting..." when isConnecting is true', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Loading...')).toBeInTheDocument();
        });

        it('should call onConnect when clicked', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            fireEvent.click(screen.getByTestId('connect-button-test-plugin'));
            expect(mockOnConnect).toHaveBeenCalledTimes(1);
        });
    });

    describe('Enable/Disable Toggle', () => {
        it('should show toggle for USER_OAUTH when connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch-test-plugin')).toBeInTheDocument();
        });

        it('should NOT show toggle for USER_OAUTH when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('toggle-switch-test-plugin')).not.toBeInTheDocument();
        });

        it('should always show toggle for SHARED_API_KEY', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch-test-plugin')).toBeInTheDocument();
        });

        it('should always show toggle for NONE auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch-test-plugin')).toBeInTheDocument();
        });

        it('should call onToggleEnabled when clicked', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            fireEvent.click(screen.getByTestId('toggle-switch-test-plugin'));
            expect(mockOnToggleEnabled).toHaveBeenCalledWith(true);
        });
    });

    describe('Disconnect Link', () => {
        it('should show Disconnect link for USER_OAUTH when connected and handler provided', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    onDisconnect={mockOnDisconnect}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Disconnect')).toBeInTheDocument();
        });

        it('should NOT show Disconnect link when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    onDisconnect={mockOnDisconnect}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByText('Disconnect')).not.toBeInTheDocument();
        });

        it('should NOT show Disconnect link when no handler provided', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByText('Disconnect')).not.toBeInTheDocument();
        });

        it('should NOT show Disconnect link for SHARED_API_KEY', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    onDisconnect={mockOnDisconnect}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByText('Disconnect')).not.toBeInTheDocument();
        });

        it('should call onDisconnect when clicked', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserApiKey });

            render(
                <IntegrationCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    onDisconnect={mockOnDisconnect}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            fireEvent.click(screen.getByText('Disconnect'));
            expect(mockOnDisconnect).toHaveBeenCalledTimes(1);
        });
    });
});
