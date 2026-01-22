import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import AiConnectionCard from '@app/settingsV2/personal/aiConnections/AiConnectionCard';

import { AiPluginAuthType, AiPluginConfig, AiPluginType } from '@types';

// Mock antd components
vi.mock('antd', () => ({
    Switch: ({ checked, onChange, loading, size }: any) => (
        <button
            type="button"
            data-testid="toggle-switch"
            data-checked={checked}
            data-loading={loading}
            data-size={size}
            onClick={() => onChange(!checked)}
        >
            {checked ? 'On' : 'Off'}
        </button>
    ),
    Tooltip: ({ children, title }: any) => <span title={title}>{children}</span>,
}));

// Mock alchemy-components
vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled, isLoading, icon: _icon }: any) => (
        <button type="button" data-testid="connect-button" onClick={onClick} disabled={disabled || isLoading}>
            {isLoading ? 'Loading...' : children}
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

// Mock @ant-design/icons
vi.mock('@ant-design/icons', () => ({
    CheckCircleFilled: () => <span data-testid="check-circle-icon">✓</span>,
    CheckOutlined: () => <span data-testid="check-icon">✓</span>,
}));

describe('AiConnectionCard', () => {
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
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('My Plugin')).toBeInTheDocument();
            expect(screen.getByText('My description')).toBeInTheDocument();
        });

        it('should display "Unknown Plugin" when no display name provided', () => {
            const plugin = {
                ...createPlugin(),
                service: { properties: {} },
            };

            render(
                <AiConnectionCard
                    plugin={plugin as any}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Unknown Plugin')).toBeInTheDocument();
        });
    });

    describe('Auth Type Badge', () => {
        it('should display "OAuth" for USER_OAUTH auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
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
                <AiConnectionCard
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
                <AiConnectionCard
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
                <AiConnectionCard
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

    describe('Status Display - USER_OAUTH/USER_API_KEY plugins', () => {
        it('should show "Not Connected" when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Not Connected')).toBeInTheDocument();
        });

        it('should show "Connected" when connected and enabled', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Connected')).toBeInTheDocument();
            expect(screen.getByTestId('check-circle-icon')).toBeInTheDocument();
        });

        it('should show "Connected (Disabled)" when connected but disabled', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Connected (Disabled)')).toBeInTheDocument();
        });
    });

    describe('Status Display - SHARED_API_KEY/NONE plugins', () => {
        it('should show "Ready to use" when enabled', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Ready to use')).toBeInTheDocument();
            expect(screen.getByTestId('check-icon')).toBeInTheDocument();
        });

        it('should show "Available" when not enabled', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Available')).toBeInTheDocument();
        });
    });

    describe('Connect Button', () => {
        it('should show Connect button for USER_OAUTH when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('connect-button')).toBeInTheDocument();
            expect(screen.getByText('Connect')).toBeInTheDocument();
        });

        it('should show Connect button for USER_API_KEY when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('connect-button')).toBeInTheDocument();
        });

        it('should NOT show Connect button when already connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button')).not.toBeInTheDocument();
        });

        it('should NOT show Connect button for SHARED_API_KEY', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button')).not.toBeInTheDocument();
        });

        it('should NOT show Connect button for NONE auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('connect-button')).not.toBeInTheDocument();
        });

        it('should show "Connecting..." when isConnecting is true', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
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
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            fireEvent.click(screen.getByTestId('connect-button'));
            expect(mockOnConnect).toHaveBeenCalledTimes(1);
        });
    });

    describe('Enable/Disable Toggle', () => {
        it('should show toggle for USER_OAUTH when connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch')).toBeInTheDocument();
            expect(screen.getByText('Enabled')).toBeInTheDocument();
        });

        it('should NOT show toggle for USER_OAUTH when not connected', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.queryByTestId('toggle-switch')).not.toBeInTheDocument();
        });

        it('should always show toggle for SHARED_API_KEY', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch')).toBeInTheDocument();
        });

        it('should always show toggle for NONE auth type', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.None });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected={false}
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByTestId('toggle-switch')).toBeInTheDocument();
        });

        it('should show "Disabled" text when not enabled', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            expect(screen.getByText('Disabled')).toBeInTheDocument();
        });

        it('should call onToggleEnabled when clicked', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.SharedApiKey });

            render(
                <AiConnectionCard
                    plugin={plugin}
                    isConnected
                    isEnabled={false}
                    onConnect={mockOnConnect}
                    onToggleEnabled={mockOnToggleEnabled}
                    isConnecting={false}
                    isToggling={false}
                />,
            );

            fireEvent.click(screen.getByTestId('toggle-switch'));
            expect(mockOnToggleEnabled).toHaveBeenCalledWith(true);
        });
    });

    describe('Disconnect Link', () => {
        it('should show Disconnect link for USER_OAUTH when connected and handler provided', () => {
            const plugin = createPlugin({ authType: AiPluginAuthType.UserOauth });

            render(
                <AiConnectionCard
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
                <AiConnectionCard
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
                <AiConnectionCard
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
                <AiConnectionCard
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
                <AiConnectionCard
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
