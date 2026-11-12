import { render, screen } from '@testing-library/react';
import React from 'react';
import { Route, Switch } from 'react-router';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PlatformAiSettings } from '@app/settingsV2/platform/PlatformAiSettings';
import { aiTabUrl, getAiTabFromPath, isAiSettingsBasePath } from '@app/settingsV2/platform/PlatformAiSettings.types';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockCanManage = vi.fn();
vi.mock('@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers', () => ({
    useCanManageMcpServers: () => mockCanManage(),
}));

vi.mock('@app/settingsV2/platform/ai/mcpServers/McpServersTab', () => ({
    McpServersTab: () => <div data-testid="mcp-servers-tab" />,
}));

function renderAt(path: string) {
    let currentPath = '';
    render(
        <CustomThemeProvider>
            <MemoryRouter initialEntries={[path]}>
                <Switch>
                    <Route path="/settings/ai" component={PlatformAiSettings} />
                </Switch>
                <Route
                    path="*"
                    render={({ location }) => {
                        currentPath = location.pathname;
                        return null;
                    }}
                />
            </MemoryRouter>
        </CustomThemeProvider>,
    );
    return () => currentPath;
}

describe('PlatformAiSettings', () => {
    beforeEach(() => mockCanManage.mockReturnValue(true));

    it('sends /settings/ai to the MCP Servers tab', () => {
        const currentPath = renderAt('/settings/ai');

        expect(currentPath()).toBe('/settings/ai/mcp-servers');
        expect(screen.getByTestId('mcp-servers-tab')).toBeInTheDocument();
        expect(screen.getByText('Connect AI tools to DataHub through the Model Context Protocol.')).toBeInTheDocument();
    });

    it('sends a user with no AI settings tab back to Settings', () => {
        mockCanManage.mockReturnValue(false);
        const currentPath = renderAt('/settings/ai');

        expect(currentPath()).toBe('/settings');
        expect(screen.queryByTestId('mcp-servers-tab')).not.toBeInTheDocument();
    });
});

describe('PlatformAiSettings.types', () => {
    it('matches a tab and its sub-paths, and nothing else', () => {
        const keys = ['mcp-servers', 'plugins'];

        expect(getAiTabFromPath('/settings/ai/mcp-servers', keys)).toBe('mcp-servers');
        expect(getAiTabFromPath('/settings/ai/plugins/new', keys)).toBe('plugins');
        expect(getAiTabFromPath('/settings/ai/mcp-servers-old', keys)).toBeUndefined();
        expect(aiTabUrl('mcp-servers')).toBe('/settings/ai/mcp-servers');
        expect(isAiSettingsBasePath('/settings/ai/')).toBe(true);
    });
});
