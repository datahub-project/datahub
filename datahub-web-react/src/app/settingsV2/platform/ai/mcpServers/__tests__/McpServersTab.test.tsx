import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { McpServersTab } from '@app/settingsV2/platform/ai/mcpServers/McpServersTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockToggleGlobalEnabled = vi.fn();
vi.mock('@app/settingsV2/platform/ai/mcpServers/hooks/useMcpServerActions', () => ({
    useMcpServerActions: () => ({
        handleToggleEnabled: vi.fn(),
        handleToggleGlobalEnabled: mockToggleGlobalEnabled,
    }),
}));

const mockUseGetMcpServersQuery = vi.fn();
vi.mock('@graphql/mcpServers.generated', () => ({
    GetMcpServersDocument: 'GetMcpServersDocument',
    useGetMcpServersQuery: (...args: unknown[]) => mockUseGetMcpServersQuery(...args),
}));

const mockRefetchQueries = vi.fn();
vi.mock('@apollo/client', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@apollo/client')>()),
    useApolloClient: () => ({ refetchQueries: mockRefetchQueries }),
}));

function settings(enabled: boolean) {
    return { data: { mcpSettings: { enabled, servers: [], defaultMcpServerOverrides: null } }, loading: false };
}

function renderTab() {
    return render(
        <MemoryRouter>
            <CustomThemeProvider>
                <McpServersTab />
            </CustomThemeProvider>
        </MemoryRouter>,
    );
}

describe('McpServersTab', () => {
    beforeEach(() => {
        mockToggleGlobalEnabled.mockClear();
        mockUseGetMcpServersQuery.mockReturnValue({ ...settings(true), refetch: vi.fn() });
    });

    it('lists the default server with its connection URL', () => {
        renderTab();

        expect(screen.getByTestId('mcp-server-name-__default__')).toHaveTextContent('Default');
        expect(screen.getByText(`${window.location.origin}/mcp`)).toBeInTheDocument();
    });

    it('asks for confirmation before turning the master switch off', () => {
        renderTab();

        fireEvent.click(screen.getByTestId('mcp-master-switch'));
        expect(mockToggleGlobalEnabled).not.toHaveBeenCalled();

        fireEvent.click(screen.getByTestId('modal-confirm-button'));
        expect(mockToggleGlobalEnabled).toHaveBeenCalledWith(false);
    });

    it('turns the master switch on without confirmation and hides the table while off', () => {
        mockUseGetMcpServersQuery.mockReturnValue({ ...settings(false), refetch: vi.fn() });
        renderTab();

        expect(screen.queryByTestId('mcp-servers-table')).not.toBeInTheDocument();
        fireEvent.click(screen.getByTestId('mcp-master-switch'));

        expect(mockToggleGlobalEnabled).toHaveBeenCalledWith(true);
        expect(screen.queryByTestId('modal-confirm-button')).not.toBeInTheDocument();
    });

    it('shows an error with a retry instead of default state when the settings fail to load', () => {
        mockUseGetMcpServersQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: new Error('GetMcpServers failed'),
            refetch: vi.fn(),
        });
        renderTab();

        expect(screen.getByText('Failed to load MCP servers.')).toBeInTheDocument();
        expect(screen.queryByTestId('mcp-master-switch')).not.toBeInTheDocument();

        fireEvent.click(screen.getByText('Retry'));
        expect(mockRefetchQueries).toHaveBeenCalledWith({ include: ['GetMcpServersDocument'] });
    });
});
