import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { McpServersTab } from '@app/settingsV2/platform/ai/mcpServers/McpServersTab';
import CustomThemeProvider from '@src/CustomThemeProvider';

const mockToggleGlobalEnabled = vi.fn();
vi.mock('@app/settingsV2/platform/ai/mcpServers/hooks/useMcpServerActions', async (importOriginal) => ({
    ...(await importOriginal<object>()),
    useMcpServerActions: () => ({
        handleToggleEnabled: vi.fn(),
        handleToggleGlobalEnabled: mockToggleGlobalEnabled,
    }),
}));

const mockUseGetMcpServersQuery = vi.fn();
vi.mock('@graphql/mcpServers.generated', () => ({
    GetMcpServersDocument: 'GetMcpServersDocument',
    useGetMcpServersQuery: (...args: unknown[]) => mockUseGetMcpServersQuery(...args),
    useUpdateMcpServersMutation: () => [vi.fn()],
}));

// One catalog object for every render: the tool hooks memoize on it.
const CATALOG_STATE = {
    catalog: { tools: [{ toolId: 'search', description: 'Search', groups: ['read_metadata'] }], agents: [] },
    loading: false,
    failed: false,
    retry: () => {},
};
vi.mock('@app/context/agents/useMcpToolCatalog', () => ({ useMcpToolCatalog: () => CATALOG_STATE }));

// Lets each test turn scoped servers (createMcpServersEnabled) on or off.
const mockFlags = { createMcpServersEnabled: false };
vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<object>()),
    useAppConfig: () => ({ config: { featureFlags: mockFlags } }),
}));

vi.mock('@app/context/agents/ViewSelector', () => ({ ViewSelector: () => <div data-testid="view-selector" /> }));

const mockRefetchQueries = vi.fn();
vi.mock('@apollo/client', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@apollo/client')>()),
    useApolloClient: () => ({ refetchQueries: mockRefetchQueries }),
}));

const auditStamp = { time: 0 };
const SCOPED_SERVER = {
    slug: 'finance',
    name: 'Finance',
    enabled: true,
    tools: [{ refType: 'TOOL', toolId: 'search', enabled: true }],
    viewUrn: 'urn:li:dataHubView:deleted',
    view: null,
    created: auditStamp,
    lastModified: auditStamp,
};

function settings(enabled: boolean, servers: unknown[] = []) {
    return { data: { mcpSettings: { enabled, servers, defaultMcpServerOverrides: null } }, loading: false };
}

function renderTab() {
    return render(
        <MemoryRouter>
            <CustomThemeProvider>
                <McpServersTab showCreateModal={false} setShowCreateModal={vi.fn()} />
            </CustomThemeProvider>
        </MemoryRouter>,
    );
}

describe('McpServersTab', () => {
    beforeEach(() => {
        mockToggleGlobalEnabled.mockClear();
        mockFlags.createMcpServersEnabled = false;
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

    it('hides scoped-server parts in OSS: no Scope column or View alert, and editing has no slug or scope', () => {
        mockUseGetMcpServersQuery.mockReturnValue({ ...settings(true, [SCOPED_SERVER]), refetch: vi.fn() });
        renderTab();

        expect(screen.getByText('Tools')).toBeInTheDocument();
        expect(screen.getByText('All')).toBeInTheDocument();
        expect(screen.getByText('Search')).toBeInTheDocument();
        expect(screen.queryByText('Scope')).not.toBeInTheDocument();
        expect(screen.queryByTestId('mcp-servers-view-deleted')).not.toBeInTheDocument();

        fireEvent.click(screen.getByTestId('mcp-server-name-__default__'));
        expect(screen.getByText('Edit MCP Server')).toBeInTheDocument();
        expect(screen.queryByText('Slug')).not.toBeInTheDocument();
        expect(screen.queryByTestId('view-selector')).not.toBeInTheDocument();
    });

    it('shows the Scope column and flags a deleted View when scoped servers are on', () => {
        mockFlags.createMcpServersEnabled = true;
        mockUseGetMcpServersQuery.mockReturnValue({ ...settings(true, [SCOPED_SERVER]), refetch: vi.fn() });
        renderTab();

        expect(screen.getByText('Scope')).toBeInTheDocument();
        expect(screen.getByText('View unavailable')).toBeInTheDocument();
        expect(screen.getByTestId('mcp-servers-view-deleted')).toBeInTheDocument();
    });
});
