import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, within } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import McpModule from '@app/homeV3/modules/mcp/McpModule';
import { MCP_SETUP_DOCS_URL } from '@app/homeV3/modules/mcp/mcpModule.utils';
import { useCanManageMcpServers } from '@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { useGetMcpEnabledQuery } from '@graphql/mcpServers.generated';

const mockPush = vi.fn();

vi.mock('react-router', async (importOriginal) => {
    const actual = await importOriginal<typeof import('react-router')>();
    return { ...actual, useHistory: () => ({ push: mockPush }) };
});

vi.mock('@graphql/mcpServers.generated', () => ({
    useGetMcpEnabledQuery: vi.fn(),
}));

vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/useAppConfig')>()),
    useIsMcpServerEnabled: () => true,
}));
vi.mock('@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers', () => ({
    useCanManageMcpServers: vi.fn(),
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({
        children,
        onClickViewAll,
        viewAllText,
    }: React.PropsWithChildren<{ onClickViewAll?: () => void; viewAllText?: string }>) => (
        <div data-testid="large-module">
            {children}
            {onClickViewAll && (
                <button type="button" onClick={onClickViewAll}>
                    {viewAllText}
                </button>
            )}
        </div>
    ),
}));

function mockMcpEnabled(enabled: boolean | undefined) {
    vi.mocked(useGetMcpEnabledQuery).mockReturnValue({
        data: enabled === undefined ? undefined : { mcpSettings: { enabled } },
        loading: false,
    } as ReturnType<typeof useGetMcpEnabledQuery>);
}

function renderModule() {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <McpModule {...({} as React.ComponentProps<typeof McpModule>)} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('McpModule', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockMcpEnabled(true);
        vi.mocked(useCanManageMcpServers).mockReturnValue(false);
    });

    it('shows a removable placeholder instead of setup when MCP is turned off', () => {
        mockMcpEnabled(false);
        vi.mocked(useCanManageMcpServers).mockReturnValue(true);
        renderModule();

        expect(screen.getByText('MCP is turned off')).toBeInTheDocument();
        expect(screen.queryByTestId('mcp-client-claude')).not.toBeInTheDocument();
        fireEvent.click(screen.getByText('Open MCP settings'));
        expect(mockPush).toHaveBeenCalledWith('/settings/ai/mcp-servers');
    });

    it('offers Claude, ChatGPT, Cursor and Other, in that order and with their logos, while the MCP setting is unknown', () => {
        mockMcpEnabled(undefined);
        renderModule();

        const buttons = screen.getAllByTestId(/^mcp-client-(?!logo-)/);
        expect(buttons.map((button) => button.textContent)).toEqual(['Claude', 'ChatGPT', 'Cursor', 'Other']);
        ['claude', 'chatgpt', 'cursor', 'other'].forEach((key, index) =>
            expect(within(buttons[index]).getByTestId(`mcp-client-logo-${key}`)).toBeInTheDocument(),
        );
        expect(screen.getByTestId('mcp-client-logo-claude')).toHaveAttribute('src', expect.stringMatching(/svg/));
    });

    it('opens the ChatGPT setup from its button', () => {
        renderModule();
        fireEvent.click(screen.getByTestId('mcp-client-chatgpt'));

        expect(screen.getByText('Connect ChatGPT to DataHub')).toBeInTheDocument();
        // Tests run at a localhost origin, which ChatGPT can't reach from OpenAI's servers.
        expect(screen.getByTestId('mcp-connect-chatgpt-private')).toBeInTheDocument();
    });

    it("opens the client's setup, the same the settings page shows, from its button", () => {
        renderModule();
        fireEvent.click(screen.getByTestId('mcp-client-cursor'));

        expect(screen.getByText('Connect Cursor to DataHub')).toBeInTheDocument();
        expect(screen.getByTestId('mcp-connect-snippet')).toHaveTextContent(`${window.location.origin}/mcp`);
        expect(screen.getByText('Add to ~/.cursor/mcp.json')).toBeInTheDocument();
    });

    it('opens the local Claude Desktop setup for Claude when Anthropic cannot reach DataHub', () => {
        // Tests run at a localhost origin, which Claude's cloud connectors can't reach.
        renderModule();
        fireEvent.click(screen.getByTestId('mcp-client-claude'));

        expect(screen.getByText('Connect Claude to DataHub')).toBeInTheDocument();
        expect(screen.getByTestId('mcp-connect-snippet')).toHaveTextContent('mcp-remote');
    });

    it('links admins who manage the MCP server to its settings', () => {
        vi.mocked(useCanManageMcpServers).mockReturnValue(true);
        renderModule();
        fireEvent.click(screen.getByText('Manage MCP server'));

        expect(mockPush).toHaveBeenCalledWith('/settings/ai/mcp-servers');
    });

    it('links everyone else to the setup guide', () => {
        const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
        renderModule();
        fireEvent.click(screen.getByText('View setup guide'));

        expect(openSpy).toHaveBeenCalledWith(MCP_SETUP_DOCS_URL, '_blank', 'noopener,noreferrer');
        expect(mockPush).not.toHaveBeenCalled();
        openSpy.mockRestore();
    });
});
