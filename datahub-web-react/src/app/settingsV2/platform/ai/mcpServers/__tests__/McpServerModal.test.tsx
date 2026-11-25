import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { McpServerModal } from '@app/settingsV2/platform/ai/mcpServers/McpServerModal';
import { transformServersToRows } from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import CustomThemeProvider from '@src/CustomThemeProvider';

import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';
import { McpInstructionSize, ToolRefType } from '@types';

const mockUpdate = vi.fn().mockResolvedValue({});
vi.mock('@graphql/mcpServers.generated', () => ({ useUpdateMcpServersMutation: () => [mockUpdate] }));

// No catalog: the server's stored selection must still be saved as it is.
const UNAVAILABLE = { catalog: undefined, loading: false, failed: true, retry: () => {} };
vi.mock('@app/context/agents/useMcpToolCatalog', () => ({ useMcpToolCatalog: () => UNAVAILABLE }));

const OVERRIDES: McpServerConfigFieldsFragment = {
    name: 'Company MCP',
    enabled: true,
    instructionSize: McpInstructionSize.Small,
    tools: [{ refType: ToolRefType.Group, toolId: 'read_metadata', enabled: true }],
    viewUrn: 'urn:li:dataHubView:sales',
    view: null,
    created: { time: 0 },
    lastModified: { time: 0 },
};

describe('McpServerModal', () => {
    it('saves the default server with its stored View and tools when scope and catalog are unavailable', async () => {
        const [defaultRow] = transformServersToRows([], OVERRIDES);
        const onClose = vi.fn();
        render(
            <CustomThemeProvider>
                <McpServerModal
                    editing={defaultRow}
                    existingSlugs={[]}
                    scopedServersEnabled={false}
                    onClose={onClose}
                />
            </CustomThemeProvider>,
        );

        expect(screen.getByText('Failed to load tools.')).toBeInTheDocument();
        fireEvent.click(screen.getByTestId('mcp-server-save'));

        await waitFor(() => expect(onClose).toHaveBeenCalled());
        expect(mockUpdate).toHaveBeenCalledWith({
            variables: {
                input: {
                    defaultMcpServerOverrides: expect.objectContaining({
                        slug: undefined,
                        name: 'Company MCP',
                        instructionSize: McpInstructionSize.Small,
                        tools: [{ refType: ToolRefType.Group, toolId: 'read_metadata', enabled: true }],
                        viewUrn: 'urn:li:dataHubView:sales',
                    }),
                },
            },
        });
    });
});
