import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useMcpServerActions } from '@app/settingsV2/platform/ai/mcpServers/hooks/useMcpServerActions';
import { renderHook } from '@utils/test-utils/renderHook';

import type { McpServerConfigFieldsFragment } from '@graphql/mcpServers.generated';
import { ToolRefType } from '@types';

const mockUpdate = vi.fn().mockResolvedValue({});
vi.mock('@graphql/mcpServers.generated', () => ({ useUpdateMcpServersMutation: () => [mockUpdate] }));

const DISABLED_DEFAULT: McpServerConfigFieldsFragment = {
    name: 'Company MCP',
    enabled: false,
    tools: [{ refType: ToolRefType.Group, toolId: 'read_metadata', enabled: true }],
    created: { time: 0 },
    lastModified: { time: 0 },
};

describe('useMcpServerActions.handleToggleSingleServer', () => {
    beforeEach(() => mockUpdate.mockClear());

    it('turning on also re-enables a default server switched off on its own, keeping its settings', async () => {
        const { result } = renderHook(() => useMcpServerActions());
        await result.current.handleToggleSingleServer(true, DISABLED_DEFAULT);

        expect(mockUpdate).toHaveBeenCalledWith({
            variables: {
                input: {
                    enabled: true,
                    defaultMcpServerOverrides: expect.objectContaining({
                        name: 'Company MCP',
                        enabled: true,
                        tools: DISABLED_DEFAULT.tools,
                    }),
                },
            },
        });
    });

    it('turning on leaves the default server alone when nothing disabled it', async () => {
        const { result } = renderHook(() => useMcpServerActions());
        await result.current.handleToggleSingleServer(true, null);

        expect(mockUpdate).toHaveBeenCalledWith({ variables: { input: { enabled: true } } });
    });

    it('turning off only flips the master switch', async () => {
        const { result } = renderHook(() => useMcpServerActions());
        await result.current.handleToggleSingleServer(false, { ...DISABLED_DEFAULT, enabled: true });

        expect(mockUpdate).toHaveBeenCalledWith({ variables: { input: { enabled: false } } });
    });
});
