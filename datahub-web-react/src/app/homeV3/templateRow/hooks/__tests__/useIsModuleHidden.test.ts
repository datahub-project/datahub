import { beforeEach, describe, expect, it, vi } from 'vitest';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useIsMcpEnabled } from '@app/homeV3/modules/mcp/useIsMcpEnabled';
import { useIsModuleHidden } from '@app/homeV3/templateRow/hooks/useIsModuleHidden';
import { useIsMcpServerEnabled } from '@app/useAppConfig';
import { renderHook } from '@utils/test-utils/renderHook';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, PageTemplateSurfaceType } from '@types';

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: vi.fn(),
}));

vi.mock('@app/homeV3/modules/mcp/useIsMcpEnabled', () => ({
    useIsMcpEnabled: vi.fn(),
}));

vi.mock('@app/useAppConfig', () => ({
    useIsMcpServerEnabled: vi.fn(),
}));

const moduleOfType = (type: DataHubPageModuleType) => ({ properties: { type } }) as PageModuleFragment;

function setState({
    isMcpEnabled,
    isEditingGlobalTemplate,
    mcpServerEnabled = true,
}: {
    isMcpEnabled: boolean;
    isEditingGlobalTemplate: boolean;
    mcpServerEnabled?: boolean;
}) {
    vi.mocked(useIsMcpServerEnabled).mockReturnValue(mcpServerEnabled);
    vi.mocked(usePageTemplateContext).mockReturnValue({
        templateType: PageTemplateSurfaceType.HomePage,
        isEditingGlobalTemplate,
    } as ReturnType<typeof usePageTemplateContext>);
    vi.mocked(useIsMcpEnabled).mockReturnValue({ isMcpEnabled, loading: false });
}

describe('useIsModuleHidden', () => {
    beforeEach(() => vi.clearAllMocks());

    it('hides the MCP module while MCP is turned off', () => {
        setState({ isMcpEnabled: false, isEditingGlobalTemplate: false });
        const { result } = renderHook(() => useIsModuleHidden());

        expect(result.current(moduleOfType(DataHubPageModuleType.Mcp))).toBe(true);
        expect(result.current(moduleOfType(DataHubPageModuleType.Platforms))).toBe(false);
    });

    it('shows the MCP module to admins editing the org default, so they can remove it', () => {
        setState({ isMcpEnabled: false, isEditingGlobalTemplate: true });
        const { result } = renderHook(() => useIsModuleHidden());

        expect(result.current(moduleOfType(DataHubPageModuleType.Mcp))).toBe(false);
    });

    it('hides the MCP module from everyone while the MCP server feature is off', () => {
        setState({ isMcpEnabled: false, isEditingGlobalTemplate: true, mcpServerEnabled: false });
        const { result } = renderHook(() => useIsModuleHidden());

        expect(result.current(moduleOfType(DataHubPageModuleType.Mcp))).toBe(true);
    });

    it('shows the MCP module while MCP is on', () => {
        setState({ isMcpEnabled: true, isEditingGlobalTemplate: false });
        const { result } = renderHook(() => useIsModuleHidden());

        expect(result.current(moduleOfType(DataHubPageModuleType.Mcp))).toBe(false);
    });
});
