import { vi } from 'vitest';

import { useCanManageMcpServers } from '@app/settingsV2/platform/ai/mcpServers/hooks/useCanManageMcpServers';
import { renderHook } from '@utils/test-utils/renderHook';

const mocks = vi.hoisted(() => ({ mcpServerEnabled: true, manageGlobalSettings: true }));

vi.mock('@app/useAppConfig', () => ({
    useIsMcpServerEnabled: () => mocks.mcpServerEnabled,
}));
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ platformPrivileges: { manageGlobalSettings: mocks.manageGlobalSettings } }),
}));

describe('useCanManageMcpServers', () => {
    it.each([
        [true, true, true],
        [true, false, false],
        [false, true, false],
    ])('flag %s and manageGlobalSettings %s gives %s', (flag, privilege, expected) => {
        mocks.mcpServerEnabled = flag;
        mocks.manageGlobalSettings = privilege;

        const { result } = renderHook(() => useCanManageMcpServers());

        expect(result.current).toBe(expected);
    });
});
