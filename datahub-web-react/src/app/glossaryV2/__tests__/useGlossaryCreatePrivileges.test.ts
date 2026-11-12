import { vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import useGlossaryCreatePrivileges from '@app/glossaryV2/useGlossaryCreatePrivileges';
import { renderHook } from '@utils/test-utils/renderHook';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

function mockPlatformPrivileges(privileges: { manageGlossaries?: boolean }) {
    vi.mocked(useUserContext).mockReturnValue({ platformPrivileges: privileges } as any);
}

describe('useGlossaryCreatePrivileges', () => {
    it('uses Manage Glossaries at the glossary root', () => {
        mockPlatformPrivileges({ manageGlossaries: true });
        expect(renderHook(() => useGlossaryCreatePrivileges()).result.current.canCreate).toBe(true);

        mockPlatformPrivileges({});
        expect(renderHook(() => useGlossaryCreatePrivileges()).result.current.canCreate).toBe(false);
    });

    it('follows the parent group privilege inside a group', () => {
        mockPlatformPrivileges({});
        expect(renderHook(() => useGlossaryCreatePrivileges(true)).result.current.canCreate).toBe(true);

        mockPlatformPrivileges({ manageGlossaries: true });
        expect(renderHook(() => useGlossaryCreatePrivileges(false)).result.current.canCreate).toBe(false);
    });
});
