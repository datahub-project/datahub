import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useRoleManagement } from '@app/identity/user/useRoleManagement';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

// Mock the GraphQL hook
vi.mock('@graphql/role.generated', () => ({
    useListRolesQuery: vi.fn(),
}));

const mockRoles: DataHubRole[] = [
    {
        urn: 'urn:li:role:Admin',
        name: 'Admin',
    },
    {
        urn: 'urn:li:role:Reader',
        name: 'Reader',
    },
    {
        urn: 'urn:li:role:Editor',
        name: 'Editor',
    },
] as DataHubRole[];

const mockUseListRolesQuery = vi.mocked(useListRolesQuery);

describe('useRoleManagement', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('initializes with empty state when no roles loaded', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.selectedRole).toBeUndefined();
        expect(result.current.emailInviteRole).toBeUndefined();
        expect(result.current.roles).toEqual([]);
        expect(result.current.roleSelectOptions).toEqual([{ value: '', label: 'No Role' }]);
        expect(result.current.noRoleText).toBe('No Role');
    });

    it('loads roles from GraphQL query', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.roles).toEqual(mockRoles);
    });

    it('builds role select options correctly', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.roleSelectOptions).toEqual([
            { value: 'urn:li:role:Admin', label: 'Admin' },
            { value: 'urn:li:role:Reader', label: 'Reader' },
            { value: 'urn:li:role:Editor', label: 'Editor' },
            { value: '', label: 'No Role' },
        ]);
    });

    it('handles role selection', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('urn:li:role:Admin');
        });

        expect(result.current.selectedRole).toEqual(mockRoles[0]);
    });

    it('handles role selection with empty string (No Role)', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('');
        });

        expect(result.current.selectedRole).toBeUndefined();
    });

    it('handles email invite role selection', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectEmailInviteRole('urn:li:role:Editor');
        });

        expect(result.current.emailInviteRole).toEqual(mockRoles[2]);
    });

    it('handles invalid role URN selection gracefully', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('invalid-urn');
        });

        expect(result.current.selectedRole).toBeUndefined();
    });

    it('resets roles correctly', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles } },
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        // First set some roles manually
        act(() => {
            result.current.onSelectRole('urn:li:role:Admin');
            result.current.onSelectEmailInviteRole('urn:li:role:Admin');
        });

        expect(result.current.selectedRole).toEqual(mockRoles[0]);
        expect(result.current.emailInviteRole).toEqual(mockRoles[0]);

        // Then reset - this will trigger re-initialization due to useEffect
        act(() => {
            result.current.resetRoles();
        });

        // After reset, roles should be re-initialized to default (Reader role)
        // This is expected behavior since resetRoles sets rolesInitialized to false
        expect(result.current.selectedRole?.name).toBe('Reader');
        expect(result.current.emailInviteRole?.name).toBe('Reader');
    });

    it('provides consistent noRoleText', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: undefined,
            loading: false,
            error: undefined,
        } as any);

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.noRoleText).toBe('No Role');
    });
});
