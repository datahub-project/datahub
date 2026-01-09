import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useRoleManagement } from '@app/identity/user/useRoleManagement';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';

import { DataHubRole } from '@types';

// Mock the useRoleSelector hook
vi.mock('@app/identity/user/useRoleSelector', () => ({
    useRoleSelector: vi.fn(),
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

const mockUseRoleSelector = vi.mocked(useRoleSelector);

describe('useRoleManagement', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('initializes with empty state when no roles loaded', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: [],
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: 0,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.selectedRole).toBeUndefined();
        expect(result.current.emailInviteRole).toBeUndefined();
        expect(result.current.roles).toEqual([]);
        expect(result.current.roleSelectOptions).toEqual([{ value: '', label: 'No Role' }]);
        expect(result.current.noRoleText).toBe('No Role');
    });

    it('loads roles from useRoleSelector hook', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.roles).toEqual(mockRoles);
    });

    it('builds role select options correctly', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.roleSelectOptions).toEqual([
            { value: 'urn:li:role:Admin', label: 'Admin' },
            { value: 'urn:li:role:Reader', label: 'Reader' },
            { value: 'urn:li:role:Editor', label: 'Editor' },
            { value: '', label: 'No Role' },
        ]);
    });

    it('handles role selection', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('urn:li:role:Admin');
        });

        expect(result.current.selectedRole).toEqual(mockRoles[0]);
    });

    it('handles role selection with empty string (No Role)', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('');
        });

        expect(result.current.selectedRole).toBeUndefined();
    });

    it('handles email invite role selection', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectEmailInviteRole('urn:li:role:Editor');
        });

        expect(result.current.emailInviteRole).toEqual(mockRoles[2]);
    });

    it('handles invalid role URN selection gracefully', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        act(() => {
            result.current.onSelectRole('invalid-urn');
        });

        expect(result.current.selectedRole).toBeUndefined();
    });

    it('resets roles correctly', () => {
        mockUseRoleSelector.mockReturnValue({
            roles: mockRoles,
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: mockRoles.length,
            loadMore: vi.fn(),
        });

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
        mockUseRoleSelector.mockReturnValue({
            roles: [],
            loading: false,
            hasMore: false,
            observerRef: vi.fn(),
            searchQuery: '',
            setSearchQuery: vi.fn(),
            total: 0,
            loadMore: vi.fn(),
        });

        const { result } = renderHook(() => useRoleManagement());

        expect(result.current.noRoleText).toBe('No Role');
    });
});
