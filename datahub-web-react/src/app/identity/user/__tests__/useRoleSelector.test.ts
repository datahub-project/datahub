import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useRoleSelector } from '@app/identity/user/useRoleSelector';

import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole } from '@types';

vi.mock('@graphql/role.generated', () => ({
    useListRolesQuery: vi.fn(),
}));

const createMockRoles = (count: number, startIndex = 0): DataHubRole[] => {
    return Array.from({ length: count }, (_, i) => ({
        urn: `urn:li:role:Role${startIndex + i}`,
        name: `Role ${startIndex + i}`,
    })) as DataHubRole[];
};

const mockUseListRolesQuery = vi.mocked(useListRolesQuery);

describe('useRoleSelector', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('returns empty roles when query has no data', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        const { result } = renderHook(() => useRoleSelector());

        expect(result.current.roles).toEqual([]);
        expect(result.current.loading).toBe(false);
        expect(result.current.hasMore).toBe(false);
    });

    it('loads initial batch of roles', () => {
        const mockRoles = createMockRoles(20);
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles, total: 50 } },
            loading: false,
        } as any);

        const { result } = renderHook(() => useRoleSelector());

        expect(result.current.roles).toEqual(mockRoles);
        expect(result.current.hasMore).toBe(true);
        expect(result.current.total).toBe(50);
    });

    it('sets hasMore to false when all roles are loaded', () => {
        const mockRoles = createMockRoles(10);
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles, total: 10 } },
            loading: false,
        } as any);

        const { result } = renderHook(() => useRoleSelector());

        expect(result.current.roles).toEqual(mockRoles);
        expect(result.current.hasMore).toBe(false);
    });

    it('exposes setSearchQuery function to update search query', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: [], total: 0 } },
            loading: false,
        } as any);

        const { result } = renderHook(() => useRoleSelector());

        expect(result.current.searchQuery).toBe('');

        act(() => {
            result.current.setSearchQuery('admin');
        });

        expect(result.current.searchQuery).toBe('admin');
    });

    it('exposes loadMore function for manual pagination', () => {
        const mockRoles = createMockRoles(20);
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles, total: 50 } },
            loading: false,
        } as any);

        const { result, rerender } = renderHook(() => useRoleSelector());

        expect(result.current.roles.length).toBe(20);

        act(() => {
            result.current.loadMore();
        });
        rerender();

        expect(mockUseListRolesQuery).toHaveBeenLastCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        start: 20,
                        count: 20,
                    }),
                }),
            }),
        );
    });

    it('does not advance start when hasMore is false', () => {
        const mockRoles = createMockRoles(10);
        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: mockRoles, total: 10 } },
            loading: false,
        } as any);

        const { result, rerender } = renderHook(() => useRoleSelector());

        expect(result.current.hasMore).toBe(false);

        act(() => {
            result.current.loadMore();
        });
        rerender();

        expect(mockUseListRolesQuery).toHaveBeenLastCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        start: 0,
                    }),
                }),
            }),
        );
    });

    it('provides observerRef callback for IntersectionObserver', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        const { result } = renderHook(() => useRoleSelector());

        expect(result.current.observerRef).toBeDefined();
        expect(typeof result.current.observerRef).toBe('function');
    });

    it('uses cache-and-network fetch policy', () => {
        mockUseListRolesQuery.mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        renderHook(() => useRoleSelector());

        expect(mockUseListRolesQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                fetchPolicy: 'cache-and-network',
            }),
        );
    });

    it('accumulates roles when loading more pages', () => {
        const firstPageRoles = createMockRoles(20, 0);
        const secondPageRoles = createMockRoles(20, 20);

        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: firstPageRoles, total: 50 } },
            loading: false,
        } as any);

        const { result, rerender } = renderHook(() => useRoleSelector());

        expect(result.current.roles).toHaveLength(20);
        expect(result.current.roles[0].name).toBe('Role 0');
        expect(result.current.hasMore).toBe(true);

        act(() => {
            result.current.loadMore();
        });

        mockUseListRolesQuery.mockReturnValue({
            data: { listRoles: { roles: secondPageRoles, total: 50 } },
            loading: false,
        } as any);

        rerender();

        expect(result.current.roles).toHaveLength(40);
        expect(result.current.roles[0].name).toBe('Role 0');
        expect(result.current.roles[20].name).toBe('Role 20');
        expect(result.current.hasMore).toBe(true);
    });
});
