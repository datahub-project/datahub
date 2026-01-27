import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    useRoleAssignment,
    useUserListActions,
    useUserListData,
    useUserListState,
} from '@app/identity/user/UserListV2.hooks';

const { mockUseSearchUsersQuery, mockUseBatchAssignRoleMutation } = vi.hoisted(() => ({
    mockUseSearchUsersQuery: vi.fn(),
    mockUseBatchAssignRoleMutation: vi.fn(() => [vi.fn()]),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({
        platformPrivileges: { managePolicies: true },
    }),
}));

vi.mock('@graphql/user.generated', () => ({
    useSearchUsersQuery: mockUseSearchUsersQuery,
    useBatchAssignRoleMutation: mockUseBatchAssignRoleMutation,
}));

vi.mock('@app/identity/user/useRoleSelector', () => ({
    useRoleSelector: () => ({
        roles: [
            { urn: 'urn:li:dataHubRole:Admin', name: 'Admin' },
            { urn: 'urn:li:dataHubRole:Editor', name: 'Editor' },
        ],
    }),
}));

vi.mock('@apollo/client', async (importOriginal) => {
    const actual = (await importOriginal()) as any;
    return {
        ...actual,
        useApolloClient: () => ({}),
        useMutation: vi.fn(() => [vi.fn(), { data: undefined, loading: false, error: undefined }]),
    };
});

vi.mock('@app/identity/user/cacheUtils', () => ({
    DEFAULT_USER_LIST_PAGE_SIZE: 25,
    removeUserFromListUsersCache: vi.fn(),
}));

vi.mock('@app/permissions/roles/cacheUtils', () => ({
    clearRoleListCache: vi.fn(),
}));

describe('UserListV2.hooks', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('useUserListState', () => {
        it('should initialize with default state', () => {
            const { result } = renderHook(() => useUserListState());

            expect(result.current.query).toBe('');
            expect(result.current.debouncedQuery).toBe('');
            expect(result.current.statusFilter).toBe('all');
            expect(result.current.page).toBe(1);
            expect(result.current.pageSize).toBe(25);
            expect(result.current.usersList).toEqual([]);
            expect(result.current.isViewingResetToken).toBe(false);
            expect(result.current.resetTokenUser).toBeNull();
            expect(result.current.canManagePolicies).toBe(true);
        });

        it('should update query state', () => {
            const { result } = renderHook(() => useUserListState());

            act(() => {
                result.current.setQuery('test query');
            });

            expect(result.current.query).toBe('test query');
        });

        it('should update pagination state', () => {
            const { result } = renderHook(() => useUserListState());

            act(() => {
                result.current.setPage(2);
                result.current.setPageSize(50);
            });

            expect(result.current.page).toBe(2);
            expect(result.current.pageSize).toBe(50);
        });

        it('should update status filter', () => {
            const { result } = renderHook(() => useUserListState());

            act(() => {
                result.current.setStatusFilter('active');
            });

            expect(result.current.statusFilter).toBe('active');
        });
    });

    describe('useUserListData', () => {
        beforeEach(() => {
            mockUseSearchUsersQuery.mockReturnValue({
                data: {
                    searchUsers: {
                        searchResults: [
                            {
                                entity: {
                                    __typename: 'CorpUser',
                                    urn: 'urn:li:corpuser:user1',
                                    username: 'user1',
                                },
                            },
                        ],
                        total: 1,
                    },
                },
                loading: false,
                error: null,
                refetch: vi.fn(),
            });
        });

        it('should fetch user data with correct variables', () => {
            renderHook(() => useUserListData('test', 1, 25, 'all'));

            expect(mockUseSearchUsersQuery).toHaveBeenCalledWith({
                variables: {
                    input: {
                        start: 0,
                        count: 25,
                        query: 'test',
                        filters: undefined,
                        sortInput: undefined,
                    },
                },
                fetchPolicy: 'no-cache',
            });
        });

        it('should apply status filters', () => {
            renderHook(() => useUserListData('*', 1, 25, 'active'));

            expect(mockUseSearchUsersQuery).toHaveBeenCalledWith({
                variables: {
                    input: {
                        start: 0,
                        count: 25,
                        query: '*',
                        filters: [{ field: 'status', values: ['ACTIVE'] }],
                        sortInput: undefined,
                    },
                },
                fetchPolicy: 'no-cache',
            });
        });

        it('should return data, loading state, and total users', () => {
            const { result } = renderHook(() => useUserListData('*', 1, 25, 'all'));

            expect(result.current.data).toBeDefined();
            expect(result.current.loading).toBe(false);
            expect(result.current.totalUsers).toBe(1);
            expect(result.current.selectRoleOptions).toHaveLength(2);
        });
    });

    describe('useUserListActions', () => {
        it('should handle reset password action', () => {
            const setIsViewingResetToken = vi.fn();
            const setResetTokenUser = vi.fn();
            const refetch = vi.fn();

            const { result } = renderHook(() =>
                useUserListActions(setIsViewingResetToken, setResetTokenUser, 1, 25, refetch),
            );

            const mockUser = {
                urn: 'urn:li:corpuser:user1',
                username: 'user1',
            } as any;

            act(() => {
                result.current.onResetPassword(mockUser);
            });

            expect(setResetTokenUser).toHaveBeenCalledWith(mockUser);
            expect(setIsViewingResetToken).toHaveBeenCalledWith(true);
        });

        it('should handle close reset modal', () => {
            const setIsViewingResetToken = vi.fn();
            const setResetTokenUser = vi.fn();
            const refetch = vi.fn();

            const { result } = renderHook(() =>
                useUserListActions(setIsViewingResetToken, setResetTokenUser, 1, 25, refetch),
            );

            act(() => {
                result.current.onCloseResetModal();
            });

            expect(setIsViewingResetToken).toHaveBeenCalledWith(false);
            expect(setResetTokenUser).toHaveBeenCalledWith(null);
        });
    });

    describe('useRoleAssignment', () => {
        const mockRoles = [
            { urn: 'urn:li:dataHubRole:Admin', name: 'Admin' },
            { urn: 'urn:li:dataHubRole:Editor', name: 'Editor' },
        ] as any;

        it('should initialize with null role assignment state', () => {
            const mockUsersList = [] as any;
            const setUsersList = vi.fn();
            const refetch = vi.fn();

            const { result } = renderHook(() => useRoleAssignment(mockUsersList, setUsersList, mockRoles, refetch));

            expect(result.current.roleAssignmentState).toBeNull();
        });

        it('should handle role selection with optimistic update', () => {
            const mockUsersList = [{ urn: 'urn:li:corpuser:user1', username: 'user1', roles: null }] as any;
            const setUsersList = vi.fn();
            const refetch = vi.fn();

            const { result } = renderHook(() => useRoleAssignment(mockUsersList, setUsersList, mockRoles, refetch));

            act(() => {
                result.current.onSelectRole('urn:li:corpuser:user1', 'user1', '', 'urn:li:dataHubRole:Admin');
            });

            expect(setUsersList).toHaveBeenCalled();
            expect(result.current.roleAssignmentState).not.toBeNull();
            expect(result.current.roleAssignmentState?.userUrn).toBe('urn:li:corpuser:user1');
            expect(result.current.roleAssignmentState?.currentRoleUrn).toBe('urn:li:dataHubRole:Admin');
        });

        it('should revert optimistic update on cancel', () => {
            const mockUsersList = [{ urn: 'urn:li:corpuser:user1', username: 'user1', roles: null }] as any;
            const setUsersList = vi.fn();
            const refetch = vi.fn();

            const { result } = renderHook(() => useRoleAssignment(mockUsersList, setUsersList, mockRoles, refetch));

            // First, select a role
            act(() => {
                result.current.onSelectRole('urn:li:corpuser:user1', 'user1', '', 'urn:li:dataHubRole:Admin');
            });

            expect(result.current.roleAssignmentState).not.toBeNull();

            // Then cancel
            act(() => {
                result.current.onCancelRoleAssignment();
            });

            expect(result.current.roleAssignmentState).toBeNull();
            // Verify revert was called (second call to setUsersList)
            expect(setUsersList).toHaveBeenCalledTimes(2);
        });
    });
});
