import { MockedProvider } from '@apollo/client/testing';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    useServiceAccountListData,
    useServiceAccountListState,
    useServiceAccountRoleAssignment,
} from '@app/identity/serviceAccount/ServiceAccountList.hooks';

import { ListServiceAccountsDocument } from '@graphql/auth.generated';
import { BatchAssignRoleDocument } from '@graphql/mutations.generated';
import { ListRolesDocument } from '@graphql/role.generated';
import { DataHubRole, EntityType } from '@types';

// Mock the user context
const mockUserContext = {
    platformPrivileges: {
        manageServiceAccounts: true,
    },
};

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

// Mock scrollToTop
vi.mock('@app/shared/searchUtils', () => ({
    scrollToTop: vi.fn(),
}));

// Mock message
vi.mock('antd', async () => {
    const actual = await vi.importActual('antd');
    return {
        ...actual,
        message: {
            success: vi.fn(),
            error: vi.fn(),
        },
    };
});

const mockServiceAccounts = [
    {
        __typename: 'ServiceAccount',
        urn: 'urn:li:serviceAccount:test-account',
        type: EntityType.CorpUser,
        name: 'test-account',
        displayName: 'Test Account',
        description: 'Test description',
        createdBy: 'urn:li:corpuser:datahub',
        createdAt: Date.now(),
        updatedAt: null,
        roles: {
            __typename: 'EntityRelationshipsResult',
            start: 0,
            count: 0,
            total: 0,
            relationships: [],
        },
    },
];

const mockRoles: DataHubRole[] = [
    {
        __typename: 'DataHubRole',
        urn: 'urn:li:dataHubRole:Admin',
        type: EntityType.DatahubRole,
        name: 'Admin',
        description: 'Admin role',
    },
    {
        __typename: 'DataHubRole',
        urn: 'urn:li:dataHubRole:Editor',
        type: EntityType.DatahubRole,
        name: 'Editor',
        description: 'Editor role',
    },
];

describe('useServiceAccountListState', () => {
    it('should initialize with default values', () => {
        const { result } = renderHook(() => useServiceAccountListState());

        expect(result.current.query).toBe('');
        expect(result.current.debouncedQuery).toBe('');
        expect(result.current.page).toBe(1);
        expect(result.current.pageSize).toBe(10);
        expect(result.current.isCreatingServiceAccount).toBe(false);
        expect(result.current.roleAssignmentState).toBeNull();
        expect(result.current.optimisticRoles).toEqual({});
    });

    it('should update query when setQuery is called', () => {
        const { result } = renderHook(() => useServiceAccountListState());

        act(() => {
            result.current.setQuery('test');
        });

        expect(result.current.query).toBe('test');
    });

    it('should update page when setPage is called', () => {
        const { result } = renderHook(() => useServiceAccountListState());

        act(() => {
            result.current.setPage(2);
        });

        expect(result.current.page).toBe(2);
    });

    it('should toggle isCreatingServiceAccount', () => {
        const { result } = renderHook(() => useServiceAccountListState());

        act(() => {
            result.current.setIsCreatingServiceAccount(true);
        });

        expect(result.current.isCreatingServiceAccount).toBe(true);

        act(() => {
            result.current.setIsCreatingServiceAccount(false);
        });

        expect(result.current.isCreatingServiceAccount).toBe(false);
    });

    it('should update optimisticRoles', () => {
        const { result } = renderHook(() => useServiceAccountListState());

        act(() => {
            result.current.setOptimisticRoles({
                'urn:li:serviceAccount:test': 'urn:li:dataHubRole:Admin',
            });
        });

        expect(result.current.optimisticRoles).toEqual({
            'urn:li:serviceAccount:test': 'urn:li:dataHubRole:Admin',
        });
    });

    it('should debounce query and reset page to 1 when query changes', async () => {
        const { result, waitForNextUpdate } = renderHook(() => useServiceAccountListState());

        // Set page to 2 first
        act(() => {
            result.current.setPage(2);
        });
        expect(result.current.page).toBe(2);

        // Update query (requires at least 3 characters to trigger debounce)
        act(() => {
            result.current.setQuery('test');
        });

        // Wait for debounce (300ms) and page reset
        await waitForNextUpdate({ timeout: 500 });

        expect(result.current.debouncedQuery).toBe('test');
        expect(result.current.page).toBe(1);
    });
});

describe('useServiceAccountListData', () => {
    const mocks = [
        {
            request: {
                query: ListServiceAccountsDocument,
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: undefined,
                    },
                },
            },
            result: {
                data: {
                    listServiceAccounts: {
                        __typename: 'ListServiceAccountsResult',
                        start: 0,
                        count: 10,
                        total: 1,
                        serviceAccounts: mockServiceAccounts,
                    },
                },
            },
        },
        {
            request: {
                query: ListRolesDocument,
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                    },
                },
            },
            result: {
                data: {
                    listRoles: {
                        __typename: 'ListRolesResult',
                        start: 0,
                        count: 10,
                        total: 2,
                        roles: mockRoles,
                    },
                },
            },
        },
    ];

    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={mocks} addTypename={false}>
            {children}
        </MockedProvider>
    );

    beforeEach(() => {
        mockUserContext.platformPrivileges.manageServiceAccounts = true;
    });

    it('should return loading state initially', () => {
        const { result } = renderHook(() => useServiceAccountListData(1, 10, ''), { wrapper });

        expect(result.current.loading).toBe(true);
    });

    it('should return canManageServiceAccounts from user context', () => {
        const { result } = renderHook(() => useServiceAccountListData(1, 10, ''), { wrapper });

        expect(result.current.canManageServiceAccounts).toBe(true);
    });

    it('should return false for canManageServiceAccounts when user lacks privilege', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = false;

        const { result } = renderHook(() => useServiceAccountListData(1, 10, ''), { wrapper });

        expect(result.current.canManageServiceAccounts).toBe(false);
    });

    it('should load service accounts and roles', async () => {
        const { result, waitForNextUpdate } = renderHook(() => useServiceAccountListData(1, 10, ''), { wrapper });

        await waitForNextUpdate({ timeout: 2000 });

        expect(result.current.loading).toBe(false);
        expect(result.current.serviceAccounts).toHaveLength(1);
        expect(result.current.serviceAccounts[0].name).toBe('test-account');
        expect(result.current.selectRoleOptions).toHaveLength(2);
        expect(result.current.totalServiceAccounts).toBe(1);
    });

    it('should provide onChangePage function', () => {
        const { result } = renderHook(() => useServiceAccountListData(1, 10, ''), { wrapper });

        const newPage = result.current.onChangePage(3);
        expect(newPage).toBe(3);
    });
});

describe('useServiceAccountRoleAssignment', () => {
    const mockSetRoleAssignmentState = vi.fn();
    const mockSetOptimisticRoles = vi.fn();
    const mockRefetch = vi.fn();

    const mocks = [
        {
            request: {
                query: BatchAssignRoleDocument,
                variables: {
                    input: {
                        roleUrn: 'urn:li:dataHubRole:Admin',
                        actors: ['urn:li:serviceAccount:test'],
                    },
                },
            },
            result: {
                data: {
                    batchAssignRole: true,
                },
            },
        },
    ];

    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={mocks} addTypename={false}>
            {children}
        </MockedProvider>
    );

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should call setRoleAssignmentState when onSelectRole is called', () => {
        const { result } = renderHook(
            () =>
                useServiceAccountRoleAssignment(
                    mockRoles,
                    null,
                    mockSetRoleAssignmentState,
                    mockSetOptimisticRoles,
                    mockRefetch,
                ),
            { wrapper },
        );

        act(() => {
            result.current.onSelectRole(
                'urn:li:serviceAccount:test',
                'Test Account',
                'urn:li:dataHubRole:NoRole',
                'urn:li:dataHubRole:Admin',
            );
        });

        expect(mockSetRoleAssignmentState).toHaveBeenCalledWith({
            isViewingAssignRole: true,
            serviceAccountUrn: 'urn:li:serviceAccount:test',
            serviceAccountName: 'Test Account',
            currentRoleUrn: 'urn:li:dataHubRole:Admin',
            originalRoleUrn: 'urn:li:dataHubRole:NoRole',
        });
    });

    it('should call setRoleAssignmentState(null) when onCancelRoleAssignment is called', () => {
        const roleAssignmentState = {
            isViewingAssignRole: true,
            serviceAccountUrn: 'urn:li:serviceAccount:test',
            serviceAccountName: 'Test Account',
            currentRoleUrn: 'urn:li:dataHubRole:Admin',
            originalRoleUrn: 'urn:li:dataHubRole:NoRole',
        };

        const { result } = renderHook(
            () =>
                useServiceAccountRoleAssignment(
                    mockRoles,
                    roleAssignmentState,
                    mockSetRoleAssignmentState,
                    mockSetOptimisticRoles,
                    mockRefetch,
                ),
            { wrapper },
        );

        act(() => {
            result.current.onCancelRoleAssignment();
        });

        expect(mockSetRoleAssignmentState).toHaveBeenCalledWith(null);
    });

    it('should not call mutation when roleAssignmentState is null', () => {
        const { result } = renderHook(
            () =>
                useServiceAccountRoleAssignment(
                    mockRoles,
                    null,
                    mockSetRoleAssignmentState,
                    mockSetOptimisticRoles,
                    mockRefetch,
                ),
            { wrapper },
        );

        act(() => {
            result.current.onConfirmRoleAssignment();
        });

        // Should not throw and not call any setters
        expect(mockSetOptimisticRoles).not.toHaveBeenCalled();
    });
});
