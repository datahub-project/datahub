import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import { useCallback, useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { UserListItem } from '@app/identity/user/UserListV2.components';
import { NO_ROLE_URN, buildFilters } from '@app/identity/user/UserListV2.utils';
import { DEFAULT_USER_LIST_PAGE_SIZE, removeUserFromListUsersCache } from '@app/identity/user/cacheUtils';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { useSearchUsersQuery } from '@graphql/user.generated';
import { DataHubRole } from '@types';

/**
 * Hook for managing user list state (pagination, search, filters)
 */
export function useUserListState() {
    const [query, setQuery] = useState<string>('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [usersList, setUsersList] = useState<UserListItem[]>([]);
    const [isViewingResetToken, setIsViewingResetToken] = useState(false);
    const [resetTokenUser, setResetTokenUser] = useState<UserListItem | null>(null);
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_USER_LIST_PAGE_SIZE);

    const authenticatedUser = useUserContext();
    const canManagePolicies = authenticatedUser?.platformPrivileges?.managePolicies || false;

    // Debounce search query to avoid excessive API calls
    useEffect(() => {
        const timer = setTimeout(() => {
            setDebouncedQuery(query);
        }, 300);
        return () => clearTimeout(timer);
    }, [query]);

    return {
        query,
        setQuery,
        debouncedQuery,
        setDebouncedQuery,
        statusFilter,
        setStatusFilter,
        usersList,
        setUsersList,
        isViewingResetToken,
        setIsViewingResetToken,
        resetTokenUser,
        setResetTokenUser,
        page,
        setPage,
        pageSize,
        setPageSize,
        canManagePolicies,
    };
}

/**
 * Hook for fetching user list data with filters and pagination
 */
export function useUserListData(debouncedQuery: string, page: number, pageSize: number, statusFilter: string) {
    const filters = buildFilters(statusFilter);
    const { roles: selectRoleOptions } = useRoleSelector();

    const { data, loading, error, refetch } = useSearchUsersQuery({
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query: debouncedQuery || '*',
                filters,
                sortInput: undefined,
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalUsers = data?.searchUsers?.total || 0;

    return {
        data,
        loading,
        error,
        totalUsers,
        selectRoleOptions,
        refetch,
    };
}

/**
 * Hook for user list actions (deletion, password reset)
 */
export function useUserListActions(
    setIsViewingResetToken: (viewing: boolean) => void,
    setResetTokenUser: (user: UserListItem | null) => void,
    page: number,
    pageSize: number,
    refetch: () => void,
) {
    const client = useApolloClient();

    const handleDelete = useCallback(
        (urn: string) => {
            removeUserFromListUsersCache(urn, client, page, pageSize);
            refetch();
        },
        [client, page, pageSize, refetch],
    );

    const onResetPassword = useCallback(
        (user: UserListItem) => {
            setResetTokenUser(user);
            setIsViewingResetToken(true);
        },
        [setResetTokenUser, setIsViewingResetToken],
    );

    const onCloseResetModal = useCallback(() => {
        setIsViewingResetToken(false);
        setResetTokenUser(null);
    }, [setIsViewingResetToken, setResetTokenUser]);

    const onDelete = useCallback(
        (urn: string) => {
            handleDelete(urn);
        },
        [handleDelete],
    );

    return {
        onResetPassword,
        onCloseResetModal,
        onDelete,
        handleDelete,
    };
}

/**
 * Hook for role assignment logic with optimistic updates
 */
export function useRoleAssignment(
    usersList: UserListItem[],
    setUsersList: (users: UserListItem[]) => void,
    selectRoleOptions: DataHubRole[],
    refetch: () => void,
) {
    const client = useApolloClient();
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();

    const [roleAssignmentState, setRoleAssignmentState] = useState<{
        isViewingAssignRole: boolean;
        userUrn: string;
        username: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null>(null);

    // Utility function to update user role in list
    const updateUserRole = useCallback(
        (userUrn: string, roleUrn: string) => {
            setUsersList(
                usersList.map((user) => {
                    if (user.urn === userUrn) {
                        const updatedUser = { ...user };
                        if (roleUrn === NO_ROLE_URN) {
                            // Remove role
                            updatedUser.roles = null;
                        } else {
                            // Update role
                            const role = selectRoleOptions.find((r) => r.urn === roleUrn);
                            if (role) {
                                updatedUser.roles = {
                                    ...updatedUser.roles,
                                    relationships: [
                                        {
                                            entity: role,
                                        },
                                    ],
                                };
                            }
                        }
                        return updatedUser;
                    }
                    return user;
                }),
            );
        },
        [usersList, selectRoleOptions, setUsersList],
    );

    // Role assignment handlers
    const onSelectRole = useCallback(
        (userUrn: string, username: string, currentRoleUrn: string, newRoleUrn: string) => {
            // Optimistically update the UI immediately
            updateUserRole(userUrn, newRoleUrn);

            setRoleAssignmentState({
                isViewingAssignRole: true,
                userUrn,
                username,
                currentRoleUrn: newRoleUrn,
                originalRoleUrn: currentRoleUrn,
            });
        },
        [updateUserRole],
    );

    const onCancelRoleAssignment = useCallback(() => {
        if (!roleAssignmentState) return;

        // Revert optimistic update by restoring original role
        updateUserRole(roleAssignmentState.userUrn, roleAssignmentState.originalRoleUrn);
        setRoleAssignmentState(null);
    }, [roleAssignmentState, updateUserRole]);

    const onConfirmRoleAssignment = useCallback(() => {
        if (!roleAssignmentState) return;

        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);

        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: roleToAssign?.urn === NO_ROLE_URN ? null : roleToAssign?.urn,
                    actors: [roleAssignmentState.userUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content:
                            roleToAssign?.urn === NO_ROLE_URN
                                ? `Removed role from user ${roleAssignmentState.username}!`
                                : `Assigned role ${roleToAssign?.name} to user ${roleAssignmentState.username}!`,
                        duration: 2,
                    });
                    setRoleAssignmentState(null);
                    setTimeout(() => {
                        refetch();
                        clearRoleListCache(client);
                    }, 3000);
                }
            })
            .catch((e) => {
                // Revert optimistic update on API failure
                updateUserRole(roleAssignmentState.userUrn, roleAssignmentState.originalRoleUrn);

                message.destroy();
                message.error({
                    content:
                        roleToAssign?.urn === NO_ROLE_URN
                            ? `Failed to remove role from ${roleAssignmentState.username}: \n ${e.message || ''}`
                            : `Failed to assign role ${roleToAssign?.name} to ${roleAssignmentState.username}: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    }, [roleAssignmentState, selectRoleOptions, batchAssignRoleMutation, updateUserRole, refetch, client]);

    return {
        roleAssignmentState,
        onSelectRole,
        onCancelRoleAssignment,
        onConfirmRoleAssignment,
    };
}
