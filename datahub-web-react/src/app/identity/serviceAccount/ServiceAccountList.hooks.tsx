import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import { useCallback, useEffect, useState } from 'react';
import { useDebounce } from 'react-use';

import { useUserContext } from '@app/context/useUserContext';
import { removeServiceAccountFromListCache } from '@app/identity/serviceAccount/cacheUtils';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { scrollToTop } from '@app/shared/searchUtils';

import { useDeleteServiceAccountMutation, useListServiceAccountsQuery } from '@graphql/auth.generated';
import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { useListRolesQuery } from '@graphql/role.generated';
import { DataHubRole, ServiceAccount } from '@types';

const DEFAULT_PAGE_SIZE = 10;
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

export function useServiceAccountListState() {
    const [query, setQuery] = useState<string>('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [page, setPage] = useState(1);
    const [pageSize] = useState(DEFAULT_PAGE_SIZE);
    const [isCreatingServiceAccount, setIsCreatingServiceAccount] = useState(false);
    const [roleAssignmentState, setRoleAssignmentState] = useState<{
        isViewingAssignRole: boolean;
        serviceAccountUrn: string;
        serviceAccountName: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null>(null);
    // Optimistic role updates: serviceAccountUrn -> roleUrn
    const [optimisticRoles, setOptimisticRoles] = useState<Record<string, string>>({});

    // Debounce search query
    useDebounce(
        () => {
            const trimmedQuery = query.trim();
            if (trimmedQuery === '' || trimmedQuery.length >= 3) {
                setDebouncedQuery(trimmedQuery);
            }
        },
        300,
        [query],
    );

    // Reset to page 1 when debounced search query changes
    useEffect(() => {
        setPage(1);
    }, [debouncedQuery]);

    return {
        query,
        setQuery,
        debouncedQuery,
        page,
        setPage,
        pageSize,
        isCreatingServiceAccount,
        setIsCreatingServiceAccount,
        roleAssignmentState,
        setRoleAssignmentState,
        optimisticRoles,
        setOptimisticRoles,
    };
}

export function useServiceAccountListData(page: number, pageSize: number, debouncedQuery: string) {
    const apolloClient = useApolloClient();
    const authenticatedUser = useUserContext();
    const canManageServiceAccounts = authenticatedUser?.platformPrivileges?.manageServiceAccounts || false;

    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListServiceAccountsQuery({
        skip: !canManageServiceAccounts,
        variables: {
            input: {
                start,
                count: pageSize,
                query: debouncedQuery?.length ? debouncedQuery : undefined,
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const [deleteServiceAccount] = useDeleteServiceAccountMutation();

    const {
        loading: rolesLoading,
        error: rolesError,
        data: rolesData,
    } = useListRolesQuery({
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];
    const totalServiceAccounts = data?.listServiceAccounts?.total || 0;
    const serviceAccounts = (data?.listServiceAccounts?.serviceAccounts || []) as ServiceAccount[];

    const handleDelete = useCallback(
        async (urn: string) => {
            try {
                await deleteServiceAccount({ variables: { urn } });
                message.success('Service account deleted');
                removeServiceAccountFromListCache(apolloClient, urn, page, pageSize);
            } catch (e: any) {
                message.error(`Failed to delete service account: ${e.message || ''}`);
            }
        },
        [apolloClient, deleteServiceAccount, page, pageSize],
    );

    const onChangePage = useCallback((newPage: number) => {
        scrollToTop();
        return newPage;
    }, []);

    return {
        loading: loading || rolesLoading,
        error: error || rolesError,
        data,
        serviceAccounts,
        totalServiceAccounts,
        selectRoleOptions,
        canManageServiceAccounts,
        handleDelete,
        onChangePage,
        refetch,
    };
}

export function useServiceAccountRoleAssignment(
    selectRoleOptions: DataHubRole[],
    roleAssignmentState: {
        isViewingAssignRole: boolean;
        serviceAccountUrn: string;
        serviceAccountName: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null,
    setRoleAssignmentState: React.Dispatch<
        React.SetStateAction<{
            isViewingAssignRole: boolean;
            serviceAccountUrn: string;
            serviceAccountName: string;
            currentRoleUrn: string;
            originalRoleUrn: string;
        } | null>
    >,
    setOptimisticRoles: React.Dispatch<React.SetStateAction<Record<string, string>>>,
    refetch: () => void,
) {
    const apolloClient = useApolloClient();
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();

    const onSelectRole = useCallback(
        (serviceAccountUrn: string, serviceAccountName: string, currentRoleUrn: string, newRoleUrn: string) => {
            setRoleAssignmentState({
                isViewingAssignRole: true,
                serviceAccountUrn,
                serviceAccountName,
                currentRoleUrn: newRoleUrn,
                originalRoleUrn: currentRoleUrn,
            });
        },
        [setRoleAssignmentState],
    );

    const onCancelRoleAssignment = useCallback(() => {
        setRoleAssignmentState(null);
    }, [setRoleAssignmentState]);

    const onConfirmRoleAssignment = useCallback(() => {
        if (!roleAssignmentState) return;

        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        const newRoleUrn = roleToAssign?.urn || NO_ROLE_URN;

        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: newRoleUrn === NO_ROLE_URN ? null : newRoleUrn,
                    actors: [roleAssignmentState.serviceAccountUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success(
                        newRoleUrn === NO_ROLE_URN
                            ? `Removed role from service account ${roleAssignmentState.serviceAccountName}!`
                            : `Assigned role ${roleToAssign?.name} to service account ${roleAssignmentState.serviceAccountName}!`,
                    );

                    // Optimistically update the UI immediately
                    const { serviceAccountUrn } = roleAssignmentState;
                    setOptimisticRoles((prev) => ({
                        ...prev,
                        [serviceAccountUrn]: newRoleUrn,
                    }));

                    setRoleAssignmentState(null);

                    // Refetch after 3 seconds to sync with server
                    setTimeout(async () => {
                        clearRoleListCache(apolloClient);
                        // Wait for refetch to complete before clearing optimistic state
                        await refetch();
                        // Clear optimistic state only after server data is loaded
                        setOptimisticRoles((prev) => {
                            const updated = { ...prev };
                            delete updated[serviceAccountUrn];
                            return updated;
                        });
                    }, 3000);
                }
            })
            .catch((e) => {
                message.error(
                    newRoleUrn === NO_ROLE_URN
                        ? `Failed to remove role from ${roleAssignmentState.serviceAccountName}: ${e.message || ''}`
                        : `Failed to assign role ${roleToAssign?.name} to ${roleAssignmentState.serviceAccountName}: ${e.message || ''}`,
                );
            });
    }, [
        apolloClient,
        batchAssignRoleMutation,
        refetch,
        roleAssignmentState,
        selectRoleOptions,
        setOptimisticRoles,
        setRoleAssignmentState,
    ]);

    return {
        onSelectRole,
        onCancelRoleAssignment,
        onConfirmRoleAssignment,
    };
}
