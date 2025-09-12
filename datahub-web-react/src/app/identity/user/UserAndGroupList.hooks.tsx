import * as QueryString from 'query-string';
import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

import { useUserContext } from '@app/context/useUserContext';
import { DEFAULT_USER_LIST_PAGE_SIZE, removeUserFromListUsersCache } from '@app/identity/user/cacheUtils';
import { scrollToTop } from '@app/shared/searchUtils';

import { useListRolesQuery } from '@graphql/role.generated';
import { ListUsersAndGroupsQuery, useListUsersAndGroupsQuery } from '@graphql/user.generated';
import { DataHubRole, SortOrder } from '@types';

// Type alias for the user data returned by the GraphQL query
export type UserListItem = Extract<
    NonNullable<NonNullable<ListUsersAndGroupsQuery['listUsersAndGroups']>['searchResults'][0]['entity']>,
    { __typename?: 'CorpUser' }
>;

export const useUserListState = () => {
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [query, setQuery] = useState<string>('');
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [usersList, setUsersList] = useState<Array<UserListItem>>([]);
    const [isViewingResetToken, setIsViewingResetToken] = useState(false);
    const [resetTokenUser, setResetTokenUser] = useState<{ urn: string; username: string } | null>(null);
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_USER_LIST_PAGE_SIZE);

    useEffect(() => setQuery(paramsQuery || ''), [paramsQuery]);

    const authenticatedUser = useUserContext();
    const canManagePolicies = authenticatedUser?.platformPrivileges?.managePolicies || false;

    return {
        query,
        setQuery,
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
};

export const useUserListData = (
    page: number,
    pageSize: number,
    query: string,
    setPage: SetPage,
    setPageSize: SetPageSize,
    sortInput?: { field: string; sortOrder: SortOrder },
    statusFilter?: string,
) => {
    const start = (page - 1) * pageSize;
    const count = pageSize;

    // Build filters for status filtering
    const buildFilters = (statusFilterParam?: string) => {
        if (!statusFilterParam || statusFilterParam === 'all') {
            return undefined;
        }

        switch (statusFilterParam.toLowerCase()) {
            case 'active':
                return [{ field: 'status', values: ['ACTIVE'] }];
            case 'suspended':
                return [{ field: 'status', values: ['SUSPENDED'] }];
            case 'invited':
                return [{ field: 'invitationStatus', values: ['SENT'] }];
            case 'inactive':
                return [{ field: 'status', values: ['INACTIVE'] }];
            default:
                return undefined;
        }
    };

    const filters = buildFilters(statusFilter);

    const {
        loading: usersLoading,
        error: usersError,
        data: usersData,
        client,
        refetch: usersRefetch,
    } = useListUsersAndGroupsQuery({
        variables: {
            input: {
                start,
                count,
                query: query || '*',
                filters,
                sortInput: sortInput
                    ? {
                          sortCriteria: [
                              {
                                  field: sortInput.field,
                                  sortOrder: sortInput.sortOrder,
                              },
                          ],
                      }
                    : undefined,
            },
        },
        fetchPolicy: 'no-cache',
    });

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

    const totalUsers = usersData?.listUsersAndGroups?.total || 0;
    const loading = usersLoading || rolesLoading;
    const error = usersError || rolesError;
    const selectRoleOptions = rolesData?.listRoles?.roles?.map((role) => role as DataHubRole) || [];

    const onChangePage = (newPage: number, newPageSize?: number) => {
        scrollToTop();
        setPage(newPage);
        if (newPageSize && newPageSize !== pageSize) {
            setPageSize(newPageSize);
        }
    };

    const handleDelete = (urn: string) => {
        removeUserFromListUsersCache(urn, client, page, pageSize);
        usersRefetch();
    };

    return {
        usersData,
        loading,
        error,
        totalUsers,
        selectRoleOptions,
        usersRefetch,
        onChangePage,
        handleDelete,
        pageSize,
    };
};

type SetPage = (page: number) => void;
type SetPageSize = (pageSize: number) => void;

export const useUserListActions = (
    setIsViewingResetToken: (viewing: boolean) => void,
    setResetTokenUser: (user: { urn: string; username: string } | null) => void,
    handleDelete: (urn: string) => void,
) => {
    const onResetPassword = (user: { urn: string; username: string }) => {
        setResetTokenUser(user);
        setIsViewingResetToken(true);
    };

    const onCloseResetModal = () => {
        setIsViewingResetToken(false);
        setResetTokenUser(null);
    };

    return {
        onResetPassword,
        onCloseResetModal,
        onDelete: handleDelete,
    };
};
