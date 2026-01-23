import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useDebounce } from 'react-use';

import { useUserContext } from '@app/context/useUserContext';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import {
    ActionsContainer,
    FilterContainer,
    FiltersHeader,
    ModalFooter,
    SearchContainer,
    TableContainer,
    UserActionsMenu,
    UserContainer,
    UserGroupsCell,
    UserListItem,
    UserNameCell,
    UserStatusCell,
} from '@app/identity/user/UserListV2.components';
import { STATUS_FILTER_OPTIONS } from '@app/identity/user/UserList.utils';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { DEFAULT_USER_LIST_PAGE_SIZE, removeUserFromListUsersCache } from '@app/identity/user/cacheUtils';
import { useRoleSelector } from '@app/identity/user/useRoleSelector';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { Message } from '@app/shared/Message';
import { Button, Modal, Pagination, SearchBar, SimpleSelect, Table, Text } from '@src/alchemy-components';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { useListUsersAndGroupsQuery } from '@graphql/user.generated';
import { DataHubRole } from '@types';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

export const UserList = () => {
    const client = useApolloClient();
    const [query, setQuery] = useState<string>('');
    const [debouncedQuery, setDebouncedQuery] = useState('');
    const [statusFilter, setStatusFilter] = useState<string>('all');
    const [usersList, setUsersList] = useState<UserListItem[]>([]);
    const [isViewingResetToken, setIsViewingResetToken] = useState(false);
    const [resetTokenUser, setResetTokenUser] = useState<UserListItem | null>(null);
    const [roleAssignmentState, setRoleAssignmentState] = useState<{
        isViewingAssignRole: boolean;
        userUrn: string;
        username: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null>(null);

    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_USER_LIST_PAGE_SIZE);

    const authenticatedUser = useUserContext();
    const canManagePolicies = authenticatedUser?.platformPrivileges?.managePolicies || false;

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

    // Reset to page 1 when search or filter changes
    useEffect(() => {
        setPage(1);
    }, [debouncedQuery, statusFilter]);

    // Build filters for server-side status filtering
    const buildFilters = (statusFilterParam?: string) => {
        if (!statusFilterParam || statusFilterParam === 'all') {
            return undefined;
        }

        switch (statusFilterParam.toLowerCase()) {
            case 'active':
                return [{ field: 'status', values: ['ACTIVE'] }];
            case 'suspended':
                return [{ field: 'status', values: ['SUSPENDED'] }];
            default:
                return undefined;
        }
    };

    const filters = buildFilters(statusFilter);

    const { data, loading, error, refetch } = useListUsersAndGroupsQuery({
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query: debouncedQuery || '*',
                filters,
                sortInput: undefined, // TODO: Add sorting support
            },
        },
        fetchPolicy: 'no-cache',
    });

    const totalUsers = data?.listUsersAndGroups?.total || 0;

    useEffect(() => {
        const users =
            (data?.listUsersAndGroups?.searchResults
                ?.map((result) => result.entity)
                .filter((entity) => entity?.__typename === 'CorpUser') as UserListItem[]) || [];
        setUsersList(users);
    }, [data]);

    const handleDelete = (urn: string) => {
        removeUserFromListUsersCache(urn, client, page, pageSize);
        refetch();
    };

    const onResetPassword = (user: UserListItem) => {
        setResetTokenUser(user);
        setIsViewingResetToken(true);
    };

    const onCloseResetModal = () => {
        setIsViewingResetToken(false);
        setResetTokenUser(null);
    };

    const onDelete = (urn: string) => {
        handleDelete(urn);
    };

    // Fetch roles for the role selector
    const { roles: selectRoleOptions } = useRoleSelector();

    // Utility function to update user role in list
    const updateUserRole = (userUrn: string, roleUrn: string) => {
        setUsersList((prevUsers) =>
            prevUsers.map((user) => {
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
    };

    // Role assignment handlers
    const onSelectRole = (userUrn: string, username: string, currentRoleUrn: string, newRoleUrn: string) => {
        // Optimistically update the UI immediately
        updateUserRole(userUrn, newRoleUrn);

        setRoleAssignmentState({
            isViewingAssignRole: true,
            userUrn,
            username,
            currentRoleUrn: newRoleUrn,
            originalRoleUrn: currentRoleUrn,
        });
    };

    const onCancelRoleAssignment = () => {
        if (!roleAssignmentState) return;

        // Revert optimistic update by restoring original role
        updateUserRole(roleAssignmentState.userUrn, roleAssignmentState.originalRoleUrn);
        setRoleAssignmentState(null);
    };

    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();

    const onConfirmRoleAssignment = () => {
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
    };

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: ENTITY_NAME_FIELD,
            minWidth: '30%',
            render: (user: UserListItem) => <UserNameCell user={user} />,
        },
        {
            title: 'Status',
            dataIndex: 'status',
            key: 'status',
            minWidth: '10%',
            render: (user: UserListItem) => <UserStatusCell user={user} />,
        },
        {
            title: 'Assigned Groups',
            dataIndex: 'groups',
            key: 'groups',
            minWidth: '35%',
            render: (user: UserListItem) => <UserGroupsCell user={user} />,
        },
        {
            title: 'Role',
            key: 'roles',
            minWidth: '10%',
            render: (user: UserListItem) => {
                const userRelationships = user?.roles?.relationships;
                const userRole =
                    userRelationships && userRelationships.length > 0
                        ? (userRelationships[0]?.entity as DataHubRole)
                        : null;

                const currentRoleUrn = userRole?.urn || NO_ROLE_URN;

                // Find role in options, or use userRole directly if not in options yet (during load)
                const currentRole = selectRoleOptions.find((role) => role.urn === currentRoleUrn) || userRole || undefined;

                return (
                    <SimpleSelectRole
                        selectedRole={currentRole}
                        onRoleSelect={(role) => {
                            const newRoleUrn = role?.urn || NO_ROLE_URN;
                            if (newRoleUrn !== currentRoleUrn) {
                                onSelectRole(user.urn, user.username, currentRoleUrn, newRoleUrn);
                            }
                        }}
                        placeholder={NO_ROLE_TEXT}
                        size="md"
                        width="fit-content"
                    />
                );
            },
        },
        {
            title: '',
            key: 'actions',
            minWidth: '5%',
            render: (user: UserListItem) => (
                <ActionsContainer>
                    <UserActionsMenu
                        user={user}
                        canManagePolicies={canManagePolicies}
                        onResetPassword={onResetPassword}
                        onDelete={onDelete}
                    />
                </ActionsContainer>
            ),
        },
    ];

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading users..." />}
            {error && <Message type="error" content="Failed to load users! An unexpected error occurred." />}

            <UserContainer>
                <FiltersHeader>
                    <SearchContainer>
                        <SearchBar
                            placeholder="Search..."
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                        {query.length > 0 && query.length < 3 && (
                            <Text size="xs" color="gray" style={{ marginTop: '4px' }}>
                                Enter at least 3 characters to search
                            </Text>
                        )}
                    </SearchContainer>
                    <FilterContainer>
                        <SimpleSelect
                            placeholder="Status"
                            position="end"
                            options={STATUS_FILTER_OPTIONS.filter((option) => option.value !== 'all').map((option) => ({
                                value: option.value,
                                label: option.label,
                            }))}
                            values={statusFilter === 'all' ? [] : [statusFilter]}
                            showClear
                            onUpdate={(values) => {
                                setStatusFilter(values.length > 0 ? values[0] : 'all');
                                setPage(1);
                            }}
                        />
                    </FilterContainer>
                </FiltersHeader>
            </UserContainer>

            <TableContainer>
                {usersList.length > 0 ? (
                    <>
                        <Table columns={columns} data={usersList} isLoading={loading} isScrollable />
                        <div style={{ padding: '8px 20px 0 20px', display: 'flex', justifyContent: 'center' }}>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={totalUsers}
                                onPageChange={(newPage, newPageSize) => {
                                    setPage(newPage);
                                    if (newPageSize && newPageSize !== pageSize) {
                                        setPageSize(newPageSize);
                                    }
                                }}
                                showSizeChanger
                                pageSizeOptions={[10, 25, 50, 100]}
                            />
                        </div>
                    </>
                ) : (
                    <div style={{ padding: '20px', textAlign: 'center' }}>
                        {loading ? 'Loading users...' : 'No users found'}
                    </div>
                )}
            </TableContainer>

            {resetTokenUser && (
                <ViewResetTokenModal
                    open={isViewingResetToken}
                    userUrn={resetTokenUser.urn}
                    username={resetTokenUser.username}
                    onClose={onCloseResetModal}
                />
            )}

            {roleAssignmentState && (
                <Modal
                    open={roleAssignmentState.isViewingAssignRole}
                    title="Confirm Role Assignment"
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                Cancel
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                Confirm
                            </Button>
                        </ModalFooter>
                    }
                >
                    {(() => {
                        const roleToAssign = selectRoleOptions.find(
                            (role) => role.urn === roleAssignmentState.currentRoleUrn,
                        );
                        return (
                            <Text>
                                Are you sure you want to{' '}
                                {roleToAssign?.urn === NO_ROLE_URN ? (
                                    <>
                                        remove the role from user <strong>{roleAssignmentState.username}</strong>?
                                    </>
                                ) : (
                                    <>
                                        assign role <strong>{roleToAssign?.name}</strong> to user{' '}
                                        <strong>{roleAssignmentState.username}</strong>?
                                    </>
                                )}
                            </Text>
                        );
                    })()}
                </Modal>
            )}
        </>
    );
};
