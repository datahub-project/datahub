import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
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
    UserNameCell,
    UserStatusCell,
} from '@app/identity/user/UserAndGroupList.components';
import {
    UserListItem,
    useUserListActions,
    useUserListData,
    useUserListState,
} from '@app/identity/user/UserAndGroupList.hooks';
import {
    STATUS_FILTER_OPTIONS,
    filterUsersByStatus,
    getUserStatusColor,
    getUserStatusText,
} from '@app/identity/user/UserList.utils';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { USERS_ASSIGN_ROLE_ID, USERS_INTRO_ID, USERS_SSO_ID } from '@app/onboarding/config/UsersOnboardingConfig';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { CORP_USER_STATUS_FIELD, ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { Message } from '@app/shared/Message';
import { Button, Modal, Pagination, SearchBar, SimpleSelect, Table } from '@src/alchemy-components';
import { SortingState } from '@src/alchemy-components/components/Table/types';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole, SortOrder } from '@types';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

export const UserAndGroupList = () => {
    const client = useApolloClient();
    const [roleAssignmentState, setRoleAssignmentState] = useState<{
        isViewingAssignRole: boolean;
        userUrn: string;
        username: string;
        currentRoleUrn: string;
        originalRoleUrn: string;
    } | null>(null);
    const {
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
    } = useUserListState();

    const [sortField, setSortField] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortOrder>(SortOrder.Ascending);

    const sortInput = sortField ? { field: sortField, sortOrder } : undefined;

    const { usersData, loading, error, totalUsers, selectRoleOptions, usersRefetch, onChangePage, handleDelete } =
        useUserListData(page, pageSize, query, setPage, setPageSize, sortInput, statusFilter);

    const { onResetPassword, onCloseResetModal, onDelete } = useUserListActions(
        setIsViewingResetToken,
        setResetTokenUser,
        handleDelete,
    );

    // Role assignment handlers
    const onSelectRole = (userUrn: string, username: string, currentRoleUrn: string, newRoleUrn: string) => {
        setRoleAssignmentState({
            isViewingAssignRole: true,
            userUrn,
            username,
            currentRoleUrn: newRoleUrn,
            originalRoleUrn: currentRoleUrn,
        });
    };

    const onCancelRoleAssignment = () => {
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
                    analytics.event({
                        type: EventType.SelectUserRoleEvent,
                        roleUrn: roleToAssign?.urn || 'undefined',
                        userUrn: roleAssignmentState.userUrn,
                    });
                    message.success({
                        content:
                            roleToAssign?.urn === NO_ROLE_URN
                                ? `Removed role from user ${roleAssignmentState.username}!`
                                : `Assigned role ${roleToAssign?.name} to user ${roleAssignmentState.username}!`,
                        duration: 2,
                    });
                    setRoleAssignmentState(null);
                    setTimeout(() => {
                        usersRefetch();
                        clearRoleListCache(client);
                    }, 3000);
                }
            })
            .catch((e) => {
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

    useEffect(() => {
        const users = usersData?.listUsers?.users || [];
        setUsersList(users);
    }, [usersData, setUsersList]);

    const filteredUsers = filterUsersByStatus(usersList, statusFilter);
    const isFiltering = statusFilter !== 'all';

    // Sort invited users first for ascending, last for descending
    const sortedFilteredUsers = React.useMemo(() => {
        if (!sortField || sortField !== ENTITY_NAME_FIELD) {
            return filteredUsers;
        }

        const invitedUsers = filteredUsers.filter((user) => user.invitationStatus?.status === 'SENT');
        const otherUsers = filteredUsers.filter((user) => user.invitationStatus?.status !== 'SENT');

        if (sortOrder === SortOrder.Ascending) {
            return [...invitedUsers, ...otherUsers];
        }
        return [...otherUsers, ...invitedUsers];
    }, [filteredUsers, sortField, sortOrder]);

    const handleSortColumnChange = ({
        sortColumn,
        sortOrder: tableSortOrder,
    }: {
        sortColumn: string;
        sortOrder: SortingState;
    }) => {
        setSortField(sortColumn);

        switch (tableSortOrder) {
            case SortingState.ASCENDING:
                setSortOrder(SortOrder.Ascending);
                break;
            case SortingState.DESCENDING:
                setSortOrder(SortOrder.Descending);
                break;
            default:
                setSortField(null);
                break;
        }

        setPage(1);
    };

    const columns = [
        {
            title: 'Name',
            dataIndex: 'name',
            key: ENTITY_NAME_FIELD,
            minWidth: '30%',
            sorter: false,
            render: (user: UserListItem) => <UserNameCell user={user} />,
        },
        {
            title: 'Status',
            dataIndex: 'status',
            key: CORP_USER_STATUS_FIELD,
            minWidth: '10%',
            sorter: false,
            render: (user: UserListItem) => (
                <UserStatusCell
                    user={user}
                    getUserStatusText={(status) => getUserStatusText(status, user)}
                    getUserStatusColor={(status) => getUserStatusColor(status, user)}
                />
            ),
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
            sorter: false,
            render: (user: UserListItem) => {
                const userRelationships = user?.roles?.relationships;
                const userRole =
                    userRelationships && userRelationships.length > 0
                        ? (userRelationships[0]?.entity as DataHubRole)
                        : null;

                // Check if user has a pending invitation with a role
                const invitationRole =
                    user.invitationStatus?.status === 'SENT' && user.invitationStatus?.role
                        ? user.invitationStatus.role
                        : null;

                const currentRoleUrn = userRole?.urn || invitationRole || NO_ROLE_URN;
                const currentRole = selectRoleOptions.find((role) => role.urn === currentRoleUrn);

                return (
                    <div id={USERS_ASSIGN_ROLE_ID}>
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
                            disabled={Boolean(invitationRole && user.invitationStatus?.status === 'SENT')}
                        />
                    </div>
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
                        refetch={usersRefetch}
                    />
                </ActionsContainer>
            ),
        },
    ];

    return (
        <>
            <OnboardingTour stepIds={[USERS_INTRO_ID, USERS_SSO_ID, USERS_ASSIGN_ROLE_ID]} />
            {!usersData && loading && <Message type="loading" content="Loading users..." />}
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
                {sortedFilteredUsers.length > 0 ? (
                    <>
                        <Table
                            columns={columns}
                            data={sortedFilteredUsers}
                            isLoading={loading}
                            isScrollable
                            handleSortColumnChange={handleSortColumnChange}
                        />
                        <div style={{ padding: '20px', display: 'flex', justifyContent: 'center' }}>
                            <Pagination
                                currentPage={page}
                                itemsPerPage={pageSize}
                                total={isFiltering ? sortedFilteredUsers.length : totalUsers}
                                onPageChange={onChangePage}
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
                        return roleToAssign?.urn === NO_ROLE_URN || !roleToAssign
                            ? `Would you like to remove ${roleAssignmentState.username}'s existing role?`
                            : `Would you like to assign the role ${roleToAssign?.name} to ${roleAssignmentState.username}?`;
                    })()}
                </Modal>
            )}
        </>
    );
};
