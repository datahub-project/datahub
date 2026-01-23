import React, { useEffect } from 'react';

import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { NO_ROLE_TEXT, NO_ROLE_URN, STATUS_FILTER_OPTIONS } from '@app/identity/user/UserListV2.utils';
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
import {
    useRoleAssignment,
    useUserListActions,
    useUserListData,
    useUserListState,
} from '@app/identity/user/UserListV2.hooks';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { Message } from '@app/shared/Message';
import { Button, Modal, Pagination, SearchBar, SimpleSelect, Table, Text } from '@src/alchemy-components';

import { DataHubRole } from '@types';

export const UserList = () => {
    // State management
    const {
        query,
        setQuery,
        debouncedQuery,
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

    // Data fetching
    const { data, loading, error, totalUsers, selectRoleOptions, refetch } = useUserListData(
        debouncedQuery,
        page,
        pageSize,
        statusFilter,
    );

    // Actions
    const { onResetPassword, onCloseResetModal, onDelete } = useUserListActions(
        setIsViewingResetToken,
        setResetTokenUser,
        page,
        pageSize,
        refetch,
    );

    // Role assignment
    const { roleAssignmentState, onSelectRole, onCancelRoleAssignment, onConfirmRoleAssignment } = useRoleAssignment(
        usersList,
        setUsersList,
        selectRoleOptions,
        refetch,
    );

    // Update local users list when data changes
    useEffect(() => {
        const users =
            (data?.searchUsers?.searchResults
                ?.map((result) => result.entity)
                .filter((entity) => entity?.__typename === 'CorpUser') as UserListItem[]) || [];
        setUsersList(users);
    }, [data, setUsersList]);

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
                const currentRole =
                    selectRoleOptions.find((role) => role.urn === currentRoleUrn) || userRole || undefined;

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
