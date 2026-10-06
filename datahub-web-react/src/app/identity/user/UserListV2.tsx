import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';

import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import {
    ActionsContainer,
    FilterContainer,
    FiltersHeader,
    ModalFooter,
    PageContainer,
    PaginationContainer,
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
import { NO_ROLE_URN } from '@app/identity/user/UserListV2.utils';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { Message } from '@app/shared/Message';
import { Button, Modal, Pagination, SearchBar, SimpleSelect, Table, Text } from '@src/alchemy-components';

import { DataHubRole } from '@types';

const ALL_FILTER = 'all';
const MIN_CHARS_TEXT_STYLE = { marginTop: '4px' };

export const UserList = () => {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');

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

    const statusFilterOptions = [
        { value: 'active', label: t('users.statusFilter.active') },
        { value: 'suspended', label: t('users.statusFilter.suspended') },
    ];

    const columns = [
        {
            title: t('users.columns.name'),
            dataIndex: 'name',
            key: ENTITY_NAME_FIELD,
            minWidth: '30%',
            render: (user: UserListItem) => <UserNameCell user={user} />,
        },
        {
            title: t('users.columns.status'),
            dataIndex: 'status',
            key: 'status',
            minWidth: '10%',
            render: (user: UserListItem) => <UserStatusCell user={user} />,
        },
        {
            title: t('users.columns.assignedGroups'),
            dataIndex: 'groups',
            key: 'groups',
            minWidth: '35%',
            render: (user: UserListItem) => <UserGroupsCell user={user} />,
        },
        {
            title: t('users.columns.role'),
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
                        placeholder={t('users.noRole')}
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
        <PageContainer>
            {!data && loading && <Message type="loading" content={t('users.loading')} />}
            {error && <Message type="error" content={t('users.loadError')} />}

            <UserContainer>
                <FiltersHeader>
                    <SearchContainer>
                        <SearchBar
                            placeholder={t('users.search')}
                            value={query}
                            onChange={(value) => {
                                setQuery(value);
                                setPage(1);
                            }}
                            width="300px"
                            allowClear
                        />
                        {query.length > 0 && query.length < 3 && (
                            <Text size="xs" color="gray" style={MIN_CHARS_TEXT_STYLE}>
                                {t('users.searchMinChars')}
                            </Text>
                        )}
                    </SearchContainer>
                    <FilterContainer>
                        <SimpleSelect
                            placeholder={t('users.statusFilter.placeholder')}
                            position="end"
                            options={statusFilterOptions}
                            values={statusFilter === ALL_FILTER ? [] : [statusFilter]}
                            showClear
                            onUpdate={(values) => {
                                setStatusFilter(values.length > 0 ? values[0] : ALL_FILTER);
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
                        <PaginationContainer>
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
                        </PaginationContainer>
                    </>
                ) : (
                    <div style={{ padding: '20px', textAlign: 'center' }}>
                        {loading ? t('users.loading') : t('users.noUsersFound')}
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
                    title={t('roleAssignment.confirmTitle')}
                    onCancel={onCancelRoleAssignment}
                    footer={
                        <ModalFooter>
                            <Button variant="outline" onClick={onCancelRoleAssignment}>
                                {tc('cancel')}
                            </Button>
                            <Button variant="filled" onClick={onConfirmRoleAssignment}>
                                {tc('confirm')}
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
                                {roleToAssign?.urn === NO_ROLE_URN
                                    ? t('users.roleAssign.removeConfirmation', {
                                          username: roleAssignmentState.username,
                                      })
                                    : t('users.roleAssign.assignConfirmation', {
                                          roleName: roleToAssign?.name,
                                          username: roleAssignmentState.username,
                                      })}
                            </Text>
                        );
                    })()}
                </Modal>
            )}
        </PageContainer>
    );
};
