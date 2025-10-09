import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import { useDebounce } from 'react-use';

import analytics, { EventType } from '@app/analytics';
import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import {
    ActionsContainer,
    AllUsersTab,
    ModalFooter,
    RecommendedUsersTab,
    SubTabsContainer,
    TabPillWrapper,
    UserActionsMenu,
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
import { getUserStatusColor, getUserStatusText } from '@app/identity/user/UserList.utils';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { USERS_ASSIGN_ROLE_ID } from '@app/onboarding/config/UsersOnboardingConfig';
import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { CORP_USER_STATUS_FIELD, ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';
import { Message } from '@app/shared/Message';
import { Button, Modal, Pill, Tabs } from '@src/alchemy-components';
import { removeRuntimePath } from '@utils/runtimeBasePath';

import { useBatchAssignRoleMutation, useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser, DataHubRole } from '@types';

const NO_ROLE_TEXT = 'No Role';
const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

enum SubTabType {
    All = 'all',
    Recommended = 'recommended',
}

interface Props {
    hasSsoBanner?: boolean;
}

export const UserAndGroupList = ({ hasSsoBanner }: Props) => {
    const client = useApolloClient();
    const history = useHistory();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const tabParam = (params?.tab as string) || undefined;
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

    const [debouncedQuery, setDebouncedQuery] = useState('');

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

    // Reset to page 1 when debounced search query or status filter changes
    useEffect(() => {
        setPage(1);
    }, [debouncedQuery, statusFilter, setPage]);

    // Initialize activeSubTab based on query parameter
    const getInitialSubTab = () => {
        if (tabParam === SubTabType.Recommended) return SubTabType.Recommended;
        if (tabParam === SubTabType.All) return SubTabType.All;
        return SubTabType.All; // Default to All if no valid tab param
    };
    const [activeSubTab, setActiveSubTab] = useState<SubTabType>(getInitialSubTab());

    // Update activeSubTab when URL changes
    useEffect(() => {
        if (tabParam === SubTabType.Recommended && activeSubTab !== SubTabType.Recommended) {
            setActiveSubTab(SubTabType.Recommended);
        } else if (tabParam === SubTabType.All && activeSubTab !== SubTabType.All) {
            setActiveSubTab(SubTabType.All);
        }
    }, [tabParam, activeSubTab]);

    // Handler for manual tab changes - updates both state and URL
    const handleTabChange = (key: string) => {
        const newTab = key as SubTabType;
        setActiveSubTab(newTab);

        // Update URL to reflect the new tab
        const newUrl = new URL(window.location.href);
        if (newTab === SubTabType.All) {
            newUrl.searchParams.delete('tab'); // Remove tab param for 'all' (default)
        } else {
            newUrl.searchParams.set('tab', newTab);
        }
        history.replace(removeRuntimePath(newUrl.pathname) + newUrl.search);
    };

    const { usersData, loading, error, totalUsers, selectRoleOptions, usersRefetch, onChangePage, handleDelete } =
        useUserListData(page, pageSize, debouncedQuery, setPage, setPageSize, undefined, statusFilter);

    const { onResetPassword, onCloseResetModal, onDelete } = useUserListActions(
        setIsViewingResetToken,
        setResetTokenUser,
        handleDelete,
    );

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
    const [sendUserInvitationsMutation] = useSendUserInvitationsMutation();

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

    useEffect(() => {
        const users =
            (usersData?.listUsersAndGroups?.searchResults
                ?.map((result) => result.entity)
                .filter((entity) => entity?.__typename === 'CorpUser') as UserListItem[]) || [];
        setUsersList(users);
    }, [usersData, setUsersList]);

    // Helper functions for recommended users
    const handleInviteRecommendedUser = async (user: CorpUser, role?: DataHubRole, recommendedUsers?: CorpUser[]) => {
        if (!role) {
            message.error('Please select a role before sending invitation');
            return false;
        }

        try {
            // Create EmailInvitationService instance
            const emailInvitationService = new EmailInvitationService(sendUserInvitationsMutation);

            const userEmail = user.username;
            if (!userEmail) {
                message.error('No email found for this user');
                return false;
            }

            // Create a properly formatted user for the service
            const formattedUser: CorpUser = {
                ...user,
                properties: {
                    ...user.properties,
                    email: userEmail,
                    displayName: user.properties?.displayName || user.username,
                    active: true,
                },
            };
            const userIndex = recommendedUsers?.findIndex((u) => u.urn === user.urn);

            analytics.event({
                type: EventType.ClickInviteRecommendedUserEvent,
                roleUrn: role?.urn || '',
                userEmail: userEmail || '',
                location: 'recommended_users_list',
                recommendationType: 'top_user',
                recommendationIndex: userIndex || undefined,
            });

            // Send invitation using the service
            const success = await emailInvitationService.sendSingleInvitation(formattedUser, role);

            if (success) {
                // Refresh the users list to show updated state if needed
                usersRefetch();
            }

            return success;
        } catch (invitationError) {
            message.error('Invitation failed');
            console.error('Failed to invite recommended user:', invitationError);
            return false;
        }
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
            key: CORP_USER_STATUS_FIELD,
            minWidth: '10%',
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

    const renderAllUsersTab = () => (
        <AllUsersTab
            query={query}
            setQuery={setQuery}
            setPage={setPage}
            statusFilter={statusFilter}
            setStatusFilter={setStatusFilter}
            sortedFilteredUsers={usersList}
            loading={loading}
            columns={columns}
            page={page}
            pageSize={pageSize}
            totalUsers={totalUsers}
            onChangePage={onChangePage}
            hasSsoBanner={hasSsoBanner}
        />
    );

    const renderRecommendedUsersTab = () => (
        <RecommendedUsersTab
            onInviteUser={handleInviteRecommendedUser}
            selectRoleOptions={selectRoleOptions}
            hasSsoBanner={hasSsoBanner}
        />
    );

    return (
        <>
            {!usersData && loading && <Message type="loading" content="Loading users..." />}
            {error && <Message type="error" content="Failed to load users! An unexpected error occurred." />}

            <SubTabsContainer>
                <Tabs
                    selectedTab={activeSubTab}
                    onChange={handleTabChange}
                    secondary
                    tabs={[
                        {
                            key: SubTabType.All,
                            name: 'All',
                            component: renderAllUsersTab(),
                        },
                        {
                            key: SubTabType.Recommended,
                            name: 'Recommended',
                            component: renderRecommendedUsersTab(),
                        },
                    ]}
                />
                <TabPillWrapper>
                    <Pill size="sm" color="blue" label="New" />
                </TabPillWrapper>
            </SubTabsContainer>

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
