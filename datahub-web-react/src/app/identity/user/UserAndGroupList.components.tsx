import { message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { ColorValues } from '@components/theme/config';

import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';
import { RecommendedUsersTable } from '@app/identity/user/RecommendedUsersTable';
import SimpleSelectRole from '@app/identity/user/SimpleSelectRole';
import { UserListItem } from '@app/identity/user/UserAndGroupList.hooks';
import { STATUS_FILTER_OPTIONS } from '@app/identity/user/UserList.utils';
import { useRevokeUserInvitationMutation } from '@app/identity/user/hooks/useRevokeUserInvitation';
import { useEntityRegistry } from '@app/useEntityRegistry';
import {
    Avatar,
    Button,
    Pagination,
    Pill,
    SearchBar,
    SimpleSelect,
    Table,
    Text,
    colors,
} from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';
import { ResizablePills } from '@src/alchemy-components/components/ResizablePills';

import { useSendUserInvitationsMutation } from '@graphql/mutations.generated';
import { CorpUser, CorpUserStatus, DataHubRole, EntityType } from '@types';

const UserInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

const UserDetails = styled.div`
    display: flex;
    flex-direction: column;
    color: ${colors.gray[600]};
`;

const GroupTags = styled.div`
    display: flex;
    flex-wrap: nowrap;
    gap: 4px;
    width: 100%;
    overflow: hidden;
    position: relative;
`;

const ActionsButtonStyle = {
    background: 'none',
    border: 'none',
    boxShadow: 'none',
};

export const UserContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

export const TableContainer = styled.div<{ $hasSsoBanner?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - ${(props) => (props.$hasSsoBanner ? '410px' : '330px')});
    overflow: auto;

    /* Make table header sticky */
    .ant-table-thead {
        position: sticky;
        top: 0;
        z-index: 1;
        background: white;
    }

    /* Ensure header cells have proper background */
    .ant-table-thead > tr > th {
        background: white !important;
        border-bottom: 1px solid #f0f0f0;
    }
`;

export const FiltersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

export const SearchContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

export const FilterContainer = styled.div`
    display: flex;
    align-items: center;
`;

export const ActionsContainer = styled.div`
    display: flex;
    align-items: right;
    justify-content: flex-end;
    gap: 12px;
`;

export const SubTabsContainer = styled.div`
    margin-top: 8px;
    margin-bottom: 16px;
    position: relative;
`;

export const TabPillWrapper = styled.div`
    position: absolute;
    top: 8px;
    left: 155px;
    z-index: 1;
    pointer-events: none; /* Make it non-interactive */
`;

export const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

export const BulkActionsContainer = styled.div`
    position: sticky;
    bottom: 100px;
    display: flex;
    justify-content: center;
    padding: 16px 0;
    z-index: 100;

    > div {
        display: flex;
        align-items: center;
        gap: 12px;
        background-color: white;
        border-radius: 8px;
        padding: 4px;
        border: 1px solid ${colors.gray[200]};
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        width: fit-content;
    }
`;

type BulkActionsWidgetProps = {
    selectRoleOptions: DataHubRole[];
    onBulkInvite: (role: DataHubRole) => Promise<void>;
    onBulkDismiss: () => Promise<void>;
};

export const BulkActionsWidget = ({ selectRoleOptions, onBulkInvite, onBulkDismiss }: BulkActionsWidgetProps) => {
    // Set default role to "Reader"
    const readerRole = selectRoleOptions.find((role) => role.name === 'Reader');
    const [selectedRole, setSelectedRole] = useState<DataHubRole | undefined>(readerRole);

    const handleInviteAll = async () => {
        if (!selectedRole) {
            message.error('Please select a role before inviting users');
            return;
        }
        await onBulkInvite(selectedRole);
    };

    return (
        <BulkActionsContainer>
            <div>
                <SimpleSelectRole
                    selectedRole={selectedRole}
                    onRoleSelect={setSelectedRole}
                    size="md"
                    width="fit-content"
                />
                <Button variant="filled" size="md" onClick={onBulkDismiss} color="red">
                    Dismiss All
                </Button>
                <Button variant="filled" size="md" onClick={handleInviteAll} color="green">
                    Invite All
                </Button>
            </div>
        </BulkActionsContainer>
    );
};

export const ViewAllTabMessage = () => (
    <Text size="sm">
        View Invited users on the <a href="/settings/identities/users">All Users</a> tab.
    </Text>
);

export const UserName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
`;

type UserCellProps = {
    user: UserListItem;
};

export const UserNameCell = ({ user }: UserCellProps) => {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);

    return (
        <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}>
            <UserInfo>
                <Avatar size="xl" name={displayName} imageUrl={user.editableProperties?.pictureLink || undefined} />
                <UserDetails>
                    <Text size="md" weight="semiBold" lineHeight="xs">
                        {displayName}
                    </Text>
                    <Text color="gray" size="md" lineHeight="md">
                        {user.username}
                    </Text>
                </UserDetails>
            </UserInfo>
        </Link>
    );
};

type StatusCellProps = {
    user: UserListItem;
    getUserStatusText: (userStatus: CorpUserStatus | undefined | null, user: UserListItem) => string;
    getUserStatusColor: (userStatus: CorpUserStatus | undefined | null, user: UserListItem) => ColorValues;
};

export const UserStatusCell = ({ user, getUserStatusText, getUserStatusColor }: StatusCellProps) => {
    const status = getUserStatusText(user.status, user);
    const color = getUserStatusColor(user.status, user);
    return <Pill variant="filled" color={color} label={status} />;
};

type GroupsCellProps = {
    user: UserListItem;
};

export const UserGroupsCell = ({ user }: GroupsCellProps) => {
    const groupRelationships = user?.groups?.relationships || [];
    const groups = groupRelationships.map((relationship) => {
        const group = relationship.entity;
        if (group?.__typename === 'CorpGroup') {
            return group?.info?.displayName || group?.properties?.displayName || group?.name || 'Unknown Group';
        }
        return 'Unknown Group';
    });

    return (
        <GroupTags>
            <ResizablePills
                items={groups}
                getItemWidth={(groupName) => groupName.length * 8 + 32}
                gap={4}
                overflowButtonWidth={50}
                minContainerWidthForOne={100}
                keyExtractor={(groupName) => groupName}
                renderPill={(groupName) => (
                    <Pill variant="outline" color="gray" label={groupName} customStyle={{ margin: '0 2px 2px 0' }} />
                )}
                overflowTooltipContent={() => (
                    <div>
                        <div style={{ fontWeight: 'bold', color: '#374066' }}>Groups</div>
                        <div
                            style={{
                                display: 'flex',
                                flexWrap: 'wrap',
                                gap: '6px',
                                maxWidth: '300px',
                                margin: '12px',
                            }}
                        >
                            {groups.map((groupName: string) => (
                                <Pill key={groupName} variant="outline" label={groupName} />
                            ))}
                        </div>
                    </div>
                )}
            />
        </GroupTags>
    );
};

type UserActionsMenuProps = {
    user: UserListItem;
    canManagePolicies: boolean;
    onResetPassword: (user: { urn: string; username: string }) => void;
    onDelete: (urn: string) => void;
    refetch?: () => void;
};

export const UserActionsMenu = ({
    user,
    canManagePolicies,
    onResetPassword,
    onDelete,
    refetch,
}: UserActionsMenuProps) => {
    const [revokeUserInvitation] = useRevokeUserInvitationMutation();
    const [sendUserInvitationsMutation] = useSendUserInvitationsMutation();

    const { onDeleteEntity } = useDeleteEntity(
        user.urn,
        EntityType.CorpUser,
        user,
        () => onDelete(user.urn),
        false,
        true,
    );

    const isNativeUser: boolean = user.isNativeUser as boolean;
    const shouldShowPasswordReset: boolean = canManagePolicies && isNativeUser;
    const isInvitedUser = user.invitationStatus?.status === 'SENT';

    // Custom delete handler for invited users
    const handleDeleteWithInvitationRevoke = async () => {
        if (isInvitedUser) {
            try {
                // First revoke the invitation and invalidate the token
                const revokeResult = await revokeUserInvitation({
                    variables: { userUrn: user.urn },
                });

                if (revokeResult.data?.revokeUserInvitation) {
                    message.success('Invitation revoked and token invalidated');
                    // Then delete the user
                    onDelete(user.urn);
                } else {
                    message.error('Failed to revoke invitation');
                }
            } catch (error: any) {
                message.error(`Failed to revoke invitation: ${error.message || 'Unknown error'}`);
                console.error('Error revoking invitation:', error);
            }
        } else {
            // For non-invited users, use the standard delete flow
            onDeleteEntity();
        }
    };

    // Check if user has pending invitation
    const canResendInvitation = canManagePolicies && isInvitedUser;

    const handleResendInvitation = async () => {
        // For invited users, the email is typically stored in the username field
        // since their username IS the email address they were invited with
        const email = user.info?.email || user.username;
        if (!email) {
            message.error('No email address found for this user');
            return;
        }

        // Use the role from the invitation
        const roleToUse = user.invitationStatus?.role;
        if (!roleToUse) {
            message.error('No role found for invitation');
            return;
        }

        // Create a CorpUser-compatible object for the EmailInvitationService
        const corpUserForService = {
            ...user,
            type: EntityType.CorpUser,
            properties: {
                email,
                displayName: user.info?.displayName || user.username,
            },
        };

        const emailService = new EmailInvitationService(sendUserInvitationsMutation);
        const success = await emailService.sendSingleInvitation(
            corpUserForService as CorpUser,
            { urn: roleToUse } as DataHubRole,
        );
        if (success && refetch) {
            refetch();
        }
    };

    const items: ItemType[] = [
        // Don't show Copy Urn for invited users
        ...(isInvitedUser
            ? []
            : [
                  {
                      type: 'item' as const,
                      key: 'copyurn',
                      title: 'Copy Urn',
                      icon: 'Copy',
                      onClick: () => {
                          navigator.clipboard.writeText(user.urn);
                          message.success('Urn copied to clipboard');
                      },
                  },
              ]),
        {
            type: 'item' as const,
            key: 'reset',
            title: 'Reset Password',
            icon: 'Password',
            onClick: () => {
                onResetPassword({ urn: user.urn, username: user.username });
            },
            disabled: !shouldShowPasswordReset,
        },
        {
            type: 'item' as const,
            key: 'resend-invitation',
            title: 'Resend Invitation',
            icon: 'Repeat',
            onClick: handleResendInvitation,
            disabled: !canResendInvitation,
        },
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete User',
            icon: 'Trash',
            danger: true,
            onClick: handleDeleteWithInvitationRevoke,
        },
    ];

    return (
        <Menu items={items}>
            <Button
                variant="text"
                icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: 'xl', source: 'phosphor', color: 'gray' }}
                isCircle
                style={ActionsButtonStyle}
            />
        </Menu>
    );
};

type AllUsersTabProps = {
    query: string;
    setQuery: (query: string) => void;
    setPage: (page: number) => void;
    statusFilter: string;
    setStatusFilter: (filter: string) => void;
    sortedFilteredUsers: UserListItem[];
    loading: boolean;
    columns: any[];
    page: number;
    pageSize: number;
    totalUsers: number;
    onChangePage: (page: number) => void;
    hasSsoBanner?: boolean;
};

export const AllUsersTab = ({
    query,
    setQuery,
    setPage,
    statusFilter,
    setStatusFilter,
    sortedFilteredUsers,
    loading,
    columns,
    page,
    pageSize,
    totalUsers,
    onChangePage,
    hasSsoBanner,
}: AllUsersTabProps) => (
    <>
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

        <TableContainer $hasSsoBanner={hasSsoBanner}>
            {sortedFilteredUsers.length > 0 ? (
                <>
                    <Table columns={columns} data={sortedFilteredUsers} isLoading={loading} isScrollable />
                    <div style={{ padding: '8px 20px 0 20px', display: 'flex', justifyContent: 'center' }}>
                        <Pagination
                            currentPage={page}
                            itemsPerPage={pageSize}
                            total={totalUsers}
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
    </>
);

type RecommendedUsersTabProps = {
    onInviteUser: (user: CorpUser, role?: DataHubRole) => Promise<boolean>;
    selectRoleOptions: DataHubRole[];
    hasSsoBanner?: boolean;
};

export const RecommendedUsersTab = ({ onInviteUser, selectRoleOptions, hasSsoBanner }: RecommendedUsersTabProps) => (
    <RecommendedUsersTable
        onInviteUser={onInviteUser}
        selectRoleOptions={selectRoleOptions}
        hasSsoBanner={hasSsoBanner}
    />
);
