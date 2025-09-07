import { message } from 'antd';
import { Copy, LockOpen, Repeat, Trash } from 'phosphor-react';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { ColorValues } from '@components/theme/config';

import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import { EmailInvitationService } from '@app/identity/user/EmailInvitationService';
import { UserListItem } from '@app/identity/user/UserAndGroupList.hooks';
import { useRevokeUserInvitationMutation } from '@app/identity/user/hooks/useRevokeUserInvitation';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Avatar, Button, Dropdown, Pill, Text, Tooltip, colors } from '@src/alchemy-components';

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
    flex-wrap: wrap;
    gap: 4px;
    max-width: 200px;
`;

const StyledActionsButton = styled(Button)`
    background: none !important;
    border: none !important;
    box-shadow: none !important;

    &:hover {
        background: none !important;
        border: none !important;
        box-shadow: none !important;
    }

    &:focus {
        background: none !important;
        border: none !important;
        box-shadow: none !important;
    }
`;

export const UserContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin-top: 16px;
`;

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - 300px); /* Constrain to page height minus header/filters space */
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
    align-items: center;
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
    margin-bottom: 16px;
`;

export const ModalFooter = styled.div`
    display: flex;
    justify-content: flex-end;
    gap: 8px;
`;

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

    if (groups.length === 0) {
        return null;
    }

    return (
        <GroupTags>
            {groups.slice(0, 2).map((groupName: string) => (
                <Pill
                    key={groupName}
                    variant="outline"
                    color="gray"
                    label={groupName}
                    customStyle={{ margin: '0 2px 2px 0' }}
                />
            ))}
            {groups.length > 2 && (
                <Tooltip
                    title={
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
                    }
                    placement="top"
                    overlayStyle={{ maxWidth: '350px' }}
                >
                    <span style={{ display: 'inline-block' }}>
                        <Pill
                            key="more-groups"
                            variant="outline"
                            color="gray"
                            label={`+${groups.length - 2}`}
                            customStyle={{ margin: '0 2px 2px 0', cursor: 'pointer' }}
                        />
                    </span>
                </Tooltip>
            )}
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

    const items = [
        // Don't show Copy Urn for invited users
        ...(isInvitedUser
            ? []
            : [
                  {
                      key: 'copyurn',
                      label: (
                          <span>
                              <Copy size={16} style={{ marginRight: '8px' }} />
                              Copy Urn
                          </span>
                      ),
                      onClick: () => {
                          navigator.clipboard.writeText(user.urn);
                          message.success('Urn copied to clipboard');
                      },
                  },
              ]),
        {
            key: 'reset',
            label: (
                <span>
                    <LockOpen size={16} style={{ marginRight: '8px' }} />
                    Reset Password
                </span>
            ),
            onClick: () => {
                onResetPassword({ urn: user.urn, username: user.username });
            },
            disabled: !shouldShowPasswordReset,
        },
        {
            key: 'resend-invitation',
            'data-testid': 'resend-invitation-menu-item',
            label: (
                <span>
                    <Repeat size={16} style={{ marginRight: '8px' }} />
                    Resend Invitation
                </span>
            ),
            onClick: handleResendInvitation,
            disabled: !canResendInvitation,
        },
        {
            key: 'delete',
            label: (
                <span>
                    <Trash size={16} style={{ marginRight: '8px' }} />
                    Delete User
                </span>
            ),
            onClick: handleDeleteWithInvitationRevoke,
        },
    ];

    return (
        <Dropdown trigger={['click']} menu={{ items }}>
            <StyledActionsButton
                variant="text"
                icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: 'xl', source: 'phosphor', color: 'gray' }}
                isCircle
            />
        </Dropdown>
    );
};
