import React from 'react';
import styled from 'styled-components/macro';

import { ColorValues } from '@components/theme/config';

import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import { getUserStatusColor, getUserStatusText } from '@app/identity/user/UserList.utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Avatar, Button, Pill, ResizablePills, Text, colors } from '@src/alchemy-components';
import { Menu } from '@src/alchemy-components/components/Menu';
import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { ListUsersAndGroupsQuery } from '@graphql/user.generated';
import { CorpUserStatus, EntityType } from '@types';

// Type alias for user data from search results
export type UserListItem = Extract<
    NonNullable<NonNullable<ListUsersAndGroupsQuery['listUsersAndGroups']>['searchResults'][0]['entity']>,
    { __typename?: 'CorpUser' }
>;

// Styled components
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

export const TableContainer = styled.div`
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    max-height: calc(100vh - 330px);
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
`;

export const FilterContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

export const ActionsContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

export const ModalFooter = styled.div`
    display: flex;
    gap: 8px;
    justify-content: flex-end;
`;

// Cell component props types
type UserCellProps = {
    user: UserListItem;
};

type StatusCellProps = {
    user: UserListItem;
};

type GroupsCellProps = {
    user: UserListItem;
};

type UserActionsMenuProps = {
    user: UserListItem;
    canManagePolicies: boolean;
    onResetPassword: (user: UserListItem) => void;
    onDelete: (urn: string) => void;
};

// User name cell component
export const UserNameCell = ({ user }: UserCellProps) => {
    const entityRegistry = useEntityRegistry();
    const displayName = user.info?.displayName || user.editableProperties?.displayName;
    const email = user.info?.email;
    const avatarUrl = user.editableProperties?.pictureLink || undefined;

    // Primary text: display name if available, otherwise email/username
    const primaryText = displayName || email || user.username;
    // Secondary text: email if different from primary, otherwise username
    let secondaryText = '';
    if (displayName && email) {
        secondaryText = email;
    } else if (displayName && user.username !== displayName) {
        secondaryText = user.username;
    }

    return (
        <UserInfo>
            <Avatar name={displayName || user.username} size="lg" imageUrl={avatarUrl} />
            <UserDetails>
                <a
                    href={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user.urn}`}
                    target="_blank"
                    rel="noreferrer"
                    style={{ textDecoration: 'none' }}
                >
                    <Text weight="medium" size="md" color="gray">
                        {primaryText}
                    </Text>
                </a>
                {secondaryText && (
                    <Text size="sm" color="gray">
                        {secondaryText}
                    </Text>
                )}
            </UserDetails>
        </UserInfo>
    );
};

// User status cell component
export const UserStatusCell = ({ user }: StatusCellProps) => {
    const status = user.status || CorpUserStatus.Active;
    const statusText = getUserStatusText(status, user as any);
    const statusColor = getUserStatusColor(status, user as any);

    return <Pill variant="outline" color={statusColor} label={statusText} />;
};

// User groups cell component
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

// User actions menu component
export const UserActionsMenu = ({ user, canManagePolicies, onResetPassword, onDelete }: UserActionsMenuProps) => {
    const { onDeleteEntity } = useDeleteEntity(
        user.urn,
        EntityType.CorpUser,
        user,
        () => onDelete(user.urn),
        false,
        true,
    );

    const menuItems: ItemType[] = [
        canManagePolicies
            ? {
                  type: 'item' as const,
                  key: 'reset',
                  title: 'Reset Password',
                  icon: 'Key' as const,
                  onClick: () => onResetPassword(user),
              }
            : null,
        {
            type: 'item' as const,
            key: 'delete',
            title: 'Delete User',
            icon: 'Trash' as const,
            onClick: onDeleteEntity,
            danger: true,
        },
    ].filter(Boolean) as ItemType[];

    return (
        <Menu items={menuItems}>
            <Button
                variant="text"
                icon={{ icon: 'DotsThreeVertical', weight: 'bold', size: 'xl', source: 'phosphor', color: 'gray' }}
                isCircle
                style={ActionsButtonStyle}
            />
        </Menu>
    );
};
