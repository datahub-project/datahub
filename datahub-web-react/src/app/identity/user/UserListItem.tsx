import { Avatar, Menu, Text, Tooltip, toast } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { LockOpen } from '@phosphor-icons/react/dist/csr/LockOpen';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { AvatarType } from '@components/components/AvatarStack/types';
import { ItemType } from '@components/components/Menu/types';

import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import SelectRole from '@app/identity/user/SelectRole';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { USERS_ASSIGN_ROLE_ID } from '@app/onboarding/config/UsersOnboardingConfig';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, CorpUserStatus, DataHubRole, EntityType } from '@types';

type Props = {
    user: CorpUser;
    canManageUserCredentials: boolean;
    selectRoleOptions: Array<DataHubRole>;
    rolesLoading: boolean;
    rolesHasMore: boolean;
    rolesObserverRef: (node: HTMLDivElement | null) => void;
    rolesSearchQuery: string;
    setRolesSearchQuery: (query: string) => void;
    onDelete?: () => void;
    refetch?: () => void;
};

const UserItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const ListItem = styled.div`
    display: flex;
    align-items: center;
    padding: 12px 24px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const StatusTag = styled(Text)`
    padding: 2px 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 4px;
`;

const UserHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const ButtonGroup = styled.div`
    display: flex;
    justify-content: space-evenly;
    align-items: center;
`;

const MenuIcon = styled(DotsThreeVertical)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

const MenuTriggerButton = styled.button`
    display: flex;
    align-items: center;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};
`;

export default function UserListItem({
    user,
    canManageUserCredentials,
    selectRoleOptions,
    rolesLoading,
    rolesHasMore,
    rolesObserverRef,
    rolesSearchQuery,
    setRolesSearchQuery,
    onDelete,
    refetch,
}: Props) {
    const { t } = useTranslation('entity.identity');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const [isViewingResetToken, setIsViewingResetToken] = useState(false);
    const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
    const isNativeUser: boolean = user.isNativeUser as boolean;
    const shouldShowPasswordReset: boolean = canManageUserCredentials && isNativeUser;
    const castedCorpUser = user as any;
    const userRelationships = castedCorpUser?.roles?.relationships;
    const userRole = userRelationships && userRelationships.length > 0 && (userRelationships[0]?.entity as DataHubRole);
    const userRoleUrn = userRole && userRole.urn;

    const { onDeleteEntity } = useDeleteEntity(user.urn, EntityType.CorpUser, user, onDelete, false, true);

    const getUserStatusToolTip = (userStatus: CorpUserStatus) => {
        switch (userStatus) {
            case CorpUserStatus.Active:
                return t('users.activeStatusTooltip');
            default:
                return '';
        }
    };

    const userStatus = user.status; // Support case where the user status is undefined.
    const userStatusToolTip = userStatus && getUserStatusToolTip(userStatus);

    const items: ItemType[] = [
        {
            type: 'item',
            key: 'copyurn',
            title: t('users.copyUrn'),
            icon: Copy,
            dataTestId: 'copyurn-menu-item',
            onClick: () => {
                navigator.clipboard.writeText(user.urn);
                toast.success(t('users.urnCopied'));
            },
        },
        {
            type: 'item',
            key: 'reset',
            title: t('users.resetPasswordMenu'),
            icon: LockOpen,
            disabled: !shouldShowPasswordReset,
            dataTestId: 'reset-menu-item',
            onClick: () => setIsViewingResetToken(true),
        },
        {
            type: 'item',
            key: 'delete',
            title: tc('delete'),
            icon: Trash,
            danger: true,
            dataTestId: 'delete-menu-item',
            onClick: onDeleteEntity,
        },
    ];

    return (
        <ListItem>
            <UserItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}>
                    <UserHeaderContainer>
                        <Avatar
                            name={displayName}
                            imageUrl={user.editableProperties?.pictureLink || undefined}
                            type={AvatarType.user}
                            size="xl"
                        />
                        <div style={{ marginLeft: 16, marginRight: 20 }}>
                            <div>
                                <Text>{displayName}</Text>
                            </div>
                            <div data-testid={`email-${shouldShowPasswordReset ? 'native' : 'non-native'}`}>
                                <Text type="span" color="textSecondary">
                                    {user.username}
                                </Text>
                            </div>
                        </div>
                        {userStatus && (
                            <Tooltip title={userStatusToolTip}>
                                <StatusTag
                                    type="span"
                                    size="sm"
                                    color={userStatus === CorpUserStatus.Active ? 'textBrand' : 'textDisabled'}
                                >
                                    {userStatus}
                                </StatusTag>
                            </Tooltip>
                        )}
                    </UserHeaderContainer>
                </Link>
            </UserItemContainer>
            <ButtonGroup id={USERS_ASSIGN_ROLE_ID}>
                <SelectRole
                    user={user}
                    userRoleUrn={userRoleUrn || ''}
                    selectRoleOptions={selectRoleOptions}
                    rolesLoading={rolesLoading}
                    rolesHasMore={rolesHasMore}
                    rolesObserverRef={rolesObserverRef}
                    rolesSearchQuery={rolesSearchQuery}
                    setRolesSearchQuery={setRolesSearchQuery}
                    refetch={refetch}
                />
                <Menu items={items} trigger={['click']}>
                    <MenuTriggerButton
                        type="button"
                        onClick={(e) => e.preventDefault()}
                        data-testid={`userItem-${shouldShowPasswordReset ? 'native' : 'non-native'}`}
                    >
                        <MenuIcon fontSize={20} />
                    </MenuTriggerButton>
                </Menu>
            </ButtonGroup>
            <ViewResetTokenModal
                open={isViewingResetToken}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingResetToken(false)}
            />
        </ListItem>
    );
}
