import { DeleteOutlined, MoreOutlined, UnlockOutlined } from '@ant-design/icons';
import { Dropdown, List, Tag, Tooltip, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import useDeleteEntity from '@app/entity/shared/EntityDropdown/useDeleteEntity';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entity/shared/constants';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import SelectRole from '@app/identity/user/SelectRole';
import ViewResetTokenModal from '@app/identity/user/ViewResetTokenModal';
import { USERS_ASSIGN_ROLE_ID } from '@app/onboarding/config/UsersOnboardingConfig';
import CustomAvatar from '@app/shared/avatar/CustomAvatar';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, CorpUserStatus, DataHubRole, EntityType } from '@types';

type Props = {
    user: CorpUser;
    canManageUserCredentials: boolean;
    selectRoleOptions: Array<DataHubRole>;
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

const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

export default function UserListItem({ user, canManageUserCredentials, selectRoleOptions, onDelete, refetch }: Props) {
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
                return 'The user has logged in.';
            default:
                return '';
        }
    };

    const getUserStatusColor = (userStatus: CorpUserStatus) => {
        switch (userStatus) {
            case CorpUserStatus.Active:
                return REDESIGN_COLORS.BLUE;
            default:
                return ANTD_GRAY[6];
        }
    };

    const userStatus = user.status; // Support case where the user status is undefined.
    const userStatusToolTip = userStatus && getUserStatusToolTip(userStatus);
    const userStatusColor = userStatus && getUserStatusColor(userStatus);

    const items = [
        {
            key: 'reset',
            label: (
                <MenuItemStyle
                    disabled={!shouldShowPasswordReset}
                    onClick={() => setIsViewingResetToken(true)}
                    data-testid="reset-menu-item"
                >
                    <UnlockOutlined data-testid="resetButton" /> &nbsp; Reset user password
                </MenuItemStyle>
            ),
        },
        {
            key: 'delete',
            label: (
                <MenuItemStyle onClick={onDeleteEntity}>
                    <DeleteOutlined /> &nbsp;Delete
                </MenuItemStyle>
            ),
        },
    ];

    return (
        <List.Item>
            <UserItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}>
                    <UserHeaderContainer>
                        <CustomAvatar
                            size={32}
                            name={displayName}
                            photoUrl={user.editableProperties?.pictureLink || undefined}
                        />
                        <div style={{ marginLeft: 16, marginRight: 20 }}>
                            <div>
                                <Typography.Text>{displayName}</Typography.Text>
                            </div>
                            <div data-testid={`email-${shouldShowPasswordReset ? 'native' : 'non-native'}`}>
                                <Typography.Text type="secondary">{user.username}</Typography.Text>
                            </div>
                        </div>
                        {userStatus && (
                            <Tooltip overlay={userStatusToolTip}>
                                <Tag color={userStatusColor || ANTD_GRAY[6]}>{userStatus}</Tag>
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
                    refetch={refetch}
                />
                <Dropdown trigger={['click']} menu={{ items }}>
                    <MenuIcon
                        fontSize={20}
                        data-testid={`userItem-${shouldShowPasswordReset ? 'native' : 'non-native'}`}
                    />
                </Dropdown>
            </ButtonGroup>
            <ViewResetTokenModal
                open={isViewingResetToken}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingResetToken(false)}
            />
        </List.Item>
    );
}
