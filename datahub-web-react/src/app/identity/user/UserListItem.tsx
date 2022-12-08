import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { Dropdown, List, Menu, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { DeleteOutlined, MoreOutlined, UnlockOutlined } from '@ant-design/icons';
import { CorpUser, CorpUserStatus, EntityType, DataHubRole } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';
import ViewResetTokenModal from './ViewResetTokenModal';
import useDeleteEntity from '../../entity/shared/EntityDropdown/useDeleteEntity';
import SelectRole from './SelectRole';
import { USERS_ASSIGN_ROLE_ID } from '../../onboarding/config/UsersOnboardingConfig';

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

    const { onDeleteEntity } = useDeleteEntity(user.urn, EntityType.CorpUser, user, onDelete);

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

    return (
        <List.Item data-testid={user.urn}>
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
                            <div>
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
                <Dropdown
                    trigger={['click']}
                    overlay={
                        <Menu>
                            <Menu.Item disabled={!shouldShowPasswordReset} onClick={() => setIsViewingResetToken(true)}>
                                <UnlockOutlined /> &nbsp; Reset user password
                            </Menu.Item>
                            <Menu.Item onClick={onDeleteEntity}>
                                <DeleteOutlined /> &nbsp;Delete
                            </Menu.Item>
                        </Menu>
                    }
                >
                    <MenuIcon fontSize={20} />
                </Dropdown>
            </ButtonGroup>
            <ViewResetTokenModal
                visible={isViewingResetToken}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingResetToken(false)}
            />
        </List.Item>
    );
}
