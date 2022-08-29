import React, { useState } from 'react';
import styled from 'styled-components';
import { Dropdown, List, Menu, Select, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import {
    DeleteOutlined,
    EditOutlined,
    MoreOutlined,
    ReadOutlined,
    SettingOutlined,
    UnlockOutlined,
    UserOutlined,
} from '@ant-design/icons';
import { CorpUser, CorpUserStatus, EntityType, DataHubRole } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';
import ViewResetTokenModal from './ViewResetTokenModal';
import useDeleteEntity from '../../entity/shared/EntityDropdown/useDeleteEntity';
import ViewAssignRoleModal from './ViewAssignRoleModal';
// import UserProfile from '../../entity/user/UserProfile';

type Props = {
    user: CorpUser;
    canManageUserCredentials: boolean;
    roles: Array<DataHubRole>;
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

const RoleSelect = styled(Select)`
    min-width: 100px;
`;

const RoleIcon = styled.span`
    margin-right: 6px;
    font-size: 12px;
`;

const MenuIcon = styled(MoreOutlined)<{ fontSize?: number }>`
    display: flex;
    justify-content: center;
    align-items: center;
    font-size: ${(props) => props.fontSize || '24'}px;
    height: 32px;
    margin-left: 5px;
`;

export default function UserListItem({ user, canManageUserCredentials, roles, onDelete, refetch }: Props) {
    const entityRegistry = useEntityRegistry();
    const [isViewingResetToken, setIsViewingResetToken] = useState(false);
    const [isViewingAssignRole, setIsViewingAssignRole] = useState(false);
    const [roleToAssign, setRoleToAssign] = useState<DataHubRole>();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);
    const isNativeUser: boolean = user.isNativeUser as boolean;
    const shouldShowPasswordReset: boolean = canManageUserCredentials && isNativeUser;
    const userRelationships = user.relationships?.relationships;
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
    const rolesMap: Map<string, DataHubRole> = new Map();
    roles.forEach((role) => {
        rolesMap.set(role.urn, role);
    });

    const mapRoleIcon = (roleName) => {
        let icon = <UserOutlined />;
        if (roleName === 'Admin') {
            icon = <SettingOutlined />;
        }
        if (roleName === 'Editor') {
            icon = <EditOutlined />;
        }
        if (roleName === 'Reader') {
            icon = <ReadOutlined />;
        }
        return <RoleIcon>{icon}</RoleIcon>;
    };

    const selectOptions = () =>
        roles.map((role) => {
            return (
                <Select.Option value={role.urn}>
                    {mapRoleIcon(role.name)}
                    {role.name}
                </Select.Option>
            );
        });

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
            <ButtonGroup>
                <RoleSelect
                    placeholder={
                        <RoleIcon>
                            <UserOutlined />
                            No Role
                        </RoleIcon>
                    }
                    value={userRoleUrn || undefined}
                    onChange={(e) => {
                        const roleUrn: string = e as string;
                        const roleFromMap: DataHubRole = rolesMap.get(roleUrn) as DataHubRole;
                        setRoleToAssign(roleFromMap);
                        setIsViewingAssignRole(true);
                    }}
                >
                    {selectOptions()}
                </RoleSelect>

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
            <ViewAssignRoleModal
                visible={isViewingAssignRole}
                roleToAssign={roleToAssign}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingAssignRole(false)}
                onConfirm={() => {
                    setIsViewingAssignRole(false);
                    setTimeout(function () {
                        refetch?.();
                    }, 3000);
                }}
            />
            <ViewResetTokenModal
                visible={isViewingResetToken}
                userUrn={user.urn}
                username={user.username}
                onClose={() => setIsViewingResetToken(false)}
            />
        </List.Item>
    );
}
