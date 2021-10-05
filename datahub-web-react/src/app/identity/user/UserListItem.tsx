import React from 'react';
import styled from 'styled-components';
import { Button, List, message, Modal, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { DeleteOutlined } from '@ant-design/icons';
import { CorpUser, CorpUserStatus, EntityType } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useRemoveUserMutation, useUpdateUserStatusMutation } from '../../../graphql/user.generated';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';

type Props = {
    user: CorpUser;
    onDelete?: () => void;
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

export default function UserListItem({ user, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpUser, user);

    const [removeUserMutation] = useRemoveUserMutation();
    const [updateUserStatusMutation] = useUpdateUserStatusMutation();

    const onRemoveUser = async (urn: string) => {
        try {
            await removeUserMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed user.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove user: \n ${e.message || ''}`, duration: 3 });
            }
        }
        onDelete?.();
    };

    const onChangeUserStatus = async (urn: string, status: CorpUserStatus) => {
        try {
            await updateUserStatusMutation({
                variables: { urn, status },
            });
            message.success({ content: 'Updated user status.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update user: \n ${e.message || ''}`, duration: 3 });
            }
        }
        onDelete?.();
    };

    const handleRemoveUser = (urn: string) => {
        Modal.confirm({
            title: `Confirm User Removal`,
            content: `Are you sure you want to remove this user? Note that if you have SSO auto provisioning enabled, this user will be created when they log in again.`,
            onOk() {
                onRemoveUser(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const handleDeactivateUser = (urn: string) => {
        Modal.confirm({
            title: `Confirm User Deactivation`,
            content: `Are you sure you want to deactivate this user? This will prevent them from logging in.`,
            onOk() {
                onChangeUserStatus(urn, CorpUserStatus.Deactivated);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const handleActivateUser = (urn: string) => {
        Modal.confirm({
            title: `Confirm User Activation`,
            content: `Are you sure you want to activate this user? This will allow them to log in.`,
            onOk() {
                onChangeUserStatus(urn, CorpUserStatus.Active);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const resolvedUserStatus = user.status || CorpUserStatus.Provisioned; // Support case where the user status is undefined.
    let userStatusColor;
    let userStatusToolTip;
    switch (resolvedUserStatus) {
        case CorpUserStatus.Provisioned:
            /* eslint-disable-next-line prefer-destructuring */
            userStatusColor = ANTD_GRAY[7];
            userStatusToolTip = 'The user has been created, but has not yet logged in.';
            break;
        case CorpUserStatus.Active:
            userStatusColor = REDESIGN_COLORS.BLUE;
            userStatusToolTip = 'The user has logged in.';
            break;
        case CorpUserStatus.Deactivated:
            userStatusColor = 'red';
            userStatusToolTip = 'The user has logged in previously, but is no longer allowed to log in.';
            break;
        default:
            /* eslint-disable-next-line prefer-destructuring */
            userStatusColor = ANTD_GRAY[1];
    }

    return (
        <List.Item>
            <UserItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.CorpUser, user.urn)}>
                    <UserHeaderContainer>
                        <CustomAvatar size={32} name={displayName} />
                        <div style={{ marginLeft: 16, marginRight: 20 }}>
                            <div>
                                <Typography.Text>{displayName}</Typography.Text>
                            </div>
                            <div>
                                <Typography.Text type="secondary">{user.username}</Typography.Text>
                            </div>
                        </div>
                        <Tooltip overlay={userStatusToolTip}>
                            <Tag color={userStatusColor}>{resolvedUserStatus}</Tag>
                        </Tooltip>
                    </UserHeaderContainer>
                </Link>
            </UserItemContainer>
            <ButtonGroup>
                {resolvedUserStatus === CorpUserStatus.Active && (
                    <Button onClick={() => handleDeactivateUser(user.urn)}>Deactivate User</Button>
                )}
                {resolvedUserStatus === CorpUserStatus.Deactivated && (
                    <Button onClick={() => handleActivateUser(user.urn)}>Activate User</Button>
                )}
                <Button onClick={() => handleRemoveUser(user.urn)} type="text" shape="circle" danger>
                    <DeleteOutlined />
                </Button>
            </ButtonGroup>
        </List.Item>
    );
}
