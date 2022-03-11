import React from 'react';
import styled from 'styled-components';
import { Button, List, message, Modal, Tag, Tooltip, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { DeleteOutlined } from '@ant-design/icons';
import { CorpUser, CorpUserStatus, EntityType } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useRemoveUserMutation } from '../../../graphql/user.generated';
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
                <Button onClick={() => handleRemoveUser(user.urn)} type="text" shape="circle" danger>
                    <DeleteOutlined />
                </Button>
            </ButtonGroup>
        </List.Item>
    );
}
