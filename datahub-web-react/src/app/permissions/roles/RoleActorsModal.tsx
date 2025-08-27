import { DeleteOutlined, TeamOutlined, UserOutlined } from '@ant-design/icons';
import { Button, Empty, Modal, Popconfirm, Typography, message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { CustomAvatar } from '@app/shared/avatar';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { CorpGroup, CorpUser, DataHubRole } from '@types';

const { Title, Text } = Typography;

const StyledModal = styled(Modal)`
    .ant-modal-content {
        border-radius: 8px;
    }
    .ant-modal-header {
        border-bottom: 1px solid ${ANTD_GRAY[4.5]};
        padding: 16px 24px;
    }
    .ant-modal-body {
        padding: 24px;
    }
`;

const ActorContainer = styled.div`
    margin: 16px 0;
`;

const ActorItem = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border: 1px solid ${ANTD_GRAY[4.5]};
    border-radius: 6px;
    margin-bottom: 8px;
    background-color: ${ANTD_GRAY[2]};

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }
`;

const ActorInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

interface Props {
    /** Whether the modal is visible */
    visible: boolean;
    /** The role whose actors are being managed */
    role: DataHubRole | null;
    /** Callback when modal is closed */
    onClose: () => void;
    /** Optional callback when actors are successfully updated */
    onSuccess?: () => void;
}

/**
 * Modal for viewing and managing actors assigned to a role
 */
export default function RoleActorsModal({ visible, role, onClose, onSuccess }: Props) {
    const [batchAssignRoleMutation, { loading }] = useBatchAssignRoleMutation();

    if (!role) return null;

    const castedRole = role as any;
    const users = castedRole?.users?.relationships?.map((relationship) => relationship.entity as CorpUser) || [];
    const groups = castedRole?.groups?.relationships?.map((relationship) => relationship.entity as CorpGroup) || [];

    /** Removes an actor from the current role */
    const handleRemoveActor = async (actorUrn: string, actorName: string, actorType: 'user' | 'group') => {
        try {
            await batchAssignRoleMutation({
                variables: {
                    input: {
                        roleUrn: null, // null removes the role
                        actors: [actorUrn],
                    },
                },
            });

            message.success(`Successfully removed ${actorType} "${actorName}" from role "${role.name}"`);
            onSuccess?.(); // Refresh data
        } catch (error) {
            message.error(`Failed to remove ${actorType} "${actorName}". Please try again.`);
        }
    };

    /** Gets display name for an actor */
    const getActorDisplayName = (actor: CorpUser | CorpGroup): string => {
        if ('username' in actor) {
            // CorpUser
            return actor.info?.displayName || actor.username || 'Unknown User';
        }
        // CorpGroup
        return actor.info?.displayName || actor.name || 'Unknown Group';
    };

    /** Renders an individual actor item with remove functionality */
    const renderActorItem = (actor: CorpUser | CorpGroup, type: 'user' | 'group') => {
        const displayName = getActorDisplayName(actor);
        const isUser = type === 'user';

        return (
            <ActorItem key={actor.urn}>
                <ActorInfo>
                    <CustomAvatar
                        photoUrl={actor.editableProperties?.pictureLink || undefined}
                        name={displayName}
                        size={28}
                    />
                    <div>
                        <Text strong>{displayName}</Text>
                        {isUser ? (
                            <Text type="secondary" style={{ display: 'block', fontSize: '12px' }}>
                                <UserOutlined /> User • {(actor as CorpUser).username}
                            </Text>
                        ) : (
                            <Text type="secondary" style={{ display: 'block', fontSize: '12px' }}>
                                <TeamOutlined /> Group
                            </Text>
                        )}
                    </div>
                </ActorInfo>

                <Popconfirm
                    title={`Remove ${type} from role?`}
                    onConfirm={() => handleRemoveActor(actor.urn, displayName, type)}
                    okText="Remove"
                    cancelText="Cancel"
                    okButtonProps={{ danger: true }}
                >
                    <Button
                        type="text"
                        size="small"
                        danger
                        icon={<DeleteOutlined />}
                        loading={loading}
                        title={`Remove ${displayName} from role`}
                    />
                </Popconfirm>
            </ActorItem>
        );
    };

    const totalActors = users.length + groups.length;
    const hasNoActors = totalActors === 0;

    return (
        <StyledModal
            title={<Title level={4}>Manage Actors for &quot;{role.name}&quot;</Title>}
            open={visible}
            onCancel={onClose}
            footer={[
                <Button key="close" onClick={onClose}>
                    Close
                </Button>,
            ]}
            width={600}
        >
            {hasNoActors ? (
                <Empty
                    description="No users or groups are assigned to this role"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
            ) : (
                <div>
                    <Text type="secondary">
                        {totalActors} actor{totalActors !== 1 ? 's' : ''} assigned to this role
                    </Text>

                    <ActorContainer>
                        {users.map((user) => renderActorItem(user, 'user'))}
                        {groups.map((group) => renderActorItem(group, 'group'))}
                    </ActorContainer>
                </div>
            )}
        </StyledModal>
    );
}
