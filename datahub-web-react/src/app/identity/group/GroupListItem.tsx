import React from 'react';
import styled from 'styled-components';
import { Button, List, message, Modal, Tag, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { DeleteOutlined } from '@ant-design/icons';
import { CorpGroup, EntityType } from '../../../types.generated';
import CustomAvatar from '../../shared/avatar/CustomAvatar';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useRemoveGroupMutation } from '../../../graphql/group.generated';

type Props = {
    group: CorpGroup;
    onDelete?: () => void;
};

const GroupItemContainer = styled.div`
    display: flex;
    justify-content: space-between;
    padding-left: 8px;
    padding-right: 8px;
    width: 100%;
`;

const GroupHeaderContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

export default function GroupListItem({ group, onDelete }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayName = entityRegistry.getDisplayName(EntityType.CorpGroup, group);

    const [removeGroupMutation] = useRemoveGroupMutation();

    const onRemoveGroup = async (urn: string) => {
        try {
            await removeGroupMutation({
                variables: { urn },
            });
            message.success({ content: 'Removed group.', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to remove group: \n ${e.message || ''}`, duration: 3 });
            }
        }
        onDelete?.();
    };

    const handleRemoveGroup = (urn: string) => {
        Modal.confirm({
            title: `Confirm Group Removal`,
            content: `Are you sure you want to remove this group?`,
            onOk() {
                onRemoveGroup(urn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <List.Item>
            <GroupItemContainer>
                <Link to={entityRegistry.getEntityUrl(EntityType.CorpGroup, group.urn)}>
                    <GroupHeaderContainer>
                        <CustomAvatar size={32} name={displayName} />
                        <div style={{ marginLeft: 16, marginRight: 16 }}>
                            <div>
                                <Typography.Text>{displayName}</Typography.Text>
                            </div>
                            <div>
                                <Typography.Text type="secondary">{group.properties?.description}</Typography.Text>
                            </div>
                        </div>
                        <Tag>{(group as any).memberCount?.total || 0} members</Tag>
                    </GroupHeaderContainer>
                </Link>
                <div>
                    <Button onClick={() => handleRemoveGroup(group.urn)} type="text" shape="circle" danger>
                        <DeleteOutlined />
                    </Button>
                </div>
            </GroupItemContainer>
        </List.Item>
    );
}
