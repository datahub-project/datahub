import { Skeleton, Modal, Tag, Space } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { ExclamationCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, GlobalTags, GlobalTagsUpdate, TagAssociationUpdate } from '../../types.generated';

type Props = {
    globalTags?: GlobalTags | null;
    canEdit?: boolean;
    updateTags?: (update: GlobalTagsUpdate) => void;
    loading?: boolean;
};

const AddNewTag = styled(Tag)`
    cursor: pointer;
`;

function AddTagModal({ updateTags }) {}

export default function TagGroup({ globalTags, canEdit, updateTags, loading }: Props) {
    const entityRegistry = useEntityRegistry();

    const removeTag = (urnToRemove: string) => {
        const tagToRemove = globalTags?.tags?.find((tag) => tag.tag.urn === urnToRemove);
        const newTags = globalTags?.tags?.filter((tag) => tag.tag.urn !== urnToRemove) as TagAssociationUpdate[];
        Modal.confirm({
            title: `Do you want to remove ${tagToRemove?.tag.name} tag?`,
            icon: <ExclamationCircleOutlined />,
            content: `Are you sure you want to remove the ${tagToRemove?.tag.name} tag?`,
            onOk() {
                updateTags?.({ tags: newTags });
            },
            onCancel() {},
            okText: 'Yes',
        });
    };

    if (loading) {
        return (
            <Space>
                <Skeleton.Button size="small" active shape="round" />
                <Skeleton.Button size="small" active shape="round" />
            </Space>
        );
    }

    return (
        <div>
            {globalTags?.tags?.map((tag) => (
                <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                    <Tag
                        color="blue"
                        closable={canEdit}
                        onClose={(e) => {
                            e.preventDefault();
                            removeTag(tag.tag.urn);
                        }}
                    >
                        {tag.tag.name}
                    </Tag>
                </Link>
            ))}
            {canEdit && (
                <AddNewTag color="success" onClick={() => alert('hello')}>
                    + Add Tag
                </AddNewTag>
            )}
        </div>
    );
}
