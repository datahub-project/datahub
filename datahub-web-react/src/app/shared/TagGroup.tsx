import { Space, Tag } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, GlobalTags } from '../../types.generated';

type Props = {
    globalTags?: GlobalTags | null;
};

export default function TagGroup({ globalTags }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <Space size="small">
            {globalTags?.tags?.map((tag) => (
                <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                    <Tag color="blue">{tag.tag.name}</Tag>
                </Link>
            ))}
        </Space>
    );
}
