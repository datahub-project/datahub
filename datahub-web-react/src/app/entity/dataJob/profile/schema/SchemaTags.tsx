import { Space, Badge } from 'antd';
import React from 'react';
import { Tag } from '../stories/sampleSchema';

type Props = {
    tags: Tag[];
};

export default function SchemaTags({ tags }: Props) {
    return (
        <Space>
            {tags.map((tag) => (
                <Badge
                    key={`${tag.name}-${tag.value}`}
                    style={{ color: 'black', backgroundColor: tag.color, fontSize: '10px' }}
                    count={tag.value || tag.name}
                />
            ))}
        </Space>
    );
}
