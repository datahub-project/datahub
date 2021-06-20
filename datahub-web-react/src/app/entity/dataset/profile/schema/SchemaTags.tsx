import { Space, Badge } from 'antd';
import React from 'react';

type Tag = {
    name: string;
    value?: string;
    color: string;
    category: string;
    descriptor?: boolean;
};

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
