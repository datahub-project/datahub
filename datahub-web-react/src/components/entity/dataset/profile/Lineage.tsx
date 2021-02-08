import { List, Space, Typography } from 'antd';
import React from 'react';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    upstreamEntities: Dataset[];
    downstreamEntities: Dataset[];
};

export default function Lineage({ upstreamEntities, downstreamEntities }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <List
                bordered
                dataSource={upstreamEntities}
                header={<Typography.Title level={3}>Upstream</Typography.Title>}
                renderItem={(item) => {
                    return entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item);
                }}
            />
            <List
                bordered
                dataSource={downstreamEntities}
                header={<Typography.Title level={3}>Downstream</Typography.Title>}
                renderItem={(item) => {
                    return entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item);
                }}
            />
        </Space>
    );
}
