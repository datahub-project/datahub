import { List, Space, Typography } from 'antd';
import React from 'react';
import { DownstreamLineage, EntityType, UpstreamLineage } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    upstreamLineage?: UpstreamLineage | null;
    downstreamLineage?: DownstreamLineage | null;
};

export default function Lineage({ upstreamLineage, downstreamLineage }: Props) {
    const entityRegistry = useEntityRegistry();
    const upstreamEntities = upstreamLineage?.upstreams.map((upstream) => upstream.dataset);
    const downstreamEntities = downstreamLineage?.downstreams.map((downstream) => downstream.dataset);

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <List
                bordered
                dataSource={upstreamEntities}
                header={<Typography.Title level={3}>Upstream</Typography.Title>}
                renderItem={(item) => (
                    <List.Item>{entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item)}</List.Item>
                )}
            />
            <List
                bordered
                dataSource={downstreamEntities}
                header={<Typography.Title level={3}>Downstream</Typography.Title>}
                renderItem={(item) => (
                    <List.Item>{entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item)}</List.Item>
                )}
            />
        </Space>
    );
}
