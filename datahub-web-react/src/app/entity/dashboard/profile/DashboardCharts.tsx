import { List, Space, Typography } from 'antd';
import React from 'react';
import { Chart, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    charts: Array<Chart>;
};

export default function DashboardCharts({ charts }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <List
                bordered
                dataSource={charts}
                header={<Typography.Title level={3}>Charts</Typography.Title>}
                renderItem={(item) => (
                    <List.Item>{entityRegistry.renderPreview(EntityType.Chart, PreviewType.PREVIEW, item)}</List.Item>
                )}
            />
        </Space>
    );
}
