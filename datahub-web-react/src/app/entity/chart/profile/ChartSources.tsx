import { List, Space, Typography } from 'antd';
import React from 'react';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

export type Props = {
    datasets: Array<Dataset>;
};

export default function ChartSources({ datasets }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <List
                bordered
                dataSource={datasets}
                header={<Typography.Title level={3}>Source Datasets</Typography.Title>}
                renderItem={(item) => (
                    <List.Item>{entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item)}</List.Item>
                )}
            />
        </Space>
    );
}
