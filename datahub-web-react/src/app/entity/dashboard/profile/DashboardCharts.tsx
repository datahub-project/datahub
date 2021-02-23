import { List, Typography } from 'antd';
import React from 'react';
import { Chart, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const styles = {
    list: { marginTop: '12px', padding: '16px 32px' },
    item: { paddingTop: '20px' },
};

export type Props = {
    charts: Array<Chart>;
};

export default function DashboardCharts({ charts }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <List
            style={styles.list}
            bordered
            dataSource={charts}
            header={<Typography.Title level={3}>Charts</Typography.Title>}
            renderItem={(item) => (
                <List.Item style={styles.item}>
                    {entityRegistry.renderPreview(EntityType.Chart, PreviewType.PREVIEW, item)}
                </List.Item>
            )}
        />
    );
}
