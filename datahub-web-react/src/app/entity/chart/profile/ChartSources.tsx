import { List, Typography } from 'antd';
import React from 'react';
import { Dataset, EntityType } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { PreviewType } from '../../Entity';

const styles = {
    list: { marginTop: '12px', padding: '16px 32px' },
    item: { paddingTop: '20px' },
};

export type Props = {
    datasets: Array<Dataset>;
};

export default function ChartSources({ datasets }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <List
            style={styles.list}
            bordered
            dataSource={datasets}
            header={<Typography.Title level={3}>Source Datasets</Typography.Title>}
            renderItem={(item) => (
                <List.Item style={styles.item}>
                    {entityRegistry.renderPreview(EntityType.Dataset, PreviewType.PREVIEW, item)}
                </List.Item>
            )}
        />
    );
}
