import React from 'react';
import { Space, Table, Typography } from 'antd';
import { MlHyperParam, MlMetric, MlModel } from '../../../../types.generated';

export type Props = {
    model?: MlModel;
};

export default function MLModelSummary({ model }: Props) {
    const propertyTableColumns = [
        {
            title: 'Name',
            dataIndex: 'name',
            width: 450,
        },
        {
            title: 'Value',
            dataIndex: 'value',
        },
    ];

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <Typography.Title level={3}>Training Metrics</Typography.Title>
            <Table
                pagination={false}
                columns={propertyTableColumns}
                dataSource={model?.properties?.trainingMetrics as MlMetric[]}
            />
            <Typography.Title level={3}>Hyper Parameters</Typography.Title>
            <Table
                pagination={false}
                columns={propertyTableColumns}
                dataSource={model?.properties?.hyperParams as MlHyperParam[]}
            />
        </Space>
    );
}
