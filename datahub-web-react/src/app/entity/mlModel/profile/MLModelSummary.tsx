import React from 'react';
import styled from 'styled-components';
import { Space, Table, Typography } from 'antd';

import { MlHyperParam, MlMetric } from '../../../../types.generated';
import { useBaseEntity } from '../../shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';

const TabContent = styled.div`
    padding: 16px;
`;

export default function MLModelSummary() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;

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
        <TabContent>
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
        </TabContent>
    );
}
