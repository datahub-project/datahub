import React from 'react';
import { Space, Table, Typography } from 'antd';
import Link from 'antd/lib/typography/Link';
import { ColumnsType } from 'antd/es/table';
import styled from 'styled-components';

import { EntityType, MlModelGroup } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useBaseEntity } from '../../../entity/shared/EntityContext';
import { GetMlModelQuery } from '../../../../graphql/mlModel.generated';

const TabContent = styled.div`
    padding: 16px;
`;

export default function MLModelGroupsTab() {
    const baseEntity = useBaseEntity<GetMlModelQuery>();
    const model = baseEntity?.mlModel;

    const entityRegistry = useEntityRegistry();

    const propertyTableColumns: ColumnsType<MlModelGroup> = [
        {
            title: 'Group',
            dataIndex: 'name',
            render: (name, record) => {
                return (
                    <Link href={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, record.urn)}>
                        {record.properties?.name || name}
                    </Link>
                );
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
        },
    ];

    return (
        <TabContent>
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Typography.Title level={3}>Groups</Typography.Title>
                <Table
                    pagination={false}
                    columns={propertyTableColumns}
                    dataSource={model?.properties?.groups as MlModelGroup[]}
                />
            </Space>
        </TabContent>
    );
}
