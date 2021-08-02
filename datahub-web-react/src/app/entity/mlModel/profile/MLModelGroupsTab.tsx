import React from 'react';
import { Space, Table, Typography } from 'antd';
import Link from 'antd/lib/typography/Link';
import { ColumnsType } from 'antd/es/table';

import { EntityType, MlModel, MlModelGroup } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

export type Props = {
    model?: MlModel;
};

export default function MLModelGroupsTab({ model }: Props) {
    console.log(model?.properties);
    const entityRegistry = useEntityRegistry();

    const propertyTableColumns: ColumnsType<MlModelGroup> = [
        {
            title: 'Group',
            dataIndex: 'name',
            render: (name, record) => {
                return <Link href={entityRegistry.getEntityUrl(EntityType.MlmodelGroup, record.urn)}>{name}</Link>;
            },
        },
        {
            title: 'Description',
            dataIndex: 'description',
        },
    ];

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <Typography.Title level={3}>Groups</Typography.Title>
            <Table
                pagination={false}
                columns={propertyTableColumns}
                dataSource={model?.properties?.groups as MlModelGroup[]}
            />
        </Space>
    );
}
