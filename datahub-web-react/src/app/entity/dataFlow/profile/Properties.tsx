import { Space, Table, Typography } from 'antd';
import React from 'react';
import { StringMapEntry } from '../../../../types.generated';

export type Props = {
    properties: StringMapEntry[];
};

export default function Properties({ properties }: Props) {
    const propertyTableColumns = [
        {
            title: 'Name',
            dataIndex: 'key',
        },
        {
            title: 'Value',
            dataIndex: 'value',
        },
    ];

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <Typography.Title level={3}>Properties</Typography.Title>
            <Table pagination={false} columns={propertyTableColumns} dataSource={properties} />
        </Space>
    );
}
