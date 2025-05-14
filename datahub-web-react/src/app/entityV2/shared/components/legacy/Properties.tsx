import { Space, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import React from 'react';

import { StringMapEntry } from '@types';

export type Props = {
    properties: StringMapEntry[];
};

export function Properties({ properties }: Props) {
    const propertyTableColumns: ColumnsType<StringMapEntry> = [
        {
            title: 'Name',
            dataIndex: 'key',
            sorter: (a, b) => a.key.localeCompare(b.key),
            defaultSortOrder: 'ascend',
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
