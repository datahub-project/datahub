import { Space, Table, Typography } from 'antd';
import React from 'react';
import { ColumnsType } from 'antd/es/table';
import { StringMapEntry } from '../../../../../types.generated';

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
