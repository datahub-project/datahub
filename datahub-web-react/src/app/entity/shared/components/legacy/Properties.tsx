import { Space, Table, Typography } from 'antd';
import React from 'react';
import { ColumnsType } from 'antd/es/table';
import { useTranslation } from 'react-i18next';
import { StringMapEntry } from '../../../../../types.generated';

export type Props = {
    properties: StringMapEntry[];
};

export function Properties({ properties }: Props) {
    const { t } = useTranslation();
    const propertyTableColumns: ColumnsType<StringMapEntry> = [
        {
            title: t('common.name'),
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
            <Typography.Title level={3}>{t('common.properties')}</Typography.Title>
            <Table pagination={false} columns={propertyTableColumns} dataSource={properties} />
        </Space>
    );
}
