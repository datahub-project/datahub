import { Space, Table, Typography } from 'antd';
import { ColumnsType } from 'antd/es/table';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { StringMapEntry } from '@types';

type Props = {
    properties: StringMapEntry[];
};

export function Properties({ properties }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tcl } = useTranslation('common.labels');
    const propertyTableColumns: ColumnsType<StringMapEntry> = [
        {
            title: tcl('name'),
            dataIndex: 'key',
            sorter: (a, b) => a.key.localeCompare(b.key),
            defaultSortOrder: 'ascend',
        },
        {
            title: t('legacy.value'),
            dataIndex: 'value',
        },
    ];

    return (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
            <Typography.Title level={3}>{t('legacy.propertiesTitle')}</Typography.Title>
            <Table pagination={false} columns={propertyTableColumns} dataSource={properties} />
        </Space>
    );
}
