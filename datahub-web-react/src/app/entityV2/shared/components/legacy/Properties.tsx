import { Heading } from '@components';
import { Table } from 'antd';
import { ColumnsType } from 'antd/es/table';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { StringMapEntry } from '@types';

// Matches the antd Space (direction="vertical", size="large") this replaced.
const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    width: 100%;
`;

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
        <Container>
            <Heading type="h3" size="4xl" weight="semiBold">
                {t('legacy.propertiesTitle')}
            </Heading>
            <Table pagination={false} columns={propertyTableColumns} dataSource={properties} />
        </Container>
    );
}
