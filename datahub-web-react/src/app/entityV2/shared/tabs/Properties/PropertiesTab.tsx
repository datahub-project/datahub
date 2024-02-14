import React from 'react';
import { Typography, Table, Empty } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';
import { useEntityData } from '../../EntityContext';

const NameText = styled(Typography.Text)`
    font-family: 'Roboto Mono', monospace;
    font-weight: 600;
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
`;

const ValueText = styled(Typography.Text)`
    font-family: 'Roboto Mono', monospace;
    font-weight: 400;
    font-size: 12px;
    color: ${ANTD_GRAY[8]};
`;

const StyledTable = styled(Table)`
    &&& .ant-table-cell-with-append {
        padding: 4px;
    }
    &&& .ant-table-tbody > tr > td {
        border: none;
    }
` as typeof Table;

const EmptyText = styled(Empty)`
    font-size: 14px;
`;

export const PropertiesTab = () => {
    const { entityData } = useEntityData();

    const propertyTableColumns = [
        {
            width: 210,
            title: 'Name',
            dataIndex: 'key',
            sorter: (a, b) => a?.key.localeCompare(b?.key || '') || 0,
            defaultSortOrder: 'ascend',
            render: (name: string) => <NameText>{name}</NameText>,
        },
        {
            title: 'Value',
            dataIndex: 'value',
            render: (value: string) => <ValueText>{value}</ValueText>,
        },
    ];

    return (
        <StyledTable
            pagination={false}
            // typescript is complaining that default sort order is not a valid column field- overriding this here
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            columns={propertyTableColumns}
            dataSource={entityData?.customProperties || undefined}
            locale={{
                emptyText: <EmptyText description="No properties found" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
            }}
        />
    );
};
