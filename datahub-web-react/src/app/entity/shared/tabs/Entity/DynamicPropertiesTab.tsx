import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';

import { StyledTable } from '../../components/styled/StyledTable';
import { ANTD_GRAY } from '../../constants';

type Props = {
    payload: string | undefined | null;
};

const NameText = styled(Typography.Text)`
    font-weight: 600;
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
`;

function isValidHttpUrl(string) {
    let url;

    try {
        url = new URL(string);
    } catch (_) {
        return false;
    }

    return url.protocol === 'http:' || url.protocol === 'https:';
}

const TableValueRenderer = ({ value }: { value: any }) => {
    if (typeof value === 'boolean') {
        return <span>{String(value)}</span>;
    }
    if (typeof value === 'string') {
        if (isValidHttpUrl(value)) {
            return <a href={value}>{value}</a>;
        }
        return <span>{value}</span>;
    }
    if (typeof value === 'number') {
        return <span>{value}</span>;
    }
    return null;
};

export default function DynamicTabularTab({ payload: rawPayload }: Props) {
    const payload = JSON.parse(rawPayload || '{}');
    const aspectData = payload[Object.keys(payload)[0]];
    const transformedRowData = Object.keys(aspectData).map((key) => ({ key, value: aspectData[key] }));

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
            render: (value: string) => <TableValueRenderer value={value} />,
        },
    ];

    return (
        <StyledTable
            pagination={false}
            // typescript is complaining that default sort order is not a valid column field- overriding this here
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            columns={propertyTableColumns}
            dataSource={transformedRowData}
        />
    );
}
