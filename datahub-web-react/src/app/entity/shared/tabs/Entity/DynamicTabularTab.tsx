import React from 'react';
import { StyledTable } from '../../components/styled/StyledTable';

type Props = {
    payload: string | undefined | null;
};

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
    const rowData = aspectData[Object.keys(aspectData)[0]];
    const columns = Object.keys(rowData[0]).map((columnName) => ({
        title: columnName,
        dataIndex: columnName,
        render: (value: any) => <TableValueRenderer value={value} />,
    }));

    return (
        <StyledTable
            pagination={false}
            // typescript is complaining that default sort order is not a valid column field- overriding this here
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            columns={columns}
            dataSource={rowData}
        />
    );
}
