import React from 'react';
import { StyledTable } from '../../../components/styled/StyledTable';
import TableValueElement from './TableValueElement';

type Props = {
    payload: string | undefined | null;
    tableKey: string | undefined | null;
};

export default function DynamicTabularTab({ payload: rawPayload, tableKey }: Props) {
    const aspectData = JSON.parse(rawPayload || '{}');
    const rowData = tableKey ? aspectData[tableKey] : aspectData[Object.keys(aspectData)[0]];
    const columns = Object.keys(rowData[0]).map((columnName) => ({
        title: columnName,
        dataIndex: columnName,
        render: (value: any) => <TableValueElement value={value} />,
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
