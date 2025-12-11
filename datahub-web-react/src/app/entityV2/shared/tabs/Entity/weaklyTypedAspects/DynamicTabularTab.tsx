/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { StyledTable } from '@app/entityV2/shared/components/styled/StyledTable';
import TableValueElement from '@app/entityV2/shared/tabs/Entity/weaklyTypedAspects/TableValueElement';

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
