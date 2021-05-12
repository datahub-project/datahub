import React from 'react';

import { Table } from 'antd';
import styled from 'styled-components';

import { TableChart as TableChartType } from '../../../types.generated';

type Props = {
    chartData: TableChartType;
};

const StyledTable = styled(Table)`
    padding-top: 16px;
    width: 100%;
`;

export const TableChart = ({ chartData }: Props) => {
    const columns = chartData.columns.map((column) => ({
        title: column,
        key: column,
        dataIndex: column,
    }));
    const tableData = chartData.rows.map((row) =>
        row.values.reduce((acc, value, i) => ({ ...acc, [chartData.columns[i]]: value }), {}),
    );
    return <StyledTable columns={columns} dataSource={tableData} pagination={false} size="small" />;
};
