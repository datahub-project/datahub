import { Row, Table, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { ColumnsType, ColumnType } from 'antd/lib/table';
import React, { useMemo } from 'react';
import { DatasetProfile } from '../../../../../../types.generated';
import { Highlight } from '../../../../../analyticsDashboard/components/Highlight';
import StatsSection from '../StatsSection';

const ColumnStatsTable = styled(Table)`
    margin-top: 24px;
`;

const isPresent = (val: any) => {
    return val !== undefined && val !== null;
};

const decimalToPercentStr = (decimal: number, precision: number): string => {
    return `${(decimal * 100).toFixed(precision)}%`;
};

export type Props = {
    profile: DatasetProfile;
};

export default function DataProfileView({ profile }: Props) {
    const columnStatsTableData = useMemo(
        () =>
            profile.fieldProfiles?.map((doc) => ({
                name: doc.fieldPath,
                min: doc.min,
                max: doc.max,
                mean: doc.mean,
                median: doc.median,
                stdev: doc.stdev,
                nullCount: isPresent(doc.nullCount) && doc.nullCount!.toString(),
                nullPercentage: isPresent(doc.nullProportion) && decimalToPercentStr(doc.nullProportion!, 2),
                distinctCount: isPresent(doc.uniqueCount) && doc.uniqueCount!.toString(),
                distinctPercentage: isPresent(doc.uniqueProportion) && decimalToPercentStr(doc.uniqueProportion!, 2),
                sampleValues: doc.sampleValues,
            })) || [],
        [profile],
    );

    /**
     * Returns a placeholder value to show in the column data table when data is null.
     */
    const unknownValue = () => {
        return <Typography.Text style={{ color: '#B8B8B8' }}>unknown</Typography.Text>;
    };

    /**
     * Computes a set of the object keys across all items in a given array.
     */
    const getItemKeySet = (items: Array<any>) => {
        const keySet = new Set<string>();
        items.forEach((item) => {
            Object.keys(item).forEach((key) => {
                keySet.add(key);
            });
        });
        return keySet;
    };

    /**
     * Dynamically builds column stat table columns based on the fields present in the dataset profile data.
     */
    const buildColumnStatsColumns = (tableData: Array<any>) => {
        // Optional columns. Defines how to render a column given a value exists somewhere in the profile.
        const optionalColumns: ColumnsType<any> = [
            {
                title: 'Min',
                dataIndex: 'min',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Max',
                dataIndex: 'max',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Mean',
                dataIndex: 'mean',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Median',
                dataIndex: 'median',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Null Count',
                dataIndex: 'nullCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Null %',
                dataIndex: 'nullPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Distinct Count',
                dataIndex: 'distinctCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Distinct %',
                dataIndex: 'distinctPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Std. Dev',
                dataIndex: 'stdev',
                render: (value) => value || unknownValue(),
            },
            {
                title: 'Sample Values',
                dataIndex: 'sampleValues',
                render: (sampleValues: Array<string>) => {
                    return (
                        (sampleValues &&
                            sampleValues
                                .slice(0, sampleValues.length < 3 ? sampleValues?.length : 3)
                                .map((value) => <Tag key={value}>{value}</Tag>)) ||
                        unknownValue()
                    );
                },
            },
        ];

        // Name column always required.
        const requiredColumns: ColumnsType<any> = [
            {
                title: 'Name',
                dataIndex: 'name',
            },
        ];

        // Retrieves a set of names of columns that should be shown based on their presence in the data profile.
        const columnsPresent: Set<string> = getItemKeySet(tableData);

        // Compute the final columns to render.
        const columns = [
            ...requiredColumns,
            ...optionalColumns.filter((column: ColumnType<any>) => columnsPresent.has(column.dataIndex as string)),
        ];

        // TODO: Support Quantiles && Distinct Value Frequencies.
        return columns;
    };

    const columnStatsColumns = buildColumnStatsColumns(columnStatsTableData);

    const rowCount = (isPresent(profile?.rowCount) ? profile?.rowCount : -1) as number;
    const rowCountTitle = (rowCount >= 0 && 'Rows') || 'Row Count Unknown';

    const columnCount = (isPresent(profile?.columnCount) ? profile?.columnCount : -1) as number;
    const columnCountTitle = (columnCount >= 0 && 'Columns') || 'Column Count Unknown';

    return (
        <>
            <StatsSection title="Table Stats">
                <Row align="top" justify="start">
                    <Highlight highlight={{ value: rowCount, title: rowCountTitle, body: '' }} />
                    <Highlight highlight={{ value: columnCount, title: columnCountTitle, body: '' }} />
                </Row>
            </StatsSection>
            <StatsSection title="Column Stats">
                <ColumnStatsTable
                    bordered
                    pagination={false}
                    columns={columnStatsColumns}
                    dataSource={columnStatsTableData}
                    // TODO: this table's types should be cleaned up so `any` is not needed here or in the column definitions
                    rowKey={(record: any) => record.name}
                />
            </StatsSection>
        </>
    );
}
