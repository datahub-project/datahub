import { downgradeV2FieldPath } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import { Typography } from 'antd';
import { ColumnsType, ColumnType } from 'antd/lib/table';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { DatasetFieldProfile, Maybe, PartitionSpec, PartitionType } from '../../../../../../../types.generated';
import { StyledTable } from '../../../../components/styled/StyledTable';
import { ANTD_GRAY } from '../../../../constants';
import SampleValueTag from './SampleValueTag';

type Props = {
    columnStats: Array<DatasetFieldProfile>;
    partitionSpec?: Maybe<PartitionSpec>;
};

const StatSection = styled.div`
    padding: 20px 20px;
`;

const NameText = styled(Typography.Text)`
    font-family: 'Roboto Mono', monospace;
    font-weight: 600;
    font-size: 12px;
    color: ${ANTD_GRAY[9]};
`;

const isPresent = (val?: string | number | null): val is string | number => {
    return val !== undefined && val !== null;
};

const decimalToPercentStr = (decimal: number, precision: number): string => {
    return `${(decimal * 100).toFixed(precision)}%`;
};

export default function ColumnStats({ columnStats, partitionSpec }: Props) {
    const columnStatsTableData = useMemo(
        () =>
            columnStats.map((doc) => ({
                name: downgradeV2FieldPath(doc.fieldPath),
                min: doc.min,
                max: doc.max,
                mean: doc.mean,
                median: doc.median,
                stdev: doc.stdev,
                nullCount: isPresent(doc.nullCount) && doc.nullCount.toString(),
                nullPercentage: isPresent(doc.nullProportion) && decimalToPercentStr(doc.nullProportion, 2),
                distinctCount: isPresent(doc.uniqueCount) && doc.uniqueCount.toString(),
                distinctPercentage: isPresent(doc.uniqueProportion) && decimalToPercentStr(doc.uniqueProportion, 2),
                sampleValues: doc.sampleValues,
            })) || [],
        [columnStats],
    );

    // we assume if no partition spec is provided, it's a full table
    const isPartitioned = partitionSpec && partitionSpec.type !== PartitionType.FullTable;

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
                                .map((value) => <SampleValueTag value={value} />)) ||
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
                render: (value) => <NameText>{value}</NameText>,
                ellipsis: true,
            },
        ];

        // Retrieves a set of names of columns that should be shown based on their presence in the data profile.
        const columnsPresent: Set<string> = getItemKeySet(tableData);

        // Compute the final columns to render.
        const columns = [
            ...requiredColumns,
            ...optionalColumns.filter((column: ColumnType<any>) => columnsPresent.has(column.dataIndex as string)),
        ];

        return columns;
    };

    const columnStatsColumns = buildColumnStatsColumns(columnStatsTableData);

    return (
        <StatSection>
            <Typography.Title level={5}>
                {isPartitioned ? `Column Stats for Partition ${partitionSpec.partition}` : 'Column Stats'}
            </Typography.Title>
            <StyledTable pagination={false} columns={columnStatsColumns} dataSource={columnStatsTableData} />
        </StatSection>
    );
}
