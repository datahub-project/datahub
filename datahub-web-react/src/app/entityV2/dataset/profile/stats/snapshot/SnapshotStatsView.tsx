import { Row, Table, Tag, Typography } from 'antd';
import { ColumnType, ColumnsType } from 'antd/lib/table';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Highlight } from '@app/analyticsDashboard/components/Highlight';
import StatsSection from '@app/entityV2/dataset/profile/stats/StatsSection';

import { DatasetProfile } from '@types';

const ColumnStatsTable = styled(Table)`
    margin-top: 24px;
`;

const isPresent = (val: any) => {
    return val !== undefined && val !== null;
};

const decimalToPercentStr = (decimal: number, precision: number): string => {
    return `${(decimal * 100).toFixed(precision)}%`;
};

type Props = {
    profile: DatasetProfile;
};

export default function DataProfileView({ profile }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tl } = useTranslation('common.labels');
    const theme = useTheme();
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
        return <Typography.Text style={{ color: theme.colors.textTertiary }}>{t('dataset.unknown')}</Typography.Text>;
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
                title: t('dataset.statMin'),
                dataIndex: 'min',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statMax'),
                dataIndex: 'max',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statMean'),
                dataIndex: 'mean',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statMedian'),
                dataIndex: 'median',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statNullCount'),
                dataIndex: 'nullCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statNullPercent'),
                dataIndex: 'nullPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statDistinctCount'),
                dataIndex: 'distinctCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statDistinctPercent'),
                dataIndex: 'distinctPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statStdDev'),
                dataIndex: 'stdev',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('dataset.statSampleValues'),
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
                title: tl('name'),
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
    const rowCountTitle = (rowCount >= 0 && tl('rows')) || t('dataset.rowCountUnknown');

    const columnCount = (isPresent(profile?.columnCount) ? profile?.columnCount : -1) as number;
    const columnCountTitle = (columnCount >= 0 && tl('columns')) || t('dataset.columnCountUnknown');

    return (
        <>
            <StatsSection title={t('dataset.tableStats')}>
                <Row align="top" justify="start">
                    <Highlight highlight={{ value: rowCount, title: rowCountTitle, body: '' }} />
                    <Highlight highlight={{ value: columnCount, title: columnCountTitle, body: '' }} />
                </Row>
            </StatsSection>
            <StatsSection title={t('dataset.columnStats')}>
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
