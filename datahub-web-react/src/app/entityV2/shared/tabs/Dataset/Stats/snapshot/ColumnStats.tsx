import { Column, Heading, OverflowText, Table, Text } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import SampleValueTag from '@app/entityV2/shared/tabs/Dataset/Stats/snapshot/SampleValueTag';
import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';

import { DatasetFieldProfile, Maybe, PartitionSpec, PartitionType } from '@types';

type Props = {
    columnStats: Array<DatasetFieldProfile>;
    partitionSpec?: Maybe<PartitionSpec>;
};

const StatSection = styled.div`
    padding: 20px 20px;
`;

const NameText = styled(OverflowText)`
    font-family: 'Roboto Mono', monospace;
    font-weight: 600;
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    white-space: nowrap;
`;

const UnknownValueText = styled(Text).attrs({ type: 'span' })`
    color: ${(props) => props.theme.colors.textDisabled};
    font-size: inherit;
    line-height: inherit;
    word-break: break-word;
`;

// Matches antd Typography.Title's bottom margin
const SectionTitleContainer = styled.div`
    margin-bottom: 8px;
    word-break: break-word;
`;

type ColumnStatsRow = {
    name: string;
    min?: Maybe<string>;
    max?: Maybe<string>;
    mean?: Maybe<string>;
    median?: Maybe<string>;
    stdev?: Maybe<string>;
    nullCount: string | false;
    nullPercentage: string | false;
    distinctCount: string | false;
    distinctPercentage: string | false;
    sampleValues?: Maybe<Array<string>>;
};

const isPresent = (val?: string | number | null): val is string | number => {
    return val !== undefined && val !== null;
};

const decimalToPercentStr = (decimal: number, precision: number): string => {
    return `${(decimal * 100).toFixed(precision)}%`;
};

export default function ColumnStats({ columnStats, partitionSpec }: Props) {
    const { t } = useTranslation('entity.profile.stats');
    const columnStatsTableData: ColumnStatsRow[] = useMemo(
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
        return <UnknownValueText>{t('columnStats.unknown')}</UnknownValueText>;
    };

    /**
     * Computes a set of the object keys across all items in a given array.
     */
    const getItemKeySet = (items: Array<ColumnStatsRow>) => {
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
    const buildColumnStatsColumns = (tableData: Array<ColumnStatsRow>) => {
        // Optional columns. Defines how to render a column given a value exists somewhere in the profile.
        const optionalColumns: Column<ColumnStatsRow>[] = [
            {
                title: t('columnStats.minColumn'),
                key: 'min',
                dataIndex: 'min',
                render: (record) => record.min || unknownValue(),
            },
            {
                title: t('columnStats.maxColumn'),
                key: 'max',
                dataIndex: 'max',
                render: (record) => record.max || unknownValue(),
            },
            {
                title: t('columnStats.meanColumn'),
                key: 'mean',
                dataIndex: 'mean',
                render: (record) => record.mean || unknownValue(),
            },
            {
                title: t('columnStats.medianColumn'),
                key: 'median',
                dataIndex: 'median',
                render: (record) => record.median || unknownValue(),
            },
            {
                title: t('columnStats.nullCountColumn'),
                key: 'nullCount',
                dataIndex: 'nullCount',
                render: (record) => record.nullCount || unknownValue(),
            },
            {
                title: t('columnStats.nullPercentColumn'),
                key: 'nullPercentage',
                dataIndex: 'nullPercentage',
                render: (record) => record.nullPercentage || unknownValue(),
            },
            {
                title: t('columnStats.distinctCountColumn'),
                key: 'distinctCount',
                dataIndex: 'distinctCount',
                render: (record) => record.distinctCount || unknownValue(),
            },
            {
                title: t('columnStats.distinctPercentColumn'),
                key: 'distinctPercentage',
                dataIndex: 'distinctPercentage',
                render: (record) => record.distinctPercentage || unknownValue(),
            },
            {
                title: t('columnStats.stdDevColumn'),
                key: 'stdev',
                dataIndex: 'stdev',
                render: (record) => record.stdev || unknownValue(),
            },
            {
                title: t('columnStats.sampleValuesColumn'),
                key: 'sampleValues',
                dataIndex: 'sampleValues',
                render: ({ sampleValues }) =>
                    sampleValues?.slice(0, 3).map((value) => <SampleValueTag key={value} value={value} />) ||
                    unknownValue(),
            },
        ];

        // Name column always required.
        const requiredColumns: Column<ColumnStatsRow>[] = [
            {
                title: t('columnStats.nameColumn'),
                key: 'name',
                dataIndex: 'name',
                maxWidth: '300px',
                render: (record) => <NameText text={record.name} />,
            },
        ];

        // Retrieves a set of names of columns that should be shown based on their presence in the data profile.
        const columnsPresent: Set<string> = getItemKeySet(tableData);

        // Compute the final columns to render.
        const columns = [...requiredColumns, ...optionalColumns.filter((column) => columnsPresent.has(column.key))];

        return columns;
    };

    const columnStatsColumns = buildColumnStatsColumns(columnStatsTableData);

    return (
        <StatSection>
            <SectionTitleContainer>
                <Heading type="h5" size="lg" weight="semiBold">
                    {t(isPartitioned ? 'columnStats.columnStatsForPartition' : 'columnStats.columnStatsTitle', {
                        partition: partitionSpec?.partition,
                    })}
                </Heading>
            </SectionTitleContainer>
            <Table columns={columnStatsColumns} data={columnStatsTableData} rowKey="name" />
        </StatSection>
    );
}
