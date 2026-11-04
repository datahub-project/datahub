import { Heading, Text } from '@components';
import { ColumnType, ColumnsType } from 'antd/lib/table';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { StyledTable } from '@app/entityV2/shared/components/styled/StyledTable';
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

const NameText = styled(Text).attrs({ type: 'span' })`
    font-family: 'Roboto Mono', monospace;
    font-weight: 600;
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    line-height: inherit;
    word-break: break-word;
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

const isPresent = (val?: string | number | null): val is string | number => {
    return val !== undefined && val !== null;
};

const decimalToPercentStr = (decimal: number, precision: number): string => {
    return `${(decimal * 100).toFixed(precision)}%`;
};

export default function ColumnStats({ columnStats, partitionSpec }: Props) {
    const { t } = useTranslation('entity.profile.stats');
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
        return <UnknownValueText>{t('columnStats.unknown')}</UnknownValueText>;
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
                title: t('columnStats.minColumn'),
                dataIndex: 'min',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.maxColumn'),
                dataIndex: 'max',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.meanColumn'),
                dataIndex: 'mean',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.medianColumn'),
                dataIndex: 'median',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.nullCountColumn'),
                dataIndex: 'nullCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.nullPercentColumn'),
                dataIndex: 'nullPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.distinctCountColumn'),
                dataIndex: 'distinctCount',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.distinctPercentColumn'),
                dataIndex: 'distinctPercentage',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.stdDevColumn'),
                dataIndex: 'stdev',
                render: (value) => value || unknownValue(),
            },
            {
                title: t('columnStats.sampleValuesColumn'),
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
                title: t('columnStats.nameColumn'),
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
            <SectionTitleContainer>
                <Heading type="h5" size="lg" weight="semiBold">
                    {t(isPartitioned ? 'columnStats.columnStatsForPartition' : 'columnStats.columnStatsTitle', {
                        partition: partitionSpec?.partition,
                    })}
                </Heading>
            </SectionTitleContainer>
            <StyledTable pagination={false} columns={columnStatsColumns} dataSource={columnStatsTableData} />
        </StatSection>
    );
}
