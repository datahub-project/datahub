import { Typography } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { percentStrToDecimal } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import { getItemKeySet } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { Button, colors } from '@src/alchemy-components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';

const ColumnName = styled(Typography.Text)`
    color: ${colors.gray[600]};
`;

const ViewButton = styled.div`
    display: inline-block;
`;

interface Props {
    tableData: Array<any>;
    searchQuery: string;
    setExpandedDrawerFieldPath: React.Dispatch<React.SetStateAction<string | null>>;
}

export const useGetColumnStatsColumns = ({ tableData, searchQuery, setExpandedDrawerFieldPath }: Props) => {
    // Optional columns. Defines how to render a column given a value exists somewhere in the profile.
    const optionalColumns = [
        {
            title: 'Null Percentage',
            key: 'nullPercentage',
            render: (record) => record.nullPercentage,
            alignment: 'right' as AlignmentOptions,
            sorter: (sourceA, sourceB) => {
                return percentStrToDecimal(sourceA.nullPercentage) - percentStrToDecimal(sourceB.nullPercentage);
            },
        },
        {
            title: 'Unique Values',
            key: 'uniqueValues',
            render: (record) => record.uniqueValues,
            alignment: 'right' as AlignmentOptions,
            sorter: (sourceA, sourceB) => {
                return sourceA.uniqueValues - sourceB.uniqueValues;
            },
        },
        {
            title: 'Min',
            key: 'min',
            render: (record) => record.min,
            alignment: 'right' as AlignmentOptions,
            sorter: (sourceA, sourceB) => {
                return sourceA.min - sourceB.min;
            },
        },
        {
            title: 'Max',
            key: 'max',
            render: (record) => record.max,
            alignment: 'right' as AlignmentOptions,
            sorter: (sourceA, sourceB) => {
                return sourceA.max - sourceB.max;
            },
        },
    ];

    // Column and type columns always required.
    const requiredColumns = [
        {
            title: 'Column',
            key: 'column',
            render: (record) => (
                <ColumnName ellipsis={{ tooltip: record.column }}>
                    <Highlight search={searchQuery}>{record.column}</Highlight>
                </ColumnName>
            ),
            width: '30%',
            ellipsis: true,
            sorter: (sourceA, sourceB) => {
                return sourceA.column.localeCompare(sourceB.column);
            },
        },
        {
            title: 'Type',
            key: 'type',
            render: (record) => capitalizeFirstLetter(record.type?.toLowerCase()),
            sorter: (sourceA, sourceB) => {
                return sourceA.type.localeCompare(sourceB.type);
            },
        },
    ];

    // Column with action button for viewing the detailed stats
    const viewColumn = {
        title: '',
        key: 'view',
        render: (record) => (
            <ViewButton>
                <Button variant="text" onClick={() => setExpandedDrawerFieldPath(record.column)}>
                    View
                </Button>
            </ViewButton>
        ),
        alignment: 'right' as AlignmentOptions,
    };

    // Retrieves a set of names of columns that should be shown based on their presence in the data profile.
    const columnsPresent: Set<string> = getItemKeySet(tableData);

    // Compute the final columns to render.
    const columns = [
        ...requiredColumns,
        ...optionalColumns.filter((column) => columnsPresent.has(column.key as string)),
        viewColumn,
    ];

    return columns;
};
