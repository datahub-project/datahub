import { Button, Table } from 'antd';
import type { ColumnsType } from 'antd/lib/table';
import React, { useMemo } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { navigateToSearchUrl } from '@app/search/utils/navigateToSearchUrl';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Cell, EntityType, FacetFilterInput, TableChart as TableChartType } from '@types';

type Props = {
    chartData: TableChartType;
};

type TableCellProps = {
    cell: Cell;
};

const StyledTable = styled(Table)`
    padding-top: 16px;
    width: 100%;
`;

const TableLink = styled(Button)`
    &&& {
        padding: 0px;
        font-weight: 400;
        margin-top: -6px;
        margin-bottom: -6px;
    }
`;

const TableCell = ({ cell }: TableCellProps) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const onClickQuery = (query: string, types: Array<EntityType>, filters: Array<FacetFilterInput>) => {
        navigateToSearchUrl({
            query,
            type: (types && types.length > 0 && types[0]) || undefined,
            filters: filters || [],
            history,
        });
    };

    if (cell.linkParams?.searchParams) {
        return (
            <TableLink
                type="link"
                onClick={() =>
                    onClickQuery(
                        cell.linkParams?.searchParams?.query || '',
                        cell.linkParams?.searchParams?.types || [],
                        cell.linkParams?.searchParams?.filters || [],
                    )
                }
            >
                {cell.value}
            </TableLink>
        );
    }
    if (cell.linkParams?.entityProfileParams) {
        return (
            <TableLink
                type="link"
                href={entityRegistry.getEntityUrl(
                    cell.linkParams?.entityProfileParams?.type,
                    cell.linkParams?.entityProfileParams?.urn,
                )}
            >
                {cell.value}
            </TableLink>
        );
    }
    return <span>{cell.value}</span>;
};

const NUMERIC_VALUE_RE = /^-?\d+(\.\d+)?$/;

function parseSortValue(cell: Cell): number {
    // Strip a trailing % so percentages sort numerically. Safe to assume the
    // result is finite because isNumericColumn gates sortability.
    return parseFloat((cell.value ?? '').replace(/%$/, ''));
}

// A column is numeric if every non-empty cell in it parses as a number
// (optionally followed by %). Detecting via content rather than column name
// avoids coupling sort behavior to specific header strings.
function isNumericColumn(rows: TableChartType['rows'], colIndex: number): boolean {
    const nonEmptyValues = rows
        .map((row) => row.cells?.[colIndex]?.value)
        .filter((v): v is string => v != null && v !== '');
    if (nonEmptyValues.length === 0) return false;
    return nonEmptyValues.every((v) => NUMERIC_VALUE_RE.test(v.replace(/%$/, '').trim()));
}

type TableRow = Record<string, Cell>;

export const TableChart = ({ chartData }: Props) => {
    const tableData: TableRow[] = chartData.rows.map(
        (row) =>
            row.cells?.reduce<TableRow>((acc, cell, i) => ({ ...acc, [chartData.columns[i]]: cell }), {}) ||
            ({} as TableRow),
    );

    const { numericByIndex, defaultSortColumn } = useMemo(() => {
        const numeric = chartData.columns.map((_, i) => isNumericColumn(chartData.rows, i));
        // Default-sort by the first numeric column whose header is "Count" (case-insensitive).
        const defaultSort = chartData.columns.find((c, i) => numeric[i] && /^count$/i.test(c));
        return { numericByIndex: numeric, defaultSortColumn: defaultSort };
    }, [chartData.rows, chartData.columns]);

    const columns: ColumnsType<TableRow> = chartData.columns.map((column, i) => {
        const isSortable = numericByIndex[i];
        return {
            title: column,
            key: column,
            dataIndex: column,
            render: (cell: Cell) => <TableCell cell={cell} />,
            ...(isSortable && {
                sorter: (a: TableRow, b: TableRow) => parseSortValue(a[column]) - parseSortValue(b[column]),
                defaultSortOrder: column === defaultSortColumn ? ('descend' as const) : undefined,
            }),
        };
    });

    return (
        <StyledTable columns={columns as ColumnsType<object>} dataSource={tableData} pagination={false} size="small" />
    );
};
