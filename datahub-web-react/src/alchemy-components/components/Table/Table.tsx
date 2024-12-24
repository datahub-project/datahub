import { LoadingOutlined } from '@ant-design/icons';
import { Text } from '@components';
import React, { useState } from 'react';
import {
    BaseTable,
    HeaderContainer,
    LoadingContainer,
    SortIcon,
    SortIconsContainer,
    TableCell,
    TableContainer,
    TableHeader,
    TableHeaderCell,
    TableRow,
} from './components';
import { TableProps } from './types';
import { getSortedData, handleActiveSort, renderCell, SortingState } from './utils';

export const tableDefaults: TableProps<any> = {
    columns: [],
    data: [],
    showHeader: true,
    isLoading: false,
    isScrollable: false,
    maxHeight: '100%',
};

export const Table = <T,>({
    columns = tableDefaults.columns,
    data = tableDefaults.data,
    showHeader = tableDefaults.showHeader,
    isLoading = tableDefaults.isLoading,
    isScrollable = tableDefaults.isScrollable,
    maxHeight = tableDefaults.maxHeight,
    ...props
}: TableProps<T>) => {
    const [sortColumn, setSortColumn] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortingState>(SortingState.ORIGINAL);

    const sortedData = getSortedData(columns, data, sortColumn, sortOrder);

    if (isLoading) {
        return (
            <LoadingContainer>
                <LoadingOutlined />
                <Text color="gray">Loading data...</Text>
            </LoadingContainer>
        );
    }

    return (
        <TableContainer isScrollable={isScrollable} maxHeight={maxHeight}>
            <BaseTable {...props}>
                {showHeader && (
                    <TableHeader>
                        <tr>
                            {columns.map((column) => (
                                <TableHeaderCell key={column.key} width={column.width}>
                                    <HeaderContainer>
                                        {column.title}
                                        {column.sorter && (
                                            <SortIconsContainer
                                                onClick={() =>
                                                    column.sorter &&
                                                    handleActiveSort(
                                                        column.key,
                                                        sortColumn,
                                                        setSortColumn,
                                                        setSortOrder,
                                                    )
                                                }
                                            >
                                                <SortIcon
                                                    icon="ChevronLeft"
                                                    size="md"
                                                    rotate="90"
                                                    isActive={
                                                        column.key === sortColumn &&
                                                        sortOrder === SortingState.ASCENDING
                                                    }
                                                />
                                                <SortIcon
                                                    icon="ChevronRight"
                                                    size="md"
                                                    rotate="90"
                                                    isActive={
                                                        column.key === sortColumn &&
                                                        sortOrder === SortingState.DESCENDING
                                                    }
                                                />
                                            </SortIconsContainer>
                                        )}
                                    </HeaderContainer>
                                </TableHeaderCell>
                            ))}
                        </tr>
                    </TableHeader>
                )}
                <tbody>
                    {sortedData.map((row, index) => (
                        <TableRow>
                            {columns.map((column) => {
                                return (
                                    <TableCell key={column.key} width={column.width} alignment={column.alignment}>
                                        {renderCell(column, row, index)}
                                    </TableCell>
                                );
                            })}
                        </TableRow>
                    ))}
                </tbody>
            </BaseTable>
        </TableContainer>
    );
};
