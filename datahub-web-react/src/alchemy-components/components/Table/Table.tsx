import { CaretUp, CaretDown } from 'phosphor-react';
import { LoadingOutlined } from '@ant-design/icons';
import { Text } from '@components';
import React, { useEffect, useState } from 'react';
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
import { SortingState, TableProps } from './types';
import { getSortedData, handleActiveSort, renderCell } from './utils';
import { Tooltip2 } from '../Tooltip2';
import { useGetSelectionColumn } from './useGetSelectionColumn';

export const tableDefaults: TableProps<any> = {
    columns: [],
    data: [],
    showHeader: true,
    isLoading: false,
    isScrollable: false,
    maxHeight: '100%',
    isBorderless: false,
};

export const Table = <T,>({
    columns = tableDefaults.columns,
    data = tableDefaults.data,
    showHeader = tableDefaults.showHeader,
    isLoading = tableDefaults.isLoading,
    isScrollable = tableDefaults.isScrollable,
    maxHeight = tableDefaults.maxHeight,
    expandable,
    isBorderless = tableDefaults.isBorderless,
    onRowClick,
    onExpand,
    rowClassName,
    handleSortColumnChange = undefined,
    rowKey,
    rowSelection,
    rowRefs,
    headerRef,
    rowDataTestId,
    ...props
}: TableProps<T>) => {
    const [sortColumn, setSortColumn] = useState<string | null>(null);
    const [sortOrder, setSortOrder] = useState<SortingState>(SortingState.ORIGINAL);

    const sortedData = getSortedData(columns, data, sortColumn, sortOrder);
    const isRowClickable = !!onRowClick;

    const selectionColumn = useGetSelectionColumn(data, rowKey, rowSelection);
    const finalColumns = [...selectionColumn, ...columns];

    useEffect(() => {
        if (handleSortColumnChange && sortOrder && sortColumn) {
            handleSortColumnChange({ sortColumn, sortOrder });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sortOrder, sortColumn]);

    if (isLoading) {
        return (
            <LoadingContainer>
                <LoadingOutlined />
                <Text color="gray">Loading data...</Text>
            </LoadingContainer>
        );
    }

    return (
        <TableContainer isScrollable={isScrollable} maxHeight={maxHeight} isBorderless={isBorderless}>
            <BaseTable {...props}>
                {/* Render the table header if enabled */}
                {showHeader && (
                    <TableHeader ref={headerRef}>
                        <tr>
                            {/* Map through columns to create header cells */}
                            {finalColumns.map((column, index) => (
                                <TableHeaderCell
                                    key={column.key} // Unique key for each header cell
                                    width={column.width}
                                    maxWidth={column.maxWidth}
                                    shouldAddRightBorder={index !== columns.length - 1} // Add border unless last column
                                >
                                    {column?.tooltipTitle ? (
                                        <Tooltip2 title={column.tooltipTitle}>
                                            <HeaderContainer alignment={column.alignment}>
                                                {column.title}
                                                {column.sorter && ( // Render sort icons if the column is sortable
                                                    <SortIconsContainer
                                                        onClick={() =>
                                                            handleActiveSort(
                                                                column.key,
                                                                sortColumn,
                                                                setSortColumn,
                                                                setSortOrder,
                                                            )
                                                        }
                                                    >
                                                        {/* Sort icons for ascending and descending */}
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
                                        </Tooltip2>
                                    ) : (
                                        <HeaderContainer alignment={column.alignment}>
                                            {column.title}
                                            {column.sorter && ( // Render sort icons if the column is sortable
                                                <SortIconsContainer
                                                    onClick={() =>
                                                        handleActiveSort(
                                                            column.key,
                                                            sortColumn,
                                                            setSortColumn,
                                                            setSortOrder,
                                                        )
                                                    }
                                                >
                                                    {/* Sort icons for ascending and descending */}
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
                                    )}
                                </TableHeaderCell>
                            ))}
                            {/* Placeholder for expandable icon if enabled */}
                        </tr>
                    </TableHeader>
                )}
                {/* Render table body with rows and cells */}
                <tbody>
                    {sortedData.map((row: any, index) => {
                        const isExpanded = expandable?.expandedGroupIds?.includes(row?.name); // Check if row is expanded
                        const canExpand = expandable?.rowExpandable?.(row); // Check if row is expandable
                        const key = `row-${index}-${sortColumn ?? 'none'}-${sortOrder ?? 'none'}`;
                        return (
                            <>
                                {/* Render the main row */}
                                <TableRow
                                    key={key}
                                    canExpand={canExpand}
                                    onClick={() => {
                                        if (canExpand) onExpand?.(row); // Handle row expansion
                                        onRowClick?.(row); // Handle row click
                                    }}
                                    className={rowClassName?.(row)} // Add row-specific class
                                    ref={(el) => {
                                        if (rowRefs && el) {
                                            const currentRefs = rowRefs.current;
                                            currentRefs[index] = el;
                                        }
                                    }}
                                    isRowClickable={isRowClickable}
                                    data-testId={rowDataTestId?.(row)}
                                >
                                    {/* Render each cell in the row */}

                                    {finalColumns.map((column, i) => {
                                        return (
                                            <TableCell
                                                key={column.key}
                                                width={column.width}
                                                alignment={
                                                    columns.length - 1 === i && canExpand ? 'right' : column.alignment
                                                }
                                                isGroupHeader={canExpand}
                                                isExpanded={isExpanded}
                                            >
                                                {/* Add expandable icon if applicable or render row */}
                                                {columns.length - 1 === i && canExpand ? (
                                                    <div
                                                        style={{
                                                            cursor: 'pointer',
                                                            paddingTop: '10px',
                                                            paddingRight: '10px',
                                                        }}
                                                    >
                                                        {isExpanded ? (
                                                            <CaretDown
                                                                size={16}
                                                                weight="bold"
                                                                data-testId="group-header-expanded-icon"
                                                            /> // Expanded icon
                                                        ) : (
                                                            <CaretUp
                                                                size={16}
                                                                weight="bold"
                                                                data-testId="group-header-collapsed-icon"
                                                            /> // Collapsed icon
                                                        )}
                                                    </div>
                                                ) : (
                                                    renderCell(column, row, index)
                                                )}
                                            </TableCell>
                                        );
                                    })}
                                </TableRow>
                                {/* Render expanded content if row is expanded */}
                                {isExpanded && expandable?.expandedRowRender && (
                                    <TableRow isRowClickable={isRowClickable}>
                                        <TableCell
                                            colSpan={columns.length + (expandable?.expandIconPosition ? 1 : 0)}
                                            style={{ padding: 0 }}
                                        >
                                            <div style={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
                                                {expandable.expandedRowRender(row, index)} {/* Expanded content */}
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                )}
                            </>
                        );
                    })}
                </tbody>
            </BaseTable>
        </TableContainer>
    );
};
