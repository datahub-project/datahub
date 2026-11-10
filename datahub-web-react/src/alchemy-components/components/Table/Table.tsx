import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { CaretUp } from '@phosphor-icons/react/dist/csr/CaretUp';
import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { keyframes } from 'styled-components';

import { OverflowText } from '@components/components/OverflowText';
import { StructuredPopover } from '@components/components/StructuredPopover';
import {
    BaseTable,
    EllipsisContent,
    EmptyStateCell,
    FixedTableCell,
    FixedTableHeaderCell,
    HeaderContainer,
    HorizontalScrollContainer,
    LoadingContainer,
    SortIcon,
    SortIconsContainer,
    TableCell,
    TableContainer,
    TableHeader,
    TableHeaderCell,
    TableRow,
    TreeCellContent,
    TreeExpandButton,
    TreeExpandSpacer,
} from '@components/components/Table/components';
import { Column, ExpandIconProps, SortingState, TableProps } from '@components/components/Table/types';
import { useGetSelectionColumn } from '@components/components/Table/useGetSelectionColumn';
import { useTableExpansion } from '@components/components/Table/useTableExpansion';
import {
    FlatRow,
    flattenTreeRows,
    getFixedLeftOffsets,
    getRowKey,
    getSortedData,
    handleActiveSort,
    renderCell,
} from '@components/components/Table/utils';
import { Text } from '@components/components/Text';

// Placeholder token for the unsorted state inside React row keys (programmatic, not user-facing).
const NO_SORT_KEY = 'none';
const DEFAULT_INDENT_SIZE = 16;

export const CellHoverWrapper = styled.div`
    width: 100%;
    min-height: 100%;
    display: flex;
    align-items: center;
    margin: -16px;
    padding: 16px;
`;

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

const LoadingSpinner = styled(CircleNotch)`
    animation: ${spin} 1s linear infinite;
    color: ${({ theme }) => theme.colors.iconBrand};
`;

function DefaultExpandIcon<T>({ record, expanded, expandable, onExpand }: ExpandIconProps<T>) {
    const { t } = useTranslation('alchemy');
    if (!expandable) return <TreeExpandSpacer />;
    return (
        <TreeExpandButton
            type="button"
            aria-label={expanded ? t('table.collapseRow') : t('table.expandRow')}
            aria-expanded={expanded}
            data-testid="table-row-expand-icon"
            onClick={(e) => onExpand(record, e)}
        >
            {expanded ? <CaretDown size={14} weight="bold" /> : <CaretRight size={14} weight="bold" />}
        </TreeExpandButton>
    );
}

function applyEllipsis<T>(column: Column<T>, content: React.ReactNode): React.ReactNode {
    if (!column.ellipsis) return content;
    if (typeof content === 'string' || typeof content === 'number') return <OverflowText text={String(content)} />;
    return <EllipsisContent>{content}</EllipsisContent>;
}

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
    focusedRowKey,
    onExpand,
    rowClassName,
    handleSortColumnChange = undefined,
    rowKey,
    rowSelection,
    rowRefs,
    headerRef,
    rowDataTestId,
    footer,
    renderScrollObserver,
    defaultSortColumn,
    defaultSortOrder,
    onSortedDataChange,
    emptyState,
    rowProps,
    scrollX,
    ...props
}: TableProps<T>) => {
    const { t } = useTranslation('alchemy');
    const [sortColumn, setSortColumn] = useState<string | null>(defaultSortColumn ?? null);
    const [sortOrder, setSortOrder] = useState<SortingState>(
        defaultSortColumn ? (defaultSortOrder ?? SortingState.ASCENDING) : SortingState.ORIGINAL,
    );
    const [focusedRowIndex, setFocusedRowIndex] = useState<number | null>(null);

    const childrenColumnName = expandable?.childrenColumnName;
    const indentSize = expandable?.indentSize ?? DEFAULT_INDENT_SIZE;
    const sortedData = getSortedData(columns, data, sortColumn, sortOrder, childrenColumnName);
    const isRowClickable = !!onRowClick;
    const { expandedKeys, isRowExpanded, toggleRow } = useTableExpansion({ data, rowKey, expandable });

    const flatRows: FlatRow<T>[] = childrenColumnName
        ? flattenTreeRows(sortedData, childrenColumnName, expandedKeys, rowKey)
        : sortedData.map((record, index) => ({
              record,
              key: getRowKey(record, index, rowKey),
              depth: 0,
              hasChildren: false,
          }));

    const hasFixedColumn = columns.some((column) => column.fixed === 'left');
    const selectionColumn = useGetSelectionColumn(data, rowKey, rowSelection).map((column) =>
        hasFixedColumn ? { ...column, fixed: 'left' as const } : column,
    );
    const finalColumns = [...selectionColumn, ...columns];
    const fixedLeftOffsets = getFixedLeftOffsets(finalColumns);
    const treeColumnIndex = selectionColumn.length;

    useEffect(() => {
        if (handleSortColumnChange && sortOrder && sortColumn) {
            handleSortColumnChange({ sortColumn, sortOrder });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sortOrder, sortColumn]);

    useEffect(() => {
        onSortedDataChange?.(sortedData, { sortColumn, sortOrder });
        // Columns and the callback are often recreated every render; only data and sort state matter.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data, sortColumn, sortOrder]);

    useEffect(() => {
        setFocusedRowIndex(null);
    }, [data]);

    if (isLoading) {
        return (
            <LoadingContainer>
                <LoadingSpinner />
                <Text>{t('table.loading')}</Text>
            </LoadingContainer>
        );
    }

    const Container = scrollX ? HorizontalScrollContainer : TableContainer;

    const renderSortIcons = (column: Column<T>) =>
        column.sorter && ( // Render sort icons if the column is sortable
            <SortIconsContainer
                data-testid={`sort-${column.key}`}
                onClick={() => handleActiveSort(column.key, sortColumn, setSortColumn, setSortOrder)}
            >
                {/* Sort icons for ascending and descending */}
                <SortIcon
                    icon={CaretLeft}
                    size="md"
                    rotate="90"
                    isActive={column.key === sortColumn && sortOrder === SortingState.ASCENDING}
                />
                <SortIcon
                    icon={CaretRight}
                    size="md"
                    rotate="90"
                    isActive={column.key === sortColumn && sortOrder === SortingState.DESCENDING}
                />
            </SortIconsContainer>
        );

    return (
        <Container isScrollable={isScrollable} maxHeight={maxHeight} isBorderless={isBorderless}>
            <BaseTable $width={scrollX} {...props}>
                {/* Render the table header if enabled */}
                {showHeader && (
                    <TableHeader ref={headerRef}>
                        <tr>
                            {/* Map through columns to create header cells */}
                            {finalColumns.map((column, index) => {
                                const headerContent = column?.tooltipTitle ? (
                                    <StructuredPopover title={column.tooltipTitle}>
                                        <HeaderContainer alignment={column.alignment}>
                                            {column.title}
                                            {renderSortIcons(column)}
                                        </HeaderContainer>
                                    </StructuredPopover>
                                ) : (
                                    <HeaderContainer alignment={column.alignment}>
                                        {column.title}
                                        {renderSortIcons(column)}
                                    </HeaderContainer>
                                );
                                const headerCellProps = {
                                    width: column.width,
                                    minWidth: column.minWidth,
                                    maxWidth: column.maxWidth,
                                    shouldAddRightBorder: index !== finalColumns.length - 1, // Add border unless last column
                                };
                                const left = fixedLeftOffsets[index];
                                return left !== undefined ? (
                                    <FixedTableHeaderCell key={column.key} $left={left} {...headerCellProps}>
                                        {headerContent}
                                    </FixedTableHeaderCell>
                                ) : (
                                    <TableHeaderCell key={column.key} {...headerCellProps}>
                                        {headerContent}
                                    </TableHeaderCell>
                                );
                            })}
                        </tr>
                    </TableHeader>
                )}
                {/* Render table body with rows and cells */}
                <tbody>
                    {flatRows.length === 0 && emptyState && (
                        <tr>
                            <EmptyStateCell colSpan={finalColumns.length}>{emptyState}</EmptyStateCell>
                        </tr>
                    )}
                    {flatRows.map(({ record: row, key: rowId, depth, hasChildren }, index) => {
                        const isTreeRow = !!childrenColumnName;
                        const isExpanded = isTreeRow ? expandedKeys.has(rowId) : isRowExpanded(row, rowId);
                        const canExpand = !isTreeRow && expandable?.rowExpandable?.(row); // Check if row is expandable
                        const key = isTreeRow
                            ? `tree-row-${rowId}`
                            : `row-${index}-${sortColumn ?? NO_SORT_KEY}-${sortOrder ?? NO_SORT_KEY}`;
                        const isFocused =
                            focusedRowKey !== undefined ? focusedRowKey === rowId : focusedRowIndex === index;
                        const {
                            className: extraClassName,
                            onClick: extraOnClick,
                            ...extraRowProps
                        } = rowProps?.(row, index) ?? {};
                        const className = [rowClassName?.(row), extraClassName].filter(Boolean).join(' ') || undefined;
                        const expandIconProps: ExpandIconProps<T> = {
                            record: row,
                            expanded: isExpanded,
                            expandable: hasChildren,
                            depth,
                            onExpand: (record, e) => {
                                e?.stopPropagation();
                                toggleRow(record, rowId);
                            },
                        };
                        return (
                            <React.Fragment key={key}>
                                {/* Render the main row */}
                                <TableRow
                                    canExpand={canExpand}
                                    onClick={(e) => {
                                        if (focusedRowKey === undefined) {
                                            if (focusedRowIndex === index) {
                                                setFocusedRowIndex(null);
                                            } else {
                                                setFocusedRowIndex(index);
                                            }
                                        }
                                        if (canExpand) {
                                            onExpand?.(row); // Handle row expansion
                                            toggleRow(row, rowId);
                                        }
                                        onRowClick?.(row); // Handle row click
                                        extraOnClick?.(e);
                                        e.stopPropagation();
                                    }}
                                    isFocused={isFocused}
                                    className={className} // Add row-specific class
                                    ref={(el) => {
                                        if (rowRefs && el) {
                                            const currentRefs = rowRefs.current;
                                            currentRefs[index] = el;
                                        }
                                    }}
                                    isRowClickable={isRowClickable}
                                    data-testid={rowDataTestId?.(row)}
                                    canHover
                                    {...extraRowProps}
                                >
                                    {/* Render each cell in the row */}

                                    {finalColumns.map((column, i) => {
                                        return (() => {
                                            let content;
                                            if (columns.length - 1 === i && canExpand) {
                                                content = (
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
                                                                data-testid="group-header-expanded-icon"
                                                            /> // Expanded icon
                                                        ) : (
                                                            <CaretUp
                                                                size={16}
                                                                weight="bold"
                                                                data-testid="group-header-collapsed-icon"
                                                            /> // Collapsed icon
                                                        )}
                                                    </div>
                                                );
                                            } else {
                                                content = applyEllipsis(column, renderCell(column, row, index));
                                            }

                                            if (isTreeRow && i === treeColumnIndex) {
                                                content = (
                                                    <TreeCellContent $indent={depth * indentSize}>
                                                        {expandable?.expandIcon ? (
                                                            expandable.expandIcon(expandIconProps)
                                                        ) : (
                                                            <DefaultExpandIcon {...expandIconProps} />
                                                        )}
                                                        {content}
                                                    </TreeCellContent>
                                                );
                                            }

                                            const cellContent = column.cellWrapper
                                                ? column.cellWrapper(content, row)
                                                : content;
                                            const clickable = column.isCellClickable
                                                ? column.isCellClickable(row)
                                                : !!column.onCellClick;

                                            const cellProps = {
                                                width: column.width,
                                                isGroupHeader: canExpand,
                                                isExpanded,
                                                onClick: () => {
                                                    if (clickable && column.onCellClick) {
                                                        column.onCellClick(row);
                                                    }
                                                },
                                                style: { cursor: clickable ? 'pointer' : 'default' },
                                                className: column.cellWrapper ? 'hoverable-cell' : undefined,
                                                'data-testid': column.key,
                                            } as const;
                                            const left = fixedLeftOffsets[i];
                                            const isGroupExpandCell = columns.length - 1 === i && canExpand;

                                            return left !== undefined ? (
                                                <FixedTableCell
                                                    key={column.key}
                                                    $left={left}
                                                    $isFocused={isFocused}
                                                    $isGroupRow={canExpand}
                                                    alignment={isGroupExpandCell ? 'right' : column.alignment}
                                                    {...cellProps}
                                                >
                                                    {cellContent}
                                                </FixedTableCell>
                                            ) : (
                                                <TableCell
                                                    key={column.key}
                                                    alignment={isGroupExpandCell ? 'right' : column.alignment}
                                                    {...cellProps}
                                                >
                                                    {cellContent}
                                                </TableCell>
                                            );
                                        })();
                                    })}
                                </TableRow>
                                {/* Render expanded content if row is expanded */}
                                {!isTreeRow && isExpanded && expandable?.expandedRowRender && (
                                    <TableRow isRowClickable={isRowClickable} canHover={false}>
                                        <TableCell
                                            colSpan={columns.length + (expandable?.expandIconPosition ? 1 : 0)}
                                            style={{ padding: 0, fontWeight: 'normal' }}
                                        >
                                            <div
                                                style={{
                                                    display: 'flex',
                                                    flexDirection: 'column',
                                                    width: '100%',
                                                    minWidth: 0,
                                                }}
                                            >
                                                {expandable.expandedRowRender(row, index)} {/* Expanded content */}
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                )}
                            </React.Fragment>
                        );
                    })}
                    {renderScrollObserver?.()}
                    {footer}
                </tbody>
            </BaseTable>
        </Container>
    );
};
