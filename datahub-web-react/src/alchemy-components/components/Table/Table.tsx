import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { CircleNotch } from '@phosphor-icons/react/dist/csr/CircleNotch';
import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { keyframes } from 'styled-components';

import { Button } from '@components/components/Button';
import { StructuredPopover } from '@components/components/StructuredPopover';
import { TableGroupHeaderRow } from '@components/components/Table/TableGroupHeaderRow';
import {
    BaseTable,
    FixedHeightCellContent,
    HeaderContainer,
    LoadingContainer,
    RowDetailCell,
    RowDetailContent,
    SortIcon,
    SortIconsContainer,
    SpacerCell,
    TableCell,
    TableContainer,
    TableHeader,
    TableHeaderCell,
    TableRow,
    TreeCellContent,
    TreeExpanderSlot,
} from '@components/components/Table/components';
import {
    Column,
    SortingState,
    TableGroupRenderProps,
    TableProps,
    TableSortState,
} from '@components/components/Table/types';
import { useGetSelectionColumn } from '@components/components/Table/useGetSelectionColumn';
import { useRowSelection } from '@components/components/Table/useRowSelection';
import { useToggledKeys } from '@components/components/Table/useToggledKeys';
import { useVirtualWindow } from '@components/components/Table/useVirtualWindow';
import {
    getGroupedRows,
    getNextSortState,
    getRowKey,
    getSortedData,
    renderCell,
} from '@components/components/Table/utils';
import { Text } from '@components/components/Text';

import { spacing } from '@src/alchemy-components/theme';

// Placeholder token for the unsorted state inside React row keys (programmatic, not user-facing).
const NO_SORT_KEY = 'none';
const DEFAULT_INDENT_SIZE = 16;
const UNSORTED: TableSortState = { sortColumn: null, sortOrder: SortingState.ORIGINAL };

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

export const tableDefaults: TableProps<any> = {
    columns: [],
    data: [],
    showHeader: true,
    isLoading: false,
    isScrollable: false,
    maxHeight: '100%',
    isBorderless: false,
};

type DisplayItem<T> =
    | { type: 'group'; group: TableGroupRenderProps<T>; selectableRows: T[] }
    | { type: 'row'; row: T; index: number; key: string; depth: number; hasChildren: boolean; isExpanded: boolean };

const escapeSelector = (value: string) =>
    typeof CSS !== 'undefined' && typeof CSS.escape === 'function' ? CSS.escape(value) : value;

/** Left offsets for leading `fixed: 'left'` columns, from measured header cells or the declared widths. */
const getFixedOffsets = <T,>(columns: Column<T>[], measuredWidths: number[]): (number | undefined)[] => {
    let offset = 0;
    let isLeadingFixed = true;
    return columns.map((column, i) => {
        isLeadingFixed = isLeadingFixed && column.fixed === 'left';
        if (!isLeadingFixed) return undefined;
        const left = offset;
        offset += measuredWidths[i] ?? (parseFloat(column.width ?? '') || 0);
        return left;
    });
};

type CellContentProps = { column: Column<unknown>; row: unknown; index: number };

// A virtualized table re-renders each time its window moves while scrolling, and a small table can
// re-render with its parent. Memoizing cell content keeps those re-renders from re-running every
// visible cell's render; a cell still updates when its row or its column (and so its render function) changes.
const MemoizedCellContent = React.memo(({ column, row, index }: CellContentProps) => {
    return <>{renderCell(column, row, index)}</>;
});
export const Table = <T,>({
    columns = tableDefaults.columns,
    data = tableDefaults.data,
    showHeader = tableDefaults.showHeader,
    isLoading = tableDefaults.isLoading,
    isScrollable = tableDefaults.isScrollable,
    maxHeight = tableDefaults.maxHeight,
    isBorderless = tableDefaults.isBorderless,
    onRowClick,
    focusedRowKey,
    rowClassName,
    handleSortColumnChange = undefined,
    rowKey,
    rowSelection,
    rowRefs,
    headerRef,
    rowDataTestId,
    footer,
    renderScrollObserver,
    groupBy,
    treeData,
    sortState,
    onSortChange,
    virtualization,
    memoizeCells,
    scrollToRowKey,
    rowDetail,
    size = 'md',
    ...props
}: TableProps<T>) => {
    const { t } = useTranslation('alchemy');
    const [internalSortState, setInternalSortState] = useState<TableSortState>(UNSORTED);
    const { sortColumn, sortOrder } = sortState ?? internalSortState;
    // Internal click-to-focus (when `focusedRowKey` is not controlled): the row key, or its index without `rowKey`.
    const [internalFocusedRow, setInternalFocusedRow] = useState<string | null>(null);

    const containerRef = useRef<HTMLDivElement>(null);
    const internalHeaderRef = useRef<HTMLTableSectionElement>(null);
    const theadRef = headerRef ?? internalHeaderRef;
    const [headerHeight, setHeaderHeight] = useState(0);
    const [headerCellWidths, setHeaderCellWidths] = useState<number[]>([]);

    const isRowClickable = !!onRowClick;
    const shouldMemoizeCells = !!virtualization || !!memoizeCells;
    const { keys: collapsedKeys, toggleKey: toggleGroup } = useToggledKeys(
        groupBy?.collapsedKeys,
        groupBy?.defaultCollapsedKeys,
        groupBy?.onCollapsedChange,
    );
    const { keys: expandedRowKeys, toggleKey: toggleTreeRow } = useToggledKeys(
        treeData?.expandedRowKeys,
        treeData?.defaultExpandedRowKeys,
        treeData?.onExpandedRowsChange,
    );

    // Sorting, grouping and flattening are memoized so internal state changes (scrolling a virtualized
    // window, focusing a row) re-render without redoing them.
    const { selectionData, items } = useMemo(() => {
        const sortRows = (rows: T[]) => getSortedData(columns, rows, sortColumn, sortOrder);
        // Every row including nested children (expanded or not), so selection covers the whole tree.
        const flattenTree = (rows: T[]): T[] =>
            treeData ? rows.flatMap((row) => [row, ...flattenTree(sortRows(treeData.getChildren(row) ?? []))]) : rows;

        const sorted = sortRows(data);
        // Sorting applies within each group; groups keep their own order.
        const grouped = groupBy ? getGroupedRows(sorted, groupBy.getGroupKey, groupBy.groupOrder) : undefined;
        // Grouped rows render in group order, so selection keys and row indexes follow that order too.
        let selectable = data;
        if (grouped) selectable = flattenTree(grouped.flatMap((group) => group.rows));
        else if (treeData) selectable = flattenTree(data);

        // One flat list of what the body shows, so grouping, nested rows and virtualization share a render path.
        const displayItems: DisplayItem<T>[] = [];
        let rowIndex = 0;
        const pushRows = (rows: T[], depth: number) => {
            rows.forEach((row) => {
                const index = rowIndex;
                rowIndex += 1;
                const key = getRowKey(row, index, rowKey);
                const children = treeData?.getChildren(row);
                const hasChildren = !!children?.length;
                const isExpanded = hasChildren && expandedRowKeys.includes(key);
                displayItems.push({ type: 'row', row, index, key, depth, hasChildren, isExpanded });
                // Nested rows sort within their parent, so the tree shape never changes with the sort.
                if (isExpanded && children) pushRows(sortRows(children), depth + 1);
            });
        };
        if (grouped) {
            grouped.forEach((group) => {
                const collapsed = collapsedKeys.includes(group.key);
                displayItems.push({
                    type: 'group',
                    group: { ...group, collapsed },
                    selectableRows: flattenTree(group.rows),
                });
                if (collapsed) {
                    rowIndex += group.rows.length;
                } else {
                    pushRows(group.rows, 0);
                }
            });
        } else {
            pushRows(sorted, 0);
        }
        return { selectionData: selectable, items: displayItems };
    }, [data, columns, sortColumn, sortOrder, groupBy, treeData, expandedRowKeys, collapsedKeys, rowKey]);

    const selection = useRowSelection(selectionData, rowKey, rowSelection);
    const selectionColumn = useGetSelectionColumn(selection, rowKey, rowSelection);
    // The selection column stays pinned with the first column so checkboxes don't scroll away.
    const leadingColumns =
        columns[0]?.fixed === 'left' ? selectionColumn.map((c) => ({ ...c, fixed: 'left' as const })) : selectionColumn;
    const finalColumns: Column<T>[] = [...leadingColumns, ...columns];
    const fixedOffsets = getFixedOffsets(finalColumns, headerCellWidths);
    const hasFixedColumns = fixedOffsets.some((offset) => offset !== undefined);
    // First data column, after any checkbox column: styled as the row's title and holds the tree caret.
    const primaryColumnIndex = selectionColumn.length;
    // Without a checkbox column, group carets need more room than the default cell padding; the first
    // column's padding grows to match so group labels and column text still line up.
    const groupGutter = groupBy && !rowSelection ? spacing.xlg : undefined;

    const shouldMeasureHeader = showHeader && (!!groupBy?.stickyGroupHeaders || !!virtualization || hasFixedColumns);
    const columnKeys = finalColumns.map((column) => column.key).join(',');
    useLayoutEffect(() => {
        const thead = theadRef.current;
        if (!shouldMeasureHeader || !thead) return undefined;
        const measure = () => {
            setHeaderHeight(thead.offsetHeight);
            setHeaderCellWidths(Array.from(thead.querySelectorAll('th')).map((th) => th.offsetWidth));
        };
        measure();
        if (typeof ResizeObserver === 'undefined') return undefined;
        const observer = new ResizeObserver(measure);
        observer.observe(thead);
        return () => observer.disconnect();
    }, [shouldMeasureHeader, theadRef, isLoading, columnKeys]);

    const virtualWindow = useVirtualWindow(containerRef, items.length, virtualization, headerHeight, !!isLoading);

    const lastScrolledKey = useRef<string | null | undefined>(undefined);
    useEffect(() => {
        // Cleared (e.g. a drawer closed): forget the last row, so selecting it again scrolls to it again.
        if (!scrollToRowKey) {
            lastScrolledKey.current = null;
            return;
        }
        const container = containerRef.current;
        if (!container || lastScrolledKey.current === scrollToRowKey) return;
        const itemIndex = items.findIndex((item) => item.type === 'row' && item.key === scrollToRowKey);
        // Not rendered yet (data still loading, parent collapsed): try again on a later render.
        if (itemIndex < 0) return;
        lastScrolledKey.current = scrollToRowKey;
        if (virtualization) {
            const rowTop = itemIndex * virtualization.rowHeight;
            const viewTop = container.scrollTop;
            const viewHeight = container.clientHeight - headerHeight;
            if (rowTop < viewTop) {
                container.scrollTop = rowTop;
            } else if (rowTop + virtualization.rowHeight > viewTop + viewHeight) {
                container.scrollTop = rowTop + virtualization.rowHeight - viewHeight;
            }
            return;
        }
        container
            .querySelector(`[data-row-key="${escapeSelector(scrollToRowKey)}"]`)
            ?.scrollIntoView?.({ block: 'nearest' });
    });

    useEffect(() => {
        if (handleSortColumnChange && sortOrder && sortColumn) {
            handleSortColumnChange({ sortColumn, sortOrder });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sortOrder, sortColumn]);

    useEffect(() => {
        setInternalFocusedRow(null);
    }, [data]);

    const handleSortClick = (key: string) => {
        const nextSortState = getNextSortState(key, { sortColumn, sortOrder });
        if (!sortState) setInternalSortState(nextSortState);
        onSortChange?.(nextSortState);
    };

    const renderSortIcons = (column: Column<T>) =>
        column.sorter && (
            <SortIconsContainer data-testid={`sort-${column.key}`} onClick={() => handleSortClick(column.key)}>
                {/* Sort icons for ascending and descending */}
                <SortIcon
                    icon={CaretLeft}
                    size="md"
                    rotate="90"
                    color={column.key === sortColumn && sortOrder === SortingState.ASCENDING ? 'iconBrand' : 'icon'}
                />
                <SortIcon
                    icon={CaretRight}
                    size="md"
                    rotate="90"
                    color={column.key === sortColumn && sortOrder === SortingState.DESCENDING ? 'iconBrand' : 'icon'}
                />
            </SortIconsContainer>
        );

    // A flat list given to `treeData` (e.g. a schema with no nested fields) gets no caret column.
    const hasExpandableRows = !!treeData && data.some((row) => !!treeData.getChildren(row)?.length);

    const renderTreeCell = (item: Extract<DisplayItem<T>, { type: 'row' }>, content: React.ReactNode) => (
        <TreeCellContent indent={item.depth * (treeData?.indentSize ?? DEFAULT_INDENT_SIZE)}>
            <TreeExpanderSlot>
                {item.hasChildren && (
                    <Button
                        type="button"
                        variant="text"
                        color="gray"
                        size="xs"
                        isCircle
                        icon={{ icon: item.isExpanded ? CaretDown : CaretRight, color: 'icon' }}
                        aria-expanded={item.isExpanded}
                        aria-label={item.isExpanded ? t('table.tree.collapseRow') : t('table.tree.expandRow')}
                        onClick={(e) => {
                            e.stopPropagation();
                            toggleTreeRow(item.key);
                        }}
                    />
                )}
            </TreeExpanderSlot>
            {content}
        </TreeCellContent>
    );

    const renderRow = (item: Extract<DisplayItem<T>, { type: 'row' }>) => {
        const { row, index } = item;
        const isDetailOpen = !!rowDetail?.expandedRowKeys.includes(item.key);
        // With `rowKey`, rows keep their identity when sorting or expanding shifts their position.
        const key = rowKey
            ? `row-${item.key}`
            : `row-${index}-${sortColumn ?? NO_SORT_KEY}-${sortOrder ?? NO_SORT_KEY}`;
        const focusId = rowKey ? item.key : String(index);
        const isFocused = focusedRowKey !== undefined ? focusedRowKey === item.key : internalFocusedRow === focusId;
        return (
            <React.Fragment key={key}>
                {/* Render the main row */}
                <TableRow
                    onClick={(e) => {
                        if (focusedRowKey === undefined) {
                            setInternalFocusedRow(internalFocusedRow === focusId ? null : focusId);
                        }
                        onRowClick?.(row); // Handle row click
                        e.stopPropagation();
                    }}
                    isFocused={isFocused}
                    className={rowClassName?.(row)} // Add row-specific class
                    ref={(el) => {
                        if (rowRefs && el) {
                            const currentRefs = rowRefs.current;
                            currentRefs[index] = el;
                        }
                    }}
                    isRowClickable={isRowClickable}
                    data-testid={rowDataTestId?.(row)}
                    data-row-key={rowKey ? item.key : undefined}
                    style={virtualization ? { height: virtualization.rowHeight } : undefined}
                    canHover
                >
                    {/* Render each cell in the row */}

                    {finalColumns.map((column, i) => {
                        let content = shouldMemoizeCells ? (
                            <MemoizedCellContent column={column as Column<unknown>} row={row} index={index} />
                        ) : (
                            renderCell(column, row, index)
                        );
                        if (hasExpandableRows && i === primaryColumnIndex) content = renderTreeCell(item, content);
                        if (virtualization) {
                            content = (
                                <FixedHeightCellContent rowHeight={virtualization.rowHeight}>
                                    {content}
                                </FixedHeightCellContent>
                            );
                        }
                        const cellContent = column.cellWrapper ? column.cellWrapper(content, row) : content;
                        const clickable = column.isCellClickable ? column.isCellClickable(row) : !!column.onCellClick;

                        return (
                            <TableCell
                                key={column.key}
                                width={column.width}
                                alignment={column.alignment}
                                isPrimary={i === primaryColumnIndex}
                                isFixedHeight={!!virtualization}
                                size={size}
                                fixedLeft={fixedOffsets[i]}
                                gutter={i === 0 ? groupGutter : undefined}
                                data-fixed={fixedOffsets[i] !== undefined || undefined}
                                onClick={() => {
                                    if (clickable && column.onCellClick) {
                                        column.onCellClick(row);
                                    }
                                }}
                                style={{ cursor: clickable ? 'pointer' : 'default' }}
                                className={column.cellWrapper ? 'hoverable-cell' : undefined}
                                data-testid={column.key}
                            >
                                {cellContent}
                            </TableCell>
                        );
                    })}
                </TableRow>
                {isDetailOpen && rowDetail && (
                    <TableRow isRowClickable={false} canHover={false} data-testid="table-row-detail">
                        <RowDetailCell colSpan={finalColumns.length}>
                            <RowDetailContent>{rowDetail.render(row, index)}</RowDetailContent>
                        </RowDetailCell>
                    </TableRow>
                )}
            </React.Fragment>
        );
    };

    const renderGroupHeader = (group: TableGroupRenderProps<T>, selectableRows: T[]) =>
        groupBy && (
            <TableGroupHeaderRow
                key={`group-${group.key}`}
                group={group}
                groupBy={groupBy}
                columnCount={finalColumns.length}
                selection={
                    rowSelection
                        ? {
                              ...selection.getGroupSelectionState(selectableRows),
                              onSelect: () => selection.handleGroupSelect(selectableRows),
                          }
                        : undefined
                }
                onToggle={() => toggleGroup(group.key)}
                stickyTop={headerHeight}
                rowHeight={virtualization?.rowHeight}
                gutter={groupGutter}
            />
        );

    const renderSpacer = (height: number) =>
        height > 0 && (
            <tr aria-hidden style={{ height }}>
                <SpacerCell colSpan={finalColumns.length} />
            </tr>
        );

    if (isLoading) {
        return (
            <LoadingContainer>
                <LoadingSpinner />
                <Text>{t('table.loading')}</Text>
            </LoadingContainer>
        );
    }

    return (
        <TableContainer
            ref={containerRef}
            isScrollable={isScrollable}
            maxHeight={maxHeight}
            isBorderless={isBorderless}
            isVirtualized={!!virtualization}
        >
            <BaseTable {...props}>
                {/* Render the table header if enabled */}
                {showHeader && (
                    <TableHeader ref={theadRef}>
                        <tr>
                            {/* Map through columns to create header cells */}
                            {finalColumns.map((column, index) => (
                                <TableHeaderCell
                                    key={column.key} // Unique key for each header cell
                                    width={column.width}
                                    minWidth={column.minWidth}
                                    maxWidth={column.maxWidth}
                                    fixedLeft={fixedOffsets[index]}
                                    gutter={index === 0 ? groupGutter : undefined}
                                    size={size}
                                    shouldAddRightBorder={index !== finalColumns.length - 1} // Add border unless last column
                                >
                                    {column?.tooltipTitle ? (
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
                                    )}
                                </TableHeaderCell>
                            ))}
                        </tr>
                    </TableHeader>
                )}
                {/* Render table body with rows and cells */}
                <tbody>
                    {renderSpacer(virtualWindow.paddingTop)}
                    {items
                        .slice(virtualWindow.start, virtualWindow.end)
                        .map((item) =>
                            item.type === 'group'
                                ? renderGroupHeader(item.group, item.selectableRows)
                                : renderRow(item),
                        )}
                    {renderSpacer(virtualWindow.paddingBottom)}
                    {renderScrollObserver?.()}
                    {footer}
                </tbody>
            </BaseTable>
        </TableContainer>
    );
};
