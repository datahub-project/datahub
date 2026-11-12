import React, { TableHTMLAttributes } from 'react';

import { AlignmentOptions } from '@src/alchemy-components/theme/config';

export interface Column<T> {
    title: string | React.ReactNode;
    key: string;
    dataIndex?: string;
    render?: (record: T, index: number) => React.ReactNode;
    width?: string;
    minWidth?: string;
    maxWidth?: string;
    sorter?: ((a: T, b: T) => number) | boolean;
    alignment?: AlignmentOptions;
    tooltipTitle?: string;
    onCellClick?: (record: T) => void;
    isCellClickable?: (record: T) => boolean;
    cellWrapper?: (content: React.ReactNode, record: T) => React.ReactNode;
    /** Pins the column to the left edge while the table scrolls horizontally. Use on leading columns. */
    fixed?: 'left';
}

export interface TableProps<T> extends TableHTMLAttributes<HTMLTableElement> {
    columns: Column<T>[];
    data: T[];
    showHeader?: boolean;
    isLoading?: boolean;
    isScrollable?: boolean;
    maxHeight?: string;
    isBorderless?: boolean;
    onRowClick?: (record: T) => void;
    /**
     * When set (including `null`), row highlight is controlled by this key
     * instead of the table's internal click-to-focus state. Pass `null` to
     * show no focused row.
     */
    focusedRowKey?: string | null;
    rowClassName?: (record: T) => string;
    rowDataTestId?: (record: T) => string;
    handleSortColumnChange?: ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => void;
    rowKey?: string | ((record: T) => string);
    rowSelection?: RowSelectionProps<T>;
    rowRefs?: React.MutableRefObject<HTMLTableRowElement[]>;
    headerRef?: React.RefObject<HTMLTableSectionElement>;
    footer?: React.ReactNode;
    renderScrollObserver?: () => React.ReactNode;
    /** Renders rows in collapsible groups, each with a header row inside the same table. */
    groupBy?: TableGroupByProps<T>;
    /** Renders nested rows (e.g. schema fields) as an expandable tree. Needs `rowKey`. */
    treeData?: TableTreeDataProps<T>;
    /** Controlled sort. Pass together with `onSortChange`. */
    sortState?: TableSortState;
    onSortChange?: (sortState: TableSortState) => void;
    /** Renders only the rows in view. Rows must have a fixed height. Needs `isScrollable` and a bounded height. */
    virtualization?: TableVirtualizationProps;
    /**
     * Re-renders a cell only when its row or column object changes, so pass memoized columns. Always on
     * with `virtualization`; worth turning on for small tables whose parent re-renders often.
     */
    memoizeCells?: boolean;
    /** Scrolls this row into view whenever the key changes, including rows outside the virtualized window. */
    scrollToRowKey?: string | null;
    /** Cell density. `sm` uses 8px cell padding for narrow, dense lists (e.g. sidebars). Defaults to `md`. */
    size?: 'sm' | 'md';
    /** Opens a full-width detail panel under each row in `expandedRowKeys`. Needs `rowKey`. */
    rowDetail?: TableRowDetailProps<T>;
}

export interface TableRowDetailProps<T> {
    /** Keys (from `rowKey`) of the rows whose detail panel is open. The caller owns the toggle. */
    expandedRowKeys: string[];
    render: (record: T, index: number) => React.ReactNode;
}

export interface TableSortState {
    sortColumn: string | null;
    sortOrder: SortingState;
}

export interface TableTreeDataProps<T> {
    getChildren: (record: T) => T[] | undefined | null;
    /** Uncontrolled: rows expanded on first render. */
    defaultExpandedRowKeys?: string[];
    /** Controlled: the expanded rows. Pass together with `onExpandedRowsChange`. */
    expandedRowKeys?: string[];
    onExpandedRowsChange?: (expandedRowKeys: string[]) => void;
    /** Indent per nesting level, in px. Defaults to 16. */
    indentSize?: number;
}

export interface TableVirtualizationProps {
    /** Height of every row (and group header row), in px. */
    rowHeight: number;
    /** Extra rows rendered above and below the visible window. Defaults to 10. */
    overscan?: number;
}

export interface TableRowGroup<T> {
    key: string;
    rows: T[];
}

export interface TableGroupRenderProps<T> extends TableRowGroup<T> {
    collapsed: boolean;
}

export interface TableGroupByProps<T> {
    getGroupKey: (record: T) => string;
    /** Content for the group header row: typically an icon and a label. The table adds the caret and count. */
    renderGroupHeader: (group: TableGroupRenderProps<T>) => React.ReactNode;
    /** Optional content pinned to the right of the group header row, e.g. an add or actions button. */
    renderGroupActions?: (group: TableGroupRenderProps<T>) => React.ReactNode;
    /** Orders groups. Defaults to the order in which each group's first row appears in the data. */
    groupOrder?: (a: TableRowGroup<T>, b: TableRowGroup<T>) => number;
    /** Uncontrolled: groups collapsed on first render. */
    defaultCollapsedKeys?: string[];
    /** Controlled: the collapsed groups. Pass together with `onCollapsedChange`. */
    collapsedKeys?: string[];
    onCollapsedChange?: (collapsedKeys: string[]) => void;
    /** Shows the number of rows in each group header. Defaults to true. */
    showCount?: boolean;
    /** Keeps each group's header row pinned below the column header while its rows scroll. Needs `isScrollable`. */
    stickyGroupHeaders?: boolean;
    groupHeaderDataTestId?: (group: TableGroupRenderProps<T>) => string;
}

export interface RowSelectionProps<T> {
    selectedRowKeys: string[];
    onChange?: (selectedKeys: string[], selectedRows: T[]) => void;
    getCheckboxProps?: (T) => {
        disabled: boolean;
    };
}

export enum SortingState {
    ASCENDING = 'ascending',
    DESCENDING = 'descending',
    ORIGINAL = 'original',
}
