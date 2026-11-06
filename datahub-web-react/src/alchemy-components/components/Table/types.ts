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
    /** Pins the column to the left edge while scrolling horizontally. Give fixed columns a px `width`. */
    fixed?: 'left';
    /** Truncates the cell content to one line; plain-text content shows a tooltip when truncated. */
    ellipsis?: boolean;
}

export type RowHTMLProps = React.HTMLAttributes<HTMLTableRowElement> & { [dataAttribute: `data-${string}`]: string };

export interface SortState {
    sortColumn: string | null;
    sortOrder: SortingState;
}

export interface TableProps<T> extends TableHTMLAttributes<HTMLTableElement> {
    columns: Column<T>[];
    data: T[];
    showHeader?: boolean;
    isLoading?: boolean;
    isScrollable?: boolean;
    maxHeight?: string;
    isBorderless?: boolean;
    isExpandedInnerTable?: boolean;
    expandable?: ExpandableProps<T>;
    onRowClick?: (record: T) => void;
    /**
     * When set (including `null`), row highlight is controlled by this key
     * instead of the table's internal click-to-focus state. Pass `null` to
     * show no focused row.
     */
    focusedRowKey?: string | null;
    rowClassName?: (record: T) => string;
    rowDataTestId?: (record: T) => string;
    onExpand?: (record: T) => void;
    handleSortColumnChange?: ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => void;
    rowKey?: string | ((record: T) => string);
    rowSelection?: RowSelectionProps<T>;
    rowRefs?: React.MutableRefObject<HTMLTableRowElement[]>;
    headerRef?: React.RefObject<HTMLTableSectionElement>;
    footer?: React.ReactNode;
    renderScrollObserver?: () => React.ReactNode;
    /** Key of the column to sort by on first render. */
    defaultSortColumn?: string;
    /** Sort order applied to `defaultSortColumn` on first render. Defaults to ascending. */
    defaultSortOrder?: SortingState;
    /** Called with the rows in display order (tree children sorted too) whenever data or sort state changes. */
    onSortedDataChange?: (sortedData: T[], sortState: SortState) => void;
    /** Rendered in place of the rows when `data` is empty. */
    emptyState?: React.ReactNode;
    /** Extra attributes (id, data-*, etc.) for each body row. */
    rowProps?: (record: T, index: number) => RowHTMLProps;
    /** Table width when it should scroll horizontally (e.g. 'max-content' or '1200px'). */
    scrollX?: string;
}

export interface RowSelectionProps<T> {
    selectedRowKeys: string[];
    onChange?: (selectedKeys: string[], selectedRows: T[]) => void;
    getCheckboxProps?: (T) => {
        disabled: boolean;
    };
}

export interface ExpandIconProps<T> {
    record: T;
    expanded: boolean;
    /** Whether the row has children to expand. */
    expandable: boolean;
    depth: number;
    onExpand: (record: T, event?: React.MouseEvent) => void;
}

export interface ExpandableProps<T> {
    expandedRowRender?: (record: T, index: number) => React.ReactNode;
    rowExpandable?: (record: T) => boolean;
    /** Initially expanded row keys (by `rowKey`) when expansion is uncontrolled. */
    defaultExpandedRowKeys?: string[];
    expandIconPosition?: 'start' | 'end'; // Configurable position of the expand icon
    /** @deprecated Matches rows by `record.name`; prefer `expandedRowKeys`. */
    expandedGroupIds?: string[];
    /** Controlled expanded row keys (by `rowKey`). */
    expandedRowKeys?: string[];
    /** Called when a row is expanded or collapsed. */
    onExpand?: (expanded: boolean, record: T) => void;
    /**
     * Enables tree data: rows holding an array under this field (e.g. 'children') render their
     * children as nested, indented rows. Provide `rowKey` so expansion survives sorting.
     */
    childrenColumnName?: string;
    /** Tree data only: expand every row with children on first render. */
    defaultExpandAllRows?: boolean;
    /** Tree data only: indentation per nesting level, in px. Defaults to 16. */
    indentSize?: number;
    /** Tree data only: custom expand toggle rendered at the start of the first column. */
    expandIcon?: (props: ExpandIconProps<T>) => React.ReactNode;
}

export enum SortingState {
    ASCENDING = 'ascending',
    DESCENDING = 'descending',
    ORIGINAL = 'original',
}
