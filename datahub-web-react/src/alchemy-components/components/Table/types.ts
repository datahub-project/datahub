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
}

export interface RowSelectionProps<T> {
    selectedRowKeys: string[];
    onChange?: (selectedKeys: string[], selectedRows: T[]) => void;
    getCheckboxProps?: (T) => {
        disabled: boolean;
    };
}

interface ExpandableProps<T> {
    expandedRowRender?: (record: T, index: number) => React.ReactNode;
    rowExpandable?: (record: T) => boolean;
    defaultExpandedRowKeys?: string[];
    expandIconPosition?: 'start' | 'end'; // Configurable position of the expand icon
    expandedGroupIds?: string[];
}

export enum SortingState {
    ASCENDING = 'ascending',
    DESCENDING = 'descending',
    ORIGINAL = 'original',
}
