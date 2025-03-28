import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import React, { TableHTMLAttributes } from 'react';

export interface Column<T> {
    title: string | React.ReactNode;
    key: string;
    dataIndex?: string;
    render?: (record: T, index: number) => React.ReactNode;
    width?: string;
    maxWidth?: string;
    sorter?: (a: T, b: T) => number;
    alignment?: AlignmentOptions;
    tooltipTitle?: string;
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
    rowClassName?: (record: T) => string;
    rowDataTestId?: (record: T) => string;
    onExpand?: (record: T) => void;
    handleSortColumnChange?: ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => void;
    rowKey?: string | ((record: T) => string);
    rowSelection?: RowSelectionProps<T>;
    rowRefs?: React.MutableRefObject<HTMLTableRowElement[]>;
    headerRef?: React.RefObject<HTMLTableSectionElement>;
}

export interface RowSelectionProps<T> {
    selectedRowKeys: string[];
    onChange?: (selectedKeys: string[], selectedRows: T[]) => void;
}

export interface ExpandableProps<T> {
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
