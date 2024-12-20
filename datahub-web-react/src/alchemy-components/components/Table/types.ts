import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { TableHTMLAttributes } from 'react';

export interface Column<T> {
    title: string;
    key: string;
    dataIndex?: string;
    render?: (record: T, index: number) => React.ReactNode;
    width?: string;
    sorter?: (a: T, b: T) => number;
    alignment?: AlignmentOptions;
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
    onExpand?: (record: T) => void;
    handleSortColumnChange?: ({ sortColumn, sortOrder }: { sortColumn: string; sortOrder: SortingState }) => void;
    rowRefs?: React.MutableRefObject<HTMLTableRowElement[]>;
    headerRef?: React.RefObject<HTMLTableSectionElement>;
}

export interface ExpandableProps<T> {
    expandedRowRender?: (record: T, index: number) => React.ReactNode;
    rowExpandable?: (record: T) => boolean;
    defaultExpandedRowKeys?: string[];
    expandIconPosition?: 'start' | 'end'; // Configurable position of the expand icon
    expandedRowKeys?: string[];
}

export enum SortingState {
    ASCENDING = 'ascending',
    DESCENDING = 'descending',
    ORIGINAL = 'original',
}
