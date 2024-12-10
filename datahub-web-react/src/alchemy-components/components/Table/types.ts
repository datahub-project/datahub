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
}
