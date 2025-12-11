/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
