import { Column, SortingState } from '@components/components/Table/types';

export const getRowKey = <T>(record: T, index: number, rowKey?: string | ((record: T) => string)): string => {
    if (typeof rowKey === 'function') return rowKey(record);
    if (typeof rowKey === 'string') return record[rowKey];
    return index.toString();
};

export const getTreeChildren = <T>(record: T, childrenColumnName?: string): T[] | undefined => {
    if (!childrenColumnName) return undefined;
    const children = (record as Record<string, unknown> | undefined)?.[childrenColumnName];
    return Array.isArray(children) ? (children as T[]) : undefined;
};

export const handleActiveSort = (
    key: string,
    sortColumn: string | null,
    setSortColumn: React.Dispatch<React.SetStateAction<string | null>>,
    setSortOrder: React.Dispatch<React.SetStateAction<SortingState>>,
) => {
    if (sortColumn === key) {
        // Toggle sort order
        setSortOrder((prevOrder) => {
            if (prevOrder === SortingState.ASCENDING) return SortingState.DESCENDING;
            if (prevOrder === SortingState.DESCENDING) return SortingState.ORIGINAL;
            return SortingState.ASCENDING;
        });
    } else {
        // Set new column and default sort order
        setSortColumn(key);
        setSortOrder(SortingState.ASCENDING);
    }
};

export const getSortedData = <T>(
    columns: Column<T>[],
    data: T[],
    sortColumn: string | null,
    sortOrder: SortingState,
    childrenColumnName?: string,
): T[] => {
    if (sortOrder === SortingState.ORIGINAL || !sortColumn) {
        return data;
    }

    const activeColumn = columns.find((column) => column.key === sortColumn);

    // Sort based on the order and column sorter
    if (activeColumn && typeof activeColumn.sorter === 'function') {
        const sorterFn = activeColumn.sorter;
        // Tree children are sorted within their parent, at every level.
        const sortLevel = (rows: T[]): T[] => {
            const sorted = rows
                .slice()
                .sort((a, b) => (sortOrder === SortingState.ASCENDING ? sorterFn(a, b) : sorterFn(b, a)));
            if (!childrenColumnName) return sorted;
            return sorted.map((row) => {
                const children = getTreeChildren(row, childrenColumnName);
                return children ? { ...row, [childrenColumnName]: sortLevel(children) } : row;
            });
        };
        return sortLevel(data);
    }

    return data;
};

export type FlatRow<T> = {
    record: T;
    key: string;
    depth: number;
    hasChildren: boolean;
};

// Without a rowKey, tree rows fall back to their position path (e.g. "0-2-1") so keys stay stable
// as other rows expand and collapse.
const getTreeRowKey = <T>(record: T, path: string, rowKey?: string | ((record: T) => string)): string =>
    rowKey ? getRowKey(record, 0, rowKey) : path;

/** Flattens tree data into display order, descending only into expanded rows. */
export const flattenTreeRows = <T>(
    data: T[],
    childrenColumnName: string,
    expandedKeys: Set<string> | 'all',
    rowKey?: string | ((record: T) => string),
): FlatRow<T>[] => {
    const result: FlatRow<T>[] = [];
    const visit = (rows: T[], depth: number, parentPath: string) => {
        rows.forEach((record, i) => {
            const key = getTreeRowKey(record, parentPath ? `${parentPath}-${i}` : `${i}`, rowKey);
            const children = getTreeChildren(record, childrenColumnName);
            const hasChildren = !!children?.length;
            result.push({ record, key, depth, hasChildren });
            if (children && hasChildren && (expandedKeys === 'all' || expandedKeys.has(key)))
                visit(children, depth + 1, key);
        });
    };
    visit(data, 0, '');
    return result;
};

/** Keys of every row (at any depth) that has children. */
export const getExpandableRowKeys = <T>(
    data: T[],
    childrenColumnName: string,
    rowKey?: string | ((record: T) => string),
): string[] =>
    flattenTreeRows(data, childrenColumnName, 'all', rowKey)
        .filter((row) => row.hasChildren)
        .map((row) => row.key);

/** Left offsets for `fixed: 'left'` columns: the summed widths of the fixed columns before each one. */
export const getFixedLeftOffsets = <T>(columns: Column<T>[]): (string | undefined)[] => {
    const precedingWidths: string[] = [];
    return columns.map((column) => {
        if (column.fixed !== 'left') return undefined;
        const offset = precedingWidths.length ? `calc(${precedingWidths.join(' + ')})` : '0px';
        precedingWidths.push(column.width || '0px');
        return offset;
    });
};

export const renderCell = <T>(column: Column<T>, row: T, index: number) => {
    const { render, dataIndex } = column;

    let cellData;

    if (dataIndex) {
        cellData = row[dataIndex];

        if (typeof dataIndex === 'string') {
            cellData = dataIndex.split('.').reduce((acc, prop) => acc && acc[prop], row);
        }

        if (Array.isArray(dataIndex)) {
            cellData = dataIndex.reduce((acc, prop) => acc && acc[prop], row);
        }
    }

    if (render) {
        return render(row, index);
    }

    return cellData;
};
