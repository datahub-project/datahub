import { Column, SortingState, TableRowGroup, TableSortState } from '@components/components/Table/types';

/** Header click cycles a column through ascending, descending and back to the original order. */
export const getNextSortState = (key: string, { sortColumn, sortOrder }: TableSortState): TableSortState => {
    if (sortColumn !== key) return { sortColumn: key, sortOrder: SortingState.ASCENDING };
    if (sortOrder === SortingState.ASCENDING) return { sortColumn: key, sortOrder: SortingState.DESCENDING };
    if (sortOrder === SortingState.DESCENDING) return { sortColumn: key, sortOrder: SortingState.ORIGINAL };
    return { sortColumn: key, sortOrder: SortingState.ASCENDING };
};

export const getSortedData = <T>(
    columns: Column<T>[],
    data: T[],
    sortColumn: string | null,
    sortOrder: SortingState,
) => {
    if (sortOrder === SortingState.ORIGINAL || !sortColumn) {
        return data;
    }

    const activeColumn = columns.find((column) => column.key === sortColumn);

    // Sort based on the order and column sorter
    if (activeColumn && typeof activeColumn.sorter === 'function') {
        const sorterFn = activeColumn.sorter;
        return data.slice().sort((a, b) => {
            return sortOrder === SortingState.ASCENDING ? sorterFn(a, b) : sorterFn(b, a);
        });
    }

    return data;
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

export const getRowKey = <T>(record: T, index: number, rowKey?: string | ((record: T) => string)): string => {
    if (typeof rowKey === 'function') return rowKey(record);
    if (typeof rowKey === 'string') return record[rowKey];
    return index.toString();
};

/**
 * Splits rows into groups, keeping the incoming (already sorted) row order inside each group.
 * Without a comparator, groups follow the order in which their first row appears.
 */
export const getGroupedRows = <T>(
    rows: T[],
    getGroupKey: (record: T) => string,
    groupOrder?: (a: TableRowGroup<T>, b: TableRowGroup<T>) => number,
): TableRowGroup<T>[] => {
    const groupsByKey = new Map<string, TableRowGroup<T>>();
    rows.forEach((row) => {
        const key = getGroupKey(row);
        const group = groupsByKey.get(key);
        if (group) {
            group.rows.push(row);
        } else {
            groupsByKey.set(key, { key, rows: [row] });
        }
    });
    const groups = Array.from(groupsByKey.values());
    return groupOrder ? groups.sort(groupOrder) : groups;
};
