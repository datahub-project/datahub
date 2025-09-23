import { Column, SortingState } from '@components/components/Table/types';

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
