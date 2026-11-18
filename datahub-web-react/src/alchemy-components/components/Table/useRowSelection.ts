import { useCallback, useMemo } from 'react';

import { RowSelectionProps } from '@components/components/Table/types';
import { getRowKey } from '@components/components/Table/utils';

export const useRowSelection = <T>(
    data: T[],
    rowKey?: string | ((record: T) => string),
    rowSelection?: RowSelectionProps<T>,
) => {
    const { selectedRowKeys = [], onChange, getCheckboxProps } = rowSelection || {};
    const disabledRows = useMemo(
        () => data.filter((row) => getCheckboxProps?.(row)?.disabled),
        [data, getCheckboxProps],
    );

    const selectableRowsKeys = useMemo(
        () =>
            rowSelection
                ? data
                      .filter((row) => !rowSelection.getCheckboxProps?.(row).disabled)
                      .map((row, index) => getRowKey(row, index, rowKey))
                : [],
        [data, rowSelection, rowKey],
    );

    const isSelectAll = useMemo(
        () => selectableRowsKeys.length > 0 && selectableRowsKeys.every((key) => selectedRowKeys.includes(key)),
        [selectedRowKeys, selectableRowsKeys],
    );

    const isSelectAllDisabled = useMemo(() => data.length === disabledRows.length, [data, disabledRows]);

    const isIntermediate = useMemo(
        () =>
            selectableRowsKeys.some((key) => selectedRowKeys.includes(key)) &&
            !selectableRowsKeys.every((key) => selectedRowKeys.includes(key)),
        [selectedRowKeys, selectableRowsKeys],
    );

    const handleSelectAll = useCallback(() => {
        if (!rowSelection) return;

        const currentPageKeys = selectableRowsKeys;
        let newSelectedKeys;

        if (isSelectAll) {
            // Deselect only the current page's rows
            newSelectedKeys = selectedRowKeys.filter((key) => !currentPageKeys.includes(key));
        } else {
            // Add current page's rows to existing selection (removing duplicates)
            const keysToAdd = currentPageKeys.filter((key) => !selectedRowKeys.includes(key));
            newSelectedKeys = [...selectedRowKeys, ...keysToAdd];
        }

        const selectedRows = data.filter((row, idx) => newSelectedKeys.includes(getRowKey(row, idx, rowKey)));

        onChange?.(newSelectedKeys, selectedRows);
    }, [rowSelection, isSelectAll, selectedRowKeys, selectableRowsKeys, data, onChange, rowKey]);

    // Index fallback for `getRowKey` when no `rowKey` is given, matching the keys `handleSelectAll` uses.
    const rowIndexes = useMemo(() => new Map(data.map((row, index) => [row, index])), [data]);

    const getSelectableKeys = useCallback(
        (rows: T[]) =>
            rows
                .filter((row) => !getCheckboxProps?.(row)?.disabled)
                .map((row) => getRowKey(row, rowIndexes.get(row) ?? 0, rowKey)),
        [getCheckboxProps, rowIndexes, rowKey],
    );

    const getGroupSelectionState = useCallback(
        (rows: T[]) => {
            const keys = getSelectableKeys(rows);
            const selectedCount = keys.filter((key) => selectedRowKeys.includes(key)).length;
            return {
                isChecked: keys.length > 0 && selectedCount === keys.length,
                isIntermediate: selectedCount > 0 && selectedCount < keys.length,
                isDisabled: keys.length === 0,
            };
        },
        [getSelectableKeys, selectedRowKeys],
    );

    const handleGroupSelect = useCallback(
        (rows: T[]) => {
            if (!rowSelection) return;
            const keys = getSelectableKeys(rows);
            const isGroupSelected = keys.length > 0 && keys.every((key) => selectedRowKeys.includes(key));
            const newSelectedKeys = isGroupSelected
                ? selectedRowKeys.filter((key) => !keys.includes(key))
                : [...selectedRowKeys, ...keys.filter((key) => !selectedRowKeys.includes(key))];
            const selectedRows = data.filter((row, idx) => newSelectedKeys.includes(getRowKey(row, idx, rowKey)));
            onChange?.(newSelectedKeys, selectedRows);
        },
        [rowSelection, getSelectableKeys, selectedRowKeys, data, onChange, rowKey],
    );

    const handleRowSelect = (record: T, index: number) => {
        if (!rowSelection) return;
        if (rowSelection.getCheckboxProps?.(record).disabled) return;
        const key = getRowKey(record, index, rowKey);
        const newSelectedKeys = selectedRowKeys.includes(key)
            ? selectedRowKeys.filter((k) => k !== key)
            : [...selectedRowKeys, key];

        const selectedRows = data.filter((row, idx) => newSelectedKeys.includes(getRowKey(row, idx, rowKey)));
        onChange?.(newSelectedKeys, selectedRows);
    };

    return {
        isSelectAll,
        isSelectAllDisabled,
        isIntermediate,
        handleSelectAll,
        handleRowSelect,
        selectedRowKeys,
        getGroupSelectionState,
        handleGroupSelect,
    };
};
