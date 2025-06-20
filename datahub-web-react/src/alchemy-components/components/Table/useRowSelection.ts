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

    const isSelectAll = useMemo(
        () =>
            data.length > 0 &&
            selectedRowKeys.length !== 0 &&
            selectedRowKeys.length === data.length - disabledRows.length,
        [selectedRowKeys, data, disabledRows],
    );

    const isSelectAllDisabled = useMemo(() => data.length === disabledRows.length, [data, disabledRows]);

    const isIntermediate = useMemo(
        () => selectedRowKeys.length > 0 && selectedRowKeys.length < data.length - disabledRows.length,
        [selectedRowKeys, data, disabledRows],
    );

    const handleSelectAll = useCallback(() => {
        if (!rowSelection) return;
        const newSelectedKeys = isSelectAll
            ? []
            : data
                  .filter((row) => !rowSelection.getCheckboxProps?.(row).disabled)
                  .map((row, index) => getRowKey(row, index, rowKey));
        onChange?.(newSelectedKeys, data);
    }, [rowSelection, isSelectAll, data, onChange, rowKey]);

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
    };
};
