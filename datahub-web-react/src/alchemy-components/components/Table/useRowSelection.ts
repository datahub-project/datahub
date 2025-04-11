import { useCallback, useMemo } from 'react';
import { RowSelectionProps } from './types';
import { getRowKey } from './utils';

export const useRowSelection = <T>(
    data: T[],
    rowKey?: string | ((record: T) => string),
    rowSelection?: RowSelectionProps<T>,
) => {
    const { selectedRowKeys = [], onChange } = rowSelection || {};

    const isSelectAll = useMemo(
        () => data.length > 0 && selectedRowKeys.length === data.length,
        [selectedRowKeys, data],
    );

    const isIntermediate = useMemo(
        () => selectedRowKeys.length > 0 && selectedRowKeys.length < data.length,
        [selectedRowKeys, data],
    );

    const handleSelectAll = useCallback(() => {
        if (!rowSelection) return;
        const newSelectedKeys = isSelectAll ? [] : data.map((row, index) => getRowKey(row, index, rowKey));
        onChange?.(newSelectedKeys, data);
    }, [rowSelection, isSelectAll, data, onChange, rowKey]);

    const handleRowSelect = useCallback(
        (record: T, index: number) => {
            if (!rowSelection) return;
            const key = getRowKey(record, index, rowKey);
            const newSelectedKeys = selectedRowKeys.includes(key)
                ? selectedRowKeys.filter((k) => k !== key)
                : [...selectedRowKeys, key];

            const selectedRows = data.filter((row, idx) => newSelectedKeys.includes(getRowKey(row, idx, rowKey)));
            onChange?.(newSelectedKeys, selectedRows);
        },
        [rowSelection, rowKey, selectedRowKeys, data, onChange],
    );

    return {
        isSelectAll,
        isIntermediate,
        handleSelectAll,
        handleRowSelect,
        selectedRowKeys,
    };
};
