import React from 'react';
import { Checkbox } from '@components';
import { Column, RowSelectionProps } from './types';
import { CheckboxWrapper } from './components';
import { getRowKey } from './utils';
import { useRowSelection } from './useRowSelection';

export const useGetSelectionColumn = <T,>(
    data: T[],
    rowKey?: string | ((record: T) => string),
    rowSelection?: RowSelectionProps<T>,
): Column<T>[] => {
    const { isSelectAll, isIntermediate, handleSelectAll, handleRowSelect, selectedRowKeys } = useRowSelection(
        data,
        rowKey,
        rowSelection,
    );

    const selectionColumn = {
        title: (
            <Checkbox
                isChecked={isSelectAll}
                isIntermediate={isIntermediate}
                onCheckboxChange={handleSelectAll}
                size="xs"
            />
        ),
        key: 'row-selection',
        render: (record: T, index: number) => (
            <CheckboxWrapper>
                <Checkbox
                    isChecked={selectedRowKeys.includes(getRowKey(record, index, rowKey))}
                    onCheckboxChange={() => handleRowSelect(record, index)}
                    size="xs"
                />
            </CheckboxWrapper>
        ),
        width: '48px',
        maxWidth: '60px',
    };

    return rowSelection ? [selectionColumn] : [];
};
