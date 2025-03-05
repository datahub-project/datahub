import React from 'react';
import { Checkbox } from '@components';
import { useRowSelection } from './useRowSelection';
import { Column, RowSelectionProps } from './types';
import { CheckboxWrapper } from './components';
import { getRowKey } from './utils';

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
        title: <Checkbox isChecked={isSelectAll} isIntermediate={isIntermediate} onCheckboxChange={handleSelectAll} />,
        key: 'row-selection',
        render: (record: T, index: number) => (
            <CheckboxWrapper>
                <Checkbox
                    isChecked={selectedRowKeys.includes(getRowKey(record, index, rowKey))}
                    onCheckboxChange={() => handleRowSelect(record, index)}
                />
            </CheckboxWrapper>
        ),
        width: '60px',
    };

    return rowSelection ? [selectionColumn] : [];
};
