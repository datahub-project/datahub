import { Checkbox } from '@components';
import React from 'react';

import { CheckboxWrapper } from '@components/components/Table/components';
import { Column, RowSelectionProps } from '@components/components/Table/types';
import { useRowSelection } from '@components/components/Table/useRowSelection';
import { getRowKey } from '@components/components/Table/utils';

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
