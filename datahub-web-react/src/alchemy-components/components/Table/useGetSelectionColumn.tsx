/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    const { isSelectAll, isSelectAllDisabled, isIntermediate, handleSelectAll, handleRowSelect, selectedRowKeys } =
        useRowSelection(data, rowKey, rowSelection);

    const selectionColumn = {
        title: (
            <Checkbox
                isChecked={isSelectAll}
                isIntermediate={isIntermediate}
                isDisabled={isSelectAllDisabled}
                onCheckboxChange={handleSelectAll}
                size="xs"
            />
        ),
        key: 'row-selection',
        render: (record: T, index: number) => {
            return (
                <CheckboxWrapper>
                    <Checkbox
                        isChecked={selectedRowKeys.includes(getRowKey(record, index, rowKey))}
                        isDisabled={!!rowSelection && rowSelection?.getCheckboxProps?.(record)?.disabled}
                        onCheckboxChange={() => handleRowSelect(record, index)}
                        size="xs"
                    />
                </CheckboxWrapper>
            );
        },
        width: '48px',
        maxWidth: '60px',
        dataTestId: 'table-selection-column',
    };

    return rowSelection ? [selectionColumn] : [];
};
