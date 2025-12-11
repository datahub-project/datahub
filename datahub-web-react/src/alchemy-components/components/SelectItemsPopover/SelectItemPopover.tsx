/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import '@components/components/SelectItemsPopover/select-items-popover.less';

import { Popover } from 'antd';
import { CheckboxValueType } from 'antd/lib/checkbox/Group';
import React, { ReactNode } from 'react';

import { SelectItems } from '@components/components/SelectItemsPopover/SelectItems';

import { Entity, EntityType } from '@src/types.generated';

export type SelectItemPopoverProps = {
    entities: Entity[];
    selectedItems: any[];
    visible: boolean;
    onVisibleChange: (isOpen: boolean) => void;
    refetch?: () => void;
    onClose?: () => void;
    entityType: EntityType;
    handleSelectionChange: ({
        selectedItems,
        removedItems,
    }: {
        selectedItems: CheckboxValueType[];
        removedItems: CheckboxValueType[];
    }) => void;
    renderOption?: (option: { value: string; label: ReactNode | string; item?: any }) => React.ReactNode;
    children: React.ReactNode;
};

export const SelectItemPopover = ({
    visible,
    onVisibleChange,
    entities,
    selectedItems,
    refetch,
    onClose,
    entityType,
    handleSelectionChange,
    renderOption,
    children,
}: SelectItemPopoverProps) => {
    return (
        <Popover
            trigger="click"
            open={visible}
            onOpenChange={onVisibleChange}
            overlayClassName="select-items-popover"
            content={
                <SelectItems
                    key={`${visible}`}
                    entities={entities}
                    selectedItems={selectedItems}
                    refetch={refetch}
                    onClose={onClose}
                    entityType={entityType}
                    handleSelectionChange={handleSelectionChange}
                    renderOption={renderOption}
                />
            }
            showArrow={false}
        >
            {children}
        </Popover>
    );
};
