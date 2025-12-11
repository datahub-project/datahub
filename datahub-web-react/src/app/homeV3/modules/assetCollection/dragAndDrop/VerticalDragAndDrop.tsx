/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DndContext } from '@dnd-kit/core';
import { SortableContext } from '@dnd-kit/sortable';
import React from 'react';

import { useVerticalDragAndDrop } from '@app/homeV3/modules/assetCollection/dragAndDrop/useVerticalDragAndDrop';

type Props = {
    items: string[];
    onChange: (newItems: string[]) => void;
    children: React.ReactNode;
};

export default function VerticalDragAndDrop({ items, onChange, children }: Props) {
    const { sensors, handleDragEnd, strategy, collisionDetection, modifiers } = useVerticalDragAndDrop({
        items,
        onChange,
    });
    return (
        <DndContext
            sensors={sensors}
            collisionDetection={collisionDetection}
            onDragEnd={handleDragEnd}
            modifiers={modifiers}
        >
            <SortableContext items={items} strategy={strategy}>
                {children}
            </SortableContext>
        </DndContext>
    );
}
