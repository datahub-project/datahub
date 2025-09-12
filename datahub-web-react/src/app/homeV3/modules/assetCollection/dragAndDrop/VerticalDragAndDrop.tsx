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
