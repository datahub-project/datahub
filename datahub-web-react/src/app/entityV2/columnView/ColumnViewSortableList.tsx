import {
    DndContext,
    DragEndEvent,
    KeyboardSensor,
    MouseSensor,
    TouchSensor,
    closestCenter,
    useSensor,
    useSensors,
} from '@dnd-kit/core';
import { restrictToParentElement, restrictToVerticalAxis } from '@dnd-kit/modifiers';
import { SortableContext, arrayMove, sortableKeyboardCoordinates, verticalListSortingStrategy } from '@dnd-kit/sortable';
import React, { useCallback } from 'react';

type Props = {
    items: string[];
    onChange: (newItems: string[]) => void;
    children: React.ReactNode;
};

/**
 * Column Views' vertical sortable list. Same shape as homeV3's VerticalDragAndDrop but with
 * touch (long-press) and keyboard sensors, so reordering works without a mouse. Pair with
 * `moveItem` for the explicit up / down / top buttons.
 */
export default function ColumnViewSortableList({ items, onChange, children }: Props) {
    const sensors = useSensors(
        useSensor(MouseSensor),
        useSensor(TouchSensor, { activationConstraint: { delay: 250, tolerance: 5 } }),
        useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
    );

    const handleDragEnd = useCallback(
        (event: DragEndEvent) => {
            const { active, over } = event;
            if (!over || active.id === over.id) return;
            const oldIndex = items.indexOf(String(active.id));
            const newIndex = items.indexOf(String(over.id));
            if (oldIndex !== -1 && newIndex !== -1) onChange(arrayMove(items, oldIndex, newIndex));
        },
        [items, onChange],
    );

    return (
        <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={handleDragEnd}
            modifiers={[restrictToVerticalAxis, restrictToParentElement]}
        >
            <SortableContext items={items} strategy={verticalListSortingStrategy}>
                {children}
            </SortableContext>
        </DndContext>
    );
}

export type MoveDirection = 'up' | 'down' | 'top';

/** Reorder without drag: returns a new id list, or the same list when the move is a no-op. */
export function moveItem(items: string[], id: string, direction: MoveDirection): string[] {
    const from = items.indexOf(id);
    if (from === -1) return items;
    const to = direction === 'top' ? 0 : direction === 'up' ? from - 1 : from + 1;
    if (to < 0 || to >= items.length || to === from) return items;
    return arrayMove(items, from, to);
}
