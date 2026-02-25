import { DragEndEvent, PointerSensor, closestCenter, useSensor, useSensors } from '@dnd-kit/core';
import { restrictToParentElement, restrictToVerticalAxis } from '@dnd-kit/modifiers';
import { arrayMove, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { useCallback } from 'react';

type Props = {
    items: string[];
    onChange: (newItems: string[]) => void;
};

export function useVerticalDragAndDrop({ items, onChange }: Props) {
    const sensors = useSensors(useSensor(PointerSensor));

    const handleDragEnd = useCallback(
        (event: DragEndEvent) => {
            const { active, over } = event;
            if (active.id !== over?.id && over) {
                const oldIndex = items.indexOf(String(active.id));
                const newIndex = items.indexOf(String(over.id));
                if (oldIndex !== -1 && newIndex !== -1) {
                    const newItems = arrayMove(items, oldIndex, newIndex);
                    onChange(newItems);
                }
            }
        },
        [items, onChange],
    );

    return {
        sensors,
        handleDragEnd,
        strategy: verticalListSortingStrategy,
        collisionDetection: closestCenter,
        modifiers: [restrictToVerticalAxis, restrictToParentElement],
    };
}
