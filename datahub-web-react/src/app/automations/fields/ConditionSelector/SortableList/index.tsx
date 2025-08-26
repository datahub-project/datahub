import { DndContext, KeyboardSensor, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import type { Active, UniqueIdentifier } from '@dnd-kit/core';
import { SortableContext, arrayMove, sortableKeyboardCoordinates } from '@dnd-kit/sortable';
import React, { useMemo, useState } from 'react';
import type { ReactNode } from 'react';

import { DragHandle, SortableItem } from '@app/automations/fields/ConditionSelector/SortableList/SortableItem';
import { SortableOverlay } from '@app/automations/fields/ConditionSelector/SortableList/SortableOverlay';

interface BaseItem {
    id: UniqueIdentifier;
}

interface Props<T extends BaseItem> {
    items: T[];
    groupId?: UniqueIdentifier;
    onChange(items: T[], id?: UniqueIdentifier): void;
    renderItem(item: T): ReactNode;
}

export function SortableList<T extends BaseItem>({ items, groupId, onChange, renderItem }: Props<T>) {
    const [active, setActive] = useState<Active | null>(null);

    const activeItem = useMemo(() => items.find((item) => item.id === active?.id), [active, items]);

    const sensors = useSensors(
        useSensor(PointerSensor),
        useSensor(KeyboardSensor, {
            coordinateGetter: sortableKeyboardCoordinates,
        }),
    );

    const onDragStart = ({ active: a }) => setActive(a);

    const onDragEnd = ({ active: a, over }) => {
        if (over && a.id !== over?.id) {
            const activeIndex = items.findIndex(({ id }) => id === a.id);
            const overIndex = items.findIndex(({ id }) => id === over.id);
            const newArray = arrayMove(items, activeIndex, overIndex);
            onChange(newArray, groupId);
        }
        setActive(null);
    };

    const onDragCancel = () => setActive(null);

    return (
        <DndContext sensors={sensors} onDragStart={onDragStart} onDragEnd={onDragEnd} onDragCancel={onDragCancel}>
            <SortableContext items={items}>
                {items.map((item) => (
                    <React.Fragment key={item.id}>{renderItem(item)}</React.Fragment>
                ))}
            </SortableContext>
            <SortableOverlay>{activeItem ? renderItem(activeItem) : null}</SortableOverlay>
        </DndContext>
    );
}

SortableList.Item = SortableItem;
SortableList.DragHandle = DragHandle;
