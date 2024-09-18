import React, { createContext, useContext, useMemo } from 'react';
import type { CSSProperties, PropsWithChildren } from 'react';

import type { DraggableSyntheticListeners, UniqueIdentifier } from '@dnd-kit/core';

import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

// TODO: Bring this into this component so it's fully resuable / self-contained
import { SortButton } from '@app/automations/sharedComponents';

interface Props {
    id: UniqueIdentifier;
    isSortable?: boolean;
}

interface Context {
    attributes: Record<string, any>;
    listeners: DraggableSyntheticListeners;
    ref(node: HTMLElement | null): void;
}

const SortableItemContext = createContext<Context>({
    attributes: {},
    listeners: undefined,
    ref() {},
});

export function SortableItem({ children, id, isSortable = true }: PropsWithChildren<Props>) {
    const { attributes, isDragging, listeners, setNodeRef, setActivatorNodeRef, transform, transition } = useSortable({
        id,
    });

    const context = useMemo(
        () => ({
            attributes,
            listeners,
            ref: setActivatorNodeRef,
        }),
        [attributes, listeners, setActivatorNodeRef],
    );

    const style: CSSProperties = {
        opacity: isDragging ? 0.4 : undefined,
        transform: CSS.Translate.toString(transform),
        transition,
    };

    // Return nothing if ID isn't set
    if (!id) return null;

    // Return unsortable item if isSortable is false
    if (isSortable === false) {
        return <div>{children}</div>;
    }

    // Return sortable item
    return (
        <SortableItemContext.Provider value={context}>
            <div ref={setNodeRef} style={style}>
                {children}
            </div>
        </SortableItemContext.Provider>
    );
}

export function DragHandle() {
    const { attributes, listeners, ref } = useContext(SortableItemContext);

    return (
        <SortButton className="DragHandle" {...attributes} {...listeners} ref={ref}>
            <svg viewBox="0 0 20 20" width="12">
                <path d="M7 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 2zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 14zm6-8a2 2 0 1 0-.001-4.001A2 2 0 0 0 13 6zm0 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 14z" />
            </svg>
        </SortButton>
    );
}
