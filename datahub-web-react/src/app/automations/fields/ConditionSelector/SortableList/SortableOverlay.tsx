import { DragOverlay, defaultDropAnimationSideEffects } from '@dnd-kit/core';
import type { DropAnimation } from '@dnd-kit/core';
import React from 'react';

const dropAnimationConfig: DropAnimation = {
    sideEffects: defaultDropAnimationSideEffects({
        styles: {
            active: {
                opacity: '0.4',
            },
        },
    }),
};

export function SortableOverlay({ children }: any) {
    return <DragOverlay dropAnimation={dropAnimationConfig}>{children}</DragOverlay>;
}
