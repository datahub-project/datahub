import { DndContext, DragOverlay, MouseSensor, TouchSensor, useSensor, useSensors } from '@dnd-kit/core';
import React, { memo } from 'react';
import styled from 'styled-components';

import { useDragAndDrop } from '@app/homeV3/context/hooks/useDragAndDrop';
import Module from '@app/homeV3/module/Module';
import { pointerClosestCollisionDetector } from '@app/homeV3/template/components/utils';

// Styled wrapper for drag overlay to make it look like it's floating
const DragOverlayWrapper = styled.div`
    transform: rotate(5deg) scale(1.05);
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
    border-radius: 8px;
    opacity: 0.95;
    /* Performance optimizations */
    will-change: transform;
    pointer-events: none;
`;

interface Props {
    children: React.ReactNode;
}

function DragAndDropProvider({ children }: Props) {
    const mouseSensor = useSensor(MouseSensor, {
        // Require the mouse to move by 2 pixels before activating
        activationConstraint: {
            distance: 2,
        },
    });

    const touchSensor = useSensor(TouchSensor, {
        // Press delay of 50ms, with tolerance of 2px of movement
        activationConstraint: {
            delay: 50,
            tolerance: 2,
        },
    });

    const sensors = useSensors(mouseSensor, touchSensor);

    const { activeModule, handleDragStart, handleDragEnd } = useDragAndDrop();

    return (
        <DndContext
            collisionDetection={pointerClosestCollisionDetector}
            onDragStart={handleDragStart}
            onDragEnd={handleDragEnd}
            sensors={sensors}
        >
            {children}

            <DragOverlay>
                {activeModule && (
                    <DragOverlayWrapper>
                        <Module module={activeModule.module} position={activeModule.position} />
                    </DragOverlayWrapper>
                )}
            </DragOverlay>
        </DndContext>
    );
}

export default memo(DragAndDropProvider);
