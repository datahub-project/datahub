import React from 'react';
import styled from 'styled-components';
import { useDroppable, useDndContext } from '@dnd-kit/core';

import Module from '@app/homeV3/module/Module';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { ModulePositionInput } from '@app/homeV3/template/types';
import { WrappedRow } from '@app/homeV3/templateRow/types';
import { spacing } from '@components';

const RowWrapper = styled.div`
    display: flex;
    align-items: flex-start;
    gap: ${spacing.md};
    position: relative;
`;

const DropZone = styled.div<{ $isOver?: boolean; $canDrop?: boolean }>`
    min-width: 8px;
    height: 316px;
    border-radius: 8px;
    transition: all 0.2s ease;
    
    ${({ $isOver, $canDrop }) => {
        if ($isOver && $canDrop) {
            return `
                background-color: rgba(59, 130, 246, 0.1);
                border: 2px dashed #3b82f6;
                min-width: 16px;
            `;
        }
        if ($canDrop) {
            return `
                background-color: rgba(59, 130, 246, 0.05);
                border: 1px dashed #3b82f6;
                opacity: 0.7;
            `;
        }
        return `
            background-color: transparent;
            border: 1px solid transparent;
        `;
    }}
`;

interface Props {
    row: WrappedRow;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    rowIndex: number;
}

interface DropZoneProps {
    rowIndex: number;
    moduleIndex?: number;
    disabled?: boolean;
}

function ModuleDropZone({ rowIndex, moduleIndex, disabled }: DropZoneProps) {
    const {
        isOver,
        setNodeRef,
    } = useDroppable({
        id: `drop-zone-${rowIndex}-${moduleIndex ?? 'end'}`,
        disabled,
        data: {
            rowIndex,
            moduleIndex,
        },
    });

    return (
        <DropZone
            ref={setNodeRef}
            $isOver={isOver}
            $canDrop={!disabled}
        />
    );
}

export default function TemplateRow({ row, modulesAvailableToAdd, rowIndex }: Props) {
    const maxModulesPerRow = 3;
    const currentModuleCount = row.modules.length;
    const isRowFull = currentModuleCount >= maxModulesPerRow;
    
    // Get the active drag context to determine if we're dragging from the same row
    const { active } = useDndContext();
    const activeDragData = active?.data?.current as { position?: ModulePositionInput } | undefined;
    const isDraggingFromSameRow = activeDragData?.position?.rowIndex === rowIndex;
    
    // Only disable drop zones if the row is full AND we're not rearranging within the same row
    const shouldDisableDropZones = isRowFull && !isDraggingFromSameRow;

    return (
        <RowWrapper>
            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="left"
            />

            {/* Drop zone at the beginning of the row */}
            <ModuleDropZone 
                rowIndex={rowIndex} 
                moduleIndex={0} 
                disabled={shouldDisableDropZones}
            />

            {row.modules.map((module, moduleIndex) => {
                const position: ModulePositionInput = {
                    rowIndex,
                    rowSide: moduleIndex === 0 ? 'left' : 'right',
                    moduleIndex,
                };
                const key = `${module.urn}-${moduleIndex}`;
                
                return (
                    <React.Fragment key={key}>
                        <Module module={module} position={position} />
                        {/* Drop zone after each module (except the last one if row is full) */}
                        {(moduleIndex < row.modules.length - 1 || !shouldDisableDropZones) && (
                            <ModuleDropZone 
                                rowIndex={rowIndex} 
                                moduleIndex={moduleIndex + 1} 
                                disabled={shouldDisableDropZones && moduleIndex < row.modules.length - 1}
                            />
                        )}
                    </React.Fragment>
                );
            })}

            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="right"
            />
        </RowWrapper>
    );
}
