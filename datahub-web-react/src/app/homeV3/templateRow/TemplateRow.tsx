import { spacing } from '@components';
import { useDndContext, useDroppable } from '@dnd-kit/core';
import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import Module from '@app/homeV3/module/Module';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { ModulePositionInput } from '@app/homeV3/template/types';
import { WrappedRow } from '@app/homeV3/templateRow/types';

const RowWrapper = styled.div`
    display: flex;
    // align-items: flex-start;
    // position: relative;
    gap: ${spacing.xsm};
    flex: 1;
`;

const DropZone = styled.div<{ $isOver?: boolean; $canDrop?: boolean }>`
    // min-width: 2px;
    height: 316px;
    // border-radius: 8px;
    transition: all 0.2s ease;

    ${({ $isOver, $canDrop }) => {
        if ($isOver && $canDrop) {
            return `
                // min-width: 2px;
                background-color: rgba(59, 130, 246, 0.1);
                border: 2px solid #CAC3F1;
                // min-width: 16px;
            `;
        }
        if ($canDrop) {
            return `
                // background-color: rgba(59, 130, 246, 0.05);
                // border: 1px dashed #3b82f6;
                // opacity: 0.7;
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

// Memoized drop zone component to prevent unnecessary re-renders
const ModuleDropZone = memo(function ModuleDropZone({ rowIndex, moduleIndex, disabled }: DropZoneProps) {
    const { isOver, setNodeRef } = useDroppable({
        id: `drop-zone-${rowIndex}-${moduleIndex ?? 'end'}`,
        disabled,
        data: {
            rowIndex,
            moduleIndex,
        },
    });

    return <DropZone ref={setNodeRef} $isOver={isOver} $canDrop={!disabled} />;
});

// Optimized hook to get drag context with minimal re-renders
function useDragRowContext(rowIndex: number) {
    const { active } = useDndContext();

    return useMemo(() => {
        const activeDragData = active?.data?.current as { position?: ModulePositionInput } | undefined;
        return activeDragData?.position?.rowIndex === rowIndex;
    }, [active?.data?.current, rowIndex]);
}

// Memoized module wrapper to prevent unnecessary re-renders
const ModuleWrapper = memo(function ModuleWrapper({
    module,
    position,
}: {
    module: WrappedRow['modules'][0];
    position: ModulePositionInput;
}) {
    return <Module module={module} position={position} />;
});

function TemplateRow({ row, modulesAvailableToAdd, rowIndex }: Props) {
    const maxModulesPerRow = 3;
    const currentModuleCount = row.modules.length;
    const isRowFull = currentModuleCount >= maxModulesPerRow;

    // Optimized drag context check
    const isDraggingFromSameRow = useDragRowContext(rowIndex);

    // Memoize the drop zone disabled state calculation
    const shouldDisableDropZones = useMemo(
        () => isRowFull && !isDraggingFromSameRow,
        [isRowFull, isDraggingFromSameRow],
    );

    // Memoize module positions to prevent recalculation
    const modulePositions = useMemo(
        () =>
            row.modules.map((module, moduleIndex) => ({
                module,
                position: {
                    rowIndex,
                    rowSide: moduleIndex === 0 ? 'left' : 'right',
                    moduleIndex,
                } as ModulePositionInput,
                key: `${module.urn}-${moduleIndex}`,
            })),
        [row.modules, rowIndex],
    );

    return (
        <RowWrapper>
            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="left"
            />

            {/* Drop zone at the beginning of the row */}
            <ModuleDropZone rowIndex={rowIndex} moduleIndex={0} disabled={shouldDisableDropZones} />

            {modulePositions.map(({ module, position, key }, moduleIndex) => (
                <React.Fragment key={key}>
                    <ModuleWrapper module={module} position={position} />
                    {/* Drop zone after each module (except the last one if row is full) */}
                    {(moduleIndex < modulePositions.length - 1 || !shouldDisableDropZones) && (
                        <ModuleDropZone
                            rowIndex={rowIndex}
                            moduleIndex={moduleIndex + 1}
                            disabled={shouldDisableDropZones && moduleIndex < modulePositions.length - 1}
                        />
                    )}
                </React.Fragment>
            ))}

            <AddModuleButton
                orientation="vertical"
                modulesAvailableToAdd={modulesAvailableToAdd}
                rowIndex={rowIndex}
                rowSide="right"
            />
        </RowWrapper>
    );
}

// Export memoized component to prevent unnecessary re-renders from parent
export default memo(TemplateRow);
