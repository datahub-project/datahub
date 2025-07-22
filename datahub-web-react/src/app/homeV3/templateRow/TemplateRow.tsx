import { useDndContext } from '@dnd-kit/core';
import React, { memo } from 'react';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import RowLayout from '@app/homeV3/templateRow/components/RowLayout';
import { useTemplateRowLogic } from '@app/homeV3/templateRow/hooks/useTemplateRowLogic';
import { WrappedRow } from '@app/homeV3/templateRow/types';

interface Props {
    row: WrappedRow;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    rowIndex: number;
}

function TemplateRow({ row, modulesAvailableToAdd, rowIndex }: Props) {
    const { modulePositions, shouldDisableDropZones, isSmallRow } = useTemplateRowLogic(row, rowIndex);
    const { active } = useDndContext();
    const isActiveModuleSmall = active?.data?.current?.isSmall;

    const isDropAllowed =
        !active ||
        isSmallRow == null || // empty row
        isActiveModuleSmall === isSmallRow;

    const isDropZoneDisabled = shouldDisableDropZones || !isDropAllowed;

    return (
        <RowLayout
            rowIndex={rowIndex}
            modulePositions={modulePositions}
            shouldDisableDropZones={isDropZoneDisabled}
            modulesAvailableToAdd={modulesAvailableToAdd}
            isSmallRow={isSmallRow}
        />
    );
}

export default memo(TemplateRow);
