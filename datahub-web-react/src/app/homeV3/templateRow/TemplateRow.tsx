import { useDndContext } from '@dnd-kit/core';
import React, { memo } from 'react';

import RowLayout from '@app/homeV3/templateRow/components/RowLayout';
import { useTemplateRowLogic } from '@app/homeV3/templateRow/hooks/useTemplateRowLogic';
import { WrappedRow } from '@app/homeV3/templateRow/types';

interface Props {
    row: WrappedRow;
    rowIndex: number;
}

function TemplateRow({ row, rowIndex }: Props) {
    const { modulePositions, isSmallRow } = useTemplateRowLogic(row, rowIndex);
    const { active } = useDndContext();
    const isActiveModuleSmall = active?.data?.current?.isSmall;

    const isDropAllowed =
        !active ||
        isSmallRow == null || // empty row
        isActiveModuleSmall === isSmallRow;

    const isDropZoneDisabled = !isDropAllowed;

    return (
        <RowLayout
            rowIndex={rowIndex}
            modulePositions={modulePositions}
            shouldDisableDropZones={isDropZoneDisabled}
            isSmallRow={isSmallRow}
        />
    );
}

export default memo(TemplateRow);
