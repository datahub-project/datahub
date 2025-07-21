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

    return (
        <RowLayout
            rowIndex={rowIndex}
            modulePositions={modulePositions}
            shouldDisableDropZones={shouldDisableDropZones}
            modulesAvailableToAdd={modulesAvailableToAdd}
            isSmallRow={isSmallRow}
        />
    );
}

export default memo(TemplateRow);
