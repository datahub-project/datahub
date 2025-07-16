import React, { memo } from 'react';

import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import { WrappedRow } from '@app/homeV3/templateRow/types';

import RowLayout from './components/RowLayout';
import { useTemplateRowLogic } from './hooks/useTemplateRowLogic';

interface Props {
    row: WrappedRow;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    rowIndex: number;
}

function TemplateRow({ row, modulesAvailableToAdd, rowIndex }: Props) {
    const { modulePositions, shouldDisableDropZones } = useTemplateRowLogic(row, rowIndex);

    return (
        <RowLayout
            rowIndex={rowIndex}
            modulePositions={modulePositions}
            shouldDisableDropZones={shouldDisableDropZones}
            modulesAvailableToAdd={modulesAvailableToAdd}
        />
    );
}

export default memo(TemplateRow);
