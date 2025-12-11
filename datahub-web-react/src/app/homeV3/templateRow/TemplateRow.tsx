/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
