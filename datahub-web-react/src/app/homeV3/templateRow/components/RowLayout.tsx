import { spacing } from '@components';
import React, { memo } from 'react';
import styled from 'styled-components';

import Module from '@app/homeV3/module/Module';
import { ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { ModulePositionInput } from '@app/homeV3/template/types';
import ModuleDropZone from '@app/homeV3/templateRow/components/ModuleDropZone';
import { WrappedRow } from '@app/homeV3/templateRow/types';

const RowWrapper = styled.div`
    display: flex;
    gap: ${spacing.xxsm};
    flex: 1;
`;

interface ModulePosition {
    module: WrappedRow['modules'][0];
    position: ModulePositionInput;
    key: string;
}

interface Props {
    rowIndex: number;
    modulePositions: ModulePosition[];
    shouldDisableDropZones: boolean;
    modulesAvailableToAdd: ModulesAvailableToAdd;
    isSmallRow?: boolean;
}

interface ModuleWrapperProps {
    module: WrappedRow['modules'][0];
    position: ModulePositionInput;
}

// Memoized module wrapper to prevent unnecessary re-renders
const ModuleWrapper = memo(({ module, position }: ModuleWrapperProps) => (
    <Module module={module} position={position} />
));

function RowLayout({ rowIndex, modulePositions, shouldDisableDropZones, modulesAvailableToAdd, isSmallRow }: Props) {
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
                isSmall={isSmallRow}
            />

            {modulePositions.map(({ module, position, key }, moduleIndex) => (
                <React.Fragment key={key}>
                    <ModuleWrapper module={module} position={position} />
                    {/* Drop zone after each module */}
                    <ModuleDropZone
                        rowIndex={rowIndex}
                        moduleIndex={moduleIndex + 1}
                        disabled={shouldDisableDropZones}
                        isSmall={isSmallRow}
                    />
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

export default memo(RowLayout);
