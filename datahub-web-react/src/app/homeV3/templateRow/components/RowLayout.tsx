import { spacing } from '@components';
import React, { memo, useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import Module from '@app/homeV3/module/Module';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import { ModulePositionInput } from '@app/homeV3/template/types';
import ModuleDropZone from '@app/homeV3/templateRow/components/ModuleDropZone';
import { WrappedRow } from '@app/homeV3/templateRow/types';

import { PageTemplateSurfaceType } from '@types';

const RowWrapper = styled.div<{ $isHidden: boolean }>`
    display: ${({ $isHidden }) => ($isHidden ? 'none' : 'flex')};
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
    isSmallRow: boolean | null;
}

interface ModuleWrapperProps {
    module: WrappedRow['modules'][0];
    position: ModulePositionInput;
    moduleKey: string;
    onModuleHiddenChange: (moduleKey: string, isHidden: boolean) => void;
}

// Memoized module wrapper to prevent unnecessary re-renders
const ModuleWrapper = memo(({ module, position, moduleKey, onModuleHiddenChange }: ModuleWrapperProps) => {
    const onHiddenChange = useCallback(
        (isHidden: boolean) => onModuleHiddenChange(moduleKey, isHidden),
        [onModuleHiddenChange, moduleKey],
    );
    return <Module module={module} position={position} onHiddenChange={onHiddenChange} />;
});

function RowLayout({ rowIndex, modulePositions, shouldDisableDropZones, isSmallRow }: Props) {
    const { templateType } = usePageTemplateContext();
    const showAddButtons = templateType === PageTemplateSurfaceType.HomePage;

    const [hiddenModuleKeys, setHiddenModuleKeys] = useState<Set<string>>(new Set());

    const onModuleHiddenChange = useCallback((moduleKey: string, isHidden: boolean) => {
        setHiddenModuleKeys((prev) => {
            if (prev.has(moduleKey) === isHidden) return prev;
            const next = new Set(prev);
            if (isHidden) next.add(moduleKey);
            else next.delete(moduleKey);
            return next;
        });
    }, []);

    // Collapse the row (including its tall drop zones) when every module in it renders nothing.
    // Modules stay mounted so they can un-hide once their data changes.
    const isRowHidden = useMemo(
        () => modulePositions.length > 0 && modulePositions.every(({ key }) => hiddenModuleKeys.has(key)),
        [modulePositions, hiddenModuleKeys],
    );

    return (
        <RowWrapper $isHidden={isRowHidden} data-testid={`template-row-${rowIndex}`}>
            {showAddButtons && <AddModuleButton orientation="vertical" rowIndex={rowIndex} rowSide="left" />}

            {/* Drop zone at the beginning of the row */}
            <ModuleDropZone
                rowIndex={rowIndex}
                moduleIndex={0}
                disabled={shouldDisableDropZones}
                isSmall={isSmallRow}
            />

            {modulePositions.map(({ module, position, key }, moduleIndex) => (
                <React.Fragment key={key}>
                    <ModuleWrapper
                        module={module}
                        position={position}
                        moduleKey={key}
                        onModuleHiddenChange={onModuleHiddenChange}
                    />
                    {/* Drop zone after each module */}
                    <ModuleDropZone
                        rowIndex={rowIndex}
                        moduleIndex={moduleIndex + 1}
                        disabled={shouldDisableDropZones}
                        isSmall={isSmallRow}
                    />
                </React.Fragment>
            ))}

            {showAddButtons && <AddModuleButton orientation="vertical" rowIndex={rowIndex} rowSide="right" />}
        </RowWrapper>
    );
}

export default memo(RowLayout);
