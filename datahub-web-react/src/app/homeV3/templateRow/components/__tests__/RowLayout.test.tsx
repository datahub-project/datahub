import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { ModuleProvider, useReportModuleHidden } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import RowLayout from '@app/homeV3/templateRow/components/RowLayout';

import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateSurfaceType } from '@types';

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: () => ({ templateType: PageTemplateSurfaceType.AssetSummary }),
}));

vi.mock('@app/homeV3/templateRow/components/ModuleDropZone', () => ({
    default: () => <div data-testid="module-drop-zone" />,
}));

vi.mock('@app/homeV3/template/components/AddModuleButton', () => ({
    default: () => null,
}));

vi.mock('@app/sharedV2/reloadableContext/hooks/useReloadableContext', () => ({
    useReloadableContext: () => ({ shouldBeReloaded: () => false, markAsReloaded: () => {} }),
}));

// Modules whose name starts with "hidden" render nothing and report themselves hidden
function FakeModule({ module }: ModuleProps) {
    const isHidden = module.properties.name.startsWith('hidden');
    useReportModuleHidden(isHidden);
    return isHidden ? null : <div data-testid="visible-module" />;
}

vi.mock('@app/homeV3/module/Module', () => ({
    default: ({ onHiddenChange, ...props }: ModuleProps & { onHiddenChange: (isHidden: boolean) => void }) => (
        <ModuleProvider {...props} onHiddenChange={onHiddenChange}>
            <FakeModule {...props} />
        </ModuleProvider>
    ),
}));

function buildModulePositions(names: string[]) {
    return names.map((name, moduleIndex) => ({
        module: {
            urn: `urn:li:dataHubPageModule:${name}`,
            type: EntityType.DatahubPageModule,
            properties: {
                name,
                type: DataHubPageModuleType.OutputPorts,
                visibility: { scope: PageModuleScope.Global },
                params: {},
            },
        },
        position: { rowIndex: 0, moduleIndex, numberOfModulesInRow: names.length },
        key: `${name}-${moduleIndex}`,
    }));
}

describe('RowLayout', () => {
    it('collapses the row when every module in it is hidden', () => {
        render(
            <RowLayout
                rowIndex={0}
                modulePositions={buildModulePositions(['hidden-a', 'hidden-b'])}
                shouldDisableDropZones={false}
                isSmallRow={false}
            />,
        );

        expect(screen.getByTestId('template-row-0')).not.toBeVisible();
    });

    it('keeps the row visible when at least one module renders', () => {
        render(
            <RowLayout
                rowIndex={0}
                modulePositions={buildModulePositions(['hidden-a', 'visible-b'])}
                shouldDisableDropZones={false}
                isSmallRow={false}
            />,
        );

        expect(screen.getByTestId('template-row-0')).toBeVisible();
        expect(screen.getByTestId('visible-module')).toBeInTheDocument();
    });
});
