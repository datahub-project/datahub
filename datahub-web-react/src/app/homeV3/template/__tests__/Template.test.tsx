import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import Template from '@app/homeV3/template/Template';
import { getBottomButtonMode } from '@app/homeV3/template/components/utils';
import { useIsModuleHidden } from '@app/homeV3/templateRow/hooks/useIsModuleHidden';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, PageTemplateSurfaceType } from '@types';

vi.mock('@app/entityV2/shared/EntityContext', () => ({ useEntityData: () => ({ urn: undefined }) }));
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({ usePageTemplateContext: vi.fn() }));
vi.mock('@app/homeV3/templateRow/hooks/useIsModuleHidden', () => ({ useIsModuleHidden: vi.fn() }));
vi.mock('@app/homeV3/moduleModals/ModuleModalMapper', () => ({ default: () => null }));
vi.mock('@app/homeV3/template/components/DragAndDropProvider', () => ({
    default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));
vi.mock('@app/homeV3/template/components/NewRowDropZone', () => ({
    default: () => <div data-testid="new-row-drop-zone" />,
}));
vi.mock('@app/homeV3/templateRow/TemplateRow', () => ({ default: () => <div data-testid="template-row" /> }));
vi.mock('@app/homeV3/template/components/AddModuleButton', () => ({
    default: ({ className }: { className?: string }) => <div data-testid="bottom-add-button" className={className} />,
}));
vi.mock('@app/homeV3/template/components/utils', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/homeV3/template/components/utils')>();
    return { ...actual, getBottomButtonMode: vi.fn(actual.getBottomButtonMode) };
});

const MCP_MODULE = {
    urn: 'urn:li:dataHubPageModule:mcp',
    properties: { type: DataHubPageModuleType.Mcp },
} as PageModuleFragment;

function renderTemplate({ mcpHidden }: { mcpHidden: boolean }) {
    vi.mocked(usePageTemplateContext).mockReturnValue({
        templateType: PageTemplateSurfaceType.HomePage,
        template: { properties: { rows: [{ modules: [MCP_MODULE] }] } },
        isTemplateEditable: true,
        moduleContext: {},
    } as unknown as ReturnType<typeof usePageTemplateContext>);
    vi.mocked(useIsModuleHidden).mockReturnValue(
        (module) => mcpHidden && module.properties.type === DataHubPageModuleType.Mcp,
    );
    render(<Template />);
}

describe('Template', () => {
    beforeEach(() => vi.mocked(getBottomButtonMode).mockClear());

    it('keeps the bottom add button, laid out for an empty page, when every row is hidden', () => {
        renderTemplate({ mcpHidden: true });

        expect(screen.queryByTestId('template-row')).not.toBeInTheDocument();
        expect(screen.getByTestId('bottom-add-button')).toBeInTheDocument();
        expect(getBottomButtonMode).toHaveBeenLastCalledWith(PageTemplateSurfaceType.HomePage, false);
    });

    it('lays the bottom add button out for rows when a row is visible', () => {
        renderTemplate({ mcpHidden: false });

        expect(screen.getByTestId('template-row')).toBeInTheDocument();
        expect(screen.getAllByTestId('new-row-drop-zone')).toHaveLength(2);
        expect(getBottomButtonMode).toHaveBeenLastCalledWith(PageTemplateSurfaceType.HomePage, true);
    });
});
