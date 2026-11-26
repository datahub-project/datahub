import { act, fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import Module from '@app/homeV3/module/Module';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

const lineage = vi.hoisted(() => {
    let resolveChunk: () => void = () => undefined;
    const chunkLoaded = new Promise<void>((resolve) => {
        resolveChunk = resolve;
    });
    return { shouldThrow: true, chunkLoaded, resolveChunk };
});

vi.mock('@app/entityV2/summary/modules/lineage/LineageModule', async () => {
    await lineage.chunkLoaded;
    return {
        default: function LineageModule() {
            if (lineage.shouldThrow) {
                throw new Error('render failed');
            }
            return <div>Lineage content</div>;
        },
    };
});

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children, loading }: React.PropsWithChildren<{ loading?: boolean }>) => (
        <div data-testid="large-module">{loading ? 'Large module loading' : children}</div>
    ),
}));

vi.mock('@app/homeV3/module/components/EmptyContent', () => ({
    default: ({ onLinkClick }: { onLinkClick?: () => void }) => (
        <button type="button" onClick={onLinkClick}>
            Retry module
        </button>
    ),
}));

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    ModuleProvider: ({ children }: React.PropsWithChildren<unknown>) => <>{children}</>,
}));

const props: ModuleProps = {
    module: {
        urn: 'urn:li:dataHubPageModule:lineage',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Lineage',
            type: DataHubPageModuleType.Lineage,
            visibility: { scope: PageModuleScope.Global },
            params: {},
        },
    },
    position: { rowIndex: 0, moduleIndex: 0 },
};

describe('Module', () => {
    it('keeps the large-module frame while loading and retries render errors through the module boundary', async () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);

        render(<Module {...props} />);
        expect(screen.getByText('Large module loading')).toBeInTheDocument();

        await act(async () => {
            lineage.resolveChunk();
        });

        const retry = await screen.findByRole('button', { name: 'Retry module' });
        expect(screen.queryByRole('button', { name: 'Refresh' })).not.toBeInTheDocument();

        lineage.shouldThrow = false;
        fireEvent.click(retry);
        expect(await screen.findByText('Lineage content')).toBeInTheDocument();

        errorSpy.mockRestore();
    });
});
