import { fireEvent, render, waitFor } from '@testing-library/react';
import { toPng } from 'html-to-image';
import React from 'react';
import { ReactFlowProvider } from 'reactflow';
import { ThemeProvider } from 'styled-components';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import LineageVisualizationContext from '@app/lineageV2/LineageVisualizationContext';
import { LineageNodesContext } from '@app/lineageV2/common';
import DownloadLineageScreenshotButton from '@app/lineageV2/controls/DownloadLineageScreenshotButton';

import { EntityType, LineageDirection } from '@types';

// Control toPng resolution so we can assert state DURING the capture window
// vs after it resolves.
vi.mock('html-to-image', () => ({ toPng: vi.fn() }));

// Stub the React Flow geometry helpers — out of scope for the screenshot
// wiring assertions, and we don't want to mount a real lineage graph in jsdom.
vi.mock('reactflow', async () => {
    const actual = await vi.importActual<typeof import('reactflow')>('reactflow');
    return {
        ...actual,
        useReactFlow: () => ({ getNodes: () => [] }),
        getRectOfNodes: () => ({ x: 0, y: 0, width: 800, height: 600 }),
        getTransformForBounds: () => [0, 0, 1] as const,
    };
});

describe('Entity name cleaning', () => {
    it('should clean special characters', () => {
        const cleanName = (name: string) => name.replace(/[^a-zA-Z0-9_-]/g, '_');

        expect(cleanName('dataset-with/special@chars#and$symbols')).toBe('dataset-with_special_chars_and_symbols');
        expect(cleanName('user.transactions')).toBe('user_transactions');
        expect(cleanName('normal_name')).toBe('normal_name');
        expect(cleanName('123-valid_name')).toBe('123-valid_name');
    });
});

describe('DownloadLineageScreenshotButton — virtualisation interop', () => {
    const setForceMountAll = vi.fn();
    const baseVisualizationContext = {
        searchQuery: '',
        setSearchQuery: () => {},
        searchedEntity: null,
        setSearchedEntity: () => {},
        isFocused: false,
        forceMountAll: false,
        setForceMountAll,
    };
    // Cast through unknown — the real context has 15+ unrelated state/setter
    // pairs the button never touches.
    const baseNodesContext = {
        rootUrn: 'urn:li:dataset:(urn:li:dataPlatform:perf,sample,PROD)',
        rootType: EntityType.Dataset,
        nodes: new Map(),
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
    } as unknown as React.ContextType<typeof LineageNodesContext>;

    const theme = { colors: { bgSurface: '#fff' } } as React.ComponentProps<typeof ThemeProvider>['theme'];

    function renderButton() {
        return render(
            <ThemeProvider theme={theme}>
                <ReactFlowProvider>
                    <LineageNodesContext.Provider value={baseNodesContext}>
                        <LineageVisualizationContext.Provider value={baseVisualizationContext}>
                            <DownloadLineageScreenshotButton showExpandedText />
                        </LineageVisualizationContext.Provider>
                    </LineageNodesContext.Provider>
                </ReactFlowProvider>
            </ThemeProvider>,
        );
    }

    beforeEach(() => {
        setForceMountAll.mockClear();
        // jsdom doesn't ship a real rAF; drive it from setTimeout so the
        // component's "two frames + tail" wait resolves inside `waitFor`.
        vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => {
            setTimeout(() => cb(performance.now()), 0);
            return 0;
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.mocked(toPng).mockReset();
    });

    it('toggles forceMountAll on before capture and off after toPng resolves', async () => {
        let resolveCapture: (value: string) => void = () => {};
        const capturePromise = new Promise<string>((resolve) => {
            resolveCapture = resolve;
        });
        vi.mocked(toPng).mockReturnValue(capturePromise);

        const { getByRole } = renderButton();
        fireEvent.click(getByRole('button'));

        await waitFor(() => {
            expect(setForceMountAll).toHaveBeenCalledWith(true);
        });
        expect(setForceMountAll).not.toHaveBeenCalledWith(false);

        resolveCapture('data:image/png;base64,abc');
        await waitFor(() => {
            expect(setForceMountAll).toHaveBeenCalledWith(false);
        });

        const calls = setForceMountAll.mock.calls.map((args) => args[0]);
        expect(calls).toEqual([true, false]);
    });

    // A failed capture must still restore the flag — otherwise virt stays
    // disabled for the rest of the session.
    it('still restores forceMountAll if toPng rejects', async () => {
        vi.mocked(toPng).mockRejectedValue(new Error('canvas tainted'));

        const { getByRole } = renderButton();
        fireEvent.click(getByRole('button'));

        await waitFor(() => {
            expect(setForceMountAll).toHaveBeenCalledWith(true);
            expect(setForceMountAll).toHaveBeenCalledWith(false);
        });
    });
});
