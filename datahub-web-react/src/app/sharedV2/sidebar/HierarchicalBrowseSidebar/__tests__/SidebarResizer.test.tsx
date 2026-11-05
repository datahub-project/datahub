import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import SidebarResizer from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarResizer';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const START_WIDTH = 400;

function renderResizer() {
    const onWidthChange = vi.fn();
    const onResizeEnd = vi.fn();
    const utils = render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <div data-testid="panel">
                    <SidebarResizer
                        edge="left"
                        minWidth={320}
                        maxWidth={800}
                        onWidthChange={onWidthChange}
                        onResizeEnd={onResizeEnd}
                    />
                </div>
            </TestPageContainer>
        </MockedProvider>,
    );
    const panel = screen.getByTestId('panel');
    vi.spyOn(panel, 'getBoundingClientRect').mockReturnValue({ width: START_WIDTH } as DOMRect);
    return { ...utils, panel, onWidthChange, onResizeEnd, handle: screen.getByTestId('sidebar-resizer') };
}

describe('SidebarResizer', () => {
    beforeEach(() => {
        // Run each frame immediately so the live width can be asserted synchronously.
        vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => {
            callback(0);
            return 1;
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    it('resizes the panel live and commits the final width once on release', () => {
        const { handle, panel, onWidthChange } = renderResizer();

        fireEvent.mouseDown(handle, { clientX: 500 });
        fireEvent.mouseMove(window, { clientX: 450, buttons: 1 });

        // The handle is on the panel's left edge, so dragging left widens it.
        expect(panel.style.width).toBe('450px');
        expect(handle.dataset.dragging).toBe('true');
        expect(onWidthChange).not.toHaveBeenCalled();

        fireEvent.mouseMove(window, { clientX: 400, buttons: 1 });
        fireEvent.mouseUp(window);
        fireEvent.mouseUp(window);

        expect(onWidthChange).toHaveBeenCalledTimes(1);
        expect(onWidthChange).toHaveBeenCalledWith(500);
        expect(panel.style.width).toBe('');
        expect(handle.dataset.dragging).toBeUndefined();
    });

    it('keeps the live width within its bounds', () => {
        const { handle, panel, onWidthChange } = renderResizer();

        fireEvent.mouseDown(handle, { clientX: 500 });
        fireEvent.mouseMove(window, { clientX: 0, buttons: 1 });
        expect(panel.style.width).toBe('800px');

        fireEvent.mouseMove(window, { clientX: 1000, buttons: 1 });
        fireEvent.mouseUp(window);
        expect(onWidthChange).toHaveBeenCalledWith(320);
    });

    it('does not commit a width on a click without movement', () => {
        const { handle, onWidthChange, onResizeEnd } = renderResizer();

        fireEvent.mouseDown(handle, { clientX: 500 });
        fireEvent.mouseUp(window);

        expect(onWidthChange).not.toHaveBeenCalled();
        expect(onResizeEnd).toHaveBeenCalledTimes(1);
    });

    it('ends the drag when a move arrives with no button held', () => {
        const { handle, onWidthChange, onResizeEnd } = renderResizer();

        fireEvent.mouseDown(handle, { clientX: 500 });
        fireEvent.mouseMove(window, { clientX: 460, buttons: 1 });
        fireEvent.mouseMove(window, { clientX: 0, buttons: 0 });
        fireEvent.mouseMove(window, { clientX: 0, buttons: 1 });

        expect(onWidthChange).toHaveBeenCalledTimes(1);
        expect(onWidthChange).toHaveBeenCalledWith(440);
        expect(onResizeEnd).toHaveBeenCalledTimes(1);
    });

    it('drops the drag without committing when unmounted mid-drag', () => {
        const { handle, panel, onWidthChange, unmount } = renderResizer();

        fireEvent.mouseDown(handle, { clientX: 500 });
        fireEvent.mouseMove(window, { clientX: 450, buttons: 1 });
        unmount();
        fireEvent.mouseMove(window, { clientX: 0, buttons: 1 });
        fireEvent.mouseUp(window);

        expect(onWidthChange).not.toHaveBeenCalled();
        expect(panel.style.width).toBe('');
    });
});
