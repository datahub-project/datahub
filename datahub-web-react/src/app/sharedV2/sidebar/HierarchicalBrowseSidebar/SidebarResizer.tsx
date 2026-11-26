import React, { RefObject, useCallback, useEffect, useRef } from 'react';
import styled from 'styled-components';

import { capToRowShare } from '@app/sharedV2/sidebar/usePersistedSidebarWidth';

type Props = {
    minWidth: number;
    maxWidth: number;
    /** Also cap the width at `SIDE_PANEL_MAX_ROW_SHARE` of the row that holds the panel. */
    capToRow?: boolean;
    /** Called once, with the final width, when a drag that moved the edge ends. */
    onWidthChange: (width: number) => void;
    onResizeStart?: () => void;
    onResizeEnd?: () => void;
    /**
     * Which edge of the panel the handle sits on. `right` (default) is for left-hand sidebars
     * (drag right to widen); `left` is for right-hand panels (drag left to widen).
     */
    edge?: 'left' | 'right';
    /** The element whose width is dragged. Defaults to the handle's parent. */
    targetRef?: RefObject<HTMLElement>;
};

/**
 * Right edge: hit target centered on the sidebar’s right border, extending into the
 * layout gap — not over row actions / carets inside the sidebar.
 * Left edge: sits just inside the panel, since right-hand panels clip their overflow.
 */
const ResizerBar = styled.div<{ $edge: 'left' | 'right' }>`
    position: absolute;
    top: 0;
    bottom: 0;
    ${({ $edge }) => ($edge === 'right' ? 'right: 0; transform: translateX(50%);' : 'left: 0;')}
    width: ${({ $edge }) => ($edge === 'right' ? '8px' : '6px')};
    cursor: col-resize;
    z-index: 2;

    /* Thin accent line on the panel edge: fades in after a short hover delay (so passing over the
       edge doesn't flicker) and stays on for the whole drag. The hit strip itself stays invisible. */
    &::after {
        content: '';
        position: absolute;
        top: 0;
        bottom: 0;
        ${({ $edge }) => ($edge === 'right' ? 'left: calc(50% - 1px);' : 'left: 0;')}
        width: 2px;
        background-color: ${(props) => props.theme.colors.borderBrand};
        opacity: 0;
        transition: opacity 0.15s ease;
        pointer-events: none;
    }

    &:hover::after {
        opacity: 1;
        transition-delay: 0.15s;
    }

    &[data-dragging='true']::after {
        opacity: 1;
        transition-delay: 0s;
    }
`;

export default function SidebarResizer({
    minWidth,
    maxWidth,
    capToRow,
    onWidthChange,
    onResizeStart,
    onResizeEnd,
    edge = 'right',
    targetRef,
}: Props) {
    const handleRef = useRef<HTMLDivElement>(null);
    // Ends the current drag; set while a drag is in progress.
    const endDragRef = useRef<((commit: boolean) => void) | null>(null);

    // Unmounting mid-drag (e.g. the panel collapses) must not leave listeners resizing from the cursor.
    useEffect(() => () => endDragRef.current?.(false), []);

    const onMouseDown = useCallback(
        (event: React.MouseEvent) => {
            const target = targetRef?.current ?? handleRef.current?.parentElement;
            if (!target) return;
            const initialX = event.clientX;
            // Start from the rendered width, which a row cap or a narrow window may hold below the saved one.
            const initialWidth = target.getBoundingClientRect().width;
            const listeners = new AbortController();
            let latestX = initialX;
            let liveWidth = initialWidth;
            let frame: number | null = null;

            // Dragging resizes the panel element directly, at most once per frame, instead of
            // re-rendering the page on every mousemove. React gets the final width once, on release.
            const applyWidth = () => {
                frame = null;
                const delta = latestX - initialX;
                const raw = edge === 'right' ? initialWidth + delta : initialWidth - delta;
                const bounded = Math.min(Math.max(raw, minWidth), maxWidth);
                liveWidth = capToRow ? capToRowShare(bounded, target) : bounded;
                target.style.width = `${liveWidth}px`;
                target.style.minWidth = `${liveWidth}px`;
                target.style.maxWidth = `${liveWidth}px`;
            };

            const handle = handleRef.current;

            const endDrag = (commit: boolean) => {
                listeners.abort();
                if (handle) delete handle.dataset.dragging;
                if (frame != null) cancelAnimationFrame(frame);
                endDragRef.current = null;
                if (commit && latestX !== initialX) {
                    applyWidth();
                    onWidthChange(liveWidth);
                }
                // React re-renders with the committed width before the next paint, so dropping the
                // inline override doesn't flash the old width.
                target.style.width = '';
                target.style.minWidth = '';
                target.style.maxWidth = '';
                onResizeEnd?.();
            };

            const onMove = (moveEvent: MouseEvent) => {
                // The mouseup was lost (released over an iframe, after alt-tab, ...): end the drag here.
                if (moveEvent.buttons === 0) {
                    endDrag(true);
                    return;
                }
                latestX = moveEvent.clientX;
                if (frame == null) frame = requestAnimationFrame(applyWidth);
            };

            endDragRef.current?.(false);
            endDragRef.current = endDrag;
            // Set on the element directly so showing the drag line doesn't re-render anything.
            if (handle) handle.dataset.dragging = 'true';
            onResizeStart?.();
            window.addEventListener('mousemove', onMove, { signal: listeners.signal });
            window.addEventListener('mouseup', () => endDrag(true), { signal: listeners.signal });
            window.addEventListener('blur', () => endDrag(true), { signal: listeners.signal });
            event.preventDefault();
        },
        [minWidth, maxWidth, capToRow, onWidthChange, onResizeStart, onResizeEnd, edge, targetRef],
    );

    return (
        <ResizerBar
            ref={handleRef}
            $edge={edge}
            role="separator"
            aria-orientation="vertical"
            onMouseDown={onMouseDown}
            data-testid="sidebar-resizer"
        />
    );
}
