import { RefObject, useLayoutEffect, useRef, useState } from 'react';

import { TableVirtualizationProps } from '@components/components/Table/types';

const DEFAULT_OVERSCAN = 10;
// Rows to render when the container has never had a measurable height (no layout, as in tests).
const UNMEASURED_ROW_COUNT = 30;

export interface VirtualWindow {
    start: number;
    end: number;
    paddingTop: number;
    paddingBottom: number;
}

/**
 * Picks the slice of fixed-height rows that is inside the scroll container's viewport, plus overscan.
 * The table renders spacer rows for the rest, so the scrollbar keeps the full height.
 */
export const useVirtualWindow = (
    containerRef: RefObject<HTMLElement>,
    itemCount: number,
    virtualization: TableVirtualizationProps | undefined,
    headerHeight: number,
    // The container only mounts once loading finishes, so the listener has to attach again then.
    isLoading: boolean,
): VirtualWindow => {
    const rowHeight = virtualization?.rowHeight;
    const overscan = virtualization?.overscan ?? DEFAULT_OVERSCAN;
    // Only the first visible row and the viewport's row count are state, so scrolling within a row
    // (most scroll events) doesn't re-render the table.
    const [firstVisibleRow, setFirstVisibleRow] = useState(0);
    const [visibleRowCount, setVisibleRowCount] = useState(0);
    // Rows mount only after the first measurement (a layout effect, so still before paint). Rendering
    // the unmeasured fallback first would mount far more rows than fit, just to unmount them again.
    const [isMeasured, setIsMeasured] = useState(false);
    // Set once the container has had a real height. A later height of 0 then means it was hidden (an
    // inactive tab stays mounted with display: none), not that there is no layout at all.
    const hasLaidOutRef = useRef(false);

    useLayoutEffect(() => {
        const container = containerRef.current;
        if (!rowHeight || !container) return undefined;
        const measure = () => {
            // Hidden: keep the current window rather than mounting the fallback rows in a pane nobody sees.
            if (container.clientHeight === 0 && hasLaidOutRef.current) return;
            if (container.clientHeight > 0) hasLaidOutRef.current = true;
            setFirstVisibleRow(Math.floor(Math.max(0, container.scrollTop - headerHeight) / rowHeight));
            setVisibleRowCount(Math.ceil(container.clientHeight / rowHeight));
            setIsMeasured(true);
        };
        measure();
        container.addEventListener('scroll', measure, { passive: true });
        const observer = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(measure);
        observer?.observe(container);
        return () => {
            container.removeEventListener('scroll', measure);
            observer?.disconnect();
        };
    }, [containerRef, rowHeight, headerHeight, isLoading]);

    if (!rowHeight) {
        return { start: 0, end: itemCount, paddingTop: 0, paddingBottom: 0 };
    }

    if (!isMeasured) {
        return { start: 0, end: 0, paddingTop: 0, paddingBottom: itemCount * rowHeight };
    }

    // A height of 0 before any layout means none at all (tests): fall back to a fixed count.
    const visibleCount = visibleRowCount > 0 ? visibleRowCount : UNMEASURED_ROW_COUNT;
    const start = Math.max(0, firstVisibleRow - overscan);
    const end = Math.min(itemCount, firstVisibleRow + visibleCount + overscan);

    return {
        start,
        end,
        paddingTop: start * rowHeight,
        paddingBottom: Math.max(0, itemCount - end) * rowHeight,
    };
};
