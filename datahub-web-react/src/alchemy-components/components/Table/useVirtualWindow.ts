import { RefObject, useLayoutEffect, useState } from 'react';

import { TableVirtualizationProps } from '@components/components/Table/types';

const DEFAULT_OVERSCAN = 10;
// Rows to render before the container has been measured (first paint, or no layout in tests).
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

    useLayoutEffect(() => {
        const container = containerRef.current;
        if (!rowHeight || !container) return undefined;
        const measure = () => {
            setFirstVisibleRow(Math.floor(Math.max(0, container.scrollTop - headerHeight) / rowHeight));
            setVisibleRowCount(Math.ceil(container.clientHeight / rowHeight));
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
