import { useCallback, useState } from 'react';

import {
    clampSidebarWidth,
    readStoredSidebarWidth,
    writeStoredSidebarWidth,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/sidebarWidth';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';

/**
 * Shared expanded width for hierarchical browse sidebars.
 * Prefers a persisted user drag; otherwise tracks viewport × 0.2 (clamped).
 */
export default function useHierarchicalBrowseSidebarWidth(): {
    width: number;
    setWidth: (width: number) => void;
} {
    const measuredWidth = useSidebarWidth(0.2);
    const [userWidth, setUserWidth] = useState<number | null>(() => readStoredSidebarWidth());

    const width = userWidth ?? clampSidebarWidth(measuredWidth);

    const setWidth = useCallback((next: number) => {
        const clamped = clampSidebarWidth(next);
        setUserWidth(clamped);
        writeStoredSidebarWidth(clamped);
    }, []);

    return { width, setWidth };
}
