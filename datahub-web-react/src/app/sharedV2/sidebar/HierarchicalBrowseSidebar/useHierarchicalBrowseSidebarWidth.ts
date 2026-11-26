import { useCallback, useState } from 'react';

import {
    clampSidebarWidth,
    readStoredSidebarWidth,
    writeStoredSidebarWidth,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/sidebarWidth';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';

type Options = {
    /** Width before the user drags; defaults to viewport × 0.2. */
    defaultWidth?: number;
    /** localStorage key for the dragged width; defaults to the key shared across hierarchical sidebars. */
    storageKey?: string;
};

/**
 * Shared expanded width for hierarchical browse sidebars.
 * Prefers a persisted user drag; otherwise uses `defaultWidth` or tracks viewport × 0.2 (clamped).
 */
export default function useHierarchicalBrowseSidebarWidth({ defaultWidth, storageKey }: Options = {}): {
    width: number;
    setWidth: (width: number) => void;
} {
    const measuredWidth = useSidebarWidth(0.2);
    const [userWidth, setUserWidth] = useState<number | null>(() => readStoredSidebarWidth(storageKey));

    const width = userWidth ?? clampSidebarWidth(defaultWidth ?? measuredWidth);

    const setWidth = useCallback(
        (next: number) => {
            const clamped = clampSidebarWidth(next);
            setUserWidth(clamped);
            writeStoredSidebarWidth(clamped, storageKey);
        },
        [storageKey],
    );

    return { width, setWidth };
}
