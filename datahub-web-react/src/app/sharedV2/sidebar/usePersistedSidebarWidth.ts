import { useCallback, useState } from 'react';

import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';

/** Largest share of its row a right-hand side panel may take, so the page keeps room. */
export const SIDE_PANEL_MAX_ROW_SHARE = 0.5;

/** Caps a dragged width to `SIDE_PANEL_MAX_ROW_SHARE` of the row that holds `panel`. */
export function capToRowShare(width: number, panel: HTMLElement | null): number {
    const rowWidth = panel?.parentElement?.getBoundingClientRect().width;
    return rowWidth ? Math.min(width, rowWidth * SIDE_PANEL_MAX_ROW_SHARE) : width;
}

type Options = {
    /** Where the dragged width is saved. Without one the width lasts only while the page is open. */
    storageKey?: string;
    minWidth: number;
    maxWidth: number;
    /** Fraction of the viewport used until the user drags the panel. */
    defaultRatio: number;
};

function clamp(width: number, minWidth: number, maxWidth: number): number {
    return Math.min(Math.max(width, minWidth), maxWidth);
}

function readStoredWidth(storageKey: string | undefined): number | null {
    if (!storageKey) return null;
    try {
        const raw = localStorage.getItem(storageKey);
        if (raw == null || raw === '') return null;
        const parsed = Number(raw);
        return Number.isFinite(parsed) ? parsed : null;
    } catch {
        return null;
    }
}

function writeStoredWidth(storageKey: string | undefined, width: number): void {
    if (!storageKey) return;
    try {
        localStorage.setItem(storageKey, String(width));
    } catch {
        // private mode / quota — ignore
    }
}

/**
 * Drag-resizable panel width that persists across pages and reloads.
 * Until the user drags, tracks `viewport × defaultRatio`. Bounds are re-applied on read
 * so a width saved on a larger screen still fits.
 */
export default function usePersistedSidebarWidth({ storageKey, minWidth, maxWidth, defaultRatio }: Options): {
    width: number;
    setWidth: (width: number) => void;
} {
    const measuredWidth = useSidebarWidth(defaultRatio);
    const [userWidth, setUserWidth] = useState<number | null>(() => readStoredWidth(storageKey));

    const width = clamp(userWidth ?? measuredWidth, minWidth, maxWidth);

    const setWidth = useCallback(
        (next: number) => {
            const clamped = clamp(next, minWidth, maxWidth);
            setUserWidth(clamped);
            writeStoredWidth(storageKey, clamped);
        },
        [storageKey, minWidth, maxWidth],
    );

    return { width, setWidth };
}
