import {
    SIDEBAR_MAX_WIDTH,
    SIDEBAR_MIN_WIDTH,
    SIDEBAR_WIDTH_STORAGE_KEY,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

export function clampSidebarWidth(width: number): number {
    return Math.min(Math.max(width, SIDEBAR_MIN_WIDTH), SIDEBAR_MAX_WIDTH);
}

/** Returns a clamped stored width, or null if missing / invalid. */
export function readStoredSidebarWidth(): number | null {
    try {
        const raw = localStorage.getItem(SIDEBAR_WIDTH_STORAGE_KEY);
        if (raw == null || raw === '') {
            return null;
        }
        const parsed = Number(raw);
        if (!Number.isFinite(parsed)) {
            return null;
        }
        return clampSidebarWidth(parsed);
    } catch {
        return null;
    }
}

export function writeStoredSidebarWidth(width: number): void {
    try {
        localStorage.setItem(SIDEBAR_WIDTH_STORAGE_KEY, String(clampSidebarWidth(width)));
    } catch {
        // private mode / quota — ignore
    }
}
