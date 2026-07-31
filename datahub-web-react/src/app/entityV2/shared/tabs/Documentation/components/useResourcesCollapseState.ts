import { useCallback, useEffect, useRef, useState } from 'react';

const STORAGE_KEY = 'datahub:resources-section:expanded';

// Auto-collapse the Resources section once it reaches this many items. Below the
// threshold the section stays expanded inline; at/above it, we collapse by default
// so a long list doesn't push the rest of the entity landing page below the fold.
// Matches the "collapse when it's a lot" pattern used by Notion/Linear/Jira.
export const RESOURCES_COLLAPSE_THRESHOLD = 5;

function readStoredExpanded(): boolean | null {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw === null) return null;
        return raw === 'true';
    } catch {
        // localStorage can be unavailable (Safari private mode, disabled cookies, tests).
        return null;
    }
}

function writeStoredExpanded(expanded: boolean): void {
    try {
        localStorage.setItem(STORAGE_KEY, String(expanded));
    } catch {
        // Silently ignore — collapse state simply won't persist across reloads.
    }
}

/**
 * Manages expand/collapse state for the Resources section on an entity page.
 *
 * Behavior:
 * - Once the user explicitly toggles, their preference is persisted in localStorage
 *   and applied on future page loads across all entities.
 * - Until then, the section auto-collapses when the item count reaches
 *   RESOURCES_COLLAPSE_THRESHOLD, and stays expanded for smaller counts.
 */
export function useResourcesCollapseState(itemCount: number): {
    isExpanded: boolean;
    toggle: () => void;
} {
    // Ref so re-reads don't cause renders; once the user has toggled (or hydrated
    // from storage) we stop reacting to itemCount changes.
    const hasUserPreferenceRef = useRef<boolean>(readStoredExpanded() !== null);

    const [isExpanded, setIsExpanded] = useState<boolean>(() => {
        const stored = readStoredExpanded();
        if (stored !== null) return stored;
        return itemCount < RESOURCES_COLLAPSE_THRESHOLD;
    });

    useEffect(() => {
        if (!hasUserPreferenceRef.current) {
            setIsExpanded(itemCount < RESOURCES_COLLAPSE_THRESHOLD);
        }
    }, [itemCount]);

    const toggle = useCallback(() => {
        setIsExpanded((prev) => {
            const next = !prev;
            hasUserPreferenceRef.current = true;
            writeStoredExpanded(next);
            return next;
        });
    }, []);

    return { isExpanded, toggle };
}
