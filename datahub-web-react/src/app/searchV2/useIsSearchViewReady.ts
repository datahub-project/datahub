import { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';

/**
 * How long to wait for the default view before searching without one.
 * A hung settings request must not block search forever. If the default arrives
 * later, a second search with that view is acceptable.
 */
export const SEARCH_VIEW_READY_TIMEOUT_MS = 10_000;

/**
 * Search can run once the default view has been applied, or a view is already stored.
 * `null` is an explicit clear and is ready. `undefined` means nothing is stored yet.
 */
export function isSearchViewReady(hasSetDefaultView: boolean, selectedViewUrn: string | null | undefined): boolean {
    return hasSetDefaultView || selectedViewUrn !== undefined;
}

export function useIsSearchViewReady(): boolean {
    const { localState, state } = useUserContext();
    const ready = isSearchViewReady(state.views.hasSetDefaultView, localState.selectedViewUrn);
    const [timedOut, setTimedOut] = useState(false);

    useEffect(() => {
        if (ready || timedOut) return undefined;
        const timeoutId = window.setTimeout(() => setTimedOut(true), SEARCH_VIEW_READY_TIMEOUT_MS);
        return () => window.clearTimeout(timeoutId);
    }, [ready, timedOut]);

    return ready || timedOut;
}
