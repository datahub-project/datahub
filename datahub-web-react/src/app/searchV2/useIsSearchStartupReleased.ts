import { useEffect, useState } from 'react';
import { useRouteMatch } from 'react-router';

import { useSearchContext } from '@app/search/context/SearchContext';
import { PageRoutes } from '@conf/Global';

/**
 * How long startup queries that do not affect search results wait for the first search to settle
 * before running anyway. A slow or hung search must not leave them pending.
 */
export const SEARCH_STARTUP_HOLD_TIMEOUT_MS = 5_000;

const IDLE_RELEASE_TIMEOUT_MS = 1_000;

/**
 * Whether startup queries that the first /search results do not depend on may run.
 *
 * On /search they wait for the results request, so it does not queue behind them for a browser
 * connection. They are released when the search settles (loaded or errored) or after
 * SEARCH_STARTUP_HOLD_TIMEOUT_MS, and are never held on any other route. Once released, a
 * consumer stays released, so later navigation or refiltering does not hold them again.
 */
export function useIsSearchStartupReleased(): boolean {
    const isSearchRoute = useRouteMatch(PageRoutes.SEARCH_RESULTS) !== null;
    const { isSearchSettled } = useSearchContext();
    const [timedOut, setTimedOut] = useState(false);
    const released = !isSearchRoute || isSearchSettled || timedOut;
    const [seenReleased, setSeenReleased] = useState(released);

    useEffect(() => {
        if (released) setSeenReleased(true);
    }, [released]);

    useEffect(() => {
        if (released || seenReleased) return undefined;
        const timeoutId = window.setTimeout(() => setTimedOut(true), SEARCH_STARTUP_HOLD_TIMEOUT_MS);
        return () => window.clearTimeout(timeoutId);
    }, [released, seenReleased]);

    return released || seenReleased;
}

/**
 * Waits for the main thread to go idle, so queries released by a settled search respond after the
 * results have rendered instead of competing with that render. Falls back to a timer where
 * requestIdleCallback is missing.
 */
function runWhenIdle(callback: () => void): () => void {
    if (typeof window.requestIdleCallback === 'function') {
        const idleId = window.requestIdleCallback(callback, { timeout: IDLE_RELEASE_TIMEOUT_MS });
        return () => window.cancelIdleCallback(idleId);
    }
    const timeoutId = window.setTimeout(callback, 0);
    return () => window.clearTimeout(timeoutId);
}

/**
 * Reports that the search results request has settled, which releases useIsSearchStartupReleased
 * once the results have had a chance to render.
 */
export function useReportSearchSettled(settled: boolean): void {
    const { markSearchSettled } = useSearchContext();

    useEffect(() => {
        if (!settled) return undefined;
        return runWhenIdle(markSearchSettled);
    }, [settled, markSearchSettled]);
}
