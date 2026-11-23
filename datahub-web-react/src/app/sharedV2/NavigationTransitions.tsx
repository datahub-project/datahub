import React, { ReactNode, Suspense, startTransition, use, useLayoutEffect } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

import { SuspenseBlock } from '@app/sharedV2/SuspenseBlock';

type RouterHistory = ReturnType<typeof useHistory>;
type Destination = Parameters<RouterHistory['push']>[0];
type PendingNavigation = { promise: Promise<void>; settle: () => void };

const wrappedHistories = new WeakSet<RouterHistory>();
// Settles when the new location commits.
const pendingNavigations = new WeakMap<RouterHistory, PendingNavigation>();

function navigateInTransition(history: RouterHistory, navigate: () => void) {
    if (!pendingNavigations.has(history)) {
        let settle = () => {};
        const promise = new Promise<void>((resolve) => {
            settle = resolve;
        });
        pendingNavigations.set(history, { promise, settle });
    }
    startTransition(navigate);
}

function changesPathname(history: RouterHistory, to: Destination) {
    const pathname = typeof to === 'string' ? to.split(/[?#]/)[0] : to.pathname;
    return !!pathname && pathname !== history.location.pathname;
}

/**
 * Runs navigations as transitions so React keeps the current page while a lazy route loads, instead of a
 * fallback it holds for at least 300ms. Same-path `replace` stays sync: deferred URL-synced inputs drop keys.
 */
export function NavigationTransitions({ children }: { children: ReactNode }) {
    const history = useHistory();
    const location = useLocation();

    // During render: child effects run first and may already navigate.
    if (!wrappedHistories.has(history)) {
        wrappedHistories.add(history);
        const { push, replace } = history;
        history.push = ((...args: Parameters<typeof push>) =>
            navigateInTransition(history, () => push(...args))) as typeof push;
        history.replace = ((...args: Parameters<typeof replace>) =>
            changesPathname(history, args[0])
                ? navigateInTransition(history, () => replace(...args))
                : replace(...args)) as typeof replace;
    }

    useLayoutEffect(() => {
        pendingNavigations.get(history)?.settle();
        pendingNavigations.delete(history);
    }, [history, location]);

    return <>{children}</>;
}

function RouteFallback() {
    const pending = pendingNavigations.get(useHistory());
    // A new boundary shows its fallback even in a transition; suspending defers to the visible one above.
    if (pending) use(pending.promise);
    return <SuspenseBlock />;
}

/** Suspense for route-level lazy pages: a spinner, except while a navigation is pending. */
export function RouteSuspense({ children }: { children: ReactNode }) {
    return <Suspense fallback={<RouteFallback />}>{children}</Suspense>;
}
