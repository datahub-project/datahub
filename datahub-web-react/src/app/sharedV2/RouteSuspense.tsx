import React, { ReactNode, Suspense, use } from 'react';
import { useHistory } from 'react-router-dom';

import { getPendingNavigation } from '@app/sharedV2/NavigationTransitions';
import { SuspenseBlock } from '@app/sharedV2/SuspenseBlock';

function RouteFallback() {
    const pending = getPendingNavigation(useHistory());
    // A new boundary shows its fallback even in a transition; suspending defers to the visible one above.
    if (pending) use(pending);
    return <SuspenseBlock />;
}

/** Suspense for route-level lazy pages: a spinner, except while a navigation is pending. */
export function RouteSuspense({ children }: { children: ReactNode }) {
    return <Suspense fallback={<RouteFallback />}>{children}</Suspense>;
}
