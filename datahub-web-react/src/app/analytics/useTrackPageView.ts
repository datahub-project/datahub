import { useEffect } from 'react';
import { useLocation } from 'react-router-dom';

import analytics from '@app/analytics/analytics';

// Note: we explicitly keep this outside of React state management.
let prevPathname: string = document.referrer;

/**
 * Check if two paths represent navigation between tabs of the same entity:
 *  /<entity-type>/<entity-urn>/<tab-name>
 *
 * @param prevPath - The previous path
 * @param currentPath - The current path
 * @returns true if both paths are for the same entity but different tabs
 */
export function isSameEntityTabNavigation(prevPath: string, currentPath: string): boolean {
    const prevParts = prevPath.split('/');
    const currentParts = currentPath.split('/');

    // Entity URLs have format: ['', entity-type, entity-urn, tab-name, ...]
    // We need at least 4 parts to be an entity tab URL
    if (prevParts.length < 4 || currentParts.length < 4) {
        return false;
    }

    // Compare entity type (index 1) and entity URN (index 2)
    const prevEntityType = prevParts[1];
    const prevEntityUrn = prevParts[2];
    const currentEntityType = currentParts[1];
    const currentEntityUrn = currentParts[2];

    return prevEntityType === currentEntityType && prevEntityUrn === currentEntityUrn;
}

/**
 * Hook used for logging page view events.
 * Excludes tab navigation within the same entity profile.
 */
export const useTrackPageView = () => {
    const location = useLocation();

    return useEffect(() => {
        if (prevPathname !== location.pathname) {
            // Don't fire PageViewEvent for tab navigation within the same entity
            if (!isSameEntityTabNavigation(prevPathname, location.pathname)) {
                analytics.page({ prevPathname });
            }
            prevPathname = location.pathname;
        }
    }, [location]);
};
