import { useMemo } from 'react';
import { useLocation } from 'react-router';

import { getHomePageRoute } from '@app/defaultLandingPage';
import { PageRoutes } from '@conf/Global';

/**
 * Hook to detect if the current page is the home page
 * Abstracts location.pathname for better testability and reusability
 */
export const useIsHomePage = (): boolean => {
    const { pathname } = useLocation();
    const homePageRoute = getHomePageRoute(import.meta.env.REACT_APP_DEFAULT_LANDING_PAGE);

    return useMemo(() => {
        return pathname === homePageRoute || pathname === PageRoutes.HOME;
    }, [homePageRoute, pathname]);
};
