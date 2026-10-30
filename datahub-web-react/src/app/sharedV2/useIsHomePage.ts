import { useLocation } from 'react-router';

import { getDefaultLandingPage } from '@app/defaultLandingPage';
import { PageRoutes } from '@conf/Global';

/**
 * Hook to detect if the current page is the home page
 * Abstracts location.pathname for better testability and reusability
 */
export const useIsHomePage = (): boolean => {
    const { pathname } = useLocation();
    const defaultLandingPage = getDefaultLandingPage(import.meta.env.REACT_APP_DEFAULT_LANDING_PAGE);

    return pathname === PageRoutes.HOME || (pathname === PageRoutes.ROOT && defaultLandingPage === undefined);
};
