import { useMemo } from 'react';
import { useLocation } from 'react-router';

import { PageRoutes } from '@conf/Global';

/**
 * Hook to detect if the current page is the home page
 * Abstracts location.pathname for better testability and reusability
 */
export const useIsHomePage = (): boolean => {
    const { pathname } = useLocation();

    return useMemo(() => {
        return pathname === PageRoutes.HOME;
    }, [pathname]);
};
