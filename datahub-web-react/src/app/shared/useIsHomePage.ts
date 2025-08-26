import { useMemo } from 'react';
import { useLocation } from 'react-router';

/**
 * Hook to detect if the current page is the home page
 * Abstracts location.pathname for better testability and reusability
 */
export const useIsHomePage = (): boolean => {
    const { pathname } = useLocation();

    return useMemo(() => {
        return pathname === '/';
    }, [pathname]);
};
