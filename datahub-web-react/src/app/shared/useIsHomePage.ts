/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
