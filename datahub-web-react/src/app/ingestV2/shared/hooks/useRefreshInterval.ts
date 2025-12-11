/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

const REFRESH_INTERVAL_MS = 3000;

export default function useRefreshInterval(refresh: () => void, isLoading: boolean, shouldRefresh: boolean) {
    useEffect(() => {
        if (isLoading) return () => {};

        if (!shouldRefresh) return () => {};

        const timer = setTimeout(() => refresh(), REFRESH_INTERVAL_MS);

        return () => {
            clearTimeout(timer);
        };
    }, [refresh, isLoading, shouldRefresh]);
}
