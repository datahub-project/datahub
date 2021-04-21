import { useEffect } from 'react';
import analytics from './analytics';

/**
 * Hook used for logging default page view events that need to occur a single time on initial component mount.
 */
export const useTrackPageView = () => {
    return useEffect(() => {
        analytics.page();
    }, []);
};
