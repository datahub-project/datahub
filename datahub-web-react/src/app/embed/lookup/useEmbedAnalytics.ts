import { useCallback } from 'react';
import analytics from '../../analytics/analytics';
import { EventType } from '../../analytics';

const useEmbedLookupAnalytics = () => {
    const trackLookupNotFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'No entity found' });
    }, []);

    const trackLookupMultipleFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'Multiple entities found' });
    }, []);

    return { trackLookupNotFoundEvent, trackLookupMultipleFoundEvent } as const;
};

export default useEmbedLookupAnalytics;
