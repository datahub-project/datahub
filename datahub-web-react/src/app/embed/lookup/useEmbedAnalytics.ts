import { useCallback } from 'react';
import analytics from '../../analytics/analytics';
import { EventType } from '../../analytics';

const useEmbedLookupAnalytics = () => {
    const trackLookupNotFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'NO_ENTITY_FOUND' });
    }, []);

    const trackLookupMultipleFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'MULTIPLE_ENTITIES_FOUND' });
    }, []);

    return { trackLookupNotFoundEvent, trackLookupMultipleFoundEvent } as const;
};

export default useEmbedLookupAnalytics;
