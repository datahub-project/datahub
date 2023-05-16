import { useCallback } from 'react';
import analytics from '../../analytics/analytics';
import { EventType } from '../../analytics';
import { EntityType } from '../../../types.generated';

const useEmbedLookupAnalytics = () => {
    const trackLookupRoutingEvent = useCallback((entityType: EntityType, entityUrn: string) => {
        analytics.event({ type: EventType.EmbedLookupRouteEvent, entityType, entityUrn });
    }, []);

    const trackLookupNotFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'No entity found' });
    }, []);

    const trackLookupMultipleFoundEvent = useCallback((url: string) => {
        analytics.event({ type: EventType.EmbedLookupNotFoundEvent, url, reason: 'Multiple entities found' });
    }, []);

    return { trackLookupRoutingEvent, trackLookupNotFoundEvent, trackLookupMultipleFoundEvent } as const;
};

export default useEmbedLookupAnalytics;
