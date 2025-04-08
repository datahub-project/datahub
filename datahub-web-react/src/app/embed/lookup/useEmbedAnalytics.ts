import { useCallback } from 'react';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { EMBED_LOOKUP_NOT_FOUND_REASON } from '@app/embed/lookup/constants';

const useEmbedLookupAnalytics = () => {
    const trackLookupNotFoundEvent = useCallback((url: string) => {
        analytics.event({
            type: EventType.EmbedLookupNotFoundEvent,
            url,
            reason: EMBED_LOOKUP_NOT_FOUND_REASON.NO_ENTITY_FOUND,
        });
    }, []);

    const trackLookupMultipleFoundEvent = useCallback((url: string) => {
        analytics.event({
            type: EventType.EmbedLookupNotFoundEvent,
            url,
            reason: EMBED_LOOKUP_NOT_FOUND_REASON.MULTIPLE_ENTITIES_FOUND,
        });
    }, []);

    return { trackLookupNotFoundEvent, trackLookupMultipleFoundEvent } as const;
};

export default useEmbedLookupAnalytics;
