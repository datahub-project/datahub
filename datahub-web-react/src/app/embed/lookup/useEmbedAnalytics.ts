/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
