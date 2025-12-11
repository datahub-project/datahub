/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';

const useSearchFilterAnalytics = () => {
    const trackClearAllFiltersEvent = (total: number) => {
        analytics.event({
            type: EventType.SearchFiltersClearAllEvent,
            total,
        });
    };

    const trackShowMoreEvent = (activeFilterCount: number, hiddenFilterCount: number) => {
        analytics.event({
            type: EventType.SearchFiltersShowMoreEvent,
            activeFilterCount,
            hiddenFilterCount,
        });
    };

    return { trackClearAllFiltersEvent, trackShowMoreEvent } as const;
};

export default useSearchFilterAnalytics;
