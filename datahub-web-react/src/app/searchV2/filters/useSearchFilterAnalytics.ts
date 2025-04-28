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
