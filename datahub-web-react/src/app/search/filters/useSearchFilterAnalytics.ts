import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';

const useSearchFilterAnalytics = () => {
    const trackClearAllFiltersEvent = (total: number) => {
        analytics.event({
            type: EventType.SearchFiltersClearAllEvent,
            total,
        });
    };

    const trackShowMoreEvent = () => {
        analytics.event({
            type: EventType.SearchFiltersShowMoreEvent,
        });
    };

    return { trackClearAllFiltersEvent, trackShowMoreEvent } as const;
};

export default useSearchFilterAnalytics;
