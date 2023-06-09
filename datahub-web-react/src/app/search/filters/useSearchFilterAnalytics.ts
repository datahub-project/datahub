import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';

const useSearchFilterAnalytics = () => {
    const trackClearAllFiltersEvent = (total: number) => {
        analytics.event({
            type: EventType.SearchFiltersClearAllEvent,
            total,
        });
    };

    return { trackClearAllFiltersEvent } as const;
};

export default useSearchFilterAnalytics;
