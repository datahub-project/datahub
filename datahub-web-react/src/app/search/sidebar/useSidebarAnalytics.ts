import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import {
    useBrowsePathLength,
    useEntityType,
    useMaybeBrowseResultGroup,
    useMaybeEnvironmentAggregation,
    useMaybePlatformAggregation,
} from './BrowseContext';

// todo - add an event on the main sidebar mount that shows how many entities are there? :/

const useSidebarAnalytics = () => {
    const entityType = useEntityType();
    const environment = useMaybeEnvironmentAggregation()?.value;
    const platform = useMaybePlatformAggregation()?.value;
    const isBrowsePathNode = !!useMaybeBrowseResultGroup();
    const browsePathDepth = useBrowsePathLength();

    const trackToggleNodeEvent = (isOpen: boolean) => {
        analytics.event({
            type: EventType.BrowseV2ToggleNodeEvent,
            action: isOpen ? 'open' : 'close',
            entityType,
            ...(environment ? { environment } : {}),
            ...(platform ? { platform } : {}),
            isBrowsePathNode,
            browsePathDepth,
        });
    };

    return { trackToggleNodeEvent } as const;
};

export default useSidebarAnalytics;
