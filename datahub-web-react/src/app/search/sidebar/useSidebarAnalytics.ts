import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import {
    useBrowsePathLength,
    useEntityType,
    useMaybeBrowseResultGroup,
    useMaybeEnvironmentAggregation,
    useMaybePlatformAggregation,
} from './BrowseContext';

const useSidebarAnalytics = () => {
    const entityType = useEntityType();
    const environment = useMaybeEnvironmentAggregation()?.value;
    const platform = useMaybePlatformAggregation()?.value;
    const isBrowsePathNode = !!useMaybeBrowseResultGroup();
    const browsepathLength = useBrowsePathLength();
    const depth = 1 + (environment ? 1 : 0) + (platform ? 1 : 0) + browsepathLength;

    const trackToggleNodeEvent = (isOpen: boolean) => {
        analytics.event({
            type: EventType.BrowseV2ToggleNodeEvent,
            action: isOpen ? 'open' : 'close',
            entityType,
            ...(environment ? { environment } : {}),
            ...(platform ? { platform } : {}),
            isBrowsePathNode,
            depth,
        });
    };

    return { trackToggleNodeEvent } as const;
};

export default useSidebarAnalytics;
