import { EntityType } from '../../../types.generated';
import { BrowseV2SelectNodeEvent, BrowseV2ToggleNodeEvent, EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    useBrowsePathLength,
    useEntityType,
    useMaybeEnvironmentAggregation,
    useMaybePlatformAggregation,
} from './BrowseContext';

const useSidebarAnalytics = () => {
    const registry = useEntityRegistry();
    const entityType = useEntityType();
    const entityCollectionName = registry.getCollectionName(entityType);
    const environment = useMaybeEnvironmentAggregation()?.value;
    const platformAggregation = useMaybePlatformAggregation();
    const platform = platformAggregation?.entity
        ? registry.getDisplayName(EntityType.DataPlatform, platformAggregation.entity)
        : platformAggregation?.value;
    const targetDepth = (environment ? 1 : 0) + (platform ? 1 : 0) + useBrowsePathLength();

    const trackToggleNodeEvent = (isOpen: boolean, targetNode: BrowseV2ToggleNodeEvent['targetNode']) => {
        analytics.event({
            type: EventType.BrowseV2ToggleNodeEvent,
            targetNode,
            action: isOpen ? 'open' : 'close',
            entity: entityCollectionName,
            ...(environment ? { environment } : {}),
            ...(platform ? { platform } : {}),
            targetDepth,
        });
    };

    const trackBrowseNodeSelected = (
        action: BrowseV2SelectNodeEvent['action'],
        targetNode: BrowseV2SelectNodeEvent['targetNode'],
    ) => {
        analytics.event({
            type: EventType.BrowseV2SelectNodeEvent,
            targetNode,
            action,
            entity: entityCollectionName,
            ...(environment ? { environment } : {}),
            ...(platform ? { platform } : {}),
            targetDepth,
        });
    };

    return { trackToggleNodeEvent, trackBrowseNodeSelected } as const;
};

export default useSidebarAnalytics;
