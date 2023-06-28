import { EntityType } from '../../../types.generated';
import {
    BrowseV2EntityLinkClickEvent,
    BrowseV2SelectNodeEvent,
    BrowseV2ToggleNodeEvent,
    EventType,
} from '../../analytics';
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
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = useMaybePlatformAggregation();
    const entityDisplayName = registry.getCollectionName(entityType);
    const environmentDisplayName = environmentAggregation?.value;
    const platformDisplayName = platformAggregation?.entity
        ? registry.getDisplayName(EntityType.DataPlatform, platformAggregation.entity)
        : platformAggregation?.value;
    const targetDepth = (environmentAggregation ? 1 : 0) + (platformAggregation ? 1 : 0) + useBrowsePathLength();

    const trackToggleNodeEvent = (isOpen: boolean, targetNode: BrowseV2ToggleNodeEvent['targetNode']) => {
        analytics.event({
            type: EventType.BrowseV2ToggleNodeEvent,
            targetNode,
            action: isOpen ? 'open' : 'close',
            entity: entityDisplayName,
            environment: environmentDisplayName,
            platform: platformDisplayName,
            targetDepth,
        });
    };

    const trackSelectNodeEvent = (
        action: BrowseV2SelectNodeEvent['action'],
        targetNode: BrowseV2SelectNodeEvent['targetNode'],
    ) => {
        analytics.event({
            type: EventType.BrowseV2SelectNodeEvent,
            targetNode,
            action,
            entity: entityDisplayName,
            environment: environmentDisplayName,
            platform: platformDisplayName,
            targetDepth,
        });
    };

    const trackEntityLinkClickEvent = (targetNode: BrowseV2EntityLinkClickEvent['targetNode']) => {
        analytics.event({
            type: EventType.BrowseV2EntityLinkClickEvent,
            targetNode,
            entity: entityDisplayName,
            environment: environmentDisplayName,
            platform: platformDisplayName,
            targetDepth,
        });
    };

    return { trackToggleNodeEvent, trackSelectNodeEvent, trackEntityLinkClickEvent } as const;
};

export default useSidebarAnalytics;
