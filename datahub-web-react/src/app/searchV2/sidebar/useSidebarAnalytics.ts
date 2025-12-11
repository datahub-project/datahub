/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    BrowseV2EntityLinkClickEvent,
    BrowseV2SelectNodeEvent,
    BrowseV2ToggleNodeEvent,
    EventType,
} from '@app/analytics';
import analytics from '@app/analytics/analytics';
import {
    useBrowsePathLength,
    useEntityType,
    useMaybeEnvironmentAggregation,
    useMaybePlatformAggregation,
} from '@app/searchV2/sidebar/BrowseContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const useSidebarAnalytics = () => {
    const registry = useEntityRegistry();
    const entityType = useEntityType();
    const environmentAggregation = useMaybeEnvironmentAggregation();
    const platformAggregation = useMaybePlatformAggregation();
    const entityDisplayName = entityType && registry.getCollectionName(entityType);
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
