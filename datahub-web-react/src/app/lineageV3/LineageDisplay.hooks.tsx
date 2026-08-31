import { useEffect, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { FetchStatus, LineageEntity, LineageNode } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

interface UseTrackLineageViewProps {
    initialized: boolean;
    type: EntityType;
    adjacencyList: Record<LineageDirection, Map<string, Set<string>>>;
    rootUrn: string;
    nodes: Map<string, LineageNode>;
}

/**
 * Custom hook to track visual lineage view analytics events
 */
export function useTrackLineageView({ initialized, type, adjacencyList, rootUrn, nodes }: UseTrackLineageViewProps) {
    const [hasTracked, setHasTracked] = useState(false);

    useEffect(() => {
        if (!initialized) return;

        // Reset tracking state when entity changes
        setHasTracked(false);
    }, [rootUrn, initialized]);

    useEffect(() => {
        if (!initialized || hasTracked) return;

        const upstreamNeighbors = adjacencyList[LineageDirection.Upstream].get(rootUrn) || new Set<string>();
        const downstreamNeighbors = adjacencyList[LineageDirection.Downstream].get(rootUrn) || new Set<string>();

        // Wait for nodes to have entity data before tracking
        const allNodes = Array.from(nodes.values());
        const entitiesLoaded =
            allNodes.length > 0 &&
            allNodes.every((node) => {
                if (node.type === 'lineage-filter') return true;
                const entity = node as LineageEntity;
                return !!entity.entity; // Wait for entity data to be loaded
            });

        if (!entitiesLoaded) return;

        // Check if there are expandable upstream entities
        const hasExpandableUpstreams = Array.from(nodes.values()).some((node) => {
            if (node.type === 'lineage-filter') return false;
            const entity = node as LineageEntity;
            const upstreamNotComplete = entity.fetchStatus[LineageDirection.Upstream] !== FetchStatus.COMPLETE;
            return upstreamNotComplete && upstreamNeighbors.size > 0;
        });

        // Check if there are expandable downstream entities
        const hasExpandableDownstreams = Array.from(nodes.values()).some((node) => {
            if (node.type === 'lineage-filter') return false;
            const entity = node as LineageEntity;
            const downstreamNotComplete = entity.fetchStatus[LineageDirection.Downstream] !== FetchStatus.COMPLETE;
            return downstreamNotComplete && downstreamNeighbors.size > 0;
        });

        // Check if any entities have column-level lineage
        const hasColumnLevelLineage = Array.from(nodes.values()).some((node) => {
            if (node.type === 'lineage-filter') return false;
            const entity = node as LineageEntity;
            return entity.entity?.fineGrainedLineages && entity.entity.fineGrainedLineages.length > 0;
        });

        analytics.event({
            type: EventType.VisualLineageViewEvent,
            entityType: type,
            numUpstreams: upstreamNeighbors.size,
            numDownstreams: downstreamNeighbors.size,
            hasColumnLevelLineage: hasColumnLevelLineage || false,
            hasExpandableUpstreamsV3: hasExpandableUpstreams,
            hasExpandableDownstreamsV3: hasExpandableDownstreams,
        });

        setHasTracked(true);
    }, [initialized, type, adjacencyList, rootUrn, nodes, hasTracked]);
}
