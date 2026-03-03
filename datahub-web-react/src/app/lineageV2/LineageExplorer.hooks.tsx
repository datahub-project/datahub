import { useEffect, useState } from 'react';

import analytics, { EventType } from '@app/analytics';
import { LineageEntity } from '@app/lineageV2/common';
import { ColumnAsset, LineageAssetType } from '@app/lineageV2/types';

import { EntityType, LineageDirection } from '@types';

interface UseTrackLineageViewV2Props {
    initialized: boolean;
    urn: string;
    type: EntityType;
    adjacencyList: Record<LineageDirection, Map<string, Set<string>>>;
    nodes: Map<string, LineageEntity>;
    nodeVersion: number;
}

/**
 * Custom hook to track visual lineage view analytics events for V2
 */
export function useTrackLineageViewV2({
    initialized,
    urn,
    type,
    adjacencyList,
    nodes,
    nodeVersion,
}: UseTrackLineageViewV2Props) {
    const [hasTracked, setHasTracked] = useState(false);

    useEffect(() => {
        if (!initialized) return;

        // Reset tracking state when entity changes
        setHasTracked(false);
    }, [urn, initialized]);

    useEffect(() => {
        if (!initialized || hasTracked) return;

        // Wait for some nodes to be loaded before tracking
        if (nodeVersion === 0) return;

        const upstreamNeighbors = adjacencyList[LineageDirection.Upstream].get(urn) || new Set<string>();
        const downstreamNeighbors = adjacencyList[LineageDirection.Downstream].get(urn) || new Set<string>();

        // Check if any loaded entities have column-level lineage data
        const hasColumnLevelLineage = Array.from(nodes.values()).some((node) => {
            const { entity } = node;
            if (!entity) return false;

            // Check for fine-grained lineage data
            if (entity.fineGrainedLineages && entity.fineGrainedLineages.length > 0) return true;

            // Check for input fields (chart -> dataset column relationships)
            if (entity.inputFields && entity.inputFields.fields && entity.inputFields.fields.length > 0) return true;

            // Check for lineage assets with column information
            if (entity.lineageAssets && entity.lineageAssets.size > 0) {
                return Array.from(entity.lineageAssets.values()).some((asset) => {
                    // Type guard to ensure we have a ColumnAsset
                    if (asset.type === LineageAssetType.Column) {
                        const columnAsset = asset as ColumnAsset;
                        return (columnAsset.numUpstream || 0) > 0 || (columnAsset.numDownstream || 0) > 0;
                    }
                    return false;
                });
            }

            return false;
        });

        analytics.event({
            type: EventType.VisualLineageViewEvent,
            entityType: type,
            numUpstreams: upstreamNeighbors.size,
            numDownstreams: downstreamNeighbors.size,
            hasColumnLevelLineage,
            // set these both to false since it is difficult to fetch in V2 code and we are moving to V3 for default anyway.
            hasExpandableUpstreamsV2: false,
            hasExpandableDownstreamsV2: false,
        });

        setHasTracked(true);
    }, [initialized, urn, type, adjacencyList, nodes, nodeVersion, hasTracked]);
}
