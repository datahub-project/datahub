import { useCallback, useContext, useEffect, useState } from 'react';
import { FitViewOptions, useOnViewportChange, useReactFlow } from 'reactflow';

import analytics, { EventType } from '@app/analytics';
import { FetchStatus, LineageEntity, LineageNode, LineageNodesContext } from '@app/lineageV3/common';

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

/** Fits the whole graph once it loads. Returns whether a fit is still scheduled or animating. */
export function useFitView(loaded: boolean): boolean {
    const { fitView } = useReactFlow();
    const { displayVersion } = useContext(LineageNodesContext);
    const [, displayVersionNodes] = displayVersion;
    const [pendingFits, setPendingFits] = useState(0);

    const scheduleFit = useCallback(
        (delay: number, options: FitViewOptions) => {
            let finished = false;
            const finish = () => {
                if (finished) return;
                finished = true;
                setPendingFits((n) => n - 1);
            };
            setPendingFits((n) => n + 1);
            const fitTimeout = setTimeout(() => fitView(options), delay);
            const finishTimeout = setTimeout(finish, delay + (options.duration ?? 0));
            return () => {
                clearTimeout(fitTimeout);
                clearTimeout(finishTimeout);
                finish();
            };
        },
        [fitView],
    );

    useEffect(() => {
        if (!loaded) return () => {};
        return scheduleFit(1000, { duration: 1000, maxZoom: 2, padding: 0.5 });
    }, [loaded, scheduleFit]);

    useEffect(() => {
        if (!loaded || !displayVersionNodes.length) return () => {};
        return scheduleFit(100, {
            duration: 1000,
            nodes: displayVersionNodes.map((urn) => ({ id: urn })),
            maxZoom: 1,
            padding: 0.2,
        });
    }, [loaded, displayVersionNodes, scheduleFit]);

    return pendingFits > 0;
}

const SETTLED_DELAY_MS = 250;

/**
 * Whether the graph has stopped changing: nothing is loading and the viewport is still.
 * Waits briefly before reporting settled, since back-to-back requests leave a render between them with neither in flight.
 */
export function useLineageSettled(loading: boolean, fitting: boolean): boolean {
    const [moving, setMoving] = useState(false);
    const [settled, setSettled] = useState(false);
    useOnViewportChange({
        onStart: useCallback(() => setMoving(true), []),
        onEnd: useCallback(() => setMoving(false), []),
    });

    const busy = loading || fitting || moving;
    useEffect(() => {
        if (busy) {
            setSettled(false);
            return () => {};
        }
        const timeout = setTimeout(() => setSettled(true), SETTLED_DELAY_MS);
        return () => clearTimeout(timeout);
    }, [busy]);

    return settled && !busy;
}
