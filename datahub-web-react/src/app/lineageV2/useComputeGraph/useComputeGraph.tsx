import { LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV2/common';
import NodeBuilder, { LineageVisualizationNode } from '@app/lineageV2/NodeBuilder';
import hideNodes, { HideNodesConfig } from '@app/lineageV2/useComputeGraph/filterNodes';
import getDisplayedNodes from '@app/lineageV2/useComputeGraph/getDisplayedNodes';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV2/useComputeGraph/getFineGrainedLineage';
import orderNodes from '@app/lineageV2/useComputeGraph/orderNodes';
import usePrevious from '@app/shared/usePrevious';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EntityType, LineageDirection } from '@types';
import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
    resetPositions: boolean;
}

export default function useComputeGraph(urn: string, type: EntityType): ProcessedData {
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const {
        nodes,
        edges,
        adjacencyList,
        nodeVersion,
        dataVersion,
        displayVersion,
        hideTransformations,
        showDataProcessInstances,
        showGhostEntities,
    } = useContext(LineageNodesContext);
    const entityRegistry = useEntityRegistryV2();
    const displayVersionNumber = displayVersion[0];

    const fineGrainedLineage = useMemo(
        () => {
            const fgl = getFineGrainedLineage({ nodes, edges }, entityRegistry);
            console.debug(fgl);
            return fgl;
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, dataVersion],
    );

    const prevHideTransformations = usePrevious(hideTransformations);
    const { flowNodes, flowEdges, resetPositions } = useMemo(
        () => {
            const smallContext = { nodes, edges, adjacencyList };
            console.debug(smallContext);

            // Computed before nodes are hidden by `hideNodes`, to keep node order consistent.
            // Includes nodes that will be hidden, but they'll be filtered out by `getDisplayedNodes`.
            const orderedNodes = {
                [LineageDirection.Upstream]: orderNodes(urn, LineageDirection.Upstream, smallContext),
                [LineageDirection.Downstream]: orderNodes(urn, LineageDirection.Downstream, smallContext),
            };

            const config: HideNodesConfig = {
                hideTransformations,
                hideDataProcessInstances: !showDataProcessInstances,
                hideGhostEntities: !showGhostEntities,
                ignoreSchemaFieldStatus,
            };
            const newSmallContext = hideNodes(urn, config, smallContext);
            console.debug(newSmallContext);

            const { displayedNodes, parents } = getDisplayedNodes(urn, orderedNodes, newSmallContext);
            const nodeBuilder = new NodeBuilder(urn, type, displayedNodes, parents);

            const orderIndices = {
                [urn]: 0,
                ...Object.fromEntries(orderedNodes[LineageDirection.Downstream].map((e, idx) => [e.id, idx + 1])),
                ...Object.fromEntries(orderedNodes[LineageDirection.Upstream].map((e, idx) => [e.id, -idx - 1])),
            };
            return {
                flowNodes: nodeBuilder
                    .createNodes(newSmallContext, ignoreSchemaFieldStatus)
                    .sort((a, b) => (orderIndices[a.id] || 0) - (orderIndices[b.id] || 0)),
                flowEdges: nodeBuilder.createEdges(newSmallContext.edges),
                resetPositions: prevHideTransformations !== hideTransformations,
            };
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [
            urn,
            type,
            nodes,
            edges,
            adjacencyList,
            nodeVersion,
            displayVersionNumber,
            hideTransformations,
            prevHideTransformations,
            showDataProcessInstances,
            showGhostEntities,
            ignoreSchemaFieldStatus,
            dataVersion,
        ],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, resetPositions };
}
