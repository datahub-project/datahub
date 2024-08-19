import { LineageNodesContext } from '@app/lineageV2/common';
import NodeBuilder, { LineageVisualizationNode } from '@app/lineageV2/NodeBuilder';
import hideNodes from '@app/lineageV2/useComputeGraph/filterNodes';
import getDisplayedNodes from '@app/lineageV2/useComputeGraph/getDisplayedNodes';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV2/useComputeGraph/getFineGrainedLineage';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EntityType } from '@types';
import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
}

export default function useComputeGraph(urn: string, type: EntityType): ProcessedData {
    const { nodes, edges, adjacencyList, nodeVersion, dataVersion, displayVersion, hideTransformations } =
        useContext(LineageNodesContext);
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

    const [flowNodes, flowEdges] = useMemo(
        () => {
            const smallContext = { nodes, edges, adjacencyList };
            console.debug(smallContext);
            const config = { hideTransformations };
            const newSmallContext = hideNodes(urn, config, smallContext);
            console.debug(newSmallContext);

            const displayedNodes = getDisplayedNodes(urn, newSmallContext);
            const nodeBuilder = new NodeBuilder(urn, type, displayedNodes);
            return [
                nodeBuilder.createNodes(newSmallContext.adjacencyList),
                nodeBuilder.createEdges(newSmallContext.edges),
            ];
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, adjacencyList, nodeVersion, displayVersionNumber, hideTransformations],
    );

    return { flowNodes, flowEdges, fineGrainedLineage };
}
