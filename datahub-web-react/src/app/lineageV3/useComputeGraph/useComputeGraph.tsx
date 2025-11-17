import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

import LineageGraphContext from '@app/lineageV3/LineageGraphContext';
import { LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';
import useLimitNodesPerLevel, { LevelsInfo } from '@app/lineageV3/useComputeGraph/useLimitNodesPerLevel';

import { EntityType } from '@types';

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
    resetPositions: boolean;
    levelsInfo: LevelsInfo;
    levelsMap?: Map<string, number>;
}

/**
 * Computes nodes and edges to render via ReactFlow.
 * See child functions for more details.
 */
export default function useComputeGraph(): ProcessedData {
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const {
        rootUrn,
        rootType,
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
    const displayVersionNumber = displayVersion[0];
    const { isModuleView } = useContext(LineageGraphContext);
    const limitNodesPerLevel = useLimitNodesPerLevel();

    const fineGrainedLineage = useMemo(
        () => {
            const fgl = getFineGrainedLineage({ nodes, edges, rootType });
            console.debug(fgl);
            return fgl;
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, rootType, dataVersion],
    );

    const { flowNodes, flowEdges, resetPositions, levelsInfo, levelsMap } = useMemo(
        () => {
            const context = {
                rootType,
                nodes,
                edges,
                adjacencyList,
                hideTransformations,
                showDataProcessInstances,
                showGhostEntities,
            };

            const base =
                rootType === EntityType.DataFlow
                    ? computeDataFlowGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus)
                    : computeImpactAnalysisGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus);

            if (!isModuleView) {
                return { ...base, levelsInfo: {} };
            }

            const limitedGraph = limitNodesPerLevel(base.flowNodes, base.flowEdges, rootUrn, 2);

            return {
                flowNodes: limitedGraph.nodes,
                flowEdges: limitedGraph.edges,
                resetPositions: base.resetPositions,
                levelsInfo: limitedGraph.levelsInfo,
                levelsMap: limitedGraph.levelsMap,
            };
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [
            rootUrn,
            rootType,
            nodes,
            edges,
            adjacencyList,
            nodeVersion,
            displayVersionNumber,
            hideTransformations,
            showDataProcessInstances,
            showGhostEntities,
            ignoreSchemaFieldStatus,
            dataVersion,
            isModuleView,
        ],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, resetPositions, levelsInfo, levelsMap };
}
