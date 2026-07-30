import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

import LineageGraphContext from '@app/lineageV3/LineageGraphContext';
import { LineageFilter, LineageNodesContext, NodeContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import computeDataProductGraph from '@app/lineageV3/useComputeGraph/dataProduct/computeDataProductGraph';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';
import { useAppConfig } from '@app/useAppConfig';

import { EntityType } from '@types';

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
    resetPositions: boolean;
    levelsInfo: LevelsInfo;
    levelsMap?: Map<string, number>;
    /** Pagination state for each node and direction with filtered-out children,
     * keyed by `createLineageFilterNodeId`. */
    lineageFilters: Map<string, LineageFilter>;
    /** Adjacency list of the computed graph, e.g. with edges connected through hidden nodes.
     * Used to compute node highlighting. */
    adjacencyList: NodeContext['adjacencyList'];
}

/**
 * Computes nodes and edges to render via ReactFlow.
 * See child functions for more details.
 */
export default function useComputeGraph(): ProcessedData {
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();
    const appConfig = useAppConfig();
    const { showLineageFilterNodes } = appConfig.config.featureFlags;
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
        outputPortsOnly,
        dataProductEntities,
    } = useContext(LineageNodesContext);
    const displayVersionNumber = displayVersion[0];
    const { isModuleView } = useContext(LineageGraphContext);

    const fineGrainedLineage = useMemo(
        () => {
            const fgl = getFineGrainedLineage({ nodes, edges, rootType });
            console.debug(fgl);
            return fgl;
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, rootType, dataVersion],
    );

    const {
        flowNodes,
        flowEdges,
        resetPositions,
        levelsInfo,
        levelsMap,
        lineageFilters,
        adjacencyList: displayedAdjacencyList,
    } = useMemo(
        () => {
            const context = {
                rootType,
                nodes,
                edges,
                adjacencyList,
                hideTransformations,
                showDataProcessInstances,
                showGhostEntities,
                outputPortsOnly,
                dataProductEntities,
            };

            if (rootType === EntityType.DataFlow) {
                const result = computeDataFlowGraph(
                    rootUrn,
                    rootType,
                    context,
                    ignoreSchemaFieldStatus,
                    showLineageFilterNodes,
                );
                return {
                    ...result,
                    levelsInfo: {},
                    levelsMap: new Map(),
                };
            }

            if (rootType === EntityType.DataProduct) {
                const result = computeDataProductGraph(
                    rootUrn,
                    context,
                    ignoreSchemaFieldStatus,
                    showLineageFilterNodes,
                );
                return {
                    ...result,
                    levelsInfo: {},
                    levelsMap: new Map(),
                };
            }

            return computeImpactAnalysisGraph(
                rootUrn,
                rootType,
                context,
                ignoreSchemaFieldStatus,
                undefined,
                new Map(),
                undefined,
                isModuleView,
                showLineageFilterNodes,
            );
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
            outputPortsOnly,
            ignoreSchemaFieldStatus,
            dataVersion,
            isModuleView,
            showLineageFilterNodes,
        ],
    );

    return {
        flowNodes,
        flowEdges,
        fineGrainedLineage,
        resetPositions,
        levelsInfo,
        levelsMap,
        lineageFilters,
        adjacencyList: displayedAdjacencyList,
    };
}
