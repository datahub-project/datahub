/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

import { LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';

import { EntityType } from '@types';

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
    resetPositions: boolean;
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

    const fineGrainedLineage = useMemo(
        () => {
            const fgl = getFineGrainedLineage({ nodes, edges, rootType });
            console.debug(fgl);
            return fgl;
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, rootType, dataVersion],
    );

    const { flowNodes, flowEdges, resetPositions } = useMemo(
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
            if (rootType === EntityType.DataFlow) {
                return computeDataFlowGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus);
            }
            return computeImpactAnalysisGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus);
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
        ],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, resetPositions };
}
