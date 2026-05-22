import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';

import LineageGraphContext from '@app/lineageV3/LineageGraphContext';
import { LineageNodesContext, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeDataFlowGraph from '@app/lineageV3/useComputeGraph/computeDataFlowGraph';
import computeDataProductGraph from '@app/lineageV3/useComputeGraph/computeDataProductGraph';
import computeDomainGraph from '@app/lineageV3/useComputeGraph/computeDomainGraph';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import getFineGrainedLineage, { FineGrainedLineageData } from '@app/lineageV3/useComputeGraph/getFineGrainedLineage';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

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
        aggregatedDomainEdges,
        aggregatedInnerEdges,
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
                aggregatedDomainEdges,
                aggregatedInnerEdges,
            };

            if (rootType === EntityType.DataFlow) {
                const result = computeDataFlowGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus);
                return {
                    ...result,
                    levelsInfo: {},
                    levelsMap: new Map(),
                };
            }

            if (rootType === EntityType.DataProduct) {
                const result = computeDataProductGraph(rootUrn, rootType, context, ignoreSchemaFieldStatus);
                return {
                    ...result,
                    levelsInfo: {},
                    levelsMap: new Map(),
                };
            }

            if (rootType === EntityType.Domain) {
                const result = computeDomainGraph(rootUrn, rootType, context);
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
            ignoreSchemaFieldStatus,
            dataVersion,
            isModuleView,
            aggregatedDomainEdges,
            aggregatedInnerEdges,
        ],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, resetPositions, levelsInfo, levelsMap };
}
