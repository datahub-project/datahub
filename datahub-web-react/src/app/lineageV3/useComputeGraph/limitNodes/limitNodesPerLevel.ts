import { LineageNode, isTransformational } from '@app/lineageV3/common';
import {
    LevelsInfo,
    assignLevelsToFilterNodes,
    buildAdjacency,
    computeEntityLevels,
    computeLimitedLevels,
    groupNodesByLevel,
    isEntityNode,
    mergeTransformNodesIntoLevels,
} from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

import { EntityType, LineageDirection } from '@types';

export interface LimitedGraphResult {
    limitedNodes: LineageNode[];
    levelsInfo: LevelsInfo;
    levelsMap: Map<string, number>;
}

interface Props {
    nodes: LineageNode[];
    rootUrn: string;
    rootType: EntityType;
    adjacencyList: Record<LineageDirection, Map<string, Set<string>>>;
    maxPerLevel: number;
}

export function limitNodesPerLevel({
    nodes,
    rootUrn,
    rootType,
    adjacencyList,
    maxPerLevel = 2,
}: Props): LimitedGraphResult {
    const visibleNodeIds = new Set(nodes.map((node) => node.id));
    const nodeById = new Map(nodes.map((node) => [node.id, node]));

    // Build adjacencies excluding transform nodes
    const outgoing = buildAdjacency(
        adjacencyList[LineageDirection.Downstream],
        nodes,
        rootType,
        rootUrn,
        visibleNodeIds,
        nodeById,
    );

    const incoming = buildAdjacency(
        adjacencyList[LineageDirection.Upstream],
        nodes,
        rootType,
        rootUrn,
        visibleNodeIds,
        nodeById,
    );

    // Compute entity levels
    const entityNodeIds = new Set(nodes.filter((node) => isEntityNode(node, rootType)).map((node) => node.id));
    const levelsMap = computeEntityLevels(rootUrn, outgoing, incoming, entityNodeIds);

    assignLevelsToFilterNodes(nodes, levelsMap);

    const nodesByLevel = groupNodesByLevel(nodes, levelsMap);

    // To ensure that transform nodes get included in levels info counts
    const mergedNodesByLevel = mergeTransformNodesIntoLevels(
        nodesByLevel,
        nodes,
        levelsMap,
        rootType,
        adjacencyList[LineageDirection.Downstream],
        rootUrn,
    );

    const { allowedNodeIds, levelsInfo } = computeLimitedLevels(mergedNodesByLevel, maxPerLevel, rootType);

    // Add transform nodes for rendering
    nodes.forEach((node) => {
        if (isTransformational(node, rootType)) {
            allowedNodeIds.add(node.id);
        }
    });

    const limitedNodes = nodes.filter((node) => allowedNodeIds.has(node.id));

    return {
        limitedNodes,
        levelsInfo,
        levelsMap,
    };
}
