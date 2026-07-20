import type { XYPosition } from '@reactflow/core/dist/esm/types';
import { Node } from 'reactflow';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { TRANSFORMATION_NODE_SIZE } from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import {
    LINEAGE_FILTER_TYPE,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
    isTransformational,
} from '@app/lineageV3/common';
import NodeBuilder, { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeConnectedComponents from '@app/lineageV3/useComputeGraph/computeConnectedComponents';
import { BoxLayout, DataProductGroup, GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.utils';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';

import { EntityType, LineageDirection } from '@types';

// Vertical gap between separate connected components within a data product bounding box: one node
// height (to clear the component above) plus a small gap.
const COMPONENT_SEPARATION = 24 + LINEAGE_NODE_HEIGHT;

/**
 * Lays out the displayed members of one data product via NodeBuilder, as in the standard
 * impact-analysis view but with layers computed from intra-product topology (members lack a shared
 * home node).
 * Members with revealed lineage to each other are placed horizontally; disconnected components are
 * stacked vertically.
 * Returns member nodes positioned relative to the resulting bounding box, and the box's dimensions.
 */
export default function layoutDataProductInterior(
    group: DataProductGroup,
    graphStore: GraphStore,
    ignoreSchemaFieldStatus: boolean,
): BoxLayout | undefined {
    const config: HideNodesConfig = {
        hideTransformations: false,
        hideDataProcessInstances: false,
        hideGhostEntities: false,
        ignoreSchemaFieldStatus,
    };
    const subStore = {
        ...hideNodes(group.urn, graphStore.rootType, config, graphStore, (node) => group.memberUrns.has(node.urn)),
        rootType: graphStore.rootType,
    };

    const { displayedNodesByRoots, parents } = computeConnectedComponents(subStore);
    const relativeNodes: LineageVisualizationNode[] = [];

    displayedNodesByRoots
        .sort(([_rootsA, componentA], [_rootsB, componentB]) => componentB.length - componentA.length)
        .forEach(([roots, displayedNodes]) => {
            const maxY = relativeNodes.reduce((max, node) => Math.max(max, node.position.y), 0);
            const offset: [number, number] = [0, relativeNodes.length ? maxY + COMPONENT_SEPARATION : 0];
            const offsets = new Map<LineageDirection | undefined, [number, number]>([
                [undefined, offset],
                [LineageDirection.Upstream, offset],
                [LineageDirection.Downstream, offset],
            ]);
            // A member's `direction` is relative to the home data product and would mirror
            // NodeBuilder's layering for upstream boxes (children placed left of their parents).
            // The interior layout is purely topological, left to right, so lay out direction-less
            // copies; the original direction is restored on the rendered member data below.
            const layoutNodes = displayedNodes.map((node) =>
                node.type === LINEAGE_FILTER_TYPE ? node : { ...node, direction: undefined },
            );
            const nodeBuilder = new NodeBuilder(
                group.urn,
                EntityType.DataProduct,
                roots,
                layoutNodes,
                parents,
                true,
                true,
            );
            relativeNodes.push(...nodeBuilder.createNodes(subStore, ignoreSchemaFieldStatus, offsets));
        });

    if (!relativeNodes.length) return undefined;

    // Transformational members render as small circles, not full cards, so the box extent must use
    // each node's rendered size — otherwise a column of circles reserves a full card's width/height.
    const isMini = (node: LineageVisualizationNode) =>
        node.type !== LINEAGE_FILTER_TYPE && isTransformational(node.data as LineageEntity, graphStore.rootType);
    const minX = Math.min(...relativeNodes.map((node) => node.position.x));
    const minY = Math.min(...relativeNodes.map((node) => node.position.y));
    const maxX = Math.max(
        ...relativeNodes.map(
            (node) => node.position.x + (isMini(node) ? TRANSFORMATION_NODE_SIZE : LINEAGE_NODE_WIDTH),
        ),
    );
    const maxY = Math.max(
        ...relativeNodes.map(
            (node) => node.position.y + (isMini(node) ? TRANSFORMATION_NODE_SIZE : LINEAGE_NODE_HEIGHT),
        ),
    );
    const width = maxX - minX + BOUNDING_BOX_PADDING * 2;
    const height = maxY - minY + BOUNDING_BOX_PADDING * 2;

    const memberNodes = relativeNodes.map((node) => {
        const data = node.data as LineageEntity;
        const id = createMemberNodeId(group.urn, node.id);
        return {
            ...node,
            id,
            parentId: group.urn,
            extent: 'parent' as const,
            position: {
                x: node.position.x + BOUNDING_BOX_PADDING - minX,
                y: node.position.y + BOUNDING_BOX_PADDING - minY,
            },
            data: { ...data, id, direction: graphStore.nodes.get(node.id)?.direction },
        };
    });

    return { group, memberNodes, width, height };
}

export function createBoundingBoxNode(box: BoxLayout, position: XYPosition | undefined): Node<LineageBoundingBox> {
    const { group, width, height } = box;
    return {
        id: group.urn,
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: position ?? { x: 0, y: 0 },
        data: {
            urn: group.urn,
            type: EntityType.DataProduct,
            entity: group.entity,
            colorHex: group.colorHex,
            memberCount: box.memberNodes.length,
        },
        selectable: true,
        draggable: true,
        style: { width, height, zIndex: -2 },
        width,
        height,
    };
}
