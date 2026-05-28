import { Edge, Node } from 'reactflow';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import {
    GraphStoreFields,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
    LineageTableEdgeData,
    LineageToggles,
    NodeContext,
} from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

const MEMBER_VERTICAL_GAP = 30;
const NEIGHBOUR_VERTICAL_GAP = 30;
const NEIGHBOUR_HORIZONTAL_GAP = 240;

type DomainGraphContext = Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>;

/**
 * Computes the lineage graph for a Domain.
 *
 * Layout:
 *
 *   1. The source Domain is rendered as a bounding box at the origin, containing its child
 *      DataProducts (member nodes) stacked vertically. Members don't fan out their own lineage in
 *      v1 -- their relationships flow through DataProductUpstreams / asset-level upstreams, which
 *      get their own visualisation in the dedicated DataProduct and dataset lineage tabs.
 *   2. Neighbour Domains discovered by the standard `searchAcrossLineage` walk on the root Domain
 *      URN (Phase A: declared edges from the `domainUpstreams` aspect) are rendered as standalone
 *      neighbour bounding boxes. Upstream neighbours sit to the left of the source box, downstream
 *      to the right; each is ordered by URN for stable layout. The walk uses the same machinery
 *      as every other entity type, so a follow-up that adds inferred Domain edges (e.g. via
 *      member-asset lineage aggregation) plugs straight into the same adjacency list without any
 *      change to this file.
 *
 * No explicit edges are drawn between the source box and neighbour boxes in v1 -- ReactFlow
 * handles attach to entity nodes, not to bounding boxes, so a Domain -> Domain edge between two
 * bboxes would require a custom edge type. Visual proximity (upstream-left, downstream-right) is
 * enough to communicate direction; a count-labelled custom edge lands in a follow-up.
 */
export default function computeDomainGraph(urn: string, type: EntityType, context: DomainGraphContext) {
    const { nodes, adjacencyList } = context;
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge<LineageTableEdgeData>[] = [];

    const memberFlowNodes = layoutMembers(nodes, urn);
    flowNodes.push(...memberFlowNodes);

    const boundingBox = addSourceBoundingBox(flowNodes, memberFlowNodes, nodes.get(urn), type);

    addNeighborDomainBoxes(flowNodes, nodes, adjacencyList, boundingBox, urn);

    return { flowNodes, flowEdges, resetPositions: false };
}

function layoutMembers(nodes: NodeContext['nodes'], rootUrn: Urn): LineageVisualizationNode[] {
    const members: LineageEntity[] = [];
    nodes.forEach((node) => {
        if (node.parentDomain === rootUrn && node.urn !== rootUrn) {
            members.push(node);
        }
    });

    return members.map((member, idx) => ({
        id: member.urn,
        type: LINEAGE_ENTITY_NODE_NAME,
        position: {
            x: BOUNDING_BOX_PADDING,
            y: BOUNDING_BOX_PADDING + idx * (LINEAGE_NODE_HEIGHT + MEMBER_VERTICAL_GAP),
        },
        data: member,
        draggable: true,
        selectable: true,
    }));
}

function addSourceBoundingBox(
    flowNodes: LineageVisualizationNode[],
    memberFlowNodes: LineageVisualizationNode[],
    rootNode: LineageEntity | undefined,
    rootType: EntityType,
): Node<LineageBoundingBox> {
    const width = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 2;

    // Always size the box for at least one row so an empty Domain still gets a visible card.
    const memberCount = Math.max(memberFlowNodes.length, 1);
    const height =
        LINEAGE_NODE_HEIGHT +
        BOUNDING_BOX_PADDING * 2 +
        (memberCount - 1) * (LINEAGE_NODE_HEIGHT + MEMBER_VERTICAL_GAP);

    const colorHex = rootNode?.entity?.genericEntityProperties?.displayProperties?.colorHex ?? undefined;

    const box: Node<LineageBoundingBox> = {
        id: rootNode?.urn ?? '',
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x: 0, y: 0 },
        data: {
            urn: rootNode?.urn ?? '',
            type: rootType,
            entity: rootNode?.entity,
            colorHex,
        },
        selectable: true,
        draggable: true,
        style: { width, height, zIndex: -2 },
        width,
        height,
    };

    flowNodes.unshift(box);
    return box;
}

/**
 * Renders neighbour-Domain boxes for every Domain URN that the standard searchAcrossLineage walk
 * surfaced as a direct upstream / downstream neighbour of the root Domain.
 *
 * Exported for unit testing.
 */
export function addNeighborDomainBoxes(
    flowNodes: LineageVisualizationNode[],
    nodes: NodeContext['nodes'],
    adjacencyList: NodeContext['adjacencyList'],
    sourceBox: Node<LineageBoundingBox>,
    rootUrn: Urn,
): void {
    layoutNeighbours(
        flowNodes,
        collectNeighbourDomains(adjacencyList, nodes, rootUrn, LineageDirection.Upstream),
        LineageDirection.Upstream,
        sourceBox,
    );
    layoutNeighbours(
        flowNodes,
        collectNeighbourDomains(adjacencyList, nodes, rootUrn, LineageDirection.Downstream),
        LineageDirection.Downstream,
        sourceBox,
    );
}

function collectNeighbourDomains(
    adjacencyList: NodeContext['adjacencyList'],
    nodes: NodeContext['nodes'],
    rootUrn: Urn,
    direction: LineageDirection,
): LineageEntity[] {
    const neighbourUrns = adjacencyList[direction].get(rootUrn) ?? new Set<Urn>();
    const out: LineageEntity[] = [];
    neighbourUrns.forEach((neighbourUrn) => {
        if (neighbourUrn === rootUrn) return;
        const neighbour = nodes.get(neighbourUrn);
        if (!neighbour) return;
        if (neighbour.type !== EntityType.Domain) return;
        out.push(neighbour);
    });
    // Stable layout: alphabetical by URN. Ordering by name would require resolving entity data
    // which may load asynchronously and cause jitter.
    out.sort((a, b) => a.urn.localeCompare(b.urn));
    return out;
}

function layoutNeighbours(
    flowNodes: LineageVisualizationNode[],
    neighbours: LineageEntity[],
    direction: LineageDirection,
    sourceBox: Node<LineageBoundingBox>,
): void {
    if (!neighbours.length) return;

    const boxWidth = sourceBox.width ?? (sourceBox.style?.width as number) ?? LINEAGE_NODE_WIDTH;
    const boxHeight = sourceBox.height ?? (sourceBox.style?.height as number) ?? LINEAGE_NODE_HEIGHT;

    const neighbourBoxHeight = LINEAGE_NODE_HEIGHT + BOUNDING_BOX_PADDING * 2;
    const neighbourBoxWidth = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 2;

    const totalSpan = neighbours.length * (neighbourBoxHeight + NEIGHBOUR_VERTICAL_GAP) - NEIGHBOUR_VERTICAL_GAP;
    const startY = sourceBox.position.y + (boxHeight - totalSpan) / 2;

    const x =
        direction === LineageDirection.Upstream
            ? sourceBox.position.x - NEIGHBOUR_HORIZONTAL_GAP - neighbourBoxWidth
            : sourceBox.position.x + boxWidth + NEIGHBOUR_HORIZONTAL_GAP;

    neighbours.forEach((neighbour, idx) => {
        const y = startY + idx * (neighbourBoxHeight + NEIGHBOUR_VERTICAL_GAP);
        const colorHex = neighbour.entity?.genericEntityProperties?.displayProperties?.colorHex ?? undefined;
        flowNodes.push({
            id: neighbour.urn,
            type: LINEAGE_BOUNDING_BOX_NODE_NAME,
            position: { x, y },
            data: {
                urn: neighbour.urn,
                type: EntityType.Domain,
                entity: neighbour.entity,
                colorHex,
            },
            selectable: true,
            draggable: true,
            style: { width: neighbourBoxWidth, height: neighbourBoxHeight, zIndex: -2 },
            width: neighbourBoxWidth,
            height: neighbourBoxHeight,
        });
    });
}
