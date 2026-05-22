import { Edge, Node } from 'reactflow';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import {
    AggregatedDomainEdge,
    FetchStatus,
    GraphStoreFields,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
    LineageTableEdgeData,
    LineageToggles,
    NodeContext,
} from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

const MEMBER_VERTICAL_GAP = 30;
const NEIGHBOUR_VERTICAL_GAP = 30;
const NEIGHBOUR_HORIZONTAL_GAP = 240;

type DomainGraphContext = Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType' | 'aggregatedDomainEdges'>;

/**
 * Computes the lineage graph for a Domain.
 *
 * Layout:
 * 1. The source Domain is rendered as a bounding box at the origin, containing its child
 *    DataProducts (member nodes) stacked vertically. There are no intra-box edges in v1 —
 *    members aren't lineage-expanded individually because the resolver does the aggregation.
 * 2. Neighbour Domains, taken straight from {@link AggregatedDomainEdge} entries that the
 *    {@code domainLineage} resolver returned, are rendered as standalone {@link
 *    LineageEntityNode}s — upstream neighbours to the left of the source box, downstream to
 *    the right. Each neighbour's display name carries its `memberMatchCount` so the
 *    aggregated counts are visible without bespoke edge labels.
 *
 * Why no edges between BBox and neighbour nodes in v1? BBoxes don't expose ReactFlow handles
 * directly, and fanning out per-member → per-neighbour edges would produce M×N visual noise
 * that defeats the point of server-side aggregation. The follow-up "pagination UI" branch
 * will add count-labelled edges anchored on a synthetic source anchor (and Show More / Show
 * All controls driven by start/count).
 */
export default function computeDomainGraph(urn: string, type: EntityType, context: DomainGraphContext) {
    const { nodes, aggregatedDomainEdges } = context;
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge<LineageTableEdgeData>[] = [];

    const memberFlowNodes = layoutMembers(nodes, urn);
    flowNodes.push(...memberFlowNodes);

    const boundingBox = addSourceBoundingBox(flowNodes, memberFlowNodes, nodes.get(urn));

    if (aggregatedDomainEdges && aggregatedDomainEdges.size > 0) {
        flowNodes.push(...layoutNeighbours(aggregatedDomainEdges, LineageDirection.Upstream, boundingBox, urn));
        flowNodes.push(...layoutNeighbours(aggregatedDomainEdges, LineageDirection.Downstream, boundingBox, urn));
    }

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
            type: EntityType.Domain,
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

function layoutNeighbours(
    edges: ReadonlyMap<string, AggregatedDomainEdge>,
    direction: LineageDirection,
    sourceBox: Node<LineageBoundingBox>,
    rootUrn: Urn,
): LineageVisualizationNode[] {
    const matching: AggregatedDomainEdge[] = [];
    edges.forEach((edge) => {
        if (edge.direction !== direction) return;
        if (edge.neighbourUrn === rootUrn) return;
        matching.push(edge);
    });

    // Stable ordering: by member-match count desc, then by URN — same tiebreak the resolver uses.
    matching.sort((a, b) => {
        if (b.memberMatchCount !== a.memberMatchCount) return b.memberMatchCount - a.memberMatchCount;
        return a.neighbourUrn.localeCompare(b.neighbourUrn);
    });

    const boxWidth = sourceBox.width ?? (sourceBox.style?.width as number) ?? LINEAGE_NODE_WIDTH;
    const boxHeight = sourceBox.height ?? (sourceBox.style?.height as number) ?? LINEAGE_NODE_HEIGHT;

    const totalSpan = matching.length * (LINEAGE_NODE_HEIGHT + NEIGHBOUR_VERTICAL_GAP) - NEIGHBOUR_VERTICAL_GAP;
    const startY = sourceBox.position.y + (boxHeight - totalSpan) / 2;

    const x =
        direction === LineageDirection.Upstream
            ? sourceBox.position.x - NEIGHBOUR_HORIZONTAL_GAP - LINEAGE_NODE_WIDTH
            : sourceBox.position.x + boxWidth + NEIGHBOUR_HORIZONTAL_GAP;

    return matching.map((edge, idx) => ({
        id: edge.neighbourUrn,
        type: LINEAGE_ENTITY_NODE_NAME,
        position: {
            x,
            y: startY + idx * (LINEAGE_NODE_HEIGHT + NEIGHBOUR_VERTICAL_GAP),
        },
        data: toNeighbourLineageEntity(edge),
        draggable: true,
        selectable: true,
    }));
}

function toNeighbourLineageEntity(edge: AggregatedDomainEdge): LineageEntity {
    const baseName = edge.neighbourName ?? edge.neighbourUrn;
    const displayName = `${baseName} (${edge.memberMatchCount} ${edge.memberMatchCount === 1 ? 'asset' : 'assets'})`;

    const fetchedEntity: FetchedEntityV2 = {
        urn: edge.neighbourUrn,
        type: edge.neighbourType,
        name: displayName,
        exists: true,
        genericEntityProperties: {
            type: edge.neighbourType,
            displayProperties: edge.neighbourColorHex ? { colorHex: edge.neighbourColorHex } : undefined,
            properties: { name: displayName },
        } as FetchedEntityV2['genericEntityProperties'],
    };

    return {
        id: edge.neighbourUrn,
        urn: edge.neighbourUrn,
        type: edge.neighbourType,
        entity: fetchedEntity,
        isExpanded: {
            [LineageDirection.Upstream]: false,
            [LineageDirection.Downstream]: false,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}
