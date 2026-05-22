import { Edge, Node } from 'reactflow';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    AGGREGATED_LINEAGE_EDGE_NAME,
    AggregatedLineageEdgeData,
} from '@app/lineageV3/LineageEdge/AggregatedLineageEdge';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import {
    AggregatedDomainEdge,
    AggregatedInnerEdge,
    FetchStatus,
    GraphStoreFields,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
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

type DomainGraphContext = Pick<
    NodeContext,
    GraphStoreFields | LineageToggles | 'rootType' | 'aggregatedDomainEdges' | 'aggregatedInnerEdges'
>;

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
 *    the right. Each neighbour's card shows its `memberMatchCount` as a subtitle ("N assets").
 * 3. One {@link AggregatedLineageEdge} is drawn between the source-Domain bounding box and
 *    each neighbour Domain node, with the asset-level rollup count rendered as the edge label
 *    and a tooltip exposing the full resolver bucket (memberMatchCount,
 *    neighbourEntityCount, degree min/max). Bounding boxes expose anchor handles via
 *    {@link LineageBoundingBoxNode} so ReactFlow can route these edges cleanly.
 * 4. Inner edges from {@code aggregatedInnerEdges} (server-computed DP↔DP rollups where both
 *    DPs belong to the source Domain) become {@link AggregatedLineageEdge}s drawn between the
 *    member DP nodes inside the source bbox. These edges only appear when assets across two
 *    member DPs have direct asset-level lineage — they're not a transitive rollup.
 */
export default function computeDomainGraph(urn: string, type: EntityType, context: DomainGraphContext) {
    const { nodes, aggregatedDomainEdges, aggregatedInnerEdges } = context;
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge[] = [];

    const memberFlowNodes = layoutMembers(nodes, urn);
    flowNodes.push(...memberFlowNodes);

    const boundingBox = addSourceBoundingBox(flowNodes, memberFlowNodes, nodes.get(urn));

    if (aggregatedDomainEdges && aggregatedDomainEdges.size > 0) {
        flowNodes.push(...layoutNeighbours(aggregatedDomainEdges, LineageDirection.Upstream, boundingBox, urn));
        flowNodes.push(...layoutNeighbours(aggregatedDomainEdges, LineageDirection.Downstream, boundingBox, urn));
        flowEdges.push(...buildAggregatedEdges(aggregatedDomainEdges, urn));
    }

    if (aggregatedInnerEdges && aggregatedInnerEdges.size > 0) {
        const memberDpUrns = new Set(memberFlowNodes.map((n) => n.id));
        flowEdges.push(...buildInnerAggregatedEdges(aggregatedInnerEdges, memberDpUrns));
    }

    return { flowNodes, flowEdges, resetPositions: false };
}

function buildInnerAggregatedEdges(
    innerEdges: ReadonlyMap<string, AggregatedInnerEdge>,
    memberDpUrns: Set<Urn>,
): Edge<AggregatedLineageEdgeData>[] {
    const out: Edge<AggregatedLineageEdgeData>[] = [];
    innerEdges.forEach((edge) => {
        // Only render edges whose endpoints are both member DPs we actually laid out in the
        // source bbox. Stray endpoints (e.g. a DP that's referenced by inner-edge data but
        // isn't in the current member set) have no node to anchor onto and would render as
        // dangling ReactFlow edges, so we skip them.
        if (!memberDpUrns.has(edge.upstreamUrn) || !memberDpUrns.has(edge.downstreamUrn)) {
            return;
        }
        out.push({
            id: `aggregated::inner::${edge.upstreamUrn}::${edge.downstreamUrn}`,
            source: edge.upstreamUrn,
            target: edge.downstreamUrn,
            type: AGGREGATED_LINEAGE_EDGE_NAME,
            data: {
                memberMatchCount: edge.memberMatchCount,
                degreeMin: edge.degreeMin,
                degreeMax: edge.degreeMax,
            },
        });
    });
    return out;
}

function buildAggregatedEdges(
    edges: ReadonlyMap<string, AggregatedDomainEdge>,
    rootUrn: Urn,
): Edge<AggregatedLineageEdgeData>[] {
    const flowEdges: Edge<AggregatedLineageEdgeData>[] = [];
    edges.forEach((edge) => {
        if (edge.neighbourUrn === rootUrn) return;
        const isUpstream = edge.direction === LineageDirection.Upstream;
        const source = isUpstream ? edge.neighbourUrn : rootUrn;
        const target = isUpstream ? rootUrn : edge.neighbourUrn;
        flowEdges.push({
            id: `aggregated::${source}::${target}`,
            source,
            target,
            type: AGGREGATED_LINEAGE_EDGE_NAME,
            data: {
                memberMatchCount: edge.memberMatchCount,
                neighbourEntityCount: edge.neighbourEntityCount,
                degreeMin: edge.degreeMin,
                degreeMax: edge.degreeMax,
            },
        });
    });
    return flowEdges;
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
        // Position is relative to the source-Domain bounding box (set as parent below).
        // ReactFlow expects parent nodes to precede their children in the flowNodes array —
        // the caller unshifts the bounding box once members are laid out.
        position: {
            x: BOUNDING_BOX_PADDING,
            y: BOUNDING_BOX_PADDING + idx * (LINEAGE_NODE_HEIGHT + MEMBER_VERTICAL_GAP),
        },
        data: member,
        parentId: rootUrn,
        extent: 'parent',
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
    const name = edge.neighbourName ?? edge.neighbourUrn;

    const fetchedEntity: FetchedEntityV2 = {
        urn: edge.neighbourUrn,
        type: edge.neighbourType,
        name,
        exists: true,
        genericEntityProperties: {
            type: edge.neighbourType,
            displayProperties: edge.neighbourColorHex ? { colorHex: edge.neighbourColorHex } : undefined,
            properties: { name },
        } as FetchedEntityV2['genericEntityProperties'],
    };

    return {
        id: edge.neighbourUrn,
        urn: edge.neighbourUrn,
        type: edge.neighbourType,
        entity: fetchedEntity,
        displaySubtitle: formatAssetCount(edge.memberMatchCount),
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

function formatAssetCount(count: number): string {
    return `${count} ${count === 1 ? 'asset' : 'assets'}`;
}
