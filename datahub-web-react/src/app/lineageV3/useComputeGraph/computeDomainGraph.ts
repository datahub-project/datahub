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
    GraphStoreFields,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
    LineageToggles,
    NodeContext,
} from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

const MEMBER_VERTICAL_GAP = 30;
const NEIGHBOUR_VERTICAL_GAP = 30;
const NEIGHBOUR_HORIZONTAL_GAP = 240;
// DP card labels float above their bbox via translateY(-100%) (~54px); the gap clears the label
// plus breathing room.
const NESTED_DP_VERTICAL_GAP = 70;
const NESTED_ASSET_VERTICAL_GAP = 20;
const DOMAIN_BBOX_WIDTH = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 4;
const DP_BBOX_WIDTH = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 2;

type DomainGraphContext = Pick<
    NodeContext,
    GraphStoreFields | LineageToggles | 'rootType' | 'aggregatedDomainEdges' | 'aggregatedInnerEdges'
>;

/**
 * Layout for the Domain lineage view:
 * - Source Domain renders as an outer bbox; each member DP renders as a nested bbox containing
 *   its asset rows. Directly-tagged Domain assets (no DP) stack below the DP bboxes.
 * - Neighbour Domains are placed by BFS depth/side (see {@link layoutNeighbours}); deeper hops
 *   render as plain entity nodes rather than nested bboxes to keep the layout linear.
 * - Edges: {@link AggregatedLineageEdge}s connect source↔neighbour Domains; DP↔DP inner edges
 *   from {@code aggregatedInnerEdges} render between adjacent DP bboxes.
 */
export default function computeDomainGraph(urn: string, type: EntityType, context: DomainGraphContext) {
    const { nodes, aggregatedDomainEdges, aggregatedInnerEdges } = context;
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge[] = [];

    const { memberFlowNodes, memberAreaHeight, memberDpUrns } = layoutNestedMembers(nodes, urn);
    flowNodes.push(...memberFlowNodes);

    const boundingBox = addSourceBoundingBox(flowNodes, memberAreaHeight, nodes.get(urn));

    if (aggregatedDomainEdges && aggregatedDomainEdges.size > 0) {
        const { neighbourNodes, sides } = layoutNeighbours(aggregatedDomainEdges, nodes, urn, boundingBox);
        flowNodes.push(...neighbourNodes);
        flowEdges.push(...buildAggregatedEdges(aggregatedDomainEdges, urn, sides));
    }

    if (aggregatedInnerEdges && aggregatedInnerEdges.size > 0) {
        flowEdges.push(...buildInnerAggregatedEdges(aggregatedInnerEdges, memberDpUrns));
    }

    return { flowNodes, flowEdges, resetPositions: false };
}

/**
 * BFS placement metadata for a neighbour Domain. `side` is sticky (first discovery wins, so
 * Domains reachable both ways don't oscillate); `depth` drives the horizontal column position.
 */
export type NeighbourPlacement = {
    side: LineageDirection;
    depth: number;
};

function layoutNeighbours(
    edges: ReadonlyMap<string, AggregatedDomainEdge>,
    nodes: NodeContext['nodes'],
    rootUrn: Urn,
    sourceBox: Node<LineageBoundingBox>,
): { neighbourNodes: LineageVisualizationNode[]; sides: ReadonlyMap<Urn, NeighbourPlacement> } {
    const placements = computeNeighbourPlacements(edges, rootUrn);
    if (placements.size === 0) {
        return { neighbourNodes: [], sides: placements };
    }

    // Group Domains by (side, depth) so we can stack them vertically inside each column.
    const columns = new Map<string, Urn[]>();
    placements.forEach((placement, neighbourUrn) => {
        const key = `${placement.side}::${placement.depth}`;
        const arr = columns.get(key);
        if (arr) arr.push(neighbourUrn);
        else columns.set(key, [neighbourUrn]);
    });

    const boxWidth = sourceBox.width ?? (sourceBox.style?.width as number) ?? LINEAGE_NODE_WIDTH;
    const boxHeight = sourceBox.height ?? (sourceBox.style?.height as number) ?? LINEAGE_NODE_HEIGHT;
    const sourceCenterY = sourceBox.position.y + boxHeight / 2;

    const result: LineageVisualizationNode[] = [];
    columns.forEach((urnsInColumn, key) => {
        // Stable per-column ordering: by memberMatchCount desc (largest first), then URN tiebreak.
        const sorted = [...urnsInColumn].sort((a, b) => {
            const ma = bestMemberMatchCount(edges, a);
            const mb = bestMemberMatchCount(edges, b);
            if (mb !== ma) return mb - ma;
            return a.localeCompare(b);
        });

        const [sideStr, depthStr] = key.split('::');
        const direction = sideStr as LineageDirection;
        const depth = Number(depthStr);
        const totalSpan = sorted.length * (LINEAGE_NODE_HEIGHT + NEIGHBOUR_VERTICAL_GAP) - NEIGHBOUR_VERTICAL_GAP;
        const startY = sourceCenterY - totalSpan / 2;

        const xOffset = depth * (LINEAGE_NODE_WIDTH + NEIGHBOUR_HORIZONTAL_GAP);
        const x =
            direction === LineageDirection.Upstream
                ? sourceBox.position.x - xOffset - LINEAGE_NODE_WIDTH + NEIGHBOUR_HORIZONTAL_GAP
                : sourceBox.position.x + boxWidth + xOffset - NEIGHBOUR_HORIZONTAL_GAP;

        sorted.forEach((neighbourUrn, idx) => {
            const node = nodes.get(neighbourUrn);
            if (!node) return;
            result.push({
                id: neighbourUrn,
                type: LINEAGE_ENTITY_NODE_NAME,
                position: {
                    x,
                    y: startY + idx * (LINEAGE_NODE_HEIGHT + NEIGHBOUR_VERTICAL_GAP),
                },
                data: node,
                draggable: true,
                selectable: true,
            });
        });
    });
    return { neighbourNodes: result, sides: placements };
}

/**
 * BFS from `rootUrn` returning each reachable Domain's side (relative to source) and hop depth.
 * Side is carried through the chain — the user's mental model is "this column is upstream of
 * source", so once we're on the upstream side every further hop stays upstream regardless of
 * the edge direction used to reach the next node.
 */
export function computeNeighbourPlacements(
    edges: ReadonlyMap<string, AggregatedDomainEdge>,
    rootUrn: Urn,
): Map<Urn, NeighbourPlacement> {
    const outgoing = new Map<Urn, Array<{ to: Urn; via: LineageDirection }>>();
    edges.forEach((edge) => {
        if (edge.neighbourUrn === rootUrn) return;
        const fromList = outgoing.get(edge.sourceUrn);
        const entry = { to: edge.neighbourUrn, via: edge.direction };
        if (fromList) fromList.push(entry);
        else outgoing.set(edge.sourceUrn, [entry]);
    });

    const placements = new Map<Urn, NeighbourPlacement>();
    type QueueEntry = { urn: Urn; side: LineageDirection | null; depth: number };
    const queue: QueueEntry[] = [{ urn: rootUrn, side: null, depth: 0 }];
    const visited = new Set<Urn>([rootUrn]);

    while (queue.length > 0) {
        const head = queue.shift();
        if (!head) break;
        const neighbours = outgoing.get(head.urn) ?? [];
        neighbours.forEach((next) => {
            if (visited.has(next.to)) return;
            visited.add(next.to);
            const side = head.side ?? next.via;
            placements.set(next.to, { side, depth: head.depth + 1 });
            queue.push({ urn: next.to, side, depth: head.depth + 1 });
        });
    }
    return placements;
}

function bestMemberMatchCount(edges: ReadonlyMap<string, AggregatedDomainEdge>, urn: Urn): number {
    let best = 0;
    edges.forEach((edge) => {
        if (edge.neighbourUrn === urn && edge.memberMatchCount > best) {
            best = edge.memberMatchCount;
        }
    });
    return best;
}

function buildInnerAggregatedEdges(
    innerEdges: ReadonlyMap<string, AggregatedInnerEdge>,
    memberDpUrns: Set<Urn>,
): Edge<AggregatedLineageEdgeData>[] {
    const out: Edge<AggregatedLineageEdgeData>[] = [];
    innerEdges.forEach((edge) => {
        // Skip edges with endpoints we didn't lay out — they'd render as dangling ReactFlow edges.
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
    sides: ReadonlyMap<Urn, NeighbourPlacement>,
): Edge<AggregatedLineageEdgeData>[] {
    const flowEdges: Edge<AggregatedLineageEdgeData>[] = [];
    edges.forEach((edge) => {
        if (edge.neighbourUrn === rootUrn) return;
        // Skip until both endpoints have been placed — the neighbour may not be registered yet.
        if (edge.sourceUrn !== rootUrn && !sides.has(edge.sourceUrn)) return;
        if (!sides.has(edge.neighbourUrn)) return;

        const isUpstream = edge.direction === LineageDirection.Upstream;
        const source = isUpstream ? edge.neighbourUrn : edge.sourceUrn;
        const target = isUpstream ? edge.sourceUrn : edge.neighbourUrn;
        flowEdges.push({
            id: `aggregated::${edge.sourceUrn}::${edge.neighbourUrn}::${edge.direction}`,
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

/**
 * Lays out the Domain interior: member DP bboxes (with their assets) stacked vertically, then
 * any directly-tagged Domain assets beneath. Returns nodes in parent-then-children order (a
 * ReactFlow requirement for nested bboxes), the total interior Y-extent (used to size the
 * outer Domain bbox), and the DP URN set used to filter inner-edge endpoints.
 */
function layoutNestedMembers(
    nodes: NodeContext['nodes'],
    rootUrn: Urn,
): { memberFlowNodes: LineageVisualizationNode[]; memberAreaHeight: number; memberDpUrns: Set<Urn> } {
    const memberDps: LineageEntity[] = [];
    const memberDirectAssets: LineageEntity[] = [];
    nodes.forEach((node) => {
        if (node.parentDomain !== rootUrn || node.urn === rootUrn) return;
        if (node.type === EntityType.DataProduct) memberDps.push(node);
        else memberDirectAssets.push(node);
    });
    // Stable ordering: by URN. Avoids node-reshuffle on each re-render when nodes Map iteration
    // order is unstable across versions.
    // Stable URN-based ordering — Map iteration order isn't guaranteed across re-renders.
    memberDps.sort((a, b) => a.urn.localeCompare(b.urn));
    memberDirectAssets.sort((a, b) => a.urn.localeCompare(b.urn));

    const memberDpUrns = new Set<Urn>(memberDps.map((dp) => dp.urn));
    const assetsByDp = collectAssetsByDp(nodes, memberDpUrns, rootUrn);

    const flowNodes: LineageVisualizationNode[] = [];
    let cursorY = BOUNDING_BOX_PADDING;

    memberDps.forEach((dp) => {
        const assets = assetsByDp.get(dp.urn) ?? [];
        const dpHeight = nestedDpHeight(assets.length);
        const dpX = (DOMAIN_BBOX_WIDTH - DP_BBOX_WIDTH) / 2;
        flowNodes.push(makeDpBox(dp, rootUrn, dpX, cursorY, dpHeight));
        assets.forEach((asset, idx) => {
            flowNodes.push(makeNestedAsset(asset, dp.urn, idx));
        });
        cursorY += dpHeight + NESTED_DP_VERTICAL_GAP;
    });

    if (memberDirectAssets.length > 0) {
        const assetX = (DOMAIN_BBOX_WIDTH - LINEAGE_NODE_WIDTH) / 2;
        memberDirectAssets.forEach((asset, idx) => {
            flowNodes.push({
                id: asset.urn,
                type: LINEAGE_ENTITY_NODE_NAME,
                position: { x: assetX, y: cursorY + idx * (LINEAGE_NODE_HEIGHT + MEMBER_VERTICAL_GAP) },
                data: asset,
                parentId: rootUrn,
                extent: 'parent',
                draggable: true,
                selectable: true,
            });
        });
        cursorY +=
            memberDirectAssets.length * LINEAGE_NODE_HEIGHT +
            (memberDirectAssets.length - 1) * MEMBER_VERTICAL_GAP +
            NESTED_DP_VERTICAL_GAP;
    }

    const memberAreaHeight = Math.max(cursorY, LINEAGE_NODE_HEIGHT + BOUNDING_BOX_PADDING);
    return { memberFlowNodes: flowNodes, memberAreaHeight, memberDpUrns };
}

function collectAssetsByDp(
    nodes: NodeContext['nodes'],
    memberDpUrns: Set<Urn>,
    rootUrn: Urn,
): Map<Urn, LineageEntity[]> {
    const out = new Map<Urn, LineageEntity[]>();
    nodes.forEach((node) => {
        const dpUrn = node.parentDataProduct;
        if (!dpUrn || !memberDpUrns.has(dpUrn)) return;
        // Source-Domain pinning wins: don't double-render an asset both at the Domain level and
        // inside its DP.
        if (node.parentDomain === rootUrn) return;
        const list = out.get(dpUrn);
        if (list) list.push(node);
        else out.set(dpUrn, [node]);
    });
    out.forEach((list) => list.sort((a, b) => a.urn.localeCompare(b.urn)));
    return out;
}

function nestedDpHeight(assetCount: number): number {
    const rows = Math.max(assetCount, 1);
    return BOUNDING_BOX_PADDING * 2 + rows * LINEAGE_NODE_HEIGHT + (rows - 1) * NESTED_ASSET_VERTICAL_GAP;
}

function makeDpBox(dp: LineageEntity, parentUrn: Urn, x: number, y: number, height: number): Node<LineageBoundingBox> {
    return {
        id: dp.urn,
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x, y },
        data: {
            urn: dp.urn,
            type: EntityType.DataProduct,
            entity: dp.entity,
            // Nested DPs stay grey so the Domain colour stays the primary tint.
            colorHex: undefined,
            subtitle: dp.displaySubtitle,
        },
        parentId: parentUrn,
        extent: 'parent',
        selectable: true,
        draggable: true,
        style: { width: DP_BBOX_WIDTH, height, zIndex: -1 },
        width: DP_BBOX_WIDTH,
        height,
    };
}

function makeNestedAsset(asset: LineageEntity, dpUrn: Urn, idx: number): LineageVisualizationNode {
    return {
        id: asset.urn,
        type: LINEAGE_ENTITY_NODE_NAME,
        position: {
            x: BOUNDING_BOX_PADDING,
            y: BOUNDING_BOX_PADDING + idx * (LINEAGE_NODE_HEIGHT + NESTED_ASSET_VERTICAL_GAP),
        },
        data: asset,
        parentId: dpUrn,
        extent: 'parent',
        draggable: true,
        selectable: true,
    };
}

function addSourceBoundingBox(
    flowNodes: LineageVisualizationNode[],
    memberAreaHeight: number,
    rootNode: LineageEntity | undefined,
): Node<LineageBoundingBox> {
    const height = memberAreaHeight + BOUNDING_BOX_PADDING;
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
        style: { width: DOMAIN_BBOX_WIDTH, height, zIndex: -2 },
        width: DOMAIN_BBOX_WIDTH,
        height,
    };

    flowNodes.unshift(box);
    return box;
}
