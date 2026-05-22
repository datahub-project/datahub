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
// Vertical clearance between stacked DP bboxes inside the Domain bbox. The next DP's card label
// floats above its bbox via translateY(-100%) (~54px); the gap needs to clear that plus a small
// breathing buffer so labels don't kiss the bbox below.
const NESTED_DP_VERTICAL_GAP = 70;
// Inner spacing between asset rows stacked inside a DP bbox.
const NESTED_ASSET_VERTICAL_GAP = 20;
// Width of the Domain (outer) bbox in pixels. Wide enough to host a DP (inner) bbox plus the
// Domain's own padding on either side.
const DOMAIN_BBOX_WIDTH = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 4;
// Width of each DP (inner) bbox. Centered horizontally inside the Domain bbox.
const DP_BBOX_WIDTH = LINEAGE_NODE_WIDTH + BOUNDING_BOX_PADDING * 2;

type DomainGraphContext = Pick<
    NodeContext,
    GraphStoreFields | LineageToggles | 'rootType' | 'aggregatedDomainEdges' | 'aggregatedInnerEdges'
>;

/**
 * Computes the lineage graph for a Domain.
 *
 * Layout:
 * 1. The source Domain renders as an outer bounding box at the origin. Inside it, each member
 *    DataProduct renders as its own inner bounding box containing the DP's asset members
 *    (datasets / ML models / data jobs) stacked vertically. Directly-tagged Domain assets that
 *    aren't part of any DP render as plain rows at the Domain level beneath the nested DP bboxes.
 * 2. Neighbour Domains (placed via {@link layoutNeighbours}) come from BFS over the union of
 *    `aggregatedDomainEdges` (initial root load + every multi-hop expansion the user has
 *    triggered). Each Domain is positioned at column = its hop depth and on the side it was
 *    first discovered from (upstream / downstream of source). The single source bbox is the
 *    only outer bbox; further Domain hops render as standalone entity nodes so the layout stays
 *    linear and predictable as the user drills out.
 * 3. {@link AggregatedLineageEdge}s connect every (source, neighbour) pair we know about — this
 *    is what gives the user "actual lineage between data domains" once a chain is expanded.
 * 4. Intra-Domain DP↔DP rollups from {@code aggregatedInnerEdges} render as edges inside the
 *    source bbox, between adjacent DP bboxes.
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
 * Per-Domain placement metadata derived by BFS from the source Domain.
 *
 * `side` is what we draw — once a Domain is first reached from the source via an upstream edge
 * we anchor it on the left; subsequent edges from the opposite direction don't move the node.
 * This avoids oscillation when a Domain is reachable both ways.
 *
 * `depth` is the BFS hop count from source (1 for direct neighbours, 2 for next hop, …) and
 * drives the horizontal column position.
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
            if (!node) return; // Defensive: ingest hook should have registered every neighbour.
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
 * BFS from `rootUrn` over the directed edges in `aggregatedDomainEdges`. Returns each reachable
 * Domain's discovered side (left/right of source) and BFS depth (hop count from source).
 *
 * Self-loops (edges back to `rootUrn`) are skipped so the source bbox stays the single anchor.
 * Cycles are handled naturally because we only assign placement on first visit.
 */
export function computeNeighbourPlacements(
    edges: ReadonlyMap<string, AggregatedDomainEdge>,
    rootUrn: Urn,
): Map<Urn, NeighbourPlacement> {
    // Build adjacency: from-source-perspective, "discovered via upstream edge" → side=Upstream.
    // A 2-hop neighbour reached from a 1-hop upstream is also placed upstream — we don't flip
    // sides mid-chain because the user's mental model is "this column is upstream of source".
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
            // The "side" carries through the chain: if I'm already on the upstream side, every
            // further hop from me stays upstream regardless of the edge direction taken to reach
            // the next node (the chain is "upstream of source").
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
    sides: ReadonlyMap<Urn, NeighbourPlacement>,
): Edge<AggregatedLineageEdgeData>[] {
    const flowEdges: Edge<AggregatedLineageEdgeData>[] = [];
    edges.forEach((edge) => {
        if (edge.neighbourUrn === rootUrn) return;
        // We can only draw an edge if both endpoints were placed in the layout. The source side
        // is always laid out (source bbox is fixed; expansion-source Domains are always already
        // placed via the BFS that produced them); the neighbour might still be missing if the
        // ingest hook hasn't registered it yet, in which case skip until the next render tick.
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
 * Lays out the Domain's interior: member DataProduct bboxes (each containing its asset members)
 * stacked vertically, followed by any directly-tagged Domain assets that aren't part of a DP.
 *
 * Returns the ReactFlow nodes in parent-then-children order (ReactFlow requires bbox parents to
 * precede their children), the total Y-extent of the laid-out interior (used to size the outer
 * Domain bbox), and the set of DP URNs we actually rendered (used to filter inner-edge
 * endpoints — see {@code buildInnerAggregatedEdges}).
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
        // Directly-tagged Domain assets render as a single column under the DP bboxes. Indent
        // them to the same X as the inner DP bboxes so the visual gridlines align.
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

    // Floor for an empty Domain so the outer bbox still has a sensible footprint.
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
        // Assets directly pinned to the source Domain stay at the Domain level (rendered by the
        // direct-asset path); don't double-render them inside their DP.
        if (node.parentDomain === rootUrn) return;
        const list = out.get(dpUrn);
        if (list) list.push(node);
        else out.set(dpUrn, [node]);
    });
    // Stable per-DP ordering by URN.
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
            // Nested DP bboxes stay grey to keep the Domain colour as the primary tint — matches
            // the existing rule in NodeContents.tsx that drops the DP tint when the DP is a
            // Domain member.
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
