import type { XYPosition } from '@reactflow/core/dist/esm/types';

import { BOUNDING_BOX_PADDING } from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    FetchStatus,
    LINEAGE_FILTER_TYPE,
    LineageEdge,
    LineageEntity,
    LineageNode,
    NeighborMap,
    NodeContext,
    addToAdjacencyList,
    createEdgeId,
    isTransformational,
    parseEdgeId,
    setDefault,
} from '@app/lineageV3/common';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import BoundingBoxNodeBuilder, {
    BoundingBoxSize,
} from '@app/lineageV3/useComputeGraph/dataProduct/BoundingBoxNodeBuilder';
import { BoxLayout, GraphStore } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.types';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

// Matches the default cardHeight in LineageBoundingBoxNode (rendered above the box via translateY(-100%)).
const BOUNDING_BOX_LABEL_HEIGHT = 54;

/**
 * Flag for data-product-to-data-product positioning.
 * - false: box-to-box lineage collapses through intermediate free nodes, so every box
 *   reachable from the home box — even only through free nodes — is positioned first, box-to-box.
 * - true: only DIRECT box-to-box lineage positions boxes first. A box reachable only through a free
 *   node is positioned like a normal node, as the topmost child of that free node.
 */
const POSITION_INDIRECT_BOXES_AS_FREE_CHILDREN = true;

/**
 * Positions bounding boxes and free nodes (displayed entities not in any data product) together,
 * via BoundingBoxNodeBuilder, in a single graph rooted at the home data product's bounding box:
 * bounding boxes are placed first from the data-product-to-data-product lineage, then free nodes
 * are arranged around them.
 * Lineage between two data products' members becomes an edge between their bounding boxes, and
 * lineage between a member and a free node becomes an edge between the box and the free node.
 * A bounding box is placed downstream (right) of the home box if its entities are downstream of the
 * home box's entities, and upstream (left) otherwise. If it is both, the side with more direct
 * edges wins, defaulting to downstream on a tie.
 *
 * Returns positions for all top-level nodes (used to place the bounding boxes), plus the
 * positioned flow nodes for the free nodes, created by the builder.
 */
export default function positionTopLevelNodes(
    rootUrn: Urn,
    boxes: Map<Urn, BoxLayout>,
    freeNodes: LineageNode[],
    displayedFreeIds: Set<string>,
    graphStore: GraphStore,
    displayedMembership: Map<Urn, Urn[]>,
    memberOffsets: Map<Urn, Map<Urn, number>>,
    ignoreSchemaFieldStatus: boolean,
): { positions: Map<string, XYPosition>; freeFlowNodes: LineageVisualizationNode[] } {
    // Lineage edges, with member endpoints mapped to their data products' bounding boxes.
    // Direct edge counts between the root box and each other box determine the side of boxes
    // whose members are both upstream and downstream of the root box's members.
    const edges: NodeContext['edges'] = new Map();
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    const rootEdgeCounts = new Map<Urn, Record<LineageDirection, number>>();
    const addEdge = (source: Urn, target: Urn, edge: LineageEdge) => {
        if (source === target) return;
        edges.set(createEdgeId(source, target), edge);
        addToAdjacencyList(adjacencyList, LineageDirection.Downstream, source, target);
        const defaultCounts = { [LineageDirection.Upstream]: 0, [LineageDirection.Downstream]: 0 };
        if (source === rootUrn) {
            setDefault(rootEdgeCounts, target, defaultCounts)[LineageDirection.Downstream] += 1;
        } else if (target === rootUrn) {
            setDefault(rootEdgeCounts, source, defaultCounts)[LineageDirection.Upstream] += 1;
        }
    };
    const endpointsFor = (urn: Urn): Urn[] => {
        const dataProducts = displayedMembership.get(urn)?.filter((dataProduct) => boxes.has(dataProduct));
        if (dataProducts?.length) return dataProducts;
        return displayedFreeIds.has(urn) ? [urn] : [];
    };
    graphStore.edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        // Route through displayed query nodes so they're placed between their neighbors
        const via = edge.via && displayedFreeIds.has(edge.via) ? edge.via : undefined;
        endpointsFor(upstream).forEach((source) => {
            endpointsFor(downstream).forEach((target) => {
                if (source === target) return;
                addEdge(source, target, edge);
                if (via) {
                    addEdge(source, via, edge);
                    addEdge(via, target, edge);
                }
            });
        });
    });

    const reachableDownstream = bfsReachable(rootUrn, adjacencyList[LineageDirection.Downstream]);
    const reachableUpstream = bfsReachable(rootUrn, adjacencyList[LineageDirection.Upstream]);
    const boxDirection = (boxUrn: Urn): LineageDirection | undefined => {
        if (boxUrn === rootUrn) return undefined;
        const isDownstream = reachableDownstream.has(boxUrn);
        const isUpstream = reachableUpstream.has(boxUrn);
        if (isDownstream && isUpstream) {
            const counts = rootEdgeCounts.get(boxUrn);
            return (counts?.[LineageDirection.Upstream] ?? 0) > (counts?.[LineageDirection.Downstream] ?? 0)
                ? LineageDirection.Upstream
                : LineageDirection.Downstream;
        }
        if (isDownstream) return LineageDirection.Downstream;
        if (isUpstream) return LineageDirection.Upstream;
        return undefined; // Disconnected boxes stack below the home box
    };

    const makeBoxNode = (boxUrn: Urn): LineageEntity => ({
        id: boxUrn,
        urn: boxUrn,
        type: EntityType.DataProduct,
        direction: boxDirection(boxUrn),
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    });

    // Boxes come before free nodes so they're placed first, prioritizing their desired positions
    const rootBoxNode = makeBoxNode(rootUrn);
    const layoutNodes: LineageNode[] = [rootBoxNode];
    boxes.forEach((box) => {
        if (box.group.urn !== rootUrn) layoutNodes.push(makeBoxNode(box.group.urn));
    });
    freeNodes.forEach((node) => {
        if (node.type === LINEAGE_FILTER_TYPE) {
            // Lineage filter nodes point at the entity they filter; remap members to their bounding box
            layoutNodes.push({ ...node, parent: endpointsFor(node.parent)[0] ?? node.parent });
        } else {
            layoutNodes.push(node);
        }
    });

    // Data product -> data product lineage, placing the bounding boxes before any free node
    const boxSizes = new Map<string, BoundingBoxSize>();
    boxes.forEach((box) =>
        boxSizes.set(box.group.urn, {
            width: box.width,
            height: box.height,
            topMargin: BOUNDING_BOX_LABEL_HEIGHT,
            contentPadding: BOUNDING_BOX_PADDING,
        }),
    );
    const boxAdjacency = computeBoxAdjacency(boxes, adjacencyList, !POSITION_INDIRECT_BOXES_AS_FREE_CHILDREN);
    const childBoxAnchors = computeChildBoxAnchors(
        boxes,
        graphStore,
        displayedMembership,
        memberOffsets,
        displayedFreeIds,
    );

    const builder = new BoundingBoxNodeBuilder(
        rootUrn,
        EntityType.DataProduct,
        [rootBoxNode],
        layoutNodes,
        new Map(),
        { sizes: boxSizes, adjacency: boxAdjacency },
        childBoxAnchors,
        POSITION_INDIRECT_BOXES_AS_FREE_CHILDREN,
    );
    const createdNodes = builder.createNodes({ adjacencyList, edges }, ignoreSchemaFieldStatus, new Map());
    const positions = new Map(createdNodes.map((node) => [node.id, node.position]));

    const freeNodesById = new Map(freeNodes.map((node) => [node.id, node]));
    const freeFlowNodes = createdNodes
        .filter((node) => displayedFreeIds.has(node.id))
        .map((node) => {
            const original = freeNodesById.get(node.id);
            if (original?.type !== LINEAGE_FILTER_TYPE) return node;
            // Restore the original parent, which was remapped to a bounding box for layout, and
            // count children displayed anywhere in the graph, free or within a data product
            const numShown = Array.from(original.allChildren).filter(
                (childUrn) => displayedFreeIds.has(childUrn) || displayedMembership.has(childUrn),
            ).length;
            return { ...node, data: { ...original, numShown } };
        });

    return { positions, freeFlowNodes };
}

/**
 * Computes lineage between bounding boxes, used by BoundingBoxNodeBuilder to position boxes before
 * anything else.
 * - When `collapseThroughFreeNodes` is true, box A is upstream of box B if A reaches B without
 *   passing through another box, collapsing any intermediate displayed free nodes.
 * - When false, only DIRECT box-to-box edges are recorded; a box reached only through a free node
 *   gets no box-lineage edge and is instead positioned as that free node's child by the builder.
 */
function computeBoxAdjacency(
    boxes: Map<Urn, BoxLayout>,
    adjacencyList: NodeContext['adjacencyList'],
    collapseThroughFreeNodes: boolean,
): Record<LineageDirection, NeighborMap> {
    const boxAdjacency: Record<LineageDirection, NeighborMap> = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    boxes.forEach((_box, boxUrn) => {
        if (!collapseThroughFreeNodes) {
            adjacencyList[LineageDirection.Downstream].get(boxUrn)?.forEach((neighbor) => {
                if (boxes.has(neighbor)) {
                    addToAdjacencyList(boxAdjacency, LineageDirection.Downstream, boxUrn, neighbor);
                }
            });
            return;
        }
        const visited = new Set<Urn>([boxUrn]);
        const queue = [boxUrn];
        for (let i = 0; i < queue.length; i += 1) {
            adjacencyList[LineageDirection.Downstream].get(queue[i])?.forEach((neighbor) => {
                if (visited.has(neighbor)) return;
                visited.add(neighbor);
                if (boxes.has(neighbor)) {
                    addToAdjacencyList(boxAdjacency, LineageDirection.Downstream, boxUrn, neighbor);
                } else {
                    queue.push(neighbor);
                }
            });
        }
    });
    return boxAdjacency;
}

/**
 * For each free node, the row offset within a bounding box at which it should be anchored, so a
 * data product's external neighbors line up with the member they connect to rather than the box's
 * top. Keyed by free node id, then box urn; the average is used when a node links to several members.
 *
 * From each member, its row offset is propagated outward through transformational nodes (queries,
 * dbt models) — which the layout collapses through — and applied to the first regular free node on
 * each branch. This mirrors how the layout resolves a free node's parent through those chains.
 */
function computeChildBoxAnchors(
    boxes: Map<Urn, BoxLayout>,
    graphStore: GraphStore,
    displayedMembership: Map<Urn, Urn[]>,
    memberOffsets: Map<Urn, Map<Urn, number>>,
    displayedFreeIds: Set<string>,
): Map<string, Map<Urn, number>> {
    const isMember = (urn: Urn) => (displayedMembership.get(urn) || []).some((boxUrn) => boxes.has(boxUrn));
    const isTransformationalUrn = (urn: Urn) => {
        const node = graphStore.nodes.get(urn);
        return !!node && isTransformational(node, graphStore.rootType);
    };
    const neighbors = (urn: Urn) => [
        ...(graphStore.adjacencyList[LineageDirection.Upstream].get(urn) || []),
        ...(graphStore.adjacencyList[LineageDirection.Downstream].get(urn) || []),
    ];

    const offsetsByChildBox = new Map<string, Map<Urn, number[]>>();
    memberOffsets.forEach((offsetByMember, boxUrn) => {
        offsetByMember.forEach((offset, memberUrn) => {
            // Walk outward from the member through transformational nodes, anchoring free nodes reached.
            const visited = new Set<Urn>([memberUrn]);
            const queue: Urn[] = [];
            const enqueue = (urn: Urn) => {
                if (!visited.has(urn)) {
                    visited.add(urn);
                    queue.push(urn);
                }
            };
            neighbors(memberUrn).forEach(enqueue);
            for (let i = 0; i < queue.length; i += 1) {
                const urn = queue[i];
                // Skip members: don't anchor to them or cross into another data product
                if (!isMember(urn)) {
                    if (displayedFreeIds.has(urn)) {
                        setDefault(setDefault(offsetsByChildBox, urn, new Map()), boxUrn, []).push(offset);
                    }
                    if (isTransformationalUrn(urn)) neighbors(urn).forEach(enqueue);
                }
            }
        });
    });

    const anchors = new Map<string, Map<Urn, number>>();
    offsetsByChildBox.forEach((byBox, childId) => {
        const boxAnchors = new Map<Urn, number>();
        byBox.forEach((offsets, boxUrn) =>
            boxAnchors.set(boxUrn, offsets.reduce((sum, y) => sum + y, 0) / offsets.length),
        );
        anchors.set(childId, boxAnchors);
    });
    return anchors;
}

function bfsReachable(start: Urn, neighbors: Map<Urn, Set<Urn>>): Set<Urn> {
    const visited = new Set<Urn>([start]);
    const queue = [start];
    for (let i = 0; i < queue.length; i += 1) {
        neighbors.get(queue[i])?.forEach((neighbor) => {
            if (!visited.has(neighbor)) {
                visited.add(neighbor);
                queue.push(neighbor);
            }
        });
    }
    visited.delete(start);
    return visited;
}
