import { TRANSFORMATION_NODE_SIZE } from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import {
    LINEAGE_FILTER_TYPE,
    LINEAGE_HANDLE_OFFSET,
    LineageEntity,
    LineageNode,
    NodeContext,
    getEdgeId,
    getParents,
    isTransformational,
    setDefault,
} from '@app/lineageV3/common';
import NodeBuilder, {
    MAIN_TO_MINI_X_SEP_RATIO,
    MAIN_X_SEP_RATIO,
    MAIN_Y_SEP_RATIO,
    MINI_X_SEP_RATIO,
    MINI_Y_SEP_RATIO,
} from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType, LineageDirection } from '@types';

export interface NodeSize {
    width: number;
    height: number;
    /** Y offset of the point edges visually attach to, from the top of the node. */
    anchorY?: number;
    /** Extra space reserved above the node, e.g. for a bounding box label rendered above it. */
    topMargin?: number;
}

interface ResolvedNodeSize extends Required<NodeSize> {
    isMini: boolean;
}

interface Options {
    /** Provides sizes for nodes with non-standard dimensions, e.g. bounding boxes.
     * A zero-area size marks a layout-only node: it keeps its neighbors connected for
     * positioning purposes but is exempt from overlap checks. */
    getNodeSize?: (node: LineageNode) => NodeSize | undefined;
    /** Assigns y positions to nodes with lower values first, letting them claim their desired
     * positions; later nodes are shifted around them. Defaults to topological placement order. */
    yPriority?: (node: LineageNode) => number;
    /** If true, nodes that would overlap an already-placed node are always shifted down, below it,
     * rather than to the nearest free position (which may be above). */
    resolveOverlapsDownward?: boolean;
}

interface PlacedNode {
    x: number;
    y: number;
    width: number;
    height: number;
    topMargin: number;
    isMini: boolean;
}

/**
 * Positions nodes in topological order rather than in strict layers, supporting dynamic node sizes.
 *
 * X: each node is placed one separation after its _furthest_ parent — the parent whose far edge is
 * furthest from the root — in contrast to NodeBuilder, which assigns layers by shortest path from
 * the root. Downstream nodes go right of the root and upstream nodes left. Nodes with the same
 * parents (or different parents whose far edges coincide) align at their left edge (downstream) or
 * right edge (upstream).
 *
 * Y: each node is placed so that its anchor (the point edges visually attach to) is as close as
 * possible to the average of its parents' anchors, shifted to the nearest free spot if it would
 * overlap a previously placed node with an intersecting horizontal extent. Overlaps must be
 * checked against actual extents, not layers, because dynamic widths mean columns can intersect.
 *
 * Nodes in cycles are placed after all acyclic nodes, positioned off their already-placed parents.
 *
 * Node and edge creation is inherited from NodeBuilder; only position computation is overridden.
 */
export default class TopologicalNodeBuilder extends NodeBuilder {
    #getNodeSize?: (node: LineageNode) => NodeSize | undefined;

    #yPriority?: (node: LineageNode) => number;

    #resolveOverlapsDownward: boolean;

    #sizes = new Map<string, ResolvedNodeSize>();

    #positionalParents = new Map<string, string[]>();

    #placementOrder: LineageNode[] = [];

    #xPositions = new Map<string, number>();

    constructor(
        rootUrn: string,
        rootType: EntityType,
        roots: LineageEntity[],
        nodes: LineageNode[],
        parents: Map<string, Set<string>>,
        options: Options = {},
    ) {
        super(rootUrn, rootType, roots, nodes, parents, true, false);
        this.#getNodeSize = options.getNodeSize;
        this.#yPriority = options.yPriority;
        this.#resolveOverlapsDownward = options.resolveOverlapsDownward ?? false;
    }

    computeNodeX({ adjacencyList, edges }: Pick<NodeContext, 'adjacencyList' | 'edges'>): void {
        this.topologicalNodes.forEach((node) => {
            this.#sizes.set(node.id, this.#resolveSize(node));
            const nodeParents = this.#resolveParents(node, adjacencyList, edges);
            this.#positionalParents.set(node.id, nodeParents);
            this.nodeInformation[node.id].positionalParents = new Set(nodeParents);
        });
        this.#placementOrder = this.#computePlacementOrder();
        this.#placementOrder.forEach((node) => {
            const x = this.#computeX(node);
            this.#xPositions.set(node.id, x);
            // Each node gets its own "layer", so that the inherited createNode reads its exact x position
            this.nodeInformation[node.id].layer = node.id;
            this.layerPositions.set(node.id, x);
        });
    }

    #resolveSize(node: LineageNode): ResolvedNodeSize {
        const override = this.#getNodeSize?.(node);
        const isMini = node.type !== LINEAGE_FILTER_TYPE && isTransformational(node, this.homeType);
        const width = override?.width ?? (isMini ? TRANSFORMATION_NODE_SIZE : this.nodeWidth);
        const height = override?.height ?? (isMini ? TRANSFORMATION_NODE_SIZE : this.nodeHeight);
        const anchorY = override?.anchorY ?? (isMini ? height / 2 : LINEAGE_HANDLE_OFFSET);
        const topMargin = override?.topMargin ?? 0;
        return { width, height, anchorY, topMargin, isMini };
    }

    /**
     * Resolves the parents used to position `node`: displayed adjacent nodes one step closer to the
     * root. Mirrors the filtering in NodeBuilder.computeNodeX: lineage filter nodes and query nodes
     * are exempt from the edge check because they aren't fully represented in the adjacency list.
     */
    #resolveParents(
        node: LineageNode,
        adjacencyList: NodeContext['adjacencyList'],
        edges: NodeContext['edges'],
    ): string[] {
        return getParents(node, adjacencyList).filter((parentId) => {
            const parent = this.nodeInformation[parentId];
            if (!parent) return false;
            if (node.type === LINEAGE_FILTER_TYPE) return true;
            if (node.type === EntityType.Query) return [node.direction, undefined].includes(parent.direction);
            return !!parent.urn && !!node.direction && edges.has(getEdgeId(parent.urn, node.id, node.direction));
        });
    }

    /**
     * Orders nodes so that each node appears after all of its parents (Kahn's algorithm), ensuring
     * parents are positioned before their children. Nodes in cycles never reach in-degree 0 and are
     * appended in their original BFS order, to be positioned off whichever parents are placed.
     */
    #computePlacementOrder(): LineageNode[] {
        const nodesById = new Map(this.topologicalNodes.map((node) => [node.id, node]));
        const childIds = new Map<string, string[]>();
        const inDegree = new Map<string, number>();
        this.topologicalNodes.forEach((node) => {
            const nodeParents = this.#positionalParents.get(node.id) || [];
            inDegree.set(node.id, nodeParents.length);
            nodeParents.forEach((parentId) => setDefault(childIds, parentId, []).push(node.id));
        });

        const queue = this.topologicalNodes.filter((node) => !inDegree.get(node.id)).map((node) => node.id);
        const order: LineageNode[] = [];
        const placed = new Set<string>();
        for (let i = 0; i < queue.length; i++) {
            const id = queue[i];
            const node = nodesById.get(id);
            if (node) order.push(node);
            placed.add(id);
            childIds.get(id)?.forEach((childId) => {
                const remaining = (inDegree.get(childId) || 0) - 1;
                inDegree.set(childId, remaining);
                if (remaining === 0) queue.push(childId);
            });
        }
        this.topologicalNodes.forEach((node) => {
            if (!placed.has(node.id)) order.push(node);
        });
        return order;
    }

    #computeX(node: LineageNode): number {
        const size = this.#sizes.get(node.id);
        if (!node.direction || !size) return 0;

        let parentIds = (this.#positionalParents.get(node.id) || []).filter((id) => this.#xPositions.has(id));
        if (!parentIds.length) {
            // No placed parent (e.g. first node of a cycle): fall back to placing adjacent to the root
            if (node.id === this.homeUrn || !this.#xPositions.has(this.homeUrn)) return 0;
            parentIds = [this.homeUrn];
        }

        if (node.direction === LineageDirection.Downstream) {
            return Math.max(
                ...parentIds.map((id) => {
                    const parentSize = this.#sizes.get(id);
                    return (
                        (this.#xPositions.get(id) ?? 0) + (parentSize?.width ?? 0) + this.#xSeparation(parentSize, size)
                    );
                }),
            );
        }
        return (
            Math.min(
                ...parentIds.map(
                    (id) => (this.#xPositions.get(id) ?? 0) - this.#xSeparation(this.#sizes.get(id), size),
                ),
            ) - size.width
        );
    }

    #xSeparation(a: ResolvedNodeSize | undefined, b: ResolvedNodeSize): number {
        if (a?.isMini && b.isMini) return this.nodeWidth * MINI_X_SEP_RATIO;
        if (a?.isMini || b.isMini) return this.nodeWidth * MAIN_TO_MINI_X_SEP_RATIO;
        return this.nodeWidth * MAIN_X_SEP_RATIO;
    }

    computeNodeY(): void {
        const yPriority = this.#yPriority;
        // Stable sort: equal-priority nodes keep their topological placement order,
        // so parents still receive y positions before their children within each priority group
        const orderedNodes = yPriority
            ? Array.from(this.#placementOrder).sort((a, b) => yPriority(a) - yPriority(b))
            : this.#placementOrder;

        const placedNodes: PlacedNode[] = [];
        orderedNodes.forEach((node) => {
            const size = this.#sizes.get(node.id);
            if (!size) return;
            const x = this.#xPositions.get(node.id) ?? 0;

            const parentAnchors = (this.#positionalParents.get(node.id) || [])
                .map((id) => {
                    const parentY = this.nodeInformation[id]?.y;
                    const parentSize = this.#sizes.get(id);
                    return parentY !== undefined && parentSize ? parentY + parentSize.anchorY : undefined;
                })
                .filter((anchor): anchor is number => anchor !== undefined);

            const desiredY = parentAnchors.length
                ? parentAnchors.reduce((a, b) => a + b) / parentAnchors.length - size.anchorY
                : 0;
            const isLayoutOnly = size.width <= 0 || size.height <= 0;
            const y = isLayoutOnly ? desiredY : this.#resolveOverlaps(desiredY, x, size, placedNodes);
            this.nodeInformation[node.id].y = y;
            if (!isLayoutOnly) {
                placedNodes.push({
                    x,
                    y,
                    width: size.width,
                    height: size.height,
                    topMargin: size.topMargin,
                    isMini: size.isMini,
                });
            }
        });

        this.filterNodes.forEach((node) => {
            const info = this.nodeInformation[node.id];
            if (info.y !== undefined) {
                info.y -= 15; // Manual offset until filter node positioning is improved
            }
        });
    }

    /**
     * Returns the y position closest to `desiredY` at which a node of the given size can be placed
     * without vertically overlapping (within the minimum separation) any already-placed node whose
     * horizontal extent intersects the new node's. Top margins extend a node's occupied extent
     * above its position, without moving the position itself.
     */
    #resolveOverlaps(desiredY: number, x: number, size: ResolvedNodeSize, placedNodes: PlacedNode[]): number {
        const forbidden = placedNodes
            .filter((placedNode) => placedNode.x < x + size.width && x < placedNode.x + placedNode.width)
            .map((placedNode): [number, number] => {
                const gap =
                    placedNode.isMini && size.isMini
                        ? this.separationNodeHeight * MINI_Y_SEP_RATIO
                        : this.separationNodeHeight * MAIN_Y_SEP_RATIO;
                return [
                    placedNode.y - placedNode.topMargin - gap - size.height,
                    placedNode.y + placedNode.height + gap + size.topMargin,
                ];
            })
            .sort((a, b) => a[0] - b[0]);

        const merged: [number, number][] = [];
        forbidden.forEach(([start, end]) => {
            const last = merged[merged.length - 1];
            if (last && start <= last[1]) {
                last[1] = Math.max(last[1], end);
            } else {
                merged.push([start, end]);
            }
        });

        const containing = merged.find(([start, end]) => desiredY > start && desiredY < end);
        if (!containing) return desiredY;
        const [start, end] = containing;
        if (this.#resolveOverlapsDownward) return end;
        return desiredY - start < end - desiredY ? start : end;
    }
}
