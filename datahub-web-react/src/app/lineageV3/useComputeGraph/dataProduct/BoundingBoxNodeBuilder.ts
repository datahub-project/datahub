import { TRANSFORMATION_NODE_SIZE } from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import {
    LINEAGE_FILTER_TYPE,
    LINEAGE_HANDLE_OFFSET,
    LineageEntity,
    LineageNode,
    NeighborMap,
    NodeContext,
    getEdgeId,
    getParents,
    isTransformational,
    reverseDirection,
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

export interface BoundingBoxSize {
    width: number;
    height: number;
    /** Extra space reserved above the box, e.g. for its label rendered above it. */
    topMargin?: number;
    /** Padding between the box's border and its contents. Boxes are offset by this, so that their
     * contents, not their border, align with the rows and columns of the layout. */
    contentPadding?: number;
}

export interface BoundingBoxLineage {
    /** Sizes of each bounding box, keyed by its node id. */
    sizes: Map<string, BoundingBoxSize>;
    /** Direct lineage between bounding boxes, with intermediate non-box nodes collapsed. */
    adjacency: Record<LineageDirection, NeighborMap>;
}

/** How a node is rendered, which sets its size and how much room it wants from its neighbors:
 * `box` — a bounding box (large container), `main` — a standard entity card, `mini` — a
 * transformational node (a small circle). */
type NodeKind = 'box' | 'main' | 'mini';

/** Precedence when a layer holds several kinds: it takes on the heaviest, which drives its spacing. */
const KIND_RANK: Record<NodeKind, number> = { mini: 0, main: 1, box: 2 };

interface ResolvedSize {
    width: number;
    height: number;
    /** Extra space reserved above the node, e.g. for a bounding box label rendered above it. */
    topMargin: number;
    /** Offset between the node's border and its content anchor; nonzero only for boxes. */
    contentPadding: number;
    kind: NodeKind;
}

/** A node's occupied extent within its layer. Mutated in place when the node is pushed down. */
interface PlacedRect {
    id: string;
    y: number;
    height: number;
    topMargin: number;
    kind: NodeKind;
}

const MAX_RESOLUTION_STEPS = 1000;
const TRANSFORMATIONAL_LEAF_OFFSET = 25;
/**
 * Horizontal space reserved on each side of a transformational node when widening a column gap to
 * fit a transformational chain. Increase to spread chains, and the columns around them, further apart.
 */
export const TRANSFORMATION_NODE_X_SPACING = TRANSFORMATION_NODE_SIZE * 1.25;

/** Extra room bounding boxes keep from their neighbors, wider than a card-to-card gap since a box is
 * a large labeled container. */
const BOX_X_SEP_RATIO = 0.75;

/**
 * Horizontal separation between two adjacent layers, as a fraction of nodeWidth, by the kinds of
 * nodes on each side. A bounding box keeps generous room from anything; two transformation circles
 * pack tightest; a circle sits half-tight against a card.
 */
function layerSeparationRatio(a: NodeKind, b: NodeKind): number {
    if (a === 'box' || b === 'box') return BOX_X_SEP_RATIO;
    if (a === 'mini' && b === 'mini') return MINI_X_SEP_RATIO;
    if (a === 'mini' || b === 'mini') return MAIN_TO_MINI_X_SEP_RATIO;
    return MAIN_X_SEP_RATIO;
}

/**
 * Positions dynamically-sized bounding boxes and fixed-size nodes together, in vertically-aligned
 * layers. Built for the data product graph, where each data product is a bounding box and the
 * entities around them are regular nodes.
 *
 * Transformational nodes (queries, dbt models, data jobs) are excluded from the layered layout
 * entirely: regular nodes are positioned by their nearest regular (or box) ancestors, collapsing
 * through transformational chains, so a parent's children keep their display order whether or not
 * they are reached through a transformation. Transformational nodes are placed afterward, on the
 * edges they belong to: horizontally within the gap between their neighbors' columns (spread out
 * by their depth within a transformational chain) and vertically at the average of their placed
 * neighbors' edge anchors.
 *
 * Layers: each regular node's layer is one past its furthest collapsed parent (max parent layer
 * + 1), negated for upstream nodes. Each layer's width is the maximum width of the nodes it
 * contains: bounding boxes have caller-provided sizes and everything else is a standard
 * `nodeWidth` x `nodeHeight` card. Downstream nodes left-align within their layer, upstream nodes
 * right-align.
 *
 * Placement: bounding boxes are placed first, from the box-to-box lineage provided via
 * `boundingBoxes` — the home box anchors at y=0 and each box's first child top-aligns with it,
 * with subsequent children stacked below. Regular nodes are then placed by the same rule,
 * processing layers outward from the home box and, within each layer, parents from top to bottom.
 * A parent's first child that would land on a bounding box moves below it; a second-or-later child
 * instead pushes the box down (keeping the sibling stack contiguous), cascading the push to
 * anything the box then overlaps. Already-placed regular nodes are never displaced except by such
 * cascades: later placements always resolve downward past them.
 *
 * Node and edge creation is inherited from NodeBuilder; only position computation is overridden.
 */
export default class BoundingBoxNodeBuilder extends NodeBuilder {
    #boundingBoxes: BoundingBoxLineage;

    /** Row offset within a bounding box at which to anchor a free node, keyed by node id then box id. */
    #childBoxAnchors: Map<string, Map<string, number>>;

    /** When true, boxes reachable only through free nodes are laid out as those free nodes' children
     * rather than positioned first via box-to-box lineage. */
    #positionIndirectBoxesAsFree: boolean;

    /** Boxes positioned first, via box-to-box lineage (vs. laid out as free-node children). */
    #boxesPlacedFirst = new Set<string>();

    #sizes = new Map<string, ResolvedSize>();

    /** Signed layer per regular node id: 0 at the home box, positive downstream, negative upstream. */
    #layers = new Map<string, number>();

    #layerX = new Map<number, number>();

    #layerWidths = new Map<number, number>();

    /** The heaviest node kind in each layer, driving how much it separates from its neighbors. */
    #layerKinds = new Map<number, NodeKind>();

    /** Direct positional parents per node, including transformational nodes. */
    #directParents = new Map<string, string[]>();

    /** Direct positional children per node, derived from #directParents. */
    #directChildren = new Map<string, string[]>();

    /** Ids of displayed transformational nodes, which are excluded from the layered layout. */
    #transformationalIds = new Set<string>();

    /** Nearest regular (or box) ancestors of each transformational node. */
    #regularAncestors = new Map<string, string[]>();

    /** Positional parents per regular node, with transformational chains collapsed out. */
    #positionalParents = new Map<string, string[]>();

    #childrenByParent = new Map<string, string[]>();

    /** Per transformational node: which column gap it sits in and where along it. */
    #transformationalGapInfo = new Map<string, { nearLayer: number; farLayer: number; fraction: number }>();

    /** Longest transformational chain crossing each column gap, keyed by the gap's far layer. */
    #gapChainLengths = new Map<number, number>();

    /** Regular non-box nodes in topological order, so parents are placed before their children. */
    #freeNodeOrder: LineageNode[] = [];

    /** Occupied extents of placed nodes, by layer. Overlaps can only occur within a layer. */
    #layerRects = new Map<number, PlacedRect[]>();

    constructor(
        rootUrn: string,
        rootType: EntityType,
        roots: LineageEntity[],
        nodes: LineageNode[],
        parents: Map<string, Set<string>>,
        boundingBoxes: BoundingBoxLineage,
        childBoxAnchors: Map<string, Map<string, number>> = new Map(),
        positionIndirectBoxesAsFree = false,
    ) {
        super(rootUrn, rootType, roots, nodes, parents, true, false);
        this.#boundingBoxes = boundingBoxes;
        this.#childBoxAnchors = childBoxAnchors;
        this.#positionIndirectBoxesAsFree = positionIndirectBoxesAsFree;
    }

    #isBox(id: string): boolean {
        return this.#boundingBoxes.sizes.has(id);
    }

    /** Whether a box is positioned first (via box-to-box lineage). A non-placed-first box (reachable
     * only through free nodes, when the experiment is on) is laid out like a regular node instead. */
    #isBoxPlacedFirst(id: string): boolean {
        return this.#boxesPlacedFirst.has(id);
    }

    /** A box laid out like a regular node — the topmost child of the free node that reaches it. */
    #isFloatingBox(id: string): boolean {
        return this.#isBox(id) && !this.#isBoxPlacedFirst(id);
    }

    /**
     * Boxes positioned first. With the experiment off, that's every box (unchanged). With it on,
     * only boxes reachable from the home box through DIRECT box-to-box lineage; the rest float and
     * are laid out as free-node children.
     */
    #computeBoxesPlacedFirst(): Set<string> {
        const boxIds = this.topologicalNodes.filter((node) => this.#isBox(node.id)).map((node) => node.id);
        if (!this.#positionIndirectBoxesAsFree) return new Set(boxIds);

        const placed = new Set<string>([this.homeUrn]);
        const queue = [this.homeUrn];
        for (let i = 0; i < queue.length; i += 1) {
            [LineageDirection.Upstream, LineageDirection.Downstream].forEach((direction) => {
                this.#boundingBoxes.adjacency[direction].get(queue[i])?.forEach((neighbor) => {
                    if (this.#isBox(neighbor) && !placed.has(neighbor)) {
                        placed.add(neighbor);
                        queue.push(neighbor);
                    }
                });
            });
        }
        return placed;
    }

    #isTransformationalNode(node: LineageNode): boolean {
        return node.type !== LINEAGE_FILTER_TYPE && isTransformational(node, this.homeType);
    }

    computeNodeX({ adjacencyList, edges }: Pick<NodeContext, 'adjacencyList' | 'edges'>): void {
        this.#boxesPlacedFirst = this.#computeBoxesPlacedFirst();
        this.topologicalNodes.forEach((node) => {
            this.#sizes.set(node.id, this.#resolveSize(node));
            // Placed-first boxes are positioned via box-to-box lineage, not the layered layout;
            // floating boxes fall through and are treated like regular nodes.
            if (this.#isBoxPlacedFirst(node.id)) return;
            if (this.#isTransformationalNode(node)) this.#transformationalIds.add(node.id);
            const nodeParents = this.#resolveParents(node, adjacencyList, edges);
            this.#directParents.set(node.id, nodeParents);
            nodeParents.forEach((parentId) => setDefault(this.#directChildren, parentId, []).push(node.id));
        });

        // Collapse transformational nodes out of the positional graph: regular nodes are
        // positioned by their nearest regular (or box) ancestors, so a parent's children keep
        // their display order whether or not they are reached through a transformation
        this.topologicalNodes.forEach((node) => {
            if (this.#isBoxPlacedFirst(node.id) || this.#transformationalIds.has(node.id)) return;
            const collapsed = this.#collapseParents(node.id);
            this.#positionalParents.set(node.id, collapsed);
            this.nodeInformation[node.id].positionalParents = new Set(collapsed);
            collapsed.forEach((parentId) => setDefault(this.#childrenByParent, parentId, []).push(node.id));
        });

        this.#assignBoxLayers();
        this.#freeNodeOrder = this.#computeFreeNodeOrder();
        this.#assignFreeNodeLayers();
        this.#computeTransformationalGapInfo();
        this.#computeXPositions();
        this.#computeTransformationalXPositions();
    }

    #resolveSize(node: LineageNode): ResolvedSize {
        const boxSize = this.#boundingBoxes.sizes.get(node.id);
        if (boxSize) {
            return {
                width: boxSize.width,
                height: boxSize.height,
                topMargin: boxSize.topMargin ?? 0,
                contentPadding: boxSize.contentPadding ?? 0,
                kind: 'box',
            };
        }
        // Transformational nodes render as small circles, not full cards; measuring them at card
        // width would reserve ~280px of phantom space in their layer (e.g. a transformational root).
        if (this.#isTransformationalNode(node)) {
            return {
                width: TRANSFORMATION_NODE_SIZE,
                height: TRANSFORMATION_NODE_SIZE,
                topMargin: 0,
                contentPadding: 0,
                kind: 'mini',
            };
        }
        return { width: this.nodeWidth, height: this.nodeHeight, topMargin: 0, contentPadding: 0, kind: 'main' };
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

    /** Direct parents of a node, with transformational parents replaced by their regular ancestors. */
    #collapseParents(id: string): string[] {
        const result: string[] = [];
        const seen = new Set<string>();
        (this.#directParents.get(id) || []).forEach((parentId) => {
            const resolved = this.#transformationalIds.has(parentId)
                ? this.#resolveRegularAncestors(parentId, new Set())
                : [parentId];
            resolved.forEach((ancestorId) => {
                if (!seen.has(ancestorId)) {
                    seen.add(ancestorId);
                    result.push(ancestorId);
                }
            });
        });
        return result;
    }

    /** Nearest regular (or box) ancestors of a transformational node, walking up through chains. */
    #resolveRegularAncestors(id: string, visiting: Set<string>): string[] {
        const memoized = this.#regularAncestors.get(id);
        if (memoized) return memoized;
        if (visiting.has(id)) return []; // Cycle of transformational nodes
        visiting.add(id);
        const result: string[] = [];
        const seen = new Set<string>();
        (this.#directParents.get(id) || []).forEach((parentId) => {
            const resolved = this.#transformationalIds.has(parentId)
                ? this.#resolveRegularAncestors(parentId, visiting)
                : [parentId];
            resolved.forEach((ancestorId) => {
                if (!seen.has(ancestorId)) {
                    seen.add(ancestorId);
                    result.push(ancestorId);
                }
            });
        });
        this.#regularAncestors.set(id, result);
        return result;
    }

    /**
     * Assigns layers to bounding boxes from the box-to-box lineage alone, before any regular node
     * is considered: per direction, each box is one layer past its furthest same-direction parent
     * (Kahn's algorithm; boxes in cycles fall back to their placed parents). The home box is layer
     * 0, as are boxes with no direction (disconnected from the home box).
     */
    #assignBoxLayers(): void {
        const boxIds = this.topologicalNodes.filter((node) => this.#isBoxPlacedFirst(node.id)).map((node) => node.id);
        this.#layers.set(this.homeUrn, 0);

        [LineageDirection.Downstream, LineageDirection.Upstream].forEach((direction) => {
            const sign = direction === LineageDirection.Downstream ? 1 : -1;
            const directionBoxIds = boxIds.filter((id) => this.nodeInformation[id]?.direction === direction);
            const directionBoxSet = new Set(directionBoxIds);
            const parentsOf = (id: string): string[] =>
                Array.from(this.#boundingBoxes.adjacency[reverseDirection(direction)].get(id) || []).filter(
                    (parentId) => parentId === this.homeUrn || directionBoxSet.has(parentId),
                );

            const inDegree = new Map(
                directionBoxIds.map((id) => [id, parentsOf(id).filter((parentId) => parentId !== this.homeUrn).length]),
            );
            const assignLayer = (id: string) => {
                const parentDepths = parentsOf(id)
                    .map((parentId) => this.#layers.get(parentId))
                    .filter((layer): layer is number => layer !== undefined)
                    .map(Math.abs);
                this.#layers.set(id, sign * (parentDepths.length ? Math.max(...parentDepths) + 1 : 1));
            };

            const queue = directionBoxIds.filter((id) => !inDegree.get(id));
            for (let i = 0; i < queue.length; i++) {
                assignLayer(queue[i]);
                this.#boundingBoxes.adjacency[direction].get(queue[i])?.forEach((childId) => {
                    if (!directionBoxSet.has(childId)) return;
                    const remaining = (inDegree.get(childId) || 0) - 1;
                    inDegree.set(childId, remaining);
                    if (remaining === 0) queue.push(childId);
                });
            }
            // Boxes in cycles never reach in-degree 0; position them off whichever parents are placed
            directionBoxIds.forEach((id) => {
                if (!this.#layers.has(id)) assignLayer(id);
            });
        });

        // Boxes disconnected from the home box share its layer, stacked below it
        boxIds.forEach((id) => {
            if (!this.#layers.has(id)) this.#layers.set(id, 0);
        });
    }

    /**
     * Orders regular non-box nodes so that each appears after all of its non-box collapsed parents
     * (Kahn's algorithm); box parents are always placed first, so they don't constrain the order.
     * Nodes in cycles never reach in-degree 0 and are appended in their original order.
     */
    #computeFreeNodeOrder(): LineageNode[] {
        const freeNodes = this.topologicalNodes.filter(
            (node) => !this.#isBoxPlacedFirst(node.id) && !this.#transformationalIds.has(node.id),
        );
        const nodesById = new Map(freeNodes.map((node) => [node.id, node]));
        const childIds = new Map<string, string[]>();
        const inDegree = new Map<string, number>();
        freeNodes.forEach((node) => {
            const freeParents = (this.#positionalParents.get(node.id) || []).filter((parentId) =>
                nodesById.has(parentId),
            );
            inDegree.set(node.id, freeParents.length);
            freeParents.forEach((parentId) => setDefault(childIds, parentId, []).push(node.id));
        });

        const queue = freeNodes.filter((node) => !inDegree.get(node.id)).map((node) => node.id);
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
        freeNodes.forEach((node) => {
            if (!placed.has(node.id)) order.push(node);
        });
        return order;
    }

    #assignFreeNodeLayers(): void {
        this.#freeNodeOrder.forEach((node) => {
            const sign = node.direction === LineageDirection.Upstream ? -1 : 1;
            const parentDepths = (this.#positionalParents.get(node.id) || [])
                .map((parentId) => this.#layers.get(parentId))
                .filter((layer): layer is number => layer !== undefined)
                .map(Math.abs);
            this.#layers.set(node.id, sign * (parentDepths.length ? Math.max(...parentDepths) + 1 : 1));
        });
    }

    /**
     * Computes each layer's width (the maximum width of its nodes) and x position, then each
     * node's x within its layer: downstream nodes left-align, upstream nodes right-align.
     */
    #computeXPositions(): void {
        this.#layers.forEach((layer, id) => {
            const size = this.#sizes.get(id);
            if (!size) return;
            // Boxes are shifted so their contents align with the column, protruding one
            // `contentPadding` into the preceding gap; only the rest counts toward the layer width
            this.#layerWidths.set(layer, Math.max(this.#layerWidths.get(layer) ?? 0, size.width - size.contentPadding));
            const current = this.#layerKinds.get(layer);
            if (current === undefined || KIND_RANK[size.kind] > KIND_RANK[current]) {
                this.#layerKinds.set(layer, size.kind);
            }
        });

        const layerValues = Array.from(this.#layers.values());
        this.#layerX.set(0, 0);
        const maxLayer = Math.max(0, ...layerValues);
        const minLayer = Math.min(0, ...layerValues);
        for (let layer = 1; layer <= maxLayer; layer++) {
            this.#layerX.set(
                layer,
                (this.#layerX.get(layer - 1) ?? 0) +
                    (this.#layerWidths.get(layer - 1) ?? 0) +
                    this.#gapSeparation(layer),
            );
        }
        for (let layer = -1; layer >= minLayer; layer--) {
            this.#layerX.set(
                layer,
                (this.#layerX.get(layer + 1) ?? 0) - this.#gapSeparation(layer) - (this.#layerWidths.get(layer) ?? 0),
            );
        }

        this.topologicalNodes.forEach((node) => {
            if (this.#transformationalIds.has(node.id)) return;
            const size = this.#sizes.get(node.id);
            if (!size) return;
            const layer = this.#layers.get(node.id) ?? 0;
            let x = (this.#layerX.get(layer) ?? 0) - size.contentPadding;
            if (node.direction === LineageDirection.Upstream) {
                x += (this.#layerWidths.get(layer) ?? size.width) - size.width + 2 * size.contentPadding;
            }
            // Each node gets its own "layer", so that the inherited createNode reads its exact x position
            this.nodeInformation[node.id].layer = node.id;
            this.layerPositions.set(node.id, x);
        });
    }

    /**
     * Determines which column gap each transformational node sits in, and where along it, based
     * on its depth within its transformational chain. Also records the longest chain crossing
     * each gap, so `#computeXPositions` can widen the gap to fit it.
     */
    #computeTransformationalGapInfo(): void {
        const ancestorDepths = new Map<string, number>();
        const descendantDepths = new Map<string, number>();
        const depthThrough = (id: string, neighbors: Map<string, string[]>, memo: Map<string, number>): number => {
            const memoized = memo.get(id);
            if (memoized !== undefined) return memoized;
            memo.set(id, 1); // Break cycles of transformational nodes
            const transformationalNeighbors = (neighbors.get(id) || []).filter((neighborId) =>
                this.#transformationalIds.has(neighborId),
            );
            const depth = 1 + Math.max(0, ...transformationalNeighbors.map((n) => depthThrough(n, neighbors, memo)));
            memo.set(id, depth);
            return depth;
        };

        this.#transformationalIds.forEach((id) => {
            const sign = this.nodeInformation[id]?.direction === LineageDirection.Upstream ? -1 : 1;
            const ancestorDepth = depthThrough(id, this.#directParents, ancestorDepths);
            const descendantDepth = depthThrough(id, this.#directChildren, descendantDepths);
            const chainLength = ancestorDepth + descendantDepth - 1;

            const ancestorLayers = this.#resolveRegularAncestors(id, new Set())
                .map((ancestorId) => this.#layers.get(ancestorId))
                .filter((layer): layer is number => layer !== undefined)
                .map(Math.abs);
            const nearLayer = sign * (ancestorLayers.length ? Math.max(...ancestorLayers) : 0);
            const farLayer = nearLayer + sign;

            this.#transformationalGapInfo.set(id, {
                nearLayer,
                farLayer,
                fraction: ancestorDepth / (chainLength + 1),
            });
            this.#gapChainLengths.set(farLayer, Math.max(this.#gapChainLengths.get(farLayer) ?? 0, chainLength));
        });
    }

    /** Separation before the given layer: the base gap for the kinds of nodes on each side, widened
     * if needed to fit the transformational chain crossing the gap. */
    #gapSeparation(farLayer: number): number {
        const nearLayer = farLayer - Math.sign(farLayer);
        const ratio = layerSeparationRatio(
            this.#layerKinds.get(nearLayer) ?? 'main',
            this.#layerKinds.get(farLayer) ?? 'main',
        );
        const chainLength = this.#gapChainLengths.get(farLayer) ?? 0;
        return Math.max(
            this.nodeWidth * ratio,
            (chainLength + 1) * TRANSFORMATION_NODE_X_SPACING + chainLength * TRANSFORMATION_NODE_SIZE,
        );
    }

    /**
     * Places transformational nodes horizontally within the gap between their regular ancestors'
     * column and the next one, spread out by their depth within their transformational chain.
     */
    #computeTransformationalXPositions(): void {
        this.#transformationalIds.forEach((id) => {
            const gapInfo = this.#transformationalGapInfo.get(id);
            if (!gapInfo) return;
            const { nearLayer, farLayer, fraction } = gapInfo;
            const sign = farLayer - nearLayer;

            // Gap edges: from the near column's far side to the far column's near side
            const gapStart =
                (this.#layerX.get(nearLayer) ?? 0) + (sign > 0 ? (this.#layerWidths.get(nearLayer) ?? 0) : 0);
            const farLayerX = this.#layerX.get(farLayer);
            const gapEnd =
                farLayerX !== undefined
                    ? farLayerX + (sign > 0 ? 0 : (this.#layerWidths.get(farLayer) ?? 0))
                    : gapStart + sign * this.#gapSeparation(farLayer);

            const centerX = gapStart + (gapEnd - gapStart) * fraction;
            this.nodeInformation[id].layer = id;
            this.layerPositions.set(id, centerX - TRANSFORMATION_NODE_SIZE / 2);
        });
    }

    computeNodeY(): void {
        this.#placeBoundingBoxes();
        this.#placeFreeNodes();
        this.#placeTransformationalNodes();

        this.filterNodes.forEach((node) => {
            const info = this.nodeInformation[node.id];
            if (info.y !== undefined) {
                info.y -= 15; // Manual offset until filter node positioning is improved
            }
        });
    }

    #placeBoundingBoxes(): void {
        const boxIds = this.topologicalNodes.filter((node) => this.#isBoxPlacedFirst(node.id)).map((node) => node.id);
        if (this.nodeInformation[this.homeUrn]) this.#setY(this.homeUrn, 0);

        [LineageDirection.Downstream, LineageDirection.Upstream].forEach((direction) => {
            const directionBoxIds = boxIds.filter((id) => this.nodeInformation[id]?.direction === direction);
            const depths = Array.from(new Set(directionBoxIds.map((id) => Math.abs(this.#layers.get(id) ?? 0)))).sort(
                (a, b) => a - b,
            );
            depths.forEach((depth) => {
                this.#placeLayerChildren(
                    directionBoxIds.filter((id) => Math.abs(this.#layers.get(id) ?? 0) === depth),
                    (id) => Array.from(this.#boundingBoxes.adjacency[reverseDirection(direction)].get(id) || []),
                    (parentId) => Array.from(this.#boundingBoxes.adjacency[direction].get(parentId) || []),
                    false,
                );
            });
        });

        // Boxes disconnected from the home box stack below it
        boxIds.forEach((id) => {
            if (this.nodeInformation[id]?.y === undefined) this.#placeBelowLayer(id);
        });
    }

    #placeFreeNodes(): void {
        const freeIds = this.#freeNodeOrder.map((node) => node.id);
        // Outward from the home box; downstream before upstream at equal depth (the two never interact)
        const layers = Array.from(new Set(freeIds.map((id) => this.#layers.get(id) ?? 0))).sort(
            (a, b) => Math.abs(a) - Math.abs(b) || b - a,
        );
        layers.forEach((layer) => {
            this.#placeLayerChildren(
                freeIds.filter((id) => (this.#layers.get(id) ?? 0) === layer),
                (id) => this.#positionalParents.get(id) || [],
                (parentId) => this.#childrenByParent.get(parentId) || [],
                true,
            );
        });
    }

    /**
     * Places transformational nodes on the edges they belong to: vertically at the average of
     * their placed neighbors' edge anchors. Nodes are processed in chain order, so transformational
     * parents have positions before their transformational children.
     */
    #placeTransformationalNodes(): void {
        // Anchor of a neighbor `id`, as seen from the transformational node `fromId` being placed.
        const anchorOf = (id: string, fromId: string): number | undefined => {
            const y = this.nodeInformation[id]?.y;
            if (y === undefined) return undefined;
            if (this.#transformationalIds.has(id)) return y + TRANSFORMATION_NODE_SIZE / 2;
            // A box anchors at the row of the member `fromId` connects to (its own anchor into the
            // box), falling back to the first member row; regular nodes anchor at their own row.
            const memberOffset = this.#isBox(id) ? this.#childBoxAnchors.get(fromId)?.get(id) : undefined;
            return y + (memberOffset ?? this.#sizes.get(id)?.contentPadding ?? 0) + LINEAGE_HANDLE_OFFSET;
        };

        // Order by depth from regular ancestors, so chains place outward
        const chainDepths = new Map<string, number>();
        const chainDepth = (id: string): number => {
            const memoized = chainDepths.get(id);
            if (memoized !== undefined) return memoized;
            chainDepths.set(id, 1); // Break cycles of transformational nodes
            const transformationalParents = (this.#directParents.get(id) || []).filter((parentId) =>
                this.#transformationalIds.has(parentId),
            );
            const depth = 1 + Math.max(0, ...transformationalParents.map(chainDepth));
            chainDepths.set(id, depth);
            return depth;
        };
        const ordered = Array.from(this.#transformationalIds).sort((a, b) => chainDepth(a) - chainDepth(b));

        // Transformational nodes don't affect the layout of regular nodes, but they must not
        // overlap each other (e.g. parallel chains between the same two columns): each one shifts
        // down below any horizontally-intersecting, already-placed transformational node
        const placedCircles: { x: number; y: number }[] = [];
        const minSeparation = this.separationNodeHeight * MINI_Y_SEP_RATIO;
        const resolveCircleOverlaps = (x: number, desiredY: number): number => {
            const topmostBlocking = (y: number) =>
                placedCircles
                    .filter(
                        (circle) =>
                            circle.x < x + TRANSFORMATION_NODE_SIZE &&
                            x < circle.x + TRANSFORMATION_NODE_SIZE &&
                            circle.y - minSeparation < y + TRANSFORMATION_NODE_SIZE &&
                            y - minSeparation < circle.y + TRANSFORMATION_NODE_SIZE,
                    )
                    .sort((a, b) => a.y - b.y)[0];

            let y = desiredY;
            for (let step = 0; step < MAX_RESOLUTION_STEPS; step++) {
                const blocking = topmostBlocking(y);
                if (!blocking) return y;
                y = blocking.y + TRANSFORMATION_NODE_SIZE + minSeparation;
            }
            return y;
        };

        ordered.forEach((id) => {
            const anchors = [
                ...(this.#directParents.get(id) || []),
                ...(this.#directChildren.get(id) || []).filter((childId) => !this.#transformationalIds.has(childId)),
            ]
                .map((neighborId) => anchorOf(neighborId, id))
                .filter((anchor): anchor is number => anchor !== undefined);
            let centerY = anchors.length ? anchors.reduce((a, b) => a + b) / anchors.length : 0;
            if (!this.#directChildren.get(id)?.length) {
                // Leaves sit just below the edge line, so they don't overlap sibling edges
                centerY += TRANSFORMATIONAL_LEAF_OFFSET;
            }
            const x = this.layerPositions.get(id) ?? 0;
            const y = resolveCircleOverlaps(x, centerY - TRANSFORMATION_NODE_SIZE / 2);
            placedCircles.push({ x, y });
            this.nodeInformation[id].y = y;
        });
    }

    /**
     * Places one layer's nodes. Parents (already-placed nodes with unplaced children in this
     * layer) are processed from top to bottom, so higher parents' children claim space first. Each
     * parent's first child top-aligns with it and subsequent children stack below, with lineage
     * filter nodes ordered above all other children. Nodes with no placed parent (cycles,
     * disconnected boxes) stack below everything already in the layer.
     *
     * With `allowBoxPush`, second-or-later children push intersecting bounding boxes down instead
     * of yielding to them, keeping each sibling stack contiguous.
     */
    #placeLayerChildren(
        layerIds: string[],
        parentsOf: (id: string) => string[],
        childrenOf: (parentId: string) => string[],
        allowBoxPush: boolean,
    ): void {
        const unplaced = new Set(layerIds.filter((id) => this.nodeInformation[id]?.y === undefined));

        const parentIds = new Set<string>();
        unplaced.forEach((id) =>
            parentsOf(id).forEach((parentId) => {
                if (this.nodeInformation[parentId]?.y !== undefined) parentIds.add(parentId);
            }),
        );
        const orderedParents = Array.from(parentIds).sort(
            (a, b) => (this.nodeInformation[a].y ?? 0) - (this.nodeInformation[b].y ?? 0) || a.localeCompare(b),
        );

        orderedParents.forEach((parentId) => {
            const parentY = this.nodeInformation[parentId]?.y;
            if (parentY === undefined) return;
            // Nodes align by their content anchor: a box's first member row, inset by its padding
            const parentContentY = parentY + (this.#sizes.get(parentId)?.contentPadding ?? 0);
            // Row offset within `parentId` at which to anchor a child that links to a specific
            // member of that bounding box (rather than to the box as a whole). Filter nodes anchor
            // to the box top, as before.
            const anchorOffset = (childId: string): number | undefined =>
                this.nodeInformation[childId]?.type === LINEAGE_FILTER_TYPE
                    ? undefined
                    : this.#childBoxAnchors.get(childId)?.get(parentId);
            const children = childrenOf(parentId).filter((childId) => unplaced.has(childId));
            // Floating boxes (positioned as this free node's children) go at the very top, then
            // lineage filter nodes, then the rest ordered by the member row they anchor to (so they
            // stack in member order, unanchored last).
            const orderedChildren = [
                ...children.filter((id) => this.#isFloatingBox(id)),
                ...children.filter(
                    (id) => !this.#isFloatingBox(id) && this.nodeInformation[id]?.type === LINEAGE_FILTER_TYPE,
                ),
                ...children
                    .filter((id) => !this.#isFloatingBox(id) && this.nodeInformation[id]?.type !== LINEAGE_FILTER_TYPE)
                    .sort((a, b) => (anchorOffset(a) ?? Infinity) - (anchorOffset(b) ?? Infinity)),
            ];

            let prev: PlacedRect | undefined;
            orderedChildren.forEach((childId) => {
                const size = this.#sizes.get(childId);
                if (!size) return;
                const layer = this.#layers.get(childId) ?? 0;
                const offset = anchorOffset(childId);
                const belowPrev = prev ? prev.y + prev.height + this.#yGap() + size.topMargin : undefined;
                // Anchor to the linked member's row when known, else top-align to the parent (first
                // child) or stack below the previous sibling. Never place above the previous sibling.
                let base: number;
                if (offset !== undefined) {
                    base = parentY + offset;
                } else {
                    base = belowPrev ?? parentContentY - size.contentPadding;
                }
                const desiredY = belowPrev !== undefined ? Math.max(base, belowPrev) : base;
                const y =
                    allowBoxPush && prev
                        ? this.#placeWithBoxPush(layer, size, desiredY)
                        : this.#resolveDownward(layer, size, desiredY);
                unplaced.delete(childId);
                prev = this.#setY(childId, y) ?? prev;
            });
        });

        unplaced.forEach((id) => this.#placeBelowLayer(id));
    }

    /** Records a node's y position, reserving its extent within its layer. */
    #setY(id: string, y: number): PlacedRect | undefined {
        this.nodeInformation[id].y = y;
        const size = this.#sizes.get(id);
        if (!size) return undefined;
        const rect: PlacedRect = { id, y, height: size.height, topMargin: size.topMargin, kind: size.kind };
        setDefault(this.#layerRects, this.#layers.get(id) ?? 0, []).push(rect);
        return rect;
    }

    /** Stacks a node below everything already placed in its layer (or at 0 if the layer is empty). */
    #placeBelowLayer(id: string): void {
        const size = this.#sizes.get(id);
        if (!size) return;
        const rects = this.#layerRects.get(this.#layers.get(id) ?? 0) ?? [];
        if (!rects.length) {
            this.#setY(id, 0);
            return;
        }
        let floorBottom = -Infinity;
        rects.forEach((rect) => {
            floorBottom = Math.max(floorBottom, rect.y + rect.height);
        });
        this.#setY(id, floorBottom + this.#yGap() + size.topMargin);
    }

    /**
     * Returns the closest y at or below `desiredY` at which the node fits without overlapping
     * anything already placed in its layer.
     */
    #resolveDownward(layer: number, size: ResolvedSize, desiredY: number): number {
        let y = desiredY;
        for (let step = 0; step < MAX_RESOLUTION_STEPS; step++) {
            const blocking = this.#topmostIntersecting(layer, size, y);
            if (!blocking) return y;
            y = blocking.y + blocking.height + this.#yGap() + size.topMargin;
        }
        return y;
    }

    /**
     * Places a second-or-later sibling: it still resolves downward past regular nodes, but
     * bounding boxes yield to it instead — every box it intersects is pushed below it, and the
     * push cascades to anything the moved boxes then overlap.
     */
    #placeWithBoxPush(layer: number, size: ResolvedSize, desiredY: number): number {
        let y = desiredY;
        for (let step = 0; step < MAX_RESOLUTION_STEPS; step++) {
            const blocking = this.#topmostIntersecting(layer, size, y, (rect) => rect.kind !== 'box');
            if (!blocking) break;
            y = blocking.y + blocking.height + this.#yGap() + size.topMargin;
        }

        const intersectingBoxes = (this.#layerRects.get(layer) ?? [])
            .filter((rect) => rect.kind === 'box' && this.#intersects(rect, y, size))
            .sort((a, b) => a.y - b.y);
        let floorBottom = y + size.height;
        intersectingBoxes.forEach((rect) => {
            this.#moveRect(rect, floorBottom + this.#yGap() + rect.topMargin);
            floorBottom = rect.y + rect.height;
        });
        if (intersectingBoxes.length) this.#sweepLayerDown(layer);
        return y;
    }

    #moveRect(rect: PlacedRect, y: number): void {
        rect.y = y; // eslint-disable-line no-param-reassign
        this.nodeInformation[rect.id].y = y;
    }

    /**
     * Cascades pushes: sweeps a layer top to bottom, moving any node down that no longer clears
     * the nodes above it. Nodes only ever move down, so anything already separated is untouched.
     */
    #sweepLayerDown(layer: number): void {
        const rects = this.#layerRects.get(layer);
        if (!rects) return;
        rects.sort((a, b) => a.y - b.y || a.id.localeCompare(b.id));
        let floorBottom = -Infinity;
        rects.forEach((rect) => {
            if (floorBottom !== -Infinity) {
                const minY = floorBottom + this.#yGap() + rect.topMargin;
                if (rect.y < minY) this.#moveRect(rect, minY);
            }
            floorBottom = Math.max(floorBottom, rect.y + rect.height);
        });
    }

    #topmostIntersecting(
        layer: number,
        size: ResolvedSize,
        y: number,
        filterFn?: (rect: PlacedRect) => boolean,
    ): PlacedRect | undefined {
        const intersecting = (this.#layerRects.get(layer) ?? []).filter(
            (rect) => (!filterFn || filterFn(rect)) && this.#intersects(rect, y, size),
        );
        if (!intersecting.length) return undefined;
        return intersecting.reduce((top, rect) => (rect.y - rect.topMargin < top.y - top.topMargin ? rect : top));
    }

    #intersects(rect: PlacedRect, y: number, size: ResolvedSize): boolean {
        const gap = this.#yGap();
        return y - size.topMargin - gap < rect.y + rect.height && rect.y - rect.topMargin - gap < y + size.height;
    }

    #yGap(): number {
        return this.separationNodeHeight * MAIN_Y_SEP_RATIO;
    }
}
