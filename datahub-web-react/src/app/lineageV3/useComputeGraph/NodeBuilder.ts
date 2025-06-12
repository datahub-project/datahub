import { EdgeMarkerType } from '@reactflow/core/dist/esm/types/edges';
import { Edge, Node } from 'reactflow';

import { LINEAGE_BOUNDING_BOX_NODE_NAME } from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { DATA_JOB_INPUT_OUTPUT_EDGE_NAME } from '@app/lineageV3/LineageEdge/DataJobInputOutputEdge';
import { LINEAGE_TABLE_EDGE_NAME } from '@app/lineageV3/LineageEdge/LineageTableEdge';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import {
    LINEAGE_TRANSFORMATION_NODE_NAME,
    TRANSFORMATION_NODE_SIZE,
} from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import {
    DataJobInputOutputEdgeData,
    EdgeId,
    LINEAGE_FILTER_TYPE,
    LINEAGE_HANDLE_OFFSET,
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageTableEdgeData,
    NodeContext,
    createEdgeId,
    getEdgeId,
    getParents,
    isGhostEntity,
    isTransformational,
    parseEdgeId,
    setDefault,
} from '@app/lineageV3/common';
import { LINEAGE_ARROW_MARKER } from '@app/lineageV3/lineageSVGs';

import { EntityType, LineageDirection } from '@types';

export const MAIN_X_SEP_RATIO = 0.75;
const MAIN_TO_MINI_X_SEP_RATIO = 0.25;
const MINI_X_SEP_RATIO = 0.125;
const MAIN_Y_SEP_RATIO = 0.5;
const MINI_Y_SEP_RATIO = MAIN_Y_SEP_RATIO / 2;
const TRANSFORMATIONAL_LEAF_OFFSET = 25;

export type LineageVisualizationNode = Node<LineageEntity | LineageFilter | LineageBoundingBox>;
export type LineageVisualizationEdgeData = LineageTableEdgeData | DataJobInputOutputEdgeData;

export function getNodePriority(node: LineageVisualizationNode) {
    switch (node.type) {
        case LINEAGE_BOUNDING_BOX_NODE_NAME:
            return 1;
        default:
            return 0;
    }
}

type BaseEdge<T> = Pick<Edge<T>, 'source' | 'target' | 'markerEnd' | 'data'>;

type Layer = string; // [main (entity) layer, mini (transformation) layer]
const defaultLayer = '0.0';

function createLayer(main: number, mini: number): Layer {
    // toLocaleString preserves negative 0, used for transformational nodes directly upstream of home node
    return `${main.toLocaleString()}.${mini}`;
}

function parseLayer(layer?: Layer): { main: number; mini: number } {
    if (!layer) {
        return parseLayer(defaultLayer);
    }
    const [main, mini] = layer.split('.');
    return { main: parseInt(main, 10), mini: parseInt(mini, 10) };
}

interface NodeInformation {
    urn?: string;
    type: EntityType | typeof LINEAGE_FILTER_TYPE;
    direction?: LineageDirection;
    inCycle?: boolean;
    layer?: Layer;
    positionalParents?: Set<string>;
    y?: number;
}

/**
 * Mega class used to determine node positions and edges to render for lineage visualizations.
 *
 * Also generates nodes and edges into a form consumable by ReactFlow,
 * assigning node and edge types, markers, parent nodes for subgraphs, whether the node is selectable, etc.
 * This should probably be factored out.
 *
 * Arranges nodes into layers, where each layer contains nodes of the same type, main (big rectangle) or mini (small circle).
 * Nodes are assigned to layers based on the shortest path from the root (home node, at layer 0).
 *
 * Also supports having no explicit home node for the data flow DAG visualization; in this case,
 * nodes must be in topological order, and all root nodes are assigned layer 0.
 *
 * In the future, would like to build another node positioner that assigns layers based on longest path distance instead.
 * Can also consider creating one that uses a force-directed layout.
 */
export default class NodeBuilder {
    homeUrn: string;

    homeType: EntityType;

    roots: Set<string>;

    isHomeTransformational: boolean;

    parents: Map<string, Set<string>>;

    nodeWidth: number;

    nodeHeight: number;

    separationNodeHeight: number;

    isHorizontal: boolean;

    // Must set node layers in rough topological order
    // A node must be preceded by all its min-parents, the parents along the shortest paths from the home node to it
    // TODO: Memoize this min-parent calculation?
    topologicalNodes: LineageNode[] = [];

    entities: LineageEntity[] = [];

    transformations: LineageEntity[] = [];

    filterNodes: LineageFilter[] = [];

    layerPositions = new Map<Layer, number>();

    layerNodes = new Map<Layer, Set<string>>();

    nodeInformation: Record<string, NodeInformation> = {};

    // Note: Relies on the fact that transformation node id == urn
    transformationChildren = new Map<string, Set<string>>();

    // Note: nodes must be provided in shortest-path order
    constructor(
        rootUrn: string,
        rootType: EntityType,
        roots: LineageEntity[],
        nodes: LineageNode[],
        parents: Map<string, Set<string>>,
        isHorizontal = true,
    ) {
        this.homeUrn = rootUrn;
        this.homeType = rootType;
        this.roots = new Set(roots.map((node) => node.urn));
        this.parents = parents;
        this.isHorizontal = isHorizontal;
        this.nodeHeight = rootType === EntityType.DataFlow ? LINEAGE_NODE_WIDTH : LINEAGE_NODE_HEIGHT;
        this.nodeWidth = rootType === EntityType.DataFlow ? LINEAGE_NODE_HEIGHT - 10 : LINEAGE_NODE_WIDTH;

        // +15 accounts for column footer
        this.separationNodeHeight = this.nodeHeight + (rootType === EntityType.SchemaField ? 15 : 0);

        this.isHomeTransformational = isTransformational({ urn: rootUrn, type: rootType }, rootType);
        nodes.forEach((node) => {
            this.nodeInformation[node.id] = {
                urn: node.urn,
                type: node.type,
                direction: node.direction,
                inCycle: node.inCycle,
            };
            this.#getNodeList(node).push(node);
            this.topologicalNodes.push(node);
        });
        roots.forEach((node) => {
            this.nodeInformation[node.urn] = { urn: node.urn, type: node.type, y: 0 };
        });
    }

    #getNodeList(node: LineageNode): LineageNode[] {
        if (node.type === LINEAGE_FILTER_TYPE) return this.filterNodes;
        if (isTransformational(node, this.homeType)) return this.transformations;
        return this.entities;
    }

    #isMainNode(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
        return !isTransformational(node, this.homeType);
    }

    #getMarker(information?: NodeInformation): EdgeMarkerType | undefined {
        return information && this.#isMainNode(information) ? LINEAGE_ARROW_MARKER : undefined;
    }

    createNodes(
        context: Pick<NodeContext, 'adjacencyList' | 'edges'>,
        ignoreSchemaFieldStatus: boolean,
        offsets: Map<LineageDirection | undefined, [number, number]>,
        parent?: string,
    ): LineageVisualizationNode[] {
        this.computeNodeX(context);
        this.computeNodeY();

        const nodes: LineageVisualizationNode[] = [];
        nodes.push(
            ...this.entities.map((n) =>
                this.createNode(
                    n,
                    LINEAGE_ENTITY_NODE_NAME,
                    !isGhostEntity(n.entity, ignoreSchemaFieldStatus),
                    offsets.get(n.direction) || [0, 0],
                    parent,
                ),
            ),
        );
        nodes.push(
            ...this.transformations.map((n) =>
                this.createNode(
                    n,
                    LINEAGE_TRANSFORMATION_NODE_NAME,
                    !isGhostEntity(n.entity, ignoreSchemaFieldStatus),
                    offsets.get(n.direction) || [0, 0],
                    parent,
                ),
            ),
        );
        nodes.push(
            ...this.filterNodes.map((n) => this.createFilterNode(n, offsets.get(n.direction) || [0, 0], parent)),
        );
        console.debug(this);
        return nodes;
    }

    #addEdge(
        edgeMap: Map<EdgeId, BaseEdge<LineageTableEdgeData>>,
        source: string,
        target: string,
        data?: LineageTableEdgeData,
    ): void {
        const edge = setDefault(edgeMap, createEdgeId(source, target), {
            source,
            target,
            markerEnd: this.#getMarker(this.nodeInformation[target]),
        });
        if (data) {
            edge.data = { ...edge.data, ...data };
        }
    }

    createEdge<T>(edge: BaseEdge<T>, handle: string | undefined, edgeType: string = LINEAGE_TABLE_EDGE_NAME): Edge {
        return {
            ...edge,
            id: createEdgeId(edge.source, edge.target),
            type: edgeType,
            sourceHandle: handle,
            targetHandle: handle,
        };
    }

    createEdges(
        edges: NodeContext['edges'],
        offsets: Map<LineageDirection | undefined, [number, number]>,
        handle?: string,
        isDataJobInputOutput = false,
    ): Edge<LineageTableEdgeData>[] {
        const baseEdges = new Map<EdgeId, BaseEdge<LineageVisualizationEdgeData>>();
        edges.forEach((edge, edgeId) => {
            const [upstream, downstream] = parseEdgeId(edgeId);
            if (upstream in this.nodeInformation && downstream in this.nodeInformation) {
                const upstreamDirection = this.nodeInformation[upstream].direction;
                const downstreamDirection = this.nodeInformation[downstream].direction;
                if (
                    !this.nodeInformation[upstream].inCycle &&
                    !this.nodeInformation[downstream].inCycle &&
                    upstreamDirection &&
                    downstreamDirection &&
                    upstreamDirection !== downstreamDirection
                ) {
                    // Don't render edges between nodes upstream of home node and nodes downstream of home node
                    return;
                }

                const originalId = createEdgeId(upstream, downstream);
                const edgeData = { ...edge, originalId };
                if (edge.via) {
                    this.#addEdge(baseEdges, upstream, edge.via, edgeData);
                    this.#addEdge(baseEdges, edge.via, downstream, edgeData);
                } else {
                    this.#addEdge(baseEdges, upstream, downstream, edgeData);
                }
            }
        });
        this.filterNodes.forEach((node) => {
            if (node.direction === LineageDirection.Upstream) {
                this.#addEdge(baseEdges, node.id, node.parent);
            } else {
                this.#addEdge(baseEdges, node.parent, node.id);
            }
        });
        return Array.from(baseEdges.values()).map((v) => {
            if (isDataJobInputOutput) {
                const isUpstreamOfHome = v.target === this.homeUrn;
                const isDownstreamOfHome = v.source === this.homeUrn;
                const direction = isUpstreamOfHome ? LineageDirection.Upstream : LineageDirection.Downstream;
                if (isUpstreamOfHome || isDownstreamOfHome) {
                    return this.createEdge(
                        { ...v, data: { ...v.data, direction, yOffset: offsets.get(direction)?.[1] } },
                        handle,
                        DATA_JOB_INPUT_OUTPUT_EDGE_NAME,
                    );
                }
            }
            return this.createEdge(v, handle, LINEAGE_TABLE_EDGE_NAME);
        });
    }

    #isLayerMini(layer?: Layer): boolean {
        const { main, mini } = parseLayer(layer);
        return !!mini || (!main && !mini && this.isHomeTransformational);
    }

    #getLayerSeparation(layer: Layer, prevLayer?: Layer): number {
        const isCurrentLayerMini = this.#isLayerMini(layer);
        const wasLastLayerMini = this.#isLayerMini(prevLayer);
        if (isCurrentLayerMini && wasLastLayerMini) {
            return this.nodeWidth * MINI_X_SEP_RATIO;
        }
        if (isCurrentLayerMini || wasLastLayerMini) {
            return this.nodeWidth * MAIN_TO_MINI_X_SEP_RATIO;
        }
        return this.nodeWidth * MAIN_X_SEP_RATIO;
    }

    #getNodeSize(layer: Layer): number {
        return this.#isLayerMini(layer) ? TRANSFORMATION_NODE_SIZE : this.nodeWidth;
    }

    /**
     * Computes the x position of each node, by organizing them into layers.
     */
    computeNodeX({ adjacencyList, edges }: Pick<NodeContext, 'adjacencyList' | 'edges'>): void {
        this.topologicalNodes.forEach((node) => {
            // Filter out parents that are in the opposite direction
            // Exemptions for lineage filter node and queries because they aren't fully in the adjacency list
            // As well as the data flow graph, in which nodes have no direction
            // TODO: Unify into single function + figure out why this.parents doesn't work
            const parents = this.isHorizontal
                ? getParents(node, adjacencyList)
                : Array.from(this.parents.get(node.id) || []);
            const minParentLayer = parents
                .map<NodeInformation | undefined>((parent) => this.nodeInformation[parent])
                .filter(
                    (parent) =>
                        this.homeType === EntityType.DataFlow ||
                        node.type === LINEAGE_FILTER_TYPE ||
                        (node.type === EntityType.Query && [node.direction, undefined].includes(parent?.direction)) ||
                        (parent?.urn && node.direction && edges.has(getEdgeId(parent.urn, node.id, node.direction))),
                )
                .map((parent) => parent?.layer)
                .filter((layer): layer is Layer => layer !== undefined)
                .sort(compareLayers)?.[0];
            const { main: parentMain, mini: parentMini } = parseLayer(minParentLayer);
            if (this.#isMainNode(node)) {
                const factor = node.direction === LineageDirection.Upstream ? -1 : 1;
                const mainLayer = minParentLayer ? factor + parentMain : 0;
                this.addNodeToLayer(node, createLayer(mainLayer, 0));
            } else {
                this.addNodeToLayer(
                    node,
                    createLayer(
                        parentMain === 0 && node.direction === LineageDirection.Upstream ? -0 : parentMain,
                        parentMini + 1,
                    ),
                );
            }

            if (node.type === LINEAGE_FILTER_TYPE) {
                this.nodeInformation[node.id].positionalParents = new Set([node.parent]);
            } else {
                const positionalParents = Array.from(this.parents.get(node.id) || [])
                    .map((p) => [p, this.nodeInformation[p]?.layer])
                    .filter(([_p, l]) => parseLayer(l).main === parseLayer(minParentLayer).main)
                    .map(([p]) => p)
                    .filter((p): p is string => !!p);
                this.nodeInformation[node.id].positionalParents = new Set(positionalParents);
            }
        });

        const nextLayerMap = this.#computeLayerPositions();
        this.transformations.forEach((node) => {
            const nextLayer = nextLayerMap.get(this.nodeInformation[node.id].layer || '');
            if (node.direction) {
                const children = Array.from(adjacencyList[node.direction].get(node.urn) || []).filter(
                    (child) => !nextLayer || this.nodeInformation[child]?.layer === nextLayer,
                );
                this.transformationChildren.set(node.urn, new Set(children));
            }
        });
    }

    addNodeToLayer(node: LineageNode, layer: Layer): void {
        this.nodeInformation[node.id].layer = layer;
        setDefault(this.layerNodes, layer, new Set()).add(node.id);
    }

    /**
     * Computes the x position of each layer, based on the type (main or mini) of adjacent layers.
     * Returns a map of each layer to the next layer in the same direction.
     */
    #computeLayerPositions(): Map<Layer, Layer> {
        const nextLayerMap = new Map<Layer, Layer>();
        const upstreamLayers = Array.from(this.layerNodes.keys())
            .filter((layer) => layer.startsWith('-'))
            .sort(compareLayers);
        const downstreamLayers = Array.from(this.layerNodes.keys())
            .filter((layer) => !layer.startsWith('-'))
            .sort(compareLayers);

        this.layerPositions.set(defaultLayer, 0);
        upstreamLayers.forEach((layer, i) => {
            const prevLayer = upstreamLayers[i - 1];
            const separation = this.#getLayerSeparation(layer, prevLayer);
            this.layerPositions.set(
                layer,
                (this.layerPositions.get(prevLayer) || 0) - this.#getNodeSize(layer) - separation,
            );

            if (prevLayer !== defaultLayer) nextLayerMap.set(prevLayer, layer);
        });
        downstreamLayers.forEach((layer, i) => {
            if (i === 0) {
                return;
            }
            const prevLayer = downstreamLayers[i - 1];
            const separation = this.#getLayerSeparation(layer, prevLayer);
            this.layerPositions.set(
                layer,
                (this.layerPositions.get(prevLayer) || 0) + this.#getNodeSize(prevLayer) + separation,
            );

            if (prevLayer !== defaultLayer) nextLayerMap.set(prevLayer, layer);
        });

        return nextLayerMap;
    }

    /**
     * Computes the y position of each node, placing them between their parents' y positions,
     *   while maintaining a minimum separation between nodes. Results in a tree-like structure.
     * Must be called after computeNodeX.
     */
    computeNodeY(): void {
        const sortedLayers = Array.from(this.layerPositions.keys()).sort(compareLayersMinisLast);
        sortedLayers.forEach((layer) => {
            const { mini } = parseLayer(layer);
            const nodeHeight = mini
                ? this.separationNodeHeight * MINI_Y_SEP_RATIO + TRANSFORMATION_NODE_SIZE
                : this.separationNodeHeight * MAIN_Y_SEP_RATIO + this.separationNodeHeight;
            const nodes = this.layerNodes.get(layer) || new Set();
            const goalY: Record<string, number> = {};

            // Set initial position
            nodes.forEach((id) => {
                let nodeY: number;
                if (layer === defaultLayer) {
                    nodeY = 0;
                } else {
                    const relatives: string[] = Array.from(this.nodeInformation[id].positionalParents || []);
                    if (mini) {
                        relatives.push(...(this.transformationChildren.get(id) || []));
                    }
                    if (!relatives.length && !this.roots.has(id)) {
                        console.debug(`MISSING RELATIVES: ${id}`);
                    }
                    const relativesY = relatives
                        .map((p) => this.nodeInformation[p]?.y)
                        .filter((y): y is number => y !== undefined);
                    nodeY = relativesY.length ? relativesY.reduce((a, b) => a + b) / relativesY.length : 0;
                    if (mini && !this.transformationChildren.get(id)?.size) {
                        nodeY += TRANSFORMATIONAL_LEAF_OFFSET;
                    }
                }
                goalY[id] = nodeY;
                this.nodeInformation[id].y = nodeY;
            });

            if (nodes.size < 2) {
                return;
            }

            const sortedNodes = Array.from(nodes).sort((idA, idB) => goalY[idA] - goalY[idB] || idA.localeCompare(idB));
            const getY = (idx: number): number => {
                const id = sortedNodes[idx];
                const val = this.nodeInformation[id].y;
                if (val === undefined) {
                    console.warn(`Node at ${idx} has no y position`);
                    return 0;
                }
                return val;
            };

            // Create separation between nodes
            // TODO: Clean this up a bit
            let i = 0;
            let j = 1;
            let ySum = goalY[sortedNodes[0]];
            // eslint-disable-next-line no-constant-condition
            while (true) {
                const n = j - i;
                const groupHeight = n * nodeHeight;
                const avg = ySum / n + nodeHeight / 2;
                const leftBound = avg - groupHeight / 2;
                const rightBound = avg + groupHeight / 2;

                if (i > 0 && getY(i - 1) + nodeHeight > leftBound) {
                    ySum += goalY[sortedNodes[i - 1]];
                    i--;
                } else if (j === sortedNodes.length) {
                    for (let k = i; k < j; k++) {
                        this.nodeInformation[sortedNodes[k]].y = leftBound + (k - i) * nodeHeight;
                    }
                    break;
                } else if (getY(j) < rightBound) {
                    ySum += goalY[sortedNodes[j]];
                    j++;
                } else {
                    for (let k = i; k < j; k++) {
                        this.nodeInformation[sortedNodes[k]].y = leftBound + (k - i) * nodeHeight;
                    }
                    i = j;
                    j = i + 1;
                    ySum = goalY[sortedNodes[i]];
                }
            }
        });

        // Offset transformation nodes to align handles
        this.transformations.forEach((node) => {
            const info = this.nodeInformation[node.id];
            if (info.y !== undefined) {
                info.y += LINEAGE_HANDLE_OFFSET - TRANSFORMATION_NODE_SIZE / 2;
            }
        });

        this.filterNodes.forEach((node) => {
            const info = this.nodeInformation[node.id];
            if (info.y !== undefined) {
                info.y -= 15; // Manual offset until filter node positioning is improved
            }
        });
    }

    createNode<T extends LineageNode>(
        node: T,
        type: string,
        selectable: boolean,
        [offsetX, offsetY]: [number, number],
        parent: string | undefined,
        transformData = (v: T) => v,
    ): LineageVisualizationNode {
        const info = this.nodeInformation[node.id];
        const layer = info.layer || '';
        const x = this.layerPositions.get(layer) || 0;
        const y = info.y || 0;

        return {
            type,
            parentId: parent,
            id: node.id,
            position: {
                x: (this.isHorizontal ? x : y) + offsetX,
                y: (this.isHorizontal ? y : x) + offsetY,
            },
            data: transformData(node),
            selectable: selectable && node.type !== EntityType.SchemaField,
            extent: parent ? 'parent' : undefined,
        };
    }

    createFilterNode(
        filter: LineageFilter,
        offset: [number, number],
        parent: string | undefined,
    ): LineageVisualizationNode {
        return this.createNode(filter, LINEAGE_FILTER_NODE_NAME, false, offset, parent, (node) => ({
            ...node,
            numShown: Array.from(node.allChildren).filter((urn) => urn in this.nodeInformation).length,
        }));
    }
}

function compareLayers(a: Layer, b: Layer): number {
    const { main: aMain, mini: aMini } = parseLayer(a);
    const { main: bMain, mini: bMini } = parseLayer(b);
    return Math.abs(aMain) - Math.abs(bMain) || aMini - bMini;
}

function compareLayersMinisLast(a: Layer, b: Layer): number {
    const { mini: aMini } = parseLayer(a);
    const { mini: bMini } = parseLayer(b);
    if (aMini && !bMini) {
        return 1;
    }
    if (bMini && !aMini) {
        return -1;
    }
    return compareLayers(a, b);
}
