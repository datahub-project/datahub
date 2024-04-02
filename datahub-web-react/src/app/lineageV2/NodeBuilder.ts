import { EdgeMarker } from '@reactflow/core/dist/esm/types/edges';
import { Edge, MarkerType, Node } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import {
    createEdgeId,
    getParents,
    isQuery,
    isTransformational,
    LINEAGE_FILTER_TYPE,
    LineageEdge,
    LineageEntity,
    LineageFilter,
    LineageNode,
    NodeContext,
    parseEdgeId,
    setDefault,
} from './common';
import { LINEAGE_TABLE_EDGE_NAME } from './LineageEdge/LineageTableEdge';
import { LINEAGE_ENTITY_NODE_NAME } from './LineageEntityNode/LineageEntityNode';
import { LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH } from './LineageEntityNode/useDisplayedColumns';
import { LINEAGE_FILTER_NODE_NAME } from './LineageFilterNode/LineageFilterNode';
import {
    LINEAGE_TRANSFORMATION_NODE_NAME,
    TRANSFORMATION_NODE_SIZE,
} from './LineageTransformationNode/LineageTransformationNode';
import { LINEAGE_WORKBOOK_NODE_NAME, WORKBOOK_NODE_MAX_WIDTH } from './MinorNodes/TableauWorkbookNode';

const MAIN_X_SEP = 120;
const MINI_X_SEP = MAIN_X_SEP / 2;
const MAIN_Y_SEP = 30;
const MINI_Y_SEP = MAIN_Y_SEP / 2;

export type NodeWithMetadata = Node<LineageEntity | LineageFilter> & {
    layer?: number;
};
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
    layer?: Layer;
    positionalParents?: Set<string>;
    y?: number;
}

export default class NodeBuilder {
    homeUrn: string;

    isHomeTransformational: boolean;

    // Must set node layers in rough topological order
    // A node must be preceded by all its min-parents, the parents along the shortest paths from the home node to it
    // TODO: Memoize this min-parent calculation?
    topologicalNodes: LineageNode[] = [];

    entities: LineageEntity[] = [];

    transformations: LineageEntity[] = [];

    workbooks: LineageEntity[] = [];

    filterNodes: LineageFilter[] = [];

    layerPositions = new Map<Layer, number>();

    layerNodes = new Map<Layer, Set<string>>();

    nodeInformation: Record<string, NodeInformation> = {};

    // Note: Relies on the fact that transformation node id == urn
    transformationChildren = new Map<string, Set<string>>();

    // Note: nodes must be provided in shortest-path order
    constructor(homeUrn: string, homeType: EntityType, nodes: LineageNode[]) {
        this.homeUrn = homeUrn;
        this.isHomeTransformational = isTransformational({ urn: homeUrn, type: homeType });
        nodes.forEach((node) => {
            this.nodeInformation[node.id] = { urn: node.urn, type: node.type };
            this.#getNodeList(node).push(node);
            this.topologicalNodes.push(node);
        });
        this.nodeInformation[homeUrn] = { urn: homeUrn, type: homeType, y: 0 };
    }

    #getNodeList(node: LineageNode): LineageNode[] {
        if (node.type === LINEAGE_FILTER_TYPE) return this.filterNodes;
        if (node.type === EntityType.Container) return this.workbooks;
        if (isTransformational(node)) return this.transformations;
        return this.entities;
    }

    #isMainNode(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
        return !isTransformational(node);
    }

    #getMarker(information: NodeInformation): EdgeMarker | undefined {
        return this.#isMainNode(information) ? { type: MarkerType.ArrowClosed } : undefined;
    }

    createNodes(adjacencyList: NodeContext['adjacencyList']): NodeWithMetadata[] {
        this.computeNodeX(adjacencyList);
        this.computeNodeY();

        const nodes: NodeWithMetadata[] = [];
        nodes.push(...this.entities.map((n) => this.createNode(n, LINEAGE_ENTITY_NODE_NAME)));
        nodes.push(...this.transformations.map((n) => this.createNode(n, LINEAGE_TRANSFORMATION_NODE_NAME)));
        nodes.push(...this.workbooks.map((n) => this.createNode(n, LINEAGE_WORKBOOK_NODE_NAME)));
        nodes.push(...this.filterNodes.map((n) => this.createFilterNode(n)));
        return nodes;
    }

    createEdges(edges: NodeContext['edges']): Edge[] {
        const baseEdges: BaseEdge<LineageEdge>[] = [];
        edges.forEach((edge, edgeId) => {
            if (!edge.isDisplayed) return;
            const [upstream, downstream] = parseEdgeId(edgeId);
            if (upstream in this.nodeInformation && downstream in this.nodeInformation) {
                if (edge.via) {
                    baseEdges.push({
                        source: upstream,
                        target: edge.via,
                        markerEnd: this.#getMarker(this.nodeInformation[edge.via]),
                        data: edge,
                    });
                    baseEdges.push({
                        source: edge.via,
                        target: downstream,
                        markerEnd: this.#getMarker(this.nodeInformation[downstream]),
                        data: edge,
                    });
                } else {
                    baseEdges.push({
                        source: upstream,
                        target: downstream,
                        markerEnd: this.#getMarker(this.nodeInformation[downstream]),
                        data: edge,
                    });
                }
            }
        });
        this.filterNodes.forEach((node) => {
            if (node.direction === LineageDirection.Upstream) {
                baseEdges.push({
                    source: node.id,
                    target: node.parent,
                    markerEnd: this.#getMarker(this.nodeInformation[node.parent]),
                });
            } else {
                baseEdges.push({ source: node.parent, target: node.id, markerEnd: this.#getMarker(node) });
            }
        });
        return baseEdges.map(createEdge);
    }

    /**
     * Computes the x position of each node, by organizing them into layers.
     */
    computeNodeX(adjacencyList: NodeContext['adjacencyList']): void {
        this.topologicalNodes.forEach((node) => {
            const parentLayers = new Map<string, Layer>(
                getParents(node, adjacencyList)
                    .map((parent) => [parent, this.nodeInformation[parent]?.layer])
                    .filter((pair): pair is [string, Layer] => pair[1] !== undefined),
            );
            const minParentLayer = Array.from(parentLayers.values()).sort(compareLayers)[0] || defaultLayer;

            const { main: parentMain, mini: parentMini } = parseLayer(minParentLayer);
            if (this.#isMainNode(node)) {
                const factor = node.direction === LineageDirection.Upstream ? -1 : 1;
                const mainLayer = parentLayers.size ? factor + parentMain : 0;
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

            const positionalParents = Array.from(parentLayers.entries())
                .filter(([_p, l]) => parseLayer(l).main === parseLayer(minParentLayer).main)
                .map(([p]) => p);

            this.nodeInformation[node.id].positionalParents = new Set(positionalParents);
        });

        this.entities.forEach((node) => {
            const mainLayer = parseLayer(this.nodeInformation[node.id].layer).main;
            const factor = node.direction === LineageDirection.Upstream ? -1 : 1;

            // Transformational nodes for which this node is a positional child
            const transformationalParents = getParents(node, adjacencyList).filter((p) => {
                const { main, mini } = parseLayer(this.nodeInformation[p]?.layer);
                return main + factor === mainLayer && mini > 0;
            });

            // Only use non-transformation entities when calculating a transformation node's children
            transformationalParents.forEach((parent) => {
                if (parent) {
                    setDefault(this.transformationChildren, parent, new Set()).add(node.id);
                }
            });
        });

        this.transformations.forEach((node) => {
            if (node.direction && isQuery(node)) {
                const children = Array.from(adjacencyList[node.direction].get(node.urn) || []).filter(
                    (child) => child in this.nodeInformation,
                );
                this.transformationChildren.set(node.urn, new Set(children));
            }
        });

        const getNodeSize = (layer: Layer): number => {
            const { mini } = parseLayer(layer);
            if (mini) {
                const layerHasContainer = !!Array.from(this.layerNodes.get(layer) || []).find((id) =>
                    id.startsWith('urn:li:container'),
                );
                return layerHasContainer ? WORKBOOK_NODE_MAX_WIDTH : TRANSFORMATION_NODE_SIZE;
            }
            if (layer === defaultLayer) {
                return this.isHomeTransformational ? TRANSFORMATION_NODE_SIZE : LINEAGE_NODE_WIDTH;
            }
            return LINEAGE_NODE_WIDTH;
        };

        const upstreamLayers = Array.from(this.layerNodes.keys())
            .filter((layer) => layer.startsWith('-'))
            .sort(compareLayers);
        const downstreamLayers = Array.from(this.layerNodes.keys())
            .filter((layer) => !layer.startsWith('-'))
            .sort(compareLayers);

        this.layerPositions.set(defaultLayer, 0);
        upstreamLayers.forEach((layer, i) => {
            const { mini } = parseLayer(layer);
            const prevLayer = upstreamLayers[i - 1];
            const { mini: prevMini } = parseLayer(prevLayer || defaultLayer);
            const separation = mini || prevMini ? MINI_X_SEP : MAIN_X_SEP;
            this.layerPositions.set(layer, (this.layerPositions.get(prevLayer) || 0) - getNodeSize(layer) - separation);
        });
        downstreamLayers.forEach((layer, i) => {
            if (i === 0) {
                return;
            }
            const { mini } = parseLayer(layer);
            const prevLayer = downstreamLayers[i - 1];
            const { mini: prevMini } = parseLayer(prevLayer || defaultLayer);
            const separation = mini || prevMini ? MINI_X_SEP : MAIN_X_SEP;
            this.layerPositions.set(
                layer,
                (this.layerPositions.get(prevLayer) || 0) + getNodeSize(prevLayer) + separation,
            );
        });

        console.log(this);
    }

    addNodeToLayer(node: LineageNode, layer: Layer): void {
        this.nodeInformation[node.id].layer = layer;
        setDefault(this.layerNodes, layer, new Set()).add(node.id);
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
            const nodeHeight = mini ? MINI_Y_SEP + TRANSFORMATION_NODE_SIZE : MAIN_Y_SEP + LINEAGE_NODE_HEIGHT;
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
                    if (!relatives.length && id !== this.homeUrn) {
                        console.log(`MISSING RELATIVES: ${id}`);
                    }
                    const relativesY = relatives
                        .map((p) => this.nodeInformation[p].y)
                        .filter((y): y is number => y !== undefined);
                    nodeY = relativesY.length ? relativesY.reduce((a, b) => a + b) / relativesY.length : 0;
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

        // Offset transformation nodes
        this.transformations.forEach((node) => {
            const info = this.nodeInformation[node.id];
            if (info.y !== undefined) info.y += 20;
        });
    }

    createNode<T extends LineageNode>(node: T, type: string, transformData = (v: T) => v): NodeWithMetadata {
        const info = this.nodeInformation[node.id];
        const layer = info.layer || '';
        return {
            type,
            id: node.id,
            position: {
                x: this.layerPositions.get(layer) || 0,
                y: info.y || 0,
            },
            layer: parseLayer(layer).main,
            data: transformData(node),
        };
    }

    createFilterNode(filter: LineageFilter): NodeWithMetadata {
        return this.createNode(filter, LINEAGE_FILTER_NODE_NAME, (node) => ({
            ...node,
            numShown: Array.from(node.contents).filter((urn) => urn in this.nodeInformation).length,
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

function createEdge<T>(edge: BaseEdge<T>): Edge {
    return {
        ...edge,
        id: createEdgeId(edge.source, edge.target),
        type: LINEAGE_TABLE_EDGE_NAME,
    };
}
