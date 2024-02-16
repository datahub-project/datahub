import { EdgeMarker } from '@reactflow/core/dist/esm/types/edges';
import { Edge, MarkerType, Node } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import {
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    Path,
    setDefault,
    TRANSFORMATION_TYPES,
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
type BaseEdge = Pick<Edge, 'source' | 'target' | 'markerEnd'>;

type Layer = string; // [main (entity) layer, mini (transformation) layer]
const defaultLayer = '0.0';

function createLayer(main: number, mini: number): Layer {
    // toLocaleString preserves negative 0, used for transformational nodes directly upstream of home node
    return `${main.toLocaleString()}.${mini}`;
}

function parseLayer(layer?: Layer): [number, number] {
    if (!layer) {
        return parseLayer(defaultLayer);
    }
    const [main, mini] = layer.split('.');
    return [parseInt(main, 10), parseInt(mini, 10)];
}

export default class NodeBuilder {
    homeUrn: string;

    isHomeTransformational: boolean;

    entities: LineageEntity[] = [];

    transformations: LineageEntity[] = [];

    workbooks: LineageEntity[] = [];

    filterNodes: Map<string, LineageFilter[]> = new Map();

    layerPositions = new Map<Layer, number>();

    layerNodes = new Map<Layer, Set<string>>();

    nodeParents = new Map<string, Set<string>>();

    nodeLayers = new Map<string, Layer>();

    nodeY = new Map<string, number>();

    // Note: Relies on the fact that transformation node id == urn
    transformationChildren = new Map<string, Set<string>>();

    constructor(homeUrn: string, homeType: EntityType, nodes: LineageNode[]) {
        this.homeUrn = homeUrn;
        this.isHomeTransformational = TRANSFORMATION_TYPES.includes(homeType);
        nodes.forEach((node) => {
            if (node.type === LINEAGE_FILTER_TYPE) {
                setDefault(this.filterNodes, node.parent, []).push(node);
            } else if (node.type === EntityType.Container) {
                this.workbooks.push(node);
            } else if (TRANSFORMATION_TYPES.includes(node.type)) {
                this.transformations.push(node);
            } else {
                this.entities.push(node);
            }
        });
    }

    createNodes(): NodeWithMetadata[] {
        this.computeNodeX();
        this.computeNodeY();

        const nodes: NodeWithMetadata[] = [];
        nodes.push(...this.entities.map((n) => this.createNode(n, LINEAGE_ENTITY_NODE_NAME)));
        nodes.push(...this.transformations.map((n) => this.createNode(n, LINEAGE_TRANSFORMATION_NODE_NAME)));
        nodes.push(...this.workbooks.map((n) => this.createNode(n, LINEAGE_WORKBOOK_NODE_NAME)));
        nodes.push(
            ...Array.from(this.filterNodes.values())
                .flat()
                .map((n) => this.createNode(n, LINEAGE_FILTER_NODE_NAME)),
        );
        return nodes;
    }

    createEdges(): Edge[] {
        const baseEdges: BaseEdge[] = [];
        [...this.entities, ...this.transformations, ...this.workbooks].forEach((node) => {
            node.paths.forEach((path) => {
                if (path.length > 0) {
                    const parent = path[path.length - 1];
                    if (node.direction === LineageDirection.Upstream) {
                        baseEdges.push({ source: node.urn, target: parent.urn, markerEnd: getMarker(parent) });
                    } else {
                        baseEdges.push({ source: parent.urn, target: node.urn, markerEnd: getMarker(node) });
                    }
                }
            });
        });
        Array.from(this.filterNodes.values())
            .flat()
            .forEach((node) => {
                if (node.direction === LineageDirection.Upstream) {
                    baseEdges.push({ source: node.id, target: node.parent, markerEnd: getMarker(node.parentNode) });
                } else {
                    baseEdges.push({ source: node.parent, target: node.id, markerEnd: getMarker(node) });
                }
            });

        // TODO: Come up with a cleaner solution here
        return Array.from(new Set(baseEdges.map((edge) => JSON.stringify(edge)))).map((edge) =>
            createEdge(JSON.parse(edge)),
        );
    }

    /**
     * Computes the x position of each node, by organizing them into layers.
     */
    computeNodeX(): void {
        // Compute main layers, parents, and children
        this.entities.forEach((node) => {
            const factor = node.direction === LineageDirection.Upstream ? -1 : 1;
            const paths = node.paths.map((path) =>
                path.filter((n) => n.urn === this.homeUrn || !TRANSFORMATION_TYPES.includes(n.type)),
            );
            const minPathLength = Math.min(...paths.map((path) => path.length));
            this.addNodeToLayer(node, createLayer(minPathLength * factor, 0));

            const parents = paths
                .filter((path) => path.length && path.length === minPathLength)
                .map((path) => path[path.length - 1].urn);
            this.nodeParents.set(node.id, new Set(parents));

            // Only use non-transformation entities when calculating a transformation node's children
            const allParents = node.paths
                .filter((path) => path.filter((n) => !TRANSFORMATION_TYPES.includes(n.type)).length === minPathLength)
                .map((path) => {
                    const reversed = [...path];
                    reversed.reverse();
                    const excludeIndex = reversed.findIndex((n) => !TRANSFORMATION_TYPES.includes(n.type));
                    return reversed.slice(0, excludeIndex);
                })
                .flat();
            allParents.forEach((parent) => {
                if (parent) {
                    setDefault(this.transformationChildren, parent.urn, new Set()).add(node.id);
                }
            });

            const filterNodes = this.filterNodes.get(node.id);
            if (filterNodes?.length) {
                filterNodes.forEach((sm) => {
                    const smFactor = sm.direction === LineageDirection.Upstream ? -1 : 1;
                    this.addNodeToLayer(sm, createLayer((minPathLength + 1) * smFactor, 0));
                    this.nodeParents.set(sm.id, new Set([node.urn]));
                });
            }
        });

        // Compute mini layers
        [...this.transformations, ...this.workbooks].forEach((node) => {
            const parentLayers = node.paths.map((p) => this.calculateTransformationLayer(p, node.direction));
            const layer = parentLayers.sort(compareLayers)[0] || defaultLayer;
            const [mainLayer] = parseLayer(layer);
            this.addNodeToLayer(node, layer);

            const parents = parentLayers
                .map<[Layer, Path[]]>((lyr, i) => [lyr, node.paths[i]])
                .filter(([lyr]) => parseLayer(lyr)[0] === mainLayer)
                .map(([_lyr, path]) => path.filter((n) => !TRANSFORMATION_TYPES.includes(n.type)))
                .filter((path) => path.length)
                .map((path) => path[path.length - 1].urn);
            this.nodeParents.set(node.id, new Set(parents));

            const filterNodes = this.filterNodes.get(node.id);

            if (filterNodes?.length) {
                filterNodes.forEach((sm) => {
                    const factor = sm.direction === LineageDirection.Upstream ? -1 : 1;
                    this.addNodeToLayer(
                        sm,
                        createLayer(
                            (Math.abs(mainLayer) + 1) * factor,

                            0,
                        ),
                    );
                    this.nodeParents.set(sm.id, new Set([node.urn]));
                });
            }
        });

        const getNodeSize = (layer: Layer): number => {
            const [, mini] = parseLayer(layer);
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
            const [, mini] = parseLayer(layer);
            const prevLayer = upstreamLayers[i - 1];
            const [, prevMini] = parseLayer(prevLayer || defaultLayer);
            const separation = mini || prevMini ? MINI_X_SEP : MAIN_X_SEP;
            this.layerPositions.set(layer, (this.layerPositions.get(prevLayer) || 0) - getNodeSize(layer) - separation);
        });
        downstreamLayers.forEach((layer, i) => {
            if (i === 0) {
                return;
            }
            const [, mini] = parseLayer(layer);
            const prevLayer = downstreamLayers[i - 1];
            const [, prevMini] = parseLayer(prevLayer || defaultLayer);
            const separation = mini || prevMini ? MINI_X_SEP : MAIN_X_SEP;
            this.layerPositions.set(
                layer,
                (this.layerPositions.get(prevLayer) || 0) + getNodeSize(prevLayer) + separation,
            );
        });

        console.log(this);
    }

    addNodeToLayer(node: LineageNode, layer: Layer): void {
        this.nodeLayers.set(node.id, layer);
        setDefault(this.layerNodes, layer, new Set()).add(node.id);
    }

    /**
     * Transformation nodes are place between entity layers, as "mini" layers.
     * This method computes the layer of a transformation node for a single path.
     * @param path One path of the transformation node
     * @param direction The direction of the transformation node
     */
    calculateTransformationLayer(path: Path[], direction?: LineageDirection): Layer {
        for (let i = path.length - 1; i >= 0; i--) {
            const node = path[i];
            if (!TRANSFORMATION_TYPES.includes(node.type)) {
                let mainLayer = parseLayer(this.nodeLayers.get(node.urn))[0];
                if (mainLayer === 0 && direction === LineageDirection.Upstream) {
                    mainLayer = -0;
                }
                return createLayer(mainLayer, path.length - i);
            }
        }
        return defaultLayer;
    }

    /**
     * Computes the y position of each node, placing them between their parents' y positions,
     *   while maintaining a minimum separation between nodes. Results in a tree-like structure.
     * Must be called after computeNodeX.
     */
    computeNodeY(): void {
        const sortedLayers = Array.from(this.layerPositions.keys()).sort(compareLayersMinisLast);
        sortedLayers.forEach((layer) => {
            const [, mini] = parseLayer(layer);
            const nodeHeight = mini ? MINI_Y_SEP + TRANSFORMATION_NODE_SIZE : MAIN_Y_SEP + LINEAGE_NODE_HEIGHT;
            const nodes = this.layerNodes.get(layer) || new Set();
            const goalY: Record<string, number> = {};

            // Set initial position
            nodes.forEach((id) => {
                let nodeY: number;
                if (layer === defaultLayer) {
                    nodeY = 0;
                } else {
                    const relatives: string[] = Array.from(this.nodeParents.get(id) || []);
                    if (mini) {
                        relatives.push(...(this.transformationChildren.get(id) || []));
                    }
                    if (!relatives.length && id !== this.homeUrn) {
                        console.log(`MISSING RELATIVES: ${id}`);
                    }
                    const relativesY = relatives
                        .map((p) => this.nodeY.get(p))
                        .filter((y): y is number => y !== undefined);
                    nodeY = relativesY.length ? relativesY.reduce((a, b) => a + b) / relativesY.length : 0;
                }
                goalY[id] = nodeY;
                this.nodeY.set(id, nodeY);
            });

            if (nodes.size < 2) {
                return;
            }

            const sortedNodes = Array.from(nodes).sort((idA, idB) => goalY[idA] - goalY[idB] || idA.localeCompare(idB));
            const getY = (idx: number): number => {
                const id = sortedNodes[idx];
                const val = this.nodeY.get(id);
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
                        this.nodeY.set(sortedNodes[k], leftBound + (k - i) * nodeHeight);
                    }
                    break;
                } else if (getY(j) < rightBound) {
                    ySum += goalY[sortedNodes[j]];
                    j++;
                } else {
                    for (let k = i; k < j; k++) {
                        this.nodeY.set(sortedNodes[k], leftBound + (k - i) * nodeHeight);
                    }
                    i = j;
                    j = i + 1;
                    ySum = goalY[sortedNodes[i]];
                }
            }
        });

        // Offset transformation nodes
        this.transformations.forEach((node) => {
            const prevY = this.nodeY.get(node.id);
            if (prevY !== undefined) {
                this.nodeY.set(node.id, prevY + 20);
            }
        });
        this.workbooks.forEach((node) => {
            const prevY = this.nodeY.get(node.id);
            if (prevY !== undefined) {
                this.nodeY.set(node.id, prevY + 12.5);
            }
        });
    }

    createNode(data: LineageNode, type: string): NodeWithMetadata {
        const layer = this.nodeLayers.get(data.id) || '';
        return {
            type,
            id: data.id,
            position: {
                x: this.layerPositions.get(layer) || 0,
                y: this.nodeY.get(data.id) || 0,
            },
            layer: parseLayer(layer)[0],
            data,
        };
    }
}

function compareLayers(a: Layer, b: Layer): number {
    const [aMain, aMini] = parseLayer(a);
    const [bMain, bMini] = parseLayer(b);
    return Math.abs(aMain) - Math.abs(bMain) || aMini - bMini;
}

function compareLayersMinisLast(a: Layer, b: Layer): number {
    const [, aMini] = parseLayer(a);
    const [, bMini] = parseLayer(b);
    if (aMini && !bMini) {
        return 1;
    }
    if (bMini && !aMini) {
        return -1;
    }
    return compareLayers(a, b);
}

function getMarker({ type }: Pick<LineageNode, 'type'>): EdgeMarker | undefined {
    return TRANSFORMATION_TYPES.includes(type as EntityType) ? undefined : { type: MarkerType.ArrowClosed };
}

export function createEdgeId(source: string, target: string): string {
    return `${source}-${target}`;
}

function createEdge(edge: BaseEdge): Edge {
    return {
        ...edge,
        id: createEdgeId(edge.source, edge.target),
        type: LINEAGE_TABLE_EDGE_NAME,
    };
}
