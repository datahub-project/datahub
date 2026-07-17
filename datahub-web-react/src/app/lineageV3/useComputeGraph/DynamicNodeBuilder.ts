import type { XYPosition } from '@reactflow/core/dist/esm/types';

import { setDefault } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

export interface DynamicSizeNode {
    id: string;
    width: number;
    height: number;
    /** Extra space reserved above the node, e.g. for a bounding box label rendered above it. */
    topMargin?: number;
    /** Nodes with lower rank are placed higher within their layer. */
    rank?: number;
    /** Side of the root to place this node on, when lineage does not determine one. */
    defaultDirection?: LineageDirection;
}

export interface DynamicSizeEdge {
    source: string;
    target: string;
}

interface LayoutOptions {
    layerSeparation: number;
    nodeSeparation: number;
}

/**
 * Positions nodes of dynamic sizes into layers around a root node, akin to NodeBuilder,
 * but supports per-node widths and heights and is only approximate in its positioning:
 * layers are placed left to right based on the widest node in each layer, and nodes within
 * a layer are stacked top down (ordered by `rank`), rather than being aligned with their neighbors.
 *
 * A node is placed downstream (right) of the root if it is reachable from the root,
 * and upstream (left) if the root is reachable from it. If both, the side with more direct
 * edges to the root wins, defaulting to downstream on a tie. If neither, `defaultDirection`
 * is used; nodes without one are stacked below the root.
 */
export default class DynamicNodeBuilder {
    rootId: string;

    nodes: DynamicSizeNode[];

    edgeCounts = new Map<string, Map<string, number>>();

    outgoing = new Map<string, Set<string>>();

    incoming = new Map<string, Set<string>>();

    options: LayoutOptions;

    constructor(rootId: string, nodes: DynamicSizeNode[], edges: DynamicSizeEdge[], options: LayoutOptions) {
        this.rootId = rootId;
        this.nodes = nodes;
        this.options = options;

        const nodeIds = new Set(nodes.map((node) => node.id));
        edges.forEach(({ source, target }) => {
            if (!nodeIds.has(source) || !nodeIds.has(target) || source === target) return;
            const counts = setDefault(this.edgeCounts, source, new Map());
            counts.set(target, (counts.get(target) || 0) + 1);
            setDefault(this.outgoing, source, new Set()).add(target);
            setDefault(this.incoming, target, new Set()).add(source);
        });
    }

    computePositions(): Map<string, XYPosition> {
        const sides = this.#resolveSides();
        const layers = this.#assignLayers(sides);
        return this.#positionNodes(layers);
    }

    #bfs(neighbors: Map<string, Set<string>>): Set<string> {
        const visited = new Set<string>([this.rootId]);
        const queue = [this.rootId];
        for (let i = 0; i < queue.length; i++) {
            neighbors.get(queue[i])?.forEach((neighbor) => {
                if (!visited.has(neighbor)) {
                    visited.add(neighbor);
                    queue.push(neighbor);
                }
            });
        }
        visited.delete(this.rootId);
        return visited;
    }

    #resolveSides(): Map<string, LineageDirection> {
        const sides = new Map<string, LineageDirection>();
        const reachableDownstream = this.#bfs(this.outgoing);
        const reachableUpstream = this.#bfs(this.incoming);

        this.nodes.forEach((node) => {
            if (node.id === this.rootId) return;
            const isDownstream = reachableDownstream.has(node.id);
            const isUpstream = reachableUpstream.has(node.id);

            let side: LineageDirection | undefined;
            if (isDownstream && isUpstream) {
                const downstreamEdges = this.edgeCounts.get(this.rootId)?.get(node.id) || 0;
                const upstreamEdges = this.edgeCounts.get(node.id)?.get(this.rootId) || 0;
                side = upstreamEdges > downstreamEdges ? LineageDirection.Upstream : LineageDirection.Downstream;
            } else if (isDownstream) {
                side = LineageDirection.Downstream;
            } else if (isUpstream) {
                side = LineageDirection.Upstream;
            } else {
                side = node.defaultDirection;
            }
            if (side) {
                sides.set(node.id, side);
            }
        });
        return sides;
    }

    /**
     * Assigns each node a layer, its shortest-path distance from the root along its side,
     * only traversing through nodes on the same side. Downstream layers are positive,
     * upstream layers negative. Nodes without a resolved side share layer 0 with the root.
     */
    #assignLayers(sides: Map<string, LineageDirection>): Map<string, number> {
        const layers = new Map<string, number>([[this.rootId, 0]]);

        const traverse = (neighbors: Map<string, Set<string>>, side: LineageDirection, step: number) => {
            const queue = [this.rootId];
            for (let i = 0; i < queue.length; i++) {
                const layer = layers.get(queue[i]) ?? 0;
                neighbors.get(queue[i])?.forEach((neighbor) => {
                    if (!layers.has(neighbor) && sides.get(neighbor) === side) {
                        layers.set(neighbor, layer + step);
                        queue.push(neighbor);
                    }
                });
            }
        };
        traverse(this.outgoing, LineageDirection.Downstream, 1);
        traverse(this.incoming, LineageDirection.Upstream, -1);

        // Nodes on a side but not reachable through same-side nodes go in the closest layer
        this.nodes.forEach((node) => {
            if (layers.has(node.id)) return;
            const side = sides.get(node.id);
            if (side === LineageDirection.Downstream) layers.set(node.id, 1);
            else if (side === LineageDirection.Upstream) layers.set(node.id, -1);
            else layers.set(node.id, 0);
        });

        return layers;
    }

    #positionNodes(layers: Map<string, number>): Map<string, XYPosition> {
        const { layerSeparation, nodeSeparation } = this.options;
        const nodesByLayer = new Map<number, DynamicSizeNode[]>();
        this.nodes.forEach((node) => {
            setDefault(nodesByLayer, layers.get(node.id) ?? 0, []).push(node);
        });

        const layerWidths = new Map<number, number>();
        nodesByLayer.forEach((layerNodes, layer) => {
            layerWidths.set(layer, Math.max(...layerNodes.map((node) => node.width)));
        });

        const sortedLayers = Array.from(nodesByLayer.keys()).sort((a, b) => a - b);
        const layerX = new Map<number, number>([[0, 0]]);
        sortedLayers
            .filter((layer) => layer > 0)
            .forEach((layer, i, downstreamLayers) => {
                const prevLayer = i === 0 ? 0 : downstreamLayers[i - 1];
                layerX.set(layer, (layerX.get(prevLayer) || 0) + (layerWidths.get(prevLayer) || 0) + layerSeparation);
            });
        sortedLayers
            .filter((layer) => layer < 0)
            .reverse()
            .forEach((layer, i, upstreamLayers) => {
                const prevLayer = i === 0 ? 0 : upstreamLayers[i - 1];
                layerX.set(layer, (layerX.get(prevLayer) || 0) - layerSeparation - (layerWidths.get(layer) || 0));
            });

        const positions = new Map<string, XYPosition>();
        nodesByLayer.forEach((layerNodes, layer) => {
            const x = layerX.get(layer) || 0;
            const width = layerWidths.get(layer) || 0;
            layerNodes.sort(
                (a, b) =>
                    Number(b.id === this.rootId) - Number(a.id === this.rootId) ||
                    (a.rank ?? 0) - (b.rank ?? 0) ||
                    a.id.localeCompare(b.id),
            );

            let y = 0;
            layerNodes.forEach((node) => {
                y += node.topMargin ?? 0;
                // Upstream layers hug the root: right-align nodes within their layer
                positions.set(node.id, { x: layer < 0 ? x + width - node.width : x, y });
                y += node.height + nodeSeparation;
            });
        });
        return positions;
    }
}
