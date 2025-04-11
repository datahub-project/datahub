import {
    createEdgeId,
    getEdgeId,
    isUrnDataProcessInstance,
    isUrnTransformational,
    NodeContext,
} from '@app/lineageV2/common';
import { EntityType, LineageDirection } from '@types';

enum HideOption {
    TRANSFORMATIONS = 'transformations',
    DATA_PROCESS_INSTANCES = 'dataProcessInstances',
}

const hideOptionIncludeUrnFunctions: Record<HideOption, (urn: string, rootType: EntityType) => boolean> = {
    [HideOption.TRANSFORMATIONS]: isUrnTransformational,
    [HideOption.DATA_PROCESS_INSTANCES]: isUrnDataProcessInstance,
};

/**
 * Remove direct edges between non-transformational nodes, if there is a path between them through a transformational node.
 * Remove direct edges between non-data process instances, if there is a path between them through data process instances.
 * This prevents the graph from being cluttered with effectively duplicate edges.
 * @param urn Urn for which to remove parent edges.
 * @param direction Direction to look for parents.
 * @param context Lineage node context.
 */
export default function pruneAllDuplicateEdges(
    urn: string,
    direction: LineageDirection | null,
    context: Pick<NodeContext, 'adjacencyList' | 'edges' | 'setDisplayVersion' | 'rootType'>,
) {
    let changed = false;
    Object.values(HideOption).forEach((hideOption) => {
        changed ||= pruneDuplicateEdges(urn, direction, hideOption, context);
    });
    if (changed) {
        context.setDisplayVersion(([version, nodes]) => [version + 1, nodes]);
    }
}

/**
 * Remove direct edges between a certain set of "excluded" nodes, if there is a path between them through only "included" nodes.
 */
export function pruneDuplicateEdges(
    urn: string,
    direction: LineageDirection | null,
    hideOption: HideOption,
    context: Pick<NodeContext, 'adjacencyList' | 'edges' | 'rootType'>,
): boolean {
    const { edges, rootType } = context;
    const neighbors: Record<LineageDirection, Set<string>> = {
        [LineageDirection.Downstream]: new Set(),
        [LineageDirection.Upstream]: new Set(),
    };

    const includeUrn = hideOptionIncludeUrnFunctions[hideOption];
    const isUrnIncluded = includeUrn(urn, rootType);

    function getNeighbors(d: LineageDirection) {
        return getNeighborsByFunction(urn, d, includeUrn, context);
    }

    if (direction) {
        neighbors[direction] = getNeighbors(direction);
    } else {
        neighbors[LineageDirection.Upstream] = getNeighbors(LineageDirection.Upstream);
        neighbors[LineageDirection.Downstream] = getNeighbors(LineageDirection.Downstream);
    }

    let changed = false;
    if (isUrnIncluded) {
        neighbors[LineageDirection.Upstream].forEach((source) => {
            neighbors[LineageDirection.Downstream].forEach((destination) => {
                const edge = edges.get(createEdgeId(source, destination));
                if (edge?.isDisplayed) {
                    edge.isDisplayed = false;
                    changed = true;
                }
            });
        });
    } else {
        Object.values(LineageDirection).forEach((d) => {
            neighbors[d].forEach((source) => {
                const edge = edges.get(getEdgeId(urn, source, d));
                if (edge?.isDisplayed) {
                    edge.isDisplayed = false;
                    changed = true;
                }
            });
        });
    }

    return changed;
}

/**
 * Get the non-transformational nodes that are reachable from `urn` in `direction` via a transformational path.
 * @param urn Urn for which to get neighbors.
 * @param direction Direction to look for neighbors.
 * @param includeUrn Function to determine if a node should be included, based on its urn.
 * @param adjacencyList Adjacency list of the lineage graph.
 * @param rootType Entity type of the home node.
 */
function getNeighborsByFunction(
    urn: string,
    direction: LineageDirection,
    includeUrn: (urn: string, rootType: EntityType) => boolean,
    { adjacencyList, rootType }: Pick<NodeContext, 'adjacencyList' | 'rootType'>,
) {
    const neighbors = new Set<string>();
    // If urn is included, then direct neighbors can be included
    const stack = includeUrn(urn, rootType)
        ? [urn]
        : Array.from(adjacencyList[direction].get(urn) || []).filter((p) => includeUrn(p, rootType));
    const seen = new Set<string>(stack);
    for (let u = stack.pop(); u; u = stack.pop()) {
        Array.from(adjacencyList[direction].get(u) || []).forEach((parent) => {
            if (includeUrn(parent, rootType)) {
                if (!seen.has(parent)) {
                    stack.push(parent);
                    seen.add(parent);
                }
            } else {
                neighbors.add(parent);
            }
        });
    }
    return neighbors;
}
