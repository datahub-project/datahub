import { LineageEntity, LineageFilter, isTransformational } from '@app/lineageV3/common';

import { EntityType } from '@types';

/**
 * Resolve the graph node that a `getBulkEntityLineageV2` result belongs to.
 *
 * Results come back positionally and 1:1 with the requested urns. A neighbor the caller
 * isn't permitted to view is returned as a Restricted placeholder whose urn is
 * re-encrypted server-side (`urn:li:restricted:v2:...`), so it no longer matches the node
 * key we requested it under — a direct `nodes.get(resultUrn)` misses and the node is left
 * as a perpetual loading skeleton. For Restricted results, fall back to matching by request
 * position so the node renders as a Restricted node instead of hanging.
 *
 * The positional fallback is scoped to Restricted results (and requires the requested-urn
 * list to line up with the response) so a stale/mismatched batch can never attach a real
 * entity to the wrong node.
 */
export function getNodeForBulkResult(
    nodes: Map<string, LineageEntity>,
    resultUrn: string,
    resultType: EntityType,
    requestedUrns: string[],
    index: number,
): LineageEntity | undefined {
    const direct = nodes.get(resultUrn);
    if (direct) return direct;

    if (resultType === EntityType.Restricted) {
        const requestedUrn = requestedUrns[index];
        if (requestedUrn) return nodes.get(requestedUrn);
    }
    return undefined;
}

/**
 * Transformational leaves among each lineage filter's paginated children that are not shown and
 * not yet fetched. Search across lineage may not return the edges beyond a transformation, e.g.
 * from a dbt source to its warehouse table when that table is reached through a query instead.
 * Fetching the transformation adds those edges, so it is no longer counted as a separate child.
 *
 * @param lineageFilters Pagination state of each node with filtered-out children.
 * @param nodes All nodes in the graph.
 * @param rootType Entity type of the home node.
 * @param shownUrns Urns of displayed nodes, which are fetched anyway.
 */
export function getUnfetchedTransformationalLeaves(
    lineageFilters: Map<string, LineageFilter>,
    nodes: Map<string, LineageEntity>,
    rootType: EntityType,
    shownUrns: string[],
): string[] {
    const shown = new Set(shownUrns);
    const result = new Set<string>();
    lineageFilters.forEach((filter) => {
        filter.contents.forEach((urn) => {
            const node = nodes.get(urn);
            if (node && !node.entity && !shown.has(urn) && isTransformational(node, rootType)) {
                result.add(urn);
            }
        });
    });
    return Array.from(result);
}
