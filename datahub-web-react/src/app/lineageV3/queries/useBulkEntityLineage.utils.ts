import { LineageEntity } from '@app/lineageV3/common';

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
