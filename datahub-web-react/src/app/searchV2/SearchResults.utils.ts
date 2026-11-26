import { SearchResultLineageCounts } from '@app/sharedV2/EntitySidebarContext';

import { Entity } from '@types';

export type SearchEntityWithLineage = Entity & {
    upstream?: SearchResultLineageCounts['upstream'];
    downstream?: SearchResultLineageCounts['downstream'];
    siblingsSearch?: { total?: number | null } | null;
};

export type LineageCountEntity = {
    urn: string;
    upstream?: SearchResultLineageCounts['upstream'];
    downstream?: SearchResultLineageCounts['downstream'];
} | null;

const EMPTY_LINEAGE_COUNT = { total: 0, filtered: 0 };

/**
 * Index complete count pairs by URN. Partial GraphQL payloads (one direction null under
 * errorPolicy) are skipped so callers do not treat them as settled. Pass `requestedUrns`
 * only after the deferred query has finished for the current URN set — missing,
 * incomplete, or null entity slots then get explicit zero counts so the footer can
 * render the badge and the sidebar does not re-fetch. Auth-denied / missing entities
 * arrive as null slots and settle the same as a true empty lineage graph.
 */
export function lineageCountsByUrn(
    entities: ReadonlyArray<LineageCountEntity> | null | undefined,
    requestedUrns?: ReadonlyArray<string>,
): Map<string, SearchResultLineageCounts> {
    const counts = new Map<string, SearchResultLineageCounts>();
    entities?.forEach((entity) => {
        if (!entity?.urn) return;
        if (entity.upstream == null || entity.downstream == null) return;
        counts.set(entity.urn, {
            urn: entity.urn,
            upstream: entity.upstream,
            downstream: entity.downstream,
        });
    });
    requestedUrns?.forEach((urn) => {
        if (!urn || counts.has(urn)) return;
        counts.set(urn, {
            urn,
            upstream: EMPTY_LINEAGE_COUNT,
            downstream: EMPTY_LINEAGE_COUNT,
        });
    });
    return counts;
}

export function applySearchResultLineageCounts<T extends { entity: { urn: string } }>(
    results: T[],
    countsByUrn: ReadonlyMap<string, SearchResultLineageCounts>,
): T[] {
    if (countsByUrn.size === 0) return results;
    return results.map((result) => {
        const counts = countsByUrn.get(result.entity.urn);
        if (!counts || (counts.upstream == null && counts.downstream == null)) return result;
        return {
            ...result,
            entity: {
                ...result.entity,
                upstream: counts.upstream,
                downstream: counts.downstream,
            },
        };
    });
}

export function getSearchResultLineage(entity?: SearchEntityWithLineage | null): SearchResultLineageCounts | null {
    if (!entity?.urn) {
        return null;
    }
    // Combined sibling cards can rewrite urn while leaving the other sibling's lineage
    // fields in place. The compact profile already hides this section for siblings.
    if ((entity.siblingsSearch?.total || 0) > 0) {
        return null;
    }
    if (entity.upstream == null && entity.downstream == null) {
        return null;
    }
    return {
        urn: entity.urn,
        upstream: entity.upstream,
        downstream: entity.downstream,
    };
}
