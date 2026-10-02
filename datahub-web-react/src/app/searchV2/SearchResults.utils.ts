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

export function lineageCountsByUrn(
    entities: ReadonlyArray<LineageCountEntity> | null | undefined,
): Map<string, SearchResultLineageCounts> {
    const counts = new Map<string, SearchResultLineageCounts>();
    entities?.forEach((entity) => {
        if (!entity?.urn) return;
        if (entity.upstream == null && entity.downstream == null) return;
        counts.set(entity.urn, {
            urn: entity.urn,
            upstream: entity.upstream,
            downstream: entity.downstream,
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
