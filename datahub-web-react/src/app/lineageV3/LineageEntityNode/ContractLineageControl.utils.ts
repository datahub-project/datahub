import { LineageFilter } from '@app/lineageV3/common';

type FilterCounts = {
    shown: number;
    total: number;
};

/**
 * Counts for the "shown / total" badge, in the same units `ShowMoreButton` paginates over:
 * `filter.contents`, the non-transformational children and transformational leaves. Transformations
 * passed through on the way to those children (e.g. dbt sources in front of their warehouse
 * tables) are excluded, as they are not paginated themselves.
 *
 * @param filter Pagination state of the parent node in one direction.
 * @param apiTotal Number of direct children reported by the lineage search, if fetched. It also
 *        counts transformations passed through, so it is only used when there are none.
 * @param showGhostEntities Whether ghost entities are shown, in which case they are counted locally.
 */
export function getFilterCounts(
    filter: Pick<LineageFilter, 'contents' | 'shown' | 'allChildren'>,
    apiTotal: number | undefined,
    showGhostEntities: boolean,
): FilterCounts {
    const localTotal = filter.contents.length;
    const hasTransformations = filter.allChildren.size !== localTotal;
    const total = showGhostEntities || hasTransformations ? localTotal : (apiTotal ?? localTotal);
    const shown = filter.contents.filter((urn) => filter.shown.has(urn)).length;
    return { shown: Math.min(shown, total), total };
}
