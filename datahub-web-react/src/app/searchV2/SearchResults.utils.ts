import { SearchResultLineageCounts } from '@app/sharedV2/EntitySidebarContext';

import { Entity } from '@types';

export type SearchEntityWithLineage = Entity & {
    upstream?: SearchResultLineageCounts['upstream'];
    downstream?: SearchResultLineageCounts['downstream'];
    siblingsSearch?: { total?: number | null } | null;
};

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
