import isEqual from 'lodash/isEqual';

import { FacetFilterInput } from '@types';

const getFieldFilters = (filters: FacetFilterInput[], field: string) => filters.filter((f) => f.field === field);

/**
 * Applies the fields that `next` changes relative to `base` on top of `current`.
 *
 * Filter menus compute their new filters from the filters they were rendered with. When several
 * commit together (the submenus of More, on close), each result is built from the same `base`, so
 * applying them one after another would drop all but the last change. Merging by field keeps each.
 */
export function mergeFilterChanges(
    base: FacetFilterInput[],
    current: FacetFilterInput[],
    next: FacetFilterInput[],
): FacetFilterInput[] {
    const fields = new Set([...base, ...next].map((f) => f.field));
    const changedFields = [...fields].filter(
        (field) => !isEqual(getFieldFilters(base, field), getFieldFilters(next, field)),
    );
    if (!changedFields.length) return current;

    return [
        ...current.filter((f) => !changedFields.includes(f.field)),
        ...next.filter((f) => changedFields.includes(f.field)),
    ];
}
