import { AndFilterInput, FacetFilterInput } from '../../../types.generated';
import { UnionType } from './constants';
import { generateOrFilters } from './generateOrFilters';

// todo - what if someone had a NOT hive in the url and we selected looker
// then we'll end up setting a new platform value, right? which may not be desired
// should we also match on the value of the filter?
// mainly we just need to ensure that our filter overrides what logic is currently up there

// Swap in new overrides at the same filter positions, with the remainder at the end
export const applyFacetFilterOverrides = (filters: Array<FacetFilterInput>, overrides: Array<FacetFilterInput>) => {
    const overrideMap = overrides.reduce((map, override) => {
        // todo - should we also match on the override value too? just worried about always forcing a swap based on field (ie. all platforms get cleared)
        if (!map.has(override.field)) map.set(override.field, override);
        return map;
    }, new Map<string, FacetFilterInput>());

    const added = new Set<string>();

    const updatedFilters = filters.map((filter) => {
        const override = overrideMap.get(filter.field);
        if (override) {
            added.add(override.field);
            return override;
        }
        return filter;
    });

    const remainder = overrides.filter((override) => !added.has(override.field));
    updatedFilters.push(...remainder);

    return updatedFilters;
};

export const applyOrFilterOverrides = (orFilters: Array<AndFilterInput>, overrides: Array<FacetFilterInput>) => {
    if (!orFilters.length) return generateOrFilters(UnionType.AND, overrides);

    return orFilters.map((orFilter) => ({
        and: applyFacetFilterOverrides(orFilter.and ?? [], overrides),
    }));
};
