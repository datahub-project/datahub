import { AndFilterInput, FacetFilterInput } from '../../../types.generated';
import { UnionType } from './constants';
import { generateOrFilters } from './generateOrFilters';

// Swap in new overrides at the same filter positions, with the remainder at the end
// Assumes we find matches just by field name, so if multiple filters for the same field were passed, we replace those
export const applyFacetFilterOverrides = (filters: Array<FacetFilterInput>, overrides: Array<FacetFilterInput>) => {
    const added = new Set<string>();
    const overrideMap = new Map<string, FacetFilterInput>();

    overrides.forEach((override) => {
        if (!overrideMap.has(override.field)) overrideMap.set(override.field, override);
    });

    return filters
        .map((filter) => {
            const override = overrideMap.get(filter.field);
            if (!override) return filter;
            added.add(override.field);
            return override;
        })
        .concat(overrides.filter((override) => !added.has(override.field)));
};

export const applyOrFilterOverrides = (orFilters: Array<AndFilterInput>, overrides: Array<FacetFilterInput>) => {
    if (!orFilters.length) return generateOrFilters(UnionType.AND, overrides);

    return orFilters.map((orFilter) => ({
        and: applyFacetFilterOverrides(orFilter.and ?? [], overrides),
    }));
};
