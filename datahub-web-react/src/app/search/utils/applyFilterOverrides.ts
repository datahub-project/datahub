import { AndFilterInput, FacetFilterInput } from '../../../types.generated';
import { UnionType } from './constants';
import { generateOrFilters } from './generateOrFilters';

// Swap in new overrides at the same filter positions, with the remainder at the end
// Assumes we find matches just by field name, so if multiple filters for the same field were passed, we replace those
export const applyFacetFilterOverrides = (filters: Array<FacetFilterInput>, overrides: Array<FacetFilterInput>) => {
    const overrideMap = overrides.reduce((map, override) => {
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
