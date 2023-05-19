import { AndFilterInput, FacetFilterInput } from '../../../types.generated';
import { UnionType } from './constants';
import { generateOrFilters } from './generateOrFilters';

const applyOrFilterOverrides = (orFilters: Array<AndFilterInput>, overrides: Array<FacetFilterInput>) => {
    if (!orFilters.length) return generateOrFilters(UnionType.AND, overrides);

    return orFilters.map((orFilter) => ({
        and: [...(orFilter.and ?? []), ...overrides],
    }));
};

export default applyOrFilterOverrides;
