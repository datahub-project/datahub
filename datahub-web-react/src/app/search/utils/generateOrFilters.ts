import { FacetFilterInput, OrFilter } from '../../../types.generated';
import { UnionType } from './constants';

export function generateOrFilters(unionType: UnionType, filters: FacetFilterInput[]): OrFilter[] {
    if ((filters?.length || 0) === 0) {
        return [];
    }

    if (unionType === UnionType.OR) {
        return filters.map((filter) => ({
            and: [filter],
        }));
    }

    return [
        {
            and: filters,
        },
    ];
}
