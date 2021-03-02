import { FacetFilterInput } from '../../../types.generated';

export function filtersToGraphqlParams(filters: Array<FacetFilterInput>): Array<FacetFilterInput> {
    return Object.entries(
        filters.reduce((acc, filter) => {
            acc[filter.field] = [...(acc[filter.field] || []), filter.value];
            return acc;
        }, {} as Record<string, string[]>),
    ).map(([field, values]) => ({ field, value: values.join(',') } as FacetFilterInput));
}
