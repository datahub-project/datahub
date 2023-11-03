import { FacetFilterInput, AndFilterInput } from '../../../types.generated';
import { FILTER_DELIMITER, UnionType } from './constants';

// Generates a list of AND filter inputs to be combined in orFilters. This is used when unionType is OR or AND.
// When unionType = OR, pass in empty `filters` so the nested filters live alone in their AND statement.
// When unionType = AND, pass in all filters to combine with each nested filter to get AND between filter types,
// but OR within a nested filter.
function generateInputWithNestedFilters(filters: FacetFilterInput[], nestedFilters: FacetFilterInput[]) {
    const filtersWithNestedFilters: AndFilterInput[] = [];

    nestedFilters.forEach((nestedFilter) => {
        const [entity, subType] = nestedFilter.field.split(FILTER_DELIMITER);
        nestedFilter.values?.forEach((value) => {
            const [entityValue, subTypeValue] = value.split(FILTER_DELIMITER);
            const andFilters = [...filters, { field: entity, values: [entityValue] }];
            if (subTypeValue) {
                andFilters.push({ field: subType, values: [subTypeValue] });
            }
            filtersWithNestedFilters.push({ and: andFilters });
        });
    });

    return filtersWithNestedFilters;
}

export function generateOrFilters(
    unionType: UnionType,
    filters: FacetFilterInput[],
    excludedFilterFields: string[] = [],
): AndFilterInput[] {
    if ((filters?.length || 0) === 0) {
        return [];
    }
    const nonNestedFilters = filters.filter(
        (f) => !f.field.includes(FILTER_DELIMITER) && !excludedFilterFields?.includes(f.field),
    );
    const nestedFilters = filters.filter(
        (f) => f.field.includes(FILTER_DELIMITER) && !excludedFilterFields?.includes(f.field),
    );

    if (unionType === UnionType.OR) {
        const orFiltersWithNestedFilters = generateInputWithNestedFilters([], nestedFilters);
        const orFilters = nonNestedFilters.map((filter) => ({
            and: [filter],
        }));
        return [...orFilters, ...orFiltersWithNestedFilters];
    }
    const andFiltersWithNestedFilters = generateInputWithNestedFilters(nonNestedFilters, nestedFilters);

    if (andFiltersWithNestedFilters.length) {
        return andFiltersWithNestedFilters;
    }

    return [
        {
            and: nonNestedFilters,
        },
    ];
}
