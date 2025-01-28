import { FacetFilterInput, AndFilterInput, FilterOperator } from '../../../types.generated';
import { FrontendFacetFilterInput, FrontendFilterOperator } from '../filters/types';
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
                andFilters.push({
                    field: subType,
                    values: [subTypeValue],
                    condition: nestedFilter.condition,
                    negated: nestedFilter.negated,
                });
            }
            filtersWithNestedFilters.push({ and: andFilters });
        });
    });

    return filtersWithNestedFilters;
}

function isAllEqualsFilter(filter: FacetFilterInput | FrontendFacetFilterInput) {
    return (filter.condition as any) === FrontendFilterOperator.AllEqual;
}

function filterOutAllEqualsFilters(filters: (FacetFilterInput | FrontendFacetFilterInput)[]): FacetFilterInput[] {
    return filters.filter((f) => f.condition !== FrontendFilterOperator.AllEqual) as FacetFilterInput[];
}

export function generateOrFilters(
    unionType: UnionType,
    filters: (FacetFilterInput | FrontendFacetFilterInput)[],
    excludedFilterFields: string[] = [],
): AndFilterInput[] {
    if ((filters?.length || 0) === 0) {
        return [];
    }

    const finalFilters: FacetFilterInput[] = filterOutAllEqualsFilters(filters);
    filters.filter(isAllEqualsFilter).forEach((filterToSplit) => {
        filterToSplit.values?.forEach((value) => {
            finalFilters.push({ ...filterToSplit, value, values: [value], condition: FilterOperator.Equal });
        });
    });

    const nonNestedFilters = finalFilters.filter(
        (f) => !f.field.includes(FILTER_DELIMITER) && !excludedFilterFields?.includes(f.field),
    );
    const nestedFilters = finalFilters.filter(
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
