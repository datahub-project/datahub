import { FacetFilterInput, AndFilterInput, QuickFilter, EntityType } from '../../../types.generated';
import { FilterSet } from '../../entity/shared/components/styled/search/types';
import { QuickFilterField } from '../autoComplete/quickFilters/utils';
import { UnionType } from './constants';

/**
 * Combines 2 sets of conjunctive filters in Disjunctive Normal Form
 *
 * Input:
 *
 *     conjunction1 -> [a, b]
 *     conjunction2 -> [c, d]
 *
 * Output:
 *
 *   or: [{
 *      and: [a, b, c, d]
 *   }]
 *
 * @param conjunction1 a conjunctive set of filters
 * @param conjunction2 a conjunctive set of filters
 */
const mergeConjunctions = (conjunction1: FacetFilterInput[], conjunction2: FacetFilterInput[]): AndFilterInput[] => {
    return [
        {
            and: [...conjunction1, ...conjunction2],
        },
    ];
};

/**
 * Combines 2 sets of disjunctive filters in Disjunctive Normal Form
 *
 * Input:
 *
 *     disjunction1 -> [a, b]
 *     disjunction2 -> [c, d]
 *
 * Output:
 *
 *   or: [
 *      {
 *          and: [a, c]
 *      },
 *      {
 *          and: [a, d]
 *      },
 *      {
 *          and: [b, c]
 *      },
 *      {
 *          and: [b, d]
 *      }
 *   ]
 *
 * @param disjunction1 a disjunctive set of filters
 * @param disjunction2 a disjunctive set of filters
 */
const mergeDisjunctions = (disjunction1: FacetFilterInput[], disjunction2: FacetFilterInput[]): AndFilterInput[] => {
    const finalOrFilters: AndFilterInput[] = [];

    disjunction1.forEach((d1) => {
        disjunction2.forEach((d2) => {
            const andFilters = [d1, d2];
            finalOrFilters.push({ and: andFilters });
        });
    });

    return finalOrFilters;
};

/**
 * Combines 2 sets of filters, one conjunctive and the other disjunctive in Disjunctive Normal Form
 *
 * Input:
 *
 *     conjunction -> [a, b]
 *     disjunction -> [c, d]
 *
 * Output:
 *
 *   or: [
 *      {
 *        and: [a, b, c]
 *      },
 *      {
 *        and: [a, b, d]
 *      }
 *   ]
 *
 * @param conjunction a conjunctive set of filters
 * @param disjunction a disjunctive set of filters
 */
const mergeConjunctionDisjunction = (
    conjunction: FacetFilterInput[],
    disjunction: FacetFilterInput[],
): AndFilterInput[] => {
    const finalOrFilters: AndFilterInput[] = [];

    disjunction.forEach((filter) => {
        const andFilters = [filter, ...conjunction];
        finalOrFilters.push({ and: andFilters });
    });

    return finalOrFilters;
};

/**
 * Merges the "fixed" set of disjunctive (OR) filters with a
 * set of base filters that are themselves joined by a logical operator (AND, OR)
 *
 * It does this by producing a set of filters that model the Disjunctive Normal Form,
 * which is a way of representing any boolean logical operator as an OR (disjunction) performed
 * across a set of AND (conjunctions) conditions.
 *
 * @param orFilters the fixed set of filters in disjunction
 * @param baseFilters a base set of filters in either conjunction or disjunction
 */
const mergeOrFilters = (orFilters: FacetFilterInput[], baseFilters: FilterSet): AndFilterInput[] => {
    // If the user-provided union type is AND, we need to treat differenty
    // than if user-provided union type is OR.
    if (baseFilters.unionType === UnionType.AND) {
        return mergeConjunctionDisjunction(baseFilters.filters, orFilters);
    }
    return mergeDisjunctions(orFilters, baseFilters.filters);
};

/**
 * Merges the "fixed" set of conjunctive (AND) filters with a
 * set of base filters that are themselves joined by a logical operator (AND, OR)
 *
 * It does this by producing a set of filters that model the Disjunctive Normal Form,
 * which is a way of representing any boolean logical operator as an OR (disjunction) performed
 * across a set of AND (conjunctions) conditions.
 *
 * @param andFilters the fixed set of filters in conjunction
 * @param baseFilters a base set of filters in either conjunction or disjunction
 */
const mergeAndFilters = (andFilters: FacetFilterInput[], baseFilters: FilterSet): AndFilterInput[] => {
    // If the user-provided union type is AND, we need to treat differenty
    // than if user-provided union type is OR.
    if (baseFilters.unionType === UnionType.AND) {
        return mergeConjunctions(andFilters, baseFilters.filters);
    }
    return mergeConjunctionDisjunction(andFilters, baseFilters.filters);
};

/**
 * Merges in a set of Fixed Filters with user-provided base filters.
 * Both arguments are required.
 *
 * @param filterSet1 the fixed set of filters to be merged.
 * @param filterSet2 the set of base filters to merge into.
 */
export const mergeFilterSets = (filterSet1: FilterSet, filterSet2: FilterSet): AndFilterInput[] => {
    if (filterSet1 && filterSet2) {
        if (filterSet1.unionType === UnionType.AND) {
            // Inject fixed AND filters.
            return mergeAndFilters(filterSet1.filters, filterSet2);
        }
        // Inject fixed OR filters.
        return mergeOrFilters(filterSet1.filters, filterSet2);
    }
    return [];
};

function generateFilterInputFromQuickFilter(selectedQuickFilter: QuickFilter) {
    return { field: selectedQuickFilter.field, values: [selectedQuickFilter.value] };
}

/**
 * Generates a list of a singular facet filter with the selected quick filter.
 * If no selected quick filter, return an empty list
 */
export function getFiltersWithQuickFilter(selectedQuickFilter: QuickFilter | null) {
    const filters: FacetFilterInput[] = [];
    if (selectedQuickFilter) {
        filters.push(generateFilterInputFromQuickFilter(selectedQuickFilter));
    }
    return filters;
}

export function getAutoCompleteInputFromQuickFilter(selectedQuickFilter: QuickFilter | null) {
    const filters: FacetFilterInput[] = [];
    const types: EntityType[] = [];
    if (selectedQuickFilter) {
        if (selectedQuickFilter.field === QuickFilterField.Entity) {
            types.push(selectedQuickFilter.value as EntityType);
        } else if (selectedQuickFilter.field === QuickFilterField.Platform) {
            filters.push(generateFilterInputFromQuickFilter(selectedQuickFilter));
        }
    }

    return { filters, types };
}
