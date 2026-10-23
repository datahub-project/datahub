import { encodeComma } from '@app/entity/shared/utils';
import { DEGREE_FILTER_NAME, FILTER_URL_PREFIX } from '@app/searchV2/utils/constants';

import { FacetFilterInput, FilterOperator } from '@types';

export const URL_PARAM_SEPARATOR = '___';

/** Placeholder so EXISTS filters survive query-string serialization (empty arrays are dropped). */
export const FILTER_URL_EXISTS_VALUE = 'true';

// In the checkbox-based filter view, usually, selecting two facets ANDs them together.
// E.g., if you select the checkbox for tagA and tagB, that means "has tagA AND tagB"
// we need to special case `degree` filter since it is a OR grouping vs the others which are ANDS by default
function reduceFiltersToCombineDegreeFilters(acc: FacetFilterInput[], filter: FacetFilterInput) {
    // if we see a `degree` filter and we already have one, combine it with the other degree filter
    if (filter.field === DEGREE_FILTER_NAME && acc.filter((f) => f.field === DEGREE_FILTER_NAME).length > 0) {
        // instead of appending this new degree filter, combine it with the previous one and continue
        return acc.map((f) =>
            f.field === DEGREE_FILTER_NAME ? { ...f, values: [...(f.values || []), ...(filter.values || [])] } : f,
        ) as FacetFilterInput[];
    }
    return [...acc, filter] as FacetFilterInput[];
}

// we need to reformat our list of filters into a dict
function reduceFiltersIntoQueryStringDict(acc, filter: FacetFilterInput, idx) {
    const condition = filter.condition || FilterOperator.Equal;
    const rawValues = filter.values || [];
    let serializedValues = rawValues.map((value) => encodeComma(value));
    // EXISTS has no real values — write a sentinel so query-string keeps the param.
    if (!serializedValues.length && condition === FilterOperator.Exists) {
        serializedValues = [FILTER_URL_EXISTS_VALUE];
    }

    if (!serializedValues.length) {
        return acc;
    }

    acc[
        `${FILTER_URL_PREFIX}${filter.field}${URL_PARAM_SEPARATOR}${String(!!filter.negated)}${URL_PARAM_SEPARATOR}${condition}${URL_PARAM_SEPARATOR}${idx}`
    ] = serializedValues;
    return acc;
}

// Serialize FacetFilterInput `values` arrays into query-string params that useFilters can parse
export default function filtersToQueryStringParams(filters: Array<FacetFilterInput> = []) {
    return filters
        .reduce(reduceFiltersToCombineDegreeFilters, [])
        .reduce(reduceFiltersIntoQueryStringDict, {} as Record<string, string[]>);
}
