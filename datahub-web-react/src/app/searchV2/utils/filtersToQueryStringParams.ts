import { FacetFilterInput, FilterOperator } from '../../../types.generated';
import { encodeComma } from '../../entity/shared/utils';
import { DEGREE_FILTER_NAME, FILTER_URL_PREFIX } from './constants';

export const URL_PARAM_SEPARATOR = '___';

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
function reduceFiltersIntoQueryStringDict(acc, filter, idx) {
    acc[
        `${FILTER_URL_PREFIX}${filter.field}${URL_PARAM_SEPARATOR}${String(!!filter.negated)}${URL_PARAM_SEPARATOR}${
            filter.condition || FilterOperator.Equal
        }${URL_PARAM_SEPARATOR}${idx}`
    ] = [...filter.values.map((value) => encodeComma(value))];
    return acc;
}

// transform filters from [{ filter, value }, { filter, value }] to { filter: [value, value ] } that QueryString can parse
export default function filtersToQueryStringParams(filters: Array<FacetFilterInput> = []) {
    return filters
        .reduce(reduceFiltersToCombineDegreeFilters, [])
        .reduce(reduceFiltersIntoQueryStringDict, {} as Record<string, string[]>);
}
