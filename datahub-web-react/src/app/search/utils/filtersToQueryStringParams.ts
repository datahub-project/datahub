import { FacetFilterInput, SearchCondition } from '../../../types.generated';
import { encodeComma } from '../../entity/shared/utils';
import { DEGREE_FILTER, FILTER_URL_PREFIX } from './constants';

// transform filters from [{ filter, value }, { filter, value }] to { filter: [value, value ] } that QueryString can parse
export default function filtersToQueryStringParams(filters: Array<FacetFilterInput> = []) {
    return (
        filters
            // we need to special case `degree` filter since it is a OR grouping vs the others which are ANDS by default
            .reduce((acc: FacetFilterInput[], filter: FacetFilterInput) => {
                if (filter.field === DEGREE_FILTER && acc.filter((f) => f.field === DEGREE_FILTER).length > 0) {
                    return acc.map((f) =>
                        f.field === DEGREE_FILTER ? { ...f, values: [...f.values, ...filter.values] } : f,
                    ) as FacetFilterInput[];
                }
                return [...acc, filter] as FacetFilterInput[];
            }, [])
            .reduce((acc, filter, idx) => {
                acc[
                    `${FILTER_URL_PREFIX}${filter.field}___${String(!!filter.negated)}___${
                        filter.condition || SearchCondition.Equal
                    }___${idx}`
                ] = [...filter.values.map((value) => encodeComma(value))];
                return acc;
            }, {} as Record<string, string[]>)
    );
}
