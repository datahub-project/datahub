import { FacetFilterInput } from '../../../types.generated';
import { encodeComma } from '../../entity/shared/utils';
import { FILTER_URL_PREFIX } from './constants';

// transform filters from [{ filter, value }, { filter, value }] to { filter: [value, value ] } that QueryString can parse
export default function filtersToQueryStringParams(filters: Array<FacetFilterInput> = []) {
    return filters.reduce((acc, filter) => {
        acc[`${FILTER_URL_PREFIX}${filter.field}`] = [
            ...(acc[`${FILTER_URL_PREFIX}${filter.field}`] || []),
            encodeComma(filter.value),
        ];
        return acc;
    }, {} as Record<string, string[]>);
}
