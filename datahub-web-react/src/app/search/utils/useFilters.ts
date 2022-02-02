import { useMemo } from 'react';
import * as QueryString from 'query-string';

import { FILTER_URL_PREFIX } from './constants';
import { FacetFilterInput } from '../../../types.generated';
import { decodeComma } from '../../entity/shared/utils';

export default function useFilters(params: QueryString.ParsedQuery<string>): Array<FacetFilterInput> {
    return useMemo(
        () =>
            // get all query params
            Object.entries(params)
                // select only the ones with the `filter_` prefix
                .filter(([key, _]) => key.indexOf(FILTER_URL_PREFIX) >= 0)
                // transform the filters currently in format [key, [value1, value2]] to [{key: key, value: value1}, { key: key, value: value2}] format that graphql expects
                .flatMap(([key, value]) => {
                    // remove the `filter_` prefix
                    const field = key.replace(FILTER_URL_PREFIX, '');
                    if (!value) return [];

                    if (Array.isArray(value)) {
                        return value.map((distinctValue) => ({ field, value: decodeComma(distinctValue) }));
                    }
                    return [{ field, value: decodeComma(value) }];
                }),
        [params],
    );
}
