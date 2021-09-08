import { useMemo } from 'react';
import * as QueryString from 'query-string';

import { ENTITY_FILTER_NAME, FILTER_URL_PREFIX } from './constants';
import { EntityType, FacetFilterInput } from '../../../types.generated';

export default function useFilters(
    params: QueryString.ParsedQuery<string>,
    includeEntityFilters: boolean,
): Array<FacetFilterInput> {
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

                    if (!includeEntityFilters && field === ENTITY_FILTER_NAME) {
                        return [];
                    }

                    if (Array.isArray(value)) {
                        return value.map((distinctValue) => ({ field, value: distinctValue }));
                    }
                    return [{ field, value }];
                }),
        [params, includeEntityFilters],
    );
}

export function useEntityFilters(params: QueryString.ParsedQuery<string>): Array<EntityType> {
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

                    if (field !== ENTITY_FILTER_NAME) {
                        return [];
                    }

                    if (Array.isArray(value)) {
                        return value.map((val) => val.toUpperCase()) as EntityType[];
                    }
                    return [value.toUpperCase() as EntityType];
                }),
        [params],
    );
}
