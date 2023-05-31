import { useMemo } from 'react';
import * as QueryString from 'query-string';

import { ENTITY_FILTER_NAME, FILTER_URL_PREFIX, LEGACY_ENTITY_FILTER_NAME } from './constants';
import { FacetFilterInput, FilterOperator } from '../../../types.generated';
import { decodeComma } from '../../entity/shared/utils';
import { URL_PARAM_SEPARATOR } from './filtersToQueryStringParams';

function ifLegacyFieldNameTranslate(fieldName) {
    if (fieldName === LEGACY_ENTITY_FILTER_NAME) {
        return ENTITY_FILTER_NAME;
    }
    return fieldName;
}

export default function useFilters(params: QueryString.ParsedQuery<string>): Array<FacetFilterInput> {
    return useMemo(() => {
        return (
            Object.entries(params)
                // select only the ones with the `filter_` prefix
                .filter(([key, _]) => key.indexOf(FILTER_URL_PREFIX) >= 0)
                // transform the filters currently in format [key, [value1, value2]] to [{key: key, value: value1}, { key: key, value: value2}] format that graphql expects
                .map(([key, value]) => {
                    // remove the `filter_` prefix
                    const fieldIndex = key.replace(FILTER_URL_PREFIX, '');
                    const fieldParts = fieldIndex.split(URL_PARAM_SEPARATOR);
                    const field = ifLegacyFieldNameTranslate(fieldParts[0]);
                    const negated = fieldParts[1] === 'true';
                    const condition = fieldParts[2] || FilterOperator.Equal;
                    if (!value) return null;

                    if (Array.isArray(value)) {
                        return {
                            field,
                            condition,
                            negated,
                            values: value.map((distinctValue) => decodeComma(distinctValue)),
                        };
                    }
                    return { field, condition, values: [decodeComma(value)], negated };
                })
                .filter((val) => !!val) as Array<FacetFilterInput>
        );
    }, [params]);
}
