import { History, Location } from 'history';
import { useCallback } from 'react';
import { useHistory, useLocation } from 'react-router';

import {
    FAILING_ASSERTION_TYPE_FILTER_FIELD,
    HAS_FAILING_ASSERTIONS_FILTER_FIELD,
} from '@app/observe/dataset/assertion/constants';
import { UnionType } from '@app/search/utils/constants';

export type QueryParamDecoder<T> = { [K in keyof T]: (value: string) => T[K] | undefined };
export type QueryParamEncoder<T> = { [K in keyof T]: (value: T[K]) => string };

// visible for testing
export const getFilterFromQueryParams = <T extends object>(
    decoder: QueryParamDecoder<T>,
    defaultFilter: T,
    location: Location,
) => {
    const searchParams = new URLSearchParams(location.search);
    const result: T = { ...defaultFilter };
    Object.keys(decoder).forEach((key) => {
        const value = searchParams.get(key);
        if (value) {
            result[key] = decoder[key](value) || defaultFilter[key];
        }
    });
    return result;
};

// visible for testing
export const setFilterToQueryParams = <T extends object>(
    filter: T,
    encoder: QueryParamEncoder<T>,
    location: Location,
    history: History,
) => {
    const searchParams = new URLSearchParams(location.search);
    Object.keys(filter).forEach((key) => {
        const value = encoder[key](filter[key]);
        if (value) {
            searchParams.set(key, value);
        } else {
            searchParams.delete(key);
        }
    });
    history.push({ search: searchParams.toString() });
};

export function useSyncFiltersWithQueryParams() {
    const location = useLocation();
    const history = useHistory();

    return {
        getFilterFromQueryParams: useCallback(
            <T extends object>(decoder: QueryParamDecoder<T>, defaultFilter: T) =>
                getFilterFromQueryParams(decoder, defaultFilter, location),
            [location],
        ),
        setFilterToQueryParams: useCallback(
            <T extends object>(filter: T, encoder: QueryParamEncoder<T>) =>
                setFilterToQueryParams(filter, encoder, location, history),
            [location, history],
        ),
    };
}

export const buildAssertionTypeFilters = (selectedAssertionTypes) => {
    if (selectedAssertionTypes) {
        return {
            unionType: UnionType.OR,
            filters: selectedAssertionTypes.map((assertionType) => ({
                field: FAILING_ASSERTION_TYPE_FILTER_FIELD,
                value: assertionType,
            })),
        };
    }
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: HAS_FAILING_ASSERTIONS_FILTER_FIELD,
                value: 'true',
            },
        ],
    };
};

export const compareListItems = (list1: string[], list2: string[]): boolean => {
    if (list1.length !== list2.length) {
        return false;
    }
    return list1.every((item) => list2.includes(item));
};
