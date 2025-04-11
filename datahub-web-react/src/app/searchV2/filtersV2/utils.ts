import { convertNestedSubTypeFilter } from '@src/app/entityV2/view/builder/utils';
import { FieldName, FieldToAppliedFieldFiltersMap } from './types';
import { UnionType } from '../utils/constants';
import { generateOrFilters } from '../utils/generateOrFilters';

export function itemsToMap<T, K>(items: Array<T>, keyAccessor?: (item: T) => K | T) {
    const accessor = (item: T) => (keyAccessor ? keyAccessor(item) : item);
    return new Map<K | T, T>(items.map((item) => [accessor(item), item]));
}

export function getUniqueItemsByKey<T>(items: Array<T>, keyAccessor: (item: T) => string) {
    return Array.from(itemsToMap(items, keyAccessor).values());
}

export function getUniqueItemsByKeyFromArrrays<T, K>(arrays: Array<Array<T>>, keyAccessor?: (item: T) => K | T) {
    return Array.from(
        arrays
            .reduce(
                (uniqueItems, items) => new Map([...uniqueItems, ...itemsToMap(items, keyAccessor)]),
                new Map<K | T, T>(),
            )
            .values(),
    );
}

// TODO: add tests
export function convertFiltersMapToFilters(
    filtersMap: FieldToAppliedFieldFiltersMap | undefined,
    options?: {
        includedFields?: FieldName[];
        excludedFields?: FieldName[];
    },
) {
    return Array.from(filtersMap?.entries?.() || [])
        .filter(([fieldName, _]) => options?.includedFields === undefined || options.includedFields.includes(fieldName))
        .filter(
            ([fieldName, _]) => options?.excludedFields === undefined || !options.excludedFields.includes(fieldName),
        )
        .flatMap(([_, value]) => value.filters)
        .filter((filter) => filter.values?.length);
}
