import { FieldName, FieldToAppliedFieldFiltersMap, FieldToFacetStateMap } from '@app/searchV2/filtersV2/types';
import { FacetMetadata } from '@src/types.generated';

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

// TODO:: add tests
export function convertFacetsToFieldToFacetStateMap(
    facets: FacetMetadata[] | undefined,
): FieldToFacetStateMap | undefined {
    if (facets === undefined) return undefined;

    return new Map(facets.map((facet) => [facet.field, { facet, loading: false }]));
}
