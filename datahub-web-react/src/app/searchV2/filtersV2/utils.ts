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
