export const isPresent = (val?: string | number | null): val is string | number => {
    return val !== undefined && val !== null;
};

/**
 * Computes a set of the object keys across all items in a given array.
 */
export const getItemKeySet = (items: Array<any>) => {
    const keySet = new Set<string>();
    items.forEach((item) => {
        Object.keys(item).forEach((key) => {
            keySet.add(key);
        });
    });
    return keySet;
};
